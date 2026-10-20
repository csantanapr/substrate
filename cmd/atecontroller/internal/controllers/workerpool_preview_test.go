// Copyright 2026 The Agent Substrate Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controllers

import (
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	k8errors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	atev1 "go.ate.dev/substrate/api/infra/k8s/ate/v1"
	"go.ate.dev/substrate/internal/preview"
)

// poolWithNetworkTransport returns a pool that opts into network transport. It
// carries the gvisor sandbox class, both because networkTransport is only valid
// with gvisor and so the reconciler test's success path can resolve a class.
func poolWithNetworkTransport() *atev1.WorkerPool {
	return &atev1.WorkerPool{
		ObjectMeta: metav1.ObjectMeta{Name: "pool-a", Namespace: "default"},
		Spec: atev1.WorkerPoolSpec{
			Replicas:         1,
			WorkerImage:      "example.com/worker:v1",
			SandboxClasses:   []atev1.WorkerPoolSandboxClass{{Name: atev1.SandboxClassGvisor}},
			NetworkTransport: &atev1.WorkerPoolNetworkTransport{RuntimeClassName: "guest-class"},
		},
	}
}

// poolWithoutNetworkTransport returns a pool that does not opt in.
func poolWithoutNetworkTransport() *atev1.WorkerPool {
	wp := poolWithNetworkTransport()
	wp.Spec.NetworkTransport = nil
	return wp
}

// TestNetworkTransportPreviewGate proves networkTransportGateError gates the
// spec.networkTransport field by the NetworkWorkers preview gate: a pool that
// sets the field is refused when the gate is off and admitted when it is on,
// while a pool that leaves it unset is admitted either way, so the gate being
// off cannot change a colocated pool. It is the sole decider the reconciler
// consults, so exercising it both ways covers the reconcile path.
func TestNetworkTransportPreviewGate(t *testing.T) {
	t.Run("gate off: network pool is refused", func(t *testing.T) {
		preview.InitForTest(t) // no gates
		if err := networkTransportGateError(poolWithNetworkTransport()); err == nil {
			t.Fatal("expected networkTransport to be refused with the gate off, got nil")
		}
	})
	t.Run("gate on: network pool is admitted", func(t *testing.T) {
		preview.InitForTest(t, string(preview.GateNetworkWorkers))
		if err := networkTransportGateError(poolWithNetworkTransport()); err != nil {
			t.Fatalf("expected networkTransport to be admitted with the gate on, got %v", err)
		}
	})
	t.Run("gate off: a colocated pool is unaffected", func(t *testing.T) {
		preview.InitForTest(t) // no gates
		if err := networkTransportGateError(poolWithoutNetworkTransport()); err != nil {
			t.Fatalf("expected a pool without networkTransport to be admitted with the gate off, got %v", err)
		}
	})
	t.Run("gate on: a colocated pool is still admitted", func(t *testing.T) {
		preview.InitForTest(t, string(preview.GateNetworkWorkers))
		if err := networkTransportGateError(poolWithoutNetworkTransport()); err != nil {
			t.Fatalf("expected a pool without networkTransport to be admitted with the gate on, got %v", err)
		}
	})
}

// newGateReconciler builds a WorkerPoolReconciler backed by a fake client that
// already holds objs plus the default gvisor SandboxConfig, so a networkTransport
// pool's reconcile can resolve its class and apply a Deployment. It returns the
// reconciler and the client so a test can read back what reconcile wrote.
func newGateReconciler(t *testing.T, objs ...client.Object) (*WorkerPoolReconciler, client.Client) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := atev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	all := append([]client.Object{makeSandboxConfig(testGvisorDefault, atev1.SandboxClassGvisor, true)}, objs...)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(all...).
		WithStatusSubresource(&atev1.WorkerPool{}, &atev1.SandboxConfig{}).Build()
	r := &WorkerPoolReconciler{
		Client:               c,
		Scheme:               scheme,
		APIReader:            c,
		Recorder:             events.NewFakeRecorder(10),
		SystemNamespace:      "ate-system",
		AteletServiceAccount: "atelet",
		RouterServiceAccount: "atenet-router",
	}
	return r, c
}

func deploymentExists(t *testing.T, c client.Client, wp *atev1.WorkerPool) bool {
	t.Helper()
	err := c.Get(t.Context(), types.NamespacedName{Name: wp.Name, Namespace: wp.Namespace}, &appsv1.Deployment{})
	if err == nil {
		return true
	}
	if k8errors.IsNotFound(err) {
		return false
	}
	t.Fatalf("get Deployment: %v", err)
	return false
}

// TestReconcileWorkerPoolNetworkTransportGate drives reconcileWorkerPool itself,
// not just networkTransportGateError, so it proves the reconciler actually
// CALLS the gate and acts on its result. A call-site change (deleting the call,
// or moving it to after the Deployment is applied) that the helper's own test
// cannot see is caught here by the "no Deployment created" assertion.
//
// With the NetworkWorkers gate off, a pool that sets networkTransport must be
// rejected AND leave no worker Deployment behind. With the gate on, the
// reconcile proceeds and the Deployment is created. A pool without
// networkTransport reconciles to a Deployment whatever the gate, so the gate
// being off never changes a colocated pool.
//
// InitForTestFake sets the process-wide gates for the test and restores them in
// cleanup, and (via Setenv) forces the test to be non-parallel so it does not
// race another test's gate state.
func TestReconcileWorkerPoolNetworkTransportGate(t *testing.T) {
	gates := []preview.Gate{preview.GateNetworkWorkers}

	t.Run("gate off: network pool is rejected and no Deployment is created", func(t *testing.T) {
		preview.InitForTestFake(t, gates) // NetworkWorkers off
		wp := poolWithNetworkTransport()
		r, c := newGateReconciler(t, wp)
		_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Name: wp.Name, Namespace: wp.Namespace}})
		if err == nil {
			t.Fatal("expected reconcile to return the gate error with the gate off, got nil")
		}
		if deploymentExists(t, c, wp) {
			t.Fatal("a worker Deployment was created even though the gate is off and the pool sets networkTransport")
		}
	})

	t.Run("gate on: network pool reconciles and the Deployment is created", func(t *testing.T) {
		preview.InitForTestFake(t, gates, string(preview.GateNetworkWorkers)) // NetworkWorkers on
		wp := poolWithNetworkTransport()
		r, c := newGateReconciler(t, wp)
		if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Name: wp.Name, Namespace: wp.Namespace}}); err != nil {
			t.Fatalf("expected reconcile to succeed with the gate on, got %v", err)
		}
		if !deploymentExists(t, c, wp) {
			t.Fatal("no worker Deployment was created with the gate on for a networkTransport pool")
		}
	})

	t.Run("gate off: a colocated pool still reconciles to a Deployment", func(t *testing.T) {
		preview.InitForTestFake(t, gates) // NetworkWorkers off
		wp := poolWithoutNetworkTransport()
		r, c := newGateReconciler(t, wp)
		if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Name: wp.Name, Namespace: wp.Namespace}}); err != nil {
			t.Fatalf("expected a pool without networkTransport to reconcile with the gate off, got %v", err)
		}
		if !deploymentExists(t, c, wp) {
			t.Fatal("no worker Deployment was created for a colocated pool with the gate off")
		}
	})
}

func getDeploymentOrFail(t *testing.T, c client.Client, wp *atev1.WorkerPool) *appsv1.Deployment {
	t.Helper()
	dep := &appsv1.Deployment{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: wp.Name, Namespace: wp.Namespace}, dep); err != nil {
		t.Fatalf("get Deployment: %v", err)
	}
	return dep
}

// TestReconcileWorkerPoolExistingDeploymentGate covers the ordering case the
// first reconcile test cannot: a pool that ALREADY has a Deployment and is then
// updated to set networkTransport with the gate off. reconcileWorkerPool
// re-applies an existing Deployment early (the `if existing != nil` block), so
// the gate must be checked BEFORE that re-apply; otherwise the pool's shape
// would be rewritten (runtimeClassName, the guest-userns initContainer, the
// ate-base volume flipping hostPath->emptyDir) under a gate that is off. This
// test creates a real prior Deployment by reconciling the same pool WITHOUT
// networkTransport (gate off), then sets networkTransport (gate still off) and
// requires the reconcile to return the gate error AND leave the existing
// Deployment byte-identical. Moving the gate check after the early re-apply is
// caught here (and only here).
func TestReconcileWorkerPoolExistingDeploymentGate(t *testing.T) {
	preview.InitForTestFake(t, []preview.Gate{preview.GateNetworkWorkers}) // NetworkWorkers off

	// A colocated pool, reconciled once to create a real prior Deployment.
	wp := poolWithoutNetworkTransport()
	r, c := newGateReconciler(t, wp)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: wp.Name, Namespace: wp.Namespace}}
	if _, err := r.Reconcile(t.Context(), req); err != nil {
		t.Fatalf("first reconcile (colocated, gate off) should succeed, got %v", err)
	}
	before := getDeploymentOrFail(t, c, wp)
	// A colocated Deployment carries no runtimeClassName, no initContainers, and
	// a hostPath ate-base volume. A network re-apply would change all three.
	if before.Spec.Template.Spec.RuntimeClassName != nil {
		t.Fatalf("prior Deployment unexpectedly has runtimeClassName set: %q", *before.Spec.Template.Spec.RuntimeClassName)
	}
	if len(before.Spec.Template.Spec.InitContainers) != 0 {
		t.Fatalf("prior Deployment unexpectedly has %d initContainers", len(before.Spec.Template.Spec.InitContainers))
	}

	// Now set networkTransport on the stored pool, gate still off, and reconcile.
	stored := &atev1.WorkerPool{}
	if err := c.Get(t.Context(), req.NamespacedName, stored); err != nil {
		t.Fatalf("get WorkerPool: %v", err)
	}
	stored.Spec.NetworkTransport = &atev1.WorkerPoolNetworkTransport{RuntimeClassName: "guest-class"}
	if err := c.Update(t.Context(), stored); err != nil {
		t.Fatalf("update WorkerPool with networkTransport: %v", err)
	}

	if _, err := r.Reconcile(t.Context(), req); err == nil {
		t.Fatal("expected the reconcile to return the gate error for an existing-Deployment pool that sets networkTransport with the gate off, got nil")
	}

	// The existing Deployment must be byte-identical: the gate rejected the
	// pool before any re-apply. resourceVersion is the strongest signal (any
	// write bumps it in the fake client); the pod-template invariants catch the
	// network re-apply directly even if resourceVersion were stable.
	after := getDeploymentOrFail(t, c, wp)
	if after.ResourceVersion != before.ResourceVersion {
		t.Errorf("existing Deployment resourceVersion changed from %q to %q: the reconcile re-applied it under a gate that is off",
			before.ResourceVersion, after.ResourceVersion)
	}
	if after.Spec.Template.Spec.RuntimeClassName != nil {
		t.Errorf("existing Deployment gained runtimeClassName %q: the network shape was applied under a gate that is off", *after.Spec.Template.Spec.RuntimeClassName)
	}
	if len(after.Spec.Template.Spec.InitContainers) != 0 {
		t.Errorf("existing Deployment gained %d initContainers: the network shape was applied under a gate that is off", len(after.Spec.Template.Spec.InitContainers))
	}
}
