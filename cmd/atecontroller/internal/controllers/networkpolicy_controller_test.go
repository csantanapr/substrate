// Copyright 2026 The Agent Substrate Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controllers

import (
	"context"
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"
	networkingv1ac "k8s.io/client-go/applyconfigurations/networking/v1"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"

	atev1 "go.ate.dev/substrate/api/infra/k8s/ate/v1"
	"go.ate.dev/substrate/internal/installdefaults"
	"go.ate.dev/substrate/internal/resources"
)

func TestWorkerPoolCreatesNetworkPolicy(t *testing.T) {
	ctx := t.Context()
	wp := makeWorkerPool("test-netpolicy-create", "default", 2, "worker:v1")
	if err := k8sClient.Create(ctx, wp); err != nil {
		t.Fatalf("create WorkerPool: %v", err)
	}
	deleteOnCleanup(t, wp)

	eventually(t, func(ctx context.Context) (bool, error) {
		npName := resources.NetworkPolicyName(wp.Name)
		np := &networkingv1.NetworkPolicy{}
		err := k8sClient.Get(ctx, types.NamespacedName{Name: npName, Namespace: wp.Namespace}, np)
		if err != nil {
			return false, nil
		}

		// Verify OwnerReference
		if len(np.OwnerReferences) == 0 || np.OwnerReferences[0].Name != wp.Name {
			return false, nil
		}

		// Verify metadata label matches the worker pool
		if np.Labels == nil || np.Labels["ate.dev/worker-pool"] != wp.Name {
			return false, nil
		}

		// Verify PodSelector matches the worker pool
		if np.Spec.PodSelector.MatchLabels == nil || np.Spec.PodSelector.MatchLabels["ate.dev/worker-pool"] != wp.Name {
			return false, nil
		}

		// Verify PolicyTypes contains Ingress
		hasIngress := false
		for _, pt := range np.Spec.PolicyTypes {
			if pt == networkingv1.PolicyTypeIngress {
				hasIngress = true
			}
		}
		if !hasIngress {
			return false, nil
		}

		// Verify Ingress Rules (Allow only ingress from ATE router)
		if len(np.Spec.Ingress) != 1 {
			return false, nil
		}
		ingressRule := np.Spec.Ingress[0]
		if len(ingressRule.From) != 1 {
			return false, nil
		}
		fromPeer := ingressRule.From[0]
		if fromPeer.NamespaceSelector == nil || fromPeer.NamespaceSelector.MatchLabels["kubernetes.io/metadata.name"] != installdefaults.SystemNamespace {
			return false, nil
		}
		if fromPeer.PodSelector == nil || fromPeer.PodSelector.MatchLabels["app"] != atenetRouterAppName {
			return false, nil
		}

		// Verify Egress Rules are unmanaged (empty)
		if len(np.Spec.Egress) != 0 {
			return false, nil
		}

		return true, nil
	})
}

// TestBuildNetworkPolicyRelocatedNamespace pins the ingress peer to the
// reconciler's SystemNamespace rather than the canonical install namespace.
// The rest of the suite configures the reconciler with the default, so it
// passes just as well against a hardcoded "ate-system"; this is the case that
// catches that. A policy naming the wrong namespace admits nobody, and the CNI
// drops every request to the pool with no error from substrate itself.
func TestBuildNetworkPolicyRelocatedNamespace(t *testing.T) {
	const relocated = "substrate-test"

	r := &NetworkPolicyReconciler{SystemNamespace: relocated}
	np := r.buildNetworkPolicyApplyConfig(testWorkerPoolApplyConfig(nil))

	if len(np.Spec.Ingress) != 1 || len(np.Spec.Ingress[0].From) != 1 {
		t.Fatalf("expected exactly one ingress rule with one peer, got %+v", np.Spec.Ingress)
	}
	got := np.Spec.Ingress[0].From[0].NamespaceSelector.MatchLabels["kubernetes.io/metadata.name"]
	if got != relocated {
		t.Errorf("ingress namespace selector = %q, want %q", got, relocated)
	}
}

// TestNetworkPolicyTakeover pre-creates the pool's NetworkPolicy and checks
// that reconcile rewrites it only when it belongs to the pool.
func TestNetworkPolicyTakeover(t *testing.T) {
	tests := []struct {
		name     string
		label    string
		ownerUID types.UID
		wantErr  bool
	}{
		{
			name:    "labeled for another pool",
			label:   "other-pool",
			wantErr: true,
		},
		{
			name:    "unlabeled",
			wantErr: true,
		},
		{
			name:     "controlled by an earlier pool with the same name",
			label:    "netpol-takeover",
			ownerUID: "earlier-uid",
			wantErr:  true,
		},
		{
			name:  "orphaned policy of this pool",
			label: "netpol-takeover",
		},
		{
			name:     "controlled by this pool with an edited label",
			label:    "edited",
			ownerUID: "netpol-takeover-uid",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			// The pool is never created, so only this test reconciles it.
			wp := makeWorkerPool("netpol-takeover", "default", 1, "worker:v1")
			wp.UID = "netpol-takeover-uid"

			// Apply as the controller's field manager, as an earlier reconcile would.
			// The API server rejects a second controller reference written by another
			// manager, so the earlier-pool row would pass even without the check.
			ac := networkingv1ac.NetworkPolicy(resources.NetworkPolicyName(wp.Name), wp.Namespace).
				WithSpec(networkingv1ac.NetworkPolicySpec().
					WithPolicyTypes(networkingv1.PolicyTypeIngress))
			if tt.label != "" {
				ac.WithLabels(map[string]string{workerPoolLabel: tt.label})
			}
			if tt.ownerUID != "" {
				ac.WithOwnerReferences(metav1ac.OwnerReference().
					WithAPIVersion(atev1.GroupVersion.String()).
					WithKind("WorkerPool").
					WithName(wp.Name).
					WithUID(tt.ownerUID).
					WithController(true))
			}
			if err := k8sClient.Apply(ctx, ac, client.FieldOwner(networkPolicyFieldOwner)); err != nil {
				t.Fatalf("apply NetworkPolicy: %v", err)
			}
			np := &networkingv1.NetworkPolicy{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: *ac.Name, Namespace: *ac.Namespace}, np); err != nil {
				t.Fatalf("get NetworkPolicy: %v", err)
			}
			deleteOnCleanup(t, np)

			rec := events.NewFakeRecorder(10)
			r := &NetworkPolicyReconciler{Client: k8sClient, SystemNamespace: installdefaults.SystemNamespace, Recorder: rec}
			err := r.reconcileImpl(ctx, wp)
			if (err != nil) != tt.wantErr {
				t.Fatalf("reconcileImpl() error = %v, wantErr %v", err, tt.wantErr)
			}
			var gotEvents, wantEvents []string
			for len(rec.Events) > 0 {
				gotEvents = append(gotEvents, <-rec.Events)
			}
			if err != nil {
				wantEvents = []string{"Warning " + eventReasonNetworkPolicyConflict + " " + err.Error()}
			}
			if !slices.Equal(gotEvents, wantEvents) {
				t.Errorf("events = %q, want %q", gotEvents, wantEvents)
			}

			got := &networkingv1.NetworkPolicy{}
			if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(np), got); err != nil {
				t.Fatalf("get NetworkPolicy: %v", err)
			}
			if tt.wantErr {
				if got.ResourceVersion != np.ResourceVersion {
					t.Errorf("NetworkPolicy was rewritten: %+v", got)
				}
				return
			}
			if !metav1.IsControlledBy(got, wp) {
				t.Errorf("NetworkPolicy owners = %+v, want controller UID %s", got.OwnerReferences, wp.UID)
			}
			if got.Labels[workerPoolLabel] != wp.Name {
				t.Errorf("NetworkPolicy label %s = %q, want %q", workerPoolLabel, got.Labels[workerPoolLabel], wp.Name)
			}
		})
	}
}

// TestBuildNetworkPolicyNetworkTransport covers the atelet ingress rule a
// network pool needs. Its workers serve their Worker and WorkerStaging
// services on networkWorkerListenPort instead of the node-local socket, so
// atelet dials them over the Pod network. Without the rule the CNI drops that
// dial, the worker never stages an activation, and every actor on the pool
// crashes with no error from the worker itself. A colocated pool must not get
// the rule, so both directions are pinned here.
func TestBuildNetworkPolicyNetworkTransport(t *testing.T) {
	r := &NetworkPolicyReconciler{SystemNamespace: installdefaults.SystemNamespace}

	colocated := r.buildNetworkPolicyApplyConfig(testWorkerPoolApplyConfig(nil))
	if len(colocated.Spec.Ingress) != 1 {
		t.Errorf("colocated pool ingress rules = %d, want 1 (router only)", len(colocated.Spec.Ingress))
	}

	wp := testWorkerPoolApplyConfig(nil)
	wp.Spec.NetworkTransport = &atev1.WorkerPoolNetworkTransport{RuntimeClassName: "kata"}
	np := r.buildNetworkPolicyApplyConfig(wp)

	if len(np.Spec.Ingress) != 2 {
		t.Fatalf("network pool ingress rules = %d, want 2 (router and atelet), got %+v", len(np.Spec.Ingress), np.Spec.Ingress)
	}
	atelet := np.Spec.Ingress[1]
	if len(atelet.From) != 1 {
		t.Fatalf("atelet rule peers = %d, want 1", len(atelet.From))
	}
	if got := atelet.From[0].PodSelector.MatchLabels["app"]; got != ateletAppName {
		t.Errorf("atelet rule pod selector app = %q, want %q", got, ateletAppName)
	}
	if got := atelet.From[0].NamespaceSelector.MatchLabels["kubernetes.io/metadata.name"]; got != installdefaults.SystemNamespace {
		t.Errorf("atelet rule namespace selector = %q, want %q", got, installdefaults.SystemNamespace)
	}
	// The port is the contract networkWorkerArgs builds --worker-listen-address
	// from; widening the rule to every port would expose atunnel on :443 too.
	if len(atelet.Ports) != 1 {
		t.Fatalf("atelet rule ports = %d, want 1", len(atelet.Ports))
	}
	if got := atelet.Ports[0].Port.IntValue(); got != networkWorkerListenPort {
		t.Errorf("atelet rule port = %d, want %d", got, networkWorkerListenPort)
	}
	if got := atelet.Ports[0].Protocol; got == nil || *got != corev1.ProtocolTCP {
		t.Errorf("atelet rule protocol = %v, want TCP", got)
	}
}
