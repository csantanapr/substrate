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

package main

import (
	"context"
	"testing"

	"go.ate.dev/substrate/internal/installdefaults"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/substratex509"
	"go.ate.dev/substrate/internal/workertransport"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

// workerIdentity and atelet identity share a node incarnation; the worker Pod
// is a network-enabled worker on that node with a non-empty worker-pool label.
func testIdentities() (atelet, worker substratex509.PodIdentity) {
	atelet = substratex509.PodIdentity{
		Namespace: "ate-system", ServiceAccountName: "atelet", PodName: "atelet-xyz",
		PodUID: "atelet-uid", NodeName: "node-a", NodeUID: "node-a-incarnation",
	}
	worker = substratex509.PodIdentity{
		Namespace: "ate-system", ServiceAccountName: installdefaults.WorkerServiceAccount, PodName: "worker-xyz",
		PodUID: "worker-uid", NodeName: "node-a", NodeUID: "node-a-incarnation",
	}
	return atelet, worker
}

func networkWorkerPodFixture(worker substratex509.PodIdentity) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: worker.PodName, Namespace: worker.Namespace, UID: types.UID(worker.PodUID),
			Labels: map[string]string{workertransport.ModeLabel: workertransport.NetworkMode, workertransport.WorkerPoolLabel: "pool"},
		},
		Spec:   corev1.PodSpec{NodeName: worker.NodeName, ServiceAccountName: worker.ServiceAccountName},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "192.0.2.10"},
	}
}

// newTestWorkerNetwork builds a nodeWorkerNetwork whose directory is backed by
// a fake clientset seeded with the given pods. --network-workers is forced on
// for the duration of the test.
func newTestWorkerNetwork(t *testing.T, atelet substratex509.PodIdentity, pods ...*corev1.Pod) *nodeWorkerNetwork {
	t.Helper()
	old := *networkWorkers
	*networkWorkers = true
	t.Cleanup(func() { *networkWorkers = old })
	objects := make([]runtime.Object, 0, len(pods))
	for _, p := range pods {
		objects = append(objects, p)
	}
	client := fake.NewSimpleClientset(objects...)
	net, err := newNodeWorkerNetwork(t.Context(), client, &atelet)
	if err != nil {
		t.Fatalf("newNodeWorkerNetwork: %v", err)
	}
	if net == nil {
		t.Fatal("network worker transport was nil with --network-workers set")
	}
	return net
}

func TestAuthorizeWorkerGatesMethodsAndInventory(t *testing.T) {
	atelet, worker := testIdentities()
	net := newTestWorkerNetwork(t, atelet, networkWorkerPodFixture(worker))

	// A listed, same-node worker is accepted for every WorkerSupport method,
	// including the sandbox-runtimes seam and all three OTLP Export methods the
	// network worker exports over this connection (trace, metrics AND logs --
	// the worker's serverboot.InitLogging exports logs over the same conn, so
	// omitting LogsService/Export would reject every log export).
	for _, method := range []string{
		ateletpb.WorkerSupport_MintActorCertificate_FullMethodName,
		ateletpb.WorkerSupport_AcquireWorkerEpoch_FullMethodName,
		ateletpb.WorkerSupport_RegisterWorker_FullMethodName,
		ateletpb.WorkerSupport_SetWorkerSandboxRuntimes_FullMethodName,
		ateletpb.WorkerSupport_RequestActorSuspend_FullMethodName,
		ateletpb.WorkerSupport_GetActorEgressPolicy_FullMethodName,
		ateletpb.WorkerSupport_GetSandboxRuntimes_FullMethodName,
		"/opentelemetry.proto.collector.trace.v1.TraceService/Export",
		"/opentelemetry.proto.collector.metrics.v1.MetricsService/Export",
		"/opentelemetry.proto.collector.logs.v1.LogsService/Export",
	} {
		if err := net.authorizeWorker(context.Background(), &worker, method); err != nil {
			t.Fatalf("listed worker rejected for %s: %v", method, err)
		}
	}

	// The same worker is rejected for a lifecycle/control method a worker must
	// never call on atelet, even though its identity is valid.
	for _, method := range []string{
		ateworkerpb.Worker_RunWorkload_FullMethodName,
		ateworkerpb.Worker_CheckpointWorkload_FullMethodName,
		ateworkerpb.Worker_TerminateWorkload_FullMethodName,
		ateletpb.Atelet_Run_FullMethodName,
	} {
		if err := net.authorizeWorker(context.Background(), &worker, method); err == nil {
			t.Fatalf("worker admitted for non-worker-support method %s", method)
		}
	}

	// A peer not in the inventory (unknown pod UID) is rejected even for a
	// WorkerSupport method.
	unlisted := worker
	unlisted.PodUID = "not-in-inventory"
	unlisted.PodName = "not-in-inventory"
	if err := net.authorizeWorker(context.Background(), &unlisted, ateletpb.WorkerSupport_RegisterWorker_FullMethodName); err == nil {
		t.Fatal("a peer absent from the inventory was authorized")
	}

	// A peer on another node incarnation is rejected (the directory's node
	// check), even with an otherwise valid identity.
	otherNode := worker
	otherNode.NodeUID = "node-b-incarnation"
	if err := net.authorizeWorker(context.Background(), &otherNode, ateletpb.WorkerSupport_RegisterWorker_FullMethodName); err == nil {
		t.Fatal("a peer on another node incarnation was authorized")
	}
}

// A nil directory must fail closed: it would make the live inventory re-check
// vacuous, so authorizeWorker must deny rather than fall through to the method
// allowlist.
func TestAuthorizeWorkerNilDirectoryFailsClosed(t *testing.T) {
	net := &nodeWorkerNetwork{serviceAccount: installdefaults.WorkerServiceAccount}
	_, worker := testIdentities()
	if err := net.authorizeWorker(context.Background(), &worker, ateletpb.WorkerSupport_RegisterWorker_FullMethodName); err == nil {
		t.Fatal("a nil inventory authorizer admitted a worker-support method")
	}
}

// The default (--network-workers unset) path adds nothing: newNodeWorkerNetwork
// returns a nil network and no error, so no TCP listener or authorizer is wired.
func TestNetworkWorkersOffByDefaultAddsNothing(t *testing.T) {
	// Set the flag explicitly off (and restore it) rather than skipping if it
	// was left on: skipping would make this default-off invariant vacuous if
	// another test mutated the global flag and did not restore it.
	old := *networkWorkers
	*networkWorkers = false
	t.Cleanup(func() { *networkWorkers = old })
	atelet, _ := testIdentities()
	net, err := newNodeWorkerNetwork(t.Context(), fake.NewSimpleClientset(), &atelet)
	if err != nil {
		t.Fatalf("default path errored: %v", err)
	}
	if net != nil {
		t.Fatal("network worker transport was built with --network-workers unset")
	}
}

// The PeerPolicy admits a network worker only when its Pod runs as the service
// account named here, and the controller creates worker Pods as
// installdefaults.WorkerServiceAccount. If the two defaults drift apart every
// network worker is refused (a fail-closed outage), so the default is pinned
// to the controller's.
func TestNetworkWorkerServiceAccountDefaultMatchesTheControllersWorkerAccount(t *testing.T) {
	if got, want := *networkWorkerServiceAccount, installdefaults.WorkerServiceAccount; got != want {
		t.Fatalf("--network-worker-service-account defaults to %q, but the controller runs worker Pods as %q: a default install would refuse every network worker", got, want)
	}
}
