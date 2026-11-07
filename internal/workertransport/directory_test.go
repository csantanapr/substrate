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

package workertransport

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
)

func TestDirectoryUsesPodUIDAndRejectsDeletedOrMisplacedWorkers(t *testing.T) {
	index := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{podUIDIndex: func(obj any) ([]string, error) {
		return []string{string(obj.(*corev1.Pod).UID)}, nil
	}})
	local := podIdentity("atelet", "helper")
	directory := &Directory{index, local, "ate-worker", 8086}
	identity := podIdentity("ate-worker", "worker-uid")
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: identity.PodName, Namespace: identity.Namespace, UID: types.UID(identity.PodUID),
			Labels: map[string]string{ModeLabel: NetworkMode, WorkerPoolLabel: "pool"}},
		Spec:   corev1.PodSpec{NodeName: local.NodeName, ServiceAccountName: "ate-worker"},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "192.0.2.10"},
	}
	if err := index.Add(pod); err != nil {
		t.Fatal(err)
	}
	if !directory.Has(identity.PodUID) {
		t.Fatal("a listed network worker is not in the directory")
	}
	address, ns, err := directory.Resolve(identity.PodUID)
	if err != nil || address != "192.0.2.10:8086" || ns != identity.Namespace {
		t.Fatalf("resolve: %q %q %v", address, ns, err)
	}
	if err := directory.Authorize(&identity); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*corev1.Pod){
		func(p *corev1.Pod) { p.Spec.NodeName = "another-node" },
		func(p *corev1.Pod) { p.Spec.ServiceAccountName = "atelet" },
		func(p *corev1.Pod) { p.Labels[ModeLabel] = "unix" },
		func(p *corev1.Pod) { now := metav1.Now(); p.DeletionTimestamp = &now },
		func(p *corev1.Pod) { p.UID = "replacement-worker" },
	} {
		changed := pod.DeepCopy()
		mutate(changed)
		if err := index.Update(changed); err != nil {
			t.Fatal(err)
		}
		if err := directory.Authorize(&identity); err == nil {
			t.Fatal("accepted removed, replaced or misplaced worker")
		}
		if err := index.Update(pod); err != nil {
			t.Fatal(err)
		}
	}
	if err := index.Delete(pod); err != nil {
		t.Fatal(err)
	}
	if _, _, err := directory.Resolve(identity.PodUID); err == nil {
		t.Fatal("deleted worker still resolves")
	}
	if directory.Has(identity.PodUID) {
		t.Fatal("deleted worker is still in the directory")
	}
	if len(directory.UIDs()) != 0 {
		t.Fatal("stats inventory retained deleted worker")
	}
}

// TestColocatedWorkerIsNeverInTheNetworkDirectory is the light default-unchanged
// pin: the network transport must not capture a default (colocated)
// worker. A pod carrying neither the network-transport label nor a worker-pool
// label -- what an unannotated colocated worker looks like -- is not usable and
// never resolves, so nothing in this package ever touches the colocated path.
// (The informer label selector is a coarse API-server pre-filter; the usable
// predicate exercised here is the authoritative test, and it is strictly at
// least as strict as the selector, so this pins the real exclusion.)
func TestColocatedWorkerIsNeverInTheNetworkDirectory(t *testing.T) {
	index := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{podUIDIndex: func(obj any) ([]string, error) {
		return []string{string(obj.(*corev1.Pod).UID)}, nil
	}})
	local := podIdentity("atelet", "helper")
	directory := &Directory{index, local, "ate-worker", 8086}
	identity := podIdentity("ate-worker", "colocated-worker")
	colocated := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: identity.PodName, Namespace: identity.Namespace, UID: types.UID(identity.PodUID)},
		Spec:       corev1.PodSpec{NodeName: local.NodeName, ServiceAccountName: "ate-worker"},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "192.0.2.20"},
	}
	if err := index.Add(colocated); err != nil {
		t.Fatal(err)
	}
	if directory.Has(identity.PodUID) {
		t.Fatal("an unannotated colocated worker is in the network directory")
	}
	if _, _, err := directory.Resolve(identity.PodUID); err == nil {
		t.Fatal("an unannotated colocated worker resolved through the network directory")
	}
	if err := directory.Authorize(&identity); err == nil {
		t.Fatal("an unannotated colocated worker was authorized as a network worker")
	}
	if len(directory.UIDs()) != 0 {
		t.Fatal("an unannotated colocated worker appeared in the network inventory")
	}
	// Adding only the network-mode label, still without a worker-pool label, is
	// also excluded by usable: a non-empty worker-pool value is required. (The
	// coarse informer selector only requires the label to exist, so usable is
	// the stricter, authoritative guard here.)
	labeledHalfway := colocated.DeepCopy()
	labeledHalfway.Labels = map[string]string{ModeLabel: NetworkMode}
	if err := index.Update(labeledHalfway); err != nil {
		t.Fatal(err)
	}
	if _, _, err := directory.Resolve(identity.PodUID); err == nil {
		t.Fatal("a half-labeled worker resolved through the network directory")
	}
	// It is still labeled a network worker, so atelet must not treat it as
	// colocated: Resolve's error is the only path it gets.
	if !directory.Has(identity.PodUID) {
		t.Fatal("a pod labeled as a network worker is not in the directory")
	}
	// A pod carrying the network-mode label and an EMPTY worker-pool label is
	// what the coarse informer selector (which only requires the label to
	// exist) would admit into the index, yet usable rejects it for the empty
	// value. This pins that the selector/predicate difference is fail-closed.
	emptyPool := colocated.DeepCopy()
	emptyPool.Labels = map[string]string{ModeLabel: NetworkMode, WorkerPoolLabel: ""}
	if err := index.Update(emptyPool); err != nil {
		t.Fatal(err)
	}
	if _, _, err := directory.Resolve(identity.PodUID); err == nil {
		t.Fatal("a worker with an empty worker-pool label resolved through the network directory")
	}
	if err := directory.Authorize(&identity); err == nil {
		t.Fatal("a worker with an empty worker-pool label was authorized")
	}
}
