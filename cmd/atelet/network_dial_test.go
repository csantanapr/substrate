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
	"strings"
	"testing"

	"google.golang.org/grpc/connectivity"
)

// TestDialWorkerPodNetworkResolvesThroughInventory proves that with a network
// transport set the dialer resolves the worker Pod's endpoint from the live
// Kubernetes inventory (not from a request) and takes the mTLS branch -- never
// the Unix-socket fallback. A listed worker resolves to its Pod IP and then
// reaches the mTLS ClientTLS step (which fails here only because the test does
// not provide a real credential bundle, proving the network branch was taken
// rather than the plaintext Unix path, which would have produced a lazy conn
// and no bundle error).
func TestDialWorkerPodNetworkResolvesThroughInventory(t *testing.T) {
	atelet, worker := testIdentities()
	net := newTestWorkerNetwork(t, atelet, networkWorkerPodFixture(worker))
	// Point the credential material at a path that does not exist so ClientTLS
	// fails deterministically: reaching that failure proves the dial took the
	// mTLS network branch (n.dial -> ClientTLS), because the Unix branch never
	// loads a bundle.
	oldBundle, oldTrust := *grpcServerCredBundle, *clientCACerts
	*grpcServerCredBundle = t.TempDir() + "/no-such-bundle.pem"
	*clientCACerts = t.TempDir() + "/no-such-trust.pem"
	t.Cleanup(func() { *grpcServerCredBundle, *clientCACerts = oldBundle, oldTrust })

	d := newWorkerDialer(4)
	d.network = net

	_, err := d.DialWorkerPod(context.Background(), worker.PodUID)
	if err == nil {
		t.Fatal("network dial of a listed worker succeeded without a credential bundle; it must have taken the mTLS branch and failed loading the bundle")
	}
	if !strings.Contains(err.Error(), "bundle") {
		t.Fatalf("network dial error = %v; want a credential-bundle load failure proving the mTLS branch was taken (not the Unix fallback)", err)
	}
}

// TestDialWorkerPodNetworkFailsClosedForAbsentWorker proves the network dial
// fails closed: a worker absent from the live inventory (or a colocated worker
// that never opted into network mode) cannot be dialed, and there is no
// plaintext or Unix fallback. The error comes from Resolve, before any dial.
func TestDialWorkerPodNetworkFailsClosedForAbsentWorker(t *testing.T) {
	atelet, worker := testIdentities()
	// Seed the inventory with the one real worker, then ask for a different UID.
	net := newTestWorkerNetwork(t, atelet, networkWorkerPodFixture(worker))

	d := newWorkerDialer(4)
	d.network = net

	if _, err := d.DialWorkerPod(context.Background(), "not-in-inventory"); err == nil {
		t.Fatal("network dial of a worker absent from the inventory succeeded; it must fail closed")
	}
	// A conn must NOT have been cached under the absent UID: a failed resolve
	// adds nothing.
	if _, ok := d.conns.Get("not-in-inventory"); ok {
		t.Fatal("a failed network resolve cached a connection")
	}
}

// TestDialWorkerPodColocatedUnchanged pins the default (no network transport)
// path unchanged: with network nil the dialer uses the Unix socket and
// returns a (lazy) live connection, exactly as upstream. This is the
// default-unchanged guard for the dial branch: a colocated worker takes none of
// the network path.
func TestDialWorkerPodColocatedUnchanged(t *testing.T) {
	d := newWorkerDialer(4)
	if d.network != nil {
		t.Fatal("a freshly built dialer must have no network transport by default")
	}
	conn, err := d.DialWorkerPod(context.Background(), "colocated-pod")
	if err != nil {
		t.Fatalf("colocated dial error = %v; the Unix path must be unchanged", err)
	}
	if got := conn.GetState(); got == connectivity.Shutdown {
		t.Fatalf("colocated conn is shut down; want a live lazy conn")
	}
	// Cached by bare UID (no "/address" suffix), the upstream key.
	if _, ok := d.conns.Get("colocated-pod"); !ok {
		t.Fatal("colocated dial did not cache under the bare pod UID")
	}
}
