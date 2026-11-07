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
	corev1 "k8s.io/api/core/v1"
)

// TestDialWorkerPodWithNetworkWorkers covers a dialer with network workers
// enabled on a node that also runs colocated workers. The inventory lists one
// running network worker and one that is not running yet.
//
// The running network worker resolves to its Pod IP from the live inventory
// and reaches the mTLS ClientTLS step, which fails here only because the test
// provides no credential bundle; the Unix branch never loads a bundle. A worker
// the inventory does not list is colocated and gets the lazy Unix-socket conn,
// cached under its bare UID. A network worker that is not running fails closed
// in Resolve, with no Unix fallback.
func TestDialWorkerPodWithNetworkWorkers(t *testing.T) {
	oldBundle, oldTrust := *grpcServerCredBundle, *clientCACerts
	*grpcServerCredBundle = t.TempDir() + "/no-such-bundle.pem"
	*clientCACerts = t.TempDir() + "/no-such-trust.pem"
	t.Cleanup(func() { *grpcServerCredBundle, *clientCACerts = oldBundle, oldTrust })

	atelet, worker := testIdentities()
	pending := worker
	pending.PodUID, pending.PodName = "pending-worker-uid", "worker-pending"
	pendingPod := networkWorkerPodFixture(pending)
	pendingPod.Status.Phase = corev1.PodPending
	net := newTestWorkerNetwork(t, atelet, networkWorkerPodFixture(worker), pendingPod)

	for _, tc := range []struct {
		name    string
		uid     string
		wantErr string // substring of the dial error; empty when the dial succeeds
	}{
		{"colocated worker dials the Unix socket", "colocated-pod", ""},
		{"network worker dials over mTLS", worker.PodUID, "bundle"},
		{"network worker that is not running fails closed", pending.PodUID, "not active"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := newWorkerDialer(4)
			d.network = net
			conn, err := d.DialWorkerPod(context.Background(), tc.uid)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("DialWorkerPod(%q) error = %v, want one containing %q", tc.uid, err, tc.wantErr)
				}
				if n := d.conns.Len(); n != 0 {
					t.Fatalf("a failed dial cached %d connections", n)
				}
				return
			}
			if err != nil {
				t.Fatalf("DialWorkerPod(%q) error = %v, want the Unix-socket conn", tc.uid, err)
			}
			if got := conn.GetState(); got == connectivity.Shutdown {
				t.Fatal("colocated conn is shut down; want a live lazy conn")
			}
			if _, ok := d.conns.Get(tc.uid); !ok {
				t.Fatal("colocated dial did not cache under the bare pod UID")
			}
		})
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
