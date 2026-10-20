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
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"

	ateapi "go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/ateletdial"
	"go.ate.dev/substrate/internal/hardware"
	"go.ate.dev/substrate/internal/installdefaults"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/sandboxruntimes"
	"go.ate.dev/substrate/internal/workertransport"
)

// recordingWorkers is the slice of the control plane's WorkerService that
// workerSupportServer forwards registration and crash reports to. It records
// the worker each call names.
type recordingWorkers struct {
	ateapi.WorkerServiceClient
	mu    sync.Mutex
	calls []string
	names []string
}

func (w *recordingWorkers) record(call string, worker *ateapi.ObjectRef) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.calls = append(w.calls, call)
	w.names = append(w.names, worker.GetName())
}

func (w *recordingWorkers) snapshot() ([]string, []string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.calls...), append([]string(nil), w.names...)
}

func (w *recordingWorkers) AcquireWorkerEpoch(_ context.Context, in *ateapi.AcquireWorkerEpochRequest, _ ...grpc.CallOption) (*ateapi.AcquireWorkerEpochResponse, error) {
	w.record("AcquireWorkerEpoch", in.GetWorker())
	return &ateapi.AcquireWorkerEpochResponse{Epoch: 7}, nil
}

func (w *recordingWorkers) RegisterWorker(_ context.Context, in *ateapi.RegisterWorkerRequest, _ ...grpc.CallOption) (*ateapi.RegisterWorkerResponse, error) {
	w.record("RegisterWorker", in.GetWorker())
	return &ateapi.RegisterWorkerResponse{}, nil
}

func (w *recordingWorkers) SetWorkerSandboxRuntimes(_ context.Context, in *ateapi.SetWorkerSandboxRuntimesRequest, _ ...grpc.CallOption) (*ateapi.SetWorkerSandboxRuntimesResponse, error) {
	w.record("SetWorkerSandboxRuntimes", in.GetWorker())
	return &ateapi.SetWorkerSandboxRuntimesResponse{}, nil
}

func (w *recordingWorkers) ReportActorCrashed(_ context.Context, in *ateapi.ReportActorCrashedRequest, _ ...grpc.CallOption) (*ateapi.ReportActorCrashedResponse, error) {
	w.record("ReportActorCrashed", in.GetWorker())
	return &ateapi.ReportActorCrashedResponse{}, nil
}

// TestNetworkWorkerRegistersAndReportsCrashOverListener drives atelet's
// network WorkerSupport listener with the same RPCs a network worker's
// register client issues, built as atelet builds the listener: ServerTLS with
// the network peer policy, the per-RPC authorizeWorker interceptor over a
// fake Kubernetes worker inventory, and the real workerSupportServer
// forwarding to a fake control plane. It registers (AcquireWorkerEpoch ->
// RegisterWorker -> GetSandboxRuntimes -> SetWorkerSandboxRuntimes) and then
// reports a crashed actor; each forwarded call names the worker from its
// certificate.
func TestNetworkWorkerRegistersAndReportsCrashOverListener(t *testing.T) {
	atelet, worker := testIdentities()
	atelet.ServiceAccountUID, worker.ServiceAccountUID = "atelet-sa-uid", "worker-sa-uid"
	network := newTestWorkerNetwork(t, atelet, networkWorkerPodFixture(worker))
	ca := newTestPodCA(t)
	trust := ca.writeTrustBundle(t)
	_, ateletBundle := ca.issue(t, &atelet)
	_, workerBundle := ca.issue(t, &worker)

	serverTLS, err := workertransport.ServerTLS(ateletBundle, trust, network.peerPolicy())
	if err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	runtimesDir := t.TempDir()
	runtimesPath := func(podUID string) string { return filepath.Join(runtimesDir, podUID+".json") }
	if _, err := sandboxruntimes.Write(runtimesPath(worker.PodUID), &sandboxruntimes.File{SandboxClasses: []sandboxruntimes.Class{{
		SandboxClass:   "gvisor",
		DefaultRuntime: sandboxruntimes.Runtime{Version: sandboxruntimes.Attributes{"architecture": "amd64"}},
	}}}); err != nil {
		t.Fatal(err)
	}
	workers := &recordingWorkers{}
	srv := grpc.NewServer(
		grpc.Creds(workertransport.RefreshingCredentials(serverTLS)),
		grpc.UnaryInterceptor(workertransport.UnaryAuthorizer(network.authorizeWorker)),
		grpc.StreamInterceptor(workertransport.StreamAuthorizer(network.authorizeWorker)),
	)
	ateletpb.RegisterWorkerSupportServer(srv, &workerSupportServer{workers: workers, runtimesPath: runtimesPath})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	workerTLS, err := ateletdial.TLSConfig(workerBundle, trust, installdefaults.SPIFFEID(atelet.Namespace, atelet.ServiceAccountName))
	if err != nil {
		t.Fatal(err)
	}
	conn, err := workertransport.Dial(lis.Addr().String(), workerTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := ateletpb.NewWorkerSupportClient(conn)

	epochResp, err := client.AcquireWorkerEpoch(ctx, &ateletpb.AcquireWorkerEpochRequest{})
	if err != nil {
		t.Fatalf("AcquireWorkerEpoch over the network listener: %v", err)
	}
	if _, err := client.RegisterWorker(ctx, &ateletpb.RegisterWorkerRequest{
		Epoch:    epochResp.GetEpoch(),
		Ips:      []string{"192.0.2.10"},
		Capacity: &ateletpb.WorkerResources{Actors: 3},
	}); err != nil {
		t.Fatalf("RegisterWorker over the network listener: %v", err)
	}
	// GetSandboxRuntimes is served locally by atelet from the sandbox runtimes
	// file written above; it is never forwarded to the control plane.
	runtimes, err := client.GetSandboxRuntimes(ctx, &ateletpb.GetSandboxRuntimesRequest{})
	if err != nil {
		t.Fatalf("GetSandboxRuntimes over the network listener: %v", err)
	}
	// atelet reads the runtimes file named by the caller's certificate. A real
	// register client finds no class to report from an empty answer and never
	// calls SetWorkerSandboxRuntimes, so pin the answer here.
	classes := runtimes.GetSandboxClasses()
	if len(classes) != 1 || classes[0].GetSandboxClass() != "gvisor" {
		t.Fatalf("GetSandboxRuntimes classes = %v, want the gvisor class from the worker's own runtimes file", classes)
	}
	if attrs := classes[0].GetDefaultRuntime().GetAttributes(); len(attrs) != 1 || attrs[0].GetKey() != "architecture" || attrs[0].GetValue() != "amd64" {
		t.Fatalf("GetSandboxRuntimes default runtime = %v, want architecture=amd64", attrs)
	}
	if got := epochResp.GetEpoch(); got != 7 {
		t.Fatalf("AcquireWorkerEpoch epoch = %d, want the control plane's 7", got)
	}
	if _, err := client.SetWorkerSandboxRuntimes(ctx, &ateletpb.SetWorkerSandboxRuntimesRequest{
		Epoch:          epochResp.GetEpoch(),
		SandboxClasses: toWorkerSandboxClasses(runtimes.GetSandboxClasses()),
	}); err != nil {
		t.Fatalf("SetWorkerSandboxRuntimes over the network listener: %v", err)
	}

	actor := resources.ActorAttribution{Ref: resources.ActorRef{Atespace: "team", Name: "counter"}, UID: uuid.NewString()}
	if _, err := client.ReportActorCrashed(ctx, &ateletpb.ReportActorCrashedRequest{
		ActorAtespace: actor.Ref.Atespace,
		ActorName:     actor.Ref.Name,
		ActorUid:      actor.UID,
		Message:       "sandbox exited",
	}); err != nil {
		t.Fatalf("ReportActorCrashed over the network listener: %v", err)
	}

	want := []string{"AcquireWorkerEpoch", "RegisterWorker", "SetWorkerSandboxRuntimes"}
	calls, names := workers.snapshot()
	if len(calls) != len(want)+1 || calls[len(calls)-1] != "ReportActorCrashed" {
		t.Fatalf("control plane calls = %v, want %v then ReportActorCrashed", calls, want)
	}
	for i, call := range want {
		if calls[i] != call {
			t.Fatalf("control plane calls = %v, want %v first", calls, want)
		}
	}
	for i, name := range names {
		if name != worker.PodUID {
			t.Errorf("%s named worker %q, want the certificate's pod UID %q", calls[i], name, worker.PodUID)
		}
	}
}

// toWorkerSandboxClasses converts the sandbox runtimes atelet served over
// GetSandboxRuntimes into the shape a worker reports back over
// SetWorkerSandboxRuntimes, the way a real register client does.
func toWorkerSandboxClasses(classes []*ateletpb.SandboxRuntimesClass) []*ateletpb.WorkerSandboxClass {
	out := make([]*ateletpb.WorkerSandboxClass, 0, len(classes))
	for _, c := range classes {
		out = append(out, &ateletpb.WorkerSandboxClass{
			SandboxClass: c.GetSandboxClass(),
			DefaultRuntime: &ateletpb.WorkerSandboxRuntime{Version: &ateletpb.VersionedSandboxCompat{
				SchemaVersion: hardware.SchemaVersionV1,
				Attributes:    c.GetDefaultRuntime().GetAttributes(),
			}},
		})
	}
	return out
}
