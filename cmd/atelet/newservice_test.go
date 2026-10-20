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
	"os"
	"path/filepath"
	"testing"

	"go.ate.dev/substrate/internal/nodepath"
	wspb "go.ate.dev/substrate/internal/proto/workerstagingpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// stagingProbe is a worker staging service whose one answer is recognizable.
type stagingProbe struct {
	wspb.UnimplementedWorkerStagingServer
}

func (stagingProbe) CleanupActor(context.Context, *wspb.ActivationRef) (*wspb.Empty, error) {
	return nil, status.Error(codes.AlreadyExists, "probe")
}

// TestWorkerDialerStagingClientReachesTheWorker checks the production staging
// connector: the client the dialer returns talks to the worker it dialed.
func TestWorkerDialerStagingClientReachesTheWorker(t *testing.T) {
	dir, err := os.MkdirTemp("", "worker-")
	if err != nil {
		t.Fatalf("creating socket dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	sock := filepath.Join(dir, "worker.sock")
	lis, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listening on %q: %v", sock, err)
	}
	srv := grpc.NewServer()
	wspb.RegisterWorkerStagingServer(srv, stagingProbe{})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	orig := workerSocketPath
	workerSocketPath = func(string) string { return sock }
	t.Cleanup(func() { workerSocketPath = orig })

	client, err := newWorkerDialer(1).StagingClient(t.Context(), "worker-pod")
	if err != nil {
		t.Fatalf("StagingClient: %v", err)
	}
	if _, err := client.CleanupActor(t.Context(), &wspb.ActivationRef{}); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("CleanupActor = %v, want the probe's AlreadyExists", err)
	}
}

// TestNewServiceWiresTheStagingDependencies checks that the Atelet NewService
// builds stages through its worker dialer and the node's base path. The network
// tests set these fields directly, so only this test would notice NewService
// leaving them empty.
func TestNewServiceWiresTheStagingDependencies(t *testing.T) {
	dialer := newWorkerDialer(1)
	s := NewService(t.Context(), dialer, nil, nil, nil, "", nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, "")

	if s.staging != stagingConnector(dialer) {
		t.Errorf("staging = %v, want the worker dialer", s.staging)
	}
	if s.nodeBase != nodepath.BasePath {
		t.Errorf("nodeBase = %q, want %q", s.nodeBase, nodepath.BasePath)
	}
}
