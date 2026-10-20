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
	"os"
	"path/filepath"
	"testing"

	"go.ate.dev/substrate/internal/nodepath"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/sandboxruntimes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// writeRuntimesFileFor writes a sandbox runtimes file for podUID under a test
// directory, returning a runtimesPath func rooted there (mirroring
// nodepath.WorkerSandboxRuntimesPath's layout: <dir>/<podUID>/sandbox-runtimes.json).
func writeRuntimesFileFor(t *testing.T, podUID string, file *sandboxruntimes.File) func(string) string {
	t.Helper()
	dir := t.TempDir()
	runtimesPath := func(uid string) string {
		return filepath.Join(dir, uid, nodepath.SandboxRuntimesFileName)
	}
	if file != nil {
		if err := os.MkdirAll(filepath.Join(dir, podUID), 0o700); err != nil {
			t.Fatal(err)
		}
		if _, err := sandboxruntimes.Write(runtimesPath(podUID), file); err != nil {
			t.Fatal(err)
		}
	}
	return runtimesPath
}

// TestGetSandboxRuntimesServesTheAuthenticatedWorkersFile proves the network
// seam returns the runtimes atelet keeps for the CALLING worker (identified by
// its mTLS certificate, never by the request), converted from the same on-node
// file a colocated worker reads. A worker with no file yet gets no sandbox
// classes (not an error), so its register reconciler waits and retries
// exactly as it would for an absent file.
func TestGetSandboxRuntimesServesTheAuthenticatedWorkersFile(t *testing.T) {
	want := &sandboxruntimes.File{SandboxClasses: []sandboxruntimes.Class{{
		SandboxClass: "gvisor",
		DefaultRuntime: sandboxruntimes.Runtime{Version: sandboxruntimes.Attributes{
			"architecture": "amd64", "sandbox_runtime_hash": "sha256:aaa", "cpu_features": "0x1",
		}},
		RestorableRuntimes: []sandboxruntimes.Runtime{
			{Version: sandboxruntimes.Attributes{"architecture": "amd64", "sandbox_runtime_hash": "sha256:bbb", "cpu_features": "0x3"}},
		},
	}}}
	runtimesPath := writeRuntimesFileFor(t, "worker-uid", want)
	s := &workerSupportServer{runtimesPath: runtimesPath}

	resp, err := s.GetSandboxRuntimes(workerContext(t, "worker-uid"), &ateletpb.GetSandboxRuntimesRequest{})
	if err != nil {
		t.Fatalf("GetSandboxRuntimes: %v", err)
	}
	if n := len(resp.GetSandboxClasses()); n != 1 {
		t.Fatalf("sandbox classes = %d; want 1", n)
	}
	gotClass, wantClass := resp.GetSandboxClasses()[0], want.SandboxClasses[0]
	if gotClass.GetSandboxClass() != wantClass.SandboxClass {
		t.Fatalf("sandbox class = %q; want %q", gotClass.GetSandboxClass(), wantClass.SandboxClass)
	}
	gotDefault := attrsFromWire(gotClass.GetDefaultRuntime())
	if !equalAttrs(gotDefault, wantClass.DefaultRuntime.Version) {
		t.Fatalf("default runtime = %v; want %v", gotDefault, wantClass.DefaultRuntime.Version)
	}
	if n := len(gotClass.GetRestorableRuntimes()); n != 1 {
		t.Fatalf("restorable runtimes = %d; want 1", n)
	}
	gotRestorable := attrsFromWire(gotClass.GetRestorableRuntimes()[0])
	if !equalAttrs(gotRestorable, wantClass.RestorableRuntimes[0].Version) {
		t.Fatalf("restorable runtime = %v; want %v", gotRestorable, wantClass.RestorableRuntimes[0].Version)
	}

	// A different worker UID (no file written for it) gets no sandbox
	// classes, not an error: the worker treats that as "not ready yet".
	empty, err := s.GetSandboxRuntimes(workerContext(t, "other-worker-uid"), &ateletpb.GetSandboxRuntimesRequest{})
	if err != nil {
		t.Fatalf("GetSandboxRuntimes for a worker with no file errored: %v", err)
	}
	if len(empty.GetSandboxClasses()) != 0 {
		t.Fatalf("a worker with no runtimes file got non-empty runtimes: %v", empty)
	}
}

// TestGetSandboxRuntimesUnavailableWithoutNetworkSeam proves the handler fails
// closed on any listener that did not set runtimesPath -- i.e. the default Unix
// WorkerSupport listener. A colocated worker reads the file itself and never
// calls this, so serving it there would be a surface with no caller; it returns
// Unimplemented instead.
func TestGetSandboxRuntimesUnavailableWithoutNetworkSeam(t *testing.T) {
	s := &workerSupportServer{} // runtimesPath nil, as on the Unix listener
	_, err := s.GetSandboxRuntimes(workerContext(t, "worker-uid"), &ateletpb.GetSandboxRuntimesRequest{})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("GetSandboxRuntimes without the network seam = %v; want Unimplemented", err)
	}
}

// TestGetSandboxRuntimesRequiresAuthenticatedWorker proves identity comes from
// the certificate: a call with no peer certificate is refused before any file
// is read, so a caller cannot ask for another worker's runtimes (there is no
// pod-UID field to forge, and an unauthenticated caller is refused outright).
func TestGetSandboxRuntimesRequiresAuthenticatedWorker(t *testing.T) {
	runtimesPath := writeRuntimesFileFor(t, "worker-uid", &sandboxruntimes.File{SandboxClasses: []sandboxruntimes.Class{{
		SandboxClass:   "gvisor",
		DefaultRuntime: sandboxruntimes.Runtime{Version: sandboxruntimes.Attributes{"architecture": "amd64"}},
	}}})
	s := &workerSupportServer{runtimesPath: runtimesPath}
	if _, err := s.GetSandboxRuntimes(context.Background(), &ateletpb.GetSandboxRuntimesRequest{}); err == nil {
		t.Fatal("GetSandboxRuntimes served a caller with no peer certificate")
	}
}

func attrsFromWire(in *ateletpb.SandboxRuntimeAttributes) map[string]string {
	out := map[string]string{}
	for _, e := range in.GetAttributes() {
		out[e.GetKey()] = e.GetValue()
	}
	return out
}

func equalAttrs(got map[string]string, want sandboxruntimes.Attributes) bool {
	if len(got) != len(want) {
		return false
	}
	for k, v := range want {
		if got[k] != v {
			return false
		}
	}
	return true
}
