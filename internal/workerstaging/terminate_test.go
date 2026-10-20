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

package workerstaging

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	pb "go.ate.dev/substrate/internal/proto/workerstagingpb"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestTerminateBeforeActivationDoesNotInvokeRuntime(t *testing.T) {
	guest := t.TempDir()
	ref := &pb.ActivationRef{ActorUid: uuid.NewString(), WorkerUid: uuid.NewString(), Generation: uuid.NewString()}
	rpc, lifecycle, runtime := stagingConnection(t, guest, ref.WorkerUid)
	req := &ateworkerpb.TerminateWorkloadRequest{Atespace: "team", ActorName: "counter",
		ActorUid: ref.ActorUid}
	if _, err := lifecycle.TerminateWorkload(t.Context(), req); status.Code(err) != codes.NotFound {
		t.Fatalf("unstarted termination must use atelet's NotFound completion path: %v", err)
	}
	for range 2 {
		if _, err := rpc.CleanupActor(t.Context(), ref); err != nil {
			t.Fatalf("unstarted or already-cleaned actor cleanup: %v", err)
		}
	}
	runtime.active.Store(true)
	if _, err := lifecycle.TerminateWorkload(t.Context(), req); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("active actor without its receipt must not be treated as absent: %v", err)
	}
	runtime.active.Store(false)
	fixtureFile(t, guest, filepath.Join("actors", ref.ActorUid, "unknown-state"), "preserve")
	if _, err := lifecycle.TerminateWorkload(t.Context(), req); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("unmarked actor state must not be treated as absent: %v", err)
	}
	if _, err := rpc.CleanupActor(t.Context(), ref); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("unmarked actor state must not be removed: %v", err)
	}
}

func TestUnstartedCleanupReclaimsOnlyItsPendingGeneration(t *testing.T) {
	node, guest := t.TempDir(), t.TempDir()
	ref := &pb.ActivationRef{ActorUid: uuid.NewString(), WorkerUid: uuid.NewString(), Generation: uuid.NewString()}
	assets := fixtureNode(t, node, ref.ActorUid)
	rpc, _, _ := stagingConnection(t, guest, ref.WorkerUid)
	if err := NewClient(node, rpc).Stage(t.Context(), ref, resources.ActorRef{Atespace: "team", Name: "counter"}, assets); err != nil {
		t.Fatal(err)
	}
	manifest, err := readActivation(filepath.Join(guest, "actors", ref.ActorUid, activationFile))
	if err != nil {
		t.Fatal(err)
	}
	wrong := proto.Clone(ref).(*pb.ActivationRef)
	wrong.Generation = uuid.NewString()
	if _, err := rpc.CleanupActor(t.Context(), wrong); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("wrong generation removed committed state: %v", err)
	}
	preserved, err := readActivation(filepath.Join(guest, "actors", ref.ActorUid, activationFile))
	if err != nil || !proto.Equal(preserved, manifest) {
		t.Fatalf("rejected cleanup changed the committed activation: %v", err)
	}
	if _, err := rpc.CleanupActor(t.Context(), ref); err != nil {
		t.Fatal(err)
	}
	var pending []*pb.ActivationRef
	for range 2 {
		next := proto.Clone(manifest).(*pb.Activation)
		next.Ref.Generation = uuid.NewString()
		for _, artifact := range next.Artifacts {
			if artifact.Kind == pb.ArtifactKind_ARTIFACT_KIND_ACTOR_STATE {
				artifact.Id = next.Ref.Generation
			}
		}
		if _, err := rpc.PrepareActivation(t.Context(), next); err != nil {
			t.Fatal(err)
		}
		pending = append(pending, next.Ref)
	}
	if _, err := rpc.CleanupActor(t.Context(), pending[0]); err != nil {
		t.Fatal(err)
	}
	for i, ref := range pending {
		_, err := os.Stat(filepath.Join(guest, "network-staging", ref.ActorUid, ref.Generation))
		if i == 0 && !os.IsNotExist(err) || i == 1 && err != nil {
			t.Fatalf("pending generation cleanup was not exact: %v", err)
		}
	}
}
