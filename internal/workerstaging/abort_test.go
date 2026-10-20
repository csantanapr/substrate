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
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	pb "go.ate.dev/substrate/internal/proto/workerstagingpb"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type interruptedCommit struct {
	pb.WorkerStagingClient
	cancel    context.CancelFunc
	committed bool
}

func (c interruptedCommit) CommitActivation(ctx context.Context, ref *pb.ActivationRef, options ...grpc.CallOption) (*pb.Empty, error) {
	if c.committed {
		if _, err := c.WorkerStagingClient.CommitActivation(ctx, ref, options...); err != nil {
			return nil, err
		}
	}
	c.cancel()
	return nil, status.Error(codes.Canceled, "caller cancelled before receiving commit")
}

func TestFailedStageReclaimsPendingWithCancelledCaller(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(map[bool]string{false: "before commit", true: "lost commit response"}[committed], func(t *testing.T) {
			node, guest := t.TempDir(), t.TempDir()
			ref := &pb.ActivationRef{ActorUid: uuid.NewString(), WorkerUid: uuid.NewString(), Generation: uuid.NewString()}
			assets := fixtureNode(t, node, ref.GetActorUid())
			rpc, _, _ := stagingConnection(t, guest, ref.GetWorkerUid())
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			client := NewClient(node, interruptedCommit{rpc, cancel, committed})
			if err := client.Stage(ctx, ref, resources.ActorRef{Atespace: "team", Name: "counter"}, assets); err == nil {
				t.Fatal("interrupted commit reported success")
			}
			if _, err := os.Stat(filepath.Join(guest, "network-staging", ref.ActorUid, ref.Generation)); !os.IsNotExist(err) {
				t.Fatalf("abandoned generation retained: %v", err)
			}
			_, err := os.Stat(filepath.Join(guest, "actors", ref.ActorUid, activationFile))
			if committed && err != nil || !committed && !os.IsNotExist(err) {
				t.Fatalf("abort altered committed actor state: %v", err)
			}
			// network-artifacts holds receipts only for node-wide shared
			// immutables, which after upstream #2699 is the image layer alone
			// (one receipt). A RUNTIME asset is per-actor and staged through the
			// pending tree, not this shared cache, so an abort must leave the
			// one image-layer receipt untouched and create no runtime receipt.
			receipts, err := filepath.Glob(filepath.Join(guest, "network-artifacts", "*.json"))
			if err != nil || len(receipts) != 1 {
				t.Fatalf("abort altered shared artifacts: %v %v", receipts, err)
			}
		})
	}
}

func TestAbortIsExactGenerationAndPreservesCommittedActor(t *testing.T) {
	node, guest := t.TempDir(), t.TempDir()
	ref := &pb.ActivationRef{ActorUid: uuid.NewString(), WorkerUid: uuid.NewString(), Generation: uuid.NewString()}
	assets := fixtureNode(t, node, ref.ActorUid)
	rpc, _, runtime := stagingConnection(t, guest, ref.WorkerUid)
	if err := NewClient(node, rpc).Stage(t.Context(), ref, resources.ActorRef{Atespace: "team", Name: "counter"}, assets); err != nil {
		t.Fatal(err)
	}
	committedPath := filepath.Join(guest, "actors", ref.ActorUid, activationFile)
	manifest, err := readActivation(committedPath)
	if err != nil {
		t.Fatal(err)
	}
	refs := []*pb.ActivationRef{}
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
		fixtureFile(t, guest, filepath.Join("network-staging", ref.ActorUid, next.Ref.Generation, "actor-state", snapshotsTreeName(), "restore-state", "image"), "pending")
		refs = append(refs, next.Ref)
	}
	wrong := proto.Clone(refs[0]).(*pb.ActivationRef)
	wrong.WorkerUid = uuid.NewString()
	if _, err := rpc.AbortActivation(t.Context(), wrong); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("wrong worker abort accepted: %v", err)
	}
	if _, err := rpc.AbortActivation(t.Context(), refs[0]); err != nil {
		t.Fatal(err)
	}
	for i, pending := range refs {
		_, err := os.Stat(filepath.Join(guest, "network-staging", pending.ActorUid, pending.Generation, "actor-state"))
		if i == 0 && !os.IsNotExist(err) || i == 1 && err != nil {
			t.Fatalf("abort did not preserve exact-generation ownership: %v", err)
		}
	}
	if got, err := readActivation(committedPath); err != nil || !proto.Equal(got, manifest) {
		t.Fatalf("committed actor changed: %v", err)
	}
	runtime.active.Store(true)
	if _, err := rpc.AbortActivation(t.Context(), refs[1]); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("active actor abort accepted: %v", err)
	}
}
