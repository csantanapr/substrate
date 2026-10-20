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
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "go.ate.dev/substrate/internal/proto/workerstagingpb"
)

func actorStateArtifact() *pb.Artifact {
	return &pb.Artifact{Kind: pb.ArtifactKind_ARTIFACT_KIND_ACTOR_STATE, Id: uuid.NewString()}
}

// TestActorStateRoundTripsBothTrees packs an actor's two trees and extracts
// them again: the listed roots of each tree arrive under that tree, and
// unlisted directories of either tree stay behind.
func TestActorStateRoundTripsBothTrees(t *testing.T) {
	node, actorUID := t.TempDir(), uuid.NewString()
	actor := filepath.Join(actorTreeName(), actorUID)
	snapshots := filepath.Join(snapshotsTreeName(), actorUID)
	fixtureFile(t, node, filepath.Join(actor, "bundles", "main", "config.json"), "bundle")
	fixtureFile(t, node, filepath.Join(actor, "pidfiles", "main.pid"), "1")
	fixtureFile(t, node, filepath.Join(actor, "sandbox-assets", "gvisor", "runsc"), "not actor state")
	fixtureFile(t, node, filepath.Join(snapshots, "restore-state", "image"), "restore bytes")
	fixtureFile(t, node, filepath.Join(snapshots, "checkpoint-state", "image"), "checkpoint bytes")
	fixtureFile(t, node, filepath.Join(snapshots, "local-checkpoint", "old", "image"), "node custody")

	var body bytes.Buffer
	if err := packArtifact(t.Context(), node, actorUID, actorStateArtifact(), &body); err != nil {
		t.Fatalf("pack: %v", err)
	}
	tree := t.TempDir()
	if err := extractArtifact(t.Context(), actorStateArtifact(), tree, bytes.NewReader(body.Bytes())); err != nil {
		t.Fatalf("extract: %v", err)
	}
	for name, want := range map[string]string{
		filepath.Join(actorTreeName(), "bundles", "main", "config.json"): "bundle",
		filepath.Join(actorTreeName(), "pidfiles", "main.pid"):           "1",
		filepath.Join(snapshotsTreeName(), "restore-state", "image"):     "restore bytes",
		filepath.Join(snapshotsTreeName(), "checkpoint-state", "image"):  "checkpoint bytes",
	} {
		got, err := os.ReadFile(filepath.Join(tree, name))
		if err != nil || string(got) != want {
			t.Errorf("%s = %q, %v; want %q", name, got, err, want)
		}
	}
	for _, name := range []string{
		filepath.Join(actorTreeName(), "sandbox-assets"),
		filepath.Join(snapshotsTreeName(), "local-checkpoint"),
	} {
		if _, err := os.Lstat(filepath.Join(tree, name)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("unlisted %s was transferred (%v)", name, err)
		}
	}
}

// TestActorStateWithoutSnapshotsTree checks that an actor with no snapshots
// tree yet (nothing staged for restore or checkpoint) packs an empty snapshots
// tree entry, which extracts as an empty directory (and is not installed; see
// TestCommitEmptySnapshotsTreeLeavesNothing), while a missing actor tree is an
// error.
func TestActorStateWithoutSnapshotsTree(t *testing.T) {
	node, actorUID := t.TempDir(), uuid.NewString()
	fixtureFile(t, node, filepath.Join(actorTreeName(), actorUID, "bundles", "main", "config.json"), "bundle")
	var body bytes.Buffer
	if err := packArtifact(t.Context(), node, actorUID, actorStateArtifact(), &body); err != nil {
		t.Fatalf("pack without a snapshots tree: %v", err)
	}
	tree := t.TempDir()
	if err := extractArtifact(t.Context(), actorStateArtifact(), tree, bytes.NewReader(body.Bytes())); err != nil {
		t.Fatalf("extract: %v", err)
	}
	entries, err := os.ReadDir(filepath.Join(tree, snapshotsTreeName()))
	if err != nil || len(entries) != 0 {
		t.Errorf("an absent snapshots tree did not arrive as an empty tree: %v, %v", entries, err)
	}

	empty := t.TempDir()
	fixtureFile(t, empty, filepath.Join(snapshotsTreeName(), actorUID, "restore-state", "image"), "restore bytes")
	if err := packArtifact(t.Context(), empty, actorUID, actorStateArtifact(), &bytes.Buffer{}); err == nil {
		t.Fatal("packed actor state with no actor tree")
	}
}

// TestCommitPlacesBothTreesAndFailsAsAWhole commits an activation whose actor
// state holds both trees, and checks that each lands under its own tree of the
// worker's base. When the actor tree cannot be placed, the commit fails without
// leaving either tree or a committed activation behind, and a retry places both.
func TestCommitPlacesBothTreesAndFailsAsAWhole(t *testing.T) {
	guest := t.TempDir()
	ref := &pb.ActivationRef{ActorUid: uuid.NewString(), WorkerUid: uuid.NewString(), Generation: uuid.NewString()}
	rpc, _, _ := stagingConnection(t, guest, ref.GetWorkerUid())
	body := archiveFixture(t, []*tar.Header{
		{Name: actorTreeName() + "/", Typeflag: tar.TypeDir},
		{Name: snapshotsTreeName() + "/", Typeflag: tar.TypeDir},
		{Name: actorTreeName() + "/bundles", Typeflag: tar.TypeDir},
		{Name: actorTreeName() + "/bundles/config", Typeflag: tar.TypeReg, Size: 1},
		{Name: snapshotsTreeName() + "/restore-state", Typeflag: tar.TypeDir},
		{Name: snapshotsTreeName() + "/restore-state/image", Typeflag: tar.TypeReg, Size: 3},
	})
	sum := sha256.Sum256(body)
	artifact := &pb.Artifact{Kind: pb.ArtifactKind_ARTIFACT_KIND_ACTOR_STATE, Id: ref.GetGeneration(), Size: int64(len(body)), Sha256: hex.EncodeToString(sum[:])}
	if _, err := rpc.PrepareActivation(t.Context(), &pb.Activation{Ref: ref, ActorAtespace: "team", ActorName: "counter", Artifacts: []*pb.Artifact{artifact}}); err != nil {
		t.Fatal(err)
	}
	stream, err := rpc.PutArtifact(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&pb.ArtifactChunk{Ref: ref, Kind: artifact.Kind, Id: artifact.Id, Data: body}); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.CloseAndRecv(); err != nil {
		t.Fatal(err)
	}

	restoreImage := filepath.Join(snapshotsDir(guest, ref.GetActorUid()), "restore-state", "image")
	committedRecord := filepath.Join(actorDir(guest, ref.GetActorUid()), activationFile)

	orig := renameActorTree
	renameActorTree = func(string, string) error { return errors.New("injected actor tree failure") }
	_, err = rpc.CommitActivation(t.Context(), ref)
	renameActorTree = orig
	if err == nil {
		t.Fatal("commit succeeded with the actor tree failing to install")
	}
	for _, path := range []string{snapshotsDir(guest, ref.GetActorUid()), actorDir(guest, ref.GetActorUid())} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("failed commit left %s behind (%v)", path, err)
		}
	}

	if _, err := rpc.CommitActivation(t.Context(), ref); err != nil {
		t.Fatalf("retried commit: %v", err)
	}
	if got, err := os.ReadFile(restoreImage); err != nil || string(got) != "xxx" {
		t.Fatalf("restore state after commit = %q, %v", got, err)
	}
	if got, err := os.ReadFile(filepath.Join(actorDir(guest, ref.GetActorUid()), "bundles", "config")); err != nil || string(got) != "x" {
		t.Fatalf("bundle after commit = %q, %v", got, err)
	}
	if _, err := os.Stat(committedRecord); err != nil {
		t.Fatalf("no committed activation record: %v", err)
	}
	if _, err := rpc.CommitActivation(t.Context(), ref); status.Code(err) == codes.OK {
		// A second commit of the same generation has no pending state left.
		t.Fatal("a generation committed twice")
	}
}

// TestActorStateRequiresBothTrees checks that extraction accepts actor state
// only when both trees are present, an empty snapshots tree included, and
// rejects an archive that lacks either tree.
func TestActorStateRequiresBothTrees(t *testing.T) {
	actors := &tar.Header{Name: actorTreeName() + "/", Typeflag: tar.TypeDir}
	snapshots := &tar.Header{Name: snapshotsTreeName() + "/", Typeflag: tar.TypeDir}
	bundles := &tar.Header{Name: actorTreeName() + "/bundles", Typeflag: tar.TypeDir}
	restore := &tar.Header{Name: snapshotsTreeName() + "/restore-state", Typeflag: tar.TypeDir}
	for _, tc := range []struct {
		name    string
		headers []*tar.Header
		ok      bool
	}{
		{"both trees", []*tar.Header{actors, bundles, snapshots, restore}, true},
		{"empty snapshots tree", []*tar.Header{actors, bundles, snapshots}, true},
		{"missing snapshots tree", []*tar.Header{actors, bundles}, false},
		{"missing actors tree", []*tar.Header{snapshots, restore}, false},
		{"snapshots root without its tree entry", []*tar.Header{actors, bundles, restore}, false},
		{"duplicate root entry", []*tar.Header{actors, bundles, bundles, snapshots}, false},
		{"root entry after its children", []*tar.Header{actors, {Name: actorTreeName() + "/bundles/config", Typeflag: tar.TypeReg, Size: 1}, bundles, snapshots}, true},
		{"empty archive", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			headers := make([]*tar.Header, 0, len(tc.headers))
			for _, h := range tc.headers {
				copied := *h
				headers = append(headers, &copied)
			}
			err := extractArtifact(t.Context(), actorStateArtifact(), t.TempDir(), bytes.NewReader(archiveFixture(t, headers)))
			if (err == nil) != tc.ok {
				t.Fatalf("extract = %v, want accept=%v", err, tc.ok)
			}
		})
	}
}

// TestCommitEmptySnapshotsTreeLeavesNothing commits an activation whose
// snapshots tree is empty (nothing staged for restore or checkpoint): the actor
// tree is placed, and no snapshots/<uid> directory is left behind.
func TestCommitEmptySnapshotsTreeLeavesNothing(t *testing.T) {
	guest := t.TempDir()
	ref := &pb.ActivationRef{ActorUid: uuid.NewString(), WorkerUid: uuid.NewString(), Generation: uuid.NewString()}
	rpc, _, _ := stagingConnection(t, guest, ref.GetWorkerUid())
	body := archiveFixture(t, []*tar.Header{
		{Name: actorTreeName() + "/", Typeflag: tar.TypeDir},
		{Name: actorTreeName() + "/bundles", Typeflag: tar.TypeDir},
		{Name: snapshotsTreeName() + "/", Typeflag: tar.TypeDir},
	})
	sum := sha256.Sum256(body)
	artifact := &pb.Artifact{Kind: pb.ArtifactKind_ARTIFACT_KIND_ACTOR_STATE, Id: ref.GetGeneration(), Size: int64(len(body)), Sha256: hex.EncodeToString(sum[:])}
	if _, err := rpc.PrepareActivation(t.Context(), &pb.Activation{Ref: ref, ActorAtespace: "team", ActorName: "counter", Artifacts: []*pb.Artifact{artifact}}); err != nil {
		t.Fatal(err)
	}
	stream, err := rpc.PutArtifact(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&pb.ArtifactChunk{Ref: ref, Kind: artifact.Kind, Id: artifact.Id, Data: body}); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.CloseAndRecv(); err != nil {
		t.Fatal(err)
	}
	if _, err := rpc.CommitActivation(t.Context(), ref); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, err := os.Stat(filepath.Join(actorDir(guest, ref.GetActorUid()), activationFile)); err != nil {
		t.Fatalf("actor tree not placed: %v", err)
	}
	if _, err := os.Lstat(snapshotsDir(guest, ref.GetActorUid())); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("an empty snapshots tree left %s behind (%v)", snapshotsDir(guest, ref.GetActorUid()), err)
	}
}
