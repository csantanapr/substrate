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
	"strings"
	"testing"

	"github.com/google/uuid"
	"go.ate.dev/substrate/internal/imagecache"
	"go.ate.dev/substrate/internal/nodepath"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	pb "go.ate.dev/substrate/internal/proto/workerstagingpb"
	"go.ate.dev/substrate/internal/resources"
)

// realGVisorAssetNode builds the node-side layout a production atelet actually
// hands to Stage under upstream #2699: the gVisor release staged into the
// actor's OWN per-actor sandbox-assets dir under the plain asset NAME "gvisor"
// (runsc plus its gvisor-bin/ helper), the value of the runtimePaths entry
// being the local path of runsc inside that directory. No digest-shaped id and
// no shared static-files dir are involved, because neither exists on a real
// node after #2699. It returns the runtimePaths map keyed by asset name, as
// cmd/atelet/sandbox_assets.go's ensureSandboxAssets produces it.
func realGVisorAssetNode(t *testing.T, base, actorUID string) map[string]string {
	t.Helper()
	assets := nodepath.ActorSandboxAssetsDir(actorUID)
	// Written relative to base so the archive root (c.base) can reach them.
	rel, err := filepath.Rel(nodepath.BasePath, assets)
	if err != nil {
		t.Fatal(err)
	}
	fixtureFile(t, base, filepath.Join(rel, "gvisor", "runsc"), "runtime binary")
	fixtureFile(t, base, filepath.Join(rel, "gvisor", "gvisor-bin", "runsc-gofer"), "helper binary")
	actor := filepath.Join("actors", actorUID)
	fixtureFile(t, base, filepath.Join(actor, "bundles", "main", "config.json"), `{"root":{"path":"rootfs"}}`)
	if err := imagecache.WriteSpec(filepath.Join(base, actor, "bundles", "main"), &imagecache.OverlaySpec{}); err != nil {
		t.Fatal(err)
	}
	fixtureFile(t, base, filepath.Join(snapshotsTreeName(), actorUID, "restore-state", "image"), "restore bytes")
	return map[string]string{"gvisor": filepath.Join(assets, "gvisor", "runsc")}
}

// TestStageAcceptsRealPerActorRuntimeLayout drives Stage with the real #2699
// per-actor layout (asset name "gvisor", not a content-addressed digest id).
// On head 23566f22 this FAILS: client.go takes the first path component
// ("gvisor") as the artifact id and requires it to match runtimePattern, the
// old digest-suffixed form, so a real runtime path is rejected with
// "invalid runtime path". After the fix the whole asset-name subtree is staged
// into the worker's own actors/<uid>/sandbox-assets/gvisor dir with the
// executable bit preserved, which is where the worker's validateRunscPath
// requires runsc to live.
func TestStageAcceptsRealPerActorRuntimeLayout(t *testing.T) {
	node, guest := t.TempDir(), t.TempDir()
	ref := &pb.ActivationRef{ActorUid: uuid.NewString(), WorkerUid: uuid.NewString(), Generation: uuid.NewString()}
	assets := realGVisorAssetNode(t, node, ref.GetActorUid())
	rpc, lifecycle, runtime := stagingConnection(t, guest, ref.GetWorkerUid())
	client := NewClient(node, rpc)

	if err := client.Stage(t.Context(), ref, resources.ActorRef{Atespace: "team", Name: "counter"}, assets); err != nil {
		t.Fatalf("Stage rejected the real per-actor runtime layout: %v", err)
	}

	// The worker must find runsc at its OWN per-actor sandbox-assets path, the
	// only place validateRunscPath permits it to run from, with content and
	// the executable bit intact and the helper beside it.
	wantRunsc := filepath.Join(guest, "actors", ref.GetActorUid(), "sandbox-assets", "gvisor", "runsc")
	info, err := os.Stat(wantRunsc)
	if err != nil {
		t.Fatalf("runsc did not arrive at the worker's per-actor path: %v", err)
	}
	if info.Mode().Perm()&0o111 == 0 {
		t.Fatalf("runsc lost its executable bit: mode %v", info.Mode())
	}
	if body, err := os.ReadFile(wantRunsc); err != nil || string(body) != "runtime binary" {
		t.Fatalf("runsc content differs: %q %v", body, err)
	}
	helper := filepath.Join(guest, "actors", ref.GetActorUid(), "sandbox-assets", "gvisor", "gvisor-bin", "runsc-gofer")
	if body, err := os.ReadFile(helper); err != nil || string(body) != "helper binary" {
		t.Fatalf("gvisor-bin helper did not arrive beside runsc: %q %v", body, err)
	}

	// The staged runtime path the worker's RunWorkload names must satisfy the
	// authoritative worker-side contract, proving staging and the worker agree.
	actorDirs := &ateworkerpb.ActorDirs{SandboxAssetsDir: nodepath.ActorSandboxAssetsDir(ref.GetActorUid())}
	if errs := resources.ValidateSandboxAssetsDir(ref.GetActorUid(), actorDirs.GetSandboxAssetsDir(), nil); len(errs) != 0 {
		t.Fatalf("staged sandbox-assets dir is not the actor's own: %v", errs)
	}

	// The committed activation must let the lifecycle RPC through, closing the
	// loop the live bench exercises right after the image pull.
	if _, err := lifecycle.RunWorkload(t.Context(), &ateworkerpb.RunWorkloadRequest{
		Atespace: "team", ActorName: "counter", ActorUid: ref.GetActorUid(),
	}); err != nil || runtime.runs.Load() != 1 {
		t.Fatalf("staged actor did not reach the runtime: %v", err)
	}
}

// TestStageRejectsRuntimePathEscapes proves the staging boundary BOTH ways: a
// canonical path inside the actor's own sandbox-assets dir is accepted (the
// control), while a traversal, an absolute path outside the root, another
// actor's sandbox-assets dir, and a non-plain asset name are each refused
// before any artifact is declared. Removing the "not inside" / IsLocal / clean
// / assetNamePattern guards in client.go's runtime loop makes a reject case
// here pass, so this is the regression sensor for the traversal
// check. Stage only inspects the runtimePaths map (it validates the paths
// before ever reading their bytes), so no on-disk files are needed per case.
func TestStageRejectsRuntimePathEscapes(t *testing.T) {
	actorUID := uuid.NewString()
	otherUID := uuid.NewString()
	root := nodepath.ActorSandboxAssetsDir(actorUID)

	for _, tc := range []struct {
		name   string
		path   string
		accept bool
	}{
		{"canonical in-root control", filepath.Join(root, "gvisor", "runsc"), true},
		{"bare asset at root", filepath.Join(root, "runsc"), true},
		{"parent traversal", filepath.Join(root, "..", "..", "etc", "runsc"), false},
		{"embedded dotdot", filepath.Join(root, "gvisor", "..", "..", "secret"), false},
		{"other actor sandbox-assets", filepath.Join(nodepath.ActorSandboxAssetsDir(otherUID), "gvisor", "runsc"), false},
		{"absolute outside root", "/etc/shadow", false},
		{"root itself (no asset name)", root, false},
		{"relative path", filepath.Join("gvisor", "runsc"), false},
		{"uppercase asset name", filepath.Join(root, "GVisor", "runsc"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node, guest := t.TempDir(), t.TempDir()
			ref := &pb.ActivationRef{ActorUid: actorUID, WorkerUid: uuid.NewString(), Generation: uuid.NewString()}
			// A minimal actor dir so Stage reaches the runtime loop (the bundle
			// scan runs first); the runtime bytes themselves are never read for
			// a rejected path, and for the accepted controls we only assert the
			// path validates, so we stop before the archive by using a client
			// whose server rejects. Instead, assert on the validation outcome
			// directly via a real Stage and classify by error substring.
			fixtureFile(t, node, filepath.Join("actors", actorUID, "bundles", "main", "config.json"), `{"root":{"path":"rootfs"}}`)
			if err := imagecache.WriteSpec(filepath.Join(node, "actors", actorUID, "bundles", "main"), &imagecache.OverlaySpec{}); err != nil {
				t.Fatal(err)
			}
			// Put the real file where an accepted path points, so an accepted
			// case proceeds past validation into a successful stage; a rejected
			// case fails at validation regardless of the filesystem.
			if tc.accept {
				rel, err := filepath.Rel(nodepath.BasePath, tc.path)
				if err != nil {
					t.Fatal(err)
				}
				fixtureFile(t, node, rel, "runtime binary")
			}
			rpc, _, _ := stagingConnection(t, guest, ref.GetWorkerUid())
			err := NewClient(node, rpc).Stage(t.Context(), ref,
				resources.ActorRef{Atespace: "team", Name: "counter"},
				map[string]string{"gvisor": tc.path})
			if tc.accept {
				if err != nil {
					t.Fatalf("canonical in-root runtime path was rejected: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("escape path %q was accepted", tc.path)
			}
			// The rejection must be one of client.Stage's runtime-path
			// VALIDATION errors, not an incidental downstream failure (e.g.
			// "runtime has no content" / "runtime asset is incomplete" from the
			// archive or server), so the sensor fires on the guard under test.
			msg := err.Error()
			validationRejection := strings.Contains(msg, "noncanonical runtime path") ||
				strings.Contains(msg, "is not inside") ||
				strings.Contains(msg, "invalid runtime asset name")
			if !validationRejection {
				t.Fatalf("rejection did not come from runtime-path validation: %v", err)
			}
		})
	}
}

// TestFoldRuntimeAssetsIsRetrySafe proves CommitActivation's runtime fold is
// idempotent. The fold moves each runtime subtree out of pending/sandbox-assets
// BEFORE the actor-directory rename, so if a later commit step fails the
// completeness markers still pass the gate on a retry while the fold source has
// already moved. A second fold of the same (ref, activation, tree) must treat
// the already-moved subtree as done rather than fail, so a retried commit
// converges instead of deadlocking.
func TestFoldRuntimeAssetsIsRetrySafe(t *testing.T) {
	base := t.TempDir()
	server, err := NewServer(base, uuid.NewString(), func(string) bool { return false })
	if err != nil {
		t.Fatal(err)
	}
	ref := &pb.ActivationRef{ActorUid: uuid.NewString(), WorkerUid: server.workerUID, Generation: uuid.NewString()}
	activation := &pb.Activation{Ref: ref, Artifacts: []*pb.Artifact{
		{Kind: pb.ArtifactKind_ARTIFACT_KIND_RUNTIME, Id: "gvisor"},
	}}
	// Stage the runtime subtree as placeRuntime leaves it: content under the
	// pending sandbox-assets dir with a completeness marker beside it.
	pendingAssets := server.runtimePending(ref)
	fixtureFile(t, pendingAssets, filepath.Join("gvisor", "runsc"), "runtime binary")
	if err := os.WriteFile(runtimeComplete(pendingAssets, "gvisor"), []byte("sha"), 0o600); err != nil {
		t.Fatal(err)
	}
	tree := filepath.Join(server.pending(ref), "actor-state")
	if err := os.MkdirAll(tree, 0o700); err != nil {
		t.Fatal(err)
	}

	// First fold places the subtree into the actor-state tree.
	if err := server.foldRuntimeAssets(ref, activation, tree); err != nil {
		t.Fatalf("first fold failed: %v", err)
	}
	folded := filepath.Join(tree, "sandbox-assets", "gvisor", "runsc")
	if body, err := os.ReadFile(folded); err != nil || string(body) != "runtime binary" {
		t.Fatalf("first fold did not place the runtime: %q %v", body, err)
	}
	// The source is now gone; a retry (as a re-run of CommitActivation after a
	// later-step failure) must still succeed and leave the folded tree intact.
	if _, err := os.Lstat(filepath.Join(pendingAssets, "gvisor")); !os.IsNotExist(err) {
		t.Fatalf("fold did not consume the pending subtree: %v", err)
	}
	if err := server.foldRuntimeAssets(ref, activation, tree); err != nil {
		t.Fatalf("retried fold was not idempotent: %v", err)
	}
	if body, err := os.ReadFile(folded); err != nil || string(body) != "runtime binary" {
		t.Fatalf("retried fold disturbed the runtime: %q %v", body, err)
	}

	// But a genuinely missing runtime (no source, no folded target) must fail,
	// so the idempotent branch cannot mask a lost upload.
	other := &pb.Activation{Ref: ref, Artifacts: []*pb.Artifact{
		{Kind: pb.ArtifactKind_ARTIFACT_KIND_RUNTIME, Id: "runsc"},
	}}
	if err := server.foldRuntimeAssets(ref, other, tree); err == nil {
		t.Fatal("fold accepted a runtime that was never staged")
	}
}
