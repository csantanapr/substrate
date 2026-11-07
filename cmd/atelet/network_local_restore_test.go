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

	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
	"go.ate.dev/substrate/internal/nodepath"
	wspb "go.ate.dev/substrate/internal/proto/workerstagingpb"
	"go.ate.dev/substrate/internal/resources"
)

// The fix: a NETWORK worker's directLocal
// restore used to point RestoreDir at the node's local-checkpoint dir and NOT
// stage it, but internal/workerstaging/archive.go actorRoots stages
// restore-state and NOT local-checkpoint, so a separate guest never received
// the checkpoint and `runsc restore` FATAL'd (exit 128). This is the regression
// guard, both ways, at the real staging boundary with a real workerstaging
// server (startNetWorker) -- the "network_e2e_test.go style" the ruling named.
//
// A same-node resume is the case that breaks: the node still holds the local
// snapshot from the suspend, readLocalSnapshot succeeds, so Restore takes the
// directLocal leg. For a colocated worker that is correct (shared filesystem).
// For a network worker the fix copies EXACTLY the manifest's files from the
// node-local snapshot into restore-state (which IS staged) and points RestoreDir
// there; stageNetworkActivation then pushes them into the fresh guest.

// stageNodeLocalForNetwork reproduces the fix's data path at the exact helpers
// Atelet.Restore calls for a network worker's directLocal restore: it copies
// the manifest's files from the node-local snapshot into restore-state with the
// real copyLocalCheckpoint, then pushes the fresh activation into the guest with
// the real stageNetworkActivation (and its real workerstaging.Client.Stage). It
// returns the guest's restore-state directory so a test can read what landed.
func stageNodeLocalForNetwork(t *testing.T, s *Atelet, actor resources.ActorRef, actorUID, workerUID, snapshotName string, files []string) error {
	t.Helper()
	// 1. The download/copy errgroup leg for networkLocalStage.
	if err := s.copyLocalCheckpoint(context.Background(), ateletpath.ActorSnapshotPath(actorUID), snapshotName,
		ateletpath.LocalCheckpointsDir(actorUID), ateletpath.RestoreStateDir(actorUID), files); err != nil {
		return err
	}
	// 2. The pre-RPC push into the fresh guest.
	_, err := s.stageNetworkActivation(context.Background(), actor, actorUID, workerUID,
		layoutNode(t, s.nodeBase, actorUID, nil))
	return err
}

// newNetworkRestoreAtelet builds a minimal network Atelet whose nodeBase
// is the SAME root the node's actor directories live under, so copyLocalCheckpoint
// (which writes under nodepath.ActorsDir) and the staging client (which reads under
// nodeBase) see one restore-state directory.
func newNetworkRestoreAtelet(t *testing.T, w *netWorker, workerUID string) *Atelet {
	t.Helper()
	root := t.TempDir()
	orig, origSnapshots := nodepath.ActorsDir, nodepath.SnapshotsDir
	nodepath.ActorsDir = filepath.Join(root, "actors")
	nodepath.SnapshotsDir = filepath.Join(root, "snapshots")
	t.Cleanup(func() { nodepath.ActorsDir, nodepath.SnapshotsDir = orig, origSnapshots })
	s := &Atelet{
		checkpointTimeout: defaultCheckpointTimeout,
		network:           networkFor(t, workerUID),
		generations:       newTestGenerations(t, nodeIdentity{NodeName: "node-a", NodeUID: "node-a-incarnation"}),
		nodeBase:          root,
		staging: stagingFunc(func(_ context.Context, uid string) (wspb.WorkerStagingClient, error) {
			if uid != workerUID {
				t.Fatalf("staging dial for an unexpected worker %q", uid)
			}
			return w.staging, nil
		}),
	}
	return s
}

// writeNodeLocalSnapshot writes the actor's node-local snapshot exactly where a
// same-node suspend would have left it: under the actor's local-checkpoint dir,
// keyed by the snapshot UUID.
func writeNodeLocalSnapshot(t *testing.T, actorUID, snapshotName string, contents map[string]string) {
	t.Helper()
	writeLocalSnapshot(t, ateletpath.LocalSnapshotDir(actorUID, snapshotName), sandboxAssetsRecord{
		SandboxClass:  "gvisor",
		SnapshotFiles: keysOf(contents),
	}, contents)
}

func keysOf(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// guestSnapshots is an actor's snapshots tree under a worker's staging base,
// laid out as nodepath lays it out under BasePath.
func guestSnapshots(base, actorUID string) string {
	return filepath.Join(base, filepath.Base(nodepath.SnapshotsDir), actorUID)
}

func guestRestoreState(base, actorUID string) string {
	return filepath.Join(guestSnapshots(base, actorUID), "restore-state")
}

// TestNetworkLocalStageRefusesListedButMissingFile proves the membership rule in
// the copy direction: if the manifest lists a file the node-local snapshot does
// not actually have, copyLocalCheckpoint REFUSES (so Restore fails closed and the
// worker RPC is never reached) rather than staging a partial restore-state.
func TestNetworkLocalStageRefusesListedButMissingFile(t *testing.T) {
	const (
		snapshotName = "pause-snap-1"
		workerUID    = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	)
	actorUID := "11111111-1111-1111-1111-111111111111"

	w := startNetWorker(t, workerUID, "")
	s := newNetworkRestoreAtelet(t, w, workerUID)
	// The snapshot on disk has only checkpoint.img, but we ask to stage a file
	// set that also lists pages.img.
	writeNodeLocalSnapshot(t, actorUID, snapshotName, map[string]string{"checkpoint.img": "mem"})
	err := s.copyLocalCheckpoint(context.Background(), ateletpath.ActorSnapshotPath(actorUID), snapshotName,
		ateletpath.LocalCheckpointsDir(actorUID), ateletpath.RestoreStateDir(actorUID),
		[]string{"checkpoint.img", "pages.img"})
	if err == nil {
		t.Fatal("copyLocalCheckpoint accepted a listed-but-missing file; it must refuse so the restore fails closed and no worker RPC runs")
	}
}

// TestNetworkLocalStageCopiesOnlyManifestFiles proves the other half of the
// membership rule: an extra file that sits in the node-local snapshot but is NOT
// in the file set is NOT staged into the guest. Only the listed files cross the
// boundary.
func TestNetworkLocalStageCopiesOnlyManifestFiles(t *testing.T) {
	const (
		snapshotName = "pause-snap-1"
		workerUID    = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	)
	actor := resources.ActorRef{Atespace: "team", Name: "counter"}
	actorUID := "11111111-1111-1111-1111-111111111111"

	w := startNetWorker(t, workerUID, "")
	s := newNetworkRestoreAtelet(t, w, workerUID)
	// The snapshot dir holds an extra "secret" file the manifest does not list.
	writeNodeLocalSnapshot(t, actorUID, snapshotName, map[string]string{
		"checkpoint.img": "mem",
		"secret":         "must-not-leave-the-node",
	})
	// Stage only the listed file.
	if err := stageNodeLocalForNetwork(t, s, actor, actorUID, workerUID, snapshotName,
		[]string{"checkpoint.img"}); err != nil {
		t.Fatalf("fixed network-local staging: %v", err)
	}
	guest := guestRestoreState(w.base, actorUID)
	if _, err := os.Stat(filepath.Join(guest, "checkpoint.img")); err != nil {
		t.Fatalf("guest missing the listed file checkpoint.img: %v", err)
	}
	if _, err := os.Stat(filepath.Join(guest, "secret")); !os.IsNotExist(err) {
		t.Fatalf("the unlisted file 'secret' reached the guest (err=%v); copyLocalCheckpoint must copy only the listed files", err)
	}
}

// TestWantsNetworkLocalStage pins the gating that drives BOTH the restore-state
// staging and PreserveRestoreDir in Atelet.Restore. It is the direct, staging-free
// guard for the fix's decision, and it is what makes the gating variants fail:
//
//   - "skip the new branch" (networkLocalStage := false) flips the network+directLocal
//     case to false -> this test's first row fails.
//   - "apply the branch to the colocated path too" (drop the networkEnabled guard)
//     flips the colocated directLocal case to true -> the second row fails.
//
// The PreserveRestoreDir variant ("keep true for the network case") is caught by the
// RestoreWorkload field this gates: PreserveRestoreDir = directLocal && !networkLocalStage,
// so networkLocalStage==true forces it false, and a colocated directLocal keeps it true.
func TestWantsNetworkLocalStage(t *testing.T) {
	for _, tc := range []struct {
		name           string
		directLocal    bool
		networkEnabled bool
		wantStage      bool
		wantPreserve   bool // directLocal && !stage, the value Restore sets on the worker request
	}{
		{"network worker, directLocal -> stage, do not preserve", true, true, true, false},
		{"colocated worker, directLocal -> no stage, preserve in place", true, false, false, true},
		{"network worker, object-storage restore -> no stage, no preserve", false, true, false, false},
		{"colocated worker, object-storage restore -> no stage, no preserve", false, false, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gotStage := wantsNetworkLocalStage(tc.directLocal, tc.networkEnabled)
			if gotStage != tc.wantStage {
				t.Fatalf("wantsNetworkLocalStage(%v, %v) = %v, want %v", tc.directLocal, tc.networkEnabled, gotStage, tc.wantStage)
			}
			gotPreserve := preserveRestoreDirForLocal(tc.directLocal, gotStage)
			if gotPreserve != tc.wantPreserve {
				t.Fatalf("preserveRestoreDirForLocal(%v, %v) = %v, want %v", tc.directLocal, gotStage, gotPreserve, tc.wantPreserve)
			}
		})
	}
}
