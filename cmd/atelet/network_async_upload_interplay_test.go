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

// Interplay introduced by upstream #2789 (atelet async snapshot upload) against
// the network-worker checkpoint pull. Before #2789 a network-worker pause
// snapshot was uploaded only by an explicit PromoteSnapshot; the pull → commit
// → upload sequence never raced, and the superseded-snapshot prune ran BEFORE
// the persist. #2789 turns the background upload on (it reads a COMMITTED local
// snapshot out of node custody) and moves the prune to AFTER the commit. That
// creates three runtime obligations for the network path that upstream's own
// tests do not exercise, because upstream has no pull:
//
//   (b-runtime) the files a background upload reads out of node custody must be
//               the COMPLETE set the guest reported, INCLUDING the fs/<name>
//               members. The pull is the only thing that puts them in
//               custody, so if the pull dropped them the committed snapshot
//               would upload an incomplete set.
//   (c-runtime) a FAILED pull must leave the previous snapshot in place and
//               start no upload of the failed one: the pull returns before any
//               commit, and the background upload only ever reads a committed
//               snapshot.
//
// The lexical handler-order guard (network_handler_order_test.go) pins that the
// pull precedes the commit and the prune. These tests pin the RUNTIME effect of
// that ordering, and each is proved BOTH ways with a call-site variant named in
// its comment.

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	wspb "go.ate.dev/substrate/internal/proto/workerstagingpb"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/workerstaging"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestNetworkCheckpointUploadContainsFsFiles proves (b-runtime): a network
// worker reports a checkpoint whose set includes fs/<name> members; the pull
// puts EXACTLY that set into node custody; and a later background-style upload
// of the committed local snapshot uploads every member, fs/ files included.
//
// The two halves share one guest checkpoint:
//  1. s.receiveNetworkCheckpoint pulls the guest's reported set (incl. fs/
//     files) into node custody over the real workerstaging transport.
//  2. the custody dir is laid out as the committed local snapshot (manifest +
//     files) and s.uploadLocalCheckpointDir uploads it through the object-store
//     plugin into a recording store, exactly as the background uploader's
//     trackerStore.UploadLocalSnapshot does.
//
// Asserting the recording store's keys carry the fs/ members proves the upload
// a background goroutine would perform contains the fs/ files.
//
// BOTH WAYS. Negative case: have the handler pull only the top-level set (drop the fs/
// members) into custody -- model it by pulling a set with the fs/ entries
// stripped. The recording store then lacks the fs/ keys and the assertion
// fails. The negative half below runs that stripped pull and requires the
// uploaded keys to MISS the fs/ files, so a pull that silently dropped fs/
// members cannot pass this test.
func TestNetworkCheckpointUploadContainsFsFiles(t *testing.T) {
	ctx := context.Background()
	nodeBase := t.TempDir()
	actor := resources.ActorRef{Atespace: "team", Name: "counter"}
	actorUID := "11111111-1111-1111-1111-111111111111"
	workerUID := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"

	// A checkpoint set with two top-level files and two fs/ members:
	// SnapshotFiles is the COMPLETE set, which is what the handler pulls.
	reported := []string{"memory", "config.json", "fs/rootfs.img", "fs/lower.layer"}

	w := startFsNetWorker(t, workerUID, reported)
	s := &Atelet{
		checkpointTimeout: defaultCheckpointTimeout,
		network:           networkFor(t, workerUID),
		generations:       newTestGenerations(t, nodeIdentity{NodeName: "node-a", NodeUID: "node-a-incarnation"}),
		nodeBase:          nodeBase,
		staging: stagingFunc(func(_ context.Context, uid string) (wspb.WorkerStagingClient, error) {
			if uid != workerUID {
				t.Fatalf("unexpected worker %q", uid)
			}
			return w.staging, nil
		}),
	}

	ref, err := s.stageNetworkActivation(ctx, actor, actorUID, workerUID, layoutNode(t, nodeBase, actorUID, nil))
	if err != nil {
		t.Fatalf("stage: %v", err)
	}
	if _, err := w.lifecycle.RunWorkload(ctx,
		&ateworkerpb.RunWorkloadRequest{Atespace: actor.Atespace, ActorName: actor.Name, ActorUid: actorUID}); err != nil {
		t.Fatalf("run: %v", err)
	}
	resp, err := w.lifecycle.CheckpointWorkload(ctx,
		&ateworkerpb.CheckpointWorkloadRequest{Atespace: actor.Atespace, ActorName: actor.Name, ActorUid: actorUID})
	if err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if !slices.Equal(resp.GetSnapshotFiles(), reported) {
		t.Fatalf("worker reported %v, want the complete set %v", resp.GetSnapshotFiles(), reported)
	}

	// Positive: the handler pulls the COMPLETE reported set, so the upload
	// carries the fs/ files.
	gotKeys := pullCustodyThenUpload(t, s, ref, resp.GetSnapshotFiles(), nodeBase, actorUID)
	for _, want := range []string{"fs/rootfs.img", "fs/lower.layer"} {
		if !keyHasMember(gotKeys, want) {
			t.Fatalf("uploaded object keys %v are missing fs/ member %q; a network-worker snapshot upload must contain the fs/ files", gotKeys, want)
		}
	}

	// Negative (the call-site variant that proves the assertion is not vacuous).
	// The uploader (trackerStore.UploadLocalSnapshot -> uploadLocalCheckpointDir)
	// uploads exactly the manifest's SnapshotFiles. The variant under test is a
	// checkpoint that RECORDED a membership missing the fs/ members (the
	// sandboxRec.SnapshotFiles the handler writes from the worker's report):
	// drop the fs/ members from the manifest while leaving the pulled files in
	// place, and the upload MUST then miss the fs/ keys. If it still uploaded
	// them the positive assertion would be meaningless. Note: the pull itself
	// cannot be mutated to drop fs/ -- Export enforces the EXACT guest-reported
	// membership and rejects a short set -- so the only way a committed snapshot
	// can lack fs/ is a wrong manifest, which is what this models.
	stripped := slices.DeleteFunc(slices.Clone(resp.GetSnapshotFiles()), func(f string) bool {
		return strings.HasPrefix(f, "fs/")
	})
	mutantKeys := uploadCustodyWithManifest(t, s, ref, resp.GetSnapshotFiles(), stripped, nodeBase, actorUID)
	for _, fsMember := range []string{"fs/rootfs.img", "fs/lower.layer"} {
		if keyHasMember(mutantKeys, fsMember) {
			t.Fatalf("a manifest that omitted the fs/ members still uploaded %q (keys %v); the positive assertion would pass vacuously", fsMember, mutantKeys)
		}
	}
	// And the top-level members still upload under the variant, so the variant
	// store is not simply empty (which would make "missing fs/" trivially true).
	for _, top := range []string{"memory", "config.json"} {
		if !keyHasMember(mutantKeys, top) {
			t.Fatalf("the variant upload is missing even the top-level member %q (keys %v); the negative half must upload the recorded set, just not fs/", top, mutantKeys)
		}
	}
}

// TestNetworkCheckpointFailedPullLeavesPreviousCustodyIntact proves the Export
// guarantee that invariant (c) rests on: when the pull into node custody FAILS,
// the PREVIOUS committed snapshot already in that custody dir is left
// byte-for-byte, with no partial artifacts of the failed pull.
//
// This is the piece invariant (c) needs that is NOT pinned by the handler-order
// guard. The handler-order guard (network_handler_order_test.go) already pins
// that the pull precedes the commit and the prune, so a FAILED pull returns
// before any commit and before the prune ever runs -- that is the "no upload
// starts / previous snapshot kept / recorded" half, proven by ORDER. What ORDER
// cannot prove is that the failing pull does not itself corrupt the previous
// custody on its way out. Export stages to a temp dir, verifies completion, and
// refuses to overwrite nonempty custody before an atomic rename, so a failed
// pull leaves the previous snapshot in place; this test exercises exactly that.
//
// It drives the pull directly with a worker whose committed activation the node
// does NOT match (a forged generation ref), so Export is refused on the worker
// side -- the same failure a real pull hits when the guest is gone or disagrees.
//
// Deliberately NOT asserted here: that "the object store stays empty". This test
// never drives anything that could upload, so an empty store would be vacuous;
// the no-upload-of-a-failed-snapshot property is the handler ORDER guard's job.
//
// BOTH WAYS. Negative half: the MATCHING generation makes the pull SUCCEED and
// deliver the set, proving the refusal above is the ownership check firing, not
// an unconditional failure -- a pull that could never succeed would fail here.
func TestNetworkCheckpointFailedPullLeavesPreviousCustodyIntact(t *testing.T) {
	ctx := context.Background()
	nodeBase := t.TempDir()
	actor := resources.ActorRef{Atespace: "team", Name: "counter"}
	actorUID := "11111111-1111-1111-1111-111111111111"
	workerUID := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"

	s := &Atelet{
		checkpointTimeout: defaultCheckpointTimeout,
		network:           networkFor(t, workerUID),
		generations:       newTestGenerations(t, nodeIdentity{NodeName: "node-a", NodeUID: "node-a-incarnation"}),
		nodeBase:          nodeBase,
	}

	w := startFsNetWorker(t, workerUID, []string{"memory", "fs/rootfs.img"})
	s.staging = stagingFunc(func(_ context.Context, uid string) (wspb.WorkerStagingClient, error) {
		if uid != workerUID {
			t.Fatalf("unexpected worker %q", uid)
		}
		return w.staging, nil
	})

	// A "previous" committed local snapshot already in node custody: the one a
	// failed checkpoint must leave in place.
	custody := filepath.Join(nodeBase, "node-custody")
	writeLocalSnapshot(t, custody, sandboxAssetsRecord{
		SandboxClass:  "gvisor",
		SnapshotFiles: []string{"memory"},
		Fidelity:      ateattr.SnapshotFidelityMemory,
	}, map[string]string{"memory": "previous-good-bytes"})

	// Stage + run so the worker has a committed activation, but build a pull ref
	// whose generation does NOT match it, so Export is refused on the worker
	// side -- a failing pull.
	if _, err := s.stageNetworkActivation(ctx, actor, actorUID, workerUID, layoutNode(t, nodeBase, actorUID, nil)); err != nil {
		t.Fatalf("stage: %v", err)
	}
	if _, err := w.lifecycle.RunWorkload(ctx,
		&ateworkerpb.RunWorkloadRequest{Atespace: actor.Atespace, ActorName: actor.Name, ActorUid: actorUID}); err != nil {
		t.Fatalf("run: %v", err)
	}
	good, err := s.networkActivation(actor, actorUID, workerUID)
	if err != nil {
		t.Fatalf("networkActivation: %v", err)
	}
	forged := forgeGeneration(good, "generation-that-was-never-committed")

	// Take the checkpoint so the worker goes INACTIVE and releases the actor:
	// Export is permitted only once the worker no longer holds it. With the
	// worker inactive, the forged pull can fail for exactly one reason -- the
	// generation the worker never committed -- so (1) tests the ownership check,
	// not the activeness check.
	if _, err := w.lifecycle.CheckpointWorkload(ctx,
		&ateworkerpb.CheckpointWorkloadRequest{Atespace: actor.Atespace, ActorName: actor.Name, ActorUid: actorUID}); err != nil {
		t.Fatalf("checkpoint on the worker: %v", err)
	}

	// (1) The pull FAILS on the forged generation.
	if err := s.receiveNetworkCheckpoint(ctx, forged, []string{"memory", "fs/rootfs.img"}, custody); err == nil {
		t.Fatal("a pull keyed by a generation the worker never committed succeeded; it must be refused")
	}
	// (2) The previous snapshot is untouched: Export stages to a temp dir and
	// renames atomically, and refuses to overwrite nonempty custody, so a failed
	// pull cannot have replaced it or left a partial fs/ layout.
	if got, err := os.ReadFile(filepath.Join(custody, "memory")); err != nil || string(got) != "previous-good-bytes" {
		t.Fatalf("previous snapshot after a failed pull = %q, %v; want it left in place unchanged", got, err)
	}
	if _, err := os.Stat(filepath.Join(custody, "fs")); !os.IsNotExist(err) {
		t.Fatalf("a failed pull left fs/ artifacts in custody (stat err=%v); the failed snapshot must not be partially present", err)
	}

	// Negative half (the variant): the MATCHING ref makes the pull succeed and
	// delivers the set into a fresh custody dir. This proves the refusal above
	// is the ownership (generation) check firing, not an unconditional failure --
	// the worker is already inactive, so the only difference is the generation.
	okCustody := filepath.Join(nodeBase, "ok-custody")
	if err := s.receiveNetworkCheckpoint(ctx, good, []string{"memory", "fs/rootfs.img"}, okCustody); err != nil {
		t.Fatalf("the matching-generation pull failed: %v; the failed-pull test would be vacuous if no pull can succeed", err)
	}
	for _, name := range []string{"memory", "fs/rootfs.img"} {
		if _, err := os.Stat(filepath.Join(okCustody, name)); err != nil {
			t.Fatalf("the matching pull did not deliver %q into custody: %v", name, err)
		}
	}
}

// pullCustodyThenUpload pulls files into a fresh node-custody dir over ref, then
// lays that custody out as a committed local snapshot and uploads it through the
// object-store plugin into a fresh recording store, returning the bucket/object
// keys the store received. This mirrors exactly what the Checkpoint handler's
// pull plus the background trackerStore.UploadLocalSnapshot do, in sequence.
func pullCustodyThenUpload(t *testing.T, s *Atelet, ref *wspb.ActivationRef, files []string, nodeBase, actorUID string) []string {
	t.Helper()
	// The honest case: the manifest records exactly the pulled set, as the
	// Checkpoint handler does (sandboxRec.SnapshotFiles is the guest's report).
	return uploadCustodyWithManifest(t, s, ref, files, files, nodeBase, actorUID)
}

// uploadCustodyWithManifest pulls pullSet into a fresh node-custody dir over
// ref, writes a manifest recording manifestSet beside the pulled files, then
// uploads the custody through the object-store plugin into a fresh recording
// store, returning the keys it received. The honest caller passes pullSet ==
// manifestSet; a test that needs to model a wrong recorded membership (a
// manifest that omits some pulled files) passes a shorter manifestSet while the
// files themselves remain in custody.
func uploadCustodyWithManifest(t *testing.T, s *Atelet, ref *wspb.ActivationRef, pullSet, manifestSet []string, nodeBase, actorUID string) []string {
	t.Helper()
	ctx := context.Background()
	custody := filepath.Join(t.TempDir(), "custody")
	if err := s.receiveNetworkCheckpoint(ctx, ref, pullSet, custody); err != nil {
		t.Fatalf("pull into custody: %v", err)
	}
	// The files are already present from the pull; write the manifest beside
	// them directly to avoid clobbering the pulled fs/ layout.
	rec := sandboxAssetsRecord{
		SandboxClass:  "gvisor",
		SnapshotFiles: slices.Clone(manifestSet),
		Fidelity:      ateattr.SnapshotFidelityMemory,
	}
	writeManifestBeside(t, custody, rec)

	store := &recordingObjectStorage{}
	up := newPluginHerder(t, store)
	uri, err := resources.ParseSnapshotURI(pausedSnapshotURI)
	if err != nil {
		t.Fatalf("ParseSnapshotURI: %v", err)
	}
	if _, _, err := up.uploadLocalCheckpointDir(ctx, validPromoteSnapshotRequest(), custody, uri); err != nil {
		t.Fatalf("uploadLocalCheckpointDir over pulled custody: %v", err)
	}
	return store.keys()
}

// keyHasMember reports whether any uploaded object key names the snapshot
// member. The object-store plugin uploads each file under
// .../snapshots/<uuid>/<member> and appends ".zstd" to the compressed object,
// so a member "fs/rootfs.img" appears as a key ending in "/fs/rootfs.img.zstd".
// Matching the "/<member>" path segment (with or without the .zstd suffix)
// keeps the fs/ directory structure significant: a flat "rootfs.img" upload
// would NOT satisfy a "fs/rootfs.img" member.
func keyHasMember(keys []string, member string) bool {
	for _, k := range keys {
		if strings.HasSuffix(k, "/"+member) || strings.HasSuffix(k, "/"+member+".zstd") {
			return true
		}
	}
	return false
}

// fsNetWorker is like netWorker but writes a configurable checkpoint set whose
// names may include fs/<name> members, creating the nested dirs. The
// shared netWorker.CheckpointWorkload writes flat names only, so this one is
// used for the fs/ interplay tests.
type fsNetWorker struct {
	ateworkerpb.UnimplementedWorkerServer
	base          string
	snapshotFiles []string
	staging       wspb.WorkerStagingClient
	lifecycle     ateworkerpb.WorkerClient
	active        bool
}

func (w *fsNetWorker) RunWorkload(context.Context, *ateworkerpb.RunWorkloadRequest) (*ateworkerpb.RunWorkloadResponse, error) {
	w.active = true
	return &ateworkerpb.RunWorkloadResponse{}, nil
}

func (w *fsNetWorker) RestoreWorkload(context.Context, *ateworkerpb.RestoreWorkloadRequest) (*ateworkerpb.RestoreWorkloadResponse, error) {
	w.active = true
	return &ateworkerpb.RestoreWorkloadResponse{}, nil
}

func (w *fsNetWorker) CheckpointWorkload(_ context.Context, req *ateworkerpb.CheckpointWorkloadRequest) (*ateworkerpb.CheckpointWorkloadResponse, error) {
	dir := filepath.Join(guestSnapshots(w.base, req.GetActorUid()), "checkpoint-state")
	for _, name := range w.snapshotFiles {
		full := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
			return nil, err
		}
		if err := os.WriteFile(full, []byte(name+" contents"), 0o600); err != nil {
			return nil, err
		}
	}
	w.active = false
	return &ateworkerpb.CheckpointWorkloadResponse{SnapshotFiles: w.snapshotFiles}, nil
}

// startFsNetWorker stands up an fsNetWorker over an in-process TCP listener with
// the production workerstaging.Server and its LifecycleInterceptor, so the
// checkpoint membership is recorded against the worker's committed activation
// exactly as in production. The activeness probe reads w.active (set by
// Run/Restore, cleared by Checkpoint) so Export is permitted only while the
// worker holds the actor.
func startFsNetWorker(t *testing.T, workerUID string, snapshotFiles []string) *fsNetWorker {
	t.Helper()
	w := &fsNetWorker{base: t.TempDir(), snapshotFiles: snapshotFiles}
	staging, err := workerstaging.NewServer(w.base, workerUID, func(string) bool { return w.active })
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.UnaryInterceptor(staging.LifecycleInterceptor()))
	wspb.RegisterWorkerStagingServer(server, staging)
	ateworkerpb.RegisterWorkerServer(server, w)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	w.staging = wspb.NewWorkerStagingClient(conn)
	w.lifecycle = ateworkerpb.NewWorkerClient(conn)
	return w
}

// forgeGeneration returns a copy of ref with a different generation string, the
// stale/forged ref a Checkpoint would carry if it read a generation the worker
// never committed. actor_uid and worker_uid are kept so the request reaches the
// worker and is refused on the generation, not on identity.
func forgeGeneration(ref *wspb.ActivationRef, generation string) *wspb.ActivationRef {
	return &wspb.ActivationRef{
		ActorUid:   ref.GetActorUid(),
		Generation: generation,
		WorkerUid:  ref.GetWorkerUid(),
	}
}

// writeManifestBeside writes the snapshot manifest into an existing custody dir
// without touching the files already pulled there (writeLocalSnapshot would
// re-create a flat layout and cannot preserve the pulled fs/ subdirs).
func writeManifestBeside(t *testing.T, dir string, rec sandboxAssetsRecord) {
	t.Helper()
	manifest, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("marshaling manifest: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, sandboxManifestName), manifest, 0o600); err != nil {
		t.Fatalf("writing manifest: %v", err)
	}
}
