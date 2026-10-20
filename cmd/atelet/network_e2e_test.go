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
	"strings"
	"sync/atomic"
	"testing"

	"go.ate.dev/substrate/internal/imagecache"
	"go.ate.dev/substrate/internal/nodepath"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	wspb "go.ate.dev/substrate/internal/proto/workerstagingpb"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/workerstaging"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// netWorker is an in-process stand-in for a separate-guest worker Pod: a real
// workerstaging.Server (so the staging push/pull and its committed-activation
// ownership checks are the production code) plus a tiny lifecycle runtime that
// writes a checkpoint on CheckpointWorkload. Its filesystem root (base) is
// SEPARATE from the node's, so anything the atelet reads from node custody must
// have been pulled over the transport, not read off a shared disk.
type netWorker struct {
	ateworkerpb.UnimplementedWorkerServer
	base            string
	workerUID       string
	active          atomic.Bool
	checkpointBytes string
	snapshotFiles   []string // the checkpoint file set the fake runtime reports; default ["memory"]
	staging         wspb.WorkerStagingClient
	lifecycle       ateworkerpb.WorkerClient
}

// stagingFunc adapts a function to the stagingConnector the Atelet uses, so a test can supply
// an in-process staging client for a worker UID without an mTLS dial.
type stagingFunc func(ctx context.Context, workerUID string) (wspb.WorkerStagingClient, error)

func (f stagingFunc) StagingClient(ctx context.Context, workerUID string) (wspb.WorkerStagingClient, error) {
	return f(ctx, workerUID)
}

func (w *netWorker) RunWorkload(context.Context, *ateworkerpb.RunWorkloadRequest) (*ateworkerpb.RunWorkloadResponse, error) {
	w.active.Store(true)
	return &ateworkerpb.RunWorkloadResponse{}, nil
}

func (w *netWorker) RestoreWorkload(context.Context, *ateworkerpb.RestoreWorkloadRequest) (*ateworkerpb.RestoreWorkloadResponse, error) {
	w.active.Store(true)
	return &ateworkerpb.RestoreWorkloadResponse{}, nil
}

func (w *netWorker) CheckpointWorkload(_ context.Context, req *ateworkerpb.CheckpointWorkloadRequest) (*ateworkerpb.CheckpointWorkloadResponse, error) {
	dir := filepath.Join(guestSnapshots(w.base, req.GetActorUid()), "checkpoint-state")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	files := w.snapshotFiles
	if len(files) == 0 {
		files = []string{"memory"}
	}
	for _, name := range files {
		contents := w.checkpointBytes
		if name != "memory" {
			contents = name + " contents"
		}
		if err := os.WriteFile(filepath.Join(dir, name), []byte(contents), 0o600); err != nil {
			return nil, err
		}
	}
	w.active.Store(false)
	return &ateworkerpb.CheckpointWorkloadResponse{SnapshotFiles: files}, nil
}

// startNetWorker stands up a fake worker over an in-process TCP listener. The
// staging server's LifecycleInterceptor wraps BOTH service handlers, so a
// lifecycle CheckpointWorkload records its membership against the worker's
// committed activation exactly as in production. The returned lifecycle client
// shares the same connection, so it passes through that interceptor.
func startNetWorker(t *testing.T, workerUID, checkpointBytes string) *netWorker {
	t.Helper()
	w := &netWorker{base: t.TempDir(), workerUID: workerUID, checkpointBytes: checkpointBytes}
	staging, err := workerstaging.NewServer(w.base, workerUID, func(string) bool { return w.active.Load() })
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

// layoutNode writes the node-side asset layout the staging client reads for an
// actor: one image layer, a gvisor runtime, an OCI bundle with an overlay spec,
// and (optionally) a restore-state directory holding a checkpoint to restore.
// Files live under base; the overlay spec and the returned runtime-path map use
// the CANONICAL /var/lib/ate paths the staging client validates against, so the
// bytes are read from base while the identities stay canonical.
func layoutNode(t *testing.T, base, actorUID string, restoreState map[string]string) map[string]string {
	t.Helper()
	layerID := strings.Repeat("a", 64)
	layer := filepath.Join("image-cache", "layers", "sha256", layerID)
	writeFileAt(t, base, filepath.Join(layer, "fs", "bin", "sample"), "image bytes")
	writeFileAt(t, base, filepath.Join(layer, "whiteouts.json"), `{"whiteouts":[],"opaqueDirs":[]}`)
	// The gVisor release is staged in the actor's OWN per-actor sandbox-assets
	// dir under the plain asset name "gvisor" (upstream #2699 removed the shared
	// node-wide static-files cache and its digest-keyed ids), runsc plus its
	// gvisor-bin/ helper. base stands in for /var/lib/ate, so the physical tree
	// mirrors the canonical path the staging client reads and the worker runs.
	// A test that points nodepath.ActorsDir at a temp root (the same-node local-restore tests) has
	// ActorSandboxAssetsDir under base, not under /var/lib/ate: take it relative to base then. Taking it
	// relative to BasePath in that case yields a ../../.. path that points at the right place only when the
	// temp directory happens to be exactly as deep as /var/lib/ate, so those tests passed with the default
	// TMPDIR and failed with any other (found by the fresh-clone run, which sets its own TMPDIR).
	assetsDir := nodepath.ActorSandboxAssetsDir(actorUID)
	relBase := nodepath.BasePath
	if strings.HasPrefix(assetsDir, filepath.Clean(base)+string(filepath.Separator)) {
		relBase = base
	}
	assetsRel, err := filepath.Rel(relBase, assetsDir)
	if err != nil {
		t.Fatal(err)
	}
	writeFileAt(t, base, filepath.Join(assetsRel, "gvisor", "runsc"), "runtime binary")
	writeFileAt(t, base, filepath.Join(assetsRel, "gvisor", "gvisor-bin", "runsc-gofer"), "helper binary")
	actor := filepath.Join("actors", actorUID)
	writeFileAt(t, base, filepath.Join(actor, "bundles", "main", "config.json"), `{"root":{"path":"rootfs"}}`)
	if err := imagecache.WriteSpec(filepath.Join(base, actor, "bundles", "main"), &imagecache.OverlaySpec{
		Layers: []string{filepath.Join(nodepath.BasePath, "image-cache", "layers", "sha256", layerID)},
	}); err != nil {
		t.Fatal(err)
	}
	for name, contents := range restoreState {
		writeFileAt(t, base, filepath.Join(filepath.Base(nodepath.SnapshotsDir), actorUID, "restore-state", name), contents)
	}
	return map[string]string{"gvisor": filepath.Join(nodepath.ActorSandboxAssetsDir(actorUID), "gvisor", "runsc")}
}

func writeFileAt(t *testing.T, base, rel, contents string) {
	t.Helper()
	full := filepath.Join(base, rel)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(contents), 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestNetworkCheckpointSuspendOnAResumeOnBWithByteContinuity is the in-process
// end-to-end proof of the client-side network-worker path: an actor is staged
// and checkpointed on worker A over the real workerstaging transport; the
// checkpoint is pulled into node custody; worker A's guest is then REMOVED
// entirely; the checkpoint survives in node custody; and it is staged into a
// FRESH worker B for restore, where the exact bytes the checkpoint produced
// arrive in worker B's restore-state. Byte continuity across two independent
// guest filesystems, with the source guest gone, is the whole claim.
func TestNetworkCheckpointSuspendOnAResumeOnBWithByteContinuity(t *testing.T) {
	ctx := context.Background()
	const checkpointBytes = "nested-tree memory image bytes"

	nodeBase := t.TempDir()
	actor := resources.ActorRef{Atespace: "team", Name: "counter"}
	actorUID := "11111111-1111-1111-1111-111111111111"
	workerA := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	workerB := "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"

	wa := startNetWorker(t, workerA, checkpointBytes)
	wb := startNetWorker(t, workerB, "")

	s := &Atelet{
		checkpointTimeout: defaultCheckpointTimeout,
		network:           &nodeWorkerNetwork{},
		generations:       newTestGenerations(t, nodeIdentity{NodeName: "node-a", NodeUID: "node-a-incarnation"}),
		nodeBase:          nodeBase,
		staging: stagingFunc(func(_ context.Context, workerUID string) (wspb.WorkerStagingClient, error) {
			switch workerUID {
			case workerA:
				return wa.staging, nil
			case workerB:
				return wb.staging, nil
			}
			t.Fatalf("staging dial for an unexpected worker %q", workerUID)
			return nil, nil
		}),
	}

	// --- Stage + run on worker A (push image/runtime/actor-state in, then run) ---
	refA, err := s.stageNetworkActivation(ctx, actor, actorUID, workerA, layoutNode(t, nodeBase, actorUID, nil))
	if err != nil {
		t.Fatalf("stage on worker A: %v", err)
	}
	if refA == nil {
		t.Fatal("stage on worker A returned no activation ref")
	}
	runReq := &ateworkerpb.RunWorkloadRequest{Atespace: actor.Atespace, ActorName: actor.Name, ActorUid: actorUID}
	if _, err := wa.lifecycle.RunWorkload(ctx, runReq); err != nil {
		t.Fatalf("run on worker A: %v", err)
	}

	// --- Checkpoint on worker A THROUGH the staging LifecycleInterceptor, so the
	// checkpoint membership is recorded against A's committed activation. ---
	checkpointResp, err := wa.lifecycle.CheckpointWorkload(ctx,
		&ateworkerpb.CheckpointWorkloadRequest{Atespace: actor.Atespace, ActorName: actor.Name, ActorUid: actorUID})
	if err != nil {
		t.Fatalf("checkpoint on worker A: %v", err)
	}

	// --- Pull the checkpoint OUT of guest A into node custody, keyed by the
	// re-read generation (networkActivation), BEFORE guest A is removed. ---
	pullRef, err := s.networkActivation(actor, actorUID, workerA)
	if err != nil {
		t.Fatalf("networkActivation for the pull: %v", err)
	}
	if pullRef.GetGeneration() != refA.GetGeneration() {
		t.Fatalf("pull generation %q differs from the staged generation %q", pullRef.GetGeneration(), refA.GetGeneration())
	}
	custody := filepath.Join(nodeBase, "node-custody")
	if err := s.receiveNetworkCheckpoint(ctx, pullRef, checkpointResp.GetSnapshotFiles(), custody); err != nil {
		t.Fatalf("pull checkpoint into node custody: %v", err)
	}

	// --- REMOVE guest A entirely. Node custody must survive. ---
	if err := os.RemoveAll(wa.base); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(custody, "memory"))
	if err != nil || string(got) != checkpointBytes {
		t.Fatalf("node custody after guest A removal = %q, %v; want the checkpoint bytes", got, err)
	}

	// --- Restore on a FRESH worker B: place the custody checkpoint into the
	// node's restore-state for the actor, then stage into guest B. The actor
	// state staging carries restore-state into B, so the exact checkpoint bytes
	// arrive on B's filesystem. ---
	refB, err := s.stageNetworkActivation(ctx, actor, actorUID, workerB,
		layoutNode(t, nodeBase, actorUID, map[string]string{"memory": string(got)}))
	if err != nil {
		t.Fatalf("stage on worker B: %v", err)
	}
	if refB == nil || refB.GetGeneration() == refA.GetGeneration() {
		t.Fatalf("worker B activation %v must be a fresh generation, not A's %q", refB, refA.GetGeneration())
	}
	if _, err := wb.lifecycle.RestoreWorkload(ctx,
		&ateworkerpb.RestoreWorkloadRequest{Atespace: actor.Atespace, ActorName: actor.Name, ActorUid: actorUID}); err != nil {
		t.Fatalf("restore on worker B: %v", err)
	}

	// Byte continuity: worker B's restore-state holds exactly the bytes worker
	// A's checkpoint produced, delivered across two independent guest
	// filesystems with guest A already gone.
	onB, err := os.ReadFile(filepath.Join(guestRestoreState(wb.base, actorUID), "memory"))
	if err != nil || string(onB) != checkpointBytes {
		t.Fatalf("worker B restore-state memory = %q, %v; want byte continuity with worker A's checkpoint", onB, err)
	}
}

// TestNetworkCheckpointPullMembershipIsTheCompleteSetNotAppended proves the
// Checkpoint handler pulls EXACTLY the complete reported set (SnapshotFiles),
// and that appending DataSnapshotFiles -- a subset of SnapshotFiles -- would be
// a bug, because the resulting duplicate membership is rejected by Export.
//
// This is the regression guard for a defect the earlier commit carried: the
// handler appended sandboxRec.DataSnapshotFiles to sandboxRec.SnapshotFiles,
// but DataSnapshotFiles is a subset of SnapshotFiles (validateDataSnapshotFiles),
// so a checkpoint with any data files produced a membership with duplicates and
// the pull failed with "invalid checkpoint membership". The fix pulls
// SnapshotFiles alone.
func TestNetworkCheckpointPullMembershipIsTheCompleteSetNotAppended(t *testing.T) {
	ctx := context.Background()
	nodeBase := t.TempDir()
	actor := resources.ActorRef{Atespace: "team", Name: "counter"}
	actorUID := "11111111-1111-1111-1111-111111111111"
	workerUID := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"

	w := startNetWorker(t, workerUID, "mem-bytes")
	// SnapshotFiles is the COMPLETE set and includes the data file; a real
	// sandboxRec would carry DataSnapshotFiles=["data.tar"] as a subset of it.
	w.snapshotFiles = []string{"memory", "data.tar"}

	s := &Atelet{
		checkpointTimeout: defaultCheckpointTimeout,
		network:           &nodeWorkerNetwork{},
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

	// The complete reported set (what the handler now passes) pulls cleanly.
	custody := filepath.Join(nodeBase, "custody-ok")
	if err := s.receiveNetworkCheckpoint(ctx, ref, resp.GetSnapshotFiles(), custody); err != nil {
		t.Fatalf("pull of the complete reported set failed: %v", err)
	}
	for _, name := range resp.GetSnapshotFiles() {
		if _, err := os.Stat(filepath.Join(custody, name)); err != nil {
			t.Fatalf("custody is missing reported file %q: %v", name, err)
		}
	}

	// The OLD buggy membership (SnapshotFiles with a data-file appended ->
	// duplicate) must be rejected, which is exactly why the handler must not
	// append DataSnapshotFiles.
	duplicated := append(append([]string(nil), resp.GetSnapshotFiles()...), "data.tar")
	if err := s.receiveNetworkCheckpoint(ctx, ref, duplicated, filepath.Join(nodeBase, "custody-dup")); err == nil {
		t.Fatal("a duplicate membership was accepted; Export must reject it, which is why appending DataSnapshotFiles was a bug")
	}
}
