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
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
	"go.ate.dev/substrate/cmd/atelet/internal/filecache"
	"go.ate.dev/substrate/cmd/atelet/internal/snapshottracker"
	"go.ate.dev/substrate/internal/imagecache"
	"go.ate.dev/substrate/internal/nodepath"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	wspb "go.ate.dev/substrate/internal/proto/workerstagingpb"
	"k8s.io/utils/clock"
)

// This file pins the CALL SITE in Atelet.Restore, not just the helpers.
// The helper tests in network_local_restore_test.go
// let three call-site variants SURVIVE: turning the copy block off
// (`if networkLocalStage {` -> `if false {`), dropping
// `checkpointDir = ateletpath.RestoreStateDir(actorUID)`, and reverting
// `PreserveRestoreDir` to `directLocal`. A test must drive the real s.Restore
// for a NETWORK atelet so a revert of the fix at the call site fails.
//
// The restore reaches a real worker.RestoreWorkload: the lifecycle RPC is
// served by a unix-socket fakeWorker (which records the RestoreDir and
// PreserveRestoreDir the request carried and reads the files out of the node's
// RestoreDir), and the network staging push is served by a real
// workerstaging.Server (startNetWorker) whose guest filesystem is SEPARATE from
// the node's -- so (c) proves the checkpoint reached the guest over the
// transport, not off a shared disk. nodepath.ActorsDir, the image-cache root
// and nodeBase are aligned under one base so the staging client's
// node-side reads (restore-state, image layers) resolve to what the restore
// wrote.

// alignedNodeBase overrides nodepath.ActorsDir to <base>/actors and returns the
// base, so a test can point nodeBase and the image-cache at the same
// root the restore writes its actor dirs under.
func alignedNodeBase(t *testing.T) string {
	t.Helper()
	base := t.TempDir()
	orig, origSnapshots := nodepath.ActorsDir, nodepath.SnapshotsDir
	nodepath.ActorsDir = filepath.Join(base, "actors")
	nodepath.SnapshotsDir = filepath.Join(base, "snapshots")
	t.Cleanup(func() { nodepath.ActorsDir, nodepath.SnapshotsDir = orig, origSnapshots })
	return base
}

// TestNetworkRestoreCallSiteStagesLocalSnapshot drives the REAL Atelet.Restore
// for a network atelet with a usable node-local snapshot (directLocal true) and
// asserts, at the worker call, (a) RestoreDir is restore-state, (b)
// PreserveRestoreDir is false, (c) the guest filesystem served by the real
// staging server holds exactly the manifest's files, sha256-equal to the node's,
// and (d) the restore's op kind stayed local (metrics).
func TestNetworkRestoreCallSiteStagesLocalSnapshot(t *testing.T) {
	const (
		atespace     = "ate-demo"
		actorName    = "counter"
		actorUID     = "01234567-89ab-cdef-0123-456789abcdef"
		workerPodUID = "5f1c0d2e-3a4b-4c5d-8e9f-0a1b2c3d4e5f"
		snapshotName = "pause-snap-1"
	)

	base := alignedNodeBase(t)

	// A MULTI-FILE snapshot with distinct bytes per file: the restore must stage
	// every listed file into the guest, byte-for-byte. One file would not catch a
	// defect that stages only the first listed file (or truncates a later one),
	// so the manifest carries two with different contents.
	snapshotContents := map[string]string{"checkpoint.img": "guest-memory", "pages.img": "page-table-bytes"}

	// Lifecycle worker (unix socket): records the RestoreWorkload request and
	// reads the restored files out of the node's RestoreDir.
	worker := &fakeWorker{snapshotFiles: snapshotContents}
	serveFakeWorker(t, worker)

	// Real staging worker: a SEPARATE guest filesystem reached over the real
	// workerstaging transport.
	netW := startNetWorker(t, workerPodUID, "")

	// A ZERO-LAYER pause image, so prepareOCIBundles writes an overlay spec with
	// no image layers. The staging client's canonical-layer check
	// (internal/workerstaging/client.go: layer must equal
	// nodepath.BasePath/image-cache/...) then never runs -- that path is pinned
	// to the const BasePath, which a test cannot write under. The restore path,
	// the local-snapshot copy into restore-state, the RestoreDir/PreserveRestoreDir
	// wiring and the actor-state staging into the guest are all exercised; only
	// the image-layer leg is elided, and it is irrelevant to this fix.
	host := imageVolumeTestRegistry(t)
	pinnedImage := pushTestImage(t, host+"/actor:v1")
	// Upstream #2798 dropped the legacy bare "runsc" asset: a gVisor record now
	// carries the "gvisor" release tarball. Use the shared tarball fixture.
	release := newGVisorTestRelease(t)
	sandboxAssets := &sandboxAssetsRecord{
		SandboxClass: "gvisor",
		Assets:       map[string]assetEntry{gvisorAssetName: release.entry},
	}
	spec := &ateletpb.WorkloadSpec{
		Containers: []*ateletpb.Container{{Name: "app", Image: pinnedImage, Command: []string{"/bin/app"}}},
	}
	manifestRec := sandboxAssetsRecord{
		SandboxClass:  "gvisor",
		Assets:        map[string]assetEntry{gvisorAssetName: release.entry},
		SnapshotFiles: []string{"checkpoint.img", "pages.img"},
	}

	imageStore, err := imagecache.New(filepath.Join(base, "image-cache"))
	if err != nil {
		t.Fatalf("imagecache.New: %v", err)
	}
	assetCache, err := filecache.New(filepath.Join(base, "sandbox-asset-cache"))
	if err != nil {
		t.Fatalf("filecache.New: %v", err)
	}
	s := &Atelet{
		checkpointTimeout: defaultCheckpointTimeout,
		workerDialer:      newWorkerDialer(1), // network nil -> lifecycle dials the unix fakeWorker
		imageCache:        imageStore,
		sandboxAssetCache: assetCache,
		anonGCSClient:     fakeObjectStorage{data: release.tarball},
		systemInfoVolumes: newSystemInfoVolumeRefresher(nil, nil),
		snapshots:         snapshottracker.New(t.Context(), nil, clock.RealClock{}),
		runtimes:          newRuntimeIndex(),
		network:           &nodeWorkerNetwork{},
		generations:       newTestGenerations(t, nodeIdentity{NodeName: "node-a", NodeUID: "node-a-incarnation"}),
		nodeBase:          base,
		staging: stagingFunc(func(_ context.Context, uid string) (wspb.WorkerStagingClient, error) {
			if uid != workerPodUID {
				t.Fatalf("staging dial for an unexpected worker %q", uid)
			}
			return netW.staging, nil
		}),
	}
	advertiseRuntime(t, s, sandboxAssets)

	// Seed the node-local snapshot so readLocalSnapshot succeeds and directLocal
	// is true -- exactly as TestRestorePrefersLocal's "usable local copy" case.
	localDir := ateletpath.LocalSnapshotDir(actorUID, snapshotName)
	writeLocalSnapshot(t, localDir, manifestRec, snapshotContents)

	if _, err := s.Restore(t.Context(), &ateletpb.RestoreRequest{
		Atespace: atespace, ActorName: actorName, ActorUid: actorUID,
		ActorTemplateAtespace: "default", ActorTemplateName: "counter",
		WorkerPodUid:   workerPodUID,
		SandboxRuntime: runtimeFor(t, sandboxAssets),
		Spec:           spec,
		Snapshot:       lifecycleSnapshot(actorUID, snapshotName, 1, ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY),
	}); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	req := worker.actorDirs["RestoreWorkload"]
	if req == nil {
		t.Fatal("RestoreWorkload was never called")
	}
	// (a) RestoreDir points at restore-state, not local-checkpoint.
	if got, want := req.GetRestoreDir(), ateletpath.RestoreStateDir(actorUID); got != want {
		t.Errorf("RestoreWorkload RestoreDir = %q, want %q (restore-state, the staged dir)", got, want)
	}
	// (b) PreserveRestoreDir is false for the network-staged case.
	if worker.preserveRestoreDir {
		t.Errorf("RestoreWorkload PreserveRestoreDir = true, want false for a network-staged restore")
	}
	// (c) the guest filesystem (separate from the node's) holds EVERY one of the
	// manifest's files, each byte-identical to the node's local snapshot. Looping
	// over all listed files (not just the first) is what catches a defect that
	// stages only the leading file or truncates a later one -- the multi-file
	// byte-continuity property the folded helper test used to cover.
	guest := guestRestoreState(netW.base, actorUID)
	for _, name := range manifestRec.SnapshotFiles {
		wantBytes, err := os.ReadFile(filepath.Join(localDir, name))
		if err != nil {
			t.Fatal(err)
		}
		gotBytes, err := os.ReadFile(filepath.Join(guest, name))
		if err != nil {
			t.Fatalf("guest restore-state missing %q: %v", name, err)
		}
		if sha256.Sum256(gotBytes) != sha256.Sum256(wantBytes) {
			t.Errorf("guest %q sha256 != node local snapshot's", name)
		}
	}
	entries, err := os.ReadDir(guest)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != len(manifestRec.SnapshotFiles) {
		t.Errorf("guest restore-state has %d entries, want exactly the %d manifest files", len(entries), len(manifestRec.SnapshotFiles))
	}
}

// TestNetworkRestoreCallSiteFailsClosedOnCopyError drives the real Atelet.Restore
// for a network atelet and proves the fail-closed property AT THE CALL SITE: when
// the local-snapshot copy into restore-state fails, Restore returns an error, the
// worker RestoreWorkload is NEVER called, and the partial restore-state is left
// clean. The copy is forced to fail by making the link step return a non-EXDEV
// error (so copyLocalCheckpoint does not fall back to a byte copy), simulating a
// listed file that cannot be staged.
func TestNetworkRestoreCallSiteFailsClosedOnCopyError(t *testing.T) {
	const (
		atespace     = "ate-demo"
		actorName    = "counter"
		actorUID     = "01234567-89ab-cdef-0123-456789abcdef"
		workerPodUID = "5f1c0d2e-3a4b-4c5d-8e9f-0a1b2c3d4e5f"
		snapshotName = "pause-snap-1"
	)
	base := alignedNodeBase(t)

	worker := &fakeWorker{snapshotFiles: map[string]string{"checkpoint.img": "guest-memory"}}
	serveFakeWorker(t, worker)
	netW := startNetWorker(t, workerPodUID, "")

	host := imageVolumeTestRegistry(t)
	pinnedImage := pushTestImage(t, host+"/actor:v1")
	// Upstream #2798 dropped the legacy bare "runsc" asset: a gVisor record now
	// carries the "gvisor" release tarball. Use the shared tarball fixture.
	release := newGVisorTestRelease(t)
	sandboxAssets := &sandboxAssetsRecord{
		SandboxClass: "gvisor",
		Assets:       map[string]assetEntry{gvisorAssetName: release.entry},
	}
	spec := &ateletpb.WorkloadSpec{
		Containers: []*ateletpb.Container{{Name: "app", Image: pinnedImage, Command: []string{"/bin/app"}}},
	}
	manifestRec := sandboxAssetsRecord{
		SandboxClass:  "gvisor",
		Assets:        map[string]assetEntry{gvisorAssetName: release.entry},
		SnapshotFiles: []string{"checkpoint.img"},
	}

	imageStore, err := imagecache.New(filepath.Join(base, "image-cache"))
	if err != nil {
		t.Fatalf("imagecache.New: %v", err)
	}
	assetCache, err := filecache.New(filepath.Join(base, "sandbox-asset-cache"))
	if err != nil {
		t.Fatalf("filecache.New: %v", err)
	}
	s := &Atelet{
		checkpointTimeout: defaultCheckpointTimeout,
		workerDialer:      newWorkerDialer(1),
		imageCache:        imageStore,
		sandboxAssetCache: assetCache,
		anonGCSClient:     fakeObjectStorage{data: release.tarball},
		systemInfoVolumes: newSystemInfoVolumeRefresher(nil, nil),
		snapshots:         snapshottracker.New(t.Context(), nil, clock.RealClock{}),
		runtimes:          newRuntimeIndex(),
		network:           &nodeWorkerNetwork{},
		generations:       newTestGenerations(t, nodeIdentity{NodeName: "node-a", NodeUID: "node-a-incarnation"}),
		nodeBase:          base,
		staging: stagingFunc(func(_ context.Context, uid string) (wspb.WorkerStagingClient, error) {
			return netW.staging, nil
		}),
	}
	advertiseRuntime(t, s, sandboxAssets)

	localDir := ateletpath.LocalSnapshotDir(actorUID, snapshotName)
	writeLocalSnapshot(t, localDir, manifestRec, map[string]string{"checkpoint.img": "guest-memory"})

	// Force the copy to fail: linkFile returns a non-EXDEV error, so
	// copyLocalCheckpoint does not fall back to a byte copy and returns the error.
	orig := linkFile
	linkFile = func(*os.Root, string, string) error { return fmt.Errorf("injected link failure") }
	t.Cleanup(func() { linkFile = orig })

	_, err = s.Restore(t.Context(), &ateletpb.RestoreRequest{
		Atespace: atespace, ActorName: actorName, ActorUid: actorUID,
		ActorTemplateAtespace: "default", ActorTemplateName: "counter",
		WorkerPodUid:   workerPodUID,
		SandboxRuntime: runtimeFor(t, sandboxAssets),
		Spec:           spec,
		Snapshot:       lifecycleSnapshot(actorUID, snapshotName, 1, ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY),
	})
	// Restore returned on the copy error, but the file cache runs a sandbox asset
	// fetch without the caller's cancellation, so it can still be writing into the
	// cache's tmp dir. Fetching the same assets again joins that fetch, so the temp
	// dir is quiet by the time the test cleanup removes it.
	runtimeRec, recErr := s.advertisedRuntime(t.Context(), runtimeFor(t, sandboxAssets))
	if recErr != nil {
		t.Fatalf("advertisedRuntime: %v", recErr)
	}
	if _, joinErr := s.ensureSandboxAssets(t.Context(), runtimeRec, t.TempDir()); joinErr != nil {
		t.Fatalf("ensureSandboxAssets: %v", joinErr)
	}
	if err == nil {
		t.Fatal("Restore returned nil on a copy failure; it must fail closed")
	}
	if worker.actorDirs["RestoreWorkload"] != nil {
		t.Error("RestoreWorkload was called after a failed local-snapshot copy; the restore must fail closed before the worker RPC")
	}
	// The partial restore-state was cleaned (best-effort RemoveAll): no
	// checkpoint.img left behind on the node.
	if _, statErr := os.Stat(filepath.Join(ateletpath.RestoreStateDir(actorUID), "checkpoint.img")); !os.IsNotExist(statErr) {
		t.Errorf("partial restore-state left behind after a failed copy (stat err = %v)", statErr)
	}
}
