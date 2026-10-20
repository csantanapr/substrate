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
	"encoding/json"
	"errors"
	"strings"
	"testing"

	atev1 "go.ate.dev/substrate/api/infra/k8s/ate/v1"
	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
	"go.ate.dev/substrate/cmd/atelet/internal/snapshottracker"
	"go.ate.dev/substrate/internal/ateerrors"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"k8s.io/utils/clock"
)

// TestCheckNestedRuntimeCompatible pins the comparator directly, both ways, at
// every enabled/packet-socket-write combination. nil (not nested) must match a
// disabled spec and differ from an enabled one, symmetrically.
func TestCheckNestedRuntimeCompatible(t *testing.T) {
	nc := func(enabled, psw bool) *nestedContainersManifest {
		return &nestedContainersManifest{Enabled: enabled, PacketSocketWrite: psw}
	}
	spec := func(enabled, psw bool) *ateletpb.NestedContainers {
		return &ateletpb.NestedContainers{Enabled: enabled, PacketSocketWrite: psw}
	}
	for _, tc := range []struct {
		name     string
		recorded *nestedContainersManifest
		spec     *ateletpb.NestedContainers
		wantErr  bool
	}{
		{"both nil/plain match", nil, nil, false},
		{"nil recorded vs disabled spec match", nil, spec(false, false), false},
		{"nested recorded vs nested spec match", nc(true, false), spec(true, false), false},
		{"nested+write both match", nc(true, true), spec(true, true), false},
		{"nested recorded vs plain spec (nil) differ", nc(true, false), nil, true},
		{"plain recorded (nil) vs nested spec differ", nil, spec(true, false), true},
		{"enabled vs disabled differ", nc(true, false), spec(false, false), true},
		{"write mismatch differs", nc(true, false), spec(true, true), true},
		{"write mismatch reverse differs", nc(true, true), spec(true, false), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := checkNestedRuntimeCompatible(tc.recorded, tc.spec)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("checkNestedRuntimeCompatible = nil, want an error")
				}
				if !errors.Is(err, ateerrors.ErrSnapshotRuntimeMismatch) {
					t.Errorf("error %v is not ErrSnapshotRuntimeMismatch", err)
				}
				return
			}
			if err != nil {
				t.Errorf("checkNestedRuntimeCompatible = %v, want nil", err)
			}
		})
	}
}

// nestedRuntimeHarness sets up an Atelet with a fake worker and returns a function
// that drives Run with the given nested spec, so each case starts from a clean
// actor with a known effective opt-in.
type nestedRuntimeHarness struct {
	s             *Atelet
	worker        *fakeWorker
	sandboxAssets *sandboxAssetsRecord
	pinnedImage   string
}

func newNestedRuntimeHarness(t *testing.T) *nestedRuntimeHarness {
	t.Helper()
	useTempNodeDirs(t)
	worker := &fakeWorker{snapshotFiles: map[string]string{"checkpoint.img": "guest-memory"}}
	serveFakeWorker(t, worker)

	host := imageVolumeTestRegistry(t)
	pinnedImage := pushTestImage(t, host+"/actor:v1", singleFileLayer(t, "bin/app", "app"))

	// Upstream #2798 dropped the legacy bare "runsc" asset: a gVisor
	// SandboxConfig now carries the "gvisor" release TARBALL, which atelet
	// extracts (runsc + gvisor-bin/). Match upstream's test migration: a real
	// tarball fixture served by the fake object store, named by gvisorAssetName.
	release := newGVisorTestRelease(t)
	s := &Atelet{
		checkpointTimeout: defaultCheckpointTimeout,
		workerDialer:      newWorkerDialer(1),
		imageCache:        newImageVolumeStore(t),
		anonGCSClient:     fakeObjectStorage{data: release.tarball},
		systemInfoVolumes: newSystemInfoVolumeRefresher(nil, nil),
		snapshots:         snapshottracker.New(t.Context(), nil, clock.RealClock{}),
		// Upstream #2699 made ensureSandboxAssets stage from a per-node asset
		// cache; without it the Run path nil-derefs. Match lifecycle_test.
		sandboxAssetCache: newTestAssetCache(t),
	}
	sandboxAssets := &sandboxAssetsRecord{
		SandboxClass: string(atev1.SandboxClassGvisor),
		Assets:       map[string]assetEntry{gvisorAssetName: release.entry},
	}
	// #2788: Run/Restore now resolve the runtime to stage from the request's
	// sandbox_runtime by looking its bundle hash up in the node's runtime
	// index, which a worker advertises as the syncer plans it. Advertise the
	// runtime these assets name so the hash in runtimeFor(t, sandboxAssets)
	// resolves; without it Run nil-derefs on an empty index.
	advertiseRuntime(t, s, sandboxAssets)
	return &nestedRuntimeHarness{s: s, worker: worker, sandboxAssets: sandboxAssets, pinnedImage: pinnedImage}
}

const (
	nestedAtespace     = "ate-demo"
	nestedActorName    = "counter"
	nestedActorUID     = "01234567-89ab-cdef-0123-456789abcdef"
	nestedWorkerPodUID = "5f1c0d2e-3a4b-4c5d-8e9f-0a1b2c3d4e5f"
)

func (h *nestedRuntimeHarness) spec(nested *ateletpb.NestedContainers) *ateletpb.WorkloadSpec {
	return &ateletpb.WorkloadSpec{
		Containers:       []*ateletpb.Container{{Name: "app", Image: h.pinnedImage, Command: []string{"/bin/app"}}},
		NestedContainers: nested,
	}
}

func (h *nestedRuntimeHarness) run(ctx context.Context, t *testing.T, nested *ateletpb.NestedContainers) {
	t.Helper()
	if _, err := h.s.Run(ctx, &ateletpb.RunRequest{
		Atespace: nestedAtespace, ActorName: nestedActorName, ActorUid: nestedActorUID,
		ActorTemplateAtespace: "default", ActorTemplateName: "counter",
		WorkerPodUid: nestedWorkerPodUID,
		// #2788 made sandbox_runtime required on Run: atelet reads the
		// sandbox_bundle_hash attribute off it to resolve the runtime to
		// stage. runtimeFor builds the runtime ate-api-server would send for an
		// actor placed on the worker that advertises these assets.
		SandboxRuntime: runtimeFor(t, h.sandboxAssets),
		Spec:           h.spec(nested),
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func (h *nestedRuntimeHarness) checkpointMemory(ctx context.Context, t *testing.T, nested *ateletpb.NestedContainers, snapName string, gen int32) {
	t.Helper()
	if _, err := h.s.Checkpoint(ctx, &ateletpb.CheckpointRequest{
		Atespace: nestedAtespace, ActorName: nestedActorName, ActorUid: nestedActorUID,
		ActorTemplateAtespace: "default", ActorTemplateName: "counter",
		WorkerPodUid: nestedWorkerPodUID,
		Spec:         h.spec(nested),
		Snapshot:     lifecycleSnapshot(nestedActorUID, snapName, gen, ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY),
		UploadConfig: deferredUploadConfig(),
	}); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
}

func (h *nestedRuntimeHarness) restoreMemory(ctx context.Context, t *testing.T, nested *ateletpb.NestedContainers, snapName string, gen int32) error {
	t.Helper()
	_, err := h.s.Restore(ctx, &ateletpb.RestoreRequest{
		Atespace: nestedAtespace, ActorName: nestedActorName, ActorUid: nestedActorUID,
		ActorTemplateAtespace: "default", ActorTemplateName: "counter",
		WorkerPodUid: nestedWorkerPodUID,
		// #2788 made sandbox_runtime required on Restore too; see run().
		SandboxRuntime: runtimeFor(t, h.sandboxAssets),
		Spec:           h.spec(nested),
		Snapshot:       lifecycleSnapshot(nestedActorUID, snapName, gen, ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY),
	})
	return err
}

var (
	nestedEnabled = &ateletpb.NestedContainers{Enabled: true}
	nestedPSW     = &ateletpb.NestedContainers{Enabled: true, PacketSocketWrite: true}
)

// TestNestedRuntime_PersistedFlagsReadBack is part (i) of the three-part proof: the
// effective nested flags are persisted at checkpoint and can be read back from
// the local snapshot manifest.
func TestNestedRuntime_PersistedFlagsReadBack(t *testing.T) {
	ctx := t.Context()
	for _, tc := range []struct {
		name   string
		nested *ateletpb.NestedContainers
		want   *nestedContainersManifest
	}{
		{"nested enabled", nestedEnabled, &nestedContainersManifest{Enabled: true}},
		{"nested with packet-socket-write", nestedPSW, &nestedContainersManifest{Enabled: true, PacketSocketWrite: true}},
		{"default actor records nothing", nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newNestedRuntimeHarness(t)
			h.run(ctx, t, tc.nested)
			h.checkpointMemory(ctx, t, tc.nested, "snap-1", 1)

			rec, err := readLocalSnapshot(ateletpath.LocalSnapshotDir(nestedActorUID, "snap-1"), "")
			if err != nil {
				t.Fatalf("readLocalSnapshot: %v", err)
			}
			if tc.want == nil {
				if rec.NestedContainers != nil {
					t.Errorf("manifest recorded %+v, want nil for a default actor", rec.NestedContainers)
				}
				return
			}
			if rec.NestedContainers == nil {
				t.Fatalf("manifest recorded no nested flags, want %+v", tc.want)
			}
			if *rec.NestedContainers != *tc.want {
				t.Errorf("manifest nested flags = %+v, want %+v", *rec.NestedContainers, *tc.want)
			}
		})
	}
}

// TestNestedRuntime_RunDoesNotChangeTheAdvertisedRuntime checks that Run records an actor's
// nested flags on its own copy. The advertised runtime is shared by every Run on
// the node, so writing the flags into it would give one actor's opt-in to the
// next one's record (and is a data race when two Runs overlap).
func TestNestedRuntime_RunDoesNotChangeTheAdvertisedRuntime(t *testing.T) {
	h := newNestedRuntimeHarness(t)
	h.run(t.Context(), t, nestedPSW)

	hash, err := bundleHashOf(runtimeFor(t, h.sandboxAssets))
	if err != nil {
		t.Fatalf("bundleHashOf: %v", err)
	}
	indexed, ok := h.s.runtimes.lookup(hash)
	if !ok {
		t.Fatal("the advertised runtime is not in the index")
	}
	if indexed.rec.NestedContainers != nil {
		t.Errorf("the advertised runtime now carries %+v, want nil", indexed.rec.NestedContainers)
	}
	rec, err := readSandboxRecord(nestedActorUID)
	if err != nil {
		t.Fatalf("readSandboxRecord: %v", err)
	}
	if rec.NestedContainers == nil || !rec.NestedContainers.PacketSocketWrite {
		t.Errorf("the actor's record has %+v, want its own packet-socket-write opt-in", rec.NestedContainers)
	}
}

// TestNestedRuntime_MismatchedMemoryRestoreRefused is part (ii): a MEMORY restore whose
// nested opt-in differs from the capture is refused with the typed error, both
// directions, and worker's RestoreWorkload is never called.
func TestNestedRuntime_MismatchedMemoryRestoreRefused(t *testing.T) {
	ctx := t.Context()
	for _, tc := range []struct {
		name    string
		capture *ateletpb.NestedContainers
		restore *ateletpb.NestedContainers
	}{
		{"nested capture, default restore", nestedEnabled, nil},
		{"default capture, nested restore", nil, nestedEnabled},
		{"net-raw capture, packet-socket-write restore", nestedEnabled, nestedPSW},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newNestedRuntimeHarness(t)
			h.run(ctx, t, tc.capture)
			h.checkpointMemory(ctx, t, tc.capture, "snap-1", 1)
			// RestoreWorkload has not been called yet.
			if _, called := h.worker.actorDirs["RestoreWorkload"]; called {
				t.Fatal("RestoreWorkload was called before the restore")
			}
			err := h.restoreMemory(ctx, t, tc.restore, "snap-1", 1)
			if err == nil {
				t.Fatal("Restore = nil, want a refusal")
			}
			if !errors.Is(err, ateerrors.ErrSnapshotRuntimeMismatch) {
				t.Errorf("Restore error %v is not ErrSnapshotRuntimeMismatch", err)
			}
			if _, called := h.worker.actorDirs["RestoreWorkload"]; called {
				t.Error("worker RestoreWorkload was called despite the refusal")
			}
		})
	}
}

// TestNestedRuntime_MatchingMemoryRestoreSucceeds is part (iii): a MEMORY restore whose
// nested opt-in matches the capture succeeds and reaches the worker.
func TestNestedRuntime_MatchingMemoryRestoreSucceeds(t *testing.T) {
	ctx := t.Context()
	for _, tc := range []struct {
		name   string
		nested *ateletpb.NestedContainers
	}{
		{"default", nil},
		{"nested enabled", nestedEnabled},
		{"nested with packet-socket-write", nestedPSW},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newNestedRuntimeHarness(t)
			h.run(ctx, t, tc.nested)
			h.checkpointMemory(ctx, t, tc.nested, "snap-1", 1)
			if err := h.restoreMemory(ctx, t, tc.nested, "snap-1", 1); err != nil {
				t.Fatalf("matching Restore = %v, want success", err)
			}
			if _, called := h.worker.actorDirs["RestoreWorkload"]; !called {
				t.Error("worker RestoreWorkload was not called on a matching restore")
			}
		})
	}
}

// TestNestedRuntime_CaptureAfterPacketSocketWriteRestore checks re-capture: after a
// MEMORY restore that itself used packet-socket-write, a subsequent checkpoint
// records packet-socket-write in the manifest (the record follows the flags the
// sandbox was restored with, not a stale request), so the restored sandbox can
// be re-captured and re-restored under the same flags.
func TestNestedRuntime_CaptureAfterPacketSocketWriteRestore(t *testing.T) {
	ctx := t.Context()
	h := newNestedRuntimeHarness(t)
	// Start nested with packet-socket-write and capture.
	h.run(ctx, t, nestedPSW)
	h.checkpointMemory(ctx, t, nestedPSW, "snap-1", 1)
	// Restore under the same packet-socket-write flags (matches: succeeds).
	if err := h.restoreMemory(ctx, t, nestedPSW, "snap-1", 1); err != nil {
		t.Fatalf("packet-socket-write restore = %v, want success", err)
	}
	// Re-capture the restored sandbox. The on-node record was set from the
	// restore spec, so the new manifest must carry packet-socket-write.
	h.checkpointMemory(ctx, t, nestedPSW, "snap-2", 2)
	rec, err := readLocalSnapshot(ateletpath.LocalSnapshotDir(nestedActorUID, "snap-2"), "")
	if err != nil {
		t.Fatalf("readLocalSnapshot: %v", err)
	}
	if rec.NestedContainers == nil || !rec.NestedContainers.Enabled || !rec.NestedContainers.PacketSocketWrite {
		t.Fatalf("manifest after a packet-socket-write restore = %+v, want {enabled:true packetSocketWrite:true}", rec.NestedContainers)
	}
	// And a restore of that second snapshot under the same flags succeeds.
	if err := h.restoreMemory(ctx, t, nestedPSW, "snap-2", 2); err != nil {
		t.Fatalf("restore of the re-captured snapshot = %v, want success", err)
	}
}

// TestNestedRuntime_CheckpointRecordsEffectiveNotRequested pins the stale-record wedge fix:
// Checkpoint records the flags the sandbox was CREATED with (the on-node
// record), not the flags in its own request. If the two differ -- as they can
// after a template repoint -- the manifest must follow the on-node record, so a
// later restore matches the memory image rather than a stale request value.
func TestNestedRuntime_CheckpointRecordsEffectiveNotRequested(t *testing.T) {
	ctx := t.Context()

	t.Run("nested sandbox, default checkpoint request still records nested", func(t *testing.T) {
		h := newNestedRuntimeHarness(t)
		h.run(ctx, t, nestedEnabled)
		// The checkpoint request carries a DEFAULT spec (a repointed template),
		// but the sandbox was created nested. The manifest must record nested.
		h.checkpointMemory(ctx, t, nil, "snap-1", 1)
		rec, err := readLocalSnapshot(ateletpath.LocalSnapshotDir(nestedActorUID, "snap-1"), "")
		if err != nil {
			t.Fatalf("readLocalSnapshot: %v", err)
		}
		if rec.NestedContainers == nil || !rec.NestedContainers.Enabled {
			t.Fatalf("manifest = %+v, want nested (from the on-node record, not the default request)", rec.NestedContainers)
		}
	})

	t.Run("default sandbox, nested checkpoint request still records default", func(t *testing.T) {
		h := newNestedRuntimeHarness(t)
		h.run(ctx, t, nil)
		// The checkpoint request carries a NESTED spec, but the sandbox is
		// default. The manifest must record nothing.
		h.checkpointMemory(ctx, t, nestedEnabled, "snap-1", 1)
		rec, err := readLocalSnapshot(ateletpath.LocalSnapshotDir(nestedActorUID, "snap-1"), "")
		if err != nil {
			t.Fatalf("readLocalSnapshot: %v", err)
		}
		if rec.NestedContainers != nil {
			t.Fatalf("manifest = %+v, want nil (default sandbox, ignoring the nested request)", rec.NestedContainers)
		}
	})
}

// TestNestedContainersManifestRoundTrip pins the manifest JSON: a nested record
// round-trips through marshal/unmarshal, and nil omits the key so a legacy
// manifest and a default actor read back as nil (not nested).
func TestNestedContainersManifestRoundTrip(t *testing.T) {
	marshalUnmarshal := func(rec *sandboxAssetsRecord) (*sandboxAssetsRecord, []byte) {
		t.Helper()
		b, err := json.Marshal(rec)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		got, err := unmarshalSandboxRecord(b)
		if err != nil {
			t.Fatalf("unmarshalSandboxRecord: %v", err)
		}
		return got, b
	}

	base := &sandboxAssetsRecord{SandboxClass: string(atev1.SandboxClassGvisor)}

	t.Run("nested record survives", func(t *testing.T) {
		rec := *base
		rec.NestedContainers = &nestedContainersManifest{Enabled: true, PacketSocketWrite: true}
		got, _ := marshalUnmarshal(&rec)
		if got.NestedContainers == nil || *got.NestedContainers != *rec.NestedContainers {
			t.Errorf("round-tripped nested = %+v, want %+v", got.NestedContainers, rec.NestedContainers)
		}
	})

	t.Run("nil omits the key and reads back nil", func(t *testing.T) {
		got, b := marshalUnmarshal(base)
		if got.NestedContainers != nil {
			t.Errorf("round-tripped nested = %+v, want nil", got.NestedContainers)
		}
		if strings.Contains(string(b), "nestedContainers") {
			t.Errorf("marshaled a nil NestedContainers as a key: %s", b)
		}
	})
}
