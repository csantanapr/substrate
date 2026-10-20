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
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
	"go.ate.dev/substrate/cmd/atelet/internal/snapshottracker"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/utils/clock"
)

// These tests pin the CALL SITES of the sandbox-class guards in Run and
// Restore, not just the helper functions. Removing the validateNestedSandbox
// call in Run, or in Restore, or either validateNetworkSandbox call, must make
// the matching case here fail -- proven by four call-site variants. Each
// rejection is checked to happen BEFORE any
// on-node work (no asset download, no actor dir), the same bar
// TestRunRejectsUnadvertisedRuntime holds, because a guard that fired only
// after staging would already have leaked work.

const callsiteActorUID = "01234567-89ab-cdef-0123-456789abcdef"
const callsiteWorkerUID = "5f1c0d2e-3a4b-4c5d-8e9f-0a1b2c3d4e5f"

// A pinned image so apivalidation.ValidateRunRequest/ValidateRestoreRequest --
// which runs before the class guards -- accepts the container, letting
// execution reach the guards under test.
const callsiteImage = "example.com/app@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// classAtelet builds an Atelet whose asset fetches are counted, advertises a
// runtime of the named class, and returns the matching SandboxAssets plus the
// fetch counter. network=true makes it a network atelet so validateNetworkSandbox
// is active.
func classAtelet(t *testing.T, class string, network bool) (*Atelet, *sandboxAssetsRecord, *atomic.Int32) {
	t.Helper()
	useTempNodeDirs(t)
	var gets atomic.Int32
	// Upstream #2798 dropped the legacy bare "runsc" asset: the gVisor runtime
	// is now the "gvisor" release tarball atelet extracts. Serve a real tarball
	// and name it gvisorAssetName (the staging path keys stageGVisorRelease on
	// the asset NAME, so this is right for the gVisor class these call-site
	// guards exercise).
	release := newGVisorTestRelease(t)
	s := &Atelet{
		checkpointTimeout: defaultCheckpointTimeout,
		runtimes:          newRuntimeIndex(),
		imageCache:        newImageVolumeStore(t),
		sandboxAssetCache: newTestAssetCache(t),
		anonGCSClient:     fakeObjectStorage{data: release.tarball, gets: &gets},
		systemInfoVolumes: newSystemInfoVolumeRefresher(nil, nil),
		snapshots:         snapshottracker.New(t.Context(), nil, clock.RealClock{}),
	}
	if network {
		s.network = &nodeWorkerNetwork{}
	}
	assets := &sandboxAssetsRecord{
		SandboxClass: class,
		Assets:       map[string]assetEntry{gvisorAssetName: release.entry},
	}
	advertiseRuntime(t, s, assets)
	return s, assets, &gets
}

// assertNoOnNodeWork fails if a refused lifecycle call fetched assets or created
// the actor directory -- i.e. if the guard fired after on-node work began.
func assertNoOnNodeWork(t *testing.T, gets *atomic.Int32) {
	t.Helper()
	if gets.Load() != 0 {
		t.Errorf("assets fetched %d times for a refused call, want 0", gets.Load())
	}
	if _, err := os.Lstat(ateletpath.ActorPath(callsiteActorUID)); !os.IsNotExist(err) {
		t.Errorf("refused call created the actor dir (lstat err = %v)", err)
	}
}

func nestedRunReq(assets *sandboxAssetsRecord, t *testing.T) *ateletpb.RunRequest {
	return &ateletpb.RunRequest{
		Atespace: "team-a", ActorName: "actor-1", ActorUid: callsiteActorUID, WorkerPodUid: callsiteWorkerUID,
		SandboxRuntime: runtimeFor(t, assets),
		Spec: &ateletpb.WorkloadSpec{
			Containers:       []*ateletpb.Container{{Name: "app", Image: callsiteImage}},
			NestedContainers: &ateletpb.NestedContainers{Enabled: true},
		},
	}
}

func nestedRestoreReq(assets *sandboxAssetsRecord, t *testing.T) *ateletpb.RestoreRequest {
	return &ateletpb.RestoreRequest{
		Atespace: "team-a", ActorName: "actor-1", ActorUid: callsiteActorUID, WorkerPodUid: callsiteWorkerUID,
		SandboxRuntime: runtimeFor(t, assets),
		Spec: &ateletpb.WorkloadSpec{
			Containers:       []*ateletpb.Container{{Name: "app", Image: callsiteImage}},
			NestedContainers: &ateletpb.NestedContainers{Enabled: true},
		},
		Snapshot: lifecycleSnapshot(callsiteActorUID, "snap-1", 1, ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY),
	}
}

// TestRunNestedOnNonGvisorRefused: the validateNestedSandbox call in Run rejects
// an enabled nested opt-in that resolves to a microVM runtime, before any work.
func TestRunNestedOnNonGvisorRefused(t *testing.T) {
	s, assets, gets := classAtelet(t, "microvm", false)
	_, err := s.Run(t.Context(), nestedRunReq(assets, t))
	if status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "gvisor") {
		t.Fatalf("Run nested-on-microvm = %v (code %s), want InvalidArgument naming gvisor", err, status.Code(err))
	}
	assertNoOnNodeWork(t, gets)
}

// TestRestoreNestedOnNonGvisorRefused: the same guard on the Restore path.
func TestRestoreNestedOnNonGvisorRefused(t *testing.T) {
	s, assets, gets := classAtelet(t, "microvm", false)
	_, err := s.Restore(t.Context(), nestedRestoreReq(assets, t))
	if status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "gvisor") {
		t.Fatalf("Restore nested-on-microvm = %v (code %s), want InvalidArgument naming gvisor", err, status.Code(err))
	}
	assertNoOnNodeWork(t, gets)
}

// TestRunNetworkUnsupportedClassRefused: the validateNetworkSandbox call in Run
// rejects a non-gvisor class on a network atelet, before any work. Spec carries
// no nested opt-in, so only validateNetworkSandbox can be the rejecter.
func TestRunNetworkUnsupportedClassRefused(t *testing.T) {
	s, assets, gets := classAtelet(t, "microvm", true)
	_, err := s.Run(t.Context(), &ateletpb.RunRequest{
		Atespace: "team-a", ActorName: "actor-1", ActorUid: callsiteActorUID, WorkerPodUid: callsiteWorkerUID,
		SandboxRuntime: runtimeFor(t, assets),
		Spec:           &ateletpb.WorkloadSpec{Containers: []*ateletpb.Container{{Name: "app", Image: callsiteImage}}},
	})
	if status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "network worker staging") {
		t.Fatalf("Run network non-gvisor = %v (code %s), want InvalidArgument from validateNetworkSandbox", err, status.Code(err))
	}
	assertNoOnNodeWork(t, gets)
}

// TestRestoreNetworkUnsupportedClassRefused: the same on the Restore path.
func TestRestoreNetworkUnsupportedClassRefused(t *testing.T) {
	s, assets, gets := classAtelet(t, "microvm", true)
	_, err := s.Restore(t.Context(), &ateletpb.RestoreRequest{
		Atespace: "team-a", ActorName: "actor-1", ActorUid: callsiteActorUID, WorkerPodUid: callsiteWorkerUID,
		SandboxRuntime: runtimeFor(t, assets),
		Spec:           &ateletpb.WorkloadSpec{Containers: []*ateletpb.Container{{Name: "app", Image: callsiteImage}}},
		Snapshot:       lifecycleSnapshot(callsiteActorUID, "snap-1", 1, ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY),
	})
	if status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "network worker staging") {
		t.Fatalf("Restore network non-gvisor = %v (code %s), want InvalidArgument from validateNetworkSandbox", err, status.Code(err))
	}
	assertNoOnNodeWork(t, gets)
}

// TestRunNestedOnGvisorPassesGuards: the accepting case -- a gVisor-resolved
// nested opt-in clears BOTH guards and proceeds into on-node work (so the guards
// are not a blanket deny). It fails later, at the fake pause OCI/worker step;
// what matters here is that it got PAST both class checks. A regression that
// made either guard reject gvisor would stop it before any work.
func TestRunNestedOnGvisorPassesGuards(t *testing.T) {
	s, assets, gets := classAtelet(t, "gvisor", false)
	// The accept case clears both guards and proceeds into on-node work, where
	// this minimal test Atelet (no real worker/OCI wiring) errors or panics far
	// downstream. Either outcome proves the guards were cleared; what must NOT
	// happen is a class-guard rejection. Recover so a downstream panic is read
	// as "got past the guards", not a test failure, and assert on the class
	// messages plus the fetch counter.
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				// A panic downstream of the guards means execution reached
				// on-node work; the guards did not reject. Record it as a
				// non-guard error for the assertion below.
				err = fmt.Errorf("panic past the guards: %v", r)
			}
		}()
		_, err = s.Run(t.Context(), nestedRunReq(assets, t))
	}()
	if err != nil {
		msg := err.Error()
		if strings.Contains(msg, "nested containers are supported only") || strings.Contains(msg, "network worker staging") {
			t.Fatalf("gvisor nested Run was rejected by a class guard: %v", err)
		}
	}
	// It cleared the guards, so it reached on-node work: assets were fetched.
	if gets.Load() == 0 {
		t.Errorf("gvisor nested Run fetched no assets; it may have been stopped before on-node work")
	}
}
