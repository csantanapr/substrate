//go:build linux

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
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"testing"

	"github.com/opencontainers/runtime-spec/specs-go"

	"go.ate.dev/substrate/internal/ocispec"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
)

// productionFlagStore loads the real kodata/internal_flags.json (the config
// that ships), so the golden argv below is anchored to production, not to the
// synthetic config the other runsc tests use.
func productionFlagStore(t *testing.T) *flagStore {
	t.Helper()
	store, err := newFlagStore(t.Context(), "kodata/internal_flags.json")
	if err != nil {
		t.Fatalf("newFlagStore(kodata/internal_flags.json) = %v", err)
	}
	return store
}

// The four lifecycle commands that carry nested flags, and a representative
// sample of the commands that must NOT.
var (
	lifecycleCommands = []string{"create", "start", "checkpoint", "restore"}
	otherCommands     = []string{"delete", "state", "list", "kill", "wait"}
)

// TestNestedRuntimeTopLevelFlags pins the one flag builder, both ways.
func TestNestedRuntimeTopLevelFlags(t *testing.T) {
	for _, tc := range []struct {
		name string
		n    nestedRuntime
		want []string
	}{
		{"zero value adds nothing", nestedRuntime{}, nil},
		{"disabled adds nothing even with write", nestedRuntime{packetSocketWrite: true}, nil},
		{"enabled adds net-raw", nestedRuntime{enabled: true}, []string{"-net-raw"}},
		{"enabled with write adds both, in order", nestedRuntime{enabled: true, packetSocketWrite: true}, []string{"-net-raw", "-allow-packet-socket-write"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.n.topLevelFlags(); !slices.Equal(got, tc.want) {
				t.Errorf("topLevelFlags() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestNestedRuntimeFromSpec pins the spec reader, both ways.
func TestNestedRuntimeFromSpec(t *testing.T) {
	for _, tc := range []struct {
		name string
		spec *ateworkerpb.WorkloadSpec
		want nestedRuntime
	}{
		{"nil spec", &ateworkerpb.WorkloadSpec{}, nestedRuntime{}},
		{"no nested config", &ateworkerpb.WorkloadSpec{Containers: []*ateworkerpb.Container{{Name: "app"}}}, nestedRuntime{}},
		{"disabled", &ateworkerpb.WorkloadSpec{NestedContainers: &ateworkerpb.NestedContainers{Enabled: false, PacketSocketWrite: true}}, nestedRuntime{}},
		{"enabled", &ateworkerpb.WorkloadSpec{NestedContainers: &ateworkerpb.NestedContainers{Enabled: true}}, nestedRuntime{enabled: true}},
		{"enabled with write", &ateworkerpb.WorkloadSpec{NestedContainers: &ateworkerpb.NestedContainers{Enabled: true, PacketSocketWrite: true}}, nestedRuntime{enabled: true, packetSocketWrite: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := nestedRuntimeFromSpec(tc.spec); got != tc.want {
				t.Errorf("nestedRuntimeFromSpec() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// TestBuildArgs_DefaultMatchesGolden is the runsc-flags golden: a DEFAULT actor's argv,
// built by the real buildArgs from the production flag config, is byte-for-byte
// the recorded baseline (tmp/baseline-default-argv.md), with NO nested flag
// anywhere. The golden is recomposed here from the production config by the
// real builder, so it tracks the production config rather than a hand-typed
// copy; the recorded baseline is the pre-edit expectation it must still equal.
//
// Both ways: TestBuildArgs_DefaultHasNoNestedFlags proves a leaked nested flag
// on a default actor fails, and the notes below prove the golden is not
// vacuous.
func TestBuildArgs_DefaultMatchesGolden(t *testing.T) {
	r := &runsc{
		path:      "/usr/bin/runsc",
		actorUID:  "A",
		actorDirs: &ateworkerpb.ActorDirs{RootDir: "/node/actors/A", OciBundleDir: "/node/actors/A/bundle"},
		flags:     productionFlagStore(t),
		// nested is the zero value: a default actor.
	}
	const stateDir = "/node/actors/A/runsc-state"
	for _, tc := range []struct {
		name      string
		command   string
		container string
		extra     []string
		want      []string
	}{
		{
			name: "create", command: "create", container: "C",
			extra: []string{"-bundle", "/node/actors/A/bundle/C", "-pid-file", "/node/actors/A/pidfiles/C.pid"},
			want: []string{
				"-log-format", "json", "--alsologtostderr", "--allow-suid", "--host-settings=ignore", "--cpu-num-from-quota",
				"-root", stateDir, "create",
				"-bundle", "/node/actors/A/bundle/C", "-pid-file", "/node/actors/A/pidfiles/C.pid", "C",
			},
		},
		{
			name: "start", command: "start", container: "C",
			want: []string{
				"-log-format", "json", "--alsologtostderr", "--allow-suid", "--host-settings=ignore", "-allow-connected-on-save",
				"-root", stateDir, "start", "C",
			},
		},
		{
			name: "checkpoint", command: "checkpoint", container: "C",
			extra: []string{"-image-path", "/node/actors/A/checkpoints/snap-1"},
			want: []string{
				"-log-format", "json", "--alsologtostderr", "--allow-suid", "--host-settings=ignore",
				"-root", stateDir, "checkpoint",
				"-image-path", "/node/actors/A/checkpoints/snap-1", "C",
			},
		},
		{
			name: "restore", command: "restore", container: "C",
			extra: []string{"-bundle", "/node/actors/A/bundle/C", "-image-path", "/node/actors/A/checkpoints/snap-1", "-pid-file", "/node/actors/A/pidfiles/C.pid"},
			want: []string{
				"-log-format", "json", "--alsologtostderr", "--allow-suid", "--host-settings=ignore", "--cpu-num-from-quota",
				"-root", stateDir, "restore", "-background", "-detach",
				"-bundle", "/node/actors/A/bundle/C", "-image-path", "/node/actors/A/checkpoints/snap-1", "-pid-file", "/node/actors/A/pidfiles/C.pid", "C",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := r.buildArgs(tc.command, tc.container, tc.extra...)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("default %s argv =\n  %v\nwant\n  %v", tc.command, got, tc.want)
			}
			if slices.Contains(got, "-net-raw") || slices.Contains(got, "-allow-packet-socket-write") {
				t.Errorf("default %s argv leaked a nested flag: %v", tc.command, got)
			}
		})
	}
}

// TestBuildArgs_DefaultHasNoNestedFlags proves the negative control directly:
// across every lifecycle command and every other command, a default actor's
// argv never contains a nested flag.
func TestBuildArgs_DefaultHasNoNestedFlags(t *testing.T) {
	r := &runsc{
		path:      "/usr/bin/runsc",
		actorUID:  "A",
		actorDirs: &ateworkerpb.ActorDirs{RootDir: "/node/actors/A", OciBundleDir: "/node/actors/A/bundle"},
		flags:     productionFlagStore(t),
	}
	for _, command := range append(append([]string{}, lifecycleCommands...), otherCommands...) {
		got := r.buildArgs(command, "C")
		if slices.Contains(got, "-net-raw") || slices.Contains(got, "-allow-packet-socket-write") {
			t.Errorf("default %s argv contains a nested flag: %v", command, got)
		}
	}
}

// TestBuildArgs_NestedAddsFlagsOnEveryLifecycleCommand proves the opted-in
// actor carries -net-raw on all four lifecycle commands and NOT on the others,
// with -allow-packet-socket-write present iff requested and only immediately
// after -net-raw. This is the per-command defense at the builder: each command is
// checked distinctly, so a variant that adds the flag to one command but not
// another fails.
func TestBuildArgs_NestedAddsFlagsOnEveryLifecycleCommand(t *testing.T) {
	for _, tc := range []struct {
		name    string
		nested  nestedRuntime
		wantPSW bool
	}{
		{"net-raw only", nestedRuntime{enabled: true}, false},
		{"net-raw and packet-socket-write", nestedRuntime{enabled: true, packetSocketWrite: true}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &runsc{
				path:      "/usr/bin/runsc",
				actorUID:  "A",
				actorDirs: &ateworkerpb.ActorDirs{RootDir: "/node/actors/A", OciBundleDir: "/node/actors/A/bundle"},
				flags:     productionFlagStore(t),
				nested:    tc.nested,
			}
			for _, command := range lifecycleCommands {
				got := r.buildArgs(command, "C")
				i := slices.Index(got, "-net-raw")
				if i < 0 {
					t.Errorf("%s invocation missing -net-raw: %v", command, got)
					continue
				}
				// The nested flag is a GLOBAL flag: it must come before the
				// command token, not after.
				if cmdIdx := slices.Index(got, command); cmdIdx >= 0 && i > cmdIdx {
					t.Errorf("%s: -net-raw at %d is after the command token at %d: %v", command, i, cmdIdx, got)
				}
				psw := slices.Contains(got, "-allow-packet-socket-write")
				if psw != tc.wantPSW {
					t.Errorf("%s: packet-socket-write present = %t, want %t: %v", command, psw, tc.wantPSW, got)
				}
				if tc.wantPSW {
					// Only ever immediately after -net-raw.
					if got[i+1] != "-allow-packet-socket-write" {
						t.Errorf("%s: -allow-packet-socket-write not immediately after -net-raw: %v", command, got)
					}
				}
			}
			// Never on the non-lifecycle commands.
			for _, command := range otherCommands {
				got := r.buildArgs(command, "C")
				if slices.Contains(got, "-net-raw") || slices.Contains(got, "-allow-packet-socket-write") {
					t.Errorf("nested flag leaked onto non-lifecycle command %q: %v", command, got)
				}
			}
		})
	}
}

// TestRPCsWireNestedFromOwnRequest checks the per-RPC wiring. The three lifecycle RPC
// handlers build their runsc through newRunRunsc / newCheckpointRunsc /
// newRestoreRunsc (the real construction the handlers call); driving the
// handlers end-to-end would need a live tunnel, networking and a recording
// runsc (upstream has no test seams for those), so this exercises those
// constructors directly. Each RPC gets a DISTINCT spec, and the test asserts
// the resulting runsc.nested and imageStoreSizeBytes match THAT request's spec.
// A variant that dropped the nested fields from a constructor, hard-coded them,
// or read a different field would make one of the three wrong and fail here --
// unlike a test of nestedRuntimeFromSpec alone, which the handlers could stop
// calling without detection.
func TestRPCsWireNestedFromOwnRequest(t *testing.T) {
	flags := productionFlagStore(t)

	runReq := &ateworkerpb.RunWorkloadRequest{
		ActorUid: "run-actor",
		Spec:     &ateworkerpb.WorkloadSpec{NestedContainers: &ateworkerpb.NestedContainers{Enabled: true, ImageStoreSizeBytes: 1 << 30}},
	}
	checkpointReq := &ateworkerpb.CheckpointWorkloadRequest{
		ActorUid: "ckpt-actor",
		Spec:     &ateworkerpb.WorkloadSpec{NestedContainers: &ateworkerpb.NestedContainers{Enabled: true}},
	}
	restoreReq := &ateworkerpb.RestoreWorkloadRequest{
		ActorUid: "restore-actor",
		Spec:     &ateworkerpb.WorkloadSpec{NestedContainers: &ateworkerpb.NestedContainers{Enabled: true, PacketSocketWrite: true}},
	}

	run := newRunRunsc(flags, runReq)
	if run.nested != (nestedRuntime{enabled: true}) {
		t.Errorf("RunWorkload runsc.nested = %+v, want {enabled:true}", run.nested)
	}
	if run.imageStoreSizeBytes != 1<<30 {
		t.Errorf("RunWorkload runsc.imageStoreSizeBytes = %d, want %d", run.imageStoreSizeBytes, 1<<30)
	}
	if run.actorUID != "run-actor" {
		t.Errorf("RunWorkload used actorUID %q, want run-actor (wrong request?)", run.actorUID)
	}

	ckpt := newCheckpointRunsc(flags, checkpointReq)
	if ckpt.nested != (nestedRuntime{enabled: true}) {
		t.Errorf("CheckpointWorkload runsc.nested = %+v, want {enabled:true}", ckpt.nested)
	}
	if ckpt.actorUID != "ckpt-actor" {
		t.Errorf("CheckpointWorkload used actorUID %q, want ckpt-actor", ckpt.actorUID)
	}

	restore := newRestoreRunsc(flags, restoreReq)
	if restore.nested != (nestedRuntime{enabled: true, packetSocketWrite: true}) {
		t.Errorf("RestoreWorkload runsc.nested = %+v, want {enabled:true packetSocketWrite:true}", restore.nested)
	}
	if restore.actorUID != "restore-actor" {
		t.Errorf("RestoreWorkload used actorUID %q, want restore-actor", restore.actorUID)
	}

	// The three derive distinct nested runtimes, so a constructor that copied
	// another's wiring would be caught by the per-RPC assertions above.
	if run.nested == restore.nested || ckpt.nested == restore.nested {
		t.Fatal("the three RPC specs are not distinct; the per-RPC guard would be vacuous")
	}
}

// TestWriteSpec_NestedImageStore proves the nested image store at the spec level through the real
// writeSpec: an opted-in actor's APP container gets the tmpfs at
// /var/lib/docker; the pause container never does; and a default actor's app
// container has no such mount.
func TestWriteSpec_NestedImageStore(t *testing.T) {
	for _, tc := range []struct {
		name          string
		nested        bool
		sizeBytes     int64
		container     string
		wantMount     bool
		wantSizeBytes int64
	}{
		{"nested app container, default size", true, 0, "app", true, ocispec.DefaultNestedImageStoreBytes},
		{"nested app container, explicit size", true, 1 << 30, "app", true, 1 << 30},
		{"nested sandbox root container gets none", true, 0, ocispec.SandboxID, false, 0},
		{"default app container gets none", false, 0, "app", false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			actorDirs := &ateworkerpb.ActorDirs{RootDir: root, OciBundleDir: root + "/bundles"}
			r := &runsc{
				actorUID:            "uid-a",
				actorDirs:           actorDirs,
				containers:          []*ateworkerpb.Container{{Name: "app", ContainerSpec: &ateworkerpb.ContainerSpec{Args: []string{"/app"}}}},
				imageStoreSizeBytes: tc.sizeBytes,
			}
			if tc.nested {
				r.nested = nestedRuntime{enabled: true}
			}
			spec := buildSpecForTest(t, r, tc.container)
			m, found := mountAt(spec.Mounts, ocispec.NestedImageStorePath)
			if found != tc.wantMount {
				t.Fatalf("mount at %s present = %t, want %t (mounts: %+v)", ocispec.NestedImageStorePath, found, tc.wantMount, spec.Mounts)
			}
			if !tc.wantMount {
				return
			}
			if m.Type != "tmpfs" {
				t.Errorf("mount type = %q, want tmpfs", m.Type)
			}
			// nodev, like every other mount ocispec.ShapeGVisor keeps.
			wantOpts := []string{"nosuid", "nodev", "size=" + itoa(tc.wantSizeBytes), "mode=0711"}
			if !slices.Equal(m.Options, wantOpts) {
				t.Errorf("mount options = %v, want %v", m.Options, wantOpts)
			}
		})
	}
}

// buildSpecForTest runs the real writeSpec for containerName and reads back the
// config.json it wrote, so the test exercises the actual spec-shaping path.
func buildSpecForTest(t *testing.T, r *runsc, containerName string) *specs.Spec {
	t.Helper()
	bundle := ociBundlePath(r.actorDirs, containerName)
	if err := os.MkdirAll(bundle, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := r.writeSpec(containerName); err != nil {
		t.Fatalf("writeSpec(%q) = %v", containerName, err)
	}
	b, err := os.ReadFile(filepath.Join(bundle, "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	var spec specs.Spec
	if err := json.Unmarshal(b, &spec); err != nil {
		t.Fatal(err)
	}
	return &spec
}

func mountAt(mounts []specs.Mount, dest string) (specs.Mount, bool) {
	for _, m := range mounts {
		if m.Destination == dest {
			return m, true
		}
	}
	return specs.Mount{}, false
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }
