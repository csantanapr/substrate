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

package ocispec

import (
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	specs "github.com/opencontainers/runtime-spec/specs-go"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/sizing"
)

func TestShapeGVisorGivesEachActorItsOwnCgroupLeaf(t *testing.T) {
	shape := func(actorUID, containerName string) string {
		spec := &specs.Spec{}
		ShapeGVisor(spec, GVisorOptions{ActorUID: actorUID, ContainerName: containerName})
		return spec.Linux.CgroupsPath
	}

	first := shape("uid-a", SandboxID)
	second := shape("uid-b", SandboxID)
	if first == second {
		t.Errorf("two actors share the cgroup leaf %q", first)
	}
	if !strings.Contains(first, "uid-a") {
		t.Errorf("cgroupsPath %q does not name the actor", first)
	}
	// runsc interprets colons as systemd cgroup syntax.
	if strings.Contains(first, ":") {
		t.Errorf("cgroupsPath %q contains a colon", first)
	}

	if a, b := shape("uid-a", SandboxID), shape("uid-a", "app"); a == b {
		t.Errorf("two containers of one actor share the leaf %q", a)
	}
}

func TestGVisorCgroupLeafMatchesTheShapedPath(t *testing.T) {
	spec := &specs.Spec{}
	ShapeGVisor(spec, GVisorOptions{ActorUID: "uid-a", ContainerName: SandboxID})
	if want := "/" + GVisorCgroupLeaf("uid-a", SandboxID); spec.Linux.CgroupsPath != want {
		t.Errorf("shaped cgroupsPath = %q, want %q", spec.Linux.CgroupsPath, want)
	}
}

// runsc replaces /dev and /dev/pts with its own mounts and ignores mqueue
// mounts and the strictatime option, so ShapeGVisor drops them. nodev stays.
// /tmp is capped at half the memory limit when tmpfs.
func TestShapeGVisor_SystemMounts(t *testing.T) {
	const durableDir = "/node/actors/a/durable-dir"
	for _, tc := range []struct {
		name     string
		memory   int64
		mounts   []*ateworkerpb.VolumeMount
		wantAll  []specs.Mount
		wantEach []specs.Mount
	}{
		{
			name:   "memory limit",
			memory: 512 << 20,
			wantAll: []specs.Mount{
				{Destination: "/proc", Type: "proc", Source: "proc", Options: []string{"nosuid", "noexec", "nodev"}},
				{Destination: "/dev/shm", Type: "tmpfs", Source: "shm", Options: []string{"nosuid", "noexec", "nodev", "mode=1777", "size=65536k"}},
				{Destination: "/sys", Type: "sysfs", Source: "sysfs", Options: []string{"nosuid", "noexec", "nodev", "ro"}},
				{Destination: "/tmp", Type: "tmpfs", Source: "tmpfs", Options: []string{"nosuid", "nodev", "mode=1777", "size=268435456"}},
			},
		},
		{
			name:   "minimum positive memory limit",
			memory: 1,
			wantEach: []specs.Mount{
				{Destination: "/tmp", Type: "tmpfs", Source: "tmpfs", Options: []string{"nosuid", "nodev", "mode=1777", "size=1"}},
			},
		},
		{
			name:   "no memory limit",
			memory: 0,
			wantEach: []specs.Mount{
				{Destination: "/tmp", Type: "tmpfs", Source: "tmpfs", Options: []string{"nosuid", "nodev", "mode=1777"}},
			},
		},
		{
			name:   "volumes at /dev/mqueue and /tmp",
			memory: 512 << 20,
			mounts: []*ateworkerpb.VolumeMount{
				{Name: "mq", MountPath: "/dev/mqueue"},
				{Name: "tmp", MountPath: "/tmp"},
			},
			wantEach: []specs.Mount{
				{Destination: "/dev/mqueue", Type: "bind", Source: durableDir + "/mq", Options: []string{"bind", "rw", "nodev"}},
				{Destination: "/tmp", Type: "bind", Source: durableDir + "/tmp", Options: []string{"bind", "rw", "nodev"}},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actorDirs := &ateworkerpb.ActorDirs{DurableDirVolumeMountsDir: durableDir}
			spec := Build(Options{
				ActorDirs: actorDirs,
				Container: &ateworkerpb.Container{
					ContainerSpec:          &ateworkerpb.ContainerSpec{Args: []string{"/app"}},
					DurableDirVolumeMounts: tc.mounts,
				},
			})
			ShapeGVisor(spec, GVisorOptions{ActorUID: "uid-a", ActorDirs: actorDirs, ContainerName: "app", Size: sizing.SandboxSize{MemoryBytes: tc.memory}})
			if tc.wantAll != nil && !reflect.DeepEqual(spec.Mounts, tc.wantAll) {
				t.Errorf("Mounts =\n%+v\nwant\n%+v", spec.Mounts, tc.wantAll)
			}
			for _, want := range tc.wantEach {
				if got := mountFor(t, spec, want.Destination); !reflect.DeepEqual(got, want) {
					t.Errorf("%s =\n%+v\nwant\n%+v", want.Destination, got, want)
				}
			}
		})
	}
}

// Upstream #2765 removed TestShapeGVisorBindsTheNamedResolvConf (ShapeGVisor no
// longer binds resolv.conf -- gVisor writes it into the rootfs via
// actornet.WriteRootfsEtcFiles). Our nested-image-store test below is kept.

// TestShapeGVisor_NestedImageStore proves the nested image store directly at the shaper (both the
// writeSpec caller and this shaper guard the sandbox root container; this test pins
// the shaper's own guard so removing it is caught). It also proves the mount is
// gVisor-only: ShapeMicroVM adds nothing at the path.
func TestShapeGVisor_NestedImageStore(t *testing.T) {
	const store = NestedImageStorePath

	shapeGVisor := func(containerName string, nis *NestedImageStore) *specs.Spec {
		spec := Build(parityOptions)
		ShapeGVisor(spec, GVisorOptions{ActorUID: testActorUID, ActorDirs: parityActorDirs, ContainerName: containerName, NestedImageStore: nis, Size: paritySize})
		return spec
	}

	hasMount := func(spec *specs.Spec) (specs.Mount, bool) {
		for _, m := range spec.Mounts {
			if m.Destination == store {
				return m, true
			}
		}
		return specs.Mount{}, false
	}

	t.Run("app container gets a capped tmpfs with the requested size", func(t *testing.T) {
		m, ok := hasMount(shapeGVisor("app", &NestedImageStore{SizeBytes: 1 << 30}))
		if !ok {
			t.Fatalf("app container has no mount at %s", store)
		}
		if m.Type != "tmpfs" || m.Source != "tmpfs" {
			t.Errorf("mount type/source = %q/%q, want tmpfs/tmpfs", m.Type, m.Source)
		}
		// nodev, like the system and volume mounts ShapeGVisor keeps.
		want := []string{"nosuid", "nodev", "size=1073741824", "mode=0711"}
		if !slices.Equal(m.Options, want) {
			t.Errorf("mount options = %v, want %v", m.Options, want)
		}
	})

	t.Run("system mounts are unchanged beside the image store", func(t *testing.T) {
		// The image store is one extra mount at the end; the system mounts
		// ShapeGVisor keeps (and the /tmp cap) are the same as without it.
		without := shapeGVisor("app", nil)
		with := shapeGVisor("app", &NestedImageStore{SizeBytes: 1 << 30})
		if len(with.Mounts) != len(without.Mounts)+1 {
			t.Fatalf("mount count = %d, want %d", len(with.Mounts), len(without.Mounts)+1)
		}
		if got := with.Mounts[:len(without.Mounts)]; !reflect.DeepEqual(got, without.Mounts) {
			t.Errorf("other mounts =\n%+v\nwant\n%+v", got, without.Mounts)
		}
		if got := with.Mounts[len(with.Mounts)-1].Destination; got != store {
			t.Errorf("last mount = %s, want %s", got, store)
		}
		if got := mountFor(t, with, "/tmp"); !slices.Contains(got.Options, "size="+itoa(paritySize.MemoryBytes/2)) {
			t.Errorf("/tmp options = %v, want the half-memory cap", got.Options)
		}
	})

	t.Run("zero size selects the default cap", func(t *testing.T) {
		m, ok := hasMount(shapeGVisor("app", &NestedImageStore{SizeBytes: 0}))
		if !ok {
			t.Fatalf("app container has no mount at %s", store)
		}
		if !slices.Contains(m.Options, "size="+itoa(DefaultNestedImageStoreBytes)) {
			t.Errorf("mount options %v do not carry the default cap", m.Options)
		}
	})

	t.Run("sandbox root container never gets the mount even when requested", func(t *testing.T) {
		if _, ok := hasMount(shapeGVisor(SandboxID, &NestedImageStore{SizeBytes: 1 << 30})); ok {
			t.Errorf("sandbox root container got the nested image-store tmpfs")
		}
	})

	t.Run("nil NestedImageStore adds no mount", func(t *testing.T) {
		if _, ok := hasMount(shapeGVisor("app", nil)); ok {
			t.Errorf("app container got the tmpfs with a nil NestedImageStore")
		}
	})

	t.Run("idempotent: a second shaping does not duplicate the mount", func(t *testing.T) {
		spec := Build(parityOptions)
		o := GVisorOptions{ActorUID: testActorUID, ActorDirs: parityActorDirs, ContainerName: "app", NestedImageStore: &NestedImageStore{SizeBytes: 1 << 30}, Size: paritySize}
		ShapeGVisor(spec, o)
		first := slices.Clone(spec.Mounts)
		ShapeGVisor(spec, o)
		if !reflect.DeepEqual(spec.Mounts, first) {
			t.Errorf("mounts after a second shaping =\n%+v\nwant\n%+v", spec.Mounts, first)
		}
	})

	t.Run("microVM path is unaffected", func(t *testing.T) {
		// ShapeMicroVM takes no NestedImageStore and must not add the tmpfs,
		// so the microVM runtime and the parity test stay unchanged.
		spec := Build(parityOptions)
		if err := ShapeMicroVM(spec, MicroVMOptions{ActorDirs: parityActorDirs, ContainerName: "app"}); err != nil {
			t.Fatalf("ShapeMicroVM() = %v", err)
		}
		if _, ok := hasMount(spec); ok {
			t.Errorf("ShapeMicroVM added a mount at %s", store)
		}
	})
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }
