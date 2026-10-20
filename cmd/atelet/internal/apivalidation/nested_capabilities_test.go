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

package apivalidation

import (
	"context"
	"strings"
	"testing"

	"go.ate.dev/substrate/internal/proto/ateletpb"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// nestedAddCapabilities is the capabilities.add list a nested-containers
// workload needs: the nine allowed names beyond the default set plus the three
// nested-only ones.
var nestedAddCapabilities = []string{
	"CHOWN", "DAC_OVERRIDE", "FOWNER", "FSETID", "SETFCAP", "SETGID", "SETPCAP", "SETUID", "SYS_CHROOT",
	"MKNOD", "NET_ADMIN", "SYS_ADMIN",
}

// TestWorkloadSpecNestedContainersAddCapabilities proves, through every atelet
// RPC that carries a WorkloadSpec, that MKNOD, NET_ADMIN and SYS_ADMIN are
// admitted in capabilities.add only with nested_containers.enabled, and that
// the opt-in admits nothing else.
func TestWorkloadSpecNestedContainersAddCapabilities(t *testing.T) {
	const (
		workerPodUID = "0f9a3b1c-2d4e-5f60-7182-93a4b5c6d7e8"
		actorUID     = "01234567-89ab-cdef-0123-456789abcdef"
		snapshotURI  = "gs://bucket/root/atespaces/team-a/actors/01234567-89ab-cdef-0123-456789abcdef/snapshots/snap-1"
	)
	snapshot := func() *ateletpb.Snapshot {
		return &ateletpb.Snapshot{
			SnapshotUuid: "snap-1",
			Generation:   1,
			Fidelity:     ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
			Object:       &ateletpb.ObjectStorage{SnapshotUri: snapshotURI},
		}
	}
	type entry struct {
		name     string
		prefix   *field.Path
		validate func(*ateletpb.WorkloadSpec) field.ErrorList
	}
	spec := field.NewPath("spec")
	entries := []entry{
		{"Validate_WorkloadSpec", nil, func(s *ateletpb.WorkloadSpec) field.ErrorList {
			return Validate_WorkloadSpec(context.Background(), createOp, nil, s, nil)
		}},
		{"RunRequest", spec, func(s *ateletpb.WorkloadSpec) field.ErrorList {
			return ValidateRunRequest(context.Background(), &ateletpb.RunRequest{
				WorkerPodUid: workerPodUID, Atespace: "team-a", ActorName: "actor-1", ActorUid: actorUID,
				Spec:           s,
				SandboxRuntime: runtimeWithAttributes(1),
				Resources:      &ateletpb.ResourceRequirements{Limits: &ateletpb.Resources{CpuMillicores: 500, MemoryBytes: 1 << 30}},
			})
		}},
		{"RestoreRequest", spec, func(s *ateletpb.WorkloadSpec) field.ErrorList {
			return ValidateRestoreRequest(context.Background(), &ateletpb.RestoreRequest{
				WorkerPodUid: workerPodUID, Atespace: "team-a", ActorName: "actor-1", ActorUid: actorUID,
				Spec:           s,
				Snapshot:       snapshot(),
				SandboxRuntime: runtimeWithAttributes(1),
			})
		}},
		{"CheckpointRequest", spec, func(s *ateletpb.WorkloadSpec) field.ErrorList {
			return ValidateCheckpointRequest(context.Background(), &ateletpb.CheckpointRequest{
				WorkerPodUid: workerPodUID, Atespace: "team-a", ActorName: "actor-1", ActorUid: actorUID,
				Spec:         s,
				Snapshot:     snapshot(),
				UploadConfig: &ateletpb.UploadConfig{Mode: ateletpb.UploadMode_UPLOAD_MODE_IMMEDIATE},
			})
		}},
		{"TerminateRequest", spec, func(s *ateletpb.WorkloadSpec) field.ErrorList {
			return ValidateTerminateRequest(context.Background(), &ateletpb.TerminateRequest{
				WorkerPodUid: workerPodUID, Atespace: "team-a", ActorName: "actor-1", ActorUid: actorUID,
				Spec: s,
			})
		}},
	}
	// workload returns a two-container spec whose containers add add0 and add1.
	workload := func(nested bool, add0, add1 []string) *ateletpb.WorkloadSpec {
		ctr := func(name string, add []string) *ateletpb.Container {
			c := &ateletpb.Container{Name: name, Image: testDigestImage}
			if add != nil {
				c.SecurityContext = &ateletpb.SecurityContext{Capabilities: &ateletpb.Capabilities{Add: add}}
			}
			return c
		}
		s := &ateletpb.WorkloadSpec{Containers: []*ateletpb.Container{ctr("main", add0), ctr("sidecar", add1)}}
		if nested {
			s.NestedContainers = &ateletpb.NestedContainers{Enabled: true}
		}
		return s
	}
	for _, ep := range entries {
		addPath := func(ctr, idx int) *field.Path {
			p := field.NewPath("containers")
			if ep.prefix != nil {
				p = ep.prefix.Child("containers")
			}
			return p.Index(ctr).Child("security_context", "capabilities", "add").Index(idx)
		}
		t.Run(ep.name, func(t *testing.T) {
			for _, c := range []string{"MKNOD", "NET_ADMIN", "SYS_ADMIN"} {
				for _, nc := range []*ateletpb.NestedContainers{nil, {Enabled: false}, {Enabled: false, PacketSocketWrite: true}} {
					t.Run("nested off forbids "+c, func(t *testing.T) {
						s := workload(false, []string{"CHOWN", c}, nil)
						s.NestedContainers = nc
						errs := ep.validate(s)
						assertValidateErr(t, errs, field.ErrorList{field.Forbidden(addPath(0, 1), "")})
						for _, e := range errs {
							if !strings.Contains(e.Detail, "nested_containers.enabled") {
								t.Errorf("error %q does not name nested_containers.enabled", e.Error())
							}
						}
					})
				}
			}
			t.Run("nested off forbids each one in every container", func(t *testing.T) {
				assertValidateErr(t, ep.validate(workload(false, nestedAddCapabilities, []string{"SYS_ADMIN"})), field.ErrorList{
					field.Forbidden(addPath(0, 9), ""), field.Forbidden(addPath(0, 10), ""), field.Forbidden(addPath(0, 11), ""),
					field.Forbidden(addPath(1, 0), ""),
				})
			})
			t.Run("nested off: every allowed name still passes", func(t *testing.T) {
				assertValidateErr(t, ep.validate(workload(false, []string{
					"AUDIT_WRITE", "CHOWN", "DAC_OVERRIDE", "FOWNER", "FSETID", "KILL",
					"NET_BIND_SERVICE", "SETFCAP", "SETGID", "SETPCAP", "SETUID", "SYS_CHROOT",
				}, nil)), nil)
			})
			t.Run("nested on admits the nested-containers list", func(t *testing.T) {
				assertValidateErr(t, ep.validate(workload(true, nestedAddCapabilities, []string{"MKNOD"})), nil)
			})
			for _, tc := range []struct {
				c    string
				want *field.Error
			}{
				{"NET_RAW", field.NotSupported[string](addPath(0, 0), nil, nil)},
				{"SYS_PTRACE", field.NotSupported[string](addPath(0, 0), nil, nil)},
				{"SYS_MODULE", field.NotSupported[string](addPath(0, 0), nil, nil)},
				{"NET_ADIMN", field.NotSupported[string](addPath(0, 0), nil, nil)},
				{"mknod", field.NotSupported[string](addPath(0, 0), nil, nil)},
				{"ALL", field.Invalid(addPath(0, 0), nil, "")},
				{"CAP_SYS_ADMIN", field.Invalid(addPath(0, 0), nil, "")},
			} {
				t.Run("nested on still rejects "+tc.c, func(t *testing.T) {
					assertValidateErr(t, ep.validate(workload(true, []string{tc.c}, nil)), field.ErrorList{tc.want})
				})
			}
		})
	}
}
