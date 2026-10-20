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
	"errors"
	"fmt"
	"strings"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/ocispec"
	"go.ate.dev/substrate/internal/resources"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func ValidateCreateActorTemplateRequest(ctx context.Context, req *ateapi.CreateActorTemplateRequest) field.ErrorList {
	// Call the generated validation.
	op := MakeCreateOp(ctx)
	return Validate_CreateActorTemplateRequest(ctx, op, nil, req, nil)
}

func ValidateGetActorTemplateRequest(ctx context.Context, req *ateapi.GetActorTemplateRequest) field.ErrorList {
	// Call the generated validation.
	op := MakeCreateOp(ctx)
	return Validate_GetActorTemplateRequest(ctx, op, nil, req, nil)
}

func ValidateListActorTemplatesRequest(ctx context.Context, req *ateapi.ListActorTemplatesRequest) field.ErrorList {
	// Call the generated validation.
	op := MakeCreateOp(ctx)
	return Validate_ListActorTemplatesRequest(ctx, op, nil, req, nil)
}

func ValidateDeleteActorTemplateRequest(ctx context.Context, req *ateapi.DeleteActorTemplateRequest) field.ErrorList {
	// Call the generated validation.
	op := MakeCreateOp(ctx)
	return Validate_DeleteActorTemplateRequest(ctx, op, nil, req, nil)
}

func ValidateActorTemplateUpdate(ctx context.Context, fldPath *field.Path, newVal, oldVal *ateapi.ActorTemplate) field.ErrorList {
	// Call the generated validation.
	op := MakeUpdateOp(ctx)
	return Validate_ActorTemplate(ctx, op, fldPath, newVal, oldVal)
}

// ValidateCustom_CreateActorTemplateRequest_ActorTemplate holds the rules
// that span more than one field of a template: a golden egress policy needs
// golden snapshots, container volume mounts must reference volumes the
// template declares, and (when nested containers are opted in) no volume mount
// may collide with the nested image-store tmpfs.
func ValidateCustom_CreateActorTemplateRequest_ActorTemplate(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *ateapi.ActorTemplate) field.ErrorList {
	var errs field.ErrorList
	if value.GetGoldenEgressPolicy() != nil && value.GetSnapshotConfig().GetGoldenSnapshotConfig().GetMode() == ateapi.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_DISABLED {
		errs = append(errs, field.Forbidden(fldPath.Child("golden_egress_policy"), "requires golden snapshots to be enabled"))
	}
	// The template carries no sandbox class: the worker pool sets an actor's
	// class through workerSelector. So the nested-containers opt-in is not
	// checked against a class here. Nested containers are gVisor-only: only
	// ateworker-gvisor reads the ateworkerpb.NestedContainers field, and atelet
	// refuses the opt-in at Run and Restore for any other resolved class. A
	// nested-opted template selects a gVisor pool through workerSelector.
	//
	// An opted-in actor's application containers get a tmpfs at the nested image
	// store, appended after the volume binds, so a volume mounted at, under or
	// above that path would be silently shadowed (or would shadow it): writes
	// meant for the volume would land in tmpfs. Mount paths are already clean
	// absolute paths (ValidateCustom_VolumeMount_MountPath). This check reads no
	// sandbox class.
	if value.GetRuntime().GetNestedContainers().GetEnabled() {
		store := ocispec.NestedImageStorePath
		for i, ctr := range value.GetContainers() {
			for j, mount := range ctr.GetVolumeMounts() {
				p := mount.GetMountPath()
				if p == "" {
					continue // required is enforced by tags
				}
				if p == store || strings.HasPrefix(p, store+"/") || strings.HasPrefix(store, p+"/") {
					errs = append(errs, field.Invalid(
						fldPath.Child("containers").Index(i).Child("volume_mounts").Index(j).Child("mount_path"),
						p, fmt.Sprintf("must not be at, under or above %s, where runtime.nested_containers mounts the image-store tmpfs", store)))
				}
			}
		}
	}
	declared := make(map[string]bool, len(value.GetVolumes()))
	for _, vol := range value.GetVolumes() {
		declared[vol.GetName()] = true
	}
	for i, ctr := range value.GetContainers() {
		for j, mount := range ctr.GetVolumeMounts() {
			name := mount.GetName()
			if name == "" {
				continue // required is enforced by tags
			}
			if !declared[name] {
				errs = append(errs, field.Invalid(
					fldPath.Child("containers").Index(i).Child("volume_mounts").Index(j).Child("name"),
					name, "must reference a volume declared in the template"))
			}
		}
	}
	return errs
}

// Bounds on NestedContainersConfig.image_store_size, as documented on the
// field. The floor keeps a nested runtime's image store usable at all; the
// ceiling bounds the tmpfs a MEMORY snapshot captures as memory.
const (
	minNestedImageStoreBytes = 64 << 20 // 64Mi
	maxNestedImageStoreBytes = 64 << 30 // 64Gi
)

// NestedImageStoreBytes parses an image_store_size quantity into whole bytes
// within [minNestedImageStoreBytes, maxNestedImageStoreBytes]. Admission and
// the ate-api-server -> atelet plumbing both call it, so one rule governs what
// reaches the worker. The bounds are compared on the Quantity itself, BEFORE
// Value(): Value() saturates anything at or above 8Ei to math.MaxInt64 and
// returns 0 for some out-of-range forms (10E, 1e100), and rounds a fraction up
// (1m and 0.5 become 1 byte), so a check on its result would pass all of them.
func NestedImageStoreBytes(s string) (int64, error) {
	q, err := resource.ParseQuantity(s)
	if err != nil {
		return 0, fmt.Errorf("must be a Kubernetes resource quantity: %v", err)
	}
	if q.Cmp(*resource.NewQuantity(minNestedImageStoreBytes, resource.BinarySI)) < 0 ||
		q.Cmp(*resource.NewQuantity(maxNestedImageStoreBytes, resource.BinarySI)) > 0 {
		return 0, errors.New("must be between 64Mi and 64Gi")
	}
	// In range, so Value() is exact for whole numbers; a fraction rounds up and
	// no longer compares equal.
	v := q.Value()
	if resource.NewQuantity(v, resource.BinarySI).Cmp(q) != 0 {
		return 0, errors.New("must be a whole number of bytes")
	}
	return v, nil
}

// ValidateCustom_NestedContainersConfig enforces the opt-in's dependencies at
// admission (not at run time): packet_socket_write and image_store_size are
// only meaningful when enabled, and image_store_size must be a whole number of
// bytes within the documented bounds (see NestedImageStoreBytes). Rejecting a
// half-configured opt-in here keeps it from reaching the worker, where a flag
// silently ignored would make the template lie about the actor's sandbox.
func ValidateCustom_NestedContainersConfig(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *ateapi.NestedContainersConfig) field.ErrorList {
	if value == nil {
		return nil
	}
	var errs field.ErrorList
	if !value.GetEnabled() {
		if value.GetPacketSocketWrite() {
			errs = append(errs, field.Forbidden(fldPath.Child("packet_socket_write"), "requires enabled"))
		}
		if value.GetImageStoreSize() != "" {
			errs = append(errs, field.Forbidden(fldPath.Child("image_store_size"), "requires enabled"))
		}
	}
	if s := value.GetImageStoreSize(); s != "" {
		if _, err := NestedImageStoreBytes(s); err != nil {
			errs = append(errs, field.Invalid(fldPath.Child("image_store_size"), s, err.Error()))
		}
	}
	return errs
}

// ValidateCustom_ActorTemplate_Resources requires both a cpu and a memory
// limit, which the scheduler books against worker capacity.
func ValidateCustom_ActorTemplate_Resources(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *ateapi.ResourceRequirements) field.ErrorList {
	var errs field.ErrorList
	if value.GetLimits().GetCpuMillicores() == 0 {
		errs = append(errs, field.Required(fldPath.Child("limits", "cpu_millicores"), ""))
	}
	if value.GetLimits().GetMemoryBytes() == 0 {
		errs = append(errs, field.Required(fldPath.Child("limits", "memory_bytes"), ""))
	}
	return errs
}

func ValidateCustom_HttpGetAction_Path(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return resources.ValidateHTTPGetPath(fldPath, *value)
}

func ValidateCustom_VolumeMount_MountPath(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return resources.ValidateMountPath(fldPath, *value)
}

// ValidateCustom_Container_VolumeMounts rejects mounts that nest under one
// another. Mount-path uniqueness is enforced by the list key.
func ValidateCustom_Container_VolumeMounts(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ []*ateapi.VolumeMount) field.ErrorList {
	paths := make([]string, len(value))
	for i, m := range value {
		paths[i] = m.GetMountPath()
	}
	return resources.ValidateNestedMountPaths(fldPath, paths)
}

func ValidateCustom_ActorMetadataItem_Path(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return resources.ValidateProjectedPath(fldPath, *value)
}

func ValidateCustom_TrustBundleDataSource_Path(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return resources.ValidateProjectedPath(fldPath, *value)
}

// ValidateCustom_SystemInfoVolumeSource_DataSources allows at most one
// actor_metadata entry and requires every projected file path to be unique
// across all data sources: atelet writes them in order into one tree, so a
// repeated path silently clobbers the earlier file.
func ValidateCustom_SystemInfoVolumeSource_DataSources(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ []*ateapi.SystemInfoDataSource) field.ErrorList {
	var errs field.ErrorList
	seen := sets.New[string]()
	sawMetadata := false
	for i, ds := range value {
		switch {
		case ds == nil:
		case ds.TrustBundle != nil:
			if seen.Has(ds.TrustBundle.Path) {
				errs = append(errs, field.Duplicate(fldPath.Index(i).Child("trust_bundle", "path"), ds.TrustBundle.Path))
			}
			seen.Insert(ds.TrustBundle.Path)
		case ds.ActorMetadata != nil:
			if sawMetadata {
				errs = append(errs, field.Forbidden(fldPath.Index(i).Child("actor_metadata"), "at most one actor_metadata entry may appear"))
			}
			sawMetadata = true
			for j, item := range ds.ActorMetadata.Items {
				if item == nil {
					continue
				}
				if seen.Has(item.Path) {
					errs = append(errs, field.Duplicate(fldPath.Index(i).Child("actor_metadata", "items").Index(j).Child("path"), item.Path))
				}
				seen.Insert(item.Path)
			}
		}
	}
	return errs
}

func ValidateCustom_ImageVolumeSource_Reference(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return resources.ValidatePinnedImage(fldPath, *value)
}

func ValidateCustom_Container_Image(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return resources.ValidatePinnedImage(fldPath, *value)
}

// ValidateCustom_SnapshotConfig_StorageLocation checks an ActorTemplate's
// snapshotConfig.storageLocation with resources.ValidateSnapshotLocation, so a
// bad location fails fast.
func ValidateCustom_SnapshotConfig_StorageLocation(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	if err := resources.ValidateSnapshotLocation(*value); err != nil {
		return field.ErrorList{field.Invalid(fldPath, *value, err.Error())}
	}
	return nil
}

func ValidateCustom_EnvVar_Name(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return resources.ValidateEnvVarName(fldPath, *value)
}

func ValidateCustom_Capabilities_Add(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ []string) field.ErrorList {
	return resources.ValidateAddCapabilities(fldPath, value)
}

func ValidateCustom_Capabilities_Drop(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ []string) field.ErrorList {
	return resources.ValidateDropCapabilities(fldPath, value)
}
