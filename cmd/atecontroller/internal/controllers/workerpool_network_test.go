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

// Tests for WorkerPool network transport (isolating guest sandbox worker
// pods). Each guard is proved both ways: a pool that opts in and a pool that
// does not. The default (colocated) case is also pinned byte-for-byte by
// TestBuildDeploymentApplyConfig's "default workerpool" case, which these tests
// complement with the specific invariants of the network pod shape:
//
//   - runtimeClassName is set ONLY when NetworkTransport is set (reachability);
//   - the ate.dev/worker-transport=network label is set by the controller, not
//     carried from a user template (the template rejects ate.dev/ keys);
//   - the shared node-filesystem hostPath (ate-base) is dropped for a network
//     worker and kept for a colocated one;
//   - the service account stays the upstream default ("ateworker"); network
//     transport does NOT introduce a different account.
package controllers

import (
	"slices"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"

	atev1 "go.ate.dev/substrate/api/infra/k8s/ate/v1"
	"go.ate.dev/substrate/internal/egresspolicy"
	"go.ate.dev/substrate/internal/installdefaults"
	"go.ate.dev/substrate/internal/nodepath"
)

// networkTransport is the opt-in used across these tests: the cluster's
// sandbox runtime class.
func networkTransport() *atev1.WorkerPoolNetworkTransport {
	return &atev1.WorkerPoolNetworkTransport{RuntimeClassName: "kata-fc"}
}

// buildNetworkPool returns a pool with NetworkTransport set and the given
// template, so each test exercises the same opt-in.
func buildNetworkPool(tmpl *atev1.WorkerPoolPodTemplate) *atev1.WorkerPool {
	wp := testWorkerPoolApplyConfig(tmpl)
	wp.Spec.NetworkTransport = networkTransport()
	return wp
}

func networkPodSpec(t *testing.T, wp *atev1.WorkerPool) *corev1ac.PodSpecApplyConfiguration {
	t.Helper()
	return buildDeploymentApplyConfig(wp, workerOTelSettings{}, installdefaults.SystemNamespace, installdefaults.AteletServiceAccount, installdefaults.RouterServiceAccount, egresspolicy.DNSEgressPolicyEnforceInAtunnel).Spec.Template.Spec
}

// TestNetworkTransportRuntimeClassName proves runtimeClassName is reachable
// ONLY through NetworkTransport: a network pool carries the configured class, a
// default pool carries none. A "runtimeClassName reachable without
// the setting" class fails here if the default branch ever sets it.
func TestNetworkTransportRuntimeClassName(t *testing.T) {
	t.Run("network pool sets the configured runtime class", func(t *testing.T) {
		ps := networkPodSpec(t, buildNetworkPool(nil))
		if ps.RuntimeClassName == nil {
			t.Fatalf("RuntimeClassName not set on a network worker pod")
		}
		if *ps.RuntimeClassName != "kata-fc" {
			t.Errorf("RuntimeClassName = %q, want %q", *ps.RuntimeClassName, "kata-fc")
		}
	})

	t.Run("a different configured class is honoured", func(t *testing.T) {
		wp := testWorkerPoolApplyConfig(nil)
		wp.Spec.NetworkTransport = &atev1.WorkerPoolNetworkTransport{RuntimeClassName: "custom-guest"}
		ps := networkPodSpec(t, wp)
		if ps.RuntimeClassName == nil || *ps.RuntimeClassName != "custom-guest" {
			t.Errorf("RuntimeClassName = %v, want %q", ps.RuntimeClassName, "custom-guest")
		}
	})

	t.Run("default pool sets no runtime class", func(t *testing.T) {
		ps := networkPodSpec(t, testWorkerPoolApplyConfig(nil))
		if ps.RuntimeClassName != nil {
			t.Errorf("RuntimeClassName = %q on a colocated pod, want unset", *ps.RuntimeClassName)
		}
	})
}

// TestNetworkTransportLabel proves the reserved transport label is set by the
// controller when, and only when, NetworkTransport is set, on both the
// Deployment and the pod template, and that the pool selector is NOT widened by
// it (the selector stays the worker-pool label alone, which is immutable).
func TestNetworkTransportLabel(t *testing.T) {
	const key = "ate.dev/worker-transport"

	t.Run("network pool labels the pod and the deployment", func(t *testing.T) {
		d := buildDeploymentApplyConfig(buildNetworkPool(nil), workerOTelSettings{}, installdefaults.SystemNamespace, installdefaults.AteletServiceAccount, installdefaults.RouterServiceAccount, egresspolicy.DNSEgressPolicyEnforceInAtunnel)
		if got := d.Labels[key]; got != "network" {
			t.Errorf("Deployment label %s = %q, want network", key, got)
		}
		if got := d.Spec.Template.Labels[key]; got != "network" {
			t.Errorf("pod template label %s = %q, want network", key, got)
		}
		// The selector must not gain the transport label: the Deployment
		// selector is immutable, and the worker-pool label alone already
		// identifies the pool's pods.
		if _, ok := d.Spec.Selector.MatchLabels[key]; ok {
			t.Errorf("selector must not carry %s; it would make the selector pool-and-transport specific", key)
		}
	})

	t.Run("default pool carries no transport label", func(t *testing.T) {
		d := buildDeploymentApplyConfig(testWorkerPoolApplyConfig(nil), workerOTelSettings{}, installdefaults.SystemNamespace, installdefaults.AteletServiceAccount, installdefaults.RouterServiceAccount, egresspolicy.DNSEgressPolicyEnforceInAtunnel)
		if _, ok := d.Labels[key]; ok {
			t.Errorf("colocated Deployment must not carry %s", key)
		}
		if _, ok := d.Spec.Template.Labels[key]; ok {
			t.Errorf("colocated pod must not carry %s", key)
		}
	})
}

// ateBaseVolumeOf returns the ate-base volume from a pod spec, or nil.
func ateBaseVolumeOf(ps *corev1ac.PodSpecApplyConfiguration) *corev1ac.VolumeApplyConfiguration {
	for i := range ps.Volumes {
		v := &ps.Volumes[i]
		if v.Name != nil && *v.Name == "ate-base" {
			return v
		}
	}
	return nil
}

// ateBaseMountsOf returns every worker-container volume mount named ate-base.
func ateBaseMountsOf(ps *corev1ac.PodSpecApplyConfiguration) []corev1ac.VolumeMountApplyConfiguration {
	var out []corev1ac.VolumeMountApplyConfiguration
	for _, c := range ps.Containers {
		if c.Name == nil || *c.Name != "worker" {
			continue
		}
		for _, m := range c.VolumeMounts {
			if m.Name != nil && *m.Name == "ate-base" {
				out = append(out, m)
			}
		}
	}
	return out
}

// TestNetworkTransportReplacesHostPathWithEmptyDir proves the ate-base volume is
// REPLACED, not dropped, for a network worker: the shared node-filesystem
// hostPath is gone (a separate guest shares nothing from the node), and in its
// place is a writable emptyDir of the DEFAULT medium mounted at the same path
// (nodepath.BasePath). That real, non-overlayfs directory is what lets the
// worker mount per-container overlay rootfs upperdirs under BasePath inside the
// guest; without it the upperdir would land on the container's own overlayfs
// root, and the kernel refuses an overlay upperdir on overlayfs.
// A colocated worker keeps the hostPath untouched.
func TestNetworkTransportReplacesHostPathWithEmptyDir(t *testing.T) {
	t.Run("network worker satisfies the full ate-base invariant", func(t *testing.T) {
		// The single source of truth: the same helper the rejection test proves
		// is non-vacuous. If this passes, every clause (count, emptyDir, default
		// medium, no sizeLimit, no hostPath, one writable mount at BasePath)
		// holds. The detailed sub-assertions below are kept for diagnosis.
		if reason := ateBaseIsDefaultEmptyDirAtBasePath(networkPodSpec(t, buildNetworkPool(nil))); reason != "" {
			t.Fatalf("network worker fails the ate-base invariant: %s", reason)
		}
	})

	t.Run("network worker: exactly one ate-base emptyDir (default medium), no hostPath", func(t *testing.T) {
		ps := networkPodSpec(t, buildNetworkPool(nil))

		// Exactly one volume named ate-base, and it is an emptyDir, not a
		// hostPath. Count explicitly so a duplicate (e.g. a non-idempotent
		// add) is caught here, not only by the idempotency test.
		n := 0
		for _, v := range ps.Volumes {
			if v.Name != nil && *v.Name == "ate-base" {
				n++
			}
		}
		if n != 1 {
			t.Fatalf("network worker has %d ate-base volumes, want exactly 1", n)
		}
		v := ateBaseVolumeOf(ps)
		if v.HostPath != nil {
			t.Errorf("network worker ate-base is still a hostPath (%v); a separate guest shares no node filesystem", v.HostPath)
		}
		if v.EmptyDir == nil {
			t.Fatalf("network worker ate-base is not an emptyDir; got %+v", v)
		}
		// Default medium: by the Kubernetes contract empty/unset Medium uses the
		// storage backing the kubelet root directory, not a tmpfs (medium
		// Memory). We require the default, non-Memory medium only to avoid a
		// tmpfs backing we have not proven; the actual separation from the
		// container overlay is a runtime fact of the sandbox the live run confirms, not a
		// default-medium guarantee. (No claim that overlayfs rejects a tmpfs
		// upperdir; current kernels accept one.)
		if v.EmptyDir.Medium != nil && *v.EmptyDir.Medium != "" {
			t.Errorf("network worker ate-base emptyDir medium = %q, want default (non-Memory) backing", *v.EmptyDir.Medium)
		}
		// No deliberate sizeLimit: assets, image layers and actor state land
		// here and a small cap would evict them mid-actor.
		if v.EmptyDir.SizeLimit != nil {
			t.Errorf("network worker ate-base emptyDir sizeLimit = %v, want unset", v.EmptyDir.SizeLimit)
		}

		// No OTHER hostPath leaks onto a network worker pod (a separate guest
		// shares nothing from the node filesystem).
		for _, v := range ps.Volumes {
			if v.HostPath != nil {
				t.Errorf("network worker pod carries a hostPath volume %v; a separate guest shares no node filesystem", v.Name)
			}
		}
	})

	t.Run("network worker: exactly one ate-base mount at BasePath", func(t *testing.T) {
		ps := networkPodSpec(t, buildNetworkPool(nil))
		mounts := ateBaseMountsOf(ps)
		if len(mounts) != 1 {
			t.Fatalf("network worker has %d ate-base mounts, want exactly 1", len(mounts))
		}
		m := mounts[0]
		if m.MountPath == nil || *m.MountPath != nodepath.BasePath {
			t.Errorf("network worker ate-base mountPath = %v, want %q", m.MountPath, nodepath.BasePath)
		}
		// Writable: the worker MkdirAlls its subtree and writes overlay
		// upperdirs here, so the mount must not be read-only.
		if m.ReadOnly != nil && *m.ReadOnly {
			t.Errorf("network worker ate-base mount is read-only; the worker must be able to write its subtree and overlay upperdirs")
		}
	})

	t.Run("colocated worker keeps the shared hostPath", func(t *testing.T) {
		ps := networkPodSpec(t, testWorkerPoolApplyConfig(nil))
		v := ateBaseVolumeOf(ps)
		if v == nil || v.HostPath == nil {
			t.Fatalf("colocated worker must keep the ate-base hostPath volume; got %+v", v)
		}
		if len(ateBaseMountsOf(ps)) != 1 {
			t.Errorf("colocated worker must keep exactly one ate-base mount; got %d", len(ateBaseMountsOf(ps)))
		}
	})
}

// TestNetworkTransportVolumeIdempotent proves applying the network shaping twice
// over the same config does not duplicate the ate-base emptyDir volume or its
// mount. applyNetworkTransport removes any existing ate-base before adding, so a
// second pass still yields exactly one emptyDir volume and one mount. (This is
// the volume analog of TestNetworkTransportCapabilityIdempotent; args are not
// in scope, as WithArgs appends and the controller rebuilds each reconcile.)
func TestNetworkTransportVolumeIdempotent(t *testing.T) {
	ps := networkPodSpec(t, buildNetworkPool(nil))
	var containerAC *corev1ac.ContainerApplyConfiguration
	for i := range ps.Containers {
		if ps.Containers[i].Name != nil && *ps.Containers[i].Name == "worker" {
			containerAC = &ps.Containers[i]
		}
	}
	if containerAC == nil {
		t.Fatal("worker container not found")
	}

	applyNetworkTransport(ps, containerAC, map[string]string{}, networkTransport(), installdefaults.SystemNamespace)

	nVol := 0
	for _, v := range ps.Volumes {
		if v.Name != nil && *v.Name == "ate-base" {
			nVol++
			if v.EmptyDir == nil {
				t.Errorf("ate-base volume is not an emptyDir after a second apply: %+v", v)
			}
			if v.HostPath != nil {
				t.Errorf("ate-base volume became a hostPath after a second apply: %+v", v.HostPath)
			}
		}
	}
	if nVol != 1 {
		t.Errorf("ate-base volume appears %d times after a second apply, want exactly 1", nVol)
	}

	nMnt := 0
	for _, m := range containerAC.VolumeMounts {
		if m.Name != nil && *m.Name == "ate-base" {
			nMnt++
		}
	}
	if nMnt != 1 {
		t.Errorf("ate-base mount appears %d times after a second apply, want exactly 1", nMnt)
	}
}

// ateBaseIsDefaultEmptyDirAtBasePath is THE invariant a network worker's
// ate-base volume must satisfy, factored out so the positive test
// (TestNetworkTransportReplacesHostPathWithEmptyDir) and the rejection test
// (TestNetworkTransportVolumeInvariantRejectsMutants) exercise the SAME complete
// check: the positive test calls it to prove the real pod satisfies it, the
// rejection test calls it to prove it REJECTS each broken shape. Keeping both on
// one helper is what makes the rejection test's non-vacuity carry over to the
// positive assertion. Returns "" when the pod satisfies every clause, else the
// first clause it fails. The clauses mirror, one for one, the properties of the
// pod shape: exactly one ate-base, an emptyDir (not hostPath) of
// default medium with no sizeLimit, no other hostPath on the pod, and exactly
// one writable mount at nodepath.BasePath.
func ateBaseIsDefaultEmptyDirAtBasePath(ps *corev1ac.PodSpecApplyConfiguration) string {
	n := 0
	for _, v := range ps.Volumes {
		if v.Name != nil && *v.Name == "ate-base" {
			n++
		}
	}
	if n != 1 {
		return "ate-base volume count != 1"
	}
	v := ateBaseVolumeOf(ps)
	if v == nil {
		return "no ate-base volume"
	}
	if v.HostPath != nil {
		return "ate-base is a hostPath"
	}
	if v.EmptyDir == nil {
		return "ate-base is not an emptyDir"
	}
	// Default medium -- NOT Memory (tmpfs). By the Kubernetes contract default
	// uses kubelet-root-backing storage; we require it only to avoid an
	// unproven tmpfs backing. (No claim that overlayfs rejects a tmpfs upperdir;
	// the container-overlay separation is a runtime fact of the sandbox the live run
	// confirms, not a default-medium guarantee.)
	if v.EmptyDir.Medium != nil && *v.EmptyDir.Medium != "" {
		return "ate-base emptyDir medium is not default (must not be Memory)"
	}
	// No sizeLimit: a small cap would evict assets/layers/actor state mid-actor.
	if v.EmptyDir.SizeLimit != nil {
		return "ate-base emptyDir has a sizeLimit set"
	}
	for _, other := range ps.Volumes {
		if other.HostPath != nil {
			return "pod carries a hostPath volume"
		}
	}
	mounts := ateBaseMountsOf(ps)
	if len(mounts) != 1 {
		return "ate-base mount count != 1"
	}
	if mounts[0].MountPath == nil || *mounts[0].MountPath != nodepath.BasePath {
		return "ate-base mount not at BasePath"
	}
	// Writable: the worker MkdirAlls its subtree and writes overlay upperdirs.
	if mounts[0].ReadOnly != nil && *mounts[0].ReadOnly {
		return "ate-base mount is read-only"
	}
	return ""
}

// TestNetworkTransportVolumeInvariantRejectsMutants proves the emptyDir
// invariant is not vacuous: it ACCEPTS the real network pod and REJECTS each of
// two likely regressions -- removing the new volume, and turning
// it back into a hostPath. If the invariant check were a no-op (always ""),
// these broken cases would pass and this test would fail.
func TestNetworkTransportVolumeInvariantRejectsMutants(t *testing.T) {
	// Accepts the real pod.
	if reason := ateBaseIsDefaultEmptyDirAtBasePath(networkPodSpec(t, buildNetworkPool(nil))); reason != "" {
		t.Fatalf("invariant rejected the real network pod: %s", reason)
	}

	// Case 1: the new volume is removed entirely (regression to the original
	// "drop and add nothing" that caused the overlay-upperdir failure).
	t.Run("rejects a pod with no ate-base volume", func(t *testing.T) {
		ps := networkPodSpec(t, buildNetworkPool(nil))
		ps.Volumes = slices.DeleteFunc(ps.Volumes, func(v corev1ac.VolumeApplyConfiguration) bool {
			return v.Name != nil && *v.Name == "ate-base"
		})
		if reason := ateBaseIsDefaultEmptyDirAtBasePath(ps); reason == "" {
			t.Error("invariant accepted a network pod with the ate-base volume removed; it must reject it")
		}
	})

	// Case 2: ate-base is a hostPath again (the shape a separate guest must
	// never carry).
	t.Run("rejects a pod whose ate-base is a hostPath", func(t *testing.T) {
		ps := networkPodSpec(t, buildNetworkPool(nil))
		v := ateBaseVolumeOf(ps)
		if v == nil {
			t.Fatal("real network pod unexpectedly has no ate-base volume")
		}
		v.EmptyDir = nil
		v.WithHostPath(corev1ac.HostPathVolumeSource().
			WithPath(nodepath.BasePath).
			WithType(corev1.HostPathDirectoryOrCreate))
		if reason := ateBaseIsDefaultEmptyDirAtBasePath(ps); reason == "" {
			t.Error("invariant accepted a network pod whose ate-base is a hostPath; it must reject it")
		}
	})

	// Case 3: the emptyDir is medium Memory (tmpfs) -- a backing we have not
	// proven, unlike the default (non-Memory) medium the live re-proof
	// confirmed. The invariant must reject it. (This does NOT assert overlayfs
	// rejects a tmpfs upperdir; it asserts the medium must stay default.)
	t.Run("rejects a pod whose ate-base emptyDir is medium Memory", func(t *testing.T) {
		ps := networkPodSpec(t, buildNetworkPool(nil))
		v := ateBaseVolumeOf(ps)
		if v == nil || v.EmptyDir == nil {
			t.Fatal("real network pod unexpectedly has no ate-base emptyDir")
		}
		v.EmptyDir.WithMedium(corev1.StorageMediumMemory)
		if reason := ateBaseIsDefaultEmptyDirAtBasePath(ps); reason == "" {
			t.Error("invariant accepted a network pod whose ate-base is medium Memory (tmpfs); it must require the default medium")
		}
	})

	// Case 4: the emptyDir carries a sizeLimit, which could evict
	// assets/layers/actor state mid-actor; the invariant must reject it.
	t.Run("rejects a pod whose ate-base emptyDir has a sizeLimit", func(t *testing.T) {
		ps := networkPodSpec(t, buildNetworkPool(nil))
		v := ateBaseVolumeOf(ps)
		if v == nil || v.EmptyDir == nil {
			t.Fatal("real network pod unexpectedly has no ate-base emptyDir")
		}
		v.EmptyDir.WithSizeLimit(resource.MustParse("1Gi"))
		if reason := ateBaseIsDefaultEmptyDirAtBasePath(ps); reason == "" {
			t.Error("invariant accepted a network pod whose ate-base emptyDir has a sizeLimit; it must reject it")
		}
	})

	// Case 5: the mount is read-only, which would break the worker's
	// MkdirAll and overlay upperdir writes; the invariant must reject it.
	t.Run("rejects a pod whose ate-base mount is read-only", func(t *testing.T) {
		ps := networkPodSpec(t, buildNetworkPool(nil))
		for ci := range ps.Containers {
			if ps.Containers[ci].Name == nil || *ps.Containers[ci].Name != "worker" {
				continue
			}
			for mi := range ps.Containers[ci].VolumeMounts {
				m := &ps.Containers[ci].VolumeMounts[mi]
				if m.Name != nil && *m.Name == "ate-base" {
					m.WithReadOnly(true)
				}
			}
		}
		if reason := ateBaseIsDefaultEmptyDirAtBasePath(ps); reason == "" {
			t.Error("invariant accepted a network pod whose ate-base mount is read-only; it must reject it")
		}
	})
}

// TestNetworkTransportServiceAccount pins a deliberate choice: a network worker pod
// runs as the upstream default service account (installdefaults.
// WorkerServiceAccount, "ateworker"). This change does NOT introduce a separate
// "ate-worker" account; aligning the atelet's network-worker SA flag to this
// constant is deliberate.
func TestNetworkTransportServiceAccount(t *testing.T) {
	for _, tc := range []struct {
		name string
		wp   *atev1.WorkerPool
	}{
		{"network pool", buildNetworkPool(nil)},
		{"default pool", testWorkerPoolApplyConfig(nil)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ps := networkPodSpec(t, tc.wp)
			if ps.ServiceAccountName == nil {
				t.Fatalf("ServiceAccountName not set")
			}
			if *ps.ServiceAccountName != installdefaults.WorkerServiceAccount {
				t.Errorf("ServiceAccountName = %q, want %q", *ps.ServiceAccountName, installdefaults.WorkerServiceAccount)
			}
		})
	}
}

// TestNetworkTransportTemplateCannotSetTransportLabel documents that a user
// cannot set the reserved label itself: the CRD rejects ate.dev/ keys in
// template labels (XValidation on WorkerPoolPodTemplate.Labels), so the
// controller is the only writer. This test asserts the controller's own
// writing is unconditional on the opt-in rather than copied from the template:
// a template that (hypothetically) carried the value does not make a colocated
// pod a network one.
func TestNetworkTransportLabelNotFromTemplate(t *testing.T) {
	const key = "ate.dev/worker-transport"
	// A COLOCATED pool (NetworkTransport nil) whose template labels directly
	// carry the reserved transport key/value. On a live cluster the CRD rejects
	// an ate.dev/ template label (XValidation on WorkerPoolPodTemplate.Labels),
	// but this unit builds the struct directly to prove the apply code's
	// transport decision is driven ONLY by NetworkTransport, never copied from a
	// template label: a colocated pool must not become a network one just
	// because a label says so. (If the apply code ever sourced the decision from
	// labels, this pod would wrongly be treated as a network worker.)
	wp := testWorkerPoolApplyConfig(&atev1.WorkerPoolPodTemplate{
		Labels: map[string]atev1.WorkerPoolLabelValue{
			key:                "network",
			"example.com/team": "blue",
		},
	})
	d := buildDeploymentApplyConfig(wp, workerOTelSettings{}, installdefaults.SystemNamespace, installdefaults.AteletServiceAccount, installdefaults.RouterServiceAccount, egresspolicy.DNSEgressPolicyEnforceInAtunnel)

	// The pod stays colocated: it keeps the shared hostPath and sets no runtime
	// class, regardless of the template label. These assert the transport
	// DECISION was not taken from the label.
	if d.Spec.Template.Spec.RuntimeClassName != nil {
		t.Errorf("template transport label made the pod a network worker: runtimeClassName = %q", *d.Spec.Template.Spec.RuntimeClassName)
	}
	hasAteBase := false
	for _, v := range d.Spec.Template.Spec.Volumes {
		if v.Name != nil && *v.Name == "ate-base" {
			hasAteBase = true
		}
	}
	if !hasAteBase {
		t.Errorf("template transport label made the pod drop its shared hostPath; the decision must come from NetworkTransport, not a label")
	}
	// The user label is still applied (the controller passes template labels
	// through; admission, not the controller, polices reserved keys).
	if got := d.Spec.Template.Labels["example.com/team"]; got != "blue" {
		t.Errorf("user template label dropped: example.com/team = %q, want blue", got)
	}
}

// TestNetworkTransportDefaultUnchanged is the byte-for-byte pin: a pool WITHOUT
// NetworkTransport yields exactly the golden colocated pod. If any network
// branch leaks into the default path this fails.
func TestNetworkTransportDefaultUnchanged(t *testing.T) {
	got := buildDeploymentApplyConfig(testWorkerPoolApplyConfig(nil), workerOTelSettings{}, installdefaults.SystemNamespace, installdefaults.AteletServiceAccount, installdefaults.RouterServiceAccount, egresspolicy.DNSEgressPolicyEnforceInAtunnel)
	want := expectedDeploymentApplyConfig(nil)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("default (colocated) pod drifted (-want +got):\n%s", diff)
	}
}

// workerContainerArgs returns the worker container's args from a built pod
// spec, failing the test if the container is absent.
func workerContainerArgs(t *testing.T, ps *corev1ac.PodSpecApplyConfiguration) []string {
	t.Helper()
	for _, c := range ps.Containers {
		if c.Name != nil && *c.Name == "worker" {
			return c.Args
		}
	}
	t.Fatal("worker container not found in pod spec")
	return nil
}

func hasArg(args []string, want string) bool {
	for _, a := range args {
		if a == want {
			return true
		}
	}
	return false
}

// TestNetworkTransportPassesWorkerFlags proves the controller passes the
// worker-side network flags ONLY to a network pool, and that the atelet-workers
// Service address is built from the install's own namespace (not hard-coded),
// so a relocated install points the worker at the right atelet. A colocated
// pool carries neither flag -- passing them would crash-loop the pod, since the
// worker exits on an unknown flag only when it has not opted in.
func TestNetworkTransportPassesWorkerFlags(t *testing.T) {
	t.Run("network pool carries both worker network flags", func(t *testing.T) {
		args := workerContainerArgs(t, networkPodSpec(t, buildNetworkPool(nil)))
		wantListen := "--worker-listen-address=0.0.0.0:8086"
		wantAtelet := "--atelet-address=atelet-workers." + installdefaults.SystemNamespace + ".svc:8087"
		if !hasArg(args, wantListen) {
			t.Errorf("network worker args missing %q; got %v", wantListen, args)
		}
		if !hasArg(args, wantAtelet) {
			t.Errorf("network worker args missing %q; got %v", wantAtelet, args)
		}
	})

	t.Run("atelet address uses the install namespace, not a hard-coded one", func(t *testing.T) {
		wp := buildNetworkPool(nil)
		d := buildDeploymentApplyConfig(wp, workerOTelSettings{}, "other-system-ns", installdefaults.AteletServiceAccount, installdefaults.RouterServiceAccount, egresspolicy.DNSEgressPolicyEnforceInAtunnel)
		args := workerContainerArgs(t, d.Spec.Template.Spec)
		want := "--atelet-address=atelet-workers.other-system-ns.svc:8087"
		if !hasArg(args, want) {
			t.Errorf("network worker atelet address did not follow the install namespace; want %q, got %v", want, args)
		}
	})

	t.Run("colocated pool carries no worker network flags", func(t *testing.T) {
		args := workerContainerArgs(t, networkPodSpec(t, testWorkerPoolApplyConfig(nil)))
		for _, a := range args {
			if a == "--worker-listen-address=0.0.0.0:8086" ||
				len(a) >= len("--worker-listen-address") && a[:len("--worker-listen-address")] == "--worker-listen-address" ||
				len(a) >= len("--atelet-address") && a[:len("--atelet-address")] == "--atelet-address" {
				t.Errorf("colocated worker must not carry a network flag, found %q", a)
			}
		}
	})
}

// workerSecurityContextFromPod returns the worker container's security context
// apply-config from a built pod spec, failing the test if the container or its
// security context is absent.
func workerSecurityContextFromPod(t *testing.T, ps *corev1ac.PodSpecApplyConfiguration) *corev1ac.SecurityContextApplyConfiguration {
	t.Helper()
	for _, c := range ps.Containers {
		if c.Name != nil && *c.Name == "worker" {
			if c.SecurityContext == nil {
				t.Fatal("worker container has no security context")
			}
			return c.SecurityContext
		}
	}
	t.Fatal("worker container not found in pod spec")
	return nil
}

// capsContain reports whether caps holds want.
func capsContain(caps []corev1.Capability, want corev1.Capability) bool {
	return slices.Contains(caps, want)
}

// countCap returns how many times want appears in caps, to prove no duplicate.
func countCap(caps []corev1.Capability, want corev1.Capability) int {
	n := 0
	for _, c := range caps {
		if c == want {
			n++
		}
	}
	return n
}

// TestNetworkTransportCapabilities proves the one capability change: a network
// worker's security context adds NET_BIND_SERVICE (so it can bind atunnel on the
// privileged :443 inside a guest runtime class that does not set
// net.ipv4.ip_unprivileged_port_start=0), while keeping drop:[ALL] and every
// other capability exactly as workerSecurityContext set it. A colocated worker
// gains nothing.
func TestNetworkTransportCapabilities(t *testing.T) {
	t.Run("network worker adds NET_BIND_SERVICE, drop stays [ALL], base caps unchanged", func(t *testing.T) {
		sc := workerSecurityContextFromPod(t, networkPodSpec(t, buildNetworkPool(nil)))
		if sc.Capabilities == nil {
			t.Fatal("network worker has no capabilities set")
		}
		if !capsContain(sc.Capabilities.Add, netBindServiceCapability) {
			t.Errorf("network worker add caps = %v, want NET_BIND_SERVICE present", sc.Capabilities.Add)
		}
		// drop must still be exactly [ALL].
		if len(sc.Capabilities.Drop) != 1 || sc.Capabilities.Drop[0] != "ALL" {
			t.Errorf("network worker drop caps = %v, want [ALL]", sc.Capabilities.Drop)
		}
		// Every base gVisor capability must still be present: the network
		// change adds one, it does not replace or reorder the set.
		for _, want := range workerGvisorCapabilities {
			if !capsContain(sc.Capabilities.Add, want) {
				t.Errorf("network worker add caps = %v, missing base capability %q", sc.Capabilities.Add, want)
			}
		}
		// The add set is exactly the base gVisor set plus NET_BIND_SERVICE, in
		// that order: base caps first (as workerSecurityContext set them), the
		// new one appended. This pins that nothing else was added or reordered.
		wantAdd := append(append([]corev1.Capability{}, workerGvisorCapabilities...), netBindServiceCapability)
		if diff := cmp.Diff(wantAdd, sc.Capabilities.Add); diff != "" {
			t.Errorf("network worker add caps mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("colocated worker does not gain NET_BIND_SERVICE", func(t *testing.T) {
		sc := workerSecurityContextFromPod(t, networkPodSpec(t, testWorkerPoolApplyConfig(nil)))
		if sc.Capabilities == nil {
			t.Fatal("colocated worker has no capabilities set")
		}
		if capsContain(sc.Capabilities.Add, netBindServiceCapability) {
			t.Errorf("colocated worker must not carry NET_BIND_SERVICE; add caps = %v", sc.Capabilities.Add)
		}
		// A colocated worker's add set is exactly the base gVisor set.
		if diff := cmp.Diff(workerGvisorCapabilities, sc.Capabilities.Add); diff != "" {
			t.Errorf("colocated worker add caps drifted (-want +got):\n%s", diff)
		}
	})
}

// TestNetworkTransportCapabilityIdempotent proves the capability grant is
// idempotent: applying the network transport shaping twice over the same
// container does not duplicate NET_BIND_SERVICE. This scopes to the capability
// only -- applyNetworkTransport's WithArgs appends, so calling the whole
// function twice is NOT byte-for-byte stable for args; the controller never does
// that (each reconcile rebuilds the container from scratch), and this test
// exercises only the addWorkerCapability guard so a future change that calls it
// twice cannot silently grow the capability set.
func TestNetworkTransportCapabilityIdempotent(t *testing.T) {
	// Build once, then run applyNetworkTransport a second time over the same
	// container apply-config with the same opt-in. A second controller pass
	// (reconcile) rebuilds from scratch, but this directly exercises the
	// idempotency guard in applyNetworkTransport/addWorkerCapability so a future
	// change that calls it twice cannot silently duplicate the capability.
	ps := networkPodSpec(t, buildNetworkPool(nil))
	var containerAC *corev1ac.ContainerApplyConfiguration
	for i := range ps.Containers {
		if ps.Containers[i].Name != nil && *ps.Containers[i].Name == "worker" {
			containerAC = &ps.Containers[i]
		}
	}
	if containerAC == nil {
		t.Fatal("worker container not found")
	}

	applyNetworkTransport(ps, containerAC, map[string]string{}, networkTransport(), installdefaults.SystemNamespace)

	if got := countCap(containerAC.SecurityContext.Capabilities.Add, netBindServiceCapability); got != 1 {
		t.Errorf("NET_BIND_SERVICE appears %d times after a second apply, want exactly 1 (add caps = %v)", got, containerAC.SecurityContext.Capabilities.Add)
	}
	if len(containerAC.SecurityContext.Capabilities.Drop) != 1 || containerAC.SecurityContext.Capabilities.Drop[0] != "ALL" {
		t.Errorf("drop caps changed after a second apply: %v, want [ALL]", containerAC.SecurityContext.Capabilities.Drop)
	}
}
