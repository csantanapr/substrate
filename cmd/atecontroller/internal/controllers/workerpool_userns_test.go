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

// Tests for the one-shot guest-userns initContainer a network worker pod runs
// before the worker (applyGuestUsernsInitContainer). Each guard is proved both
// ways: a network pool has exactly one such initContainer with exactly
// SYS_ADMIN+SYS_RESOURCE and the worker image; a colocated pool has none; the
// worker container's own capability set never gains SYS_RESOURCE. The rejection
// test drives the SAME complete-invariant helper the positive test uses, so its
// rejections carry over to the positive assertion (the same pattern as the volume test).
package controllers

import (
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"

	"go.ate.dev/substrate/internal/egresspolicy"
	"go.ate.dev/substrate/internal/installdefaults"
)

// guestUsernsInit returns the pod's initContainers named guest-userns (there
// must be exactly one; the slice lets the count be asserted).
func guestUsernsInits(ps *corev1ac.PodSpecApplyConfiguration) []corev1ac.ContainerApplyConfiguration {
	var out []corev1ac.ContainerApplyConfiguration
	for _, c := range ps.InitContainers {
		if c.Name != nil && *c.Name == guestUsernsInitContainerName {
			out = append(out, c)
		}
	}
	return out
}

// workerContainerOf returns the worker container from a pod spec.
func workerContainerOf(t *testing.T, ps *corev1ac.PodSpecApplyConfiguration) *corev1ac.ContainerApplyConfiguration {
	t.Helper()
	for i := range ps.Containers {
		if ps.Containers[i].Name != nil && *ps.Containers[i].Name == "worker" {
			return &ps.Containers[i]
		}
	}
	t.Fatal("worker container not found")
	return nil
}

// capsOf returns the (add, drop) capability lists of a container's security
// context, or nils if unset.
func capsOf(c *corev1ac.ContainerApplyConfiguration) (add, drop []corev1.Capability) {
	if c == nil || c.SecurityContext == nil || c.SecurityContext.Capabilities == nil {
		return nil, nil
	}
	return c.SecurityContext.Capabilities.Add, c.SecurityContext.Capabilities.Drop
}

// guestUsernsInitInvariant is THE invariant the network worker's guest-userns
// initContainer must satisfy, factored out so the positive test and the rejection
// test exercise the SAME complete check. Returns "" when every clause holds,
// else the first clause it fails. The clauses mirror, one for one, the init
// container's requirements:
//
//   - exactly one initContainer named guest-userns;
//   - its image equals the worker container's image (same binary);
//   - it carries the one-shot --setup-guest-userns arg;
//   - its securityContext is NOT privileged, runs as uid/gid 0, drops ALL and
//     adds EXACTLY SYS_ADMIN and SYS_RESOURCE (no more, no fewer), with seccomp
//     and AppArmor Unconfined;
//   - the WORKER container never gains SYS_RESOURCE.
func guestUsernsInitInvariant(t *testing.T, ps *corev1ac.PodSpecApplyConfiguration) string {
	t.Helper()
	inits := guestUsernsInits(ps)
	if len(inits) != 1 {
		return "guest-userns initContainer count != 1"
	}
	ic := inits[0]

	worker := workerContainerOf(t, ps)
	if worker.Image == nil {
		return "worker container has no image to compare against"
	}
	if ic.Image == nil || *ic.Image != *worker.Image {
		return "initContainer image != worker image"
	}

	// Args must be EXACTLY [--setup-guest-userns]: a "contains" check would pass
	// an extra-argument variant.
	if !slices.Equal(ic.Args, []string{guestUsernsSetupArg}) {
		return "initContainer args != exactly [--setup-guest-userns]"
	}

	sc := ic.SecurityContext
	if sc == nil {
		return "initContainer has no securityContext"
	}
	if sc.Privileged == nil || *sc.Privileged {
		return "initContainer is privileged or does not set privileged=false"
	}
	if sc.RunAsUser == nil || *sc.RunAsUser != 0 || sc.RunAsGroup == nil || *sc.RunAsGroup != 0 {
		return "initContainer does not run as uid/gid 0"
	}
	if sc.Capabilities == nil {
		return "initContainer has no capabilities"
	}
	if !slices.Equal(sc.Capabilities.Drop, []corev1.Capability{"ALL"}) {
		return "initContainer does not drop exactly ALL"
	}
	wantAdd := []corev1.Capability{"SYS_ADMIN", "SYS_RESOURCE"}
	gotAdd := slices.Clone(sc.Capabilities.Add)
	slices.Sort(gotAdd)
	wantSorted := slices.Clone(wantAdd)
	slices.Sort(wantSorted)
	if !slices.Equal(gotAdd, wantSorted) {
		return "initContainer add caps != {SYS_ADMIN, SYS_RESOURCE}"
	}
	if sc.SeccompProfile == nil || sc.SeccompProfile.Type == nil || *sc.SeccompProfile.Type != corev1.SeccompProfileTypeUnconfined {
		return "initContainer seccomp is not Unconfined"
	}
	if sc.AppArmorProfile == nil || sc.AppArmorProfile.Type == nil || *sc.AppArmorProfile.Type != corev1.AppArmorProfileTypeUnconfined {
		return "initContainer AppArmor is not Unconfined"
	}

	// The worker container must NOT have gained SYS_RESOURCE.
	add, _ := capsOf(worker)
	if slices.Contains(add, corev1.Capability("SYS_RESOURCE")) {
		return "worker container was given SYS_RESOURCE"
	}
	return ""
}

// TestGuestUsernsInitContainerPresentOnNetworkPod proves a network worker pod
// satisfies the full invariant.
func TestGuestUsernsInitContainerPresentOnNetworkPod(t *testing.T) {
	ps := networkPodSpec(t, buildNetworkPool(nil))
	if fail := guestUsernsInitInvariant(t, ps); fail != "" {
		t.Fatalf("network worker guest-userns initContainer invariant: %s", fail)
	}
}

// TestGuestUsernsInitContainerAbsentOnColocatedPod proves a colocated worker
// pod has no such initContainer at all.
func TestGuestUsernsInitContainerAbsentOnColocatedPod(t *testing.T) {
	ps := networkPodSpec(t, testWorkerPoolApplyConfig(nil))
	if n := len(guestUsernsInits(ps)); n != 0 {
		t.Errorf("colocated worker must have no guest-userns initContainer; got %d", n)
	}
	// And the colocated worker must not carry SYS_RESOURCE either.
	add, _ := capsOf(workerContainerOf(t, ps))
	if slices.Contains(add, corev1.Capability("SYS_RESOURCE")) {
		t.Errorf("colocated worker must not have SYS_RESOURCE")
	}
}

// TestGuestUsernsInitContainerIdempotent proves a second apply over the same
// config still yields exactly one initContainer (remove-then-add).
func TestGuestUsernsInitContainerIdempotent(t *testing.T) {
	ps := networkPodSpec(t, buildNetworkPool(nil))
	containerAC := workerContainerOf(t, ps)
	applyNetworkTransport(ps, containerAC, map[string]string{}, networkTransport(), installdefaults.SystemNamespace)
	if n := len(guestUsernsInits(ps)); n != 1 {
		t.Errorf("guest-userns initContainer appears %d times after a second apply, want exactly 1", n)
	}
	if fail := guestUsernsInitInvariant(t, ps); fail != "" {
		t.Errorf("invariant broken after a second apply: %s", fail)
	}
}

// TestGuestUsernsInitInvariantRejectsMutants proves the invariant REJECTS each
// broken shape it guards against, by mutating a real network pod spec and asserting
// the complete invariant fails. Because the positive test uses the SAME helper,
// each rejection here is a property the positive assertion actually checks (it
// is not vacuous).
func TestGuestUsernsInitInvariantRejectsMutants(t *testing.T) {
	// mutate takes a fresh, valid network pod spec and breaks it one way.
	mutants := map[string]func(ps *corev1ac.PodSpecApplyConfiguration){
		"remove the init container": func(ps *corev1ac.PodSpecApplyConfiguration) {
			ps.InitContainers = slices.DeleteFunc(ps.InitContainers, func(c corev1ac.ContainerApplyConfiguration) bool {
				return c.Name != nil && *c.Name == guestUsernsInitContainerName
			})
		},
		"add SYS_RESOURCE to the worker container": func(ps *corev1ac.PodSpecApplyConfiguration) {
			w := workerContainerOf(t, ps)
			w.SecurityContext.Capabilities.Add = append(w.SecurityContext.Capabilities.Add, "SYS_RESOURCE")
		},
		"give the init container an extra cap": func(ps *corev1ac.PodSpecApplyConfiguration) {
			for i := range ps.InitContainers {
				if ps.InitContainers[i].Name != nil && *ps.InitContainers[i].Name == guestUsernsInitContainerName {
					ps.InitContainers[i].SecurityContext.Capabilities.Add = append(ps.InitContainers[i].SecurityContext.Capabilities.Add, "SYS_PTRACE")
				}
			}
		},
		"drop a required cap from the init container": func(ps *corev1ac.PodSpecApplyConfiguration) {
			for i := range ps.InitContainers {
				if ps.InitContainers[i].Name != nil && *ps.InitContainers[i].Name == guestUsernsInitContainerName {
					ps.InitContainers[i].SecurityContext.Capabilities.Add = []corev1.Capability{"SYS_ADMIN"}
				}
			}
		},
		"use a different image": func(ps *corev1ac.PodSpecApplyConfiguration) {
			for i := range ps.InitContainers {
				if ps.InitContainers[i].Name != nil && *ps.InitContainers[i].Name == guestUsernsInitContainerName {
					ps.InitContainers[i].WithImage("someother:image")
				}
			}
		},
		"make the init container privileged": func(ps *corev1ac.PodSpecApplyConfiguration) {
			for i := range ps.InitContainers {
				if ps.InitContainers[i].Name != nil && *ps.InitContainers[i].Name == guestUsernsInitContainerName {
					ps.InitContainers[i].SecurityContext.WithPrivileged(true)
				}
			}
		},
		"drop the setup-guest-userns arg": func(ps *corev1ac.PodSpecApplyConfiguration) {
			for i := range ps.InitContainers {
				if ps.InitContainers[i].Name != nil && *ps.InitContainers[i].Name == guestUsernsInitContainerName {
					ps.InitContainers[i].Args = nil
				}
			}
		},
		"add an extra arg to the init container": func(ps *corev1ac.PodSpecApplyConfiguration) {
			for i := range ps.InitContainers {
				if ps.InitContainers[i].Name != nil && *ps.InitContainers[i].Name == guestUsernsInitContainerName {
					ps.InitContainers[i].Args = append(ps.InitContainers[i].Args, "--something-else")
				}
			}
		},
		"make seccomp not Unconfined": func(ps *corev1ac.PodSpecApplyConfiguration) {
			for i := range ps.InitContainers {
				if ps.InitContainers[i].Name != nil && *ps.InitContainers[i].Name == guestUsernsInitContainerName {
					ps.InitContainers[i].SecurityContext.SeccompProfile.WithType(corev1.SeccompProfileTypeRuntimeDefault)
				}
			}
		},
	}

	for name, mutate := range mutants {
		t.Run(name, func(t *testing.T) {
			ps := networkPodSpec(t, buildNetworkPool(nil))
			// sanity: the unmutated spec passes
			if fail := guestUsernsInitInvariant(t, ps); fail != "" {
				t.Fatalf("precondition: unmutated network pod should satisfy the invariant, got: %s", fail)
			}
			mutate(ps)
			if fail := guestUsernsInitInvariant(t, ps); fail == "" {
				t.Errorf("variant %q was NOT rejected by the invariant", name)
			}
		})
	}
}

// TestGuestUsernsInitContainerAppliedToColocatedPoolIsRejected proves the
// "apply it to colocated pods too" variant is caught: if applyGuestUsernsInitContainer
// were called unconditionally (not only inside applyNetworkTransport's nt != nil
// path), a colocated pod would carry the init container. We assert the colocated
// pod has none, which is the observable consequence.
func TestGuestUsernsInitContainerAppliedToColocatedPoolIsRejected(t *testing.T) {
	// A colocated pod never runs applyNetworkTransport's body (nt is nil), so it
	// must have no guest-userns init container.
	ps := buildDeploymentApplyConfig(testWorkerPoolApplyConfig(nil), workerOTelSettings{}, installdefaults.SystemNamespace, installdefaults.AteletServiceAccount, installdefaults.RouterServiceAccount, egresspolicy.DNSEgressPolicyEnforceInAtunnel).Spec.Template.Spec
	if n := len(guestUsernsInits(ps)); n != 0 {
		t.Errorf("colocated pod carries %d guest-userns initContainer(s); applyGuestUsernsInitContainer must run only for a network pool", n)
	}
}
