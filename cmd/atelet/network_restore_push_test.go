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
	"testing"

	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestValidateNetworkSpecRejectsNodeMountedVolumes proves that with a network
// transport a workload spec a separate guest cannot serve -- here a
// node-mounted external CSI volume -- is refused, while a spec a network worker
// can serve (a durable-dir volume, staged with the actor state) is accepted.
// For a colocated worker (network nil) nothing is rejected, so the upstream
// path is unchanged.
func TestValidateNetworkSpecRejectsNodeMountedVolumes(t *testing.T) {
	externalVolumeSpec := &ateletpb.WorkloadSpec{
		Volumes: []*ateletpb.Volume{{
			Name:     "data",
			External: &ateletpb.ExternalVolumeSource{},
		}},
	}
	durableVolumeSpec := &ateletpb.WorkloadSpec{
		Volumes: []*ateletpb.Volume{{
			Name:       "state",
			DurableDir: &ateletpb.DurableDirVolumeSource{},
		}},
	}

	network := &Atelet{network: &nodeWorkerNetwork{}}
	if err := network.validateNetworkSpec(externalVolumeSpec); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("network worker accepted a node-mounted external CSI volume: %v", err)
	}
	if err := network.validateNetworkSpec(durableVolumeSpec); err != nil {
		t.Fatalf("network worker rejected a durable-dir volume it can serve: %v", err)
	}

	colocated := &Atelet{} // network nil
	if err := colocated.validateNetworkSpec(externalVolumeSpec); err != nil {
		t.Fatalf("colocated path rejected a spec; the upstream path must be unchanged: %v", err)
	}
}

// TestValidateNetworkSandboxGatesClass proves the network staging path accepts
// the gvisor sandbox class it ships assets for and refuses another class, while
// a colocated worker accepts any class (unchanged upstream behavior).
func TestValidateNetworkSandboxGatesClass(t *testing.T) {
	network := &Atelet{network: &nodeWorkerNetwork{}}
	if err := network.validateNetworkSandbox("gvisor"); err != nil {
		t.Fatalf("network worker rejected the gvisor class: %v", err)
	}
	if err := network.validateNetworkSandbox("microvm"); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("network worker accepted a non-gvisor class: %v", err)
	}

	colocated := &Atelet{} // network nil
	if err := colocated.validateNetworkSandbox("microvm"); err != nil {
		t.Fatalf("colocated path rejected a class; the upstream path must be unchanged: %v", err)
	}
}

// TestStageNetworkActivationIsNoOpForColocated proves Run/Restore stage nothing
// for a colocated worker (network nil): it reads the node's shared filesystem,
// so no generation is minted and no push is attempted.
func TestStageNetworkActivationIsNoOpForColocated(t *testing.T) {
	s := &Atelet{} // network nil
	ref, err := s.stageNetworkActivation(t.Context(), resources.ActorRef{Atespace: "team", Name: "counter"},
		"11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222", map[string]string{})
	if err != nil {
		t.Fatalf("stageNetworkActivation for a colocated worker errored: %v", err)
	}
	if ref != nil {
		t.Fatalf("stageNetworkActivation returned a ref %v for a colocated worker; want nil", ref)
	}
}
