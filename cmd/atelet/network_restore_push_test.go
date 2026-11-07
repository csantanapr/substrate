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
	"errors"
	"testing"

	"go.ate.dev/substrate/internal/proto/ateletpb"
	wspb "go.ate.dev/substrate/internal/proto/workerstagingpb"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Worker Pod UIDs for the per-worker tests below: networkFor(t, pushNetworkWorker)
// lists the first as a network worker, so the second is a colocated worker on
// the same node.
const (
	pushNetworkWorker   = "22222222-2222-2222-2222-222222222222"
	pushColocatedWorker = "33333333-3333-3333-3333-333333333333"
)

// TestValidateNetworkSpecRejectsNodeMountedVolumes proves that a workload spec
// a network worker cannot serve -- here a node-mounted external CSI volume --
// is refused for a network worker, while a spec it can serve (a durable-dir
// volume, staged with the actor state) is accepted. A colocated worker is
// refused nothing, whether or not network workers are enabled.
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
	network := &Atelet{network: networkFor(t, pushNetworkWorker)}
	colocated := &Atelet{} // network nil

	for _, tc := range []struct {
		name     string
		s        *Atelet
		worker   string
		spec     *ateletpb.WorkloadSpec
		wantCode codes.Code
	}{
		{"network worker, external volume", network, pushNetworkWorker, externalVolumeSpec, codes.InvalidArgument},
		{"network worker, durable-dir volume", network, pushNetworkWorker, durableVolumeSpec, codes.OK},
		{"colocated worker on a network atelet, external volume", network, pushColocatedWorker, externalVolumeSpec, codes.OK},
		{"network workers disabled, external volume", colocated, pushColocatedWorker, externalVolumeSpec, codes.OK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.s.validateNetworkSpec(tc.worker, tc.spec); status.Code(err) != tc.wantCode {
				t.Fatalf("validateNetworkSpec = %v, want code %s", err, tc.wantCode)
			}
		})
	}
}

// TestValidateNetworkSandboxGatesClass proves the network staging path accepts
// the gvisor sandbox class it ships assets for and refuses another class for a
// network worker, while a colocated worker accepts any class, whether or not
// network workers are enabled.
func TestValidateNetworkSandboxGatesClass(t *testing.T) {
	network := &Atelet{network: networkFor(t, pushNetworkWorker)}
	colocated := &Atelet{} // network nil

	for _, tc := range []struct {
		name     string
		s        *Atelet
		worker   string
		class    string
		wantCode codes.Code
	}{
		{"network worker, gvisor", network, pushNetworkWorker, "gvisor", codes.OK},
		{"network worker, microvm", network, pushNetworkWorker, "microvm", codes.InvalidArgument},
		{"colocated worker on a network atelet, microvm", network, pushColocatedWorker, "microvm", codes.OK},
		{"network workers disabled, microvm", colocated, pushColocatedWorker, "microvm", codes.OK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.s.validateNetworkSandbox(tc.worker, tc.class); status.Code(err) != tc.wantCode {
				t.Fatalf("validateNetworkSandbox(%q) = %v, want code %s", tc.class, err, tc.wantCode)
			}
		})
	}
}

// TestNetworkStepsAreNoOpForColocated proves Run, Restore and Checkpoint stage
// and pull nothing for a colocated worker: it reads the node's shared
// filesystem, so no generation is minted or read and no worker is dialed. That
// holds on a network atelet too, where only the listed worker is a network
// worker.
func TestNetworkStepsAreNoOpForColocated(t *testing.T) {
	node := nodeIdentity{NodeName: "node-a", NodeUID: "node-a-incarnation"}
	actor := resources.ActorRef{Atespace: "team", Name: "counter"}
	const actorUID = "11111111-1111-1111-1111-111111111111"

	for _, tc := range []struct {
		name    string
		network *nodeWorkerNetwork
	}{
		{"network workers disabled", nil},
		{"colocated worker on a network atelet", networkFor(t, pushNetworkWorker)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &Atelet{
				network:     tc.network,
				generations: newTestGenerations(t, node),
				staging: stagingFunc(func(context.Context, string) (wspb.WorkerStagingClient, error) {
					t.Error("dialed the staging service of a colocated worker")
					return nil, errors.New("colocated worker has no staging service")
				}),
			}
			ref, err := s.stageNetworkActivation(t.Context(), actor, actorUID, pushColocatedWorker, map[string]string{})
			if err != nil || ref != nil {
				t.Fatalf("stageNetworkActivation = (%v, %v), want (nil, nil)", ref, err)
			}
			ref, err = s.networkActivation(actor, actorUID, pushColocatedWorker)
			if err != nil || ref != nil {
				t.Fatalf("networkActivation = (%v, %v), want (nil, nil)", ref, err)
			}
		})
	}
}
