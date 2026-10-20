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

	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// newTestGenerations builds a networkGenerations rooted in a temp dir for the
// given node identity.
func newTestGenerations(t *testing.T, node nodeIdentity) *networkGenerations {
	t.Helper()
	g, err := newNetworkGenerations(t.TempDir(), node)
	if err != nil {
		t.Fatalf("newNetworkGenerations: %v", err)
	}
	return g
}

// TestNetworkActivationMatchesTheRightActorAndWorker proves networkActivation's
// identity/UID checks: it returns the SAME generation prepare persisted only
// when the Checkpoint names the same actor ref, actor UID and worker UID, and
// refuses a request that names a different actor name, atespace, actor UID or
// worker UID -- so a Checkpoint cannot pull another actor's custody.
func TestNetworkActivationMatchesTheRightActorAndWorker(t *testing.T) {
	node := nodeIdentity{NodeName: "node-a", NodeUID: "node-a-incarnation"}
	s := &Atelet{network: &nodeWorkerNetwork{}, generations: newTestGenerations(t, node)}

	actor := resources.ActorRef{Atespace: "team", Name: "counter"}
	actorUID := "11111111-1111-1111-1111-111111111111"
	workerUID := "22222222-2222-2222-2222-222222222222"

	record, err := s.generations.preparePending(actor, actorUID, workerUID)
	if err != nil {
		t.Fatalf("preparePending: %v", err)
	}
	if err := s.generations.commit(record); err != nil {
		t.Fatalf("commit: %v", err)
	}

	ref, err := s.networkActivation(actor, actorUID, workerUID)
	if err != nil {
		t.Fatalf("networkActivation for the matching actor: %v", err)
	}
	if ref == nil || ref.GetGeneration() != record.Generation ||
		ref.GetActorUid() != actorUID || ref.GetWorkerUid() != workerUID {
		t.Fatalf("networkActivation returned %v; want the persisted generation %q for the same actor/worker", ref, record.Generation)
	}

	// A different actor name, atespace, actor UID or worker UID must be refused:
	// each would let a request pull a different actor's custody.
	for name, mut := range map[string]struct {
		actor               resources.ActorRef
		actorUID, workerUID string
	}{
		"different actor name":     {resources.ActorRef{Atespace: "team", Name: "other"}, actorUID, workerUID},
		"different atespace":       {resources.ActorRef{Atespace: "other", Name: "counter"}, actorUID, workerUID},
		"different worker UID":     {actor, actorUID, "33333333-3333-3333-3333-333333333333"},
		"different (absent) actor": {actor, "44444444-4444-4444-4444-444444444444", workerUID},
	} {
		if _, err := s.networkActivation(mut.actor, mut.actorUID, mut.workerUID); err == nil {
			t.Fatalf("%s: networkActivation accepted a request that does not match the persisted activation", name)
		}
	}
}

// TestNetworkActivationFailsClosedWithoutARecord proves a Checkpoint before any
// staging (no persisted generation) is refused rather than inventing a
// generation -- so a pull cannot run against an actor that was never staged.
func TestNetworkActivationFailsClosedWithoutARecord(t *testing.T) {
	node := nodeIdentity{NodeName: "node-a", NodeUID: "node-a-incarnation"}
	s := &Atelet{network: &nodeWorkerNetwork{}, generations: newTestGenerations(t, node)}
	actor := resources.ActorRef{Atespace: "team", Name: "counter"}
	actorUID := "11111111-1111-1111-1111-111111111111"
	workerUID := "22222222-2222-2222-2222-222222222222"
	if _, err := s.networkActivation(actor, actorUID, workerUID); err == nil {
		t.Fatal("networkActivation returned a ref for an actor with no persisted generation")
	}

	// A PENDING generation (staging started but Stage not yet committed) must NOT
	// be visible to a pull: networkActivation reads the COMMITTED record only, so
	// a failed or in-flight stage cannot have its speculative generation pulled.
	if _, err := s.generations.preparePending(actor, actorUID, workerUID); err != nil {
		t.Fatalf("preparePending: %v", err)
	}
	if _, err := s.networkActivation(actor, actorUID, workerUID); err == nil {
		t.Fatal("networkActivation returned a ref for a PENDING (uncommitted) generation")
	}
}

// TestNetworkGenerationRejectsOtherNodeIncarnation proves the persisted record
// is pinned to this node incarnation: a record written by a previous node (a
// different NodeUID) is rejected on read, so a stale record cannot be reused
// after a node is replaced.
func TestNetworkGenerationRejectsOtherNodeIncarnation(t *testing.T) {
	actor := resources.ActorRef{Atespace: "team", Name: "counter"}
	actorUID := "11111111-1111-1111-1111-111111111111"
	workerUID := "22222222-2222-2222-2222-222222222222"

	dir := t.TempDir()
	writer, err := newNetworkGenerations(dir, nodeIdentity{NodeName: "node-a", NodeUID: "old-incarnation"})
	if err != nil {
		t.Fatal(err)
	}
	rec, err := writer.preparePending(actor, actorUID, workerUID)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.commit(rec); err != nil {
		t.Fatal(err)
	}
	// A new node incarnation reading the same directory must reject the record.
	reader, err := newNetworkGenerations(dir, nodeIdentity{NodeName: "node-a", NodeUID: "new-incarnation"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reader.read(actorUID); err == nil {
		t.Fatal("a generation record from a previous node incarnation was accepted")
	}
}

// TestReceiveAndCleanupNetworkCheckpointAreNoOpForColocated proves the
// Checkpoint handler's network steps do nothing for a colocated worker (nil
// ref): a colocated worker already wrote its checkpoint on the shared
// filesystem and must not be asked to Export or CleanupActor.
func TestReceiveAndCleanupNetworkCheckpointAreNoOpForColocated(t *testing.T) {
	// network nil => the Checkpoint handler passes a nil activation.
	s := &Atelet{}
	if err := s.receiveNetworkCheckpoint(t.Context(), nil, []string{"image"}, t.TempDir()); err != nil {
		t.Fatalf("receiveNetworkCheckpoint with a nil ref (colocated) errored: %v", err)
	}
	if err := s.cleanupNetworkActor(t.Context(), nil); err != nil {
		t.Fatalf("cleanupNetworkActor with a nil ref (colocated) errored: %v", err)
	}
}

// TestNetworkGenerationPrepareRejectsInvalidIdentity proves prepare refuses an
// invalid actor/worker identity rather than minting a generation for it.
func TestNetworkGenerationPrepareRejectsInvalidIdentity(t *testing.T) {
	node := nodeIdentity{NodeName: "node-a", NodeUID: "node-a-incarnation"}
	g := newTestGenerations(t, node)
	actor := resources.ActorRef{Atespace: "team", Name: "counter"}
	// A value carrying a path separator would escape the host path built from
	// it; prepare must refuse it rather than mint a generation for it.
	if _, err := g.preparePending(actor, "bad/actor", "22222222-2222-2222-2222-222222222222"); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("preparePending accepted an actor UID with a separator: %v", err)
	}
	if _, err := g.preparePending(actor, "11111111-1111-1111-1111-111111111111", "../escape"); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("preparePending accepted a worker UID with a traversal: %v", err)
	}
}
