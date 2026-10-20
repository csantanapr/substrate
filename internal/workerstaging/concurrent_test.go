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

package workerstaging

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	pb "go.ate.dev/substrate/internal/proto/workerstagingpb"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/grpc"
)

type countedStaging struct {
	pb.WorkerStagingClient
	layers, states atomic.Int32
	firstLayer     chan struct{}
	releaseLayer   chan struct{}
}

func (c *countedStaging) PutArtifact(ctx context.Context, options ...grpc.CallOption) (grpc.ClientStreamingClient[pb.ArtifactChunk, pb.Empty], error) {
	stream, err := c.WorkerStagingClient.PutArtifact(ctx, options...)
	if err != nil {
		return nil, err
	}
	return &countedUpload{ClientStreamingClient: stream, ctx: ctx, owner: c}, nil
}

type countedUpload struct {
	grpc.ClientStreamingClient[pb.ArtifactChunk, pb.Empty]
	ctx   context.Context
	owner *countedStaging
	first bool
}

func (s *countedUpload) Send(chunk *pb.ArtifactChunk) error {
	if !s.first {
		s.first = true
		switch chunk.GetKind() {
		case pb.ArtifactKind_ARTIFACT_KIND_IMAGE_LAYER:
			if s.owner.layers.Add(1) == 1 {
				close(s.owner.firstLayer)
				select {
				case <-s.owner.releaseLayer:
				case <-s.ctx.Done():
					return s.ctx.Err()
				}
			}
		case pb.ArtifactKind_ARTIFACT_KIND_ACTOR_STATE:
			s.owner.states.Add(1)
		}
	}
	return s.ClientStreamingClient.Send(chunk)
}

// TestConcurrentColdActorsUploadSharedImageLayerOnce verifies the worker
// preparation gate still serializes the one artifact that remains a node-wide,
// content-addressed shared immutable, the image layer,
// so two cold actors that reference the same layer upload and extract it once,
// while their per-actor artifacts (actor state and, now, the runtime) are each
// transferred independently. The two actors share layer sha256 a*64 via CopyFS.
func TestConcurrentColdActorsUploadSharedImageLayerOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	node, guest := t.TempDir(), t.TempDir()
	worker, first, second := uuid.NewString(), uuid.NewString(), uuid.NewString()
	firstAssets := fixtureNode(t, node, first)
	if err := os.CopyFS(filepath.Join(node, "actors", second), os.DirFS(filepath.Join(node, "actors", first))); err != nil {
		t.Fatal(err)
	}
	fixtureFile(t, node, filepath.Join(snapshotsTreeName(), second, "restore-state", "image"), "second actor state")
	// CopyFS duplicated the first actor's bundles (and so its reference to the
	// shared image layer a*64) under the second actor, but the per-actor
	// sandbox-assets dir it also copied belongs to the FIRST actor's UID. Stage
	// validates each runtime path against ref.ActorUid, so give the second actor
	// its own per-actor runtime copy and asset map, as production does.
	//
	// Give the two actors DISTINCT runsc bytes so the test proves each guest
	// receives its OWN actor's runtime, not a reused copy from a client cache
	// keyed only by (kind, asset-name). Before the per-actor cache key this
	// assertion failed: actor B reused actor A's packed "gvisor" tar.
	if err := os.RemoveAll(filepath.Join(node, "actors", first, "sandbox-assets")); err != nil {
		t.Fatal(err)
	}
	fixtureActorRuntimeContent(t, node, first, "first-runsc-bytes")
	if err := os.RemoveAll(filepath.Join(node, "actors", second, "sandbox-assets")); err != nil {
		t.Fatal(err)
	}
	fixtureActorRuntimeContent(t, node, second, "second-runsc-bytes")
	secondAssets := fixtureNodeAssets(second)
	rpc, _, _ := stagingConnection(t, guest, worker)
	counted := &countedStaging{WorkerStagingClient: rpc, firstLayer: make(chan struct{}), releaseLayer: make(chan struct{})}
	var release sync.Once
	defer release.Do(func() { close(counted.releaseLayer) })
	results := make(chan error, 2)
	stage := func(actorUID, name string, assets map[string]string) {
		ref := &pb.ActivationRef{ActorUid: actorUID, WorkerUid: worker, Generation: uuid.NewString()}
		// Real call sites construct independent clients for concurrent actors.
		results <- NewClient(node, counted).Stage(ctx, ref, resources.ActorRef{Atespace: "team", Name: name}, assets)
	}
	go stage(first, "first", firstAssets)
	select {
	case <-counted.firstLayer:
	case <-ctx.Done():
		t.Fatal("first image-layer upload did not start")
	}
	go stage(second, "second", secondAssets)
	// Keep the first upload incomplete until the second activation has joined the
	// worker-preparation gate.
	for {
		workerPreparations.Lock()
		entry := workerPreparations.workers[worker]
		joined := entry != nil && entry.users == 2
		workerPreparations.Unlock()
		if joined {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("second activation did not wait for immutable preparation")
		}
		runtime.Gosched()
	}
	release.Do(func() { close(counted.releaseLayer) })
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if counted.layers.Load() != 1 || counted.states.Load() != 2 {
		t.Fatalf("uploads: image-layer=%d actor-state=%d", counted.layers.Load(), counted.states.Load())
	}
	for actorUID, expected := range map[string]string{first: "restore bytes", second: "second actor state"} {
		body, err := os.ReadFile(filepath.Join(snapshotsDir(guest, actorUID), "restore-state", "image"))
		if err != nil || string(body) != expected {
			t.Fatalf("actor state was not independently committed: %q %v", body, err)
		}
	}
	// Each guest must hold its OWN actor's runsc bytes. A client cache keyed
	// only by (kind, asset-name) would give the second actor the first's copy.
	for actorUID, expected := range map[string]string{first: "first-runsc-bytes", second: "second-runsc-bytes"} {
		body, err := os.ReadFile(filepath.Join(guest, "actors", actorUID, "sandbox-assets", fixtureRuntimeAssetName, "runsc"))
		if err != nil || string(body) != expected {
			t.Fatalf("actor %s received the wrong runtime bytes: %q %v", actorUID, body, err)
		}
	}
}

func TestPreparationWaitIsCancellableAndWorkerScoped(t *testing.T) {
	first, second := uuid.NewString(), uuid.NewString()
	unlock, err := lockWorkerPreparation(t.Context(), first)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	other, err := lockWorkerPreparation(ctx, second)
	if err != nil {
		unlock()
		t.Fatal("another worker was blocked", err)
	}
	other()
	cancelled, cancelWait := context.WithCancel(t.Context())
	cancelWait()
	if release, err := lockWorkerPreparation(cancelled, first); !errors.Is(err, context.Canceled) || release != nil {
		unlock()
		t.Fatalf("cancelled waiter acquired the gate: %v", err)
	}
	unlock()
	workerPreparations.Lock()
	defer workerPreparations.Unlock()
	if workerPreparations.workers[first] != nil || workerPreparations.workers[second] != nil {
		t.Fatal("completed preparations retained worker entries")
	}
}
