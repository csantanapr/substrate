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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	pb "go.ate.dev/substrate/internal/proto/workerstagingpb"
	"google.golang.org/protobuf/proto"
)

// TestInterruptedImmutableInstallRecoversWithoutReplacement exercises the
// node-wide content-addressed immutable path, which after upstream #2699 holds
// only IMAGE_LAYER: a RUNTIME asset is per-actor and no longer installed through
// installImmutable (its own path is covered by TestRuntimeAssetPlacedPerActor
// and the end-to-end staging tests). The actorUID argument is "" because
// IMAGE_LAYER is node-wide and ignores it.
func TestInterruptedImmutableInstallRecoversWithoutReplacement(t *testing.T) {
	base, worker := t.TempDir(), uuid.NewString()
	server, err := NewServer(base, worker, func(string) bool { return false })
	if err != nil {
		t.Fatal(err)
	}
	artifact := &pb.Artifact{Kind: pb.ArtifactKind_ARTIFACT_KIND_IMAGE_LAYER, Id: strings.Repeat("a", 64), Sha256: strings.Repeat("b", 64), Size: 2048}
	entry := "fs/data"
	tree := t.TempDir()
	fixtureFile(t, tree, entry, "validated bytes")
	receipt := server.cacheRecord(artifact)
	// Force precisely the reported window: publishing succeeds, final
	// receipt publication fails, then a new server adopts the same root.
	if err := os.MkdirAll(receipt, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := server.installImmutable(artifact, tree); err == nil || server.cached(artifact) {
		t.Fatal("interrupted installation reported complete")
	}
	if err := os.Remove(receipt); err != nil {
		t.Fatal(err)
	}
	server, err = NewServer(base, worker, func(string) bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	// The source is absent: recovery must only finish the receipt.
	if err := server.installImmutable(artifact, filepath.Join(base, "absent")); err != nil || !server.cached(artifact) {
		t.Fatalf("matching interrupted installation did not recover: %v", err)
	}
	path := filepath.Join(base, artifactDestination(artifact, ""), entry)
	if got, err := os.ReadFile(path); err != nil || string(got) != "validated bytes" {
		t.Fatalf("recovery replaced immutable data: %q %v", got, err)
	}
	other := proto.Clone(artifact).(*pb.Artifact)
	other.Sha256 = strings.Repeat("c", 64)
	if err := server.installImmutable(other, t.TempDir()); err == nil {
		t.Fatal("conflicting artifact replaced installed data")
	}
	if err := os.Remove(receipt); err != nil {
		t.Fatal(err)
	}
	if err := server.installImmutable(artifact, t.TempDir()); err == nil {
		t.Fatal("unowned existing destination was adopted without intent")
	}
}
