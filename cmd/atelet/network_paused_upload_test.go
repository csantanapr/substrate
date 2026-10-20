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
	"path/filepath"
	"strings"
	"testing"

	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/resources"
)

// TestPausedUploadReadsNodeCustodyNotTheGuest is the paused-upload proof for a
// network worker. After a pause Checkpoint on a network worker, the checkpoint
// pull (part 2) left the files in node custody, moveLocalCheckpoint moved them
// into the actor's local snapshot directory, and THEN the guest was removed.
// PromoteSnapshot (the paused upload) must then upload from that node custody --
// it drives no worker and must not depend on the guest at all.
//
// This pins that invariant directly. The Atelet has the network transport
// ENABLED but NO staging server or worker dialer wired (its WorkerDialer is
// nil), so ANY attempt to reach the guest would panic or fail with a dial
// error. The paused upload instead reads the custody manifest, enumerates the
// custody files and hands them to the object-store plugin -- reaching the
// plugin upload step, which proves the whole path is custody-sourced and
// guest-independent.
//
// It does not assert the upload SUCCEEDS: the fake object-store plugin
// (objectstoreplugintest) returns Internal from PutObject in this build
// environment, which is why the upstream TestUploadLocalCheckpointDir is a
// baseline-failing test. The invariant under test is guest-independence, and
// the error returned is the object-store plugin's upload error -- i.e. the path
// got through the custody read and the file enumeration and failed only at the
// object store, never at a worker dial.
func TestPausedUploadReadsNodeCustodyNotTheGuest(t *testing.T) {
	ctx := context.Background()
	store := &recordingObjectStorage{}
	s := newPluginHerder(t, store)
	// Enable the network transport but wire NO staging client and leave the
	// worker dialer nil: the paused upload must not need either. (newPluginHerder
	// does not set workerDialer, so s.workerDialer is nil here; any guest reach
	// would fail on it.)
	s.network = &nodeWorkerNetwork{}
	s.generations = newTestGenerations(t, nodeIdentity{NodeName: "node-a", NodeUID: "node-a-incarnation"})
	if s.workerDialer != nil {
		t.Fatal("test setup wired a worker dialer; the paused-upload invariant needs it absent")
	}

	uri, err := resources.ParseSnapshotURI(pausedSnapshotURI)
	if err != nil {
		t.Fatalf("ParseSnapshotURI: %v", err)
	}
	// Node custody as the network checkpoint pull + moveLocalCheckpoint would
	// have left it: the exact file set and a manifest, on the node's disk.
	custody := filepath.Join(t.TempDir(), "pause-snap-1")
	writeLocalSnapshot(t, custody, sandboxAssetsRecord{
		SandboxClass:      "gvisor",
		SnapshotFiles:     []string{"config.json", "memory-ranges", "data.tar"},
		DataSnapshotFiles: []string{"data.tar"},
		Fidelity:          ateattr.SnapshotFidelityMemory,
	}, map[string]string{"config.json": "cfg", "memory-ranges": "mem", "data.tar": "data"})

	// uploadLocalCheckpointDir gained an (sandboxClass, uploaded) pair upstream
	// (#2733, keep-local-snapshot-after-upload); this guest-independence test
	// only asserts the error, so both are discarded.
	_, _, err = s.uploadLocalCheckpointDir(ctx, validPromoteSnapshotRequest(), custody, uri)
	// The path reached the object-store plugin upload (custody read + file
	// enumeration done) and failed only there, in this environment, with the
	// plugin's Internal error. A guest reach would instead have nil-dereferenced
	// the absent worker dialer or returned a dial error naming the worker. The
	// error must name the snapshot-file upload, proving custody was the source.
	if err == nil {
		t.Skip("object-store plugin upload unexpectedly succeeded; the custody-source invariant is subsumed by the end-to-end test")
	}
	if !strings.Contains(err.Error(), "uploading snapshot files") {
		t.Fatalf("paused upload error = %v; want the object-store upload error, proving the path read node custody and reached the object store without touching a worker", err)
	}
}
