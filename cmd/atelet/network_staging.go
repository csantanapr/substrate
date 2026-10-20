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

// network_staging.go carries the CLIENT half of the network-worker
// path in the atelet: dialing the worker's staging service, pushing a fresh
// guest's image/runtime/checkpoint assets in before the worker lifecycle RPC
// (Run/Restore), and pulling the guest's checkpoint files OUT into node custody
// after a checkpoint and before the guest is removed. A colocated worker shares
// the node's filesystem, so none of this runs for it: every entry point is a
// no-op when network workers are disabled (s.network == nil).
//
// Generation (worker-derived): worker lifecycle requests carry no activation
// generation, and the worker derives the generation it serves
// from its own committed activation record. The atelet keeps a matching
// node-side generation record so a later Checkpoint names the SAME generation
// the fresh guest was staged with; the worker's ExportCheckpoint rejects a ref
// whose generation differs from what it committed. The frozen control-plane
// assignment mirror (checkControl/revoke against GetActor) is intentionally
// omitted: it would be redundant and could go stale, because the
// control-plane MintWorkerActorCertificate performs the authoritative live
// assignment check. Only the generation persistence is kept.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/google/uuid"
	"go.ate.dev/substrate/cmd/atelet/internal/sparsefile"
	"go.ate.dev/substrate/internal/nodepath"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	pb "go.ate.dev/substrate/internal/proto/workerstagingpb"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/workerstaging"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// networkGenerations persists, per actor on this node, the activation
// generation the fresh guest was staged with. It is written at Run/Restore
// (prepare) and read at Checkpoint (lookup) so both name the same generation in
// the ActivationRef the worker validates. It survives an atelet restart (files
// under BasePath), and it is pinned to this node incarnation so a record left
// by a previous node cannot be reused.
type networkGenerations struct {
	dir   string
	node  nodeIdentity
	locks sync.Map // actorUID -> *sync.Mutex, serializing one actor's activations
}

// lock serializes all generation work for one actor, so two concurrent
// activations of the same actor cannot interleave their prepare/stage/commit
// and leave the committed node record naming a generation the worker did not
// commit last. Different actors proceed in parallel.
func (g *networkGenerations) lock(actorUID string) func() {
	value, _ := g.locks.LoadOrStore(actorUID, &sync.Mutex{})
	mutex := value.(*sync.Mutex)
	mutex.Lock()
	return mutex.Unlock
}

// nodeIdentity is the subset of the atelet's Pod identity the generation record
// is keyed to, so a record cannot be reused across node incarnations.
type nodeIdentity struct {
	NodeName string
	NodeUID  string
}

type networkGeneration struct {
	ActorRef   resources.ActorRef `json:"actor_ref"`
	ActorUID   string             `json:"actor_uid"`
	WorkerUID  string             `json:"worker_uid"`
	NodeName   string             `json:"node_name"`
	NodeUID    string             `json:"node_uid"`
	Generation string             `json:"generation"`
}

func newNetworkGenerations(dir string, node nodeIdentity) (*networkGenerations, error) {
	if node.NodeName == "" || node.NodeUID == "" {
		return nil, fmt.Errorf("node identity is required for network generations")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	return &networkGenerations{dir: dir, node: node}, nil
}

func (g *networkGenerations) committedPath(actorUID string) string {
	return filepath.Join(g.dir, actorUID+".json")
}

func (g *networkGenerations) pendingPath(actorUID string) string {
	return filepath.Join(g.dir, actorUID+".pending.json")
}

func (g *networkGenerations) validate(record *networkGeneration, actorUID string) error {
	if record.ActorUID != actorUID || record.Generation == "" ||
		record.NodeName != g.node.NodeName || record.NodeUID != g.node.NodeUID {
		return fmt.Errorf("generation record belongs to a different actor or node incarnation")
	}
	return nil
}

func (g *networkGenerations) readFile(path, actorUID string) (*networkGeneration, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var record networkGeneration
	if err := json.Unmarshal(raw, &record); err != nil {
		return nil, err
	}
	if err := g.validate(&record, actorUID); err != nil {
		return nil, err
	}
	return &record, nil
}

// read returns the actor's COMMITTED generation: the generation the worker
// actually committed its activation under (promoted only after Stage succeeded).
// The pull and cleanup in Checkpoint read this, never the pending record.
func (g *networkGenerations) read(actorUID string) (*networkGeneration, error) {
	if !resources.IsValidResourceName(actorUID) {
		return nil, fmt.Errorf("invalid actor UID")
	}
	return g.readFile(g.committedPath(actorUID), actorUID)
}

// preparePending mints a fresh generation for an actor about to be
// (re)activated on a worker and writes it as a PENDING record before staging,
// so a crash between the write and a successful Stage leaves a handle the next
// cleanup can still name. It does NOT touch the committed record, so a failed
// or racing activation cannot overwrite the generation the worker last
// committed. The caller holds the per-actor lock across preparePending, Stage
// and commit.
func (g *networkGenerations) preparePending(ref resources.ActorRef, actorUID, workerUID string) (*networkGeneration, error) {
	if !resources.IsValidResourceName(actorUID) || !resources.IsValidResourceName(ref.Atespace) ||
		!resources.IsValidResourceName(ref.Name) || resources.ValidateWorkerPodUID(workerUID) != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid network generation identity")
	}
	record := &networkGeneration{
		ActorRef:   ref,
		ActorUID:   actorUID,
		WorkerUID:  workerUID,
		NodeName:   g.node.NodeName,
		NodeUID:    g.node.NodeUID,
		Generation: uuid.NewString(),
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	if err := writeFileAtomic(g.pendingPath(actorUID), raw, 0o600); err != nil {
		return nil, err
	}
	return record, nil
}

// commit promotes the pending generation to the committed record, atomically,
// AFTER Stage has succeeded (the worker has committed its own activation under
// the same generation). Only after this does networkActivation return it for a
// pull. The caller holds the per-actor lock.
func (g *networkGenerations) commit(record *networkGeneration) error {
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if err := writeFileAtomic(g.committedPath(record.ActorUID), raw, 0o600); err != nil {
		return err
	}
	// Best-effort: drop the pending handle now that it is committed. A leftover
	// pending file is harmless (read never consults it).
	_ = os.Remove(g.pendingPath(record.ActorUID))
	return nil
}

// writeFileAtomic writes data to path via a temp file and rename, so a reader
// never sees a half-written record.
func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".network-gen-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	_, writeErr := file.Write(data)
	if writeErr == nil {
		writeErr = file.Chmod(perm)
	}
	if writeErr == nil {
		writeErr = file.Sync()
	}
	closeErr := file.Close()
	if writeErr != nil {
		return writeErr
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Rename(file.Name(), path)
}

// enableNetworkWorkers switches the Atelet onto the network-worker path. It is
// called once at startup, only when --network-workers is set, before any RPC
// can reach the Atelet, so the fields it sets need no lock. With the flag unset
// it is never called and every network entry point below stays a no-op.
func (s *Atelet) enableNetworkWorkers(network *nodeWorkerNetwork) error {
	if network == nil {
		return fmt.Errorf("network worker transport is required")
	}
	generations, err := newNetworkGenerations(
		filepath.Join(nodepath.BasePath, "network-generations"),
		nodeIdentity{NodeName: network.local.NodeName, NodeUID: network.local.NodeUID},
	)
	if err != nil {
		return err
	}
	s.network = network
	s.generations = generations
	return nil
}

// stagingConnector opens the staging client of a worker Pod.
type stagingConnector interface {
	StagingClient(ctx context.Context, workerUID string) (pb.WorkerStagingClient, error)
}

// StagingClient opens (or reuses) the worker connection and wraps it as a
// staging client. It dials through the same network-aware WorkerDialer the
// lifecycle RPCs use, so staging and lifecycle share one authenticated
// connection per worker.
func (d *WorkerDialer) StagingClient(ctx context.Context, workerUID string) (pb.WorkerStagingClient, error) {
	conn, err := d.DialWorkerPod(ctx, workerUID)
	if err != nil {
		return nil, err
	}
	return pb.NewWorkerStagingClient(conn), nil
}

// stagingRPC returns the staging client of a worker Pod.
func (s *Atelet) stagingRPC(ctx context.Context, workerUID string) (pb.WorkerStagingClient, error) {
	return s.staging.StagingClient(ctx, workerUID)
}

// validateNetworkSpec rejects a workload spec a network worker cannot serve.
// A separate guest Pod cannot see the node's CSI mounts, so a node-mounted
// external volume is refused rather than silently ignored.
func (s *Atelet) validateNetworkSpec(spec *ateletpb.WorkloadSpec) error {
	if s.network == nil {
		return nil
	}
	for _, volume := range spec.GetVolumes() {
		if volume.GetExternal() != nil {
			return status.Error(codes.InvalidArgument, "network workers do not support node-mounted external CSI volumes")
		}
	}
	return nil
}

// validateNetworkSandbox refuses a sandbox class the staging path does not yet
// support. Network-worker staging ships gVisor assets; the microVM runtime is
// not part of this path.
func (s *Atelet) validateNetworkSandbox(class string) error {
	if s.network != nil && class != "gvisor" {
		return status.Error(codes.InvalidArgument, "network worker staging currently supports the gvisor sandbox class")
	}
	return nil
}

// validateNestedSandbox refuses a nested-containers opt-in on any sandbox class
// but gVisor, enforced at the point the class is actually known: the runtime
// the control plane assigned, resolved by advertisedRuntime on Run and Restore.
//
// Only ateworker-gvisor applies the nested runsc flags and the image-store
// tmpfs (cmd/ateworker-gvisor/runsc.go); the microVM worker has no code path
// that reads ateworkerpb.NestedContainers, so a nested opt-in placed on a
// microVM worker would be SILENTLY dropped and the template would lie about
// the actor's sandbox. The ActorTemplate carries no sandbox class (the pool
// sets the class through workerSelector), so the check cannot run at template
// admission; this guard runs it where the class is resolved. A nil/disabled opt-in or a gVisor class
// passes untouched, so a default actor and a correctly-placed nested actor are
// unaffected. Nested containers are supported only with sandbox_class
// gVisor; an accepted opt-in must actually be honored.
func validateNestedSandbox(nested *ateletpb.NestedContainers, class string) error {
	// "gvisor" is sandboxclass.GVisor / v1alpha1.SandboxClassGvisor; compared as
	// the string here to match validateNetworkSandbox above and avoid a class
	// import in this file.
	if nested.GetEnabled() && class != "gvisor" {
		return status.Errorf(codes.InvalidArgument,
			"nested containers are supported only with the gvisor sandbox class, but the actor resolved to %q", class)
	}
	return nil
}

// networkActivation re-reads the persisted generation for an actor and returns
// the ActivationRef a Checkpoint uses to pull the guest's checkpoint and to
// clean the guest up. It verifies the record names the SAME actor and worker
// the lifecycle request targets, so a request cannot pull another actor's
// custody (the UID check). Returns (nil, nil) for a colocated worker, so the
// Checkpoint handler's pull step is a no-op there.
func (s *Atelet) networkActivation(actor resources.ActorRef, actorUID, workerUID string) (*pb.ActivationRef, error) {
	if s.network == nil {
		return nil, nil
	}
	// Hold the per-actor lock so a read cannot observe a half-promoted record
	// while a concurrent activation of the same actor commits.
	unlock := s.generations.lock(actorUID)
	defer unlock()
	record, err := s.generations.read(actorUID)
	if err != nil {
		return nil, err
	}
	if record.ActorRef != actor || record.WorkerUID != workerUID {
		return nil, status.Error(codes.PermissionDenied, "lifecycle actor or worker differs from persisted activation")
	}
	return &pb.ActivationRef{ActorUid: actorUID, WorkerUid: workerUID, Generation: record.Generation}, nil
}

// receiveNetworkCheckpoint pulls exactly the runtime-reported checkpoint files
// out of the guest into node custody at destination. workerstaging.Client.Export
// stages into a temp dir and renames atomically, verifies each file's size and
// sha256, requires the exact membership (expected == the files the guest's
// checkpoint listed, nothing else), and refuses to overwrite nonempty custody.
// A failed or partial pull leaves existing custody untouched and a second
// attempt works. The worker side (ExportCheckpoint) independently validates the
// ref's actor/worker/generation against its committed activation, so a pull for
// the wrong actor is refused on both ends. A nil ref (colocated worker) is a
// no-op: a colocated worker already wrote the files on the shared filesystem.
func (s *Atelet) receiveNetworkCheckpoint(ctx context.Context, ref *pb.ActivationRef, files []string, destination string) error {
	if ref == nil {
		return nil
	}
	rpc, err := s.stagingRPC(ctx, ref.GetWorkerUid())
	if err != nil {
		return err
	}
	return workerstaging.NewClient(s.nodeBase, rpc).Export(ctx, ref, files, destination)
}

// cleanupNetworkActor asks the guest to discard the actor's staged artifacts
// after node custody is complete. Best-effort: the Checkpoint handler logs
// rather than fails, because node custody and the snapshot are already durable
// and worker removal reclaims the guest anyway. A nil ref (colocated worker) is
// a no-op.
func (s *Atelet) cleanupNetworkActor(ctx context.Context, ref *pb.ActivationRef) error {
	if ref == nil {
		return nil
	}
	rpc, err := s.stagingRPC(ctx, ref.GetWorkerUid())
	if err != nil {
		return err
	}
	_, err = rpc.CleanupActor(ctx, ref)
	return err
}

// stageNetworkActivation mints and persists a fresh generation for the actor,
// then pushes its image layers, runtime assets and actor state (the OCI
// bundles, system-info, and for a Restore the checkpoint files already staged
// into the actor's restore-state dir) into the fresh guest, so the worker's
// Run/Restore RPC finds everything on the guest's own filesystem. It returns
// the ActivationRef; the Run/Restore caller does NOT pass it to the worker RPC
// (worker lifecycle requests carry no generation field, and the
// worker derives the generation it serves from its own committed activation)
// -- the ref is returned for the caller to log and for tests to
// inspect, and the SAME generation is re-read from the committed record by a
// later Checkpoint's networkActivation. FAIL CLOSED: if staging fails the
// caller must NOT call the worker RPC; it returns the error instead. Returns
// (nil, nil) for a colocated worker, whose Run/Restore reads the node's shared
// filesystem directly and needs no push. assets is the runtime asset path set
// the worker RPC also receives, so the exact runtime binaries the RPC names are
// the ones staged.
func (s *Atelet) stageNetworkActivation(ctx context.Context, actor resources.ActorRef, actorUID, workerUID string, assets map[string]string) (*pb.ActivationRef, error) {
	if s.network == nil {
		return nil, nil
	}
	// Serialize the whole prepare/stage/commit for this actor: two concurrent
	// activations of the same actor must not interleave and leave the committed
	// record naming a generation the worker did not commit last. Different actors
	// proceed in parallel.
	unlock := s.generations.lock(actorUID)
	defer unlock()

	// Write the generation as PENDING before staging (a crash mid-stage leaves a
	// cleanup handle) but do NOT make it the committed record yet.
	record, err := s.generations.preparePending(actor, actorUID, workerUID)
	if err != nil {
		return nil, err
	}
	ref := &pb.ActivationRef{ActorUid: actorUID, Generation: record.Generation, WorkerUid: workerUID}
	rpc, err := s.stagingRPC(ctx, workerUID)
	if err != nil {
		return nil, err
	}
	// Stage ends with the worker committing its own activation under this
	// generation. Only then promote the node's committed record. A failed Stage
	// leaves the prior committed record (if any) intact.
	//
	// The two commits (worker, then node) are not a single atomic step. On the
	// happy path they agree. If the node commit below fails, or atelet crashes
	// after the worker commit and before it, the worker holds the new generation
	// while the node's committed record is absent (or still the old one). That
	// window FAILS CLOSED, not open: a later Checkpoint's networkActivation reads
	// the node's committed record, so an absent/old record makes the pull error
	// out (the actor cannot be checkpointed until it is re-staged) rather than
	// pulling under a wrong generation. Making this self-heal would require
	// reconciling the node record against the worker's committed activation on
	// read (a worker round-trip), which this code does not do; the fail-closed
	// property is the one that matters.
	if err := workerstaging.NewClient(s.nodeBase, rpc).Stage(ctx, ref, actor, assets); err != nil {
		return nil, fmt.Errorf("stage network activation: %w", err)
	}
	if err := s.generations.commit(record); err != nil {
		return nil, fmt.Errorf("commit network activation generation: %w", err)
	}
	return ref, nil
}

// copyLocalCheckpoint stages files from the local checkpoint snapshotName under
// srcDir into dstDir for a network worker's restore. Both must be inside
// actorDir, which confines every access.
func (s *Atelet) copyLocalCheckpoint(ctx context.Context, actorDir, snapshotName, srcDir, dstDir string, files []string) error {
	root, err := os.OpenRoot(actorDir)
	if err != nil {
		return fmt.Errorf("while opening actor directory: %w", err)
	}
	defer root.Close()
	srcDir, err = filepath.Rel(actorDir, filepath.Join(srcDir, snapshotName))
	if err != nil {
		return err
	}
	dstDir, err = filepath.Rel(actorDir, dstDir)
	if err != nil {
		return err
	}

	for _, fileName := range files {
		if ctx.Err() != nil {
			return fmt.Errorf("context cancelled: %w", ctx.Err())
		}
		src := filepath.Join(srcDir, fileName)
		dst := filepath.Join(dstDir, fileName)
		// A link to a symlink would be followed later, outside the root.
		info, err := root.Lstat(src)
		if err != nil {
			return fmt.Errorf("while inspecting %s: %w", src, err)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("%s is not a regular file", src)
		}
		if err := root.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
			return fmt.Errorf("while creating parent directory for %s: %w", dst, err)
		}
		// Link rather than copy. The local checkpoint lives under the same actor dir
		// as the restore staging area, so this stages the memory image in constant
		// time instead of re-writing its whole working set. Nothing rewrites the
		// shared inode: CH demand-pages from the staged image read-only,
		// rewriteSnapshotSocketPaths renames its rewritten config.json into place
		// rather than truncating, and MergeDeltaIntoBase refuses its in-place overlay
		// once the image carries a second link.
		//
		// EXDEV alone falls back to copying, so an unexpected link failure surfaces
		// instead of silently reverting to the full copy this exists to remove. It
		// also keeps copyRootFile off a dst that is already a link to src, where its
		// O_TRUNC would empty both and report a successful copy of the old size.
		switch err := linkFile(root, src, dst); {
		case err == nil:
			continue
		case !errors.Is(err, unix.EXDEV):
			return fmt.Errorf("failed to link %s to %s: %w", src, dst, err)
		}
		slog.WarnContext(ctx, "local checkpoint and restore dir are on different filesystems; copying instead of linking",
			slog.String("src", src), slog.String("dst", dst))
		if _, err := copyRootFile(root, src, dst); err != nil {
			return fmt.Errorf("failed to copy %s to %s: %w", src, dst, err)
		}
	}

	return nil
}

// linkFile is os.Root.Link, indirected so a test can force the cross-filesystem
// fallback in copyLocalCheckpoint without mounting a second filesystem.
var linkFile = (*os.Root).Link

func copyRootFile(root *os.Root, src, dst string) (int64, error) {
	source, err := root.Open(src)
	if err != nil {
		return 0, err
	}
	defer source.Close()
	info, err := source.Stat()
	if err != nil {
		return 0, err
	}
	if !info.Mode().IsRegular() {
		return 0, fmt.Errorf("%s is not a regular file", src)
	}

	destination, err := root.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o666)
	if err != nil {
		return 0, err
	}
	nBytes, err := sparsefile.Copy(source, destination)
	return nBytes, errors.Join(err, destination.Close())
}
