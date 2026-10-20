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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"go.ate.dev/substrate/internal/imagecache"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	pb "go.ate.dev/substrate/internal/proto/workerstagingpb"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/volumepath"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	activationFile     = "network-activation.json"
	checkpointFile     = "network-checkpoint.json"
	maxSystemInfoBytes = 4 << 20
)

type Server struct {
	pb.UnimplementedWorkerStagingServer
	base, workerUID string
	isActive        func(string) bool
	locks           sync.Map
	cacheMu         sync.Mutex
}

func NewServer(base, workerUID string, isActive func(string) bool) (*Server, error) {
	if !filepath.IsAbs(base) || resources.ValidateWorkerPodUID(workerUID) != nil || isActive == nil {
		return nil, fmt.Errorf("absolute staging root, worker UID and runtime activity check required")
	}
	if err := os.MkdirAll(base, 0o700); err != nil {
		return nil, err
	}
	return &Server{base: base, workerUID: workerUID, isActive: isActive}, nil
}

func (s *Server) lock(actorUID string) func() {
	value, _ := s.locks.LoadOrStore(actorUID, &sync.Mutex{})
	mutex := value.(*sync.Mutex)
	mutex.Lock()
	return mutex.Unlock
}

func (s *Server) validateRef(ref *pb.ActivationRef) error {
	if ref == nil || ref.GetWorkerUid() != s.workerUID ||
		!resources.IsValidResourceName(ref.GetActorUid()) || !resources.IsValidResourceName(ref.GetGeneration()) {
		return status.Error(codes.PermissionDenied, "activation does not identify this worker and actor")
	}
	return nil
}

func (s *Server) pending(ref *pb.ActivationRef) string {
	return filepath.Join(s.base, "network-staging", ref.GetActorUid(), ref.GetGeneration())
}

func (s *Server) committed(ref *pb.ActivationRef) (*pb.Activation, error) {
	var activation pb.Activation
	raw, err := os.ReadFile(filepath.Join(actorDir(s.base, ref.GetActorUid()), activationFile))
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, "actor has no committed activation")
	}
	if err := protojson.Unmarshal(raw, &activation); err != nil || !proto.Equal(activation.GetRef(), ref) {
		return nil, status.Error(codes.PermissionDenied, "committed activation identity differs")
	}
	return &activation, nil
}

// committedByActor reads the single committed activation an actor has on this
// worker and returns it with its stored ref (actor UID, generation, worker
// UID). Worker lifecycle requests carry no activation generation, so the
// generation an
// RPC operates on is taken from the worker's own committed state rather than a
// caller-supplied field. The stored worker UID is still checked against this
// server's, so a record cannot be served for a worker it does not belong to.
func (s *Server) committedByActor(actorUID string) (*pb.Activation, error) {
	if !resources.IsValidResourceName(actorUID) {
		return nil, status.Error(codes.PermissionDenied, "invalid actor UID")
	}
	var activation pb.Activation
	raw, err := os.ReadFile(filepath.Join(actorDir(s.base, actorUID), activationFile))
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, "actor has no committed activation")
	}
	if err := protojson.Unmarshal(raw, &activation); err != nil {
		return nil, status.Error(codes.DataLoss, "committed activation is unreadable")
	}
	ref := activation.GetRef()
	if ref.GetActorUid() != actorUID || ref.GetWorkerUid() != s.workerUID ||
		!resources.IsValidResourceName(ref.GetGeneration()) {
		return nil, status.Error(codes.PermissionDenied, "committed activation identity differs")
	}
	return &activation, nil
}

func readActivation(path string) (*pb.Activation, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var activation pb.Activation
	if err := protojson.Unmarshal(raw, &activation); err != nil {
		return nil, err
	}
	return &activation, nil
}

func writeAtomic(path string, data []byte, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".network-write-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	_, writeErr := file.Write(data)
	if writeErr == nil {
		writeErr = file.Chmod(mode)
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
	if err := os.Rename(file.Name(), path); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(path))
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

// cacheRecord is the installation receipt path for a node-wide immutable
// artifact. Only IMAGE_LAYER reaches it: ACTOR_STATE and RUNTIME are per-actor rather than shared immutables and never pass here.
func (s *Server) cacheRecord(artifact *pb.Artifact) string {
	return filepath.Join(s.base, "network-artifacts", artifactKey(artifact)+".json")
}

// cached reports whether an immutable artifact is already installed in node
// custody. Only IMAGE_LAYER is a node-wide, content-addressed immutable that
// can be reused across activations; ACTOR_STATE is always re-transferred, and a
// RUNTIME asset is per-actor and per-generation, staged afresh
// each activation through placeRuntime and never a shared immutable, so both
// report not-cached and PrepareActivation always lists them as missing.
func (s *Server) cached(artifact *pb.Artifact) bool {
	switch artifact.GetKind() {
	case pb.ArtifactKind_ARTIFACT_KIND_ACTOR_STATE, pb.ArtifactKind_ARTIFACT_KIND_RUNTIME:
		return false
	}
	if !artifactReceiptMatches(s.cacheRecord(artifact), artifact) {
		return false
	}
	info, err := os.Lstat(filepath.Join(s.base, artifactDestination(artifact, "")))
	return err == nil && info.Mode()&os.ModeSymlink == 0
}

func artifactReceiptMatches(path string, artifact *pb.Artifact) bool {
	raw, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	var stored pb.Artifact
	return protojson.Unmarshal(raw, &stored) == nil && proto.Equal(&stored, artifact)
}

func (s *Server) PrepareActivation(_ context.Context, activation *pb.Activation) (*pb.Preparation, error) {
	ref := activation.GetRef()
	if err := s.validateRef(ref); err != nil {
		return nil, err
	}
	if !resources.IsValidResourceName(activation.GetActorAtespace()) || !resources.IsValidResourceName(activation.GetActorName()) ||
		len(activation.GetArtifacts()) == 0 || len(activation.GetArtifacts()) > MaxArtifacts {
		return nil, status.Error(codes.InvalidArgument, "invalid activation manifest")
	}
	unlock := s.lock(ref.GetActorUid())
	defer unlock()
	if s.isActive(ref.GetActorUid()) {
		return nil, status.Error(codes.FailedPrecondition, "actor is still active")
	}
	var total int64
	actors := 0
	keys := map[string]bool{}
	for _, artifact := range activation.GetArtifacts() {
		if err := validateArtifact(artifact); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if keys[artifactKey(artifact)] {
			return nil, status.Error(codes.InvalidArgument, "duplicate artifact")
		}
		keys[artifactKey(artifact)] = true
		total += artifact.GetSize()
		if artifact.GetKind() == pb.ArtifactKind_ARTIFACT_KIND_ACTOR_STATE {
			actors++
			if artifact.GetId() != ref.GetGeneration() {
				return nil, status.Error(codes.InvalidArgument, "actor artifact generation differs")
			}
		}
	}
	if actors != 1 || total > MaxActivationBytes {
		return nil, status.Error(codes.InvalidArgument, "activation artifact budget exceeded or actor state missing")
	}
	if len(activation.GetSystemInfoFiles()) > 128 {
		return nil, status.Error(codes.InvalidArgument, "too many system-info files")
	}
	declared := map[string]bool{}
	for _, file := range activation.GetSystemInfoFiles() {
		if err := validateSystemInfoFile(file); err != nil || len(file.GetContents()) != 0 {
			return nil, status.Error(codes.InvalidArgument, "invalid system-info declaration")
		}
		key := file.GetVolume() + "/" + file.GetPath()
		if declared[key] {
			return nil, status.Error(codes.InvalidArgument, "duplicate system-info declaration")
		}
		declared[key] = true
	}
	path := filepath.Join(s.pending(ref), activationFile)
	if previous, err := readActivation(path); err == nil && !proto.Equal(previous, activation) {
		return nil, status.Error(codes.AlreadyExists, "generation already names a different activation")
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	raw, err := protojson.Marshal(activation)
	if err != nil {
		return nil, err
	}
	if err := writeAtomic(path, raw, 0o600); err != nil {
		return nil, err
	}
	response := &pb.Preparation{}
	for _, artifact := range activation.GetArtifacts() {
		if !s.cached(artifact) {
			response.Missing = append(response.Missing, artifact)
		}
	}
	return response, nil
}

func findArtifact(activation *pb.Activation, kind pb.ArtifactKind, id string) *pb.Artifact {
	for _, artifact := range activation.GetArtifacts() {
		if artifact.GetKind() == kind && artifact.GetId() == id {
			return artifact
		}
	}
	return nil
}

func (s *Server) PutArtifact(stream grpc.ClientStreamingServer[pb.ArtifactChunk, pb.Empty]) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	ref := first.GetRef()
	if err := s.validateRef(ref); err != nil {
		return err
	}
	unlock := s.lock(ref.GetActorUid())
	defer unlock()
	if s.isActive(ref.GetActorUid()) {
		return status.Error(codes.FailedPrecondition, "actor is active")
	}
	activation, err := readActivation(filepath.Join(s.pending(ref), activationFile))
	if err != nil || !proto.Equal(activation.GetRef(), ref) {
		return status.Error(codes.FailedPrecondition, "activation was not prepared")
	}
	artifact := findArtifact(activation, first.GetKind(), first.GetId())
	if artifact == nil {
		return status.Error(codes.PermissionDenied, "artifact is not declared by this activation")
	}
	file, err := os.CreateTemp(s.pending(ref), ".upload-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	hash := sha256.New()
	var received int64
	for chunk := first; ; {
		if !proto.Equal(chunk.GetRef(), ref) || chunk.GetKind() != artifact.GetKind() || chunk.GetId() != artifact.GetId() ||
			len(chunk.GetData()) == 0 || len(chunk.GetData()) > ChunkBytes || int64(len(chunk.GetData())) > artifact.GetSize()-received {
			return status.Error(codes.InvalidArgument, "artifact stream identity or size differs")
		}
		if _, err := io.MultiWriter(file, hash).Write(chunk.GetData()); err != nil {
			return err
		}
		received += int64(len(chunk.GetData()))
		chunk, err = stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
	}
	if received != artifact.GetSize() || hex.EncodeToString(hash.Sum(nil)) != artifact.GetSha256() {
		return status.Error(codes.DataLoss, "artifact size or SHA256 differs")
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	tree, err := os.MkdirTemp(s.pending(ref), ".extract-*")
	if err != nil {
		return err
	}
	defer func() { _ = imagecache.RemoveAllWritable(tree) }()
	if err := extractArtifact(stream.Context(), artifact, tree, file); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	switch artifact.GetKind() {
	case pb.ArtifactKind_ARTIFACT_KIND_ACTOR_STATE:
		destination := filepath.Join(s.pending(ref), "actor-state")
		if err := imagecache.RemoveAllWritable(destination); err != nil {
			return err
		}
		if err := os.Rename(tree, destination); err != nil {
			return err
		}
		if err := writeAtomic(filepath.Join(s.pending(ref), "actor-state.complete"), []byte(artifact.GetSha256()), 0o600); err != nil {
			return err
		}
	case pb.ArtifactKind_ARTIFACT_KIND_RUNTIME:
		// A sandbox runtime is per-actor, not a node-wide content-addressed
		// immutable, so it is staged like actor state rather
		// than through installImmutable: the extracted asset-name subtree is
		// placed under this generation's own pending/sandbox-assets/<id>, and
		// CommitActivation moves the whole sandbox-assets dir into the actor
		// directory in the SAME atomic rename as actor state. Installing it
		// directly under actors/<uid> would be wiped by that rename. Placing it
		// in a pending subtree separate from pending/actor-state also keeps it
		// safe from the actor-state upload, which replaces pending/actor-state
		// wholesale and may arrive after the runtime (upload order is not fixed).
		if err := s.placeRuntime(ref, artifact, tree); err != nil {
			return err
		}
	default:
		if err := s.installImmutable(artifact, tree); err != nil {
			return err
		}
	}
	return stream.SendAndClose(&pb.Empty{})
}

// runtimePending is this generation's pending sandbox-assets directory, the
// subtree CommitActivation folds into the actor directory. It is a sibling of
// pending/actor-state, so neither the actor-state upload (which replaces
// pending/actor-state wholesale) nor the final commit rename can disturb it
// before the fold.
func (s *Server) runtimePending(ref *pb.ActivationRef) string {
	return filepath.Join(s.pending(ref), "sandbox-assets")
}

func runtimeComplete(dir, id string) string {
	return filepath.Join(dir, "."+id+".complete")
}

// placeRuntime stores a validated runtime asset's single asset-name subtree
// under this generation's pending sandbox-assets dir and records a completeness
// marker holding its digest, mirroring the actor-state staging gate. The asset
// id is a single plain path component (validateArtifact enforces
// assetNamePattern), so tree/<id> and the destination are strictly inside their
// roots. A re-upload of the same generation replaces the subtree atomically.
func (s *Server) placeRuntime(ref *pb.ActivationRef, artifact *pb.Artifact, tree string) error {
	id := artifact.GetId()
	if !assetNamePattern.MatchString(id) {
		return status.Error(codes.InvalidArgument, "invalid runtime asset ID")
	}
	assets := s.runtimePending(ref)
	if err := os.MkdirAll(assets, 0o700); err != nil {
		return err
	}
	destination := filepath.Join(assets, id)
	staged, err := os.MkdirTemp(assets, "."+id+"-*")
	if err != nil {
		return err
	}
	defer func() { _ = imagecache.RemoveAllWritable(staged) }()
	if err := os.Rename(filepath.Join(tree, id), filepath.Join(staged, id)); err != nil {
		return err
	}
	if err := imagecache.RemoveAllWritable(destination); err != nil {
		return err
	}
	if err := os.Rename(filepath.Join(staged, id), destination); err != nil {
		return err
	}
	return writeAtomic(runtimeComplete(assets, id), []byte(artifact.GetSha256()), 0o600)
}

// An intent is persisted only after complete validation and before publishing
// a previously absent destination. A retry may finish that exact installation
// after a process interruption; it never replaces an existing immutable asset.
func (s *Server) installImmutable(artifact *pb.Artifact, tree string) error {
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	if s.cached(artifact) {
		return nil
	}
	destination := filepath.Join(s.base, artifactDestination(artifact, ""))
	intent := s.cacheRecord(artifact) + ".installing"
	raw, err := protojson.Marshal(artifact)
	if err != nil {
		return err
	}
	if info, err := os.Lstat(destination); err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !artifactReceiptMatches(intent, artifact) {
			return status.Error(codes.FailedPrecondition, "existing immutable artifact has no matching installation receipt")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	} else {
		if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
			return err
		}
		if err := writeAtomic(intent, raw, 0o600); err != nil {
			return err
		}
		// Only IMAGE_LAYER reaches installImmutable now; the whole extracted
		// tree is the layer's content. (RUNTIME is staged per-actor through
		// placeRuntime and never installed here.)
		if err := os.Rename(tree, destination); err != nil {
			return err
		}
	}
	if err := syncDirectory(filepath.Dir(destination)); err != nil {
		return err
	}
	if err := writeAtomic(s.cacheRecord(artifact), raw, 0o600); err != nil {
		return err
	}
	// Once the final receipt exists, a leftover intent is harmless.
	_ = os.Remove(intent)
	return nil
}

func (s *Server) CommitActivation(_ context.Context, ref *pb.ActivationRef) (*pb.Empty, error) {
	if err := s.validateRef(ref); err != nil {
		return nil, err
	}
	unlock := s.lock(ref.GetActorUid())
	defer unlock()
	if s.isActive(ref.GetActorUid()) {
		return nil, status.Error(codes.FailedPrecondition, "actor is active")
	}
	activation, err := readActivation(filepath.Join(s.pending(ref), activationFile))
	if err != nil {
		return nil, err
	}
	for _, artifact := range activation.GetArtifacts() {
		switch artifact.GetKind() {
		case pb.ArtifactKind_ARTIFACT_KIND_ACTOR_STATE:
			receipt, err := os.ReadFile(filepath.Join(s.pending(ref), "actor-state.complete"))
			if err != nil || string(receipt) != artifact.GetSha256() {
				return nil, status.Error(codes.FailedPrecondition, "actor state is incomplete")
			}
		case pb.ArtifactKind_ARTIFACT_KIND_RUNTIME:
			receipt, err := os.ReadFile(runtimeComplete(s.runtimePending(ref), artifact.GetId()))
			if err != nil || string(receipt) != artifact.GetSha256() {
				return nil, status.Error(codes.FailedPrecondition, "runtime asset is incomplete")
			}
		default:
			if !s.cached(artifact) {
				return nil, status.Error(codes.FailedPrecondition, "immutable artifact is incomplete")
			}
		}
	}
	if !proto.Equal(activation.GetRef(), ref) {
		return nil, status.Error(codes.PermissionDenied, "activation identity differs")
	}
	tree := filepath.Join(s.pending(ref), "actor-state")
	actorTree := filepath.Join(tree, actorTreeName())
	raw, err := protojson.Marshal(activation)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(actorTree, 0o700); err != nil {
		return nil, err
	}
	if err := writeAtomic(filepath.Join(actorTree, activationFile), raw, 0o600); err != nil {
		return nil, err
	}
	// Fold the per-actor sandbox runtime assets into the pending actor tree so
	// the rename of that tree places them atomically at actors/<uid>/sandbox-assets,
	// where the worker's validateRunscPath requires them. Only
	// the asset-name subtrees the activation declares are moved; the pending
	// completeness markers stay behind and are discarded with the pending dir.
	if err := s.foldRuntimeAssets(ref, activation, actorTree); err != nil {
		return nil, err
	}
	if err := s.installActorState(ref, tree); err != nil {
		return nil, err
	}
	if err := imagecache.RemoveAllWritable(s.pending(ref)); err != nil {
		return nil, err
	}
	return &pb.Empty{}, nil
}

// installActorState places the two trees of a committed activation's actor
// state, pending/actor-state/<tree>, at <base>/<tree>/<uid>, replacing what a
// previous activation left there. An empty snapshots tree is not placed. The
// snapshots tree goes first and the actor
// tree last, because the actor tree carries the committed activation record
// that every later step requires. If the actor tree cannot be placed, the
// snapshots tree is moved back into pending so a retried commit installs both
// again; if even that fails, it is removed and the actor-state receipt is
// dropped, so the commit cannot succeed until the actor state is uploaded
// again. Either way a failed commit leaves no committed activation next to a
// partial state.
func (s *Server) installActorState(ref *pb.ActivationRef, tree string) error {
	actorUID := ref.GetActorUid()
	actorSource := filepath.Join(tree, actorTreeName())
	snapshotsSource := filepath.Join(tree, snapshotsTreeName())
	actorDestination := actorDir(s.base, actorUID)
	snapshotsDestination := snapshotsDir(s.base, actorUID)
	if err := removeStoppedActor(actorDestination); err != nil {
		return err
	}
	if err := imagecache.RemoveAllWritable(snapshotsDestination); err != nil {
		return err
	}
	for _, destination := range []string{actorDestination, snapshotsDestination} {
		if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
			return err
		}
	}
	// An empty snapshots tree (nothing staged for restore or checkpoint) is
	// not placed: it stays in pending, which is removed after the commit, so
	// it leaves no snapshots/<uid> behind.
	snapshotsPlaced := false
	if entries, err := os.ReadDir(snapshotsSource); err == nil && len(entries) > 0 {
		if err := os.Rename(snapshotsSource, snapshotsDestination); err != nil {
			return err
		}
		snapshotsPlaced = true
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := renameActorTree(actorSource, actorDestination); err != nil {
		if snapshotsPlaced {
			if rerr := os.Rename(snapshotsDestination, snapshotsSource); rerr != nil {
				_ = imagecache.RemoveAllWritable(snapshotsDestination)
				_ = os.Remove(filepath.Join(s.pending(ref), "actor-state.complete"))
			}
		}
		return err
	}
	for _, destination := range []string{actorDestination, snapshotsDestination} {
		if err := syncDirectory(filepath.Dir(destination)); err != nil {
			return err
		}
	}
	return nil
}

// renameActorTree is os.Rename, indirected so a test can fail the actor tree
// install after the snapshots tree was placed.
var renameActorTree = os.Rename

// foldRuntimeAssets moves every RUNTIME asset the activation declares from this
// generation's pending sandbox-assets dir into the pending actor tree's
// sandbox-assets dir, so the rename of that tree to actors/<uid> carries
// them. Each id is a single plain path component (validateArtifact enforces
// assetNamePattern), so source and destination stay strictly inside their
// roots. Actor state never ships its own sandbox-assets (archivePath
// restricts ACTOR_STATE to the roots of actorStateTrees), so the target is
// created fresh here.
func (s *Server) foldRuntimeAssets(ref *pb.ActivationRef, activation *pb.Activation, tree string) error {
	var runtimes []*pb.Artifact
	for _, artifact := range activation.GetArtifacts() {
		if artifact.GetKind() == pb.ArtifactKind_ARTIFACT_KIND_RUNTIME {
			runtimes = append(runtimes, artifact)
		}
	}
	if len(runtimes) == 0 {
		return nil
	}
	target := filepath.Join(tree, "sandbox-assets")
	if err := os.MkdirAll(target, 0o700); err != nil {
		return err
	}
	for _, artifact := range runtimes {
		id := artifact.GetId()
		if !assetNamePattern.MatchString(id) {
			return status.Error(codes.InvalidArgument, "invalid runtime asset ID")
		}
		source := filepath.Join(s.runtimePending(ref), id)
		destination := filepath.Join(target, id)
		// The fold must be retryable: CommitActivation moves these subtrees out
		// of pending BEFORE the actor-directory rename, so if a later commit
		// step fails the runtime markers still pass the completeness gate on a
		// retry while the fold source is gone. Treat an already-folded subtree
		// (source absent, destination present) as done rather than failing, so a
		// retry converges. Only source-and-destination both absent is a real
		// error: the upload never happened or pending was lost.
		if _, err := os.Lstat(source); errors.Is(err, os.ErrNotExist) {
			if _, derr := os.Lstat(destination); derr == nil {
				continue
			}
			return status.Error(codes.FailedPrecondition, "runtime asset is missing from pending staging")
		} else if err != nil {
			return err
		}
		if err := imagecache.RemoveAllWritable(destination); err != nil {
			return err
		}
		if err := os.Rename(source, destination); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) AbortActivation(_ context.Context, ref *pb.ActivationRef) (*pb.Empty, error) {
	if err := s.validateRef(ref); err != nil {
		return nil, err
	}
	unlock := s.lock(ref.GetActorUid())
	defer unlock()
	if s.isActive(ref.GetActorUid()) {
		return nil, status.Error(codes.FailedPrecondition, "actor is active")
	}
	if err := s.abortPending(ref); err != nil {
		return nil, err
	}
	return &pb.Empty{}, nil
}

// Caller holds the actor lock and has established that its runtime is inactive.
func (s *Server) abortPending(ref *pb.ActivationRef) error {
	activation, err := readActivation(filepath.Join(s.pending(ref), activationFile))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !proto.Equal(activation.GetRef(), ref) {
		return status.Error(codes.PermissionDenied, "pending activation identity differs")
	}
	return imagecache.RemoveAllWritable(s.pending(ref))
}

func validateSystemInfoFile(file *pb.SystemInfoFile) error {
	if !resources.IsValidResourceName(file.GetVolume()) {
		return fmt.Errorf("invalid system-info volume")
	}
	return volumepath.ValidateProjected(file.GetPath())
}

func (s *Server) ApplySystemInfo(_ context.Context, update *pb.SystemInfoUpdate) (*pb.Empty, error) {
	ref := update.GetRef()
	if err := s.validateRef(ref); err != nil {
		return nil, err
	}
	unlock := s.lock(ref.GetActorUid())
	defer unlock()
	activation, err := s.committed(ref)
	if err != nil {
		return nil, err
	}
	allowed := map[string]bool{}
	for _, file := range activation.GetSystemInfoFiles() {
		allowed[file.GetVolume()+"/"+file.GetPath()] = true
	}
	var size int
	if len(update.GetFiles()) > 128 {
		return nil, status.Error(codes.InvalidArgument, "too many system-info files")
	}
	for _, file := range update.GetFiles() {
		size += len(file.GetContents())
		if validateSystemInfoFile(file) != nil || !allowed[file.GetVolume()+"/"+file.GetPath()] || size > maxSystemInfoBytes {
			return nil, status.Error(codes.PermissionDenied, "system-info file is undeclared or exceeds limits")
		}
	}
	for _, file := range update.GetFiles() {
		root, err := os.OpenRoot(filepath.Join(actorDir(s.base, ref.GetActorUid()), "system-info", file.GetVolume()))
		if err != nil {
			return nil, err
		}
		err = writeProjected(root, file.GetPath(), file.GetContents())
		root.Close()
		if err != nil {
			return nil, err
		}
	}
	return &pb.Empty{}, nil
}

func writeProjected(root *os.Root, path string, data []byte) error {
	if err := root.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := filepath.Join(filepath.Dir(path), "."+filepath.Base(path)+".network-tmp")
	file, err := root.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	defer func() { _ = root.Remove(tmp) }()
	_, writeErr := file.Write(data)
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
	return root.Rename(tmp, path)
}

type checkpointRecord struct {
	Generation string   `json:"generation"`
	Files      []string `json:"files"`
}

// validCheckpointFiles is the SINGLE checkpoint-membership rule for both the
// server (LifecycleInterceptor, ExportCheckpoint) and the client (Export), so
// the two sides cannot drift. It is exactly
// resources.ValidateSnapshotFileNames -- each name a distinct plain file name
// in the checkpoint directory OR one of its single-level checkpoint
// subdirectories: "fs" (resources.FSCheckpointSubdir) for the durable-dir
// filesystem checkpoint, and "rootfs"
// (resources.RootfsCheckpointSubdir) for the container rootfs overlays
// (runsc writes these via `runsc checkpoint -fs-checkpoint-paths` /
// `runsc fscheckpoint`), with ".." / absolute / deeper nesting / duplicates /
// the empty name all rejected (filepath.IsLocal("") is false) -- plus the
// stricter 128-name cap, which ValidateSnapshotFileNames does not impose. Any
// consumer that turns a name into a path must still do so through os.Root so a
// symlink cannot escape the checkpoint directory.
func validCheckpointFiles(files []string) bool {
	if len(files) > 128 {
		return false
	}
	return resources.ValidateSnapshotFileNames(files) == nil
}

// LifecycleInterceptor serializes staging with lifecycle for this actor and
// records the runtime's exact checkpoint file set only after its RPC succeeds.
// The outer workertransport interceptor authenticates the node atelet.
func (s *Server) LifecycleInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		// Worker lifecycle requests identify the actor but carry no activation
		// generation. Only requests that name an actor are serialized with
		// staging; anything else (stats, probes) passes straight through.
		workload, ok := req.(interface {
			GetAtespace() string
			GetActorName() string
			GetActorUid() string
		})
		if !ok {
			return handler(ctx, req)
		}
		if !resources.IsValidResourceName(workload.GetActorUid()) {
			return nil, status.Error(codes.PermissionDenied, "lifecycle request has no valid actor UID")
		}
		unlock := s.lock(workload.GetActorUid())
		defer unlock()
		// Assignment is persisted before staging starts. A failed startup can
		// therefore be terminated before any runtime assets or actor directory
		// arrived. Preserve the existing atelet NotFound completion path without
		// calling runsc or weakening the committed identity check for live state.
		if _, terminate := req.(*ateworkerpb.TerminateWorkloadRequest); terminate && !s.isActive(workload.GetActorUid()) {
			if _, err := os.Lstat(actorDir(s.base, workload.GetActorUid())); errors.Is(err, os.ErrNotExist) {
				return nil, status.Error(codes.NotFound, "actor activation was never committed or has been removed")
			}
		}
		// The generation is taken from the worker's own committed activation,
		// not from the request, so a caller cannot select a generation the
		// worker never committed.
		activation, err := s.committedByActor(workload.GetActorUid())
		if err != nil {
			return nil, err
		}
		ref := activation.GetRef()
		if workload.GetAtespace() != activation.GetActorAtespace() || workload.GetActorName() != activation.GetActorName() {
			return nil, status.Error(codes.PermissionDenied, "lifecycle actor differs from committed activation")
		}
		response, err := handler(ctx, req)
		if err != nil {
			return nil, err
		}
		if checkpoint, ok := response.(*ateworkerpb.CheckpointWorkloadResponse); ok {
			if !validCheckpointFiles(checkpoint.GetSnapshotFiles()) {
				return nil, status.Error(codes.DataLoss, "runtime returned invalid checkpoint membership")
			}
			raw, err := json.Marshal(checkpointRecord{ref.GetGeneration(), checkpoint.GetSnapshotFiles()})
			if err != nil {
				return nil, err
			}
			if err := writeAtomic(filepath.Join(actorDir(s.base, ref.GetActorUid()), checkpointFile), raw, 0o600); err != nil {
				return nil, err
			}
		}
		return response, nil
	}
}

func (s *Server) ExportCheckpoint(ref *pb.ActivationRef, stream grpc.ServerStreamingServer[pb.CheckpointChunk]) error {
	if err := s.validateRef(ref); err != nil {
		return err
	}
	unlock := s.lock(ref.GetActorUid())
	defer unlock()
	if _, err := s.committed(ref); err != nil {
		return err
	}
	if s.isActive(ref.GetActorUid()) {
		return status.Error(codes.FailedPrecondition, "actor is still active")
	}
	raw, err := os.ReadFile(filepath.Join(actorDir(s.base, ref.GetActorUid()), checkpointFile))
	if err != nil {
		return status.Error(codes.FailedPrecondition, "no completed checkpoint")
	}
	var record checkpointRecord
	if json.Unmarshal(raw, &record) != nil || record.Generation != ref.GetGeneration() || !validCheckpointFiles(record.Files) {
		return status.Error(codes.DataLoss, "checkpoint ownership record differs")
	}
	root, err := os.OpenRoot(filepath.Join(snapshotsDir(s.base, ref.GetActorUid()), "checkpoint-state"))
	if err != nil {
		return err
	}
	defer root.Close()
	var total int64
	for _, name := range record.Files {
		info, err := root.Lstat(name)
		if err != nil || !info.Mode().IsRegular() || info.Size() > MaxArtifactBytes {
			return status.Error(codes.DataLoss, "checkpoint file is absent, nonregular or excessive")
		}
		total += info.Size()
		if total > MaxActivationBytes {
			return status.Error(codes.ResourceExhausted, "checkpoint total size exceeds limit")
		}
		if err := stream.Send(&pb.CheckpointChunk{FileName: name, Size: info.Size()}); err != nil {
			return err
		}
		file, err := root.Open(name)
		if err != nil {
			return err
		}
		hash := sha256.New()
		buffer := make([]byte, ChunkBytes)
		var sent int64
		for {
			n, readErr := file.Read(buffer)
			if n > 0 {
				hash.Write(buffer[:n])
				sent += int64(n)
				if sent > info.Size() {
					file.Close()
					return status.Error(codes.DataLoss, "checkpoint changed while exporting")
				}
				if err := stream.Send(&pb.CheckpointChunk{Data: buffer[:n]}); err != nil {
					file.Close()
					return err
				}
			}
			if errors.Is(readErr, io.EOF) {
				break
			}
			if readErr != nil {
				file.Close()
				return readErr
			}
		}
		file.Close()
		if sent != info.Size() {
			return status.Error(codes.DataLoss, "checkpoint size changed while exporting")
		}
		if err := stream.Send(&pb.CheckpointChunk{Sha256: hex.EncodeToString(hash.Sum(nil))}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) CleanupActor(_ context.Context, ref *pb.ActivationRef) (*pb.Empty, error) {
	if err := s.validateRef(ref); err != nil {
		return nil, err
	}
	unlock := s.lock(ref.GetActorUid())
	defer unlock()
	if s.isActive(ref.GetActorUid()) {
		return nil, status.Error(codes.FailedPrecondition, "actor is active")
	}
	directory := actorDir(s.base, ref.GetActorUid())
	if _, err := os.Lstat(directory); !errors.Is(err, os.ErrNotExist) {
		if _, err := s.committed(ref); err != nil {
			return nil, err
		}
		if err := removeStoppedActor(directory); err != nil {
			return nil, err
		}
		if err := imagecache.RemoveAllWritable(snapshotsDir(s.base, ref.GetActorUid())); err != nil {
			return nil, err
		}
	}
	if err := s.abortPending(ref); err != nil {
		return nil, err
	}
	return &pb.Empty{}, nil
}

// Unlike node atelet, this service shares the runtime's mount namespace.
// Once lifecycle has stopped this actor, detach its remaining overlay and
// runsc namespace mounts before removing the actor's private files. Never
// traverse or detach another actor's directory or the immutable layer cache.
func removeStoppedActor(directory string) error {
	for _, name := range []string{"bundles", "runsc-state"} {
		if err := imagecache.UnmountAllUnder(filepath.Join(directory, name)); err != nil {
			return err
		}
	}
	return imagecache.RemoveAllWritable(directory)
}
