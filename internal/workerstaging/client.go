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
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"go.ate.dev/substrate/internal/imagecache"
	"go.ate.dev/substrate/internal/nodepath"
	pb "go.ate.dev/substrate/internal/proto/workerstagingpb"
	"go.ate.dev/substrate/internal/resources"
	"golang.org/x/sync/semaphore"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// imageCacheDir is the canonical cached-layer root, the same path
// cmd/atelet/internal/ateletpath.ImageCacheDir names. It is recomputed here
// from nodepath.BasePath because ateletpath lives under cmd/atelet/internal,
// which a top-level internal package cannot import; the join is identical, so
// a layer path that atelet wrote still matches byte-for-byte.
var imageCacheDir = filepath.Join(nodepath.BasePath, "image-cache")

type Client struct {
	base string
	rpc  pb.WorkerStagingClient
}

// Archive creation is uncommon (once per immutable asset on this node) and
// serialized across RPC clients. An interrupted preparation has no receipt.
var archiveMu sync.Mutex

func NewClient(base string, rpc pb.WorkerStagingClient) *Client {
	return &Client{base: base, rpc: rpc}
}

// Stage discovers assets only from the prepared OCI overlay specs and runtime
// paths. The physical source root is separate from runtime-visible paths, which
// must retain their canonical meaning in the worker.
func (c *Client) Stage(ctx context.Context, ref *pb.ActivationRef, actor resources.ActorRef, runtimePaths map[string]string) (result error) {
	if ref == nil || !resources.IsValidResourceName(ref.GetActorUid()) ||
		!resources.IsValidResourceName(ref.GetGeneration()) || resources.ValidateWorkerPodUID(ref.GetWorkerUid()) != nil {
		return fmt.Errorf("invalid staging activation")
	}
	activation := &pb.Activation{Ref: ref, ActorAtespace: actor.Atespace, ActorName: actor.Name}
	artifacts := map[string]*pb.Artifact{}
	add := func(kind pb.ArtifactKind, id string) {
		artifact := &pb.Artifact{Kind: kind, Id: id}
		artifacts[artifactKey(artifact)] = artifact
	}
	bundles := filepath.Join(actorDir(c.base, ref.GetActorUid()), "bundles")
	entries, err := os.ReadDir(bundles)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			return fmt.Errorf("unexpected non-directory OCI bundle")
		}
		spec, err := imagecache.ReadSpec(filepath.Join(bundles, entry.Name()))
		if err != nil {
			return err
		}
		if spec == nil {
			return fmt.Errorf("network workers require cached OCI overlay specs")
		}
		layers := append([]string(nil), spec.Layers...)
		for _, volume := range spec.ImageVolumes {
			layers = append(layers, volume.Layers...)
		}
		for _, layer := range layers {
			id := filepath.Base(layer)
			if !digestPattern.MatchString(id) || layer != filepath.Join(imageCacheDir, "layers", "sha256", id) {
				return fmt.Errorf("noncanonical image layer path %q", layer)
			}
			add(pb.ArtifactKind_ARTIFACT_KIND_IMAGE_LAYER, id)
		}
	}
	// Sandbox runtime assets live in a per-actor directory keyed by actor UID,
	// each under its sandbox asset NAME rather than a content-addressed digest. A runtimePaths value is the path the worker
	// runs a binary from, e.g. .../sandbox-assets/gvisor/runsc (the gvisor
	// release) or .../sandbox-assets/runsc (a bare runsc). The artifact staged
	// is the whole asset-name subtree, so the id is the FIRST path component
	// under THIS actor's own sandbox-assets dir, validated as a plain asset
	// name (no digest, no separators, no "." / ".."), matching the shape
	// cmd/atelet's plainAssetName produces and the worker's validateRunscPath
	// accepts. The path must be absolute, clean and strictly inside the actor's
	// own root; a traversal, an other-actor root, or a relative path is refused.
	// The same asset name may appear under several runtimePaths values (runsc
	// and a helper beside it); add() dedups on the artifact key, so one subtree
	// is staged once.
	assetsRoot := nodepath.ActorSandboxAssetsDir(ref.GetActorUid())
	for _, path := range runtimePaths {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return fmt.Errorf("noncanonical runtime path %q", path)
		}
		relative, err := filepath.Rel(assetsRoot, path)
		if err != nil || relative == "." || !filepath.IsLocal(relative) {
			return fmt.Errorf("runtime path %q is not inside %q", path, assetsRoot)
		}
		id := strings.SplitN(relative, string(filepath.Separator), 2)[0]
		if !assetNamePattern.MatchString(id) {
			return fmt.Errorf("invalid runtime asset name %q in path %q", id, path)
		}
		add(pb.ArtifactKind_ARTIFACT_KIND_RUNTIME, id)
	}
	add(pb.ArtifactKind_ARTIFACT_KIND_ACTOR_STATE, ref.GetGeneration())
	keys := make([]string, 0, len(artifacts))
	for key := range artifacts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	paths := map[string]string{}
	for _, key := range keys {
		artifact, path, err := c.archive(ctx, ref.GetActorUid(), artifacts[key])
		if err != nil {
			return err
		}
		if artifact.GetKind() == pb.ArtifactKind_ARTIFACT_KIND_ACTOR_STATE {
			defer os.Remove(path)
		}
		activation.Artifacts = append(activation.Artifacts, artifact)
		paths[key] = path
	}
	activation.SystemInfoFiles, err = c.systemInfoDeclarations(ref.GetActorUid())
	if err != nil {
		return err
	}
	defer func() {
		if result == nil {
			return
		}
		// The caller may have cancelled or lost the commit response. Abort
		// only pending files using a bounded independent cleanup context.
		// A committed activation is preserved even in that ambiguous case.
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
		defer cancel()
		if _, err := c.rpc.AbortActivation(cleanupCtx, ref); err != nil {
			result = errors.Join(result, fmt.Errorf("reclaim pending activation %s: %w", ref.GetGeneration(), err))
		}
	}()
	mutable, err := c.prepareImmutable(ctx, activation, paths)
	if err != nil {
		return err
	}
	for _, artifact := range mutable {
		if err := c.put(ctx, ref, artifact, paths[artifactKey(artifact)]); err != nil {
			return err
		}
	}
	_, err = c.rpc.CommitActivation(ctx, ref)
	return err
}

type workerPreparation struct {
	gate  *semaphore.Weighted
	users int
}

var workerPreparations = struct {
	sync.Mutex
	workers map[string]*workerPreparation
}{workers: make(map[string]*workerPreparation)}

// A cold worker must observe completed immutable uploads before another actor
// asks what is missing. Otherwise every concurrent actor can upload and extract
// the same large runtime. Actor-state transfers happen outside this gate.
// Reference counting releases entries when workers have no holders or waiters.
func lockWorkerPreparation(ctx context.Context, workerUID string) (func(), error) {
	workerPreparations.Lock()
	entry := workerPreparations.workers[workerUID]
	if entry == nil {
		entry = &workerPreparation{gate: semaphore.NewWeighted(1)}
		workerPreparations.workers[workerUID] = entry
	}
	entry.users++
	workerPreparations.Unlock()
	drop := func() {
		workerPreparations.Lock()
		defer workerPreparations.Unlock()
		entry.users--
		if entry.users == 0 {
			delete(workerPreparations.workers, workerUID)
		}
	}
	if err := entry.gate.Acquire(ctx, 1); err != nil {
		drop()
		return nil, err
	}
	return func() {
		entry.gate.Release(1)
		drop()
	}, nil
}

func (c *Client) prepareImmutable(ctx context.Context, activation *pb.Activation, paths map[string]string) ([]*pb.Artifact, error) {
	unlock, err := lockWorkerPreparation(ctx, activation.GetRef().GetWorkerUid())
	if err != nil {
		return nil, err
	}
	defer unlock()
	preparation, err := c.rpc.PrepareActivation(ctx, activation)
	if err != nil {
		return nil, err
	}
	sent := map[string]bool{}
	for _, missing := range preparation.GetMissing() {
		declared := findArtifact(activation, missing.GetKind(), missing.GetId())
		key := artifactKey(missing)
		if declared == nil || !proto.Equal(declared, missing) || sent[key] {
			return nil, fmt.Errorf("worker requested an undeclared or duplicate artifact")
		}
		sent[key] = true
	}
	var mutable []*pb.Artifact
	for _, missing := range preparation.GetMissing() {
		// Actor state and runtime assets are per-actor, so both are uploaded OUTSIDE the worker-preparation gate. The gate exists
		// only to upload a genuinely node-wide shared immutable (the image
		// layers) once across concurrent actors; a per-actor artifact has no
		// such sharing to serialize, and holding the gate across it would
		// needlessly block other actors.
		if missing.GetKind() == pb.ArtifactKind_ARTIFACT_KIND_ACTOR_STATE ||
			missing.GetKind() == pb.ArtifactKind_ARTIFACT_KIND_RUNTIME {
			mutable = append(mutable, missing)
			continue
		}
		if err := c.put(ctx, activation.GetRef(), missing, paths[artifactKey(missing)]); err != nil {
			return nil, err
		}
	}
	return mutable, nil
}

func (c *Client) archive(ctx context.Context, actorUID string, artifact *pb.Artifact) (*pb.Artifact, string, error) {
	archiveMu.Lock()
	defer archiveMu.Unlock()
	directory := filepath.Join(c.base, "network-artifact-cache")
	// A RUNTIME asset is per-actor: its id is a plain asset
	// NAME (e.g. "gvisor") shared across actors, not a content-addressed digest.
	// The local archive cache is keyed by (kind,id), so caching a runtime by id
	// alone would let a later actor reuse an EARLIER actor's packed tar and
	// digest without ever reading its own per-actor asset, installing another
	// actor's executable bytes. Key the runtime cache under the actor UID so
	// each actor packs and caches from its own sandbox-assets dir. IMAGE_LAYER
	// stays content-addressed and node-wide, so it is shared by id as before.
	if artifact.GetKind() == pb.ArtifactKind_ARTIFACT_KIND_RUNTIME {
		if !resources.IsValidResourceName(actorUID) {
			return nil, "", fmt.Errorf("invalid actor UID")
		}
		directory = filepath.Join(directory, "runtime", actorUID)
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, "", err
	}
	path := filepath.Join(directory, artifactKey(artifact)+".tar")
	record := path + ".json"
	if artifact.GetKind() != pb.ArtifactKind_ARTIFACT_KIND_ACTOR_STATE {
		var stored pb.Artifact
		if raw, err := os.ReadFile(record); err == nil && protojson.Unmarshal(raw, &stored) == nil &&
			stored.GetKind() == artifact.GetKind() && stored.GetId() == artifact.GetId() && validateArtifact(&stored) == nil {
			if info, err := os.Lstat(path); err == nil && info.Mode().IsRegular() && info.Size() == stored.GetSize() {
				return &stored, path, nil
			}
		}
	}
	file, err := os.CreateTemp(directory, ".archive-*")
	if err != nil {
		return nil, "", err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	hash := sha256.New()
	if err := packArtifact(ctx, c.base, actorUID, artifact, io.MultiWriter(file, hash)); err != nil {
		return nil, "", err
	}
	info, err := file.Stat()
	if err != nil {
		return nil, "", err
	}
	artifact = proto.Clone(artifact).(*pb.Artifact)
	artifact.Size, artifact.Sha256 = info.Size(), hex.EncodeToString(hash.Sum(nil))
	if err := validateArtifact(artifact); err != nil {
		return nil, "", err
	}
	if err := file.Sync(); err != nil {
		return nil, "", err
	}
	if err := file.Close(); err != nil {
		return nil, "", err
	}
	if err := os.Rename(file.Name(), path); err != nil {
		return nil, "", err
	}
	if artifact.GetKind() != pb.ArtifactKind_ARTIFACT_KIND_ACTOR_STATE {
		raw, err := protojson.Marshal(artifact)
		if err != nil {
			return nil, "", err
		}
		if err := writeAtomic(record, raw, 0o600); err != nil {
			return nil, "", err
		}
	}
	return artifact, path, nil
}

func (c *Client) systemInfoDeclarations(actorUID string) ([]*pb.SystemInfoFile, error) {
	path := filepath.Join(actorDir(c.base, actorUID), "system-info")
	root, err := os.OpenRoot(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer root.Close()
	var files []*pb.SystemInfoFile
	err = fs.WalkDir(root.FS(), ".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("system-info projection must be a regular file")
		}
		parts := strings.SplitN(filepath.ToSlash(path), "/", 2)
		if len(parts) != 2 {
			return fmt.Errorf("system-info file has no volume")
		}
		file := &pb.SystemInfoFile{Volume: parts[0], Path: parts[1]}
		if err := validateSystemInfoFile(file); err != nil {
			return err
		}
		files = append(files, file)
		return nil
	})
	return files, err
}

func (c *Client) put(ctx context.Context, ref *pb.ActivationRef, artifact *pb.Artifact, path string) (result error) {
	started := time.Now()
	var queued int64
	defer func() {
		slog.InfoContext(ctx, "Network artifact upload",
			"ate.actor.uid", ref.GetActorUid(), "worker_uid", ref.GetWorkerUid(),
			"artifact_kind", artifact.GetKind().String(), "artifact_id", artifact.GetId(),
			"declared_bytes", artifact.GetSize(), "queued_bytes", queued,
			"duration_ms", float64(time.Since(started))/float64(time.Millisecond),
			"completed", result == nil)
	}()
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	stream, err := c.rpc.PutArtifact(ctx)
	if err != nil {
		return err
	}
	buffer := make([]byte, ChunkBytes)
	for {
		n, readErr := file.Read(buffer)
		if n > 0 {
			if err := stream.Send(&pb.ArtifactChunk{Ref: ref, Kind: artifact.GetKind(), Id: artifact.GetId(), Data: buffer[:n]}); err != nil {
				return err
			}
			queued += int64(n)
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return readErr
		}
	}
	_, err = stream.CloseAndRecv()
	return err
}

// Export commits the exact runtime-reported file set into node custody only
// after the stream ends cleanly and every byte count and digest is verified.
// Existing custody remains intact on any incomplete or incorrect transfer.
func (c *Client) Export(ctx context.Context, ref *pb.ActivationRef, expected []string, destination string) error {
	if !validCheckpointFiles(expected) {
		return fmt.Errorf("invalid checkpoint membership")
	}
	stream, err := c.rpc.ExportCheckpoint(ctx, ref)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		return err
	}
	tree, err := os.MkdirTemp(filepath.Dir(destination), ".checkpoint-transfer-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(tree) }()
	if err := receiveCheckpoint(stream.Recv, expected, tree); err != nil {
		return err
	}
	if err := syncDirectory(tree); err != nil {
		return err
	}
	// The lifecycle's checkpoint directory is empty until this transfer. Refuse
	// to erase nonempty custody; the caller must explicitly finish/reset it.
	if err := os.Remove(destination); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.Rename(tree, destination); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(destination))
}

func receiveCheckpoint(recv func() (*pb.CheckpointChunk, error), expected []string, tree string) error {
	remaining := map[string]bool{}
	for _, name := range expected {
		remaining[name] = true
	}
	// Create and open every file through an os.Root anchored at tree, so a
	// symlink on any path component cannot lead a write outside the transfer
	// directory. validCheckpointFiles (shared with the server and equal to
	// resources.ValidateSnapshotFileNames) has already restricted
	// every expected name to a plain file in tree or one of its single-level
	// checkpoint subdirectories ("fs", resources.FSCheckpointSubdir, for
	// durable-dir volumes, and "rootfs",
	// resources.RootfsCheckpointSubdir, for container rootfs overlays), so the
	// only directory a name can need is one of those; whichever a name names is
	// created generically below (filepath.Dir != ".") with 0o700 before the
	// O_EXCL open. os.Root.OpenFile keeps the O_EXCL guarantee.
	root, err := os.OpenRoot(tree)
	if err != nil {
		return err
	}
	defer root.Close()
	var total int64
	for {
		header, err := recv()
		if errors.Is(err, io.EOF) {
			if len(remaining) != 0 {
				return fmt.Errorf("checkpoint stream omitted files")
			}
			return nil
		}
		if err != nil {
			return err
		}
		if !remaining[header.GetFileName()] || header.GetSize() < 0 || header.GetSize() > MaxArtifactBytes ||
			len(header.GetData()) != 0 || header.GetSha256() != "" {
			return fmt.Errorf("invalid or unexpected checkpoint header")
		}
		total += header.GetSize()
		if total > MaxActivationBytes {
			return fmt.Errorf("checkpoint exceeds total size limit")
		}
		name := header.GetFileName()
		if dir := filepath.Dir(name); dir != "." {
			if err := root.MkdirAll(dir, 0o700); err != nil {
				return err
			}
		}
		file, err := root.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			return err
		}
		err = receiveCheckpointFile(recv, header.GetSize(), file)
		closeErr := file.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		delete(remaining, name)
	}
}

func receiveCheckpointFile(recv func() (*pb.CheckpointChunk, error), size int64, file *os.File) error {
	hash := sha256.New()
	var received int64
	for {
		chunk, err := recv()
		if err != nil {
			return fmt.Errorf("incomplete checkpoint file: %w", err)
		}
		if chunk.GetFileName() != "" || chunk.GetSize() != 0 {
			return fmt.Errorf("unexpected checkpoint header inside file")
		}
		if chunk.GetSha256() != "" {
			if len(chunk.GetData()) != 0 || received != size || chunk.GetSha256() != hex.EncodeToString(hash.Sum(nil)) {
				return fmt.Errorf("checkpoint size or SHA256 differs")
			}
			return file.Sync()
		}
		if len(chunk.GetData()) == 0 || len(chunk.GetData()) > ChunkBytes || int64(len(chunk.GetData())) > size-received {
			return fmt.Errorf("checkpoint chunk exceeds declared size")
		}
		if _, err := io.MultiWriter(file, hash).Write(chunk.GetData()); err != nil {
			return err
		}
		received += int64(len(chunk.GetData()))
	}
}
