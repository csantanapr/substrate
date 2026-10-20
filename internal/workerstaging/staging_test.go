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
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"go.ate.dev/substrate/internal/imagecache"
	"go.ate.dev/substrate/internal/nodepath"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	pb "go.ate.dev/substrate/internal/proto/workerstagingpb"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func fixtureFile(t *testing.T, base, path, contents string) {
	t.Helper()
	full := filepath.Join(base, path)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(contents), 0o755); err != nil {
		t.Fatal(err)
	}
}

// fixtureRuntimeAssetName is the gVisor runtime asset NAME every fixture actor
// references. Under upstream #2699 a runtime asset is identified by its plain
// sandbox asset name, not a content-addressed digest, and lives per-actor under
// actors/<uid>/sandbox-assets/<name>/, so each actor stages its own copy and
// there is no cross-actor sharing to dedup.
const fixtureRuntimeAssetName = "gvisor"

// fixtureActorRuntime writes the gVisor release into an actor's OWN per-actor
// sandbox-assets dir, exactly as cmd/atelet's ensureSandboxAssets stages it:
// runsc plus a gvisor-bin/ helper beside it, under the asset name. base stands
// in for nodepath.BasePath, so the physical tree mirrors /var/lib/ate and the
// absolute runtimePaths value resolves under it.
func fixtureActorRuntime(t *testing.T, base, actorUID string) {
	t.Helper()
	fixtureActorRuntimeContent(t, base, actorUID, "runtime binary")
}

// fixtureActorRuntimeContent is fixtureActorRuntime with caller-chosen runsc
// bytes, so a test can give two actors DISTINCT runtime content and prove each
// guest receives its OWN actor's bytes (the per-actor client-cache property).
func fixtureActorRuntimeContent(t *testing.T, base, actorUID, runscContent string) {
	t.Helper()
	rel, err := filepath.Rel(nodepath.BasePath, nodepath.ActorSandboxAssetsDir(actorUID))
	if err != nil {
		t.Fatal(err)
	}
	fixtureFile(t, base, filepath.Join(rel, fixtureRuntimeAssetName, "runsc"), runscContent)
	fixtureFile(t, base, filepath.Join(rel, fixtureRuntimeAssetName, "gvisor-bin", "runsc-gofer"), "helper binary")
}

// fixtureNodeAssets is the runtime-visible asset map an actor presents to Stage:
// the asset name keyed to the local path of runsc inside that actor's own
// per-actor sandbox-assets dir, which Stage validates against ref.ActorUid.
func fixtureNodeAssets(actorUID string) map[string]string {
	return map[string]string{fixtureRuntimeAssetName: filepath.Join(nodepath.ActorSandboxAssetsDir(actorUID), fixtureRuntimeAssetName, "runsc")}
}

func fixtureNode(t *testing.T, base, actorUID string) map[string]string {
	t.Helper()
	layerID := strings.Repeat("a", 64)
	layer := filepath.Join("image-cache", "layers", "sha256", layerID)
	fixtureFile(t, base, filepath.Join(layer, "fs", "bin", "sample"), "image bytes")
	fixtureFile(t, base, filepath.Join(layer, "whiteouts.json"), `{"whiteouts":["old"],"opaqueDirs":["etc"]}`)
	fixtureFile(t, base, filepath.Join(layer, "finalized"), "host-only")
	if err := os.Symlink("/bin/sample", filepath.Join(base, layer, "fs", "absolute-link")); err != nil {
		t.Fatal(err)
	}
	// Each actor carries the gVisor release in its own per-actor sandbox-assets
	// dir (upstream #2699 replaced the removed shared nodepath.StaticFilesDir
	// with this per-actor layout), identified by the plain asset name "gvisor".
	fixtureActorRuntime(t, base, actorUID)
	actor := filepath.Join("actors", actorUID)
	fixtureFile(t, base, filepath.Join(actor, "bundles", "main", "config.json"), `{"root":{"path":"rootfs"}}`)
	if err := imagecache.WriteSpec(filepath.Join(base, actor, "bundles", "main"), &imagecache.OverlaySpec{
		Layers: []string{filepath.Join(imageCacheDir, "layers", "sha256", layerID)},
	}); err != nil {
		t.Fatal(err)
	}
	fixtureFile(t, base, filepath.Join(actor, "system-info", "trust", "ca.pem"), "trust-one")
	snapshots := filepath.Join(snapshotsTreeName(), actorUID)
	fixtureFile(t, base, filepath.Join(snapshots, "restore-state", "image"), "restore bytes")
	fixtureFile(t, base, filepath.Join(snapshots, "local-checkpoint", "old", "must-not-stage"), "node custody")
	fixtureFile(t, base, "credential-broker.sock", "must-not-stage")
	return fixtureNodeAssets(actorUID)
}

type fixtureRuntime struct {
	ateworkerpb.UnimplementedWorkerServer
	base   string
	active atomic.Bool
	runs   atomic.Int32
}

func (r *fixtureRuntime) RunWorkload(context.Context, *ateworkerpb.RunWorkloadRequest) (*ateworkerpb.RunWorkloadResponse, error) {
	r.runs.Add(1)
	r.active.Store(true)
	return &ateworkerpb.RunWorkloadResponse{}, nil
}

func (r *fixtureRuntime) CheckpointWorkload(_ context.Context, req *ateworkerpb.CheckpointWorkloadRequest) (*ateworkerpb.CheckpointWorkloadResponse, error) {
	directory := filepath.Join(snapshotsDir(r.base, req.GetActorUid()), "checkpoint-state")
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(directory, "image"), []byte("durable checkpoint bytes"), 0o600); err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(directory, "empty"), nil, 0o600); err != nil {
		return nil, err
	}
	r.active.Store(false)
	return &ateworkerpb.CheckpointWorkloadResponse{SnapshotFiles: []string{"image", "empty"}}, nil
}

// This fixture tests file and lifecycle wiring across independent roots.
// workertransport's separate real-TLS tests cover network authentication.
func stagingConnection(t *testing.T, base, workerUID string) (pb.WorkerStagingClient, ateworkerpb.WorkerClient, *fixtureRuntime) {
	t.Helper()
	runtime := &fixtureRuntime{base: base}
	staging, err := NewServer(base, workerUID, func(string) bool { return runtime.active.Load() })
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.UnaryInterceptor(staging.LifecycleInterceptor()))
	pb.RegisterWorkerStagingServer(server, staging)
	ateworkerpb.RegisterWorkerServer(server, runtime)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return pb.NewWorkerStagingClient(conn), ateworkerpb.NewWorkerClient(conn), runtime
}

func TestIndependentFilesystemsActivationAndCheckpointCustody(t *testing.T) {
	node, guest := t.TempDir(), t.TempDir()
	ref := &pb.ActivationRef{ActorUid: uuid.NewString(), WorkerUid: uuid.NewString(), Generation: uuid.NewString()}
	assets := fixtureNode(t, node, ref.GetActorUid())
	rpc, lifecycle, runtime := stagingConnection(t, guest, ref.GetWorkerUid())
	client := NewClient(node, rpc)
	run := &ateworkerpb.RunWorkloadRequest{Atespace: "team", ActorName: "counter", ActorUid: ref.GetActorUid()}
	if _, err := lifecycle.RunWorkload(t.Context(), run); err == nil || runtime.runs.Load() != 0 {
		t.Fatal("unstaged lifecycle was admitted")
	}
	if err := client.Stage(t.Context(), ref, resources.ActorRef{Atespace: "team", Name: "counter"}, assets); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{
		filepath.Join(snapshotsTreeName(), ref.GetActorUid(), "restore-state", "image"),
		// The runtime asset goes to the actor's own per-actor sandbox-assets
		// dir on the worker (upstream #2699), runsc and its helper together.
		filepath.Join("actors", ref.GetActorUid(), "sandbox-assets", fixtureRuntimeAssetName, "runsc"),
		filepath.Join("actors", ref.GetActorUid(), "sandbox-assets", fixtureRuntimeAssetName, "gvisor-bin", "runsc-gofer"),
		filepath.Join("image-cache", "layers", "sha256", strings.Repeat("a", 64), "whiteouts.json"),
	} {
		want, err := os.ReadFile(filepath.Join(node, path))
		if err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(filepath.Join(guest, path))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("staged %s: %q, %v", path, got, err)
		}
	}
	for _, path := range []string{"credential-broker.sock", filepath.Join(snapshotsTreeName(), ref.GetActorUid(), "local-checkpoint"),
		filepath.Join("image-cache", "layers", "sha256", strings.Repeat("a", 64), "finalized")} {
		if _, err := os.Lstat(filepath.Join(guest, path)); !os.IsNotExist(err) {
			t.Fatalf("node-only path staged: %s", path)
		}
	}
	// The interceptor derives the generation from the worker's committed
	// activation, not from the request, so a lifecycle call for an actor that
	// has no committed activation on this worker is refused (it cannot borrow a
	// committed generation). A different, uncommitted actor UID is rejected.
	unknown := proto.Clone(run).(*ateworkerpb.RunWorkloadRequest)
	unknown.ActorUid = uuid.NewString()
	if _, err := lifecycle.RunWorkload(t.Context(), unknown); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("uncommitted actor reached runtime: %v", err)
	}
	foreign := proto.Clone(run).(*ateworkerpb.RunWorkloadRequest)
	foreign.ActorName = "other"
	if _, err := lifecycle.RunWorkload(t.Context(), foreign); status.Code(err) != codes.PermissionDenied || runtime.runs.Load() != 0 {
		t.Fatalf("different actor name reached runtime: %v", err)
	}
	if _, err := lifecycle.RunWorkload(t.Context(), run); err != nil {
		t.Fatal(err)
	}
	update := &pb.SystemInfoUpdate{Ref: ref, Files: []*pb.SystemInfoFile{{Volume: "trust", Path: "ca.pem", Contents: []byte("trust-two")}}}
	if _, err := rpc.ApplySystemInfo(t.Context(), update); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(guest, "actors", ref.GetActorUid(), "system-info", "trust", "ca.pem"))
	if err != nil || string(got) != "trust-two" {
		t.Fatalf("running projection did not refresh: %q %v", got, err)
	}
	update.Files[0].Path = "undeclared.pem"
	if _, err := rpc.ApplySystemInfo(t.Context(), update); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("undeclared projection: %v", err)
	}
	if _, err := rpc.CleanupActor(t.Context(), ref); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("active actor cleanup: %v", err)
	}
	checkpointRequest := &ateworkerpb.CheckpointWorkloadRequest{Atespace: "other", ActorName: "counter", ActorUid: ref.GetActorUid()}
	if _, err := lifecycle.CheckpointWorkload(t.Context(), checkpointRequest); status.Code(err) != codes.PermissionDenied || !runtime.active.Load() {
		t.Fatalf("different atespace checkpoint reached runtime: %v", err)
	}
	checkpointRequest.Atespace = "team"
	checkpoint, err := lifecycle.CheckpointWorkload(t.Context(), checkpointRequest)
	if err != nil {
		t.Fatal(err)
	}
	custody := filepath.Join(node, "received-checkpoint")
	if err := client.Export(t.Context(), ref, checkpoint.GetSnapshotFiles(), custody); err != nil {
		t.Fatal(err)
	}
	if _, err := rpc.CleanupActor(t.Context(), ref); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(guest); err != nil {
		t.Fatal(err)
	}
	got, err = os.ReadFile(filepath.Join(custody, "image"))
	if err != nil || string(got) != "durable checkpoint bytes" {
		t.Fatalf("node custody depended on worker filesystem: %q %v", got, err)
	}
}

func TestArtifactIntegrityAndCommitGate(t *testing.T) {
	guest := t.TempDir()
	ref := &pb.ActivationRef{ActorUid: uuid.NewString(), WorkerUid: uuid.NewString(), Generation: uuid.NewString()}
	rpc, lifecycle, runtime := stagingConnection(t, guest, ref.GetWorkerUid())
	body := archiveFixture(t, withActorStateTrees([]*tar.Header{{Name: "actors/bundles", Typeflag: tar.TypeDir, Mode: 0o755}}))
	sum := sha256.Sum256(body)
	artifact := &pb.Artifact{Kind: pb.ArtifactKind_ARTIFACT_KIND_ACTOR_STATE, Id: ref.GetGeneration(), Size: int64(len(body)), Sha256: hex.EncodeToString(sum[:])}
	manifest := &pb.Activation{Ref: ref, ActorAtespace: "team", ActorName: "counter", Artifacts: []*pb.Artifact{artifact}}
	if _, err := rpc.PrepareActivation(t.Context(), manifest); err != nil {
		t.Fatal(err)
	}
	if _, err := rpc.CommitActivation(t.Context(), ref); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("partial activation committed: %v", err)
	}
	send := func(data []byte) error {
		stream, err := rpc.PutArtifact(t.Context())
		if err != nil {
			return err
		}
		if err = stream.Send(&pb.ArtifactChunk{Ref: ref, Kind: artifact.Kind, Id: artifact.Id, Data: data}); err != nil {
			return err
		}
		_, err = stream.CloseAndRecv()
		return err
	}
	bad := append([]byte(nil), body...)
	bad[len(bad)-1] ^= 1
	if err := send(bad); status.Code(err) != codes.DataLoss {
		t.Fatalf("wrong digest accepted: %v", err)
	}
	if _, err := lifecycle.RunWorkload(t.Context(), &ateworkerpb.RunWorkloadRequest{ActorUid: ref.ActorUid}); err == nil || runtime.runs.Load() != 0 {
		t.Fatal("incomplete artifact reached runtime")
	}
	if err := send(body); err != nil {
		t.Fatal(err)
	}
	if _, err := rpc.CommitActivation(t.Context(), ref); err != nil {
		t.Fatal(err)
	}
	if _, err := lifecycle.RunWorkload(t.Context(), &ateworkerpb.RunWorkloadRequest{Atespace: "team", ActorName: "counter", ActorUid: ref.ActorUid}); err != nil || runtime.runs.Load() != 1 {
		t.Fatalf("complete artifact did not reach runtime: %v", err)
	}
}

// withActorStateTrees prepends a directory entry for each actor-state tree
// that headers does not name itself.
func withActorStateTrees(headers []*tar.Header) []*tar.Header {
	var out []*tar.Header
	for _, tree := range actorStateTrees() {
		named := false
		for _, header := range headers {
			if strings.TrimSuffix(header.Name, "/") == tree.name {
				named = true
			}
		}
		if !named {
			out = append(out, &tar.Header{Name: tree.name + "/", Typeflag: tar.TypeDir, Mode: 0o700})
		}
	}
	return append(out, headers...)
}

func archiveFixture(t *testing.T, headers []*tar.Header) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := tar.NewWriter(&buffer)
	for _, header := range headers {
		if header.Mode == 0 {
			header.Mode = 0o755
		}
		if err := writer.WriteHeader(header); err != nil {
			t.Fatal(err)
		}
		if header.Size > 0 {
			if _, err := writer.Write(bytes.Repeat([]byte("x"), int(header.Size))); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

func TestArchivePathsAndLinks(t *testing.T) {
	for _, tc := range []struct {
		name    string
		kind    pb.ArtifactKind
		headers []*tar.Header
		ok      bool
	}{
		{"image symlink", pb.ArtifactKind_ARTIFACT_KIND_IMAGE_LAYER, []*tar.Header{{Name: "fs", Typeflag: tar.TypeDir}, {Name: "fs/link", Typeflag: tar.TypeSymlink, Linkname: "/bin/sh"}}, true},
		{"prepared actor directory", pb.ArtifactKind_ARTIFACT_KIND_ACTOR_STATE, []*tar.Header{{Name: "actors/bundles", Typeflag: tar.TypeDir}, {Name: "actors/bundles/config", Typeflag: tar.TypeReg, Size: 1}}, true},
		{"root traversal", pb.ArtifactKind_ARTIFACT_KIND_IMAGE_LAYER, []*tar.Header{{Name: "../outside", Typeflag: tar.TypeReg, Size: 1}}, false},
		{"filesystem root symlink", pb.ArtifactKind_ARTIFACT_KIND_IMAGE_LAYER, []*tar.Header{{Name: "fs", Typeflag: tar.TypeSymlink, Linkname: "/tmp"}}, false},
		{"actor root symlink", pb.ArtifactKind_ARTIFACT_KIND_ACTOR_STATE, []*tar.Header{{Name: "actors/bundles", Typeflag: tar.TypeSymlink, Linkname: "/tmp"}}, false},
		{"actor nested symlink", pb.ArtifactKind_ARTIFACT_KIND_ACTOR_STATE, []*tar.Header{{Name: "actors/bundles", Typeflag: tar.TypeDir}, {Name: "actors/bundles/main", Typeflag: tar.TypeSymlink, Linkname: "/tmp"}}, false},
		{"node credential path", pb.ArtifactKind_ARTIFACT_KIND_ACTOR_STATE, []*tar.Header{{Name: "credential-broker.sock", Typeflag: tar.TypeReg, Size: 1}}, false},
		{"write through image symlink", pb.ArtifactKind_ARTIFACT_KIND_IMAGE_LAYER, []*tar.Header{{Name: "fs", Typeflag: tar.TypeDir}, {Name: "fs/escape", Typeflag: tar.TypeSymlink, Linkname: "/tmp"}, {Name: "fs/escape/data", Typeflag: tar.TypeReg, Size: 1}}, false},
		{"absolute archive name", pb.ArtifactKind_ARTIFACT_KIND_IMAGE_LAYER, []*tar.Header{{Name: "/etc/passwd", Typeflag: tar.TypeReg, Size: 1}}, false},
		{"hardlink to out-of-subtree target", pb.ArtifactKind_ARTIFACT_KIND_ACTOR_STATE, []*tar.Header{{Name: "actors/bundles", Typeflag: tar.TypeDir}, {Name: "actors/bundles/link", Typeflag: tar.TypeLink, Linkname: "credential-broker.sock"}}, false},
		// The two trees of actor state: each accepts only its own roots, and no
		// entry may sit outside them, cross between them or repeat a prefix.
		{"snapshots restore state", pb.ArtifactKind_ARTIFACT_KIND_ACTOR_STATE, []*tar.Header{{Name: "snapshots/restore-state", Typeflag: tar.TypeDir}, {Name: "snapshots/restore-state/image", Typeflag: tar.TypeReg, Size: 1}}, true},
		{"snapshots checkpoint state", pb.ArtifactKind_ARTIFACT_KIND_ACTOR_STATE, []*tar.Header{{Name: "snapshots/checkpoint-state", Typeflag: tar.TypeDir}}, true},
		{"both trees", pb.ArtifactKind_ARTIFACT_KIND_ACTOR_STATE, []*tar.Header{{Name: "actors", Typeflag: tar.TypeDir}, {Name: "actors/pidfiles", Typeflag: tar.TypeDir}, {Name: "snapshots", Typeflag: tar.TypeDir}, {Name: "snapshots/restore-state", Typeflag: tar.TypeDir}}, true},
		{"unprefixed actor root", pb.ArtifactKind_ARTIFACT_KIND_ACTOR_STATE, []*tar.Header{{Name: "bundles", Typeflag: tar.TypeDir}}, false},
		{"snapshot root under actors", pb.ArtifactKind_ARTIFACT_KIND_ACTOR_STATE, []*tar.Header{{Name: "actors/restore-state", Typeflag: tar.TypeDir}}, false},
		{"actor root under snapshots", pb.ArtifactKind_ARTIFACT_KIND_ACTOR_STATE, []*tar.Header{{Name: "snapshots/bundles", Typeflag: tar.TypeDir}}, false},
		{"unlisted snapshots root", pb.ArtifactKind_ARTIFACT_KIND_ACTOR_STATE, []*tar.Header{{Name: "snapshots/local-checkpoint", Typeflag: tar.TypeDir}}, false},
		{"unknown tree", pb.ArtifactKind_ARTIFACT_KIND_ACTOR_STATE, []*tar.Header{{Name: "image-cache/layers", Typeflag: tar.TypeDir}}, false},
		{"traversal across trees", pb.ArtifactKind_ARTIFACT_KIND_ACTOR_STATE, []*tar.Header{{Name: "snapshots/../actors/bundles", Typeflag: tar.TypeDir}}, false},
		{"absolute tree entry", pb.ArtifactKind_ARTIFACT_KIND_ACTOR_STATE, []*tar.Header{{Name: "/snapshots/restore-state", Typeflag: tar.TypeDir}}, false},
		{"double tree prefix", pb.ArtifactKind_ARTIFACT_KIND_ACTOR_STATE, []*tar.Header{{Name: "snapshots/snapshots/restore-state", Typeflag: tar.TypeDir}}, false},
		{"tree entry not a directory", pb.ArtifactKind_ARTIFACT_KIND_ACTOR_STATE, []*tar.Header{{Name: "snapshots", Typeflag: tar.TypeReg, Size: 1}}, false},
		{"snapshots root not a directory", pb.ArtifactKind_ARTIFACT_KIND_ACTOR_STATE, []*tar.Header{{Name: "snapshots/restore-state", Typeflag: tar.TypeReg, Size: 1}}, false},
		{"hardlink across trees", pb.ArtifactKind_ARTIFACT_KIND_ACTOR_STATE, []*tar.Header{{Name: "actors/bundles", Typeflag: tar.TypeDir}, {Name: "actors/bundles/a", Typeflag: tar.TypeReg, Size: 1}, {Name: "snapshots/restore-state/b", Typeflag: tar.TypeLink, Linkname: "actors/bundles/a"}}, false},
		{"valid in-subtree hardlink", pb.ArtifactKind_ARTIFACT_KIND_IMAGE_LAYER, []*tar.Header{{Name: "fs", Typeflag: tar.TypeDir}, {Name: "fs/a", Typeflag: tar.TypeReg, Size: 1}, {Name: "fs/b", Typeflag: tar.TypeLink, Linkname: "fs/a"}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			headers := tc.headers
			if tc.kind == pb.ArtifactKind_ARTIFACT_KIND_ACTOR_STATE {
				// Actor state always carries both tree entries; add the ones a
				// case does not set itself, so each case fails only for its
				// own reason.
				headers = withActorStateTrees(headers)
			}
			err := extractArtifact(t.Context(), &pb.Artifact{Kind: tc.kind}, t.TempDir(), bytes.NewReader(archiveFixture(t, headers)))
			if (err == nil) != tc.ok {
				t.Fatalf("extract=%v, want accept=%v", err, tc.ok)
			}
		})
	}
}

func TestArtifactRejectsBytesAfterTarEOF(t *testing.T) {
	body := archiveFixture(t, withActorStateTrees([]*tar.Header{{Name: "actors/bundles", Typeflag: tar.TypeDir}}))
	for _, tc := range []struct {
		name   string
		suffix []byte
		ok     bool
	}{
		{"packer framing", nil, true},
		{"additional zero padding", make([]byte, 512), false},
		{"uninterpreted data", []byte("trailing bytes"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := append(append([]byte(nil), body...), tc.suffix...)
			err := extractArtifact(t.Context(), &pb.Artifact{Kind: pb.ArtifactKind_ARTIFACT_KIND_ACTOR_STATE}, t.TempDir(), bytes.NewReader(data))
			if (err == nil) != tc.ok {
				t.Fatalf("framing accept=%v want=%v: %v", err == nil, tc.ok, err)
			}
		})
	}
}

func TestCheckpointMembershipAndDigests(t *testing.T) {
	hash := sha256.Sum256([]byte("data"))
	good := []*pb.CheckpointChunk{{FileName: "image", Size: 4}, {Data: []byte("data")}, {Sha256: hex.EncodeToString(hash[:])}}
	for _, tc := range []struct {
		name     string
		chunks   []*pb.CheckpointChunk
		expected []string
		ok       bool
	}{
		{"complete", good, []string{"image"}, true},
		{"empty file set", nil, nil, true},
		{"omitted file", good, []string{"image", "memory"}, false},
		{"unexpected file", good, []string{"memory"}, false},
		{"truncated file", good[:2], []string{"image"}, false},
		{"wrong hash", []*pb.CheckpointChunk{good[0], good[1], {Sha256: strings.Repeat("0", 64)}}, []string{"image"}, false},
		{"duplicate file", append(append([]*pb.CheckpointChunk{}, good...), good...), []string{"image"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			index := 0
			recv := func() (*pb.CheckpointChunk, error) {
				if index == len(tc.chunks) {
					return nil, io.EOF
				}
				chunk := tc.chunks[index]
				index++
				return chunk, nil
			}
			err := receiveCheckpoint(recv, tc.expected, t.TempDir())
			if (err == nil) != tc.ok {
				t.Fatalf("receive=%v, want accept=%v", err, tc.ok)
			}
		})
	}
}
