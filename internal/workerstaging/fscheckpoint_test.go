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
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/google/uuid"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	pb "go.ate.dev/substrate/internal/proto/workerstagingpb"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// --- membership rule: both ways, plus the 128-name cap ---------------------
//
// validCheckpointFiles is the SINGLE rule the server interceptor, the server
// ExportCheckpoint and the client Export all share. It must be exactly
// upstream's resources.ValidateSnapshotFileNames (a plain name in the
// checkpoint directory OR its "fs" subdirectory) plus the 128-name cap. Every
// case here is pinned BOTH ways so a loosened or tightened rule fails.

func TestValidCheckpointFilesMembershipRule(t *testing.T) {
	for _, tc := range []struct {
		name  string
		files []string
		ok    bool
	}{
		// accept
		{"empty set", nil, true},
		{"plain top-level names", []string{"image", "memory", "pages.img"}, true},
		{"fs subdir name", []string{"fs/fscheckpoint.pb"}, true},
		{"top-level and fs mixed", []string{"checkpoint.img", "fs/pages.img", "fs/pages_meta.img"}, true},
		// rootfs/ became a valid single-level subdir with upstream #2673
		// (SNAPSHOT_FIDELITY_ROOTFS on gVisor): container rootfs overlays are
		// written as a separate "rootfs/" bundle beside the durable-dir "fs/"
		// bundle. Our rule delegates to upstream's ValidateSnapshotFileNames, so
		// it admits these exactly when upstream does.
		{"rootfs subdir name", []string{"rootfs/fscheckpoint.pb"}, true},
		{"a full ROOTFS-fidelity split set", []string{"checkpoint.img", "pages.img", "rootfs/fscheckpoint.pb", "rootfs/pages.img", "fs/fscheckpoint.pb", "fs/pages.img"}, true},
		{"exactly 128 names at cap", names(128), true},
		// reject
		{"empty name", []string{""}, false},
		{"dot", []string{"."}, false},
		{"dot dot", []string{".."}, false},
		{"absolute", []string{"/abs"}, false},
		{"arbitrary nested subdir", []string{"a/b/c"}, false},
		{"single nested subdir", []string{"a/b"}, false},
		{"escape through fs", []string{"fs/../x"}, false},
		{"deeper nesting under fs", []string{"fs/a/b"}, false},
		{"escape through rootfs", []string{"rootfs/../x"}, false},
		{"deeper nesting under rootfs", []string{"rootfs/a/b"}, false},
		{"sibling of rootfs rejected", []string{"rootfsx/y"}, false},
		{"duplicate", []string{"image", "image"}, false},
		{"129 names over cap", names(129), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := validCheckpointFiles(tc.files); got != tc.ok {
				t.Fatalf("validCheckpointFiles(%q) = %v, want %v", tc.files, got, tc.ok)
			}
		})
	}
}

// TestValidCheckpointFilesEqualsUpstreamBelowCap pins validCheckpointFiles to
// upstream's resources.ValidateSnapshotFileNames on a set of REPRESENTATIVE
// inputs at or below the cap (the function proves universal equality by
// delegating to the validator directly; this table samples the accept/reject
// boundary). A variant that reverts to the old per-name Base() rule (which
// rejected the valid "fs/x" name) fails here, and a variant that widens past
// upstream (accepting "a/b") also fails. Above the cap this rule is stricter by
// design, which the cap cases above cover.
func TestValidCheckpointFilesEqualsUpstreamBelowCap(t *testing.T) {
	for _, files := range [][]string{
		nil,
		{"image"},
		{"fs/pages.img"},
		{"rootfs/fscheckpoint.pb"},
		{"image", "rootfs/pages.img", "fs/pages.img"},
		{"image", "fs/pages.img"},
		{"fs/../x"},
		{"rootfs/../x"},
		{"rootfsx/y"},
		{"a/b"},
		{".."},
		{""},
		{"image", "image"},
		names(128),
	} {
		want := resources.ValidateSnapshotFileNames(files) == nil
		if got := validCheckpointFiles(files); got != want {
			t.Fatalf("validCheckpointFiles(%q) = %v, upstream ValidateSnapshotFileNames = %v", files, got, want)
		}
	}
}

func names(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = "f" + hex.EncodeToString([]byte{byte(i >> 8), byte(i)})
	}
	return out
}

// --- receiveCheckpoint: fs/ round-trip, parent-dir creation, rejects --------

// chunkStream turns a slice of chunks into the recv func receiveCheckpoint
// consumes, returning io.EOF once drained.
func chunkStream(chunks []*pb.CheckpointChunk) func() (*pb.CheckpointChunk, error) {
	index := 0
	return func() (*pb.CheckpointChunk, error) {
		if index == len(chunks) {
			return nil, io.EOF
		}
		chunk := chunks[index]
		index++
		return chunk, nil
	}
}

// fileChunks is the header+data+digest triple a well-formed file produces on
// the wire, matching server.ExportCheckpoint's framing.
func fileChunks(name string, data []byte) []*pb.CheckpointChunk {
	sum := sha256.Sum256(data)
	out := []*pb.CheckpointChunk{{FileName: name, Size: int64(len(data))}}
	if len(data) > 0 {
		out = append(out, &pb.CheckpointChunk{Data: data})
	}
	out = append(out, &pb.CheckpointChunk{Sha256: hex.EncodeToString(sum[:])})
	return out
}

func TestReceiveCheckpointFSSubdirRoundTrip(t *testing.T) {
	tree := t.TempDir()
	top := []byte("top-level checkpoint bytes")
	fsBytes := []byte("filesystem checkpoint bytes")
	var chunks []*pb.CheckpointChunk
	chunks = append(chunks, fileChunks("image", top)...)
	chunks = append(chunks, fileChunks("fs/pages.img", fsBytes)...)

	if err := receiveCheckpoint(chunkStream(chunks), []string{"image", "fs/pages.img"}, tree); err != nil {
		t.Fatalf("receiveCheckpoint with fs/ member: %v", err)
	}
	// The fs/ parent directory was created at 0o700, and both files landed
	// byte-exact where their names say.
	info, err := os.Stat(filepath.Join(tree, "fs"))
	if err != nil || !info.IsDir() {
		t.Fatalf("fs parent dir: %v (isDir=%v)", err, info != nil && info.IsDir())
	}
	if perm := info.Mode().Perm(); perm != 0o700 {
		t.Fatalf("fs parent dir mode = %o, want 0700", perm)
	}
	if got, _ := os.ReadFile(filepath.Join(tree, "image")); string(got) != string(top) {
		t.Fatalf("top-level file = %q, want %q", got, top)
	}
	if got, _ := os.ReadFile(filepath.Join(tree, "fs", "pages.img")); string(got) != string(fsBytes) {
		t.Fatalf("fs/ file = %q, want %q", got, fsBytes)
	}
}

// TestReceiveCheckpointFSSubdirSymlinkCannotEscape proves the os.Root the
// receiver opens on the transfer tree refuses to follow a pre-planted "fs"
// symlink out of the tree: a real runsc checkpoint never plants one, but if the
// tree were tampered with, MkdirAll/OpenFile through os.Root must fail rather
// than write outside it.
func TestReceiveCheckpointFSSubdirSymlinkCannotEscape(t *testing.T) {
	tree := t.TempDir()
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(tree, "fs")); err != nil {
		t.Fatal(err)
	}
	chunks := fileChunks("fs/pages.img", []byte("x"))
	if err := receiveCheckpoint(chunkStream(chunks), []string{"fs/pages.img"}, tree); err == nil {
		t.Fatal("receiveCheckpoint wrote through an escaping fs symlink")
	}
	if _, err := os.Lstat(filepath.Join(outside, "pages.img")); !os.IsNotExist(err) {
		t.Fatalf("a file escaped the transfer tree: %v", err)
	}
}

// --- full round-trip through the real gRPC server ---------------------------
//
// fsRuntime is a worker fixture that writes a caller-chosen checkpoint file set
// (including fs/ members) into checkpoint-state and reports it as SnapshotFiles,
// so the interceptor record, server ExportCheckpoint and client Export are all
// exercised on a set that contains an fs/ name.
type fsRuntime struct {
	ateworkerpb.UnimplementedWorkerServer
	base    string
	files   map[string][]byte // relative name -> bytes, names may be "fs/x"
	report  []string          // exact SnapshotFiles to report
	running bool
}

func (r *fsRuntime) RunWorkload(context.Context, *ateworkerpb.RunWorkloadRequest) (*ateworkerpb.RunWorkloadResponse, error) {
	r.running = true
	return &ateworkerpb.RunWorkloadResponse{}, nil
}

func (r *fsRuntime) CheckpointWorkload(_ context.Context, req *ateworkerpb.CheckpointWorkloadRequest) (*ateworkerpb.CheckpointWorkloadResponse, error) {
	dir := filepath.Join(snapshotsDir(r.base, req.GetActorUid()), "checkpoint-state")
	for name, data := range r.files {
		full := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
			return nil, err
		}
		if err := os.WriteFile(full, data, 0o600); err != nil {
			return nil, err
		}
	}
	r.running = false
	return &ateworkerpb.CheckpointWorkloadResponse{SnapshotFiles: r.report}, nil
}

func fsStagingConnection(t *testing.T, base, workerUID string, runtime *fsRuntime) (pb.WorkerStagingClient, ateworkerpb.WorkerClient, *Server) {
	t.Helper()
	runtime.base = base
	staging, err := NewServer(base, workerUID, func(string) bool { return runtime.running })
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
	return pb.NewWorkerStagingClient(conn), ateworkerpb.NewWorkerClient(conn), staging
}

// commitActorWithCheckpoint stages and commits a minimal activation so the
// actor has a committed record (the interceptor reads the generation from it),
// then returns the ref.
func commitActorWithCheckpoint(t *testing.T, base string, rpc pb.WorkerStagingClient, ref *pb.ActivationRef) {
	t.Helper()
	// A minimal committed activation: one ACTOR_STATE artifact with both trees
	// empty. The interceptor only needs the committed record's ref and
	// actor identity; it never reads actor-state content here.
	body := archiveFixture(t, withActorStateTrees(nil))
	sum := sha256.Sum256(body)
	artifact := &pb.Artifact{Kind: pb.ArtifactKind_ARTIFACT_KIND_ACTOR_STATE, Id: ref.GetGeneration(), Size: int64(len(body)), Sha256: hex.EncodeToString(sum[:])}
	manifest := &pb.Activation{Ref: ref, ActorAtespace: "team", ActorName: "counter", Artifacts: []*pb.Artifact{artifact}}
	if _, err := rpc.PrepareActivation(t.Context(), manifest); err != nil {
		t.Fatal(err)
	}
	stream, err := rpc.PutArtifact(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&pb.ArtifactChunk{Ref: ref, Kind: artifact.GetKind(), Id: artifact.GetId(), Data: body}); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.CloseAndRecv(); err != nil {
		t.Fatal(err)
	}
	if _, err := rpc.CommitActivation(t.Context(), ref); err != nil {
		t.Fatal(err)
	}
}

// TestInterceptorRecordsAndExportsFSCheckpoint is the call-site proof: a real
// CheckpointWorkload whose SnapshotFiles include an fs/ member passes the
// LifecycleInterceptor (so the record is written), and client.Export +
// server.ExportCheckpoint round-trip the whole set, top-level and fs/ files,
// into node custody byte-exact, across independent filesystems.
func TestInterceptorRecordsAndExportsFSCheckpoint(t *testing.T) {
	node, guest := t.TempDir(), t.TempDir()
	ref := &pb.ActivationRef{ActorUid: uuid.NewString(), WorkerUid: uuid.NewString(), Generation: uuid.NewString()}
	top := []byte("durable top-level checkpoint bytes")
	fsBytes := []byte("filesystem checkpoint bytes")
	runtime := &fsRuntime{
		files:  map[string][]byte{"image": top, "fs/pages.img": fsBytes},
		report: []string{"image", "fs/pages.img"},
	}
	rpc, lifecycle, _ := fsStagingConnection(t, guest, ref.GetWorkerUid(), runtime)
	client := NewClient(node, rpc)
	commitActorWithCheckpoint(t, guest, rpc, ref)

	checkpoint, err := lifecycle.CheckpointWorkload(t.Context(), &ateworkerpb.CheckpointWorkloadRequest{
		Atespace: "team", ActorName: "counter", ActorUid: ref.GetActorUid(),
	})
	if err != nil {
		t.Fatalf("CheckpointWorkload with fs/ member rejected by interceptor: %v", err)
	}

	// The interceptor wrote the ownership record with the exact fs/ membership.
	raw, err := os.ReadFile(filepath.Join(guest, "actors", ref.GetActorUid(), checkpointFile))
	if err != nil {
		t.Fatalf("checkpoint ownership record was not written: %v", err)
	}
	var record checkpointRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		t.Fatal(err)
	}
	gotFiles := append([]string(nil), record.Files...)
	sort.Strings(gotFiles)
	if record.Generation != ref.GetGeneration() || len(gotFiles) != 2 || gotFiles[0] != "fs/pages.img" || gotFiles[1] != "image" {
		t.Fatalf("recorded membership = %+v, want generation %q files [fs/pages.img image]", record, ref.GetGeneration())
	}

	// Export pulls the whole set into node custody and verifies each digest.
	custody := filepath.Join(node, "received-checkpoint")
	if err := client.Export(t.Context(), ref, checkpoint.GetSnapshotFiles(), custody); err != nil {
		t.Fatalf("Export of fs/ checkpoint set: %v", err)
	}
	// Prove node custody is independent of the guest filesystem.
	if err := os.RemoveAll(guest); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(custody, "image")); string(got) != string(top) {
		t.Fatalf("custody image = %q, want %q", got, top)
	}
	if got, _ := os.ReadFile(filepath.Join(custody, "fs", "pages.img")); string(got) != string(fsBytes) {
		t.Fatalf("custody fs/pages.img = %q, want %q", got, fsBytes)
	}
}

// TestInterceptorRejectsBadCheckpointMembership proves the interceptor rejects
// a runtime-reported set the rule forbids (a deeper-nested name), so the record
// is NOT written (the reject direction of the call-site).
func TestInterceptorRejectsBadCheckpointMembership(t *testing.T) {
	guest := t.TempDir()
	ref := &pb.ActivationRef{ActorUid: uuid.NewString(), WorkerUid: uuid.NewString(), Generation: uuid.NewString()}
	runtime := &fsRuntime{
		files:  map[string][]byte{"fs/a/b": []byte("x")},
		report: []string{"fs/a/b"}, // deeper than fs/<name>: must be rejected
	}
	rpc, lifecycle, _ := fsStagingConnection(t, guest, ref.GetWorkerUid(), runtime)
	commitActorWithCheckpoint(t, guest, rpc, ref)

	if _, err := lifecycle.CheckpointWorkload(t.Context(), &ateworkerpb.CheckpointWorkloadRequest{
		Atespace: "team", ActorName: "counter", ActorUid: ref.GetActorUid(),
	}); err == nil {
		t.Fatal("interceptor admitted a deeper-nested checkpoint name")
	}
	if _, err := os.Lstat(filepath.Join(guest, "actors", ref.GetActorUid(), checkpointFile)); !os.IsNotExist(err) {
		t.Fatalf("ownership record was written for a rejected membership: %v", err)
	}
}

// TestInterceptorRecordsAndExportsRootfsCheckpoint is the ROOTFS-fidelity
// call-site proof for upstream #2673 (SNAPSHOT_FIDELITY_ROOTFS on gVisor). A
// real CheckpointWorkload whose SnapshotFiles carry BOTH a rootfs/ member (the
// container rootfs overlay bundle) and an fs/ member (the durable-dir bundle)
// plus a top-level image passes the LifecycleInterceptor (so the record is
// written with the exact rootfs/+fs/+top membership), and client.Export +
// server.ExportCheckpoint round-trip the whole split set into node custody
// byte-exact across independent filesystems. This proves the network checkpoint
// path admits and carries rootfs/ files for real -- not only in the membership
// unit rule -- so a ROOTFS-fidelity snapshot taken on a NETWORK gVisor worker
// reaches node custody intact. The reject half is
// TestInterceptorRejectsBadCheckpointMembership (a deeper-nested name) plus the
// reject cases in TestValidCheckpointFilesMembershipRule (rootfs/../x,
// rootfs/a/b, rootfsx/y), so this positive cannot pass vacuously.
func TestInterceptorRecordsAndExportsRootfsCheckpoint(t *testing.T) {
	node, guest := t.TempDir(), t.TempDir()
	ref := &pb.ActivationRef{ActorUid: uuid.NewString(), WorkerUid: uuid.NewString(), Generation: uuid.NewString()}
	top := []byte("durable top-level checkpoint bytes")
	rootfsManifest := []byte("rootfs fscheckpoint manifest bytes")
	rootfsPages := []byte("rootfs overlay page bytes")
	fsBytes := []byte("durable-dir filesystem checkpoint bytes")
	runtime := &fsRuntime{
		files: map[string][]byte{
			"image":                  top,
			"rootfs/fscheckpoint.pb": rootfsManifest,
			"rootfs/pages.img":       rootfsPages,
			"fs/pages.img":           fsBytes,
		},
		report: []string{"image", "rootfs/fscheckpoint.pb", "rootfs/pages.img", "fs/pages.img"},
	}
	rpc, lifecycle, _ := fsStagingConnection(t, guest, ref.GetWorkerUid(), runtime)
	client := NewClient(node, rpc)
	commitActorWithCheckpoint(t, guest, rpc, ref)

	checkpoint, err := lifecycle.CheckpointWorkload(t.Context(), &ateworkerpb.CheckpointWorkloadRequest{
		Atespace: "team", ActorName: "counter", ActorUid: ref.GetActorUid(),
	})
	if err != nil {
		t.Fatalf("CheckpointWorkload with rootfs/ member rejected by interceptor: %v", err)
	}

	// The interceptor wrote the ownership record with the exact rootfs/+fs/+top
	// membership.
	raw, err := os.ReadFile(filepath.Join(guest, "actors", ref.GetActorUid(), checkpointFile))
	if err != nil {
		t.Fatalf("checkpoint ownership record was not written: %v", err)
	}
	var record checkpointRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		t.Fatal(err)
	}
	gotFiles := append([]string(nil), record.Files...)
	sort.Strings(gotFiles)
	want := []string{"fs/pages.img", "image", "rootfs/fscheckpoint.pb", "rootfs/pages.img"}
	if record.Generation != ref.GetGeneration() || len(gotFiles) != len(want) {
		t.Fatalf("recorded membership = %+v, want generation %q files %v", record, ref.GetGeneration(), want)
	}
	for i := range want {
		if gotFiles[i] != want[i] {
			t.Fatalf("recorded membership = %v, want %v", gotFiles, want)
		}
	}

	// Export pulls the whole split set into node custody and verifies each
	// digest; the rootfs/ and fs/ parent dirs are created generically.
	custody := filepath.Join(node, "received-checkpoint")
	if err := client.Export(t.Context(), ref, checkpoint.GetSnapshotFiles(), custody); err != nil {
		t.Fatalf("Export of rootfs/ checkpoint set: %v", err)
	}
	// Prove node custody is independent of the guest filesystem.
	if err := os.RemoveAll(guest); err != nil {
		t.Fatal(err)
	}
	for name, wantBytes := range map[string][]byte{
		"image":                  top,
		"rootfs/fscheckpoint.pb": rootfsManifest,
		"rootfs/pages.img":       rootfsPages,
		"fs/pages.img":           fsBytes,
	} {
		got, _ := os.ReadFile(filepath.Join(custody, filepath.FromSlash(name)))
		if string(got) != string(wantBytes) {
			t.Fatalf("custody %s = %q, want %q", name, got, wantBytes)
		}
	}
}
