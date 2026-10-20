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

// Package workerstaging transfers typed activation artifacts between atelet
// and workers with independent filesystems. Physical roots may differ while
// their runtime-visible canonical paths remain the same.
package workerstaging

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"syscall"

	"go.ate.dev/substrate/internal/nodepath"
	pb "go.ate.dev/substrate/internal/proto/workerstagingpb"
	"go.ate.dev/substrate/internal/resources"
)

const (
	ChunkBytes               = 1 << 20
	MaxArtifactBytes   int64 = 16 << 30
	MaxActivationBytes int64 = 64 << 30
	MaxArtifacts             = 256
	maxArchiveEntries        = 250000
)

var (
	digestPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)
	// assetNamePattern is the identifier of a RUNTIME artifact: the sandbox
	// asset NAME, one path element under the actor's own sandbox-assets dir
	// (actors/<uid>/sandbox-assets/<name>), the only place the worker runs a
	// sandbox binary from. A runtime asset is an entry in a per-actor dir, not
	// a shared, digest-keyed immutable, so its id carries no digest and is not
	// unique across actors.
	//
	// This is a STRICT allowlist, deliberately tighter than cmd/atelet's
	// plainAssetName (which admits any single local path component, including
	// uppercase, '_' and '.'): this is a trust boundary that writes an
	// executable subtree into a guest, so a lowercase DNS-label-ish token with
	// no separators is preferred. It is a single path component (no "/", no
	// "." or ".."), and it covers every sandbox asset name the producer emits
	// today (gvisor, runsc, kata-kernel, kata-image, cloud-hypervisor). A new
	// asset name that falls outside this pattern must widen it here (and only
	// here) rather than be smuggled through as a looser id.
	assetNamePattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,63}$`)
)

// An actor's state lives in two per-actor trees under the staging base, laid
// out as nodepath lays them out under BasePath: the actor tree
// (<base>/actors/<uid>) holds bundles, volumes, runtime state and the
// activation records, and the snapshots tree (<base>/snapshots/<uid>) holds
// the restore and checkpoint staging directories, so they can sit on their own
// disk. The ACTOR_STATE artifact carries both: each tar entry is prefixed with
// its tree name, and only the roots listed for that tree are accepted under it.
type stateTree struct {
	name  string
	roots []string
}

// actorTreeName and snapshotsTreeName are the names of the two per-actor trees
// relative to the staging base, taken from nodepath so both sides of the
// transfer resolve the same layout as the rest of the node.
func actorTreeName() string     { return filepath.Base(nodepath.ActorsDir) }
func snapshotsTreeName() string { return filepath.Base(nodepath.SnapshotsDir) }

// actorDir and snapshotsDir are an actor's two trees under base.
func actorDir(base, actorUID string) string {
	return filepath.Join(base, actorTreeName(), actorUID)
}

func snapshotsDir(base, actorUID string) string {
	return filepath.Join(base, snapshotsTreeName(), actorUID)
}

// actorStateTrees lists the trees ACTOR_STATE carries and the roots each may
// hold.
func actorStateTrees() []stateTree {
	return []stateTree{
		{name: actorTreeName(), roots: []string{"bundles", "durable-dir", "system-info", "runsc-state", "pidfiles"}},
		{name: snapshotsTreeName(), roots: []string{"restore-state", "checkpoint-state"}},
	}
}

// actorStatePath reports whether name is a tree directory, a root directory
// or an entry under a listed root of an ACTOR_STATE artifact, and how deep it
// is: 0 for the tree directory, 1 for a root, 2 below a root.
func actorStatePath(name string) (depth int, ok bool) {
	parts := strings.SplitN(filepath.ToSlash(name), "/", 3)
	for _, tree := range actorStateTrees() {
		if parts[0] != tree.name {
			continue
		}
		if len(parts) == 1 {
			return 0, true
		}
		for _, root := range tree.roots {
			if parts[1] == root {
				return len(parts) - 1, true
			}
		}
		return 0, false
	}
	return 0, false
}

func validateArtifact(artifact *pb.Artifact) error {
	if artifact == nil || !digestPattern.MatchString(artifact.GetSha256()) ||
		artifact.GetSize() <= 0 || artifact.GetSize() > MaxArtifactBytes {
		return fmt.Errorf("artifact needs a SHA256 and bounded nonzero size")
	}
	switch artifact.GetKind() {
	case pb.ArtifactKind_ARTIFACT_KIND_IMAGE_LAYER:
		if !digestPattern.MatchString(artifact.GetId()) {
			return fmt.Errorf("invalid image layer ID")
		}
	case pb.ArtifactKind_ARTIFACT_KIND_RUNTIME:
		if !assetNamePattern.MatchString(artifact.GetId()) {
			return fmt.Errorf("invalid runtime asset ID")
		}
	case pb.ArtifactKind_ARTIFACT_KIND_ACTOR_STATE:
		if !resources.IsValidResourceName(artifact.GetId()) {
			return fmt.Errorf("invalid activation ID")
		}
	default:
		return fmt.Errorf("unsupported artifact kind")
	}
	return nil
}

func artifactKey(artifact *pb.Artifact) string {
	return fmt.Sprintf("%d-%s", artifact.GetKind(), artifact.GetId())
}

// artifactDestination is the install subtree under the staging base for the
// kinds that still have a node-level destination: a node-wide content-addressed
// IMAGE_LAYER, and the actor's directory for ACTOR_STATE. A RUNTIME asset is
// per-actor and is staged through placeRuntime into
// pending/sandbox-assets and folded into the actor directory at commit, so it
// has no entry here; it is never passed to this function.
func artifactDestination(artifact *pb.Artifact, actorUID string) string {
	if artifact.GetKind() == pb.ArtifactKind_ARTIFACT_KIND_IMAGE_LAYER {
		return filepath.Join("image-cache", "layers", "sha256", artifact.GetId())
	}
	return filepath.Join(actorTreeName(), actorUID)
}

// archivePath permits only this kind's canonical subtree. Image finalization
// is deliberately repeated in the worker from whiteouts.json; host device
// nodes and the host's finalized marker are never transferred.
func archivePath(artifact *pb.Artifact, name string) bool {
	if !filepath.IsLocal(name) || name == "." || filepath.Clean(name) != name {
		return false
	}
	first := strings.SplitN(filepath.ToSlash(name), "/", 2)[0]
	switch artifact.GetKind() {
	case pb.ArtifactKind_ARTIFACT_KIND_IMAGE_LAYER:
		return first == "fs" || name == "whiteouts.json" || name == "size"
	case pb.ArtifactKind_ARTIFACT_KIND_RUNTIME:
		return first == artifact.GetId()
	case pb.ArtifactKind_ARTIFACT_KIND_ACTOR_STATE:
		_, ok := actorStatePath(name)
		return ok
	}
	return false
}

// packArtifact is local preparation, not a remote filesystem operation.
func packArtifact(ctx context.Context, base, actorUID string, artifact *pb.Artifact, output io.Writer) error {
	if !resources.IsValidResourceName(actorUID) {
		return fmt.Errorf("invalid actor UID")
	}
	// A source is one directory under base whose listed paths are packed, each
	// entry named with prefix (empty, or the tree name for ACTOR_STATE).
	type source struct {
		dir, prefix string
		paths       []string
		// tree marks an ACTOR_STATE tree: its directory entry is always
		// written, and a snapshots tree with nothing staged yet is written as
		// that empty entry alone.
		tree bool
	}
	var sources []source
	switch artifact.GetKind() {
	case pb.ArtifactKind_ARTIFACT_KIND_IMAGE_LAYER:
		sources = []source{{dir: artifactDestination(artifact, actorUID), paths: []string{"fs", "whiteouts.json", "size"}}}
	case pb.ArtifactKind_ARTIFACT_KIND_RUNTIME:
		// The runtime asset is packed from the actor's own per-actor
		// sandbox-assets dir, under the single asset-name subtree. archivePath keeps the tar entries confined to that subtree.
		sources = []source{{dir: filepath.Join(actorTreeName(), actorUID, "sandbox-assets"), paths: []string{artifact.GetId()}}}
	case pb.ArtifactKind_ARTIFACT_KIND_ACTOR_STATE:
		// Both trees are always present in the artifact. The actor tree must
		// exist on disk; the snapshots tree holds only restore and checkpoint
		// staging and may not exist yet, in which case it is sent empty.
		for _, tree := range actorStateTrees() {
			sources = append(sources, source{
				dir:    filepath.Join(tree.name, actorUID),
				prefix: tree.name,
				paths:  tree.roots,
				tree:   true,
			})
		}
	default:
		return fmt.Errorf("unsupported artifact kind")
	}
	writer := tar.NewWriter(output)
	links := map[[2]uint64]string{}
	for _, src := range sources {
		root, err := os.OpenRoot(filepath.Join(base, src.dir))
		missing := errors.Is(err, os.ErrNotExist)
		if err != nil && !(missing && src.tree && src.prefix == snapshotsTreeName()) {
			return err
		}
		if src.tree {
			if err := writer.WriteHeader(&tar.Header{Name: src.prefix + "/", Typeflag: tar.TypeDir, Mode: 0o700}); err != nil {
				if root != nil {
					root.Close()
				}
				return err
			}
		}
		if missing {
			continue
		}
		err = packTree(ctx, writer, root, src.prefix, src.paths, artifact, links)
		root.Close()
		if err != nil {
			return err
		}
	}
	return writer.Close()
}

// packTree writes the listed paths of root to writer, naming each entry with
// prefix joined to its path under root.
func packTree(ctx context.Context, writer *tar.Writer, root *os.Root, prefix string, paths []string, artifact *pb.Artifact, links map[[2]uint64]string) error {
	for _, start := range paths {
		if _, err := root.Lstat(start); errors.Is(err, os.ErrNotExist) {
			// Optional state roots, layer size metadata and old whiteout-free
			// layers may omit files. Runtime content and layer fs are required.
			if artifact.GetKind() == pb.ArtifactKind_ARTIFACT_KIND_RUNTIME || start == "fs" {
				return err
			}
			continue
		} else if err != nil {
			return err
		}
		err := fs.WalkDir(root.FS(), start, func(name string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			entryName := path.Join(prefix, filepath.ToSlash(name))
			if !archivePath(artifact, entryName) {
				return fmt.Errorf("unexpected artifact path %q", entryName)
			}
			info, err := entry.Info()
			if err != nil {
				return err
			}
			if info.Mode()&os.ModeCharDevice != 0 && artifact.GetKind() == pb.ArtifactKind_ARTIFACT_KIND_IMAGE_LAYER {
				stat, ok := info.Sys().(*syscall.Stat_t)
				if ok && stat.Rdev == 0 {
					return nil
				} // whiteouts.json reconstructs this in the guest.
			}
			if !info.Mode().IsRegular() && !info.IsDir() && info.Mode()&os.ModeSymlink == 0 {
				return fmt.Errorf("unsupported artifact entry %q (%v)", name, info.Mode())
			}
			link := ""
			if info.Mode()&os.ModeSymlink != 0 {
				link, err = root.Readlink(name)
				if err != nil {
					return err
				}
			}
			header, err := tar.FileInfoHeader(info, link)
			if err != nil {
				return err
			}
			header.Name = entryName
			if stat, ok := info.Sys().(*syscall.Stat_t); ok && info.Mode().IsRegular() && stat.Nlink > 1 &&
				artifact.GetKind() == pb.ArtifactKind_ARTIFACT_KIND_IMAGE_LAYER {
				key := [2]uint64{uint64(stat.Dev), stat.Ino}
				if target, exists := links[key]; exists {
					header.Typeflag, header.Linkname, header.Size = tar.TypeLink, target, 0
				} else {
					links[key] = entryName
				}
			}
			if err := writer.WriteHeader(header); err != nil {
				return err
			}
			if header.Typeflag != tar.TypeReg {
				return nil
			}
			file, err := root.Open(name)
			if err != nil {
				return err
			}
			_, copyErr := io.CopyN(writer, file, header.Size)
			closeErr := file.Close()
			if copyErr != nil {
				return copyErr
			}
			return closeErr
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func extractArtifact(ctx context.Context, artifact *pb.Artifact, directory string, input io.Reader) error {
	root, err := os.OpenRoot(directory)
	if err != nil {
		return err
	}
	defer root.Close()
	reader := tar.NewReader(input)
	dirs := map[string]os.FileMode{}
	seen := map[string]bool{}
	var bytes int64
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		name := strings.TrimSuffix(header.Name, "/")
		if !archivePath(artifact, name) || seen[name] || len(seen) >= maxArchiveEntries {
			return fmt.Errorf("invalid, duplicate or excessive artifact entry %q", header.Name)
		}
		seen[name] = true
		// Image-internal symlinks retain their container filesystem meaning.
		// Runtime assets and prepared control/config state contain real files.
		if artifact.GetKind() != pb.ArtifactKind_ARTIFACT_KIND_IMAGE_LAYER &&
			(header.Typeflag == tar.TypeSymlink || header.Typeflag == tar.TypeLink) {
			return fmt.Errorf("links are not permitted in runtime or prepared actor state")
		}
		if artifact.GetKind() == pb.ArtifactKind_ARTIFACT_KIND_ACTOR_STATE {
			// A tree or root entry of actor state must be a directory.
			if depth, _ := actorStatePath(name); depth < 2 && header.Typeflag != tar.TypeDir {
				return fmt.Errorf("artifact root %q must be a directory", name)
			}
		} else if !strings.Contains(name, "/") {
			mustBeDir := artifact.GetKind() == pb.ArtifactKind_ARTIFACT_KIND_IMAGE_LAYER && name == "fs"
			if mustBeDir && header.Typeflag != tar.TypeDir {
				return fmt.Errorf("artifact root %q must be a directory", name)
			}
			if artifact.GetKind() == pb.ArtifactKind_ARTIFACT_KIND_IMAGE_LAYER && name != "fs" && header.Typeflag != tar.TypeReg {
				return fmt.Errorf("layer metadata must be a regular file")
			}
		}
		if header.Size < 0 || header.Size > MaxArtifactBytes-bytes {
			return fmt.Errorf("artifact expanded size exceeds limit")
		}
		bytes += header.Size
		if err := root.MkdirAll(filepath.Dir(name), 0o700); err != nil {
			return err
		}
		mode := header.FileInfo().Mode() & (os.ModePerm | os.ModeSetuid | os.ModeSetgid | os.ModeSticky)
		switch header.Typeflag {
		case tar.TypeDir:
			if err := root.MkdirAll(name, mode|0o700); err != nil {
				return err
			}
			dirs[name] = mode
		case tar.TypeReg:
			file, err := root.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
			if err != nil {
				return err
			}
			_, copyErr := io.CopyN(file, reader, header.Size)
			if copyErr == nil {
				copyErr = file.Chmod(mode)
			}
			if copyErr == nil {
				copyErr = file.Sync()
			}
			closeErr := file.Close()
			if copyErr != nil {
				return copyErr
			}
			if closeErr != nil {
				return closeErr
			}
		case tar.TypeSymlink:
			if err := root.Symlink(header.Linkname, name); err != nil {
				return err
			}
		case tar.TypeLink:
			if !archivePath(artifact, header.Linkname) || !seen[header.Linkname] {
				return fmt.Errorf("invalid hardlink target")
			}
			if err := root.Link(header.Linkname, name); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported tar type %d", header.Typeflag)
		}
	}
	// The private packer writes exactly one tar stream with its standard end
	// markers. Additional padding or data after tar EOF is not part of this
	// format, even when the sender hashes that suffix into the artifact.
	var trailer [1]byte
	if n, err := io.ReadFull(input, trailer[:]); n != 0 || !errors.Is(err, io.EOF) {
		return fmt.Errorf("artifact has bytes after tar EOF")
	}
	names := make([]string, 0, len(dirs))
	for name := range dirs {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool { return len(names[i]) > len(names[j]) })
	for _, name := range names {
		if err := root.Chmod(name, dirs[name]); err != nil {
			return err
		}
	}
	if artifact.GetKind() == pb.ArtifactKind_ARTIFACT_KIND_IMAGE_LAYER && !seen["fs"] {
		return fmt.Errorf("layer has no filesystem root")
	}
	if artifact.GetKind() == pb.ArtifactKind_ARTIFACT_KIND_RUNTIME && !seen[artifact.GetId()] {
		return fmt.Errorf("runtime has no content")
	}
	if artifact.GetKind() == pb.ArtifactKind_ARTIFACT_KIND_ACTOR_STATE {
		// Actor state is complete only with both trees; an empty tree is
		// sent as its directory entry alone.
		for _, tree := range actorStateTrees() {
			if !seen[tree.name] {
				return fmt.Errorf("actor state has no %s tree", tree.name)
			}
		}
	}
	return nil
}
