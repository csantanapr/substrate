//go:build linux

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
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"slices"
	"strings"

	"github.com/opencontainers/runtime-spec/specs-go"
	"go.ate.dev/substrate/internal/ocispec"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/sizing"
)

type runsc struct {
	path     string
	actorUID string
	// actorDirs are the actor's directories, from the request.
	actorDirs *ateworkerpb.ActorDirs
	// size is the actor's declared limits.
	size sizing.SandboxSize
	// durableVolumes are the durable-dir volume names declared to the sandbox.
	durableVolumes []string
	// containers are the workload's containers, from the request.
	containers []*ateworkerpb.Container
	// flags holds the configured flags; must not be nil.
	flags *flagStore
	// enableCoreTags enables Linux core scheduling tags (--enable-core-tags).
	enableCoreTags bool
	// nested carries the actor's nested-container opt-in flags. The zero value
	// adds nothing, so a default actor's runsc argv is byte-for-byte unchanged.
	nested nestedRuntime
	// imageStoreSizeBytes caps the nested image-store tmpfs. Only read when
	// nested.enabled; 0 selects ocispec.DefaultNestedImageStoreBytes.
	imageStoreSizeBytes int64
}

// nestedRuntime is the set of runsc top-level flags an actor's nested-container
// opt-in adds. It is set from the workload spec in RunWorkload,
// CheckpointWorkload and RestoreWorkload, so the same flags accompany the
// sandbox on every lifecycle call.
type nestedRuntime struct {
	// enabled adds -net-raw, which the nested container runtime needs inside
	// gVisor.
	enabled bool
	// packetSocketWrite adds -allow-packet-socket-write (Docker v28+). It is
	// only ever set alongside enabled: admission rejects it on its own.
	packetSocketWrite bool
}

// nestedRuntimeFromSpec reads the opt-in off the worker spec. Returns the zero
// value (adds no flags) when the spec carries no nested config or it is
// disabled, keeping the default actor's argv byte-for-byte unchanged.
func nestedRuntimeFromSpec(spec *ateworkerpb.WorkloadSpec) nestedRuntime {
	n := spec.GetNestedContainers()
	if n == nil || !n.GetEnabled() {
		return nestedRuntime{}
	}
	return nestedRuntime{enabled: n.GetEnabled(), packetSocketWrite: n.GetPacketSocketWrite()}
}

// newRunRunsc builds the runsc for a RunWorkload request. Extracted so the
// nested-flag wiring (nested + imageStoreSizeBytes, read from THIS request's
// spec) is testable without standing up the full RPC: a change that dropped the
// nested fields here would fail TestNewRunsc_*.
func newRunRunsc(flags *flagStore, req *ateworkerpb.RunWorkloadRequest) *runsc {
	return &runsc{
		path:                req.GetRunscPath(),
		actorUID:            req.GetActorUid(),
		actorDirs:           req.GetActorDirs(),
		size:                sizing.FromLimits(req.GetResources().GetLimits()),
		durableVolumes:      durableVolumeNames(req.GetSpec()),
		containers:          req.GetSpec().GetContainers(),
		flags:               flags,
		nested:              nestedRuntimeFromSpec(req.GetSpec()),
		imageStoreSizeBytes: req.GetSpec().GetNestedContainers().GetImageStoreSizeBytes(),
	}
}

// newCheckpointRunsc builds the runsc for a CheckpointWorkload request. Carries
// the nested flags (they must match the create flags) but no sizing or
// image-store size: checkpoint writes no OCI spec.
func newCheckpointRunsc(flags *flagStore, req *ateworkerpb.CheckpointWorkloadRequest) *runsc {
	return &runsc{
		path:      req.GetRunscPath(),
		actorUID:  req.GetActorUid(),
		actorDirs: req.GetActorDirs(),
		flags:     flags,
		nested:    nestedRuntimeFromSpec(req.GetSpec()),
	}
}

// newRestoreRunsc builds the runsc for a RestoreWorkload request, with the same
// nested fields as create so the restored sandbox matches the capture.
func newRestoreRunsc(flags *flagStore, req *ateworkerpb.RestoreWorkloadRequest) *runsc {
	return &runsc{
		path:                req.GetRunscPath(),
		actorUID:            req.GetActorUid(),
		actorDirs:           req.GetActorDirs(),
		size:                sizing.FromLimits(req.GetResources().GetLimits()),
		durableVolumes:      durableVolumeNames(req.GetSpec()),
		containers:          req.GetSpec().GetContainers(),
		flags:               flags,
		nested:              nestedRuntimeFromSpec(req.GetSpec()),
		imageStoreSizeBytes: req.GetSpec().GetNestedContainers().GetImageStoreSizeBytes(),
	}
}

// topLevelFlags returns the runsc top-level (global) flags the nested-container
// opt-in adds, in a fixed order. The zero value returns nil, so a default actor
// gets none of them. packetSocketWrite is only emitted alongside -net-raw, so
// its admission dependency (it requires enabled) cannot be violated here.
func (n nestedRuntime) topLevelFlags() []string {
	if !n.enabled {
		return nil
	}
	flags := []string{"-net-raw"}
	if n.packetSocketWrite {
		flags = append(flags, "-allow-packet-socket-write")
	}
	return flags
}

// nestedFlagCommands are the runsc subcommands that configure the sandbox and
// so must carry the nested top-level flags: create and start bring it up, and
// gVisor restores a memory image only under the same top-level flags it was
// created with, so checkpoint and restore must match. Other
// subcommands (delete, state, list, kill, wait) rightly carry none: adding a
// flag to one call site but not another would make restore fail.
var nestedFlagCommands = map[string]bool{
	"create":     true,
	"start":      true,
	"checkpoint": true,
	"restore":    true,
}

// buildArgs constructs the runsc argument vector for command, appending
// containerName at the end if non-empty:
//
//	<combined globalFlags> <nested top-level flags> -root <stateDir> <command> <subcommandFlags> <extraArgs> [<containerName>]
//
// The nested top-level flags (empty for a default actor) are added among the
// other global flags, for the four lifecycle commands only, through this one
// builder so the four cannot diverge.
func (r *runsc) buildArgs(command, containerName string, extraArgs ...string) []string {
	cfg := r.flags.get()
	globalFlags := cfg.globalFlags(command)
	subcommandFlags := cfg.subcommandFlags(command)
	var nestedFlags []string
	if nestedFlagCommands[command] {
		nestedFlags = r.nested.topLevelFlags()
	}

	args := make([]string, 0,
		len(globalFlags)+1+len(nestedFlags)+2+1+len(subcommandFlags)+len(extraArgs)+1)
	args = append(args, globalFlags...)
	if r.enableCoreTags {
		args = append(args, "--enable-core-tags")
	}
	args = append(args, nestedFlags...)
	args = append(args, "-root", runscStateDir(r.actorDirs))
	args = append(args, command)
	args = append(args, subcommandFlags...)
	args = append(args, extraArgs...)
	if containerName != "" {
		args = append(args, containerName)
	}
	return args
}

// durableVolumeNames returns the sorted, deduplicated durable-dir volume names
// mounted by workload containers.
func durableVolumeNames(spec *ateworkerpb.WorkloadSpec) []string {
	var names []string
	for _, c := range spec.GetContainers() {
		for _, m := range c.GetDurableDirVolumeMounts() {
			names = append(names, m.GetName())
		}
	}
	slices.Sort(names)
	return slices.Compact(names)
}

// noRootContainerArgs returns the flag that tells runsc the sandbox has no
// root container. runsc refuses the flag for any other container.
func noRootContainerArgs(containerName string) []string {
	if containerName == ocispec.SandboxID {
		return []string{"-no-root-container"}
	}
	return nil
}

// writeSpec builds the container's OCI spec, shapes it for gVisor, and saves it
// into the container's bundle.
func (r *runsc) writeSpec(containerName string) error {
	bundlePath := ociBundlePath(r.actorDirs, containerName)
	var spec *specs.Spec
	if containerName == ocispec.SandboxID {
		// atelet makes only the application containers' bundles.
		if err := os.MkdirAll(bundlePath, 0o700); err != nil {
			return fmt.Errorf("while creating bundle %q: %w", bundlePath, err)
		}
		spec = ocispec.BuildSandbox(r.actorUID)
	} else {
		i := slices.IndexFunc(r.containers, func(c *ateworkerpb.Container) bool { return c.GetName() == containerName })
		if i < 0 {
			return fmt.Errorf("container %q is not in the workload spec", containerName)
		}
		spec = ocispec.Build(ocispec.Options{ActorUID: r.actorUID, ActorDirs: r.actorDirs, Container: r.containers[i]})
	}
	// A nested-container actor's application containers get a size-capped tmpfs
	// at the nested image store. Never the sandbox root container (it runs no
	// nested runtime); ShapeGVisor also guards that. nil for a default actor, so
	// its spec is byte-for-byte unchanged.
	var nestedStore *ocispec.NestedImageStore
	if r.nested.enabled && containerName != ocispec.SandboxID {
		nestedStore = &ocispec.NestedImageStore{SizeBytes: r.imageStoreSizeBytes}
	}
	ocispec.ShapeGVisor(spec, ocispec.GVisorOptions{
		// resolv.conf is written into the rootfs by setupAppRootfs, so no
		// bind source is passed; NestedImageStore is independent of it.
		ActorUID:         r.actorUID,
		ActorDirs:        r.actorDirs,
		ContainerName:    containerName,
		DurableVolumes:   r.durableVolumes,
		Size:             r.size,
		NestedImageStore: nestedStore,
	})
	return ocispec.Save(bundlePath, spec)
}

// createArgs builds the argv for `runsc create <container>`. Factored out so
// the argument construction can be unit-tested without executing runsc.
func (r *runsc) createArgs(containerName string, additionalArgs []string) []string {
	args := append(noRootContainerArgs(containerName),
		"-bundle", ociBundlePath(r.actorDirs, containerName),
		"-pid-file", pidFilePath(r.actorDirs, containerName),
	)
	args = append(args, additionalArgs...)
	return r.buildArgs("create", containerName, args...)
}

func (r *runsc) cmdCreate(ctx context.Context, out io.Writer, containerName string, additionalArgs []string) error {
	slog.InfoContext(ctx, "About to run runsc create", slog.String("container", containerName))

	if err := r.writeSpec(containerName); err != nil {
		return fmt.Errorf("while writing the OCI spec for %q: %w", containerName, err)
	}

	cmd := exec.CommandContext(ctx, r.path, r.createArgs(containerName, additionalArgs)...)
	cmd.Stdout = out
	cmd.Stderr = out

	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("while running `runsc create`: %w", err)
	}

	return nil
}

func (r *runsc) cmdStart(ctx context.Context, out io.Writer, containerName string) error {
	slog.InfoContext(ctx, "About to run runsc start", slog.String("container", containerName))

	cmd := exec.CommandContext(ctx, r.path, r.buildArgs("start", containerName)...)
	cmd.Stdout = out
	cmd.Stderr = out

	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("while running `runsc start`: %w", err)
	}

	return nil
}

// checkpointArgs builds the argv for `runsc checkpoint <container>`. Factored
// out so the argument construction can be unit-tested without executing runsc.
func (r *runsc) checkpointArgs(containerName, checkpointPath string, fsCheckpointPaths []string) []string {
	extra := make([]string, 0, 2+2*len(fsCheckpointPaths))
	extra = append(extra, "-image-path", checkpointPath)
	for _, p := range fsCheckpointPaths {
		extra = append(extra, "-fs-checkpoint-paths", p)
	}
	return r.buildArgs("checkpoint", containerName, extra...)
}

func (r *runsc) cmdCheckpoint(ctx context.Context, containerName, checkpointPath string, fsCheckpointPaths []string) error {
	slog.InfoContext(ctx, "About to run runsc checkpoint", slog.String("container", containerName))

	cmd := exec.CommandContext(ctx, r.path, r.checkpointArgs(containerName, checkpointPath, fsCheckpointPaths)...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("while running `runsc checkpoint`: %w", err)
	}
	return nil
}

// fsCheckpointArgs builds the argv for `runsc fscheckpoint <container>`.
// Factored out so the argument construction can be unit-tested without
// executing runsc.
func (r *runsc) fsCheckpointArgs(containerName, checkpointPath string, fsCheckpointPaths []string) []string {
	extra := make([]string, 0, 2+2*len(fsCheckpointPaths))
	extra = append(extra, "-image-path", checkpointPath)
	for _, ddv := range fsCheckpointPaths {
		extra = append(extra, "-path", ddv)
	}
	return r.buildArgs("fscheckpoint", containerName, extra...)
}

func (r *runsc) cmdFsCheckpoint(ctx context.Context, containerName, checkpointPath string, fsCheckpointPaths []string) error {
	slog.InfoContext(ctx, "About to run runsc fscheckpoint", slog.String("container", containerName))

	cmd := exec.CommandContext(ctx, r.path, r.fsCheckpointArgs(containerName, checkpointPath, fsCheckpointPaths)...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("while running `runsc fscheckpoint`: %w", err)
	}
	return nil
}

// restoreArgs builds the argv for `runsc restore <container>`. Factored out so
// the argument construction can be unit-tested without executing runsc.
func (r *runsc) restoreArgs(containerName, checkpointPath string) []string {
	args := append(noRootContainerArgs(containerName),
		"-bundle", ociBundlePath(r.actorDirs, containerName),
		"-image-path", checkpointPath,
		"-pid-file", pidFilePath(r.actorDirs, containerName),
	)
	return r.buildArgs("restore", containerName, args...)
}

// We take a checkpoint only of the sandbox, but we need to call restore on each
// container, using the same checkpoint.
func (r *runsc) cmdRestore(ctx context.Context, out io.Writer, containerName, checkpointPath string) error {
	slog.InfoContext(ctx, "About to run runsc restore", slog.String("container", containerName))

	if err := r.writeSpec(containerName); err != nil {
		return fmt.Errorf("while writing the OCI spec for %q: %w", containerName, err)
	}

	cmd := exec.CommandContext(ctx, r.path, r.restoreArgs(containerName, checkpointPath)...)
	cmd.Stdout = out
	cmd.Stderr = out
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("while running `runsc restore`: %w", err)
	}
	return nil
}

func (r *runsc) cmdDelete(ctx context.Context, containerName string) error {
	cmd := exec.CommandContext(
		ctx,
		r.path,
		r.buildArgs("delete", containerName)...,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("while running `runsc delete`: %w", err)
	}
	return nil
}

func (r *runsc) cmdState(ctx context.Context, containerName string) error {
	cmd := exec.CommandContext(
		ctx,
		r.path,
		r.buildArgs("state", containerName)...,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("while running `runsc state`: %w", err)
	}
	return nil
}

// cmdList returns the container IDs runsc has a record of.
func (r *runsc) cmdList(ctx context.Context) ([]string, error) {
	cmd := exec.CommandContext(
		ctx,
		r.path,
		r.buildArgs("list", "")...,
	)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("while running `runsc list`: %w", err)
	}
	return strings.Fields(out.String()), nil
}

// cmdKill sends signal to the given container's process(es) inside the gVisor
// sandbox. Used during graceful shutdown to propagate SIGTERM to the actor.
func (r *runsc) cmdKill(ctx context.Context, containerName, signal string) error {
	slog.InfoContext(ctx, "About to run runsc kill", slog.String("container", containerName), slog.String("signal", signal))

	cmd := exec.CommandContext(ctx, r.path, r.buildArgs("kill", "", containerName, signal)...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("while running `runsc kill`: %w", err)
	}
	return nil
}

// runWait runs `runsc wait [extraArgs...] <container>` and blocks until the
// waited-on condition completes.
func (r *runsc) runWait(ctx context.Context, containerName string, extraArgs ...string) error {
	cmd := exec.CommandContext(ctx, r.path, r.buildArgs("wait", containerName, extraArgs...)...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("while running `runsc wait`: %w", err)
	}
	return nil
}

// cmdWait blocks until the given container's process exits. Used during
// graceful shutdown to confirm the actor has stopped before ateworker exits.
func (r *runsc) cmdWait(ctx context.Context, containerName string) error {
	slog.InfoContext(ctx, "About to run runsc wait", slog.String("container", containerName))
	return r.runWait(ctx, containerName)
}

// cmdWaitRestore blocks until background page loading from `runsc restore -background` completes.
func (r *runsc) cmdWaitRestore(ctx context.Context, containerName string) error {
	return r.runWait(ctx, containerName, "-restore")
}

// cmdWaitFSRestore blocks until background filesystem page loading from
// `--fs-restore-image-path` completes for the given container.
func (r *runsc) cmdWaitFSRestore(ctx context.Context, containerName string) error {
	return r.runWait(ctx, containerName, "-fsrestore")
}
