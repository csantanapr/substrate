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

// Package register registers a worker process with the control plane through
// the node-local atelet: it acquires an epoch, registers once with that epoch,
// its pod IPs and its compute capacity, then reports the sandbox runtimes of
// its pool. It also provides file-watching helpers shared by worker binaries.
package register

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/ateletdial"
	"go.ate.dev/substrate/internal/hardware"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/sandboxruntimes"
	"go.ate.dev/substrate/internal/workercapacity"
	"go.ate.dev/substrate/internal/workertransport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	reportTimeout        = 10 * time.Second
	initialReportBackoff = 500 * time.Millisecond
	maxReportBackoff     = 30 * time.Second
	resyncInterval       = 5 * time.Minute
)

// Epoch is the epoch this worker process registered with. A worker process
// takes no workloads until it has registered, and reports this epoch with each
// one it starts, so the control plane can tell which process ran it.
type Epoch struct {
	v atomic.Int64
}

// Load returns the registered epoch, or 0 before registration.
func (e *Epoch) Load() int64 {
	return e.v.Load()
}

// Store records the epoch this worker process registered with.
func (e *Epoch) Store(epoch int64) {
	e.v.Store(epoch)
}

// Registered is a readiness check that fails until this worker process has
// registered. The pod is then not Ready, so the Worker takes no placements
// while a restarted worker process has yet to register.
func (e *Epoch) Registered(context.Context) error {
	if e.Load() == 0 {
		return errors.New("worker has not registered yet")
	}
	return nil
}

// Require returns the registered epoch for a workload about to start, or
// Unavailable before registration, so the caller retries once it has.
func (e *Epoch) Require() (int64, error) {
	if epoch := e.Load(); epoch != 0 {
		return epoch, nil
	}
	return 0, apierror.Unavailable("worker has not registered yet: not accepting workloads")
}

// FromFiles combines the downward API compute limits with the worker's own
// actor limit.
//
// A limit that is missing or unparseable is reported as zero, which the control
// plane reads as none: better to place nothing on a worker that cannot say what
// it has than to invent a number for it.
//
// TODO: Watch the projected files and report changes. For now we do not support
// in-place Pod vertical scaling (IPPR); capacity is read once at startup.
// NOTE: Please do not implement this yet. IPPR needs more general consideration.
//
// memoryReserved is how much of the memory limit the worker keeps for itself
// rather than offering to actors.
func FromFiles(actors int, memoryReserved int64) (*ateletpb.WorkerResources, error) {
	return fromDir(workercapacity.MountPath, actors, memoryReserved)
}

func fromDir(dir string, actors int, memoryReserved int64) (*ateletpb.WorkerResources, error) {
	memory := readLimit(filepath.Join(dir, workercapacity.MemoryLimitFile))
	if memory > 0 {
		if memory <= memoryReserved {
			return nil, fmt.Errorf("pod memory limit %d bytes is no more than the %d the worker keeps for itself, raise the pool's limits.memory", memory, memoryReserved)
		}
		memory -= memoryReserved
	}
	return &ateletpb.WorkerResources{
		CpuMillicores: readLimit(filepath.Join(dir, workercapacity.CPULimitFile)),
		MemoryBytes:   memory,
		Actors:        int32(actors),
	}, nil
}

// ParsePodIPs splits the comma-separated pod IPs the downward API exposes for
// status.podIPs.
func ParsePodIPs(value string) []string {
	var ips []string
	for ip := range strings.SplitSeq(value, ",") {
		if ip = strings.TrimSpace(ip); ip != "" {
			ips = append(ips, ip)
		}
	}
	return ips
}

// buildCompat converts the compatibility attributes atelet wrote for one
// SandboxConfig version into a VersionedSandboxCompat.
func buildCompat(attrs sandboxruntimes.Attributes) *ateletpb.VersionedSandboxCompat {
	entries := make([]*ateletpb.AttributeEntry, 0, len(attrs))
	for _, k := range slices.Sorted(maps.Keys(attrs)) {
		entries = append(entries, &ateletpb.AttributeEntry{
			Key:   k,
			Value: attrs[k],
		})
	}
	return &ateletpb.VersionedSandboxCompat{
		SchemaVersion: hardware.SchemaVersionV1,
		Attributes:    entries,
	}
}

// buildRuntime converts one runtime of the sandbox runtimes file into a
// WorkerSandboxRuntime.
func buildRuntime(rt sandboxruntimes.Runtime) *ateletpb.WorkerSandboxRuntime {
	return &ateletpb.WorkerSandboxRuntime{Version: buildCompat(rt.Version)}
}

// buildRuntimesRequest builds the SetWorkerSandboxRuntimesRequest for one
// class of the sandbox runtimes file, the one this worker implements, for the
// worker process registered with epoch.
func buildRuntimesRequest(epoch int64, class *sandboxruntimes.Class) *ateletpb.SetWorkerSandboxRuntimesRequest {
	c := &ateletpb.WorkerSandboxClass{
		SandboxClass:   class.SandboxClass,
		DefaultRuntime: buildRuntime(class.DefaultRuntime),
	}
	for _, rt := range class.RestorableRuntimes {
		c.RestorableRuntimes = append(c.RestorableRuntimes, buildRuntime(rt))
	}
	return &ateletpb.SetWorkerSandboxRuntimesRequest{
		Epoch:          epoch,
		SandboxClasses: []*ateletpb.WorkerSandboxClass{c},
	}
}

// setActorOverhead reports a copy of overhead on every runtime in req. The
// restorable runtimes run under this worker too, so they cost it the same.
func setActorOverhead(req *ateletpb.SetWorkerSandboxRuntimesRequest, overhead *ateletpb.RuntimeOverhead) {
	if overhead == nil {
		return
	}
	for _, c := range req.GetSandboxClasses() {
		c.GetDefaultRuntime().ActorOverhead = proto.CloneOf(overhead)
		for _, rt := range c.GetRestorableRuntimes() {
			rt.ActorOverhead = proto.CloneOf(overhead)
		}
	}
}

// classOf returns f's entry for class, or nil when the file has none.
func classOf(f *sandboxruntimes.File, class string) *sandboxruntimes.Class {
	for i := range f.SandboxClasses {
		if f.SandboxClasses[i].SandboxClass == class {
			return &f.SandboxClasses[i]
		}
	}
	return nil
}

func readLimit(path string) int64 {
	contents, err := os.ReadFile(path)
	if err != nil {
		slog.Warn("Ignoring unreadable capacity limit", slog.String("path", path), slog.Any("err", err))
		return 0
	}
	raw := strings.TrimSpace(string(contents))
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value < 0 {
		slog.Warn("Ignoring unusable capacity limit", slog.String("path", path), slog.String("value", raw))
		return 0
	}
	return value
}

// ReportConfig configures how a worker reaches the node-local atelet and what
// it registers with.
type ReportConfig struct {
	// SocketPath is the node-local atelet WorkerSupport Unix socket, used in
	// the default (colocated) mode where the worker shares the node's
	// filesystem. Ignored when Address is set.
	SocketPath string
	// Address is the atelet-workers Service host:port for the network
	// mode, where the worker runs in a separate guest Pod and cannot reach the
	// Unix socket. Empty (the default) keeps the Unix-socket path, so a
	// colocated worker's dial is unchanged.
	Address              string
	CredentialBundlePath string
	TrustBundlePath      string
	// AteletSPIFFEID is the identity the node-local atelet must present. It
	// names atelet's namespace, not this worker's, so it is configured rather
	// than derived from the downward API.
	AteletSPIFFEID string
	// Actors is how many actors this worker will host at once.
	Actors int
	// PodIPs are the worker pod's IPs, from the downward API.
	PodIPs []string
	// MemoryReservedBytes is how much of its memory limit the worker keeps for
	// itself rather than reporting as capacity.
	MemoryReservedBytes int64
	// ActorOverhead is what each actor costs the worker on top of its own
	// limits, reported on every runtime. Nil is none.
	ActorOverhead *ateletpb.RuntimeOverhead
	// SandboxClass is the class this worker implements ("gvisor", "microvm").
	SandboxClass string
	// RuntimesFile is the sandbox runtimes file atelet keeps for this worker
	// (nodepath.WorkerSandboxRuntimesPath).
	RuntimesFile string
}

// supportClient is the part of atelet's WorkerSupport that registration uses.
type supportClient interface {
	AcquireWorkerEpoch(ctx context.Context) (int64, error)
	RegisterWorker(ctx context.Context, req *ateletpb.RegisterWorkerRequest) error
	SetWorkerSandboxRuntimes(ctx context.Context, req *ateletpb.SetWorkerSandboxRuntimesRequest) error
}

// Report registers this worker process with the node-local atelet, stores the
// epoch it registered with in epoch, then keeps its sandbox runtimes reported
// until ctx ends: it reads its sandbox runtimes file at startup and again
// whenever atelet rewrites it.
//
// It returns early on a configuration error, which no retry would fix, and
// when the control plane refuses this process's epoch, which means another
// worker process has registered since: this one must stop serving.
//
// Retrying is what makes registration durable: atelet only accepts once the
// control plane has recorded it, and the Worker record may not exist yet when
// a worker first comes up. Nothing else reports this, so giving up would leave
// the Worker holding no capacity and hosting nothing.
func Report(ctx context.Context, cfg ReportConfig, epoch *Epoch) error {
	if cfg.SandboxClass == "" {
		// The control plane refuses a runtime without a class.
		return errors.New("register worker: SandboxClass is required")
	}
	if cfg.RuntimesFile == "" {
		return errors.New("register worker: RuntimesFile is required")
	}
	if len(cfg.PodIPs) == 0 {
		return errors.New("register worker: PodIPs is required")
	}
	tlsConfig, err := ateletdial.TLSConfig(cfg.CredentialBundlePath, cfg.TrustBundlePath, cfg.AteletSPIFFEID)
	if err != nil {
		return fmt.Errorf("register worker: %w", err)
	}
	capacity, err := FromFiles(cfg.Actors, cfg.MemoryReservedBytes)
	if err != nil {
		return fmt.Errorf("register worker: %w", err)
	}
	client := &dialingClient{socketPath: cfg.SocketPath, address: cfg.Address, tlsConfig: tlsConfig}

	// Where the runtimes come from differs by mode. A colocated worker reads
	// them from its on-node sandbox runtimes file, which atelet writes and
	// rewrites, so it reads the file and watches it for changes. A network
	// worker (Address set) shares no filesystem with atelet, so it fetches the
	// runtimes over the GetSandboxRuntimes RPC and has no file to watch; the
	// reconciler's periodic resync picks up a later change. Both feed the same
	// reconciler and build the same requests.
	if cfg.Address != "" {
		read := func() (*sandboxruntimes.File, error) {
			return fetchRuntimes(ctx, cfg.SocketPath, cfg.Address, tlsConfig)
		}
		// nil channel: the reconciler falls back to resyncInterval polling,
		// which is how a network worker notices a runtimes change.
		return runWith(ctx, cfg, capacity, client, epoch, read, nil)
	}
	changes := WatchFilePath(ctx, cfg.RuntimesFile)
	return run(ctx, cfg, capacity, client, epoch, changes)
}

// run registers, then reports the runtimes in the sandbox runtimes file until
// ctx ends or the epoch is refused.
func run(ctx context.Context, cfg ReportConfig, capacity *ateletpb.WorkerResources, client supportClient, epoch *Epoch, changes <-chan struct{}) error {
	read := func() (*sandboxruntimes.File, error) { return sandboxruntimes.Read(cfg.RuntimesFile) }
	return runWith(ctx, cfg, capacity, client, epoch, read, changes)
}

// runWith is run with the runtimes read by read.
func runWith(ctx context.Context, cfg ReportConfig, capacity *ateletpb.WorkerResources, client supportClient, epoch *Epoch, read func() (*sandboxruntimes.File, error), changes <-chan struct{}) error {
	registered, err := registerOnce(ctx, client, &ateletpb.RegisterWorkerRequest{
		Ips:      cfg.PodIPs,
		Capacity: capacity,
	}, initialReportBackoff, maxReportBackoff, time.After)
	if err != nil {
		return err
	}
	epoch.Store(registered)
	r := newReconciler(
		cfg,
		registered,
		read,
		client.SetWorkerSandboxRuntimes,
	)
	return r.run(ctx, changes)
}

// registerOnce acquires an epoch and registers this worker process with it,
// retrying with backoff until one registration is accepted, and returns the
// epoch. req carries the IPs and capacity; its epoch is set here.
//
// A registration that times out may still have landed, so it is resent with the
// same epoch, which the control plane accepts again without a write. A refusal
// of the epoch is returned, as is an invalid request.
func registerOnce(ctx context.Context, client supportClient, req *ateletpb.RegisterWorkerRequest, initialBackoff, maxBackoff time.Duration, after func(time.Duration) <-chan time.Time) (int64, error) {
	backoff := initialBackoff
	wait := func(err error, msg string) error {
		slog.WarnContext(ctx, msg, slog.Any("err", err))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-after(backoff):
		}
		backoff = min(backoff*2, maxBackoff)
		return nil
	}

	for req.Epoch == 0 {
		epoch, err := client.AcquireWorkerEpoch(ctx)
		if err == nil {
			req.Epoch = epoch
			break
		}
		if status.Code(err) == codes.InvalidArgument {
			return 0, fmt.Errorf("register worker: acquiring an epoch: %w", err)
		}
		if err := wait(err, "Could not acquire a worker epoch; will retry"); err != nil {
			return 0, err
		}
	}
	for {
		err := client.RegisterWorker(ctx, req)
		if err == nil {
			slog.InfoContext(ctx, "Registered worker",
				slog.Int64("epoch", req.GetEpoch()),
				slog.Any("ips", req.GetIps()),
				slog.Any("capacity", req.GetCapacity()))
			return req.GetEpoch(), nil
		}
		switch status.Code(err) {
		case codes.InvalidArgument:
			return 0, fmt.Errorf("register worker: %w", err)
		case codes.FailedPrecondition:
			return 0, fmt.Errorf("register worker: epoch %d refused: %w", req.GetEpoch(), err)
		}
		if err := wait(err, "Could not register worker; will retry"); err != nil {
			return 0, err
		}
	}
}

// fetchRuntimes dials atelet over the network path and reads the sandbox
// runtimes atelet keeps for this worker via GetSandboxRuntimes, returning them
// as a sandboxruntimes.File so the reconciler treats them exactly as a file
// read. A response with no sandbox classes reads as fs.ErrNotExist, which sync
// treats as "not ready yet" -- identical to an absent file for a colocated
// worker.
func fetchRuntimes(ctx context.Context, socketPath, address string, tlsConfig *tls.Config) (*sandboxruntimes.File, error) {
	conn, err := dialAtelet(socketPath, address, tlsConfig)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	callCtx, cancel := context.WithTimeout(ctx, reportTimeout)
	defer cancel()
	resp, err := ateletpb.NewWorkerSupportClient(conn).GetSandboxRuntimes(callCtx, &ateletpb.GetSandboxRuntimesRequest{})
	if err != nil {
		return nil, err
	}
	if len(resp.GetSandboxClasses()) == 0 {
		// atelet has not built this worker's runtimes yet: report it the way
		// an absent file reads for a colocated worker.
		return nil, fs.ErrNotExist
	}
	file := &sandboxruntimes.File{}
	for _, c := range resp.GetSandboxClasses() {
		class := sandboxruntimes.Class{
			SandboxClass:   c.GetSandboxClass(),
			DefaultRuntime: sandboxruntimes.Runtime{Version: attributesFromProto(c.GetDefaultRuntime())},
		}
		for _, r := range c.GetRestorableRuntimes() {
			class.RestorableRuntimes = append(class.RestorableRuntimes, sandboxruntimes.Runtime{Version: attributesFromProto(r)})
		}
		file.SandboxClasses = append(file.SandboxClasses, class)
	}
	return file, nil
}

// attributesFromProto converts the wire attribute list to the file's attribute
// map. A nil message or empty list yields a nil map, which sync reads as "no
// default runtime yet".
func attributesFromProto(in *ateletpb.SandboxRuntimeAttributes) sandboxruntimes.Attributes {
	entries := in.GetAttributes()
	if len(entries) == 0 {
		return nil
	}
	attrs := make(sandboxruntimes.Attributes, len(entries))
	for _, e := range entries {
		attrs[e.GetKey()] = e.GetValue()
	}
	return attrs
}

// reconciler reports the runtimes in the sandbox runtimes file whenever they
// differ from the ones it last reported.
type reconciler struct {
	cfg            ReportConfig
	epoch          int64
	read           func() (*sandboxruntimes.File, error)
	report         func(context.Context, *ateletpb.SetWorkerSandboxRuntimesRequest) error
	initialBackoff time.Duration
	maxBackoff     time.Duration
	resync         time.Duration
	after          func(time.Duration) <-chan time.Time
	// reported is the last request accepted by atelet, nil until the first
	// report succeeds.
	reported *ateletpb.SetWorkerSandboxRuntimesRequest
}

func newReconciler(
	cfg ReportConfig,
	epoch int64,
	read func() (*sandboxruntimes.File, error),
	report func(context.Context, *ateletpb.SetWorkerSandboxRuntimesRequest) error,
) *reconciler {
	return &reconciler{
		cfg:            cfg,
		epoch:          epoch,
		read:           read,
		report:         report,
		initialBackoff: initialReportBackoff,
		maxBackoff:     maxReportBackoff,
		resync:         resyncInterval,
		after:          time.After,
	}
}

// run syncs at startup, on every change to the file, with backoff after a
// transient failure, and periodically at resyncInterval, until ctx ends or the
// control plane refuses the epoch. Change signals are ignored while backing off
// so periodic polling or rapid writes cannot bypass the backoff delay when
// atelet or ateapi is down.
func (r *reconciler) run(ctx context.Context, changes <-chan struct{}) error {
	backoff := r.initialBackoff
	for {
		wait := r.resync
		onChange := changes
		retry, err := r.sync(ctx)
		if err != nil {
			return err
		}
		if retry {
			wait = backoff
			backoff = min(backoff*2, r.maxBackoff)
			onChange = nil
		} else {
			backoff = r.initialBackoff
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case _, ok := <-onChange:
			if !ok {
				changes = nil
			}
		case <-r.after(wait):
		}
	}
}

// sync reads the file and reports it if needed. It reports whether the caller
// should retry with backoff, and returns an error when the control plane
// refuses the epoch.
func (r *reconciler) sync(ctx context.Context) (retry bool, err error) {
	desired, err := r.read()
	if errors.Is(err, fs.ErrNotExist) {
		if r.reported == nil {
			slog.InfoContext(ctx, "Waiting for the sandbox runtimes file, which atelet writes; see its logs if this persists",
				slog.String("path", r.cfg.RuntimesFile))
		}
		return false, nil
	}
	if err != nil {
		slog.WarnContext(ctx, "Ignoring an unreadable sandbox runtimes file", slog.String("path", r.cfg.RuntimesFile), slog.Any("err", err))
		return false, nil
	}
	class := classOf(desired, r.cfg.SandboxClass)
	if class == nil {
		slog.WarnContext(ctx, "Ignoring a sandbox runtimes file without this worker's sandbox class",
			slog.String("path", r.cfg.RuntimesFile), slog.String("sandbox_class", r.cfg.SandboxClass))
		return false, nil
	}
	if len(class.DefaultRuntime.Version) == 0 {
		slog.WarnContext(ctx, "Ignoring a sandbox runtimes file with no default runtime", slog.String("path", r.cfg.RuntimesFile))
		return false, nil
	}
	req := buildRuntimesRequest(r.epoch, class)
	setActorOverhead(req, r.cfg.ActorOverhead)
	if proto.Equal(req, r.reported) {
		return false, nil
	}
	if err := r.report(ctx, req); err != nil {
		switch status.Code(err) {
		case codes.InvalidArgument:
			slog.ErrorContext(ctx, "Sandbox runtimes rejected as invalid; waiting for sandbox runtimes file to change", slog.Any("err", err))
			return false, nil
		case codes.FailedPrecondition:
			return false, fmt.Errorf("report sandbox runtimes: epoch %d refused: %w", r.epoch, err)
		}
		slog.WarnContext(ctx, "Could not report sandbox runtimes; will retry", slog.Any("err", err))
		return true, nil
	}
	r.reported = req
	slog.InfoContext(ctx, "Reported worker sandbox runtimes",
		slog.Int64("epoch", r.epoch),
		slog.Any("sandbox_classes", req.GetSandboxClasses()))
	return false, nil
}

// dialingClient dials atelet for each call.
type dialingClient struct {
	socketPath string
	// address, when set, is the atelet-workers Service a network worker
	// dials over TCP mTLS instead of the socket (see dialAtelet).
	address   string
	tlsConfig *tls.Config
}

func (c *dialingClient) AcquireWorkerEpoch(ctx context.Context) (int64, error) {
	var epoch int64
	err := c.call(ctx, func(ctx context.Context, client ateletpb.WorkerSupportClient) error {
		resp, err := client.AcquireWorkerEpoch(ctx, &ateletpb.AcquireWorkerEpochRequest{})
		epoch = resp.GetEpoch()
		return err
	})
	return epoch, err
}

func (c *dialingClient) RegisterWorker(ctx context.Context, req *ateletpb.RegisterWorkerRequest) error {
	return c.call(ctx, func(ctx context.Context, client ateletpb.WorkerSupportClient) error {
		_, err := client.RegisterWorker(ctx, req)
		return err
	})
}

func (c *dialingClient) SetWorkerSandboxRuntimes(ctx context.Context, req *ateletpb.SetWorkerSandboxRuntimesRequest) error {
	return c.call(ctx, func(ctx context.Context, client ateletpb.WorkerSupportClient) error {
		_, err := client.SetWorkerSandboxRuntimes(ctx, req)
		return err
	})
}

func (c *dialingClient) call(ctx context.Context, f func(context.Context, ateletpb.WorkerSupportClient) error) error {
	conn, err := dialAtelet(c.socketPath, c.address, c.tlsConfig)
	if err != nil {
		return err
	}
	defer conn.Close()
	callCtx, cancel := context.WithTimeout(ctx, reportTimeout)
	defer cancel()
	return f(callCtx, ateletpb.NewWorkerSupportClient(conn))
}

// dialAtelet opens the atelet connection for reporting: the TCP mTLS path to
// the atelet-workers Service when address is set, the node-local Unix socket
// otherwise. address takes precedence so a misconfiguration cannot silently
// fall back to a Unix socket a network worker cannot reach.
func dialAtelet(socketPath, address string, tlsConfig *tls.Config) (*grpc.ClientConn, error) {
	if address != "" {
		return workertransport.Dial(address, tlsConfig)
	}
	return ateletdial.Dial(socketPath, tlsConfig)
}
