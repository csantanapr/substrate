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

// network.go carries the worker network mode. It is OFF by default:
// with --worker-listen-address and --atelet-address both empty (the default),
// the worker serves its Worker service over the node-local Unix socket and
// dials the node atelet's WorkerSupport over that same socket, as a colocated
// worker does. The two flags are paired: setting one without the other is a
// configuration error, so there is no half-configured mode.
//
// With both set, the worker runs in a separate guest Pod that shares no
// filesystem with its node atelet. It then:
//   - serves Worker AND WorkerStaging over a Pod-to-Pod TCP listener whose
//     authorization is a workertransport PeerPolicy pinning the node atelet's
//     identity (its SPIFFE id's namespace + service account, same node
//     incarnation) plus a per-RPC authorizer that admits ONLY the worker
//     lifecycle and staging methods -- never a plaintext or Unix fallback;
//   - dials the atelet-workers ClusterIP Service over TCP mTLS for
//     WorkerSupport (register, mint, suspend, egress policy) and for OTLP,
//     verifying the atelet by its SPIFFE id and node incarnation.
//
// The primitives (workertransport.ServerTLS/Dial/PeerPolicy/UnaryAuthorizer/
// StreamAuthorizer/RefreshingCredentials, ateletdial.TLSConfig,
// workerstaging.NewServer) already exist; this file only wires the worker's
// network entry points to them behind the paired flags.
package main

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/spf13/pflag"
	"go.ate.dev/substrate/internal/ateletdial"
	"go.ate.dev/substrate/internal/atunnel"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/proto/workerstagingpb"
	"go.ate.dev/substrate/internal/substratex509"
	"go.ate.dev/substrate/internal/workerstaging"
	"go.ate.dev/substrate/internal/workertransport"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	workerListenAddress = pflag.String("worker-listen-address", "",
		"Optional authenticated TCP listener (host:port) on which this worker serves its Worker and WorkerStaging services to the node atelet. Empty (the default) serves only the native Unix socket. Must be set together with --atelet-address.")
	ateletAddress = pflag.String("atelet-address", "",
		"Node-local atelet-workers Service host:port for WorkerSupport and OTLP in network-worker mode. Empty (the default) reaches atelet over the node-local Unix socket. Must be set together with --worker-listen-address.")
)

// networkEnabled reports whether this worker runs in network mode. The two
// flags are validated as a pair by validateNetworkFlags, so once that has
// passed, either flag's being set is equivalent; this reads --atelet-address
// because that is the one the dial paths (register, OTLP) consult.
func networkEnabled() bool {
	return *ateletAddress != ""
}

// validateNetworkFlags rejects a half-configured network mode. Serving on a TCP
// listener without an atelet address to register through, or the reverse, is
// never intended, so it is an error rather than a silent partial mode.
func validateNetworkFlags() error {
	if (*workerListenAddress == "") != (*ateletAddress == "") {
		return fmt.Errorf("--worker-listen-address and --atelet-address must be set together (got worker-listen-address=%q, atelet-address=%q)", *workerListenAddress, *ateletAddress)
	}
	return nil
}

// applyNetworkTunnelConfig returns base with its atelet address set from
// ateletAddress, which is what makes the tunnel's credential broker and
// egress-policy fetch dial atelet over TCP mTLS in network mode (the same
// --atelet-address register uses, so the two dial paths cannot diverge). An
// empty ateletAddress leaves AteletAddress empty, so a colocated worker keeps
// the node-local Unix-socket dial unchanged. It is a pure
// function of its inputs so do()'s use of its result is testable without
// running do(): a regression that stopped wiring the address (dropped this
// call, or passed *tunnelConfig to Start instead of the result) is caught by
// TestApplyNetworkTunnelConfig.
func applyNetworkTunnelConfig(base atunnel.Config, ateletAddress string) atunnel.Config {
	base.AteletAddress = ateletAddress
	return base
}

// ateletPeerPolicy pins the one peer a network worker's TCP listener accepts:
// the node atelet, identified by the namespace and service account in its
// SPIFFE id (the same broker identity register already verifies the atelet by)
// and required to be on this worker's own node incarnation. SameNode is checked
// against the worker's own Pod identity, which workertransport.ServerTLS loads
// from the worker's credential bundle.
//
// The policy is built from the atelet SPIFFE id rather than hard-coded so a
// non-default install namespace still authorizes the right atelet.
func ateletPeerPolicy(ateletSPIFFEID string) (workertransport.PeerPolicy, error) {
	namespace, serviceAccount, err := spiffeNamespaceServiceAccount(ateletSPIFFEID)
	if err != nil {
		return workertransport.PeerPolicy{}, fmt.Errorf("atelet identity: %w", err)
	}
	return workertransport.PeerPolicy{
		Namespace:      namespace,
		ServiceAccount: serviceAccount,
		SameNode:       true,
	}, nil
}

// spiffeNamespaceServiceAccount extracts the namespace and service account from
// a Substrate workload SPIFFE id of the form
// spiffe://<trust-domain>/ns/<namespace>/sa/<service-account>, the shape
// installdefaults.SPIFFEID produces. Any other shape is an error: the worker
// must not fall back to an unpinned or partially-pinned atelet policy.
func spiffeNamespaceServiceAccount(id string) (namespace, serviceAccount string, err error) {
	u, err := url.Parse(id)
	if err != nil {
		return "", "", fmt.Errorf("parse SPIFFE id %q: %w", id, err)
	}
	if u.Scheme != "spiffe" || u.Host == "" {
		return "", "", fmt.Errorf("not a SPIFFE id: %q", id)
	}
	// A SPIFFE workload id is exactly spiffe://<trust-domain>/ns/<ns>/sa/<sa>:
	// no query, fragment, or userinfo, and the path must be those four segments
	// verbatim. Reject anything else outright rather than normalizing it, so a
	// non-canonical input (e.g. a ".." segment or a trailing query) cannot be
	// coerced into a policy. Using EscapedPath and splitting the raw segments
	// (no path.Clean) is what makes "/ns/x/../sa/y" a rejection, not "/ns/y".
	if u.RawQuery != "" || u.Fragment != "" || u.User != nil {
		return "", "", fmt.Errorf("SPIFFE id carries query, fragment, or userinfo: %q", id)
	}
	p := u.EscapedPath()
	if !strings.HasPrefix(p, "/") {
		return "", "", fmt.Errorf("SPIFFE id path is not absolute: %q", id)
	}
	segments := strings.Split(strings.TrimPrefix(p, "/"), "/")
	if len(segments) != 4 || segments[0] != "ns" || segments[2] != "sa" || segments[1] == "" || segments[3] == "" {
		return "", "", fmt.Errorf("SPIFFE id is not ns/<namespace>/sa/<service-account>: %q", id)
	}
	return segments[1], segments[3], nil
}

// authorizeAteletCall is the per-RPC authorizer on the worker's network TCP
// listener. The caller is already an authenticated same-node atelet (the mTLS
// PeerPolicy verified it); this limits what that atelet may invoke to the
// worker's own lifecycle and staging services, so a connection that passed the
// handshake still cannot reach any other service the server happens to host.
func authorizeAteletCall(_ context.Context, _ *substratex509.PodIdentity, method string) error {
	if strings.HasPrefix(method, workerServicePrefix) || strings.HasPrefix(method, workerStagingServicePrefix) {
		return nil
	}
	return status.Error(codes.PermissionDenied, "method is not a worker lifecycle or staging service")
}

// The gRPC service path prefixes the network listener admits. Derived from the
// generated full-method-name constants so a proto package rename cannot leave
// these matching nothing (a silently closed listener) or matching too much.
var (
	workerServicePrefix        = methodServicePrefix(ateworkerpb.Worker_RunWorkload_FullMethodName)
	workerStagingServicePrefix = methodServicePrefix(workerstagingpb.WorkerStaging_PrepareActivation_FullMethodName)
)

// methodServicePrefix turns a gRPC full method name "/pkg.Service/Method" into
// its service path prefix "/pkg.Service/".
func methodServicePrefix(fullMethod string) string {
	if i := strings.LastIndex(fullMethod, "/"); i > 0 {
		return fullMethod[:i+1]
	}
	return fullMethod
}

// networkServing holds what the worker needs to serve its services over the
// authenticated TCP listener in network mode.
type networkServing struct {
	// listenAddress is the TCP address the worker serves on.
	listenAddress string
	// transportOptions carry the network listener's mTLS credentials, the
	// receive-size cap, and the STREAM authorizer. They deliberately do NOT
	// include a unary interceptor: the unary chain (authorizer -> Internal
	// logging -> staging lifecycle) is assembled once by workerServerOptions
	// so there is exactly one grpc.UnaryInterceptor/ChainUnaryInterceptor in
	// the final option list. grpc-go rejects a second UnaryInterceptor with a
	// panic, which is what happened when these options carried their own unary
	// interceptor and main.go added another.
	transportOptions []grpc.ServerOption
	// staging is the WorkerStaging server registered alongside Worker. A
	// network worker cannot share files with atelet, so staging moves image
	// layers, runtime assets, actor state and checkpoints over this RPC. Its
	// LifecycleInterceptor is the innermost unary interceptor (see
	// workerServerOptions).
	staging *workerstaging.Server
}

// buildNetworkServing builds the TLS, authorizers and WorkerStaging server for
// the worker's network listener. credentialBundlePath/trustBundlePath are the
// worker's own Pod credential bundle and trust bundle; ateletSPIFFEID is the
// node atelet's identity to pin; base and podUID locate the worker's staging
// area; isActive reports whether an actor UID is currently hosted, which the
// staging LifecycleInterceptor uses to bound what a staging RPC may touch.
func buildNetworkServing(credentialBundlePath, trustBundlePath, ateletSPIFFEID, base, podUID string, isActive func(string) bool) (*networkServing, error) {
	policy, err := ateletPeerPolicy(ateletSPIFFEID)
	if err != nil {
		return nil, err
	}
	tlsConfig, err := workertransport.ServerTLS(credentialBundlePath, trustBundlePath, policy)
	if err != nil {
		return nil, fmt.Errorf("network worker server TLS: %w", err)
	}
	staging, err := workerstaging.NewServer(base, podUID, isActive)
	if err != nil {
		return nil, fmt.Errorf("network worker staging: %w", err)
	}
	return &networkServing{
		listenAddress: *workerListenAddress,
		transportOptions: []grpc.ServerOption{
			grpc.Creds(workertransport.RefreshingCredentials(tlsConfig)),
			grpc.MaxRecvMsgSize(16 << 20),
			grpc.StreamInterceptor(workertransport.StreamAuthorizer(authorizeAteletCall)),
		},
		staging: staging,
	}, nil
}

// dialAtelet opens a TCP mTLS connection to the atelet-workers Service for
// WorkerSupport and OTLP in network mode. It verifies the atelet by its SPIFFE
// id and node incarnation (ateletdial.TLSConfig) and dials the configured
// host:port over TCP (workertransport.Dial), with no plaintext or Unix
// fallback. The caller closes the connection.
func dialAtelet(credentialBundlePath, trustBundlePath, ateletSPIFFEID string) (*grpc.ClientConn, error) {
	config, err := ateletdial.TLSConfig(credentialBundlePath, trustBundlePath, ateletSPIFFEID)
	if err != nil {
		return nil, fmt.Errorf("network atelet identity: %w", err)
	}
	conn, err := workertransport.Dial(*ateletAddress, config, grpc.WithStatsHandler(otelgrpc.NewClientHandler()))
	if err != nil {
		return nil, fmt.Errorf("network atelet endpoint: %w", err)
	}
	return conn, nil
}
