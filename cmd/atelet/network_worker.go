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

// network_worker.go carries the network transport for workers that
// run in a separate guest Pod and cannot share the node atelet's
// filesystem or its worker-support Unix socket. It is OFF by default: with
// --network-workers unset, atelet serves WorkerSupport only over the node-local
// Unix socket (verifyClientOnSameNode) and this file adds nothing to the trust
// surface. With it set, atelet ALSO serves WorkerSupport over a Pod-to-Pod TCP
// listener whose authorization is the workertransport PeerPolicy plus the live
// Kubernetes worker inventory -- never a weaker fallback.
package main

import (
	"context"
	"fmt"

	"github.com/spf13/pflag"
	"go.ate.dev/substrate/internal/installdefaults"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/substratex509"
	"go.ate.dev/substrate/internal/workertransport"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/client-go/kubernetes"
)

var (
	networkWorkers = pflag.Bool("network-workers", false,
		"Serve WorkerSupport to workers over authenticated Pod networking as well as the Unix socket, for workers that run in a separate guest Pod. Default: Unix socket only.")
	networkWorkerServiceAccount = pflag.String("network-worker-service-account", installdefaults.WorkerServiceAccount,
		"Service account a network worker Pod must present on its mTLS certificate. Defaults to the service account the controller runs worker pods as.")
	networkWorkerPort = pflag.Int("network-worker-port", 8086,
		"Port a network worker Pod serves its lifecycle and staging services on.")
	workerServicesAddress = pflag.String("worker-services-listen-address", ":8087",
		"TCP listen address for the networked WorkerSupport service when --network-workers is set. Workers reach it through a ClusterIP Service; the mTLS PeerPolicy is the authorization, not the bind.")
)

// nodeWorkerNetwork holds the node-scoped worker inventory used both to
// authorize an inbound WorkerSupport handshake (Authorize, via the PeerPolicy)
// and, as a client, to resolve and dial a worker Pod for lifecycle and staging.
// bundlePath/trustPath are atelet's own Pod credential bundle and trust bundle,
// used to present atelet's client certificate and to verify the worker's, so a
// client dial over the network is mutually authenticated by the same material
// the inbound listener uses.
type nodeWorkerNetwork struct {
	directory             *workertransport.Directory
	local                 substratex509.PodIdentity
	bundlePath, trustPath string
	serviceAccount        string
}

// newNodeWorkerNetwork returns nil (and no error) when network workers are
// disabled, so the caller adds nothing to the serving topology in the default
// case. When enabled it requires atelet's own complete Pod identity and builds
// the node-scoped network-worker directory.
func newNodeWorkerNetwork(ctx context.Context, client kubernetes.Interface, local *substratex509.PodIdentity) (*nodeWorkerNetwork, error) {
	if !*networkWorkers {
		return nil, nil
	}
	if local == nil || local.NodeName == "" || local.NodeUID == "" {
		return nil, fmt.Errorf("network atelet requires a complete Pod identity")
	}
	directory, err := workertransport.NewDirectory(ctx, client, *local, *networkWorkerServiceAccount, *networkWorkerPort)
	if err != nil {
		return nil, err
	}
	return &nodeWorkerNetwork{
		directory:      directory,
		local:          *local,
		bundlePath:     *grpcServerCredBundle,
		trustPath:      *clientCACerts,
		serviceAccount: *networkWorkerServiceAccount,
	}, nil
}

// dial opens an mTLS connection to a network worker Pod, as a client. The pod
// IP comes from the live Kubernetes worker inventory (Directory.Resolve, which
// admits only a usable same-node network worker), never from a request. The
// PeerPolicy pins the exact worker Pod UID being dialed and re-authorizes the
// worker's certificate against the live inventory on the handshake, so a
// deleted or moved worker is refused even if its certificate still verifies.
// There is no plaintext or Unix fallback: workertransport.Dial refuses a nil
// TLS config and ClientTLS verifies the chain and the pinned identity.
func (n *nodeWorkerNetwork) dial(uid string) (*grpc.ClientConn, error) {
	address, namespace, err := n.directory.Resolve(uid)
	if err != nil {
		return nil, err
	}
	config, err := workertransport.ClientTLS(n.bundlePath, n.trustPath, workertransport.PeerPolicy{
		Namespace:      namespace,
		ServiceAccount: n.serviceAccount,
		PodUID:         uid,
		SameNode:       true,
		Authorize:      n.directory.Authorize,
	})
	if err != nil {
		return nil, err
	}
	return workertransport.Dial(address, config, grpc.WithStatsHandler(otelgrpc.NewClientHandler()))
}

// peerPolicy is the authorization on the inbound TCP WorkerSupport listener:
// the worker's service account, a same-node-incarnation certificate, and a live
// match in the Kubernetes worker inventory (directory.Authorize). Passing it to
// workertransport.ServerTLS is what makes mTLS plus this policy -- never a
// plaintext or Unix fallback -- the gate on the network path.
func (n *nodeWorkerNetwork) peerPolicy() workertransport.PeerPolicy {
	return workertransport.PeerPolicy{
		ServiceAccount: n.serviceAccount,
		SameNode:       true,
		Authorize:      n.directory.Authorize,
	}
}

// authorizeWorker is the per-RPC authorizer on the TCP WorkerSupport listener.
// It re-checks the authenticated peer against the live inventory on every RPC
// (so a worker deleted or moved after its connection opened is rejected) and
// then allows only the WorkerSupport methods a worker calls and the three OTLP
// Export methods its telemetry uses -- never a lifecycle or control method,
// which a worker must not call on atelet. A nil directory (which would
// make the inventory re-check vacuous) fails closed.
func (n *nodeWorkerNetwork) authorizeWorker(_ context.Context, identity *substratex509.PodIdentity, method string) error {
	if n.directory == nil {
		return status.Error(codes.PermissionDenied, "worker inventory authorizer is required")
	}
	if err := n.directory.Authorize(identity); err != nil {
		return status.Error(codes.PermissionDenied, err.Error())
	}
	switch method {
	case ateletpb.WorkerSupport_MintActorCertificate_FullMethodName,
		ateletpb.WorkerSupport_AcquireWorkerEpoch_FullMethodName,
		ateletpb.WorkerSupport_RegisterWorker_FullMethodName,
		ateletpb.WorkerSupport_SetWorkerSandboxRuntimes_FullMethodName,
		ateletpb.WorkerSupport_ReportActorCrashed_FullMethodName,
		ateletpb.WorkerSupport_RequestActorSuspend_FullMethodName,
		ateletpb.WorkerSupport_GetActorEgressPolicy_FullMethodName,
		ateletpb.WorkerSupport_GetSandboxRuntimes_FullMethodName,
		"/opentelemetry.proto.collector.trace.v1.TraceService/Export",
		"/opentelemetry.proto.collector.metrics.v1.MetricsService/Export",
		"/opentelemetry.proto.collector.logs.v1.LogsService/Export":
		return nil
	default:
		return status.Error(codes.PermissionDenied, "method is not a worker-support service")
	}
}
