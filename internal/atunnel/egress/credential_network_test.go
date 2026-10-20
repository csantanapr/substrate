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

package egress

import (
	"context"
	"crypto/x509"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.ate.dev/substrate/internal/installdefaults"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/substratex509"
	"go.ate.dev/substrate/internal/workertransport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestBrokerCertificateSourceDialsNetworkAteletOverTCP is the network-mode
// counterpart of TestBrokerCertificateSourceMintsAndReusesKey: with Address set
// (and SocketPath deliberately absent), the source reaches atelet over the TCP
// mTLS path -- the atelet-workers Service listener a network worker uses --
// rather than the Unix socket a separate guest Pod cannot see. The server here
// is assembled exactly as the node atelet's network WorkerSupport listener is
// (workertransport.ServerTLS + a PeerPolicy + UnaryAuthorizer with the
// WorkerSupport method allowlist), so a mint that succeeds here proves the
// worker-side transport, the server-side authorizer, and the mTLS handshake all
// agree. The stub checks only that the request carries the expected actor UID;
// it does NOT independently tie that UID to the authenticated mTLS Pod UID, so
// this test does not by itself prove the actor->worker binding. That binding is
// exercised against the authenticated identity in the tunnel package's e2e
// (TestTunnelNetworkPathMintsAndFetchesForItsOwnActor); here the claim is the
// TCP mTLS transport and key reuse.
func TestBrokerCertificateSourceDialsNetworkAteletOverTCP(t *testing.T) {
	source, broker := newTestNetworkBrokerCertificateSource(t, newNetworkAtelet(t))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for range 2 {
		if _, err := source.MintWorkerCertificate(ctx); err != nil {
			t.Fatalf("network mint: %v", err)
		}
	}
	first := receiveWithin(t, broker.publicKeys, "first certificate public key")
	second := receiveWithin(t, broker.publicKeys, "second certificate public key")
	if string(first) != string(second) {
		t.Fatal("renewal replaced the actor private key over the network path")
	}
}

// TestBrokerCertificateSourceNetworkMintRefusesUnauthorizedPeer proves the
// server-side authorizer gates the network path: the same source,
// dialing an atelet whose per-RPC authorizer refuses this worker, cannot mint.
// This is what guarantees the TCP path is not weaker than the Unix socket the
// colocated worker reaches -- an unauthorized peer is refused, not served.
func TestBrokerCertificateSourceNetworkMintRefusesUnauthorizedPeer(t *testing.T) {
	atelet := newNetworkAtelet(t)
	atelet.authorize = func(context.Context, *substratex509.PodIdentity, string) error {
		return status.Error(codes.PermissionDenied, "worker is not in the live inventory")
	}
	source, _ := newTestNetworkBrokerCertificateSource(t, atelet)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := source.MintWorkerCertificate(ctx); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("network mint by an unauthorized peer = %v, want PermissionDenied", err)
	}
}

// TestBrokerConfigRequiresSocketOrAddress proves NewBrokerCertificateSource
// rejects a config that names neither the Unix socket nor a network address:
// there would be no atelet to reach. It also proves that a network config (only
// Address) is accepted even though SocketPath is empty, which is the whole
// point of the network mode.
func TestBrokerConfigRequiresSocketOrAddress(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t)
	workerCert := issueTestPodCertificate(t, ca, &substratex509.PodIdentity{
		Namespace: "ate-demo", ServiceAccountName: "worker", ServiceAccountUID: "worker-sa-uid",
		PodName: "worker", PodUID: "worker-uid", NodeName: "node-a", NodeUID: "node-uid",
	}, "spiffe://cluster.local/ns/ate-demo/sa/worker", nil)
	credentialPath := filepath.Join(dir, "worker.pem")
	trustPath := filepath.Join(dir, "trust.pem")
	writeCredentialBundle(t, credentialPath, workerCert)
	if err := os.WriteFile(trustPath, ca.certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	base := BrokerConfig{
		CredentialBundlePath: credentialPath,
		TrustBundlePath:      trustPath,
		ActorAtespace:        "actor-atespace",
		ActorName:            "actor-name",
		ActorUID:             "actor-uid",
		AteletSPIFFEID:       installdefaults.AteletSPIFFEID(installdefaults.SystemNamespace),
	}
	if _, err := NewBrokerCertificateSource(base); err == nil || !strings.Contains(err.Error(), "socket or address") {
		t.Fatalf("NewBrokerCertificateSource with neither socket nor address = %v, want a socket-or-address error", err)
	}
	// Only Address: the network mode, accepted.
	networkCfg := base
	networkCfg.Address = "atelet-workers.ate-system:8087"
	if _, err := NewBrokerCertificateSource(networkCfg); err != nil {
		t.Fatalf("NewBrokerCertificateSource with only Address = %v, want nil", err)
	}
}

// networkAtelet is a node atelet's network WorkerSupport listener, assembled
// the way cmd/atelet does it: a workertransport.ServerTLS mTLS listener with a
// PeerPolicy and a UnaryAuthorizer that both re-checks the authenticated peer
// (authorize) and limits the method set to WorkerSupport's. It serves the
// egress WorkerSupport methods (MintActorCertificate here; the tunnel package
// test adds GetActorEgressPolicy against the same assembly).
type networkAtelet struct {
	ca        *testCA
	address   string
	authorize workertransport.Authorizer
}

// newNetworkAtelet stands up the listener with a CA but no worker/atelet certs
// yet; newTestNetworkBrokerCertificateSource mints those and starts the server,
// so a test that needs a custom authorizer can set it first.
func newNetworkAtelet(t *testing.T) *networkAtelet {
	t.Helper()
	return &networkAtelet{ca: newTestCA(t)}
}

// newTestNetworkBrokerCertificateSource issues the worker and atelet Pod
// certificates, starts the atelet's TCP mTLS WorkerSupport listener with the
// node-atelet authorizer assembly, and returns a BrokerCertificateSource
// configured with Address (and no SocketPath) pointed at it.
func newTestNetworkBrokerCertificateSource(t *testing.T, atelet *networkAtelet) (*BrokerCertificateSource, *workerSupportStub) {
	t.Helper()
	ca := atelet.ca
	workerIdentity := &substratex509.PodIdentity{
		Namespace: "ate-demo", ServiceAccountName: "ateworker", ServiceAccountUID: "worker-sa-uid",
		PodName: "worker", PodUID: "worker-uid", NodeName: "node-a", NodeUID: "node-uid",
	}
	workerCert := issueTestPodCertificate(t, ca, workerIdentity,
		"spiffe://cluster.local/ns/ate-demo/sa/ateworker", []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth})
	ateletIdentity := &substratex509.PodIdentity{
		Namespace: "ate-system", ServiceAccountName: "atelet", ServiceAccountUID: "atelet-sa-uid",
		PodName: "atelet", PodUID: "atelet-uid", NodeName: "node-a", NodeUID: "node-uid",
	}
	ateletCert := issueTestPodCertificate(t, ca, ateletIdentity,
		"spiffe://cluster.local/ns/ate-system/sa/atelet", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth})

	dir := t.TempDir()
	workerBundle := filepath.Join(dir, "worker.pem")
	trustPath := filepath.Join(dir, "trust.pem")
	ateletBundle := filepath.Join(dir, "atelet.pem")
	writeCredentialBundle(t, workerBundle, workerCert)
	writeCredentialBundle(t, ateletBundle, ateletCert)
	if err := os.WriteFile(trustPath, ca.certPEM, 0o600); err != nil {
		t.Fatal(err)
	}

	// The node atelet's own server TLS, pinned to the worker service account,
	// same node, plus the live-inventory authorize. This is the production
	// ServerTLS, not a hand-rolled config.
	serverTLS, err := workertransport.ServerTLS(ateletBundle, trustPath, workertransport.PeerPolicy{
		ServiceAccount: "ateworker",
		SameNode:       true,
		Authorize: func(id *substratex509.PodIdentity) error {
			if id.PodUID != workerIdentity.PodUID {
				return status.Error(codes.PermissionDenied, "worker is not in the live inventory")
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("network atelet server TLS: %v", err)
	}
	authorize := atelet.authorize
	if authorize == nil {
		authorize = allowWorkerSupport
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(
		grpc.Creds(workertransport.RefreshingCredentials(serverTLS)),
		grpc.UnaryInterceptor(workertransport.UnaryAuthorizer(authorize)),
	)
	broker := &workerSupportStub{ca: ca, lifetime: time.Hour, publicKeys: make(chan []byte, 2)}
	ateletpb.RegisterWorkerSupportServer(server, broker)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	atelet.address = listener.Addr().String()

	source, err := NewBrokerCertificateSource(BrokerConfig{
		// No SocketPath on purpose: a network worker has no Unix socket, and
		// Address alone must be enough. A fallback to the socket here would dial
		// nothing.
		Address:              atelet.address,
		CredentialBundlePath: workerBundle,
		TrustBundlePath:      trustPath,
		ActorAtespace:        "actor-atespace",
		ActorName:            "actor-name",
		ActorUID:             "actor-uid",
		AteletSPIFFEID:       installdefaults.AteletSPIFFEID(installdefaults.SystemNamespace),
	})
	if err != nil {
		t.Fatalf("network broker certificate source: %v", err)
	}
	return source, broker
}

// allowWorkerSupport is the default per-RPC authorizer: it allows only the
// WorkerSupport methods, mirroring cmd/atelet's authorizeWorker allowlist (the
// peer is already authenticated and inventory-checked by the PeerPolicy).
func allowWorkerSupport(_ context.Context, _ *substratex509.PodIdentity, method string) error {
	switch method {
	case ateletpb.WorkerSupport_MintActorCertificate_FullMethodName,
		ateletpb.WorkerSupport_GetActorEgressPolicy_FullMethodName:
		return nil
	default:
		return status.Error(codes.PermissionDenied, "method is not a worker-support service")
	}
}
