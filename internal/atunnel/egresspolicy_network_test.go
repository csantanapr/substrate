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

package atunnel

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"go.ate.dev/substrate/internal/ateletdial"
	"go.ate.dev/substrate/internal/installdefaults"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/substratex509"
	"go.ate.dev/substrate/internal/workertransport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// networkAteletHarness is the node atelet's network WorkerSupport listener,
// assembled exactly as cmd/atelet does it: a workertransport.ServerTLS mTLS
// listener with a PeerPolicy (worker service account, same node, live-inventory
// authorize) and a UnaryAuthorizer that re-checks the peer and limits the
// method set to WorkerSupport's. The handler binds the actor to the
// AUTHENTICATED worker's Pod UID -- never to a request field -- so a worker can
// only mint a certificate or fetch an egress policy for the actor currently
// assigned to it. That binding is the invariant cmd/atelet's
// MintActorCertificate enforces (identity from the mTLS cert, the control plane
// mints only for the actor assigned to that Worker); the harness reproduces it
// here so the worker-side network path is proven at least as strong as native.
type networkAteletHarness struct {
	ateletpb.UnimplementedWorkerSupportServer
	ca *tunnelNetworkCA
	// actorByWorker maps an authenticated worker Pod UID to the single actor it
	// is assigned. A request naming any other actor is refused.
	actorByWorker map[string]resources.ActorRef
	policy        *ateletpb.EgressPolicy
	// reached records the Pod UID of every peer whose RPC got as far as a handler.
	reached sync.Map
}

// served reports whether an RPC from the worker with this Pod UID reached a handler.
func (h *networkAteletHarness) served(podUID string) bool {
	_, ok := h.reached.Load(podUID)
	return ok
}

func (h *networkAteletHarness) boundActor(ctx context.Context, wantAtespace, wantName string) (resources.ActorRef, error) {
	identity, err := workertransport.Identity(ctx)
	if err != nil {
		return resources.ActorRef{}, err
	}
	h.reached.Store(identity.PodUID, true)
	actor, ok := h.actorByWorker[identity.PodUID]
	if !ok {
		return resources.ActorRef{}, status.Error(codes.PermissionDenied, "worker hosts no actor")
	}
	if wantAtespace != actor.Atespace || wantName != actor.Name {
		return resources.ActorRef{}, status.Error(codes.PermissionDenied, "actor is not assigned to this worker")
	}
	return actor, nil
}

func (h *networkAteletHarness) MintActorCertificate(ctx context.Context, req *ateletpb.MintActorCertificateRequest) (*ateletpb.MintActorCertificateResponse, error) {
	if _, err := h.boundActor(ctx, req.GetActorAtespace(), req.GetActorName()); err != nil {
		return nil, err
	}
	csr, err := x509.ParseCertificateRequest(req.GetCertificateSigningRequest())
	if err != nil || csr.CheckSignature() != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid CSR")
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(now.UnixNano()),
		Subject:      pkix.Name{CommonName: "actor"},
		URIs: []*url.URL{{
			Scheme: "spiffe", Host: "substrate-actor.local",
			Path: path.Join("worker", "actor", req.GetActorAtespace(), req.GetActorName()),
		}},
		NotBefore:   now.Add(-time.Minute),
		NotAfter:    now.Add(time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, h.ca.cert, csr.PublicKey, h.ca.key)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &ateletpb.MintActorCertificateResponse{ActorCertificates: [][]byte{der}}, nil
}

func (h *networkAteletHarness) GetActorEgressPolicy(ctx context.Context, req *ateletpb.GetActorEgressPolicyRequest) (*ateletpb.GetActorEgressPolicyResponse, error) {
	if _, err := h.boundActor(ctx, req.GetActorAtespace(), req.GetActorName()); err != nil {
		return nil, err
	}
	return &ateletpb.GetActorEgressPolicyResponse{EgressPolicy: h.policy}, nil
}

func (h *networkAteletHarness) authorize(_ context.Context, identity *substratex509.PodIdentity, method string) error {
	if _, ok := h.actorByWorker[identity.PodUID]; !ok {
		return status.Error(codes.PermissionDenied, "worker is not in the live inventory")
	}
	switch method {
	case ateletpb.WorkerSupport_MintActorCertificate_FullMethodName,
		ateletpb.WorkerSupport_GetActorEgressPolicy_FullMethodName:
		return nil
	default:
		return status.Error(codes.PermissionDenied, "method is not a worker-support service")
	}
}

// startNetworkAtelet issues the atelet's own Pod certificate, stands up the
// TCP mTLS WorkerSupport listener with the production ServerTLS + authorizer
// assembly, and returns the listen address and the harness.
func startNetworkAtelet(t *testing.T, ca *tunnelNetworkCA, workerServiceAccount string, actorByWorker map[string]resources.ActorRef, policy *ateletpb.EgressPolicy) (string, *networkAteletHarness) {
	t.Helper()
	dir := t.TempDir()
	ateletBundle := filepath.Join(dir, "atelet.pem")
	trustPath := filepath.Join(dir, "trust.pem")
	ca.writeBundle(t, ateletBundle, &substratex509.PodIdentity{
		Namespace: "ate-system", ServiceAccountName: "atelet", ServiceAccountUID: "atelet-sa-uid",
		PodName: "atelet", PodUID: "atelet-uid", NodeName: testNodeName, NodeUID: testNodeUID,
	}, installdefaults.AteletSPIFFEID(installdefaults.SystemNamespace),
		[]x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth})
	if err := os.WriteFile(trustPath, ca.certPEM, 0o600); err != nil {
		t.Fatal(err)
	}

	harness := &networkAteletHarness{ca: ca, actorByWorker: actorByWorker, policy: policy}
	serverTLS, err := workertransport.ServerTLS(ateletBundle, trustPath, workertransport.PeerPolicy{
		ServiceAccount: workerServiceAccount,
		SameNode:       true,
		Authorize: func(id *substratex509.PodIdentity) error {
			if _, ok := actorByWorker[id.PodUID]; !ok {
				return status.Error(codes.PermissionDenied, "worker is not in the live inventory")
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("network atelet server TLS: %v", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(
		grpc.Creds(workertransport.RefreshingCredentials(serverTLS)),
		grpc.UnaryInterceptor(workertransport.UnaryAuthorizer(harness.authorize)),
	)
	ateletpb.RegisterWorkerSupportServer(server, harness)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return listener.Addr().String(), harness
}

// networkTunnel builds a Tunnel configured for the network path (AteletAddress
// set, worker credentials at bundlePath/trustPath). It builds the same
// ateletTLS Start would, so the credential broker and egress-policy fetch reach
// atelet exactly as the running worker does.
func networkTunnel(t *testing.T, address, bundlePath, trustPath string) *Tunnel {
	t.Helper()
	cfg := Config{
		CredentialBundle:  bundlePath,
		TrustBundle:       trustPath,
		BrokerIdentity:    installdefaults.AteletSPIFFEID(installdefaults.SystemNamespace),
		EgressTrustBundle: trustPath,
		AteletAddress:     address,
	}
	ateletTLS, err := ateletdial.TLSConfig(cfg.CredentialBundle, cfg.TrustBundle, cfg.BrokerIdentity)
	if err != nil {
		t.Fatalf("worker atelet TLS: %v", err)
	}
	return &Tunnel{cfg: cfg, ateletTLS: ateletTLS}
}

// TestTunnelNetworkPathMintsAndFetchesForItsOwnActor is the in-process
// end-to-end proof of the worker-side network egress path against the real
// node-atelet network WorkerSupport authorizer assembly. An authorized network
// worker (Address set, no Unix socket) mints its actor certificate through
// PrepareEgress and fetches its egress policy through ActorEgressPolicy, both
// over TCP mTLS. Then it is proven that a worker asking for ANOTHER worker's
// actor is refused, and that an unauthorized peer is refused.
func TestTunnelNetworkPathMintsAndFetchesForItsOwnActor(t *testing.T) {
	ca := newTunnelNetworkCA(t)
	myActor := resources.ActorAttribution{Ref: resources.ActorRef{Atespace: "space", Name: "actor"}, UID: "actor-uid"}
	otherActor := resources.ActorAttribution{Ref: resources.ActorRef{Atespace: "space", Name: "other"}, UID: "other-uid"}
	policy := &ateletpb.EgressPolicy{Rules: []*ateletpb.EgressRule{{
		Https: &ateletpb.EgressDestinations{Hostnames: []string{"example.com"}},
	}}}

	const workerUID = "worker-uid"
	address, _ := startNetworkAtelet(t, ca, installdefaults.WorkerServiceAccount,
		map[string]resources.ActorRef{workerUID: myActor.Ref}, policy)

	dir := t.TempDir()
	workerBundle := filepath.Join(dir, "worker.pem")
	trustPath := filepath.Join(dir, "trust.pem")
	ca.writeBundle(t, workerBundle, &substratex509.PodIdentity{
		Namespace: "ate-demo", ServiceAccountName: installdefaults.WorkerServiceAccount, ServiceAccountUID: "worker-sa-uid",
		PodName: "worker", PodUID: workerUID, NodeName: testNodeName, NodeUID: testNodeUID,
	}, "spiffe://cluster.local/ns/ate-demo/sa/"+installdefaults.WorkerServiceAccount,
		[]x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth})
	if err := os.WriteFile(trustPath, ca.certPEM, 0o600); err != nil {
		t.Fatal(err)
	}

	tun := networkTunnel(t, address, workerBundle, trustPath)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Mint for ITS actor over the network, through PrepareEgress.
	gateway := &ateworkerpb.EgressGateway{Address: "gateway.example:443"}
	actorEgress, err := tun.PrepareEgress(ctx, myActor, gateway)
	if err != nil || actorEgress == nil {
		t.Fatalf("PrepareEgress for its own actor over the network = %v, %v; want a prepared egress", actorEgress, err)
	}

	// Fetch ITS actor's egress policy over the network, through ActorEgressPolicy.
	got, err := tun.ActorEgressPolicy(myActor).EgressPolicy(ctx)
	if err != nil {
		t.Fatalf("ActorEgressPolicy for its own actor over the network: %v", err)
	}
	if got == nil || len(got.GetRules()) != 1 {
		t.Fatalf("egress policy = %v, want the one-rule policy", got)
	}

	// Another worker's actor is refused for a MINT. This is production-faithful:
	// cmd/atelet's MintActorCertificate takes the worker identity from the mTLS
	// peer certificate and the control plane mints only for the actor assigned
	// to that Worker, so a worker cannot mint for an actor it does not host.
	// The harness enforces the same binding, keyed on the authenticated Pod UID.
	if _, err := tun.PrepareEgress(ctx, otherActor, gateway); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("PrepareEgress for another worker's actor = %v, want PermissionDenied", err)
	}
	// For the POLICY fetch, the harness also refuses another worker's actor, but
	// this models DESIRED behavior, not current production: cmd/atelet's
	// GetActorEgressPolicy carries a TODO(bowei) and does not yet bind the actor
	// to the authenticated worker (a weakness identical over Unix and TCP, which
	// this patch neither introduces nor worsens). The assertion documents the
	// binding we want the handler to enforce; it is not a claim that production
	// refuses this today.
	if _, err := tun.ActorEgressPolicy(otherActor).EgressPolicy(ctx); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("ActorEgressPolicy for another worker's actor (harness-enforced desired binding) = %v, want PermissionDenied", err)
	}
}

// TestTunnelNetworkPathRefusesUnauthorizedPeer proves the server-side authorizer
// gates the network path: a worker whose Pod UID is not in the
// atelet's live inventory cannot mint or fetch, so the TCP path is not a weaker
// door than the Unix socket a colocated worker reaches.
func TestTunnelNetworkPathRefusesUnauthorizedPeer(t *testing.T) {
	ca := newTunnelNetworkCA(t)
	actor := resources.ActorAttribution{Ref: resources.ActorRef{Atespace: "space", Name: "actor"}, UID: "actor-uid"}
	// The atelet knows only "known-worker"; the other worker below is "stranger".
	address, harness := startNetworkAtelet(t, ca, installdefaults.WorkerServiceAccount,
		map[string]resources.ActorRef{"known-worker": actor.Ref}, &ateletpb.EgressPolicy{})

	dir := t.TempDir()
	trustPath := filepath.Join(dir, "trust.pem")
	if err := os.WriteFile(trustPath, ca.certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	// Both workers share the service account, SPIFFE SAN and node. Only the Pod
	// UID differs, so the only thing that can tell them apart is the
	// live-inventory Authorize in the atelet's ServerTLS PeerPolicy.
	workerTunnel := func(podUID string) *Tunnel {
		bundle := filepath.Join(dir, podUID+".pem")
		ca.writeBundle(t, bundle, &substratex509.PodIdentity{
			Namespace: "ate-demo", ServiceAccountName: installdefaults.WorkerServiceAccount, ServiceAccountUID: podUID + "-sa-uid",
			PodName: podUID, PodUID: podUID, NodeName: testNodeName, NodeUID: testNodeUID,
		}, "spiffe://cluster.local/ns/ate-demo/sa/"+installdefaults.WorkerServiceAccount,
			[]x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth})
		return networkTunnel(t, address, bundle, trustPath)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	gateway := &ateworkerpb.EgressGateway{Address: "gateway.example:443"}

	// Control: the known worker is served over the same listener, so a refusal
	// below cannot come from a wrong address or a broken listener.
	if _, err := workerTunnel("known-worker").PrepareEgress(ctx, actor, gateway); err != nil {
		t.Fatalf("PrepareEgress by the known worker = %v; want it served", err)
	}

	// The Authorize check runs during the mTLS handshake, before the per-RPC
	// authorizer, so the refusal reaches the client as an Unavailable transport
	// error. With TLS 1.3 the client can see it as a bad-certificate alert, a
	// broken pipe, a reset or an EOF depending on timing, so the test does not
	// match the text. It checks the code and that no handler ran for the stranger.
	assertRefused := func(what string, err error) {
		t.Helper()
		if err == nil {
			t.Fatalf("%s by an unauthorized peer succeeded; want a refusal", what)
		}
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("%s by an unauthorized peer = %v; want an Unavailable transport error", what, err)
		}
	}
	stranger := workerTunnel("stranger")
	_, err := stranger.PrepareEgress(ctx, actor, gateway)
	assertRefused("PrepareEgress", err)
	_, err = stranger.ActorEgressPolicy(actor).EgressPolicy(ctx)
	assertRefused("ActorEgressPolicy", err)
	if harness.served("stranger") {
		t.Fatal("the atelet ran a handler for the unauthorized peer; the handshake should have refused it")
	}
	if !harness.served("known-worker") {
		t.Fatal("the control worker never reached a handler; the harness is not recording")
	}
}

// TestDialAteletPrefersAddressOverSocket proves the dial-site routing both
// ways directly on dialAtelet, the shared helper the credential broker and the
// egress-policy fetch both use. With an address set, it must dial the TCP
// atelet and IGNORE the socket path entirely (so a network worker never falls
// back to a socket it cannot reach): a deliberately bogus socket path, which
// would fail if it were dialed, still reaches the TCP server. With no address,
// it dials the Unix socket. The TLS config is the same for both -- the network
// path adds a transport, not a weaker trust check.
func TestDialAteletPrefersAddressOverSocket(t *testing.T) {
	ca := newTunnelNetworkCA(t)
	const workerUID = "worker-uid"
	actor := resources.ActorRef{Atespace: "space", Name: "actor"}
	address, harness := startNetworkAtelet(t, ca, installdefaults.WorkerServiceAccount,
		map[string]resources.ActorRef{workerUID: actor}, &ateletpb.EgressPolicy{})

	dir := t.TempDir()
	workerBundle := filepath.Join(dir, "worker.pem")
	trustPath := filepath.Join(dir, "trust.pem")
	ca.writeBundle(t, workerBundle, &substratex509.PodIdentity{
		Namespace: "ate-demo", ServiceAccountName: installdefaults.WorkerServiceAccount, ServiceAccountUID: "worker-sa-uid",
		PodName: "worker", PodUID: workerUID, NodeName: testNodeName, NodeUID: testNodeUID,
	}, "spiffe://cluster.local/ns/ate-demo/sa/"+installdefaults.WorkerServiceAccount,
		[]x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth})
	if err := os.WriteFile(trustPath, ca.certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	tlsConfig, err := ateletdial.TLSConfig(workerBundle, trustPath, installdefaults.AteletSPIFFEID(installdefaults.SystemNamespace))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Address set, with a bogus socket path that would fail if ever dialed:
	// the call must reach the TCP atelet regardless.
	conn, err := dialAtelet("/nonexistent/atelet/worker-support.sock", address, tlsConfig)
	if err != nil {
		t.Fatalf("dialAtelet with an address: %v", err)
	}
	defer conn.Close()
	if _, err := ateletpb.NewWorkerSupportClient(conn).GetActorEgressPolicy(ctx,
		&ateletpb.GetActorEgressPolicyRequest{ActorAtespace: actor.Atespace, ActorName: actor.Name}); err != nil {
		t.Fatalf("address-preferred dial did not reach the TCP atelet: %v", err)
	}
	_ = harness

	// No address: the Unix socket is used. A missing socket must fail at dial
	// time rather than silently reaching the network, which is the colocated
	// contract.
	unixConn, err := dialAtelet("/nonexistent/atelet/worker-support.sock", "", tlsConfig)
	if err != nil {
		// grpc.NewClient is lazy, so construction may succeed; the RPC must fail.
		return
	}
	defer unixConn.Close()
	if _, err := ateletpb.NewWorkerSupportClient(unixConn).GetActorEgressPolicy(ctx,
		&ateletpb.GetActorEgressPolicyRequest{ActorAtespace: actor.Atespace, ActorName: actor.Name}); err == nil {
		t.Fatal("colocated dial to a missing socket unexpectedly succeeded; the socket path must be used, not the network")
	}
}

// tunnelNetworkCA issues Pod certificates with a SPIFFE SAN and the matching
// pod-identity extension, as workertransport.ServerTLS/ateletdial require.
type tunnelNetworkCA struct {
	cert    *x509.Certificate
	key     *ecdsa.PrivateKey
	certPEM []byte
}

func newTunnelNetworkCA(t *testing.T) *tunnelNetworkCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(now.UnixNano()),
		Subject:               pkix.Name{CommonName: "tunnel network test CA"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return &tunnelNetworkCA{cert: cert, key: key, certPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

// writeBundle issues a credential bundle (leaf key + cert, then the CA) for
// identity with the given SPIFFE SAN and key usages, written to path.
func (ca *tunnelNetworkCA) writeBundle(t *testing.T, path string, identity *substratex509.PodIdentity, spiffeID string, usages []x509.ExtKeyUsage) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	uri, err := url.Parse(spiffeID)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(now.UnixNano()),
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  usages,
		URIs:         []*url.URL{uri},
	}
	if err := substratex509.AddPodIdentityToCertificate(identity, template); err != nil {
		t.Fatal(err)
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	bundle := append(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), ca.certPEM...)
	bundle = append(bundle, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})...)
	if err := os.WriteFile(path, bundle, 0o600); err != nil {
		t.Fatal(err)
	}
}
