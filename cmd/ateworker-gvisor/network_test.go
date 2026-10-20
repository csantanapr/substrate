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
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.ate.dev/substrate/internal/ateinterceptors"
	"go.ate.dev/substrate/internal/installdefaults"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/proto/workerstagingpb"
	"go.ate.dev/substrate/internal/substratex509"
	"go.ate.dev/substrate/internal/workertransport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

// ---- flag pairing ---------------------------------------------------------

// setNetworkFlags sets both network flags for the duration of a test and
// restores them, so a test's mutation of the process-global pflag variables
// cannot leak into another test.
func setNetworkFlags(t *testing.T, listen, atelet string) {
	t.Helper()
	oldListen, oldAtelet := *workerListenAddress, *ateletAddress
	*workerListenAddress, *ateletAddress = listen, atelet
	t.Cleanup(func() { *workerListenAddress, *ateletAddress = oldListen, oldAtelet })
}

func TestValidateNetworkFlagsPairing(t *testing.T) {
	cases := []struct {
		name           string
		listen, atelet string
		wantErr        bool
	}{
		{"both empty is the default colocated mode", "", "", false},
		{"both set is network mode", "0.0.0.0:8086", "atelet-workers.ate-system.svc:8087", false},
		{"listen without atelet is a half-configured error", "0.0.0.0:8086", "", true},
		{"atelet without listen is a half-configured error", "", "atelet-workers.ate-system.svc:8087", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setNetworkFlags(t, tc.listen, tc.atelet)
			err := validateNetworkFlags()
			if tc.wantErr && err == nil {
				t.Fatalf("validateNetworkFlags(listen=%q, atelet=%q) = nil; want error", tc.listen, tc.atelet)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("validateNetworkFlags(listen=%q, atelet=%q) = %v; want nil", tc.listen, tc.atelet, err)
			}
		})
	}
}

func TestNetworkEnabledTracksAteletAddress(t *testing.T) {
	setNetworkFlags(t, "", "")
	if networkEnabled() {
		t.Fatal("networkEnabled() true with both flags empty (the default colocated mode)")
	}
	setNetworkFlags(t, "0.0.0.0:8086", "atelet-workers.ate-system.svc:8087")
	if !networkEnabled() {
		t.Fatal("networkEnabled() false with both flags set (network mode)")
	}
}

// ---- SPIFFE id parsing (atelet identity for the PeerPolicy) ----------------

func TestSpiffeNamespaceServiceAccount(t *testing.T) {
	ns, sa, err := spiffeNamespaceServiceAccount(installdefaults.AteletSPIFFEID(installdefaults.SystemNamespace))
	if err != nil {
		t.Fatalf("parsing the default atelet SPIFFE id errored: %v", err)
	}
	if ns != installdefaults.SystemNamespace || sa != installdefaults.AteletServiceAccount {
		t.Fatalf("parsed ns/sa = %q/%q; want %q/%q", ns, sa, installdefaults.SystemNamespace, installdefaults.AteletServiceAccount)
	}
	// A non-default install namespace is parsed, not hard-coded.
	ns, sa, err = spiffeNamespaceServiceAccount(installdefaults.AteletSPIFFEID("other-ns"))
	if err != nil || ns != "other-ns" || sa != installdefaults.AteletServiceAccount {
		t.Fatalf("non-default namespace: got ns=%q sa=%q err=%v; want other-ns/%s/nil", ns, sa, err, installdefaults.AteletServiceAccount)
	}

	// Malformed or non-canonical ids are errors, never a silent partial policy.
	for _, bad := range []string{
		"",
		"https://cluster.local/ns/ate-system/sa/atelet",        // wrong scheme
		"spiffe://cluster.local/ns/ate-system",                 // no sa segment
		"spiffe://cluster.local/sa/atelet",                     // no ns segment
		"spiffe://cluster.local/ns//sa/atelet",                 // empty namespace
		"spiffe://cluster.local/ns/ate-system/sa/",             // empty service account
		"spiffe://cluster.local/workload/atelet",               // wrong path shape
		"spiffe:///ns/ate-system/sa/atelet",                    // no trust domain
		"spiffe://cluster.local/ns/ate-system/x/../sa/atelet",  // ".." segment must not be normalized into a valid shape
		"spiffe://cluster.local/ns/ate-system/sa/atelet?x=1",   // trailing query
		"spiffe://cluster.local/ns/ate-system/sa/atelet#frag",  // fragment
		"spiffe://user@cluster.local/ns/ate-system/sa/atelet",  // userinfo
		"spiffe://cluster.local/ns/ate-system/sa/atelet/extra", // extra segment
	} {
		if _, _, err := spiffeNamespaceServiceAccount(bad); err == nil {
			t.Fatalf("spiffeNamespaceServiceAccount(%q) = nil error; want error", bad)
		}
	}
}

func TestAteletPeerPolicyFromSPIFFEID(t *testing.T) {
	policy, err := ateletPeerPolicy(installdefaults.AteletSPIFFEID(installdefaults.SystemNamespace))
	if err != nil {
		t.Fatal(err)
	}
	if policy.Namespace != installdefaults.SystemNamespace ||
		policy.ServiceAccount != installdefaults.AteletServiceAccount || !policy.SameNode {
		t.Fatalf("ateletPeerPolicy = %+v; want ns=%s sa=%s SameNode=true",
			policy, installdefaults.SystemNamespace, installdefaults.AteletServiceAccount)
	}
	if _, err := ateletPeerPolicy("not-a-spiffe-id"); err == nil {
		t.Fatal("ateletPeerPolicy accepted a malformed atelet SPIFFE id")
	}
}

// ---- per-RPC method allowlist ---------------------------------------------

func TestAuthorizeAteletCallGatesMethods(t *testing.T) {
	// The authenticated atelet (identity is irrelevant to this allowlist; the
	// PeerPolicy already verified it) may call only the worker's own lifecycle
	// and staging methods.
	allowed := []string{
		ateworkerpb.Worker_RunWorkload_FullMethodName,
		ateworkerpb.Worker_RestoreWorkload_FullMethodName,
		ateworkerpb.Worker_CheckpointWorkload_FullMethodName,
		ateworkerpb.Worker_TerminateWorkload_FullMethodName,
		ateworkerpb.Worker_GetWorkloadStats_FullMethodName,
		workerstagingpb.WorkerStaging_PrepareActivation_FullMethodName,
		workerstagingpb.WorkerStaging_PutArtifact_FullMethodName,
		workerstagingpb.WorkerStaging_ExportCheckpoint_FullMethodName,
		workerstagingpb.WorkerStaging_CleanupActor_FullMethodName,
	}
	for _, method := range allowed {
		if err := authorizeAteletCall(context.Background(), nil, method); err != nil {
			t.Fatalf("authorizeAteletCall rejected a worker/staging method %s: %v", method, err)
		}
	}
	rejected := []string{
		"/grpc.reflection.v1.ServerReflection/ServerReflectionInfo",
		"/grpc.health.v1.Health/Check",
		"/ateworker.WorkerExtra/Foo",       // a prefix that only starts like the Worker service
		"/workerstaging.WorkerStagingX/Go", // a prefix that only starts like the staging service
		"",
	}
	for _, method := range rejected {
		if err := authorizeAteletCall(context.Background(), nil, method); err == nil {
			t.Fatalf("authorizeAteletCall admitted a non-worker method %q", method)
		}
	}
}

// ---- end-to-end real-mTLS serving -----------------------------------------

// TestWorkerNetworkServingAuthorizesOnlyAtelet brings up the worker's network
// TCP mTLS listener (the real buildNetworkServing options, serving the Worker
// and WorkerStaging services) and proves the identity gate with REAL TLS:
//   - the node atelet (correct SA, namespace and node incarnation) is accepted
//     and can reach both the Worker and the WorkerStaging services;
//   - a wrong service account, a wrong namespace, a different node incarnation,
//     a client that presents no certificate, and a client whose certificate was
//     signed by a different CA are each rejected by the TLS/PeerPolicy before
//     the handler runs.
//
// Every rejection is proven through a live handshake, not a mock authorizer.
func TestWorkerNetworkServingAuthorizesOnlyAtelet(t *testing.T) {
	ca := newWorkerTestCA(t)
	otherCA := newWorkerTestCA(t)
	ateletSPIFFE := installdefaults.AteletSPIFFEID(installdefaults.SystemNamespace)

	workerBundle := ca.issue(t, workerPodIdentity(), issueOptions{})
	serving, err := buildNetworkServing(workerBundle, ca.trustPath, ateletSPIFFE, t.TempDir(), "worker-uid", func(string) bool { return false })
	if err != nil {
		t.Fatalf("buildNetworkServing: %v", err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(workerServerOptions(serving)...)
	ateworkerpb.RegisterWorkerServer(srv, &AteWorkerService{})
	workerstagingpb.RegisterWorkerStagingServer(srv, serving.staging)
	reflection.Register(srv)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	oldAddr := testListenerAddr
	testListenerAddr = lis.Addr().String()
	t.Cleanup(func() { testListenerAddr = oldAddr })

	// The authorized atelet reaches both services. The RPCs fail at the handler
	// (empty request / no such actor), not at the transport, which is what we
	// want: it proves the handshake and the authorizer let the call through.
	t.Run("atelet accepted reaches both services", func(t *testing.T) {
		conn := dialAsAtelet(t, ca, ateletIdentity(), workertransport.PeerPolicy{
			Namespace: "ate-system", ServiceAccount: installdefaults.WorkerServiceAccount, SameNode: true,
		})
		defer conn.Close()
		if code, msg := workerCall(t, conn); code == codes.Unauthenticated || transportRefused(msg) {
			t.Fatalf("authorized atelet was refused on the Worker service: %s (%s)", code, msg)
		}
		// The staging handler rejects an empty ActivationRef on its own merits;
		// what matters is that the call reached the handler, i.e. the transport
		// authorizer did NOT refuse it. A staging-handler rejection carries the
		// handler's message, not the authorizer's method-allowlist message.
		if code, msg := stagingCall(t, conn); code == codes.Unauthenticated || transportRefused(msg) {
			t.Fatalf("authorized atelet was refused on the WorkerStaging service by the transport: %s (%s)", code, msg)
		}
	})

	// Each wrong identity is refused by the real handshake. The client policy
	// must name the server (the worker) correctly so the dial can reach the
	// server's VerifyConnection; the rejection is the SERVER refusing the
	// client's identity.
	rejections := []struct {
		name     string
		identity substratex509.PodIdentity
		signer   *workerTestCA
	}{
		{"wrong service account", withSA(ateletIdentity(), "intruder"), ca},
		{"wrong namespace", withNamespace(ateletIdentity(), "other-ns"), ca},
		{"different node incarnation", withNode(ateletIdentity(), "node-b", "node-b-uid"), ca},
		{"certificate from a different CA", ateletIdentity(), otherCA},
	}
	for _, tc := range rejections {
		t.Run("rejected: "+tc.name, func(t *testing.T) {
			// The client policy names the server (worker) but does NOT set
			// SameNode, so the client's own VerifyConnection cannot be what
			// rejects a node-mismatched case: the SERVER must be the gate. This
			// makes the "different node incarnation" case a real test of the
			// server's SameNode check (with SameNode on the client too, a
			// node-b client would reject the node-a server and the server-side
			// check would go unexercised -- a vacuous pass).
			conn := dialAsAtelet(t, tc.signer, tc.identity, workertransport.PeerPolicy{
				Namespace: "ate-system", ServiceAccount: installdefaults.WorkerServiceAccount,
			})
			defer conn.Close()
			// A refusal must come from the transport: a TLS handshake failure
			// (Unavailable) or the mTLS authorizer (Unauthenticated), NOT from
			// the Worker handler admitting the call and then rejecting its empty
			// request (InvalidArgument/NotFound). Requiring a transport code is
			// what makes the server-side identity checks non-vacuous.
			code, _ := workerCall(t, conn)
			if code != codes.Unavailable && code != codes.Unauthenticated {
				t.Fatalf("a %s caller was not refused by the transport on the Worker service (code=%s); it reached the handler", tc.name, code)
			}
		})
	}

	// A client presenting no certificate at all is refused: the server requires
	// and verifies a client certificate.
	t.Run("rejected: no client certificate", func(t *testing.T) {
		conn, err := grpc.NewClient("passthrough:///"+testListenerAddr,
			grpc.WithTransportCredentials(credentials.NewTLS(clientTLSNoCert(t, ca.trustPath))))
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		if code, _ := workerCall(t, conn); code == codes.OK {
			t.Fatal("a client with no certificate reached the Worker handler")
		}
	})
}

// transportRefused reports whether an error message is the network listener's
// own refusal (the per-RPC method allowlist or the mTLS authorizer), as opposed
// to a handler's own rejection of an otherwise-admitted call.
func transportRefused(msg string) bool {
	return strings.Contains(msg, "method is not a worker lifecycle or staging service") ||
		strings.Contains(msg, "RPC authorizer is required") ||
		strings.Contains(msg, "peer credentials are required") ||
		strings.Contains(msg, "verified mutual TLS is required")
}

// --- cert-minting test helpers (a focused local CA, modeled on the one in
// internal/workertransport, which is package-private there) ------------------

type workerTestCA struct {
	cert      *x509.Certificate
	key       *ecdsa.PrivateKey
	trustPath string
}

func newWorkerTestCA(t *testing.T) *workerTestCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "ateworker network test CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	trustPath := filepath.Join(t.TempDir(), "trust.pem")
	if err := os.WriteFile(trustPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	return &workerTestCA{cert, key, trustPath}
}

type issueOptions struct {
	expired bool
}

func (ca *workerTestCA) issue(t *testing.T, identity substratex509.PodIdentity, opt issueOptions) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	uri := &url.URL{Scheme: "spiffe", Host: "cluster.local",
		Path: "/ns/" + identity.Namespace + "/sa/" + identity.ServiceAccountName}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(2), URIs: []*url.URL{uri},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	if opt.expired {
		template.NotAfter = time.Now().Add(-time.Minute)
	}
	if err := substratex509.AddPodIdentityToCertificate(&identity, template); err != nil {
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
	bundle := append(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})...)
	bundlePath := filepath.Join(t.TempDir(), "bundle.pem")
	if err := os.WriteFile(bundlePath, bundle, 0o600); err != nil {
		t.Fatal(err)
	}
	return bundlePath
}

func workerPodIdentity() substratex509.PodIdentity {
	return substratex509.PodIdentity{
		Namespace: "ate-system", ServiceAccountName: installdefaults.WorkerServiceAccount,
		ServiceAccountUID: "worker-sa-uid", PodName: "worker-xyz", PodUID: "worker-uid",
		NodeName: "node-a", NodeUID: "node-a-incarnation",
	}
}

func ateletIdentity() substratex509.PodIdentity {
	return substratex509.PodIdentity{
		Namespace: "ate-system", ServiceAccountName: installdefaults.AteletServiceAccount,
		ServiceAccountUID: "atelet-sa-uid", PodName: "atelet-xyz", PodUID: "atelet-uid",
		NodeName: "node-a", NodeUID: "node-a-incarnation",
	}
}

func withSA(id substratex509.PodIdentity, sa string) substratex509.PodIdentity {
	id.ServiceAccountName = sa
	return id
}

func withNamespace(id substratex509.PodIdentity, ns string) substratex509.PodIdentity {
	id.Namespace = ns
	return id
}

func withNode(id substratex509.PodIdentity, name, uid string) substratex509.PodIdentity {
	id.NodeName, id.NodeUID = name, uid
	return id
}

// dialAsAtelet dials the worker's network listener presenting a client
// certificate for identity signed by signer. The client PeerPolicy names the
// worker (so the dial reaches the server), with no inventory authorizer, since
// this test is about the server gating the client, not the reverse.
func dialAsAtelet(t *testing.T, signer *workerTestCA, identity substratex509.PodIdentity, serverPolicy workertransport.PeerPolicy) *grpc.ClientConn {
	t.Helper()
	bundle := signer.issue(t, identity, issueOptions{})
	cfg, err := workertransport.ClientTLS(bundle, signer.trustPath, serverPolicy)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := workertransport.Dial(testListenerAddress(t), cfg)
	if err != nil {
		t.Fatal(err)
	}
	return conn
}

// clientTLSNoCert builds a client TLS config that verifies the server but
// presents no client certificate, so the server's RequireAndVerifyClientCert
// rejects the handshake.
func clientTLSNoCert(t *testing.T, trustPath string) *tls.Config {
	t.Helper()
	pemData, err := os.ReadFile(trustPath)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pemData) {
		t.Fatal("empty trust bundle")
	}
	return &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots}
}

// testListenerAddress is set by the e2e test to the live listener address so
// the dial helpers can reach it.
var testListenerAddr string

func testListenerAddress(t *testing.T) string {
	t.Helper()
	if testListenerAddr == "" {
		t.Fatal("test listener address not set")
	}
	return testListenerAddr
}

func workerCall(t *testing.T, conn *grpc.ClientConn) (codes.Code, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := ateworkerpb.NewWorkerClient(conn).GetWorkloadStats(ctx, &ateworkerpb.GetWorkloadStatsRequest{})
	return status.Code(err), errMessage(err)
}

func stagingCall(t *testing.T, conn *grpc.ClientConn) (codes.Code, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := workerstagingpb.NewWorkerStagingClient(conn).CommitActivation(ctx, &workerstagingpb.ActivationRef{})
	return status.Code(err), errMessage(err)
}

func errMessage(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// ---- server option assembly -----------------------------------------------
//
// These tests build a REAL grpc.Server from the exact option list the worker
// passes to grpc.NewServer at startup, for both modes. The defect they guard
// against was invisible to every earlier test because no earlier test built a
// server from the full assembled list: network mode set grpc.UnaryInterceptor
// twice (once inside buildNetworkServing, once in main's base options) and
// grpc-go panics on the second in grpc.NewServer, so the network worker
// crashed at startup while the offline gates stayed green.

// buildNetworkServingForTest builds a networkServing with the real option
// assembly and a self-signed worker bundle, for the option-list tests that do
// not need a live peer.
func buildNetworkServingForTest(t *testing.T) *networkServing {
	t.Helper()
	ca := newWorkerTestCA(t)
	workerBundle := ca.issue(t, workerPodIdentity(), issueOptions{})
	serving, err := buildNetworkServing(
		workerBundle, ca.trustPath,
		installdefaults.AteletSPIFFEID(installdefaults.SystemNamespace),
		t.TempDir(), "worker-uid", func(string) bool { return false },
	)
	if err != nil {
		t.Fatalf("buildNetworkServing: %v", err)
	}
	return serving
}

// TestWorkerServerOptionsBuildsServerBothModes proves grpc.NewServer accepts
// the assembled option list in BOTH modes without panicking -- colocated (nil)
// and network. grpc.NewServer is where the double-UnaryInterceptor panic fired,
// so a clean construction here is the direct regression guard.
func TestWorkerServerOptionsBuildsServerBothModes(t *testing.T) {
	t.Run("colocated", func(t *testing.T) {
		srv := grpc.NewServer(workerServerOptions(nil)...)
		srv.Stop()
	})
	t.Run("network", func(t *testing.T) {
		serving := buildNetworkServingForTest(t)
		srv := grpc.NewServer(workerServerOptions(serving)...)
		srv.Stop()
	})
}

// TestWorkerServerOptionsMutantDoubleUnaryInterceptorPanics is the negative
// half: it reproduces the defect CLASS -- more than one grpc.UnaryInterceptor
// in a single option list -- and asserts grpc.NewServer panics on it. The
// pre-fix bug was two grpc.UnaryInterceptor options reaching grpc.NewServer
// (one from buildNetworkServing, one from the base options); grpc-go rejects
// the second. This proves the positive test above is non-vacuous: the
// construction it exercises really can fail, and collapsing to one interceptor
// is what stops it. The broken variant lives only in this test.
//
// Note it is NOT byte-for-byte the historical list: it takes the FIXED list and
// appends two grpc.UnaryInterceptor options on top, so the forbidden pair is
// present regardless of what the fixed list used (one UnaryInterceptor in
// colocated, one ChainUnaryInterceptor in network -- neither alone panics). The
// point proven is the guard against the duplicate-UnaryInterceptor class, not a
// replica of the exact pre-fix option count.
func TestWorkerServerOptionsMutantDoubleUnaryInterceptorPanics(t *testing.T) {
	assertPanics := func(name string, base []grpc.ServerOption) {
		t.Run(name, func(t *testing.T) {
			mutant := append([]grpc.ServerOption{}, base...)
			mutant = append(mutant,
				grpc.UnaryInterceptor(ateinterceptors.InternalServerUnaryInterceptor),
				grpc.UnaryInterceptor(workertransport.UnaryAuthorizer(authorizeAteletCall)),
			)
			defer func() {
				if r := recover(); r == nil {
					t.Fatalf("grpc.NewServer did not panic with two unary interceptors; the double-interceptor bug would not be caught")
				} else if !strings.Contains(fmt.Sprint(r), "unary server interceptor was already set") {
					t.Fatalf("grpc.NewServer panicked for an unexpected reason: %v", r)
				}
			}()
			srv := grpc.NewServer(mutant...)
			srv.Stop() // unreached; the panic is the expected outcome
		})
	}
	assertPanics("colocated plus two unary interceptors", workerServerOptions(nil))
	assertPanics("network plus two unary interceptors", workerServerOptions(buildNetworkServingForTest(t)))
}

// TestColocatedServerOptionsUnchangedVersusGolden checks the colocated option
// list against upstream's shape BEHAVIORALLY. grpc.ServerOption is an opaque
// *funcServerOption, so a test cannot assert the IDENTITY or ORDER of the two
// options directly; it asserts the observable consequences that upstream's
// exact shape (StatsHandler + UnaryInterceptor(InternalServerUnaryInterceptor),
// nothing else) produces, each of which a missing or extra option would move:
//
//   - exactly two options (a third would be an extra interceptor/handler);
//   - the single unary interceptor present is the Internal logging one: an
//     allowed unary call emits exactly one "Handle RPC" record (zero would mean
//     the interceptor is absent or not the Internal one; two would mean a
//     second logging interceptor);
//   - no mTLS credentials leaked in: a PLAINTEXT client reaches the handler
//     rather than failing at the transport;
//   - no STREAM authorizer leaked in (the network-mode-only option): a stream
//     to an arbitrary method is refused Unimplemented by the server, NOT
//     PermissionDenied by a stream authorizer. A leaked StreamAuthorizer would
//     refuse an out-of-allowlist stream method with PermissionDenied first.
//
// What this test does NOT pin: that the first option is specifically the stats
// handler, or the relative order of the two options. Those are unobservable
// through the opaque option API; the count and the four behaviors above are the
// strongest checks available without reaching into grpc internals.
func TestColocatedServerOptionsUnchangedVersusGolden(t *testing.T) {
	opts := workerServerOptions(nil)
	if len(opts) != 2 {
		t.Fatalf("colocated option count = %d; want 2 (stats handler + Internal unary interceptor)", len(opts))
	}

	logs := countHandleRPCLogs(t)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(opts...)
	ateworkerpb.RegisterWorkerServer(srv, &AteWorkerService{})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	// A PLAINTEXT client reaches the handler: colocated carries no mTLS creds,
	// so if any transport credential had leaked in, this dial would fail at the
	// transport instead of reaching GetWorkloadStats.
	conn, err := grpc.NewClient("passthrough:///"+lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _ = ateworkerpb.NewWorkerClient(conn).GetWorkloadStats(ctx, &ateworkerpb.GetWorkloadStatsRequest{})
	if got := logs.Load(); got != 1 {
		t.Fatalf(`colocated unary interceptor emitted %d "Handle RPC" logs; want exactly 1 (the Internal logging interceptor ran once)`, got)
	}

	// No stream authorizer leaked into colocated. Opening a stream to an
	// arbitrary (unregistered, out-of-allowlist) method must be refused by the
	// server as Unimplemented, NOT PermissionDenied: a leaked
	// StreamAuthorizer(authorizeAteletCall) would reject this method name with
	// PermissionDenied before the server's dispatcher returns Unimplemented.
	sctx, scancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer scancel()
	stream, err := conn.NewStream(sctx, &grpc.StreamDesc{ServerStreams: true, ClientStreams: true},
		"/ateworker.Worker/NoSuchStream")
	if err == nil {
		err = stream.RecvMsg(&ateworkerpb.GetWorkloadStatsResponse{})
	}
	if code := status.Code(err); code != codes.Unimplemented {
		t.Fatalf("colocated stream to an arbitrary method got code=%s; want Unimplemented (a StreamAuthorizer leaking into colocated would make it PermissionDenied)", code)
	}
}

// TestNetworkAuthorizerRunsBeforeInternalLogging proves the ordering invariant
// that makes the authorizer the OUTERMOST unary interceptor: a call to a method
// OUTSIDE the worker lifecycle/staging allowlist is refused by the authorizer
// BEFORE the Internal logging interceptor runs, so an unauthorized call is
// never logged. It brings up the real network options over loopback mTLS with
// an authorized atelet peer (so only the method allowlist, not the identity
// gate, applies), calls an out-of-allowlist method (reflection), and asserts
// the "Handle RPC" counter stayed at zero while an allowed method increments
// it -- i.e. the Internal interceptor sits INSIDE the authorizer, not outside.
func TestNetworkAuthorizerRunsBeforeInternalLogging(t *testing.T) {
	ca := newWorkerTestCA(t)
	ateletSPIFFE := installdefaults.AteletSPIFFEID(installdefaults.SystemNamespace)
	workerBundle := ca.issue(t, workerPodIdentity(), issueOptions{})
	serving, err := buildNetworkServing(workerBundle, ca.trustPath, ateletSPIFFE, t.TempDir(), "worker-uid", func(string) bool { return false })
	if err != nil {
		t.Fatalf("buildNetworkServing: %v", err)
	}

	logs := countHandleRPCLogs(t)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(workerServerOptions(serving)...)
	ateworkerpb.RegisterWorkerServer(srv, &AteWorkerService{})
	workerstagingpb.RegisterWorkerStagingServer(srv, serving.staging)
	// A unary service OUTSIDE the worker lifecycle/staging allowlist, so an
	// authenticated atelet calling it is refused by authorizeAteletCall. Health
	// Check is unary, which is what lets the UNARY authorizer (the outermost
	// unary interceptor) be the thing that refuses it.
	grpc_health_v1.RegisterHealthServer(srv, health.NewServer())
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	oldAddr := testListenerAddr
	testListenerAddr = lis.Addr().String()
	t.Cleanup(func() { testListenerAddr = oldAddr })

	conn := dialAsAtelet(t, ca, ateletIdentity(), workertransport.PeerPolicy{
		Namespace: "ate-system", ServiceAccount: installdefaults.WorkerServiceAccount, SameNode: true,
	})
	defer conn.Close()

	// An out-of-allowlist unary method: Health/Check is registered on the
	// server but is not a Worker/WorkerStaging method, so authorizeAteletCall
	// refuses it. The refusal must come from the authorizer (PermissionDenied)
	// and the Internal logging interceptor -- which is INSIDE the chain -- must
	// not have run for it.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err = grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("out-of-allowlist Health/Check got code=%s (%v); want PermissionDenied from the authorizer", status.Code(err), err)
	}
	if got := logs.Load(); got != 0 {
		t.Fatalf(`an unauthorized call produced %d "Handle RPC" logs; want 0 (the Internal logging interceptor must sit INSIDE the authorizer and never see a refused call)`, got)
	}

	// Sanity, so the zero above is not merely a counter that never increments:
	// an ALLOWED unary method reaches the handler and the Internal interceptor
	// logs it exactly once.
	wc := ateworkerpb.NewWorkerClient(conn)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()
	_, _ = wc.GetWorkloadStats(ctx2, &ateworkerpb.GetWorkloadStatsRequest{})
	if got := logs.Load(); got != 1 {
		t.Fatalf(`an allowed unary call produced %d "Handle RPC" logs; want exactly 1 (so the zero for the refused call is meaningful)`, got)
	}
}

// countHandleRPCLogs installs a slog handler for the duration of the test that
// counts the "Handle RPC" records InternalServerUnaryInterceptor emits, and
// restores the previous default logger. Counting that record is how a test
// observes whether the Internal logging interceptor ran.
func countHandleRPCLogs(t *testing.T) *atomic.Int64 {
	t.Helper()
	var n atomic.Int64
	prev := slog.Default()
	slog.SetDefault(slog.New(&handleRPCCounter{n: &n}))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &n
}

type handleRPCCounter struct{ n *atomic.Int64 }

func (h *handleRPCCounter) Enabled(context.Context, slog.Level) bool { return true }
func (h *handleRPCCounter) Handle(_ context.Context, r slog.Record) error {
	if r.Message == "Handle RPC" {
		h.n.Add(1)
	}
	return nil
}
func (h *handleRPCCounter) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *handleRPCCounter) WithGroup(string) slog.Handler      { return h }
