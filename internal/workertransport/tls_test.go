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

package workertransport

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"go.ate.dev/substrate/internal/substratex509"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type testCA struct {
	cert      *x509.Certificate
	key       *ecdsa.PrivateKey
	trustPath string
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "transport test CA"},
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
	return &testCA{cert, key, trustPath}
}

func podIdentity(role, uid string) substratex509.PodIdentity {
	return substratex509.PodIdentity{
		Namespace: "ate-system", ServiceAccountName: role, ServiceAccountUID: role + "-sa",
		PodName: role, PodUID: uid, NodeName: "node-a", NodeUID: "node-a-uid",
	}
}

// issueOptions parameterizes the certificate the test CA mints so a test can
// mint both a well-formed credential and a specific malformation. The zero
// value is a valid, unexpired certificate whose SPIFFE SAN agrees with its
// pod-identity extension.
type issueOptions struct {
	expired bool
	expiry  time.Time
	// sanSAOverride, when non-empty, mints the SPIFFE SAN with this service
	// account instead of the pod-identity extension's, so the SAN and the
	// extension disagree (the case the PeerPolicy SAN check must reject).
	sanSAOverride string
	// omitSAN mints a certificate with no SPIFFE URI SAN at all.
	omitSAN bool
	// extraSAN adds a second SPIFFE URI SAN, so len(leaf.URIs) != 1.
	extraSAN bool
}

func (ca *testCA) issue(t *testing.T, identity substratex509.PodIdentity, opt issueOptions) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	sanSA := identity.ServiceAccountName
	if opt.sanSAOverride != "" {
		sanSA = opt.sanSAOverride
	}
	uri, err := url.Parse("spiffe://cluster.local/ns/" + identity.Namespace + "/sa/" + sanSA)
	if err != nil {
		t.Fatal(err)
	}
	var uris []*url.URL
	if !opt.omitSAN {
		uris = append(uris, uri)
	}
	if opt.extraSAN {
		uris = append(uris, &url.URL{Scheme: "spiffe", Host: "cluster.local", Path: "/ns/ate-system/sa/second"})
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(2), URIs: uris,
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	if !opt.expiry.IsZero() {
		template.NotAfter = opt.expiry
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

// loadLeaf parses the leaf certificate out of a PEM bundle written by
// testCA.issue, so a test can drive PeerPolicy.check directly.
func loadLeaf(t *testing.T, bundlePath string) (*x509.Certificate, error) {
	t.Helper()
	data, err := os.ReadFile(bundlePath)
	if err != nil {
		return nil, err
	}
	for {
		var block *pem.Block
		block, data = pem.Decode(data)
		if block == nil {
			break
		}
		if block.Type == "CERTIFICATE" {
			return x509.ParseCertificate(block.Bytes)
		}
	}
	t.Fatalf("no certificate in bundle %q", bundlePath)
	return nil, nil
}

func healthServer(t *testing.T, config *tls.Config, authorize Authorizer) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.Creds(RefreshingCredentials(config)),
		grpc.UnaryInterceptor(UnaryAuthorizer(authorize)))
	grpc_health_v1.RegisterHealthServer(server, health.NewServer())
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return listener.Addr().String()
}

func healthCall(t *testing.T, address string, config *tls.Config) error {
	t.Helper()
	conn, err := Dial(address, config)
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	_, err = grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	return err
}

func TestMutualTLSRolesAndLiveAuthorization(t *testing.T) {
	ca := newTestCA(t)
	serverBundle := ca.issue(t, podIdentity("ate-worker", "worker-1"), issueOptions{})
	serverTLS, err := ServerTLS(serverBundle, ca.trustPath, PeerPolicy{
		Namespace: "ate-system", ServiceAccount: "atelet", SameNode: true})
	if err != nil {
		t.Fatal(err)
	}
	var revoked atomic.Bool
	address := healthServer(t, serverTLS, func(_ context.Context, identity *substratex509.PodIdentity, method string) error {
		if identity.ServiceAccountName != "atelet" || method != grpc_health_v1.Health_Check_FullMethodName {
			return status.Error(codes.PermissionDenied, "wrong caller or method")
		}
		if revoked.Load() {
			return status.Error(codes.PermissionDenied, "assignment revoked")
		}
		return nil
	})
	policy := PeerPolicy{Namespace: "ate-system", ServiceAccount: "ate-worker", PodUID: "worker-1", SameNode: true}
	for _, tc := range []struct {
		name      string
		role      string
		nodeUID   string
		expired   bool
		untrusted bool
		wantOK    bool
	}{
		{name: "node atelet", role: "atelet", nodeUID: "node-a-uid", wantOK: true},
		{name: "worker role", role: "ate-worker", nodeUID: "node-a-uid"},
		{name: "old node incarnation", role: "atelet", nodeUID: "old-node"},
		{name: "expired", role: "atelet", nodeUID: "node-a-uid", expired: true},
		{name: "untrusted", role: "atelet", nodeUID: "node-a-uid", untrusted: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			issuer := ca
			if tc.untrusted {
				issuer = newTestCA(t)
			}
			identity := podIdentity(tc.role, "caller")
			identity.NodeUID = tc.nodeUID
			config, err := ClientTLS(issuer.issue(t, identity, issueOptions{expired: tc.expired}), ca.trustPath, policy)
			if err != nil {
				t.Fatal(err)
			}
			err = healthCall(t, address, config)
			if (err == nil) != tc.wantOK {
				t.Fatalf("RPC error = %v; want success %v", err, tc.wantOK)
			}
		})
	}
	clientBundle := ca.issue(t, podIdentity("atelet", "helper-1"), issueOptions{})
	config, err := ClientTLS(clientBundle, ca.trustPath, policy)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := Dial(address, config)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := grpc_health_v1.NewHealthClient(conn)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if _, err := client.Check(ctx, &grpc_health_v1.HealthCheckRequest{}); err != nil {
		t.Fatal(err)
	}
	revoked.Store(true)
	if _, err := client.Check(ctx, &grpc_health_v1.HealthCheckRequest{}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("existing connection after revocation: %v", err)
	}
	revoked.Store(false)
	replacement := ca.issue(t, podIdentity("atelet", "helper-2"), issueOptions{})
	data, err := os.ReadFile(replacement)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(clientBundle+".new", data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(clientBundle+".new", clientBundle); err != nil {
		t.Fatal(err)
	}
	if err := healthCall(t, address, config); err != nil {
		t.Fatalf("rotated helper: %v", err)
	}
	policy.PodUID = "wrong-worker"
	wrong, err := ClientTLS(clientBundle, ca.trustPath, policy)
	if err != nil {
		t.Fatal(err)
	}
	if err := healthCall(t, address, wrong); err == nil {
		t.Fatal("accepted wrong worker UID")
	}
}

// TestPeerPolicySANAgreement proves the SAN check in
// PeerPolicy.check both ways: a certificate whose SPIFFE SAN agrees with its
// pod-identity extension is accepted, and three malformations are each
// rejected -- a SAN minted for a different service account than the extension
// claims (the forged-SAN case), no SAN at all, and a second SAN. A mint that
// could substitute a SAN for a different identity than the extension is exactly
// how a worker could present one identity to the chain and another to the
// policy, so each rejection is required, not cosmetic.
func TestPeerPolicySANAgreement(t *testing.T) {
	ca := newTestCA(t)
	policy := PeerPolicy{Namespace: "ate-system", ServiceAccount: "ate-worker", SameNode: true}
	local := podIdentity("atelet", "helper-1")
	for _, tc := range []struct {
		name   string
		opt    issueOptions
		wantOK bool
	}{
		{name: "SAN agrees with extension", opt: issueOptions{}, wantOK: true},
		{name: "SAN names a different service account", opt: issueOptions{sanSAOverride: "atelet"}},
		{name: "no SPIFFE SAN", opt: issueOptions{omitSAN: true}},
		{name: "two SPIFFE SANs", opt: issueOptions{extraSAN: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bundlePath := ca.issue(t, podIdentity("ate-worker", "worker-1"), tc.opt)
			bundle, err := loadLeaf(t, bundlePath)
			if err != nil {
				t.Fatal(err)
			}
			err = policy.check(bundle, &local)
			if (err == nil) != tc.wantOK {
				t.Fatalf("check error = %v; want success %v", err, tc.wantOK)
			}
		})
	}
}

func TestIdentityRequiresVerifiedTLSAndAuthorizer(t *testing.T) {
	if _, err := Identity(context.Background()); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("no peer identity: %v", err)
	}
	if _, err := Dial("node.example:8443", nil); err == nil {
		t.Fatal("nil TLS accepted")
	}
	for _, address := range []string{"", "http://node:8443", "unix:///tmp/socket", "node:0", "node:not-port"} {
		if _, err := Dial(address, &tls.Config{MinVersion: tls.VersionTLS13}); err == nil {
			t.Errorf("accepted malformed address %q", address)
		}
	}
}

// TestNilAuthorizerFailsClosed proves both interceptors deny every RPC when the
// authorizer is nil, over a real mTLS connection whose peer would otherwise be
// accepted. A regression that changed the nil check to fail open (serve the RPC)
// would make this test fail, which the pre-existing suite did not catch because
// it never drove an interceptor with a nil authorizer.
func TestNilAuthorizerFailsClosed(t *testing.T) {
	ca := newTestCA(t)
	serverBundle := ca.issue(t, podIdentity("ate-worker", "worker-1"), issueOptions{})
	serverTLS, err := ServerTLS(serverBundle, ca.trustPath, PeerPolicy{
		Namespace: "ate-system", ServiceAccount: "atelet", SameNode: true})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	// A nil authorizer on both interceptors: the only thing under test.
	server := grpc.NewServer(grpc.Creds(RefreshingCredentials(serverTLS)),
		grpc.UnaryInterceptor(UnaryAuthorizer(nil)),
		grpc.StreamInterceptor(StreamAuthorizer(nil)))
	grpc_health_v1.RegisterHealthServer(server, health.NewServer())
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	clientBundle := ca.issue(t, podIdentity("atelet", "helper-1"), issueOptions{})
	clientTLS, err := ClientTLS(clientBundle, ca.trustPath,
		PeerPolicy{Namespace: "ate-system", ServiceAccount: "ate-worker", PodUID: "worker-1", SameNode: true})
	if err != nil {
		t.Fatal(err)
	}
	conn, err := Dial(listener.Addr().String(), clientTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	client := grpc_health_v1.NewHealthClient(conn)
	// Unary interceptor path.
	if _, err := client.Check(ctx, &grpc_health_v1.HealthCheckRequest{}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("unary with nil authorizer: got %v, want PermissionDenied", err)
	}
	// Stream interceptor path.
	stream, err := client.Watch(ctx, &grpc_health_v1.HealthCheckRequest{})
	if err == nil {
		_, err = stream.Recv()
	}
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("stream with nil authorizer: got %v, want PermissionDenied", err)
	}
}

// TestIdentityRejectsExpiredCertificate proves Identity rejects an expired peer
// certificate independently of connection retirement: it builds a peer context
// directly with an expired (but structurally valid, verified-chain) leaf, so it
// pins the NotAfter check on the RPC path even for a connection that was never
// retired. Removing that lifetime check would make this fail.
func TestIdentityRejectsExpiredCertificate(t *testing.T) {
	ca := newTestCA(t)
	id := podIdentity("ate-worker", "worker-1")
	expired := ca.issue(t, id, issueOptions{expired: true})
	leaf, err := loadLeaf(t, expired)
	if err != nil {
		t.Fatal(err)
	}
	state := tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{leaf},
		// A non-empty VerifiedChains: the handshake verified the chain; the
		// certificate only expired afterwards, which Identity must still reject.
		VerifiedChains: [][]*x509.Certificate{{leaf}},
	}
	ctx := peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{State: state}})
	if _, err := Identity(ctx); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expired certificate: got %v, want Unauthenticated", err)
	}
	// Control: the same identity with a valid certificate is accepted, so the
	// rejection above is the expiry and not some unrelated parse failure.
	valid := ca.issue(t, id, issueOptions{})
	validLeaf, err := loadLeaf(t, valid)
	if err != nil {
		t.Fatal(err)
	}
	validState := tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{validLeaf},
		VerifiedChains:   [][]*x509.Certificate{{validLeaf}},
	}
	validCtx := peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{State: validState}})
	if _, err := Identity(validCtx); err != nil {
		t.Fatalf("valid certificate rejected: %v", err)
	}
}

func TestRetainedConnectionRefreshesRotatedCredentials(t *testing.T) {
	for _, side := range []string{"client", "server"} {
		t.Run(side, func(t *testing.T) {
			ca := newTestCA(t)
			expiry := time.Now().Add(3 * time.Second).Truncate(time.Second)
			clientID, serverID := podIdentity("atelet", "helper"), podIdentity("ate-worker", "worker")
			clientBundle, serverBundle := ca.issue(t, clientID, issueOptions{}), ca.issue(t, serverID, issueOptions{})
			if side == "client" {
				clientBundle = ca.issue(t, clientID, issueOptions{expiry: expiry})
			} else {
				serverBundle = ca.issue(t, serverID, issueOptions{expiry: expiry})
			}
			serverTLS, err := ServerTLS(serverBundle, ca.trustPath, PeerPolicy{Namespace: "ate-system", ServiceAccount: "atelet", SameNode: true})
			if err != nil {
				t.Fatal(err)
			}
			var handshakes atomic.Int32
			verify := serverTLS.VerifyConnection
			serverTLS.VerifyConnection = func(state tls.ConnectionState) error { handshakes.Add(1); return verify(state) }
			address := healthServer(t, serverTLS, func(context.Context, *substratex509.PodIdentity, string) error { return nil })
			clientTLS, err := ClientTLS(clientBundle, ca.trustPath, PeerPolicy{Namespace: "ate-system", ServiceAccount: "ate-worker", PodUID: "worker", SameNode: true})
			if err != nil {
				t.Fatal(err)
			}
			conn, err := Dial(address, clientTLS)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			client := grpc_health_v1.NewHealthClient(conn)
			check := func() error {
				ctx, cancel := context.WithTimeout(t.Context(), time.Second)
				defer cancel()
				_, err := client.Check(ctx, &grpc_health_v1.HealthCheckRequest{})
				return err
			}
			if err := check(); err != nil {
				t.Fatal(err)
			}
			destination, identity := clientBundle, clientID
			if side == "server" {
				destination, identity = serverBundle, serverID
			}
			replacement := ca.issue(t, identity, issueOptions{})
			data, err := os.ReadFile(replacement)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(destination+".new", data, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(destination+".new", destination); err != nil {
				t.Fatal(err)
			}
			// The retirement timer closes the old connection near the certificate's
			// NotAfter; the next Check then re-handshakes with the rotated
			// credential. Production reads the real clock for certificate validity,
			// so wait once for the certificate's own expiry instead of polling.
			deadline := expiry.Add(4 * time.Second)
			waitCtx, waitCancel := context.WithDeadline(t.Context(), deadline)
			defer waitCancel()
			expiryTimer := time.NewTimer(time.Until(expiry) + 100*time.Millisecond)
			defer expiryTimer.Stop()
			select {
			case <-expiryTimer.C:
			case <-waitCtx.Done():
				t.Fatalf("context expired before the certificate: %v", waitCtx.Err())
			}
			for {
				err := check()
				if handshakes.Load() >= 2 && err == nil {
					break
				}
				if waitCtx.Err() != nil {
					t.Fatalf("same ClientConn did not recover: handshakes=%d, error=%v", handshakes.Load(), err)
				}
				conn.WaitForStateChange(waitCtx, conn.GetState())
			}
		})
	}
}
