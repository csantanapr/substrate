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

package register

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
	"path/filepath"
	"testing"
	"time"

	"go.ate.dev/substrate/internal/ateletdial"
	"go.ate.dev/substrate/internal/installdefaults"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/substratex509"
	"go.ate.dev/substrate/internal/workertransport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeWorkerSupport is a minimal atelet WorkerSupport server that serves the
// network runtimes seam (GetSandboxRuntimes), hands out an epoch, and records
// what a worker registers (RegisterWorker) and reports
// (SetWorkerSandboxRuntimes). It is enough to drive the worker's network
// register flow end to end over real TCP mTLS.
type fakeWorkerSupport struct {
	ateletpb.UnimplementedWorkerSupportServer
	defaultRuntime *ateletpb.SandboxRuntimeAttributes
	registered     chan *ateletpb.RegisterWorkerRequest
	runtimes       chan *ateletpb.SetWorkerSandboxRuntimesRequest
}

func (f *fakeWorkerSupport) GetSandboxRuntimes(context.Context, *ateletpb.GetSandboxRuntimesRequest) (*ateletpb.GetSandboxRuntimesResponse, error) {
	return &ateletpb.GetSandboxRuntimesResponse{SandboxClasses: []*ateletpb.SandboxRuntimesClass{{
		SandboxClass:   "gvisor",
		DefaultRuntime: f.defaultRuntime,
	}}}, nil
}

func (f *fakeWorkerSupport) AcquireWorkerEpoch(context.Context, *ateletpb.AcquireWorkerEpochRequest) (*ateletpb.AcquireWorkerEpochResponse, error) {
	return &ateletpb.AcquireWorkerEpochResponse{Epoch: 1}, nil
}

func (f *fakeWorkerSupport) RegisterWorker(_ context.Context, req *ateletpb.RegisterWorkerRequest) (*ateletpb.RegisterWorkerResponse, error) {
	select {
	case f.registered <- req:
	default:
	}
	return &ateletpb.RegisterWorkerResponse{}, nil
}

func (f *fakeWorkerSupport) SetWorkerSandboxRuntimes(_ context.Context, req *ateletpb.SetWorkerSandboxRuntimesRequest) (*ateletpb.SetWorkerSandboxRuntimesResponse, error) {
	select {
	case f.runtimes <- req:
	default:
	}
	return &ateletpb.SetWorkerSandboxRuntimesResponse{}, nil
}

// TestReportNetworkModeFetchesRuntimesAndRegisters is the end-to-end test the
// End-to-end: the worker, in network mode, over loopback TCP mTLS,
//   - fetches its sandbox runtimes from a fake atelet WorkerSupport server
//     (the delivery seam a network worker needs because it has no shared file),
//   - and registers them back through the same authenticated connection.
//
// It proves the whole network register path (dialAtelet -> fetchRuntimes ->
// registerOnce -> SetWorkerSandboxRuntimes) works over a real mutually-
// authenticated connection, and that the reported runtimes are the ones the
// server served, under the worker's own sandbox class.
func TestReportNetworkModeFetchesRuntimesAndRegisters(t *testing.T) {
	ateletSPIFFE := installdefaults.AteletSPIFFEID(installdefaults.SystemNamespace)

	// The fake atelet serves a default runtime; the worker should fetch it and
	// register it under its own class ("gvisor").
	fake := &fakeWorkerSupport{
		defaultRuntime: &ateletpb.SandboxRuntimeAttributes{Attributes: []*ateletpb.AttributeEntry{
			{Key: "architecture", Value: "amd64"},
			{Key: "sandbox_runtime_hash", Value: "sha256:aaa"},
		}},
		registered: make(chan *ateletpb.RegisterWorkerRequest, 1),
		runtimes:   make(chan *ateletpb.SetWorkerSandboxRuntimesRequest, 1),
	}

	ca := newRegTestCA(t)
	// The server (atelet) presents the atelet identity; its PeerPolicy admits
	// only the worker service account on the same node.
	ateletBundle := ca.issue(t, regIdentity("ate-system", installdefaults.AteletServiceAccount, "atelet-uid"))
	serverTLS, err := workertransport.ServerTLS(ateletBundle, ca.trustPath, workertransport.PeerPolicy{
		Namespace: "ate-system", ServiceAccount: installdefaults.WorkerServiceAccount, SameNode: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(grpc.Creds(workertransport.RefreshingCredentials(serverTLS)))
	ateletpb.RegisterWorkerSupportServer(srv, fake)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	// The worker presents the worker identity; it verifies the atelet by its
	// SPIFFE id and node incarnation (ateletdial.TLSConfig, built inside Report).
	workerBundle := ca.issue(t, regIdentity("ate-system", installdefaults.WorkerServiceAccount, "worker-uid"))

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- Report(ctx, ReportConfig{
			Address:              lis.Addr().String(),
			CredentialBundlePath: workerBundle,
			TrustBundlePath:      ca.trustPath,
			AteletSPIFFEID:       ateletSPIFFE,
			Actors:               7,
			SandboxClass:         "gvisor",
			PodIPs:               []string{"10.0.0.1"},
			// Required by Report, but never read in network mode (no file).
			RuntimesFile: filepath.Join(t.TempDir(), "unused.json"),
		}, &Epoch{})
	}()

	select {
	case req := <-fake.registered:
		if req.GetCapacity().GetActors() != 7 {
			t.Errorf("registered actor capacity = %d; want 7", req.GetCapacity().GetActors())
		}
	case <-ctx.Done():
		t.Fatalf("worker did not register over the network within the deadline")
	}
	select {
	case req := <-fake.runtimes:
		classes := req.GetSandboxClasses()
		if len(classes) != 1 || classes[0].GetSandboxClass() != "gvisor" {
			t.Fatalf("reported sandbox classes = %v; want one gvisor class (the worker's own)", classes)
		}
		attrs := map[string]string{}
		for _, a := range classes[0].GetDefaultRuntime().GetVersion().GetAttributes() {
			attrs[a.GetKey()] = a.GetValue()
		}
		if attrs["architecture"] != "amd64" || attrs["sandbox_runtime_hash"] != "sha256:aaa" {
			t.Errorf("reported runtime attributes = %v; want the ones the atelet served", attrs)
		}
	case <-ctx.Done():
		t.Fatalf("worker did not report its runtimes over the network within the deadline")
	}
	cancel()
	<-errCh // Report returns ctx.Err() after cancellation.
}

// TestReportNetworkModeRejectedByWrongServerIdentity proves the worker refuses
// to register against a server that is not the atelet it expects: a server
// presenting a different SPIFFE id fails the worker's ateletdial verification,
// so the worker never sends its runtimes to it. This is the client-side half
// of the identity gate (the server-side half is proved in the ateworker-gvisor
// package's serving test).
func TestReportNetworkModeRejectedByWrongServerIdentity(t *testing.T) {
	ca := newRegTestCA(t)
	// The server presents the ROUTER identity, not atelet's.
	badBundle := ca.issue(t, regIdentity("ate-system", installdefaults.RouterServiceAccount, "router-uid"))
	serverTLS, err := workertransport.ServerTLS(badBundle, ca.trustPath, workertransport.PeerPolicy{
		Namespace: "ate-system", ServiceAccount: installdefaults.WorkerServiceAccount, SameNode: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(grpc.Creds(workertransport.RefreshingCredentials(serverTLS)))
	ateletpb.RegisterWorkerSupportServer(srv, &fakeWorkerSupport{
		defaultRuntime: &ateletpb.SandboxRuntimeAttributes{Attributes: []*ateletpb.AttributeEntry{{Key: "architecture", Value: "amd64"}}},
		registered:     make(chan *ateletpb.RegisterWorkerRequest, 1),
	})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	workerBundle := ca.issue(t, regIdentity("ate-system", installdefaults.WorkerServiceAccount, "worker-uid"))
	tlsConfig, err := ateletdial.TLSConfig(workerBundle, ca.trustPath, installdefaults.AteletSPIFFEID(installdefaults.SystemNamespace))
	if err != nil {
		t.Fatal(err)
	}

	// fetchRuntimes must fail: the server is not the expected atelet, so the
	// worker's client TLS rejects the handshake before any runtimes are read.
	_, err = fetchRuntimes(t.Context(), "", lis.Addr().String(), tlsConfig)
	if err == nil {
		t.Fatal("fetchRuntimes succeeded against a server that is not the expected atelet")
	}
	if code := status.Code(err); code != codes.Unavailable && code != codes.Unauthenticated {
		t.Fatalf("fetchRuntimes error code = %s; want a transport-level refusal", code)
	}
}

// --- a focused cert-minting CA for this package's tests --------------------

type regTestCA struct {
	cert      *x509.Certificate
	key       *ecdsa.PrivateKey
	trustPath string
}

func newRegTestCA(t *testing.T) *regTestCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "register test CA"},
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
	return &regTestCA{cert, key, trustPath}
}

func regIdentity(namespace, serviceAccount, podUID string) substratex509.PodIdentity {
	return substratex509.PodIdentity{
		Namespace: namespace, ServiceAccountName: serviceAccount, ServiceAccountUID: serviceAccount + "-uid",
		PodName: serviceAccount, PodUID: podUID, NodeName: "node-a", NodeUID: "node-a-incarnation",
	}
}

func (ca *regTestCA) issue(t *testing.T, identity substratex509.PodIdentity) string {
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

// crashSupport is an atelet WorkerSupport server that records crash reports.
type crashSupport struct {
	ateletpb.UnimplementedWorkerSupportServer
	crashed chan *ateletpb.ReportActorCrashedRequest
}

func (c *crashSupport) ReportActorCrashed(_ context.Context, req *ateletpb.ReportActorCrashedRequest) (*ateletpb.ReportActorCrashedResponse, error) {
	c.crashed <- req
	return &ateletpb.ReportActorCrashedResponse{}, nil
}

// TestCrashReporterNetworkModeReportsOverTCP drives the worker's real crash
// reporter in network mode (Address set) against an atelet WorkerSupport
// server over loopback TCP mTLS, the path a network worker reports a crashed
// sandbox on. atelet's side of that RPC is covered in cmd/atelet.
func TestCrashReporterNetworkModeReportsOverTCP(t *testing.T) {
	ca := newRegTestCA(t)
	ateletBundle := ca.issue(t, regIdentity("ate-system", installdefaults.AteletServiceAccount, "atelet-uid"))
	serverTLS, err := workertransport.ServerTLS(ateletBundle, ca.trustPath, workertransport.PeerPolicy{
		Namespace: "ate-system", ServiceAccount: installdefaults.WorkerServiceAccount, SameNode: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fake := &crashSupport{crashed: make(chan *ateletpb.ReportActorCrashedRequest, 1)}
	srv := grpc.NewServer(grpc.Creds(workertransport.RefreshingCredentials(serverTLS)))
	ateletpb.RegisterWorkerSupportServer(srv, fake)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	workerBundle := ca.issue(t, regIdentity("ate-system", installdefaults.WorkerServiceAccount, "worker-uid"))
	reporter, err := NewCrashReporter(ReportConfig{
		Address:              lis.Addr().String(),
		CredentialBundlePath: workerBundle,
		TrustBundlePath:      ca.trustPath,
		AteletSPIFFEID:       installdefaults.AteletSPIFFEID(installdefaults.SystemNamespace),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	actor := resources.ActorAttribution{Ref: resources.ActorRef{Atespace: "team", Name: "counter"}, UID: "actor-uid"}
	if err := reporter.ReportCrashed(ctx, actor, "sandbox exited"); err != nil {
		t.Fatalf("ReportCrashed over the network: %v", err)
	}
	select {
	case req := <-fake.crashed:
		if req.GetActorAtespace() != "team" || req.GetActorName() != "counter" || req.GetActorUid() != "actor-uid" || req.GetMessage() != "sandbox exited" {
			t.Errorf("crash report = %v, want team/counter actor-uid \"sandbox exited\"", req)
		}
	default:
		t.Fatal("ReportCrashed returned nil but the atelet server received no report")
	}
}
