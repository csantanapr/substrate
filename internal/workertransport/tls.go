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

// Package workertransport authenticates optional network connections between a
// node atelet and workers that do not share its filesystem.
//
// The node atelet serves its WorkerSupport service to colocated workers over a
// Unix socket (see cmd/atelet) with mTLS and a verifyClientOnSameNode peer
// check: the worker is a process that shares the node's filesystem namespace.
// A separate-guest worker runs in a separate Pod and cannot reach that socket, so
// this package carries the network path: the same mTLS trust, moved
// onto a Pod-to-Pod TCP connection whose peer is identified by its certificate
// rather than by a shared socket. Because there is no shared socket, the peer
// certificate plus the PeerPolicy below ARE the authorization; there is no
// weaker fallback. No plaintext and no Unix fallback is performed.
package workertransport

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/url"
	"os"
	"path"
	"time"

	"go.ate.dev/substrate/internal/credbundle"
	"go.ate.dev/substrate/internal/installdefaults"
	"go.ate.dev/substrate/internal/substratex509"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// PeerPolicy identifies the permitted role and, when known, the exact Pod.
// A node listener that accepts workers from several namespaces must supply
// Authorize to check each identity against its Kubernetes worker inventory.
type PeerPolicy struct {
	Namespace      string
	ServiceAccount string
	PodUID         string
	SameNode       bool
	Authorize      func(*substratex509.PodIdentity) error
}

func (p PeerPolicy) validate() error {
	if p.ServiceAccount == "" || (p.Namespace == "" && p.Authorize == nil) {
		return fmt.Errorf("peer service account and namespace or inventory authorizer are required")
	}
	return nil
}

func (p PeerPolicy) check(leaf *x509.Certificate, local *substratex509.PodIdentity) error {
	identity, err := substratex509.PodIdentityFromCertificate(leaf)
	if err != nil || identity == nil || identity.PodUID == "" ||
		identity.Namespace == "" || identity.ServiceAccountName == "" ||
		identity.NodeName == "" || identity.NodeUID == "" {
		return fmt.Errorf("peer certificate has no complete Pod identity")
	}
	// The pod-identity extension and the SPIFFE SAN are issued together by the
	// pod-cert controller (cmd/podcertcontroller/internal/podidentitysigner):
	// spiffe://<trust-domain>/ns/<namespace>/sa/<service-account>. Requiring
	// them to agree rejects a certificate whose SAN was minted for a different
	// service account than its pod-identity extension claims.
	expectedURI := (&url.URL{Scheme: "spiffe", Host: installdefaults.AteletTrustDomain,
		Path: path.Join("ns", identity.Namespace, "sa", identity.ServiceAccountName)}).String()
	if len(leaf.URIs) != 1 || leaf.URIs[0].String() != expectedURI {
		return fmt.Errorf("peer SPIFFE identity differs from its Pod identity")
	}
	if identity.ServiceAccountName != p.ServiceAccount ||
		(p.Namespace != "" && identity.Namespace != p.Namespace) {
		return fmt.Errorf("peer is not the permitted service account")
	}
	if p.PodUID != "" && identity.PodUID != p.PodUID {
		return fmt.Errorf("peer is not the selected worker Pod")
	}
	if p.SameNode && (identity.NodeName != local.NodeName || identity.NodeUID != local.NodeUID) {
		return fmt.Errorf("peer is not on this node incarnation")
	}
	if p.Authorize != nil {
		return p.Authorize(identity)
	}
	return nil
}

func material(bundlePath, trustPath string) (*substratex509.PodIdentity, *x509.CertPool, error) {
	if bundlePath == "" || trustPath == "" {
		return nil, nil, fmt.Errorf("pod credential bundle and trust bundle are required")
	}
	bundle, err := credbundle.Parse(bundlePath)
	if err != nil {
		return nil, nil, fmt.Errorf("load Pod credential bundle: %w", err)
	}
	local, err := substratex509.PodIdentityFromCertificate(bundle.Leaf)
	if err != nil || local == nil || local.PodUID == "" || local.NodeName == "" || local.NodeUID == "" {
		return nil, nil, fmt.Errorf("local certificate has no complete Pod identity")
	}
	pem, err := os.ReadFile(trustPath)
	if err != nil {
		return nil, nil, fmt.Errorf("read Pod trust bundle: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		return nil, nil, fmt.Errorf("empty Pod trust bundle")
	}
	return local, roots, nil
}

// ClientTLS verifies the chain, SPIFFE role, Pod UID and node relationship even
// when discovery supplies a Pod IP rather than a certificate DNS name.
func ClientTLS(bundlePath, trustPath string, policy PeerPolicy) (*tls.Config, error) {
	if err := policy.validate(); err != nil {
		return nil, err
	}
	local, roots, err := material(bundlePath, trustPath)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		MinVersion: tls.VersionTLS13,
		// Pod certificates carry SPIFFE identities instead of IP/DNS SANs.
		// VerifyConnection performs both full chain and identity verification.
		InsecureSkipVerify:   true, //nolint:gosec // chain + identity verified in VerifyConnection below
		GetClientCertificate: credbundle.ClientLoader(bundlePath),
		VerifyConnection: func(state tls.ConnectionState) error {
			if len(state.PeerCertificates) == 0 {
				return fmt.Errorf("peer certificate is required")
			}
			intermediates := x509.NewCertPool()
			for _, cert := range state.PeerCertificates[1:] {
				intermediates.AddCert(cert)
			}
			leaf := state.PeerCertificates[0]
			if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates,
				KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}); err != nil {
				return fmt.Errorf("verify peer chain: %w", err)
			}
			return policy.check(leaf, local)
		},
	}, nil
}

// ServerTLS requires a trusted client certificate and the explicitly allowed
// caller role. Credential-file rotation is picked up on the next handshake.
func ServerTLS(bundlePath, trustPath string, policy PeerPolicy) (*tls.Config, error) {
	if err := policy.validate(); err != nil {
		return nil, err
	}
	local, roots, err := material(bundlePath, trustPath)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		MinVersion:     tls.VersionTLS13,
		GetCertificate: credbundle.Loader(bundlePath),
		ClientAuth:     tls.RequireAndVerifyClientCert,
		ClientCAs:      roots,
		VerifyConnection: func(state tls.ConnectionState) error {
			if len(state.VerifiedChains) == 0 || len(state.PeerCertificates) == 0 {
				return fmt.Errorf("verified peer certificate is required")
			}
			return policy.check(state.PeerCertificates[0], local)
		},
	}, nil
}

// Identity returns the authenticated caller, never a caller-supplied UID.
// Rechecking lifetime prevents an expired long-lived connection from starting
// new authorized operations.
func Identity(ctx context.Context) (*substratex509.PodIdentity, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "peer credentials are required")
	}
	info, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || len(info.State.VerifiedChains) == 0 || len(info.State.PeerCertificates) == 0 {
		return nil, status.Error(codes.Unauthenticated, "verified mutual TLS is required")
	}
	leaf := info.State.PeerCertificates[0]
	now := time.Now()
	if now.Before(leaf.NotBefore) || !now.Before(leaf.NotAfter) {
		return nil, status.Error(codes.Unauthenticated, "peer certificate expired or is not yet valid")
	}
	identity, err := substratex509.PodIdentityFromCertificate(leaf)
	if err != nil || identity == nil || identity.PodUID == "" {
		return nil, status.Error(codes.Unauthenticated, "peer Pod identity is required")
	}
	return identity, nil
}

// Authorizer rechecks live Pod/assignment authority for each RPC, including
// calls on connections established before a worker was deleted or reassigned.
type Authorizer func(context.Context, *substratex509.PodIdentity, string) error

// UnaryAuthorizer runs the authorizer before every unary handler. A nil
// authorizer fails closed: no RPC is served without an explicit decision.
func UnaryAuthorizer(authorize Authorizer) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		identity, err := Identity(ctx)
		if err != nil {
			return nil, err
		}
		if authorize == nil {
			return nil, status.Error(codes.PermissionDenied, "RPC authorizer is required")
		}
		if err := authorize(ctx, identity, info.FullMethod); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// StreamAuthorizer runs the authorizer before every stream handler. A nil
// authorizer fails closed.
func StreamAuthorizer(authorize Authorizer) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		identity, err := Identity(stream.Context())
		if err != nil {
			return err
		}
		if authorize == nil {
			return status.Error(codes.PermissionDenied, "RPC authorizer is required")
		}
		if err := authorize(stream.Context(), identity, info.FullMethod); err != nil {
			return err
		}
		return handler(srv, stream)
	}
}
