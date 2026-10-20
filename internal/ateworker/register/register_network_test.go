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
	"crypto/tls"
	"testing"

	"go.ate.dev/substrate/internal/proto/ateletpb"
)

// TestDialAteletRoutesByAddress proves the network dial branch:
// dialAtelet dials the atelet-workers Service over TCP mTLS when Address is set,
// and keeps the node-local Unix-socket path when it is empty. The two paths are
// distinguishable by their address validation: the TCP path
// (workertransport.Dial) requires a host:port and refuses a bare socket path or
// a nil TLS config, while the Unix path (ateletdial.Dial) builds a lazy
// connection for any socket-path string.
//
// This is the default-unchanged guard for the credential path: with Address
// empty a colocated worker keeps the Unix socket exactly as upstream, and a
// network worker (Address set) never silently falls back to a Unix socket it
// cannot reach.
func TestDialAteletRoutesByAddress(t *testing.T) {
	// A bare, non-nil config is enough to prove routing: both dialers build a
	// lazy connection and only workertransport.Dial inspects the address shape
	// up front. The config is never used to handshake in this test.
	cfg := &tls.Config{MinVersion: tls.VersionTLS13}

	// Network mode: a valid host:port is accepted by the TCP path.
	if _, err := dialAtelet("/run/ate/worker-support.sock", "atelet-workers.ate-system.svc:8087", cfg); err != nil {
		t.Fatalf("network dial of a valid host:port errored: %v", err)
	}
	// Network mode: a bare socket path is not host:port, so the TCP path
	// rejects it. The Unix path would have accepted it, so this proves the TCP
	// branch was taken when Address is set.
	if _, err := dialAtelet("ignored", "/run/ate/worker-support.sock", cfg); err == nil {
		t.Fatal("the TCP path accepted a non-host:port address; the network branch must reject it")
	}
	// Network mode must not fall back to plaintext: a nil config is refused by
	// workertransport.Dial.
	if _, err := dialAtelet("ignored", "atelet-workers.ate-system.svc:8087", nil); err == nil {
		t.Fatal("the TCP path accepted a nil TLS config; the network branch must refuse it")
	}

	// Colocated mode: with Address empty the Unix path accepts a socket path and
	// returns a lazy connection, and the TCP branch is never taken.
	conn, err := dialAtelet("/run/ate/worker-support.sock", "", cfg)
	if err != nil {
		t.Fatalf("colocated Unix dial errored: %v", err)
	}
	if conn != nil {
		conn.Close()
	}
}

// TestAttributesFromProtoRoundTrips proves the wire-to-file attribute
// conversion the network runtimes source relies on: a populated runtime maps to
// its attribute map, and an unset/empty runtime maps to a nil map, which sync
// reads as "no default runtime yet" (so the worker waits and retries, exactly
// as for an absent file). Both directions matter: if an empty runtime mapped to
// a non-nil map, a network worker could register a runtime with no attributes.
func TestAttributesFromProtoRoundTrips(t *testing.T) {
	in := &ateletpb.SandboxRuntimeAttributes{Attributes: []*ateletpb.AttributeEntry{
		{Key: "architecture", Value: "amd64"},
		{Key: "sandbox_runtime_hash", Value: "sha256:aaa"},
	}}
	got := attributesFromProto(in)
	if got["architecture"] != "amd64" || got["sandbox_runtime_hash"] != "sha256:aaa" || len(got) != 2 {
		t.Fatalf("attributesFromProto = %v; want the two entries", got)
	}

	if got := attributesFromProto(nil); got != nil {
		t.Fatalf("attributesFromProto(nil) = %v; want nil (treated as not-ready)", got)
	}
	if got := attributesFromProto(&ateletpb.SandboxRuntimeAttributes{}); got != nil {
		t.Fatalf("attributesFromProto(empty) = %v; want nil (treated as not-ready)", got)
	}
}
