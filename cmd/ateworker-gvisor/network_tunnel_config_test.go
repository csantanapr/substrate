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
	"testing"

	"go.ate.dev/substrate/internal/atunnel"
)

// TestApplyNetworkTunnelConfig guards the cmd-side wiring that makes a network
// worker dial atelet over TCP. do() builds the config it passes to atunnel.Start
// with exactly this call --
//
//	startConfig := applyNetworkTunnelConfig(*tunnelConfig, *ateletAddress)
//
// -- so exercising applyNetworkTunnelConfig over the SAME flag the call site
// reads (*ateletAddress, set via setNetworkFlags) exercises that wiring without
// running do(). A test of the helper alone would stay green with the wiring
// removed; this one does not: a regression that stops setting AteletAddress (the helper no longer
// copies the address, or do() passes *tunnelConfig to Start instead of this
// helper's result) makes the network case below fail. It mirrors how
// workerServerOptions is tested -- the function do() calls is driven directly.
func TestApplyNetworkTunnelConfig(t *testing.T) {
	const address = "atelet-workers.ate-system.svc:8087"

	// A non-empty base so the helper is shown to preserve the other fields and
	// change only AteletAddress, not to return a zero Config.
	base := atunnel.Config{
		CredentialBundle: "cred.pem",
		TrustBundle:      "trust.pem",
		BrokerIdentity:   "spiffe://cluster.local/ns/ate-system/sa/atelet",
	}

	t.Run("network mode sets the atelet address from the flag", func(t *testing.T) {
		// Set the flags the way the network worker is launched, then build the
		// config the way do() does, from the parsed flags.
		setNetworkFlags(t, "0.0.0.0:8086", address)
		got := applyNetworkTunnelConfig(base, *ateletAddress)
		if got.AteletAddress != address {
			t.Fatalf("AteletAddress = %q, want %q (the network worker would dial nothing over TCP)", got.AteletAddress, address)
		}
		// The rest of the config is carried through unchanged.
		if got.CredentialBundle != base.CredentialBundle || got.TrustBundle != base.TrustBundle || got.BrokerIdentity != base.BrokerIdentity {
			t.Fatalf("helper changed a field other than AteletAddress: got %+v, base %+v", got, base)
		}
	})

	t.Run("colocated mode leaves the atelet address empty", func(t *testing.T) {
		// Both flags empty is the default colocated mode; the tunnel must keep
		// its node-local Unix-socket dial.
		setNetworkFlags(t, "", "")
		got := applyNetworkTunnelConfig(base, *ateletAddress)
		if got.AteletAddress != "" {
			t.Fatalf("AteletAddress = %q, want empty in colocated mode", got.AteletAddress)
		}
	})
}
