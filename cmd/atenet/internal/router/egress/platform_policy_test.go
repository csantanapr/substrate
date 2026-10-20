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
	"net/netip"
	"os"
	"testing"

	envoy_type "github.com/envoyproxy/go-control-plane/envoy/type/v3"

	"go.ate.dev/substrate/internal/egresspolicy"
)

// dest builds a normalized CONNECT destination the way NormalizeAuthority does.
func dest(t *testing.T, ipPort string) egresspolicy.Destination {
	t.Helper()
	d, err := egresspolicy.NormalizeAuthority(ipPort)
	if err != nil {
		t.Fatalf("NormalizeAuthority(%q) error = %v", ipPort, err)
	}
	return d
}

// TestPlatformDestinationAllowedAbsentVariable proves the legacy case: with no
// variable set at all, the gate passes (the per-actor policy applies alone). It
// uses os.Unsetenv with manual restore rather than t.Setenv, because t.Setenv
// cannot express "absent" and mixing it with os.Unsetenv in one test is refused.
func TestPlatformDestinationAllowedAbsentVariable(t *testing.T) {
	old, had := os.LookupEnv(platformEgressDestinationsVar)
	if err := os.Unsetenv(platformEgressDestinationsVar); err != nil {
		t.Fatalf("Unsetenv: %v", err)
	}
	t.Cleanup(func() {
		if had {
			_ = os.Setenv(platformEgressDestinationsVar, old)
		} else {
			_ = os.Unsetenv(platformEgressDestinationsVar)
		}
	})
	if !platformDestinationAllowed(dest(t, "10.0.0.5:80")) {
		t.Error("with no platform profile set, the gate must pass (legacy-allow)")
	}
}

// TestPlatformDestinationAllowed proves the exact-IP:port allow-list both ways
// at the unit boundary: an empty variable denies all, and a configured list
// admits only an exact canonical match.
func TestPlatformDestinationAllowed(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		dest    egresspolicy.Destination
		allowed bool
	}{
		{name: "empty variable denies all", value: "", dest: dest(t, "10.0.0.5:80"), allowed: false},
		{name: "exact match allowed", value: "10.0.0.5:80", dest: dest(t, "10.0.0.5:80"), allowed: true},
		{name: "different port denied", value: "10.0.0.5:80", dest: dest(t, "10.0.0.5:81"), allowed: false},
		{name: "different ip denied", value: "10.0.0.5:80", dest: dest(t, "10.0.0.6:80"), allowed: false},
		{name: "one of several allowed", value: "10.0.0.5:80,10.0.0.6:443", dest: dest(t, "10.0.0.6:443"), allowed: true},
		{name: "spaces in list are trimmed", value: "10.0.0.5:80, 10.0.0.6:443", dest: dest(t, "10.0.0.6:443"), allowed: true},
		{name: "not in the list denied", value: "10.0.0.5:80,10.0.0.6:443", dest: dest(t, "10.0.0.7:80"), allowed: false},
		// A destination the normalizer could not resolve to an IP literal + port
		// is refused when a profile is in force: the profile lists only literals.
		{name: "no ip with profile set is denied", value: "10.0.0.5:80", dest: egresspolicy.Destination{Port: 80}, allowed: false},
		{name: "zero port with profile set is denied", value: "10.0.0.5:80", dest: egresspolicy.Destination{IP: netip.MustParseAddr("10.0.0.5")}, allowed: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(platformEgressDestinationsVar, tc.value)
			if got := platformDestinationAllowed(tc.dest); got != tc.allowed {
				t.Errorf("platformDestinationAllowed(%v) with %q = %v, want %v",
					tc.dest, tc.value, got, tc.allowed)
			}
		})
	}
}

// TestHandleConnectEnforcesPlatformProfile proves the gate end to end through
// the real CONNECT handler. The handler is built with allowAllPolicy(), so the
// per-actor egress policy permits the dialed IP:port on its own; the ONLY thing
// that can refuse it is the platform profile. That makes a change removing the
// gate from handleConnect visibly fail the "denied" case rather than pass
// vacuously.
func TestHandleConnectEnforcesPlatformProfile(t *testing.T) {
	ca := newTestCA(t, "actor-identity-ca")
	leaf := ca.issueActorCert(t, "spiffe://substrate-actor.local/worker-for-actor/foo/bar", actorCertOptions{})

	// egressMetadata dials 93.184.216.34:80 by default.
	const dialed = "93.184.216.34:80"

	t.Run("allowed destination opens the tunnel", func(t *testing.T) {
		t.Setenv(platformEgressDestinationsVar, dialed)
		h := egressHandler(ca.loadRoots, runningActor(), nil)
		res, err := h.HandleRequestHeaders(context.Background(), egressMetadata(xfccHeader(leaf)))
		if err != nil {
			t.Fatalf("HandleRequestHeaders() error = %v, want the tunnel to open for an allowed destination", err)
		}
		if res.Response == nil {
			t.Fatal("HandleRequestHeaders() returned no response for an allowed destination")
		}
	})

	t.Run("destination outside the profile is refused", func(t *testing.T) {
		// A profile that lists a different destination: the per-actor policy
		// still allows 93.184.216.34:80, so a Forbidden here is the platform
		// gate and nothing else.
		t.Setenv(platformEgressDestinationsVar, "10.1.2.3:80")
		h := egressHandler(ca.loadRoots, runningActor(), nil)
		_, err := h.HandleRequestHeaders(context.Background(), egressMetadata(xfccHeader(leaf)))
		wantStatus(t, err, envoy_type.StatusCode_Forbidden)
	})

	t.Run("empty profile denies the dialed destination", func(t *testing.T) {
		t.Setenv(platformEgressDestinationsVar, "")
		h := egressHandler(ca.loadRoots, runningActor(), nil)
		_, err := h.HandleRequestHeaders(context.Background(), egressMetadata(xfccHeader(leaf)))
		wantStatus(t, err, envoy_type.StatusCode_Forbidden)
	})
}
