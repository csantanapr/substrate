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
	"net"
	"os"
	"strconv"
	"strings"

	"go.ate.dev/substrate/internal/egresspolicy"
)

// platformEgressDestinationsVar names the installation-wide platform egress
// profile. It is read every call rather than cached so an installer can change
// the allow-list by rolling the Deployment's env, with no code path that holds
// a stale copy.
const platformEgressDestinationsVar = "ATE_PLATFORM_EGRESS_DESTINATIONS"

// platformDestinationAllowed is an opt-in installation policy enforced in front
// of the per-actor egress policy, for the private platform gateway only. When
// the variable is set, a configured gateway allows egress only to the exact
// IP:port destinations the installer chose; everything else is refused.
//
//   - Absent variable: legacy. No platform profile is in force, so this gate
//     passes and the per-actor egress policy applies alone.
//   - Set but empty: the profile denies every destination (fail closed).
//   - Set to a comma-separated list: a destination is allowed only when its
//     canonical IP:port equals one of the listed entries exactly.
//
// dest is the already-normalized CONNECT destination the caller validated with
// egresspolicy.NormalizeAuthority (a literal IP and a non-zero port), so this
// gate never re-parses actor-supplied text. A destination that is not an IP
// literal with a port is refused: the platform profile lists only literals.
func platformDestinationAllowed(dest egresspolicy.Destination) bool {
	value, enabled := os.LookupEnv(platformEgressDestinationsVar)
	if !enabled {
		return true
	}
	if !dest.IP.IsValid() || dest.Port == 0 {
		return false
	}
	canonical := net.JoinHostPort(dest.IP.String(), strconv.Itoa(int(dest.Port)))
	for _, permitted := range strings.Split(value, ",") {
		if canonical == strings.TrimSpace(permitted) {
			return true
		}
	}
	return false
}
