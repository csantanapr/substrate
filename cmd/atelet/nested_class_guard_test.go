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
	"strings"
	"testing"

	"go.ate.dev/substrate/internal/proto/ateletpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestValidateNestedSandbox pins, both ways, the guard that replaces the
// template-admission class check #2780 made impossible: a nested-containers
// opt-in is admitted only when the resolved sandbox class is gVisor. Without
// it, a nested-opted actor placed on a microVM worker would have its opt-in
// silently dropped and the template would lie about the actor's sandbox.
func TestValidateNestedSandbox(t *testing.T) {
	for _, tc := range []struct {
		name       string
		nested     *ateletpb.NestedContainers
		class      string
		wantReject bool
	}{
		// Accept: no opt-in, any class -- a default actor is unaffected.
		{"nil nested, gvisor", nil, "gvisor", false},
		{"nil nested, microvm", nil, "microvm", false},
		{"disabled nested, microvm", &ateletpb.NestedContainers{Enabled: false}, "microvm", false},
		// Accept: opt-in on gVisor, the only class that honors it.
		{"enabled nested, gvisor", &ateletpb.NestedContainers{Enabled: true}, "gvisor", false},
		// Reject: opt-in on any non-gVisor class, where it would be dropped.
		{"enabled nested, microvm", &ateletpb.NestedContainers{Enabled: true}, "microvm", true},
		{"enabled nested, empty class", &ateletpb.NestedContainers{Enabled: true}, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateNestedSandbox(tc.nested, tc.class)
			if tc.wantReject {
				if err == nil {
					t.Fatalf("validateNestedSandbox(%+v, %q) = nil, want a rejection", tc.nested, tc.class)
				}
				if status.Code(err) != codes.InvalidArgument {
					t.Errorf("rejection code = %v, want InvalidArgument", status.Code(err))
				}
				// The message must name the resolved class, so the failure is
				// attributable and the check is not a generic deny.
				if !strings.Contains(err.Error(), "gvisor") {
					t.Errorf("rejection %q does not name the required gvisor class", err.Error())
				}
			} else if err != nil {
				t.Fatalf("validateNestedSandbox(%+v, %q) = %v, want nil", tc.nested, tc.class, err)
			}
		})
	}
}
