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
	"errors"
	"slices"
	"testing"
)

// TestFinishCheckpointCleanupRunsBoth checks that the node-side and guest-side
// cleanups of a committed checkpoint both run, in that order, whichever of them
// fails, and that only the node-side error is reported as the checkpoint's.
func TestFinishCheckpointCleanupRunsBoth(t *testing.T) {
	nodeFailure := errors.New("node cleanup failed")
	guestFailure := errors.New("guest cleanup failed")
	for _, tc := range []struct {
		name                string
		nodeErr, guestErr   error
		wantNode, wantGuest error
	}{
		{"both succeed", nil, nil, nil, nil},
		{"node cleanup fails", nodeFailure, nil, nodeFailure, nil},
		{"guest cleanup fails", nil, guestFailure, nil, guestFailure},
		{"both fail", nodeFailure, guestFailure, nodeFailure, guestFailure},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls []string
			nodeErr, guestErr := finishCheckpointCleanup(
				func() error { calls = append(calls, "node"); return tc.nodeErr },
				func() error { calls = append(calls, "guest"); return tc.guestErr },
			)
			if want := []string{"node", "guest"}; !slices.Equal(calls, want) {
				t.Fatalf("cleanups ran %v, want %v", calls, want)
			}
			if !errors.Is(nodeErr, tc.wantNode) || (tc.wantNode == nil) != (nodeErr == nil) {
				t.Errorf("node error = %v, want %v", nodeErr, tc.wantNode)
			}
			if !errors.Is(guestErr, tc.wantGuest) || (tc.wantGuest == nil) != (guestErr == nil) {
				t.Errorf("guest error = %v, want %v", guestErr, tc.wantGuest)
			}
		})
	}
}

// TestFinishCheckpointCleanupRunsGuestAfterNodePanic checks that the guest
// cleanup still runs when the node-side cleanup panics, and that the panic is
// not swallowed.
func TestFinishCheckpointCleanupRunsGuestAfterNodePanic(t *testing.T) {
	guestRan := false
	func() {
		defer func() {
			if recover() == nil {
				t.Error("the node cleanup's panic was swallowed")
			}
		}()
		finishCheckpointCleanup(
			func() error { panic("node cleanup panicked") },
			func() error { guestRan = true; return nil },
		)
	}()
	if !guestRan {
		t.Fatal("the guest cleanup did not run after the node cleanup panicked")
	}
}
