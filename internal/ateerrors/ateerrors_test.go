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

package ateerrors

import (
	"errors"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestSnapshotRuntimeMismatch proves the typed error is both recognizable by
// errors.Is (so callers can classify it without the message text) and carries
// the FailedPrecondition gRPC code (so the control plane classifies it as a
// terminal, owner-fixable precondition), and that the detail reaches the
// status message. It also checks a wrapped error stays recognizable.
func TestSnapshotRuntimeMismatch(t *testing.T) {
	err := SnapshotRuntimeMismatch("flags differ")

	if !errors.Is(err, ErrSnapshotRuntimeMismatch) {
		t.Errorf("errors.Is(err, ErrSnapshotRuntimeMismatch) = false, want true")
	}
	if code := status.Code(err); code != codes.FailedPrecondition {
		t.Errorf("status.Code = %v, want FailedPrecondition", code)
	}
	s, ok := status.FromError(err)
	if !ok {
		t.Fatalf("status.FromError returned ok=false")
	}
	if s.Message() != "flags differ" {
		t.Errorf("status message = %q, want %q", s.Message(), "flags differ")
	}

	// A plain error is not this class.
	if errors.Is(errors.New("flags differ"), ErrSnapshotRuntimeMismatch) {
		t.Errorf("an unrelated error matched ErrSnapshotRuntimeMismatch")
	}

	// Wrapping keeps it recognizable.
	wrapped := fmt.Errorf("while restoring: %w", err)
	if !errors.Is(wrapped, ErrSnapshotRuntimeMismatch) {
		t.Errorf("errors.Is on a wrapped error = false, want true")
	}
}
