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

// Package ateerrors holds typed, classifiable errors that more than one
// component must recognize. Today it carries the one error the nested-container
// feature needs: a restore refused because the snapshot's effective sandbox
// flags do not match the ones the restore target asks for.
package ateerrors

import (
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ErrSnapshotRuntimeMismatch is the sentinel a snapshot-runtime mismatch wraps,
// so a caller can recognize the class with errors.Is regardless of the detail
// text. The error built by SnapshotRuntimeMismatch wraps it AND carries the
// gRPC FailedPrecondition code, so the control plane classifies it as a
// terminal owner-fixable precondition failure (not a retriable fault, and not
// an actor crash).
var ErrSnapshotRuntimeMismatch = errors.New("snapshot runtime mismatch")

// SnapshotRuntimeMismatch returns the typed, gRPC-coded error for a restore
// whose snapshot was captured under different effective sandbox flags than the
// restore target asks for. gVisor restores a memory image only under the same
// runsc top-level flags the sandbox was created with, so Substrate refuses the
// mismatch up front rather than letting runsc fail deep in restore. The owner
// fixes it by not repointing the actor's template across the opt-in.
//
// The returned error is a FailedPrecondition gRPC status whose message is
// detail, and it satisfies errors.Is(err, ErrSnapshotRuntimeMismatch).
func SnapshotRuntimeMismatch(detail string) error {
	return &snapshotRuntimeMismatchError{
		status: status.New(codes.FailedPrecondition, detail).Err(),
	}
}

type snapshotRuntimeMismatchError struct {
	status error
}

func (e *snapshotRuntimeMismatchError) Error() string { return e.status.Error() }

// Unwrap exposes the gRPC status error so status.FromError and the server's
// status-to-APIError mapping see FailedPrecondition.
func (e *snapshotRuntimeMismatchError) Unwrap() error { return e.status }

// Is reports a match against the ErrSnapshotRuntimeMismatch sentinel, so
// errors.Is recognizes the class without depending on the message text.
func (e *snapshotRuntimeMismatchError) Is(target error) bool {
	return target == ErrSnapshotRuntimeMismatch
}

// GRPCStatus lets status.FromError / status.Code read the FailedPrecondition
// code directly off this error (grpc checks for this method before Unwrap).
func (e *snapshotRuntimeMismatchError) GRPCStatus() *status.Status {
	s, _ := status.FromError(e.status)
	return s
}
