//go:build linux

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
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

// fakeUsernsState is a controllable in-memory model of the guest sysctl behind
// the seams, so setupGuestUserns can be driven through every branch without
// touching the real kernel.
type fakeUsernsState struct {
	value        int
	readOnly     bool  // /proc/sys is a ro bind mount; a write returns EROFS
	remountRW    int   // count of read-write remounts
	remountRO    int   // count of read-only remounts
	writes       int   // count of successful writes
	cloneOK      bool  // whether verifyClone succeeds
	readCount    int   // number of readSysctl calls so far
	readErr      error // force every read to fail
	readbackErr  error // force ONLY the readback (second+) read to fail
	writeErr     error // force every write to fail with this (non-EROFS) error
	remountRWErr error // force the read-write remount to fail
	remountROErr error // force the read-only remount to fail

	// writeErrAfterRemount, when set, makes a write FAIL after the mount has
	// been flipped read-write (remountRW > 0), so the "EROFS then second write
	// fails" path can be exercised distinctly from the first write.
	writeErrAfterRemount error
}

func (s *fakeUsernsState) ops() guestUsernsOps {
	return guestUsernsOps{
		readSysctl: func() (int, error) {
			s.readCount++
			if s.readErr != nil {
				return 0, s.readErr
			}
			// The readback is the second (or later) read; fail only it when set.
			if s.readbackErr != nil && s.readCount >= 2 {
				return 0, s.readbackErr
			}
			return s.value, nil
		},
		writeSysctl: func(v int) error {
			if s.writeErr != nil {
				return s.writeErr
			}
			if s.readOnly {
				return unix.EROFS
			}
			// The mount has been flipped read-write (or was already writable).
			if s.writeErrAfterRemount != nil && s.remountRW > 0 {
				return s.writeErrAfterRemount
			}
			s.value = v
			s.writes++
			return nil
		},
		remountReadWrite: func() error {
			if s.remountRWErr != nil {
				return s.remountRWErr
			}
			s.readOnly = false
			s.remountRW++
			return nil
		},
		remountReadOnly: func() error {
			if s.remountROErr != nil {
				return s.remountROErr
			}
			s.readOnly = true
			s.remountRO++
			return nil
		},
		verifyClone: func() error {
			if s.cloneOK {
				return nil
			}
			return errors.New("clone refused by fake kernel")
		},
		logf: func(string, ...any) {}, // silent
	}
}

func TestSetupGuestUserns_Success_RemountPath(t *testing.T) {
	// Starts below target on a read-only /proc/sys; must remount rw, write,
	// remount ro, read back, and verify clone.
	s := &fakeUsernsState{value: 0, readOnly: true, cloneOK: true}
	if err := setupGuestUserns(s.ops(), 16384); err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if s.value != 16384 {
		t.Errorf("value = %d, want 16384", s.value)
	}
	if s.writes != 1 {
		t.Errorf("writes = %d, want 1", s.writes)
	}
	if s.remountRW != 1 || s.remountRO != 1 {
		t.Errorf("remountRW=%d remountRO=%d, want 1 and 1 (mount restored read-only)", s.remountRW, s.remountRO)
	}
	if !s.readOnly {
		t.Errorf("/proc/sys left writable after a successful run; must be restored read-only")
	}
}

func TestSetupGuestUserns_Success_WritableAlready(t *testing.T) {
	// /proc/sys already writable: write succeeds without any remount.
	s := &fakeUsernsState{value: 0, readOnly: false, cloneOK: true}
	if err := setupGuestUserns(s.ops(), 1024); err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if s.remountRW != 0 || s.remountRO != 0 {
		t.Errorf("remountRW=%d remountRO=%d, want 0 and 0 (no remount when already writable)", s.remountRW, s.remountRO)
	}
	if s.value != 1024 {
		t.Errorf("value = %d, want 1024", s.value)
	}
}

func TestSetupGuestUserns_AlreadySet_Idempotent(t *testing.T) {
	// Value already >= target: no write, no remount, but STILL verify clone.
	s := &fakeUsernsState{value: 99999, readOnly: true, cloneOK: true}
	if err := setupGuestUserns(s.ops(), 16384); err != nil {
		t.Fatalf("expected success (idempotent), got %v", err)
	}
	if s.writes != 0 || s.remountRW != 0 {
		t.Errorf("writes=%d remountRW=%d, want 0 and 0 when already >= target", s.writes, s.remountRW)
	}
}

func TestSetupGuestUserns_AlreadySet_ButCloneStillFails(t *testing.T) {
	// A value already high but a kernel that still forbids namespaces must FAIL
	// closed at the verify step; a high sysctl is not proof the clone works.
	s := &fakeUsernsState{value: 99999, readOnly: false, cloneOK: false}
	err := setupGuestUserns(s.ops(), 16384)
	if err == nil {
		t.Fatal("expected failure when clone verify fails despite a high value")
	}
	if !strings.Contains(err.Error(), "verify") {
		t.Errorf("error should name the verify step, got %v", err)
	}
}

func TestSetupGuestUserns_TargetOutOfRange(t *testing.T) {
	for _, target := range []int{0, -1, guestUsernsMaxTarget + 1} {
		s := &fakeUsernsState{value: 0, readOnly: true, cloneOK: true}
		if err := setupGuestUserns(s.ops(), target); err == nil {
			t.Errorf("target %d: expected range error, got nil", target)
		} else if !strings.Contains(err.Error(), "out of range") {
			t.Errorf("target %d: error should name the range, got %v", target, err)
		}
		if s.writes != 0 {
			t.Errorf("target %d: must not write an out-of-range value", target)
		}
	}
}

func TestSetupGuestUserns_RemountRWFails(t *testing.T) {
	s := &fakeUsernsState{value: 0, readOnly: true, cloneOK: true, remountRWErr: errors.New("mount denied")}
	err := setupGuestUserns(s.ops(), 16384)
	if err == nil {
		t.Fatal("expected failure when the read-write remount fails")
	}
	if !strings.Contains(err.Error(), "remount-rw") {
		t.Errorf("error should name step remount-rw, got %v", err)
	}
	if s.writes != 0 {
		t.Errorf("must not have written when remount-rw failed")
	}
}

func TestSetupGuestUserns_WriteFails_NonEROFS(t *testing.T) {
	// A write error that is NOT EROFS is fatal and is not retried via remount.
	s := &fakeUsernsState{value: 0, readOnly: false, cloneOK: true, writeErr: errors.New("EINVAL-ish")}
	err := setupGuestUserns(s.ops(), 16384)
	if err == nil {
		t.Fatal("expected failure on a non-EROFS write error")
	}
	if !strings.Contains(err.Error(), "step write") {
		t.Errorf("error should name the write step, got %v", err)
	}
	if s.remountRW != 0 {
		t.Errorf("a non-EROFS write error must not trigger a remount")
	}
}

func TestSetupGuestUserns_RemountROFails_AfterWrite(t *testing.T) {
	// The write succeeds after remounting rw, but restoring read-only fails:
	// that is fatal (fail closed) and must NOT report success.
	s := &fakeUsernsState{value: 0, readOnly: true, cloneOK: true, remountROErr: errors.New("ro remount denied")}
	err := setupGuestUserns(s.ops(), 16384)
	if err == nil {
		t.Fatal("expected failure when restoring read-only fails")
	}
	if !strings.Contains(err.Error(), "remount-ro") {
		t.Errorf("error should name step remount-ro, got %v", err)
	}
}

func TestSetupGuestUserns_EROFSThenSecondWriteFails(t *testing.T) {
	// First write hits EROFS, so we remount rw; the SECOND write still fails.
	// The returned error must name step write (after remount), and the mount
	// must be restored read-only.
	s := &fakeUsernsState{value: 0, readOnly: true, cloneOK: true, writeErrAfterRemount: errors.New("second write EINVAL")}
	err := setupGuestUserns(s.ops(), 16384)
	if err == nil {
		t.Fatal("expected failure when the second write fails after remount")
	}
	if !strings.Contains(err.Error(), "step write (after remount)") {
		t.Errorf("error should name step write (after remount), got %v", err)
	}
	if s.remountRO != 1 {
		t.Errorf("mount must be restored read-only after a failed second write; remountRO=%d", s.remountRO)
	}
	if !s.readOnly {
		t.Errorf("/proc/sys must be left read-only after a failed second write")
	}
}

func TestSetupGuestUserns_EROFSThenSecondWriteFails_AndRestoreFails(t *testing.T) {
	// First write EROFS -> remount rw -> second write fails -> restore read-only
	// ALSO fails. BOTH failures must be named in the joined error (fail closed,
	// no lost remount-ro error).
	s := &fakeUsernsState{
		value: 0, readOnly: true, cloneOK: true,
		writeErrAfterRemount: errors.New("second write EINVAL"),
		remountROErr:         errors.New("ro remount denied"),
	}
	err := setupGuestUserns(s.ops(), 16384)
	if err == nil {
		t.Fatal("expected failure when both the second write and the restore fail")
	}
	if !strings.Contains(err.Error(), "step write (after remount)") {
		t.Errorf("error should name step write (after remount), got %v", err)
	}
	if !strings.Contains(err.Error(), "step remount-ro") {
		t.Errorf("error should ALSO name step remount-ro (joined), got %v", err)
	}
}

func TestSetupGuestUserns_ReadbackMismatch(t *testing.T) {
	// The write "succeeds" but the kernel clamps the value back below target:
	// the readback must catch it and fail closed. Model it with a write that
	// records a lower value than requested.
	s := &fakeUsernsState{value: 0, readOnly: false, cloneOK: true}
	ops := s.ops()
	realWrite := ops.writeSysctl
	ops.writeSysctl = func(v int) error {
		// Simulate a kernel clamp: store a value below the target.
		if err := realWrite(v); err != nil {
			return err
		}
		s.value = 1 // clamped below any sane target
		return nil
	}
	err := setupGuestUserns(ops, 16384)
	if err == nil {
		t.Fatal("expected a readback mismatch failure")
	}
	if !strings.Contains(err.Error(), "readback") {
		t.Errorf("error should name the readback step, got %v", err)
	}
}

func TestSetupGuestUserns_CloneFails_AfterWrite(t *testing.T) {
	// Everything writes and reads back fine, but the clone still fails: fail
	// closed at verify.
	s := &fakeUsernsState{value: 0, readOnly: true, cloneOK: false}
	err := setupGuestUserns(s.ops(), 16384)
	if err == nil {
		t.Fatal("expected failure when the clone verify fails after a good write")
	}
	if !strings.Contains(err.Error(), "step verify") {
		t.Errorf("error should name the verify step, got %v", err)
	}
}

func TestSetupGuestUserns_ReadbackReadFails(t *testing.T) {
	// The write succeeds but the READBACK read itself fails (I/O error, not a
	// clamp): must fail closed naming the readback step, not verify success.
	s := &fakeUsernsState{value: 0, readOnly: false, cloneOK: true, readbackErr: errors.New("readback I/O error")}
	err := setupGuestUserns(s.ops(), 16384)
	if err == nil {
		t.Fatal("expected failure when the readback read fails")
	}
	if !strings.Contains(err.Error(), "step readback") {
		t.Errorf("error should name the readback step, got %v", err)
	}
}

func TestSetupGuestUserns_InitialReadFails(t *testing.T) {
	s := &fakeUsernsState{readErr: errors.New("open denied")}
	err := setupGuestUserns(s.ops(), 16384)
	if err == nil {
		t.Fatal("expected failure when the initial read fails")
	}
	if !strings.Contains(err.Error(), "step read") {
		t.Errorf("error should name the read step, got %v", err)
	}
}

// --- procSysRemountFlags: the mountinfo option parser, both ways -------------

func TestProcSysRemountFlags(t *testing.T) {
	const mi = `21 20 0:19 / /proc rw,nosuid,nodev,noexec,relatime shared:5 - proc proc rw
22 21 0:19 /sys /proc/sys ro,nosuid,nodev,noexec,relatime - proc proc ro
23 20 0:21 / /tmp rw,nosuid,nodev - tmpfs tmpfs rw
`
	flags, found, err := procSysRemountFlags(strings.NewReader(mi), "/proc/sys")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !found {
		t.Fatal("expected /proc/sys to be found")
	}
	want := uintptr(unix.MS_NOSUID | unix.MS_NODEV | unix.MS_NOEXEC | unix.MS_RELATIME)
	if flags != want {
		t.Errorf("flags = %#x, want %#x", flags, want)
	}
	// MS_RDONLY is NOT among the preserved flags: the caller adds it.
	if flags&unix.MS_RDONLY != 0 {
		t.Errorf("preserved flags must not include MS_RDONLY; got %#x", flags)
	}
}

func TestProcSysRemountFlags_NotFound(t *testing.T) {
	const mi = `21 20 0:19 / /proc rw,nosuid,nodev,noexec,relatime - proc proc rw
`
	_, found, err := procSysRemountFlags(strings.NewReader(mi), "/proc/sys")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found {
		t.Error("expected /proc/sys NOT to be found")
	}
}

func TestProcSysRemountFlags_PartialOptions(t *testing.T) {
	// Only nosuid present: just that flag is preserved, nothing invented.
	const mi = `22 21 0:19 /sys /proc/sys rw,nosuid - proc proc rw
`
	flags, found, err := procSysRemountFlags(strings.NewReader(mi), "/proc/sys")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !found {
		t.Fatal("expected /proc/sys to be found")
	}
	if flags != uintptr(unix.MS_NOSUID) {
		t.Errorf("flags = %#x, want only MS_NOSUID (%#x)", flags, uintptr(unix.MS_NOSUID))
	}
}

// errReader returns one line and then a read error, to exercise the scanner
// error path of procSysRemountFlags (fail closed, don't silently return a
// partial result).
type errReader struct{ done bool }

func (e *errReader) Read(p []byte) (int, error) {
	if e.done {
		return 0, errors.New("simulated mountinfo read error")
	}
	e.done = true
	// A single overlong token with no newline forces bufio.Scanner to return
	// bufio.ErrTooLong, but a plain read error is simpler and just as good.
	n := copy(p, []byte("21 20 0:19 / /proc "))
	return n, nil
}

func TestProcSysRemountFlags_ScannerError(t *testing.T) {
	_, _, err := procSysRemountFlags(&errReader{}, "/proc/sys")
	if err == nil {
		t.Error("expected a scanner error to be reported, not silently discarded")
	}
}
