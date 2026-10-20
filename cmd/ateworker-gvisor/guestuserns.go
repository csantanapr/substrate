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

// Guest user-namespace setup, a one-shot mode of this binary.
//
// A network worker runs inside an isolating guest sandbox. Some guest
// kernels ship with user.max_user_namespaces=0. gVisor's sentry is started in a NEW
// user namespace whenever its network mode is "sandbox"
// (runsc/sandbox/sandbox.go: "Sandbox will be started in new user namespace"),
// so with the sysctl at 0 the guest refuses clone(CLONE_NEWUSER) and the
// sandbox dies at startup with "fork/exec gvisor-sentry-prewarmer: no space
// left on device" (ENOSPC is how the kernel surfaces the namespace-count
// limit; it is not a disk problem). With gVisor's default directfs the gofer and
// the sandbox share one new identity-mapped user namespace; with --directfs=false
// the gofer needs none but the sandbox still starts in a new user namespace as
// nobody, so no runsc flag avoids a user namespace. The lever is the guest sysctl.
//
// This one-shot mode raises user.max_user_namespaces inside the pod's own guest
// VM so the sandbox can create its user namespace. It is run by a dedicated
// initContainer (see the controller's applyNetworkTransport) that holds
// SYS_ADMIN+SYS_RESOURCE only; the long-running worker never gets SYS_RESOURCE
// and never runs this code (see guestuserns_callsites_test.go's scan that the one-shot
// is reachable from main only through the flag). Every step fails closed: any
// failure exits non-zero naming the step, so the initContainer fails and the
// pod never starts the worker.

package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

// guestUsernsSentinelArg is the single argument the clone-verify child is
// re-executed with. main() recognizes it BEFORE pflag.Parse and before any
// other setup and exits 0 immediately, so the child does nothing but prove that
// clone(CLONE_NEWUSER) now succeeds. It is a reserved internal token, not a
// user-facing flag; it deliberately does not start with "--" so pflag would
// reject it, which is an extra guard that it is never taken for a real flag.
const guestUsernsSentinelArg = "__ate_userns_clone_probe__"

// guestUsernsProcSysPath is the sysctl file whose value the guest kernel reads
// to cap the number of user namespaces a user may create. A value of 0 (the
// stock guest) forbids them entirely.
const guestUsernsProcSysPath = "/proc/sys/user/max_user_namespaces"

// guestUsernsProcSysMount is the mount point to remount read-write before the
// write. In the guest the container's /proc/sys is a read-only bind mount, so a
// direct write returns EROFS until the mount is flipped.
const guestUsernsProcSysMount = "/proc/sys"

// Default and bounds for the target value. The default is generous enough to
// never be the limiting factor (one sandbox needs one user namespace) while
// staying far below the kernel's own ceiling; the bounds reject a zero (which
// would be a no-op that still forbids namespaces) and an absurd value.
const (
	guestUsernsDefaultTarget = 16384
	guestUsernsMinTarget     = 1
	guestUsernsMaxTarget     = 1048576
)

// runGuestUsernsCloneProbeIfSentinel exits 0 if this process is the clone-verify
// child. It MUST be called first thing in main(), before pflag.Parse and before
// pid1.RunIfPID1, so the sentinel argv is never parsed as a flag and the child
// never re-enters the worker. The child's whole job is to exist in a fresh user
// namespace and exit 0; reaching here proves clone(CLONE_NEWUSER) succeeded.
func runGuestUsernsCloneProbeIfSentinel() {
	for _, a := range os.Args[1:] {
		if a == guestUsernsSentinelArg {
			os.Exit(0)
		}
	}
}

// guestUsernsOps are the OS seams the one-shot drives, injectable so unit tests
// can exercise success, already-set, remount-fail, write-fail, readback-mismatch
// and clone-fail without touching the real kernel.
type guestUsernsOps struct {
	// readSysctl returns the current value at guestUsernsProcSysPath.
	readSysctl func() (int, error)
	// writeSysctl writes value to guestUsernsProcSysPath. It returns the raw
	// error (so EROFS can be told apart) rather than wrapping it.
	writeSysctl func(value int) error
	// remountReadWrite / remountReadOnly flip guestUsernsProcSysMount, preserving
	// the per-mount nosuid/nodev/noexec/relatime flags read from mountinfo.
	remountReadWrite func() error
	remountReadOnly  func() error
	// verifyClone runs a child in a new user namespace and returns nil only if
	// the clone succeeded.
	verifyClone func() error
	// logf records a step; nil logs to stderr.
	logf func(format string, args ...any)
}

func (o guestUsernsOps) log(format string, args ...any) {
	if o.logf != nil {
		o.logf(format, args...)
		return
	}
	fmt.Fprintf(os.Stderr, "setup-guest-userns: "+format+"\n", args...)
}

// hostGuestUsernsOps are the production seams. remount preserves the per-mount
// options the guest set on /proc/sys (nosuid,nodev,noexec,relatime) by reading
// them from /proc/self/mountinfo rather than guessing: a bare MS_REMOUNT that
// dropped nosuid would silently widen the mount.
var hostGuestUsernsOps = guestUsernsOps{
	readSysctl: func() (int, error) { return readSysctlInt(guestUsernsProcSysPath) },
	writeSysctl: func(value int) error {
		// Do NOT wrap: setupGuestUserns needs to test for unix.EROFS with
		// errors.Is on the returned error.
		return os.WriteFile(guestUsernsProcSysPath, []byte(strconv.Itoa(value)+"\n"), 0o644)
	},
	remountReadWrite: func() error { return remountProcSys(false) },
	remountReadOnly:  func() error { return remountProcSys(true) },
	verifyClone:      verifyUsernsCloneWorks,
}

// readSysctlInt reads a sysctl file and parses its single integer value.
func readSysctlInt(path string) (int, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	v, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil {
		return 0, fmt.Errorf("parsing %q from %s: %w", strings.TrimSpace(string(b)), path, err)
	}
	return v, nil
}

// procSysRemountFlags returns the per-mount option flags currently set on
// guestUsernsProcSysMount, read from r in /proc/self/mountinfo format. It
// returns only the flags worth preserving across a remount
// (nosuid/nodev/noexec/relatime); MS_RDONLY is added/removed by the caller. The
// bool reports whether the mount point was found; a missing mount is a caller
// error, not a silent "no flags". A scanner error (a truncated or overlong
// mountinfo line) is returned rather than discarded, so the caller fails closed
// instead of acting on a partial scan.
//
// mountinfo format (man 5 proc):
//
//	ID parentID major:minor root MOUNTPOINT per-mount-options ...
//
// per-mount-options is field index 5 (0-based) and is a comma-separated list
// whose first token is always rw or ro.
func procSysRemountFlags(r io.Reader, mountPoint string) (uintptr, bool, error) {
	var flags uintptr
	found := false
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 6 {
			continue
		}
		if fields[4] != mountPoint {
			continue
		}
		// The LAST matching line wins: a mount point can be over-mounted, and
		// the most recent entry is the one currently visible.
		found = true
		flags = 0
		for _, opt := range strings.Split(fields[5], ",") {
			switch opt {
			case "nosuid":
				flags |= unix.MS_NOSUID
			case "nodev":
				flags |= unix.MS_NODEV
			case "noexec":
				flags |= unix.MS_NOEXEC
			case "relatime":
				flags |= unix.MS_RELATIME
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return 0, false, fmt.Errorf("reading mountinfo: %w", err)
	}
	return flags, found, nil
}

// remountProcSys remounts guestUsernsProcSysMount read-write (readOnly=false) or
// read-only (readOnly=true), preserving the per-mount nosuid/nodev/noexec/
// relatime flags the guest set. It reads those flags live from
// /proc/self/mountinfo so a remount never drops a hardening flag the guest had.
func remountProcSys(readOnly bool) error {
	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return fmt.Errorf("opening /proc/self/mountinfo: %w", err)
	}
	defer f.Close()
	preserved, found, err := procSysRemountFlags(f, guestUsernsProcSysMount)
	if err != nil {
		return fmt.Errorf("reading /proc/self/mountinfo: %w", err)
	}
	if !found {
		return fmt.Errorf("%s not found in /proc/self/mountinfo", guestUsernsProcSysMount)
	}
	flags := uintptr(unix.MS_BIND|unix.MS_REMOUNT) | preserved
	if readOnly {
		flags |= unix.MS_RDONLY
	}
	if err := unix.Mount("none", guestUsernsProcSysMount, "", flags, ""); err != nil {
		return fmt.Errorf("mount(MS_REMOUNT|MS_BIND readOnly=%t flags=%#x) %s: %w",
			readOnly, flags, guestUsernsProcSysMount, err)
	}
	return nil
}

// verifyUsernsCloneWorks re-executes this binary as a child in a NEW user
// namespace (the exact thing gVisor's sentry does) and requires it to exit 0.
// The child is recognized by guestUsernsSentinelArg in main() and exits 0
// without running the worker. A kernel that still refuses user namespaces fails
// the clone with EPERM/ENOSPC here, so the one-shot fails closed rather than
// reporting a value the sandbox cannot actually use.
func verifyUsernsCloneWorks() error {
	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("finding this executable: %w", err)
	}
	cmd := exec.Command(exe, guestUsernsSentinelArg)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Cloneflags: unix.CLONE_NEWUSER,
	}
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("clone(CLONE_NEWUSER) child failed (the guest still forbids user namespaces): %w", err)
	}
	return nil
}

// setupGuestUserns is the one-shot body. It raises user.max_user_namespaces to
// target inside the pod's own guest and VERIFIES the result by actually creating
// a user namespace. Every step is logged with its exact error, and ANY failure
// returns a non-zero-exit error naming the step, so the initContainer fails and
// the worker never starts (fail closed).
//
// The clone probe here proves only that THIS process (the initContainer) can
// now create a user namespace. The following are RUNTIME ASSUMPTIONS about the
// sandbox runtime's container topology, NOT things this code measures;
// they are the standard Kubernetes/OCI model and are verified by the live run,
// whose real evidence is the WORKER container's gVisor sandbox starting, not
// this probe alone:
//   - each container (an initContainer included) runs in its own mount
//     namespace the runtime creates and tears down with the container, so the
//     read-write /proc/sys does not reach the worker container; and
//   - the initContainer and the worker share the same guest user namespace that
//     governs user.max_user_namespaces, so the value raised here is the value
//     the worker's gVisor clone is bounded by.
//
// Under those assumptions, what PERSISTS to the worker is the sysctl VALUE, not
// the mount state (user.max_user_namespaces is a property of the user namespace,
// not of the /proc/sys mount), which is why the mount is restored read-only
// after the write. If the runtime instead placed the two containers in distinct
// user namespaces with separate ucount limits, this write and clone probe would
// still succeed while the worker's sandbox could still fail -- which is exactly
// what the live run checks.
func setupGuestUserns(ops guestUsernsOps, target int) error {
	if target < guestUsernsMinTarget || target > guestUsernsMaxTarget {
		return fmt.Errorf("target %d out of range [%d,%d]", target, guestUsernsMinTarget, guestUsernsMaxTarget)
	}

	cur, err := ops.readSysctl()
	if err != nil {
		return fmt.Errorf("step read: reading %s: %w", guestUsernsProcSysPath, err)
	}
	ops.log("current %s=%d, target=%d", guestUsernsProcSysPath, cur, target)

	if cur >= target {
		// Idempotent: a second run (or a guest that already allows namespaces)
		// is a success with no mount or write. Still verify, so a value that is
		// high but not actually usable is caught.
		ops.log("already >= target; skipping remount and write")
		if err := ops.verifyClone(); err != nil {
			return fmt.Errorf("step verify (value already >= target): %w", err)
		}
		ops.log("verified: clone(CLONE_NEWUSER) succeeds; done")
		return nil
	}

	// Try the write first; only remount if the kernel says the mount is
	// read-only. This mirrors internal/netns/sysctl.go and avoids remounting a
	// mount that is already writable.
	remounted := false
	if werr := ops.writeSysctl(target); werr != nil {
		if !errors.Is(werr, unix.EROFS) {
			return fmt.Errorf("step write: writing %d to %s: %w", target, guestUsernsProcSysPath, werr)
		}
		ops.log("write hit EROFS; remounting %s read-write", guestUsernsProcSysMount)
		if err := ops.remountReadWrite(); err != nil {
			return fmt.Errorf("step remount-rw: %w", err)
		}
		remounted = true
		if werr := ops.writeSysctl(target); werr != nil {
			// Restore read-only before returning so we do not leave the mount
			// writable even on the failure path. If the restore ALSO fails, both
			// failures are reported (errors.Join): the claim is that every
			// failure is named by its step, so a lost remount-ro error would
			// weaken it.
			writeErr := fmt.Errorf("step write (after remount): writing %d to %s: %w", target, guestUsernsProcSysPath, werr)
			if rerr := ops.remountReadOnly(); rerr != nil {
				return errors.Join(writeErr, fmt.Errorf("step remount-ro: restoring %s read-only after a failed write: %w", guestUsernsProcSysMount, rerr))
			}
			return writeErr
		}
	}

	// Restore read-only immediately after the write, before readback and verify,
	// so the window in which the mount is writable is as small as possible. A
	// failure to restore is fatal (fail closed): we do not want to report
	// success having left /proc/sys writable.
	if remounted {
		if err := ops.remountReadOnly(); err != nil {
			return fmt.Errorf("step remount-ro: restoring %s read-only: %w", guestUsernsProcSysMount, err)
		}
		ops.log("restored %s read-only", guestUsernsProcSysMount)
	}

	got, err := ops.readSysctl()
	if err != nil {
		return fmt.Errorf("step readback: re-reading %s: %w", guestUsernsProcSysPath, err)
	}
	if got < target {
		return fmt.Errorf("step readback: %s is %d after writing %d (the kernel rejected or clamped the write)",
			guestUsernsProcSysPath, got, target)
	}
	ops.log("readback %s=%d (>= target)", guestUsernsProcSysPath, got)

	if err := ops.verifyClone(); err != nil {
		return fmt.Errorf("step verify: %w", err)
	}
	ops.log("verified: clone(CLONE_NEWUSER) succeeds; done")
	return nil
}
