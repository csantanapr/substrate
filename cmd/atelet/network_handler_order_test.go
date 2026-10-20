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
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"slices"
	"strings"
	"testing"
)

// The in-process end-to-end test drives the client-side staging helpers
// directly; it cannot stand up the full plugin stack the Run/Checkpoint/Restore
// handlers need (object store, image cache), and the fake object-store plugin
// cannot complete an upload in this build environment. So the HANDLER-level
// orderings these three invariants depend on are pinned here mechanically, by
// parsing main.go and asserting the call order inside each handler. A future
// reordering (e.g. moving the nested-runtime refusal after the staging push, or cleaning up
// the guest before the pull) fails this test even though every other test still
// passes. This is the guard the end-to-end test cannot be, by construction.
//
// Limitation (documented, not a present defect): the AST walk sees LEXICAL
// order, not reachability, so a call hidden under a never-taken branch would
// still satisfy the order. The network steps are unconditional statements in
// each handler today, so this does not weaken the current guarantee.

// ateletMethodDecl returns the *ast.FuncDecl for a method named `method` whose
// receiver is *Atelet, or nil. Checking the receiver type (not just the name)
// stops the guard from matching an unrelated function that happens to share a
// handler's name.
func ateletMethodDecl(file *ast.File, method string) *ast.FuncDecl {
	for _, decl := range file.Decls {
		d, ok := decl.(*ast.FuncDecl)
		if !ok || d.Name.Name != method || d.Recv == nil || len(d.Recv.List) != 1 {
			continue
		}
		star, ok := d.Recv.List[0].Type.(*ast.StarExpr)
		if !ok {
			continue
		}
		ident, ok := star.X.(*ast.Ident)
		if ok && ident.Name == "Atelet" {
			return d
		}
	}
	return nil
}

// callOrderInFunc returns, for a *Atelet method, the source-order index at which
// each wanted call first appears, or -1 if absent. A wanted name of the form
// "recv.Method" requires the call target to be exactly that selector (receiver
// identifier AND method), so an unrelated x.RunWorkload cannot satisfy a
// "client.RunWorkload" requirement; a bare "Method" matches any call to that
// final identifier (used for a package-level call like checkNestedRuntimeCompatible).
func callOrderInFunc(t *testing.T, file *ast.File, method string, wanted ...string) map[string]int {
	t.Helper()
	order := map[string]int{}
	for _, name := range wanted {
		order[name] = -1
	}
	fn := ateletMethodDecl(file, method)
	if fn == nil {
		t.Fatalf("*Atelet method %s not found in main.go", method)
	}
	seq := 0
	ast.Inspect(fn, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		for _, form := range calleeForms(call.Fun) {
			if idx, tracked := order[form]; tracked && idx == -1 {
				order[form] = seq
			}
		}
		seq++
		return true
	})
	return order
}

// lastCallInFunc returns the position of the last call to name in method, in
// the numbering callOrderInFunc uses, or -1.
func lastCallInFunc(t *testing.T, file *ast.File, method, name string) int {
	t.Helper()
	fn := ateletMethodDecl(file, method)
	if fn == nil {
		t.Fatalf("*Atelet method %s not found in main.go", method)
	}
	last, seq := -1, 0
	ast.Inspect(fn, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		for _, form := range calleeForms(call.Fun) {
			if form == name {
				last = seq
			}
		}
		seq++
		return true
	})
	return last
}

// exprString renders an AST expression back to source text, so a call argument
// can be compared against the literal the handler must pass.
func exprString(t *testing.T, e ast.Expr) string {
	t.Helper()
	var b strings.Builder
	if err := printer.Fprint(&b, token.NewFileSet(), e); err != nil {
		t.Fatalf("printing expression: %v", err)
	}
	return b.String()
}

// assertCallArg fails unless the *Atelet method calls callee (a "recv.method"
// selector) with argIndex (0-based) rendering exactly wantArg. It pins a
// production CALL-SITE ARGUMENT a behavioral test cannot, because the behavioral
// test supplies that argument by hand. It fails loudly if the call is absent or
// has too few args, so a renamed/removed call cannot make it pass vacuously.
func assertCallArg(t *testing.T, file *ast.File, method, callee string, argIndex int, wantArg string) {
	t.Helper()
	fn := ateletMethodDecl(file, method)
	if fn == nil {
		t.Fatalf("*Atelet method %s not found in main.go", method)
	}
	found := false
	ast.Inspect(fn, func(n ast.Node) bool {
		if found {
			return false
		}
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if !slices.Contains(calleeForms(call.Fun), callee) {
			return true
		}
		found = true
		if argIndex >= len(call.Args) {
			t.Fatalf("%s: call %q has %d args, need arg index %d", method, callee, len(call.Args), argIndex)
		}
		if got := exprString(t, call.Args[argIndex]); got != wantArg {
			t.Fatalf("%s: call %q arg %d = %q, want %q (the pull must receive the complete reported set)", method, callee, argIndex, got, wantArg)
		}
		return false
	})
	if !found {
		t.Fatalf("%s: call %q not found; the call-site argument guard would pass vacuously", method, callee)
	}
}

// orderingHolds returns nil iff both calls are present (index >= 0) and first
// precedes second. It is the pure predicate assertBefore enforces, factored out
// so the non-vacuity test can assert it REJECTS an absent call without failing
// the real test via a subtest.
func orderingHolds(method, first, second string, order map[string]int) error {
	if order[first] < 0 {
		return fmt.Errorf("%s: expected call %q not found (a renamed/removed call must not make the ordering vacuous)", method, first)
	}
	if order[second] < 0 {
		return fmt.Errorf("%s: expected call %q not found", method, second)
	}
	if !(order[first] < order[second]) {
		return fmt.Errorf("%s: %q (pos %d) must come before %q (pos %d)", method, first, order[first], second, order[second])
	}
	return nil
}

func assertBefore(t *testing.T, method, first, second string, order map[string]int) {
	t.Helper()
	if err := orderingHolds(method, first, second, order); err != nil {
		t.Fatal(err)
	}
}

// calleeForms returns the names a call target can be matched against: the bare
// final identifier, and (for a selector like s.foo or client.Bar) the
// "receiver.method" form, so a caller can require a specific receiver.
func calleeForms(fun ast.Expr) []string {
	switch f := fun.(type) {
	case *ast.SelectorExpr:
		if recv, ok := f.X.(*ast.Ident); ok {
			return []string{f.Sel.Name, recv.Name + "." + f.Sel.Name}
		}
		return []string{f.Sel.Name}
	case *ast.Ident:
		return []string{f.Name}
	}
	return nil
}

func parseMainGo(t *testing.T) *ast.File {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "main.go", nil, 0)
	if err != nil {
		t.Fatalf("parse main.go: %v", err)
	}
	return file
}

func TestNetworkHandlerCallOrdering(t *testing.T) {
	file := parseMainGo(t)

	// Run: stage into the fresh guest BEFORE the worker RunWorkload (fail closed).
	// Require the exact receivers: s.stageNetworkActivation, client.RunWorkload.
	run := callOrderInFunc(t, file, "Run", "s.stageNetworkActivation", "client.RunWorkload")
	assertBefore(t, "Run", "s.stageNetworkActivation", "client.RunWorkload", run)

	// Checkpoint: pull the guest's checkpoint into node custody BEFORE the guest
	// is cleaned up, so a failed pull never leaves the guest removed.
	//
	// Upstream #2789 made the atelet upload the latest LOCAL snapshot in the
	// background once it commits, and moved the superseded-snapshot prune to
	// AFTER the commit. For a network worker the pull is what puts the files in
	// node custody, so the pull MUST precede both the commit and the prune, or
	// the two invariants the merge turns on break silently:
	//
	//   (b) the background uploader reads a COMMITTED snapshot from node custody;
	//       if the pull ran after CommitLocal/CommitDurable the uploader could
	//       observe a committed snapshot whose files are still only on the guest
	//       (or already gone), so the pull must precede every commit;
	//   (c) a failed pull must leave the PREVIOUS snapshot in place; the prune
	//       that deletes superseded snapshots now runs after the commit, so it
	//       must also run after the pull -- a pull that fails returns before the
	//       commit and therefore before the prune ever executes.
	//
	// Both commit calls are s.snapshots.Commit{Local,Durable}; the AST walk sees
	// their bare final identifier (the receiver is itself a selector s.snapshots,
	// not a plain ident). The prune runs inside s.cleanUpAfterCheckpoint, which
	// Checkpoint calls once the snapshot is committed; the guest cleanup follows
	// it. A retried Checkpoint answered from the snapshot it already committed
	// returns early through its own s.cleanUpAfterCheckpoint call, so the
	// cleanup of this checkpoint is the LAST such call.
	checkpoint := callOrderInFunc(t, file, "Checkpoint",
		"s.receiveNetworkCheckpoint", "s.cleanupNetworkActor",
		"CommitLocal", "CommitDurable")
	checkpoint["s.cleanUpAfterCheckpoint"] = lastCallInFunc(t, file, "Checkpoint", "s.cleanUpAfterCheckpoint")
	assertBefore(t, "Checkpoint", "s.receiveNetworkCheckpoint", "s.cleanupNetworkActor", checkpoint)
	// (b) pull precedes every commit.
	assertBefore(t, "Checkpoint", "s.receiveNetworkCheckpoint", "CommitLocal", checkpoint)
	assertBefore(t, "Checkpoint", "s.receiveNetworkCheckpoint", "CommitDurable", checkpoint)
	// (c) pull precedes the cleanup that prunes superseded snapshots, so a
	// failed pull never reaches the prune.
	assertBefore(t, "Checkpoint", "s.receiveNetworkCheckpoint", "s.cleanUpAfterCheckpoint", checkpoint)
	// The cleanup runs only after a commit, on BOTH persist branches (deferred
	// commits CommitLocal; immediate commits CommitDurable then CommitLocal).
	assertBefore(t, "Checkpoint", "CommitLocal", "s.cleanUpAfterCheckpoint", checkpoint)
	assertBefore(t, "Checkpoint", "CommitDurable", "s.cleanUpAfterCheckpoint", checkpoint)
	// The guest copy is discarded only after the node-side cleanup.
	assertBefore(t, "Checkpoint", "s.cleanUpAfterCheckpoint", "s.cleanupNetworkActor", checkpoint)
	// Both cleanups go through finishCheckpointCleanup, which runs the guest
	// cleanup even when the node-side cleanup fails.
	if order := callOrderInFunc(t, file, "Checkpoint", "finishCheckpointCleanup"); order["finishCheckpointCleanup"] < 0 {
		t.Fatal("Checkpoint does not finish through finishCheckpointCleanup")
	}
	// And the prune itself is still part of that cleanup.
	if prune := callOrderInFunc(t, file, "cleanUpAfterCheckpoint", "snapshottracker.PruneLocalCheckpoints"); prune["snapshottracker.PruneLocalCheckpoints"] < 0 {
		t.Fatal("cleanUpAfterCheckpoint no longer prunes superseded local checkpoints")
	}

	// The production CALL-SITE ARGUMENT: the pull must be handed the actor's
	// COMPLETE reported set, sandboxRec.SnapshotFiles. A behavioral test that
	// passes the file set to receiveNetworkCheckpoint by hand cannot catch a
	// handler that passed DataSnapshotFiles (a subset) or another truncated set,
	// so pin the argument here. Export would reject a short membership at
	// runtime, but pinning the argument fails the build-time guard first and
	// names the cause.
	assertCallArg(t, file, "Checkpoint", "s.receiveNetworkCheckpoint", 2, "sandboxRec.SnapshotFiles")

	// Restore: the nested-runtime refusal BEFORE staging into the fresh guest
	// (a mismatched memory-bearing restore stages nothing), and staging BEFORE
	// the worker RestoreWorkload (fail closed). checkNestedRuntimeCompatible is a
	// package-level call, matched by its bare name.
	restore := callOrderInFunc(t, file, "Restore", "checkNestedRuntimeCompatible", "s.stageNetworkActivation", "client.RestoreWorkload")
	assertBefore(t, "Restore", "checkNestedRuntimeCompatible", "s.stageNetworkActivation", restore)
	assertBefore(t, "Restore", "s.stageNetworkActivation", "client.RestoreWorkload", restore)
}

// TestNetworkHandlerCallOrderingGuardIsNonVacuous exercises the SAME helper the
// real test uses, so the proof is of the production machinery, not a copy: a
// call present in the handler resolves to a nonnegative index, a nonexistent
// one resolves to -1, and assertBefore fails (via a recovered sub-test) when a
// required call is absent.
func TestNetworkHandlerCallOrderingGuardIsNonVacuous(t *testing.T) {
	file := parseMainGo(t)
	order := callOrderInFunc(t, file, "Checkpoint", "s.receiveNetworkCheckpoint", "thisCallDoesNotExist")
	if order["s.receiveNetworkCheckpoint"] < 0 {
		t.Fatal("a real call (s.receiveNetworkCheckpoint) resolved to absent; the helper or handler changed")
	}
	if order["thisCallDoesNotExist"] != -1 {
		t.Fatalf("a nonexistent call resolved to %d, want -1 (the guard would pass vacuously)", order["thisCallDoesNotExist"])
	}

	// The predicate assertBefore enforces must REJECT an absent call: a missing
	// call can never make an ordering hold vacuously. A present, correctly
	// ordered pair must be accepted.
	if err := orderingHolds("Checkpoint", "s.receiveNetworkCheckpoint", "thisCallDoesNotExist", order); err == nil {
		t.Fatal("orderingHolds accepted an absent second call; the ordering could pass vacuously")
	}
	present := callOrderInFunc(t, file, "Checkpoint", "s.receiveNetworkCheckpoint", "s.cleanupNetworkActor")
	if err := orderingHolds("Checkpoint", "s.receiveNetworkCheckpoint", "s.cleanupNetworkActor", present); err != nil {
		t.Fatalf("orderingHolds rejected a real, correctly ordered pair: %v", err)
	}
}
