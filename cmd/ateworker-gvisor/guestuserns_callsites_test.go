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
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

// The one-shot guest-userns setup must be reachable from the process entry
// ONLY through the --setup-guest-userns flag, and the normal worker path
// (do(ctx) and everything it calls) must NEVER invoke setupGuestUserns. If a
// refactor ever called setupGuestUserns from do() or a handler, the worker
// container -- which lacks SYS_RESOURCE -- would try to raise the sysctl and
// fail, or worse would run privileged mount code on the hot path. This scans
// the non-test sources and pins the contract.
//
// Cases this test must fail: a call to setupGuestUserns from do or any
// function other than main; main calling setupGuestUserns without gating it on
// *setupGuestUsernsFlag.

// parseNonTestSources parses every non-test source file of package main in the
// current directory (build constraints are ignored on purpose, so every file is
// scanned).
func parseNonTestSources(t *testing.T) []*ast.File {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package directory: %v", err)
	}
	fset := token.NewFileSet()
	var files []*ast.File
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		files = append(files, f)
	}
	if len(files) == 0 {
		t.Fatal("no non-test source files parsed")
	}
	return files
}

// setupGuestUsernsCallSites returns, for every call to setupGuestUserns in the
// non-test sources, the name of the enclosing function. A call that is not a
// direct identifier call (e.g. through a function value) is counted under the
// special name "<indirect>", so it cannot evade the exactly-one-direct-call
// check below.
func setupGuestUsernsCallSites(t *testing.T) []string {
	t.Helper()
	var sites []string
	for _, f := range parseNonTestSources(t) {
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "setupGuestUserns" {
					sites = append(sites, fd.Name.Name)
				}
				return true
			})
		}
	}
	return sites
}

// setupGuestUsernsIdentUses counts every reference to the identifier
// setupGuestUserns in the non-test sources, including the callee position of a
// direct call. A value that exceeds the number of direct calls means the symbol
// is referenced somewhere other than as a direct call (taken as a function
// value), which would evade the direct-call scan.
func setupGuestUsernsIdentUses(t *testing.T) int {
	t.Helper()
	n := 0
	for _, f := range parseNonTestSources(t) {
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			ast.Inspect(fd.Body, func(node ast.Node) bool {
				if id, ok := node.(*ast.Ident); ok && id.Name == "setupGuestUserns" {
					n++
				}
				return true
			})
		}
	}
	return n
}

func TestSetupGuestUsernsCalledExactlyOnceFromMain(t *testing.T) {
	sites := setupGuestUsernsCallSites(t)
	if len(sites) != 1 {
		t.Fatalf("setupGuestUserns is called %d time(s) (%v); it must be called exactly once, from main", len(sites), sites)
	}
	if sites[0] != "main" {
		t.Errorf("setupGuestUserns is called from %q; it must be reachable ONLY from main behind the flag", sites[0])
	}
	// Every identifier use must be exactly the one direct call's callee: any
	// extra reference means the symbol is taken as a value and could be invoked
	// out of main's flag gate.
	if uses := setupGuestUsernsIdentUses(t); uses != len(sites) {
		t.Errorf("setupGuestUserns is referenced %d time(s) but called directly %d time(s); an extra reference means it is taken as a function value (evading the gate)", uses, len(sites))
	}
}

// TestMainGatesSetupGuestUsernsOnFlag proves main's call to setupGuestUserns is
// guarded by an if that reads *setupGuestUsernsFlag. A variant that called
// setupGuestUserns unconditionally in main (dropping the flag gate) would make
// every worker boot run the one-shot. We assert there is an `if` whose condition
// mentions setupGuestUsernsFlag and whose body contains the call.
func TestMainGatesSetupGuestUsernsOnFlag(t *testing.T) {
	// parser.ParseDir ignores build constraints, so BOTH the linux func main
	// (main.go) and the !linux one (main_unsupported.go) are parsed. Scan every
	// func main; the gate lives in exactly one of them, which is enough.
	var mains []*ast.FuncDecl
	for _, f := range parseNonTestSources(t) {
		for _, decl := range f.Decls {
			if fd, ok := decl.(*ast.FuncDecl); ok && fd.Name.Name == "main" && fd.Recv == nil {
				mains = append(mains, fd)
			}
		}
	}
	if len(mains) == 0 {
		t.Fatal("func main not found")
	}

	gated := false
	for _, mainDecl := range mains {
		ast.Inspect(mainDecl.Body, func(n ast.Node) bool {
			ifs, ok := n.(*ast.IfStmt)
			if !ok {
				return true
			}
			// The condition must be EXACTLY `*setupGuestUsernsFlag` -- a
			// dereference of that identifier and nothing else. This rejects a
			// weakened gate like `if true || *setupGuestUsernsFlag`, which merely
			// MENTIONS the flag while always being true.
			if !condIsExactlyFlagDeref(ifs.Cond) {
				return true
			}
			// body calls setupGuestUserns?
			ast.Inspect(ifs.Body, func(bn ast.Node) bool {
				if call, ok := bn.(*ast.CallExpr); ok {
					if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "setupGuestUserns" {
						gated = true
					}
				}
				return true
			})
			return true
		})
	}
	if !gated {
		t.Error("main's setupGuestUserns call is not inside an `if` whose condition is exactly *setupGuestUsernsFlag (the flag gate is missing or weakened)")
	}
}

// condIsExactlyFlagDeref reports whether cond is exactly the expression
// `*setupGuestUsernsFlag` -- a StarExpr whose operand is that identifier -- and
// nothing more. A compound condition (e.g. `true || *setupGuestUsernsFlag`) is
// an *ast.BinaryExpr, not a *ast.StarExpr, so it is rejected.
func condIsExactlyFlagDeref(cond ast.Expr) bool {
	star, ok := cond.(*ast.StarExpr)
	if !ok {
		return false
	}
	id, ok := star.X.(*ast.Ident)
	return ok && id.Name == "setupGuestUsernsFlag"
}
