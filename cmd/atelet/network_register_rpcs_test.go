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
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"go.ate.dev/substrate/internal/proto/ateletpb"
)

// registerNetworkRPCs returns every WorkerSupport RPC the worker's register
// package (registration and crash reporting) can send over the network path:
// the RPCs called by the methods of dialingClient (which dials through
// dialAtelet), by any function that calls dialAtelet itself, and by the
// package-level functions those call directly. The RPC names come from the
// generated WorkerSupportClient interface and the calls from the package
// source, so a new RPC on the network path shows up here without editing this
// test. Limit: helpers are followed one level deep, and only plain function
// calls (not method values or calls through interfaces) count as helpers.
func registerNetworkRPCs(t *testing.T) []string {
	t.Helper()
	rpcs := map[string]bool{}
	client := reflect.TypeOf((*ateletpb.WorkerSupportClient)(nil)).Elem()
	for i := 0; i < client.NumMethod(); i++ {
		rpcs[client.Method(i).Name] = true
	}
	dir := filepath.Join("..", "..", "internal", "ateworker", "register")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	type funcInfo struct {
		network bool
		rpcs    []string
		callees []string
	}
	funcs := map[string]*funcInfo{} // plain functions by name
	var networkFuncs []*funcInfo
	fset := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			info := &funcInfo{}
			if fn.Recv != nil && len(fn.Recv.List) == 1 {
				if star, ok := fn.Recv.List[0].Type.(*ast.StarExpr); ok {
					if id, ok := star.X.(*ast.Ident); ok && id.Name == "dialingClient" {
						info.network = true
					}
				}
			} else {
				funcs[fn.Name.Name] = info
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				switch f := call.Fun.(type) {
				case *ast.Ident:
					if f.Name == "dialAtelet" {
						info.network = true
					}
					info.callees = append(info.callees, f.Name)
				case *ast.SelectorExpr:
					if rpcs[f.Sel.Name] {
						info.rpcs = append(info.rpcs, f.Sel.Name)
					}
				}
				return true
			})
			if info.network {
				networkFuncs = append(networkFuncs, info)
			}
		}
	}
	found := map[string]bool{}
	for _, info := range networkFuncs {
		for _, rpc := range info.rpcs {
			found[rpc] = true
		}
		for _, callee := range info.callees {
			if helper, ok := funcs[callee]; ok {
				for _, rpc := range helper.rpcs {
					found[rpc] = true
				}
			}
		}
	}
	out := make([]string, 0, len(found))
	for rpc := range found {
		out = append(out, rpc)
	}
	sort.Strings(out)
	return out
}

// TestAuthorizeWorkerAllowsEveryRegisterRPC checks that the network listener's
// per-RPC authorizer admits, for a listed same-node worker, every WorkerSupport
// RPC the register client sends over the network, and still refuses methods no
// worker calls.
func TestAuthorizeWorkerAllowsEveryRegisterRPC(t *testing.T) {
	atelet, worker := testIdentities()
	net := newTestWorkerNetwork(t, atelet, networkWorkerPodFixture(worker))

	rpcs := registerNetworkRPCs(t)
	// Guard against a scan that finds nothing: registration needs an epoch,
	// the registration itself and the runtimes, and a crash needs its report.
	for _, want := range []string{"AcquireWorkerEpoch", "RegisterWorker", "SetWorkerSandboxRuntimes", "GetSandboxRuntimes", "ReportActorCrashed"} {
		if !contains(rpcs, want) {
			t.Fatalf("register network RPCs %v do not include %s; the source scan is broken", rpcs, want)
		}
	}
	for _, rpc := range rpcs {
		method := "/" + ateletpb.WorkerSupport_ServiceDesc.ServiceName + "/" + rpc
		if err := net.authorizeWorker(context.Background(), &worker, method); err != nil {
			t.Errorf("register RPC %s is refused on the network listener: %v", method, err)
		}
	}

	// Methods a worker never sends to atelet stay refused for the same
	// identity.
	for _, method := range []string{
		ateletpb.Atelet_Run_FullMethodName,
		ateletpb.Atelet_Checkpoint_FullMethodName,
	} {
		if err := net.authorizeWorker(context.Background(), &worker, method); err == nil {
			t.Errorf("worker admitted for %s", method)
		}
	}
}

func contains(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}
