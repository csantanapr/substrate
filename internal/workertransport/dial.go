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

package workertransport

import (
	"crypto/tls"
	"fmt"
	"net"
	"strconv"
	"strings"

	"google.golang.org/grpc"
)

// Dial opens an authenticated network connection. Address is a host:port from
// trusted Pod discovery or the configured node-local Service, never a gRPC
// resolver URI. No plaintext or Unix fallback is performed.
func Dial(address string, config *tls.Config, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	host, port, err := net.SplitHostPort(address)
	n, portErr := strconv.Atoi(port)
	if err != nil || host == "" || strings.ContainsAny(host, "/?#@") || portErr != nil || n < 1 || n > 65535 {
		return nil, fmt.Errorf("network worker endpoint must be host:port: %q", address)
	}
	if config == nil {
		return nil, fmt.Errorf("network worker TLS configuration is required")
	}
	opts = append(opts, grpc.WithTransportCredentials(RefreshingCredentials(config)))
	return grpc.NewClient("passthrough:///"+address, opts...)
}
