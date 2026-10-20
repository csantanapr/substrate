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
	"context"
	"crypto/tls"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc/credentials"
)

// RefreshingCredentials retires each authenticated transport before its peer
// certificate expires. gRPC reconnects the same ClientConn and the credential
// loaders read the current files. Per-RPC expiry checks remain fail-closed;
// application errors alone would never trigger a new TLS handshake.
//
// A retirement may interrupt an in-flight RPC, as a network disconnect can.
// This transport layer does not replay lifecycle operations.
func RefreshingCredentials(config *tls.Config) credentials.TransportCredentials {
	return &refreshingCredentials{TransportCredentials: credentials.NewTLS(config)}
}

type refreshingCredentials struct {
	credentials.TransportCredentials
}

func (c *refreshingCredentials) Clone() credentials.TransportCredentials {
	return &refreshingCredentials{TransportCredentials: c.TransportCredentials.Clone()}
}

func (c *refreshingCredentials) ClientHandshake(ctx context.Context, authority string, raw net.Conn) (net.Conn, credentials.AuthInfo, error) {
	conn, info, err := c.TransportCredentials.ClientHandshake(ctx, authority, raw)
	if err != nil {
		return nil, nil, err
	}
	return retireBeforeExpiry(conn, info), info, nil
}

func (c *refreshingCredentials) ServerHandshake(raw net.Conn) (net.Conn, credentials.AuthInfo, error) {
	conn, info, err := c.TransportCredentials.ServerHandshake(raw)
	if err != nil {
		return nil, nil, err
	}
	return retireBeforeExpiry(conn, info), info, nil
}

type retiringConn struct {
	net.Conn
	mu     sync.Mutex
	timer  *time.Timer
	closed bool
}

func retireBeforeExpiry(conn net.Conn, info credentials.AuthInfo) net.Conn {
	tlsInfo, ok := info.(credentials.TLSInfo)
	if !ok || len(tlsInfo.State.PeerCertificates) == 0 {
		return conn
	}
	remaining := time.Until(tlsInfo.State.PeerCertificates[0].NotAfter)
	retirement := remaining - min(time.Minute, remaining/10)
	wrapped := &retiringConn{Conn: conn}
	wrapped.mu.Lock()
	wrapped.timer = time.AfterFunc(max(0, retirement), func() { _ = wrapped.Close() })
	wrapped.mu.Unlock()
	return wrapped
}

func (c *retiringConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	if c.timer != nil {
		c.timer.Stop()
	}
	return c.Conn.Close()
}
