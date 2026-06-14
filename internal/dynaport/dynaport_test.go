// MIT License
//
// Copyright (c) 2026 ConveyorQ
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package dynaport

import (
	"net"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetReturnsDistinctUsablePorts(t *testing.T) {
	ports, err := Get(3)
	require.NoError(t, err)
	require.Len(t, ports, 3)

	seen := make(map[int]struct{}, len(ports))

	for _, port := range ports {
		require.Positive(t, port)
		seen[port] = struct{}{}

		// The reservation is released on return: the port must be bindable
		// on both protocols.
		tcp, err := net.Listen("tcp", net.JoinHostPort(loopbackHost, strconv.Itoa(port)))
		require.NoError(t, err)
		require.NoError(t, tcp.Close())

		udp, err := net.ListenPacket("udp", net.JoinHostPort(loopbackHost, strconv.Itoa(port)))
		require.NoError(t, err)
		require.NoError(t, udp.Close())
	}

	require.Len(t, seen, 3)
}

func TestGetZeroPorts(t *testing.T) {
	ports, err := Get(0)
	require.NoError(t, err)
	require.Empty(t, ports)
}

func TestGetNeverRepeatsAcrossConcurrentCallers(t *testing.T) {
	const callers = 8

	var (
		mutex sync.Mutex
		all   []int
	)

	var group sync.WaitGroup

	for range callers {
		group.Go(func() {
			ports, err := Get(3)
			require.NoError(t, err)

			mutex.Lock()
			all = append(all, ports...)
			mutex.Unlock()
		})
	}

	group.Wait()

	seen := make(map[int]struct{}, len(all))

	for _, port := range all {
		_, duplicate := seen[port]
		require.False(t, duplicate, "port %d handed out twice", port)
		seen[port] = struct{}{}
	}
}
