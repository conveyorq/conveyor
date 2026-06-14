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

// Package dynaport reserves free loopback ports for components that must
// be told their port up front (cluster remoting, gossip, peers) instead of
// binding :0 themselves. Ports are free on both TCP and UDP, and a
// process-wide ledger guarantees a port is never handed out twice, even
// to concurrent callers.
package dynaport

import (
	"fmt"
	"net"
	"sync"
)

// loopbackHost is the interface every reservation binds.
const loopbackHost = "127.0.0.1"

// portAllocAttempts bounds the retries of one port reservation.
const portAllocAttempts = 32

// allocatedPorts tracks every port this process has handed out, so
// concurrent callers can never receive the same port even after the
// reserving sockets are released.
var (
	// allocatedPortsMutex guards allocatedPorts.
	allocatedPortsMutex sync.Mutex
	// allocatedPorts is the process-wide set of handed-out ports.
	allocatedPorts = make(map[int]struct{})
)

// Get reserves n distinct loopback ports that are free on both TCP and
// UDP. Every reserving socket is held until all n ports are collected, so
// the kernel cannot hand the same port out twice within one call.
func Get(n int) ([]int, error) {
	tcpHeld := make([]*net.TCPListener, 0, n)
	udpHeld := make([]*net.UDPConn, 0, n)

	defer func() {
		for _, listener := range tcpHeld {
			_ = listener.Close()
		}

		for _, conn := range udpHeld {
			_ = conn.Close()
		}
	}()

	ports := make([]int, 0, n)

	for len(ports) < n {
		port, tcp, udp, err := reserveUniquePort()
		if err != nil {
			return nil, err
		}

		tcpHeld = append(tcpHeld, tcp)
		udpHeld = append(udpHeld, udp)
		ports = append(ports, port)
	}

	return ports, nil
}

// reserveUniquePort reserves a port this process has never handed out.
// The reserving sockets are released once Get returns, so the OS could
// reassign the port to a later call; the allocatedPorts ledger is what
// prevents that from producing duplicates.
func reserveUniquePort() (int, *net.TCPListener, *net.UDPConn, error) {
	var (
		rejectedTCP []*net.TCPListener
		rejectedUDP []*net.UDPConn
		lastErr     error
	)

	defer func() {
		for _, listener := range rejectedTCP {
			_ = listener.Close()
		}

		for _, conn := range rejectedUDP {
			_ = conn.Close()
		}
	}()

	for range portAllocAttempts {
		port, tcp, udp, err := reserveBothProtocols()
		if err != nil {
			lastErr = err

			continue
		}

		allocatedPortsMutex.Lock()

		if _, duplicate := allocatedPorts[port]; duplicate {
			allocatedPortsMutex.Unlock()
			rejectedTCP = append(rejectedTCP, tcp)
			rejectedUDP = append(rejectedUDP, udp)

			continue
		}

		allocatedPorts[port] = struct{}{}
		allocatedPortsMutex.Unlock()

		return port, tcp, udp, nil
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("all candidate ports already allocated")
	}

	return 0, nil, nil, fmt.Errorf("dynaport: could not reserve a unique TCP+UDP port after %d attempts: %w", portAllocAttempts, lastErr)
}

// reserveBothProtocols binds TCP on a kernel-assigned loopback port, then
// binds UDP on the same port, retrying when another process already holds
// the UDP side. Both sockets are returned for the caller to hold.
func reserveBothProtocols() (int, *net.TCPListener, *net.UDPConn, error) {
	var lastErr error

	for range portAllocAttempts {
		tcp, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP(loopbackHost), Port: 0})
		if err != nil {
			lastErr = err

			continue
		}

		port := tcp.Addr().(*net.TCPAddr).Port

		udp, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP(loopbackHost), Port: port})
		if err != nil {
			_ = tcp.Close()
			lastErr = err

			continue
		}

		return port, tcp, udp, nil
	}

	return 0, nil, nil, fmt.Errorf("dynaport: could not reserve a free TCP+UDP port after %d attempts: %w", portAllocAttempts, lastErr)
}
