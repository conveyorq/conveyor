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
