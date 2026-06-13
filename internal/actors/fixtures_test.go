package actors

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	goakt "github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/discovery/static"
	gtls "github.com/tochemey/goakt/v4/tls"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/conveyorq/conveyor/internal/broker"
	"github.com/conveyorq/conveyor/internal/clock"
	"github.com/conveyorq/conveyor/internal/dynaport"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// testBindAddr is the loopback host every test node binds to.
const testBindAddr = "127.0.0.1"

// gatewayHeartbeat is the test gateway re-registration cadence: the
// in-test equivalent of the production gateway heartbeat that re-arms a
// relocated queue grain with credits.
const gatewayHeartbeat = 500 * time.Millisecond

// testSettings are fast engine settings for single-node tests.
var testSettings = Settings{
	LeaseTTL:        30 * time.Second,
	LeaseBatchMax:   100,
	ReapInterval:    200 * time.Millisecond,
	PromoteInterval: 100 * time.Millisecond,
	PassivateAfter:  5 * time.Minute,
}

// recoverySettings shorten the lease TTL and maintenance cadence so
// re-delivery after a node death happens within test time.
var recoverySettings = Settings{
	LeaseTTL:        2 * time.Second,
	LeaseBatchMax:   100,
	ReapInterval:    300 * time.Millisecond,
	PromoteInterval: 100 * time.Millisecond,
	PassivateAfter:  5 * time.Minute,
}

// freePorts reserves n distinct free loopback ports.
func freePorts(t *testing.T, n int) []int {
	t.Helper()

	ports, err := dynaport.Get(n)
	require.NoError(t, err)

	return ports
}

// quietLogger discards everything below error so engine debug logs do not
// drown test output.
func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(discardWriter{}, &slog.HandlerOptions{Level: slog.LevelError}))
}

// discardWriter throws log output away.
type discardWriter struct{}

// Write implements io.Writer.
func (discardWriter) Write(p []byte) (int, error) {
	return len(p), nil
}

// newNode builds (without starting) an engine node on the given ports. The
// discovery provider is built here, as the server layer does from config:
// static with the given gossip peers, or self-discovery when peers is nil.
func newNode(taskLog broker.Broker, settings Settings, ports []int, peers []string) *Engine {
	if len(peers) == 0 {
		peers = []string{fmt.Sprintf("%s:%d", testBindAddr, ports[1])}
	}

	return NewEngine(taskLog, clock.System(), quietLogger(), Config{
		Name:          "conveyor-test",
		BindAddr:      testBindAddr,
		RemotingPort:  ports[0],
		DiscoveryPort: ports[1],
		PeersPort:     ports[2],
		Provider:      static.NewDiscovery(&static.Config{Hosts: peers}),
		Settings:      settings,
	})
}

// newLoopbackTLS builds a mutual-TLS info for cluster remoting on the
// loopback host. It mints one self-signed CA certificate valid for both
// server and client authentication and trusts it on both sides, mirroring
// a real deployment where every node shares a certificate authority: a
// node presents the certificate and verifies its peers against the same CA.
func newLoopbackTLS(t *testing.T) *gtls.Info {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "conveyor-test"},
		NotBefore:    clock.System().Now().Add(-time.Hour),
		NotAfter:     clock.System().Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses:  []net.IP{net.ParseIP(testBindAddr)},
		IsCA:         true,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)

	parsed, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	pool := x509.NewCertPool()
	pool.AddCert(parsed)

	certificate := tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}

	return &gtls.Info{
		ServerConfig: &tls.Config{
			Certificates: []tls.Certificate{certificate},
			ClientCAs:    pool,
			ClientAuth:   tls.RequireAndVerifyClientCert,
			MinVersion:   tls.VersionTLS13,
		},
		ClientConfig: &tls.Config{
			Certificates: []tls.Certificate{certificate},
			RootCAs:      pool,
			MinVersion:   tls.VersionTLS13,
		},
	}
}

// startEngine boots a single-node engine on free ports and stops it with
// the test.
func startEngine(t *testing.T, taskLog broker.Broker) *Engine {
	t.Helper()

	engine := newNode(taskLog, testSettings, freePorts(t, 3), nil)
	require.NoError(t, engine.Start(context.Background()))

	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = engine.Stop(stopCtx)
	})

	return engine
}

// killNode stops a node with an already-expired context, forbidding any
// graceful drain: leased tasks stay active in the broker exactly as after
// kill -9.
func killNode(node *Engine) {
	killCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = node.Stop(killCtx)
}

// tasksInState counts tasks in one state through the broker. It returns
// the error instead of failing the test so it can run inside
// require.Eventually conditions, which poll from a non-test goroutine.
func tasksInState(taskLog broker.Broker, state conveyorv1.TaskState) (int, error) {
	tasks, err := taskLog.ListTasks(context.Background(), broker.TaskQuery{State: state, Limit: broker.MaxListLimit})
	if err != nil {
		return 0, err
	}

	return len(tasks), nil
}

// completedReaches returns a require.Eventually condition that holds once
// the broker reports at least want completed tasks (exactly want when
// exact is desired, pass want and compare outside for stricter checks).
func completedReaches(taskLog broker.Broker, want int) func() bool {
	return func() bool {
		count, err := tasksInState(taskLog, conveyorv1.TaskState_TASK_STATE_COMPLETED)

		return err == nil && count >= want
	}
}

// requireDrained asserts that no task is left behind in any non-terminal
// or dead-letter state.
func requireDrained(t *testing.T, taskLog broker.Broker) {
	t.Helper()

	for _, state := range []conveyorv1.TaskState{
		conveyorv1.TaskState_TASK_STATE_PENDING,
		conveyorv1.TaskState_TASK_STATE_ACTIVE,
		conveyorv1.TaskState_TASK_STATE_RETRY,
		conveyorv1.TaskState_TASK_STATE_ARCHIVED,
	} {
		count, err := tasksInState(taskLog, state)
		require.NoError(t, err)
		require.Zerof(t, count, "%d tasks left in %s", count, state)
	}
}

// newTask builds a test envelope. Retention keeps completed rows visible
// to test assertions; the reaper purges zero-retention rows on its fast
// test tick.
func newTask(id, queue, taskType string, priority int32) *conveyorv1.TaskEnvelope {
	return &conveyorv1.TaskEnvelope{
		Id:          id,
		Queue:       queue,
		Type:        taskType,
		Payload:     []byte(`{}`),
		ContentType: "application/json",
		Options:     &conveyorv1.TaskOptions{MaxRetry: 25, Priority: priority, Retention: durationpb.New(time.Hour)},
	}
}

// enqueueTasks enqueues count sequential tasks on one queue through the
// engine, with ids task-00000..task-<count-1> and priority sequence%10.
func enqueueTasks(t *testing.T, engine *Engine, queue string, count int) {
	t.Helper()

	for sequence := range count {
		task := newTask(fmt.Sprintf("task-%05d", sequence), queue, "test:ok", int32(sequence%10))
		require.NoError(t, engine.Enqueue(context.Background(), task))
	}
}

// dispatchLog records the order tasks reach a gateway.
type dispatchLog struct {
	// mutex guards ids.
	mutex sync.Mutex
	// ids are task ids in dispatch order.
	ids []string
}

// record appends one dispatched task id.
func (d *dispatchLog) record(id string) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	d.ids = append(d.ids, id)
}

// snapshot copies the dispatch order.
func (d *dispatchLog) snapshot() []string {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	return append([]string(nil), d.ids...)
}

// reRegisterTick re-announces a gateway to its queue grain. It is a plain
// local message: the system scheduler delivers it without crossing the
// serialization boundary.
type reRegisterTick struct{}

// mockGateway stands in for a Phase 3 worker gateway: it registers with
// its queue grain, executes dispatched tasks through a handler function,
// records the durable outcome, and reports completion (which doubles as
// the credit refill).
type mockGateway struct {
	// runtime is the engine runtime (broker, clock).
	runtime *Runtime
	// queue is the queue this gateway serves.
	queue string
	// capacity is the declared concurrency.
	capacity int32
	// handler decides each execution's outcome; nil always succeeds.
	handler func(task *conveyorv1.TaskEnvelope) error
	// log optionally records dispatch order.
	log *dispatchLog
	// identity is the queue grain identity, resolved at start.
	identity *goakt.GrainIdentity
	// name is this gateway's actor name; empty derives gateway-<queue>.
	name string
}

// enforce interface compliance at compile time.
var _ goakt.Actor = (*mockGateway)(nil)

// PreStart implements goakt.Actor.
func (m *mockGateway) PreStart(_ *goakt.Context) error {
	return nil
}

// Receive registers on start and executes dispatched tasks.
func (m *mockGateway) Receive(ctx *goakt.ReceiveContext) {
	switch message := ctx.Message().(type) {
	case *goakt.PostStart:
		m.name = ctx.Self().Name()
		m.register(ctx)

	case *conveyorv1.ExecuteTask:
		m.execute(ctx, message)

	case reRegisterTick:
		// Heartbeat re-registration: a freshly relocated queue grain has
		// no gateway state until a gateway announces itself again.
		m.register(ctx)

	default:
		ctx.Unhandled()
	}
}

// PostStop implements goakt.Actor.
func (m *mockGateway) PostStop(_ *goakt.Context) error {
	return nil
}

// register announces this gateway and its capacity to the queue grain.
func (m *mockGateway) register(ctx *goakt.ReceiveContext) {
	background := context.Background()
	system := ctx.ActorSystem()

	identity, err := system.GrainIdentity(background, QueueGrainName(m.queue), queueGrainFactory,
		goakt.WithGrainDeactivateAfter(m.runtime.Settings().PassivateAfter))
	if err != nil {
		ctx.Err(fmt.Errorf("resolving queue grain: %w", err))

		return
	}

	m.identity = identity

	err = system.TellGrain(background, identity, &conveyorv1.RegisterGateway{
		Queue:       m.queue,
		GatewayName: m.name,
		Capacity:    m.capacity,
	})
	if err != nil {
		ctx.Err(fmt.Errorf("registering gateway: %w", err))
	}
}

// execute runs the handler and records the durable outcome, then reports
// completion — which doubles as the credit refill — to the grain.
func (m *mockGateway) execute(ctx *goakt.ReceiveContext, message *conveyorv1.ExecuteTask) {
	background := context.Background()
	task := message.GetTask()
	taskLog := m.runtime.Broker()

	if m.log != nil {
		m.log.record(task.GetId())
	}

	var handlerErr error
	if m.handler != nil {
		handlerErr = m.handler(task)
	}

	var err error
	if handlerErr == nil {
		err = taskLog.Ack(background, task.GetId(), message.GetLeaseId(), nil)
	} else {
		err = taskLog.Fail(background, task.GetId(), message.GetLeaseId(), handlerErr.Error(), m.runtime.Clock().Now())
	}

	if err != nil {
		m.runtime.Logger().Warn("gateway durable transition failed", "task_id", task.GetId(), "error", err)
	}

	completed := &conveyorv1.TaskCompleted{
		TaskId:      task.GetId(),
		Queue:       m.queue,
		Success:     handlerErr == nil,
		GatewayName: m.name,
	}
	if err = ctx.ActorSystem().TellGrain(background, m.identity, completed); err != nil {
		m.runtime.Logger().Warn("completion report failed", "task_id", task.GetId(), "error", err)
	}
}

// spawnGateway starts a mock gateway actor and its heartbeat. A non-empty
// gateway.name overrides the default actor name so one queue can be
// served by several gateways.
func spawnGateway(t *testing.T, engine *Engine, gateway *mockGateway) {
	t.Helper()

	gateway.runtime = engine.runtime

	name := gateway.name
	if name == "" {
		name = "gateway-" + gateway.queue
	}

	pid, err := engine.System().Spawn(context.Background(), name, gateway,
		goakt.WithLongLived(), goakt.WithRelocationDisabled())
	require.NoError(t, err)

	require.NoError(t, engine.System().Schedule(context.Background(), reRegisterTick{}, pid, gatewayHeartbeat))
}
