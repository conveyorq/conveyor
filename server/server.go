package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"connectrpc.com/connect"
	"github.com/tochemey/goakt/v4/discovery"
	"github.com/tochemey/goakt/v4/discovery/kubernetes"
	"github.com/tochemey/goakt/v4/discovery/static"

	"github.com/conveyorq/conveyor/internal/actors"
	"github.com/conveyorq/conveyor/internal/broker"
	"github.com/conveyorq/conveyor/internal/broker/memory"
	"github.com/conveyorq/conveyor/internal/broker/postgres"
	"github.com/conveyorq/conveyor/internal/clock"
	"github.com/conveyorq/conveyor/internal/proto/conveyor/v1/conveyorv1connect"
	"github.com/conveyorq/conveyor/server/api"
)

// Health endpoints. healthz reports process liveness only; readyz reports
// readiness: the broker answers and the engine is running.
const (
	healthzPath = "/healthz"
	readyzPath  = "/readyz"
)

// healthzBody is the response body of a passing liveness probe.
const healthzBody = "ok"

// readHeaderTimeout bounds header parsing on the API listener so idle or
// slow-loris connections cannot pin server goroutines.
const readHeaderTimeout = 10 * time.Second

// readyzTimeout bounds the broker probe of one readiness check.
const readyzTimeout = 2 * time.Second

// systemName is the actor system name; every node of a cluster shares it.
const systemName = "conveyor"

// Server is one conveyord node: the broker, the engine (actor system),
// and the API listener serving the ConnectRPC services.
type Server struct {
	// config is the validated node configuration.
	config *Config
	// logger is the process logger shared by all components.
	logger *slog.Logger
	// http serves the API mux that ConnectRPC handlers mount on.
	http *http.Server
	// listener is the bound API listener; nil until Start succeeds.
	listener net.Listener
	// taskLog is the durable task log; nil until Start succeeds.
	taskLog broker.Broker
	// engine is the coordination layer; nil until Start succeeds.
	engine *actors.Engine
	// workerService holds the live worker sessions; nil until Start
	// succeeds. The engine drains it during coordinated shutdown.
	workerService *api.WorkerService
}

// New assembles a Server from a validated configuration. All components
// boot in Start: broker and engine construction need a context.
func New(config *Config, logger *slog.Logger) (*Server, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return &Server{
		config: config,
		logger: logger,
		http:   &http.Server{ReadHeaderTimeout: readHeaderTimeout},
	}, nil
}

// Start boots the broker, the engine, and the API listener. It returns
// once the listener is bound; serving continues in the background.
func (s *Server) Start(ctx context.Context) error {
	taskLog, err := s.buildBroker(ctx)
	if err != nil {
		return err
	}

	s.taskLog = taskLog

	provider, err := s.buildDiscovery()
	if err != nil {
		return err
	}

	engine := actors.NewEngine(taskLog, clock.System(), s.logger, actors.Config{
		Name:          systemName,
		BindAddr:      s.config.Cluster.BindAddr,
		RemotingPort:  s.config.Cluster.RemotingPort,
		DiscoveryPort: s.config.Cluster.DiscoveryPort,
		PeersPort:     s.config.Cluster.PeersPort,
		Provider:      provider,
		Settings: actors.Settings{
			LeaseTTL:        s.config.Engine.LeaseTTL,
			LeaseBatchMax:   s.config.Engine.LeaseBatchMax,
			ReapInterval:    s.config.Engine.ReapInterval,
			PromoteInterval: s.config.Engine.PromoteInterval,
			PassivateAfter:  s.config.Engine.PassivateAfter,
		},
	})

	if err := engine.Start(ctx); err != nil {
		return fmt.Errorf("starting engine: %w", err)
	}

	s.engine = engine

	listener, err := net.Listen("tcp", s.config.API.Listen)
	if err != nil {
		return fmt.Errorf("binding API listener on %s: %w", s.config.API.Listen, err)
	}

	s.listener = listener

	// Unencrypted HTTP/2 carries the gRPC and bidi-stream protocols over
	// cleartext; with TLS configured, ALPN negotiates HTTP/2 natively.
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetHTTP2(true)
	protocols.SetUnencryptedHTTP2(true)

	s.http.Protocols = protocols
	s.http.Handler = s.buildMux()

	if s.config.AuthDisabled() {
		s.logger.Warn("API authentication is DISABLED — acceptable for development only; set api.auth_tokens for any non-loopback deployment")
	}

	go func() {
		var serveErr error

		if s.config.API.TLS.CertFile != "" {
			serveErr = s.http.ServeTLS(listener, s.config.API.TLS.CertFile, s.config.API.TLS.KeyFile)
		} else {
			serveErr = s.http.Serve(listener)
		}

		if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			s.logger.Error("API server terminated unexpectedly", "error", serveErr)
		}
	}()

	s.logger.Info("conveyord started", "mode", s.config.Mode, "listen", listener.Addr().String(), "broker", s.config.Broker.Driver)

	return nil
}

// buildBroker constructs the configured broker driver.
func (s *Server) buildBroker(ctx context.Context) (broker.Broker, error) {
	switch s.config.Broker.Driver {
	case BrokerMemory:
		return memory.New(clock.System()), nil

	case BrokerPostgres:
		taskLog, err := postgres.New(ctx, s.config.Broker.DSN, clock.System())
		if err != nil {
			return nil, fmt.Errorf("connecting postgres broker: %w", err)
		}

		return taskLog, nil

	default:
		return nil, fmt.Errorf("broker.driver: unknown driver %q", s.config.Broker.Driver)
	}
}

// buildDiscovery constructs the configured cluster discovery provider.
// Only static discovery is wired so far; a node with no static peers
// discovers itself, forming a cluster of one.
func (s *Server) buildDiscovery() (discovery.Provider, error) {
	switch s.config.Cluster.Discovery {
	case DiscoveryStatic:
		hosts := s.config.Cluster.StaticPeers

		if len(hosts) == 0 {
			hosts = []string{fmt.Sprintf("%s:%d", s.config.Cluster.BindAddr, s.config.Cluster.DiscoveryPort)}
		}

		return static.NewDiscovery(&static.Config{Hosts: hosts}), nil

	case DiscoveryKubernetes:
		k8s := s.config.Cluster.Kubernetes

		return kubernetes.NewDiscovery(&kubernetes.Config{
			Namespace:         k8s.Namespace,
			PodLabels:         k8s.PodLabels,
			DiscoveryPortName: k8s.DiscoveryPortName,
			RemotingPortName:  k8s.RemotingPortName,
			PeersPortName:     k8s.PeersPortName,
		}), nil

	default:
		return nil, fmt.Errorf("cluster.discovery: provider %q is not wired yet; use %q or %q", s.config.Cluster.Discovery, DiscoveryStatic, DiscoveryKubernetes)
	}
}

// buildMux assembles the API mux: health endpoints and the ConnectRPC
// services, with bearer-token authentication when tokens are configured.
func (s *Server) buildMux() *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc(healthzPath, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(healthzBody))
	})

	mux.HandleFunc(readyzPath, s.readyz)

	var options []connect.HandlerOption
	if !s.config.AuthDisabled() {
		options = append(options, connect.WithInterceptors(api.NewAuthInterceptor(s.config.API.AuthTokens)))
	}

	taskService := api.NewTaskService(s.engine, s.taskLog, clock.System(), int32(s.config.Engine.DefaultMaxRetry))
	mux.Handle(conveyorv1connect.NewTaskServiceHandler(taskService, options...))

	s.workerService = api.NewWorkerService(s.engine, s.logger)
	mux.Handle(conveyorv1connect.NewWorkerServiceHandler(s.workerService, options...))

	adminService := api.NewAdminService(s.engine, s.taskLog, clock.System())
	mux.Handle(conveyorv1connect.NewAdminServiceHandler(adminService, options...))

	return mux
}

// readyz reports readiness: the engine is running and the broker answers.
func (s *Server) readyz(w http.ResponseWriter, request *http.Request) {
	probeCtx, cancel := context.WithTimeout(request.Context(), readyzTimeout)
	defer cancel()

	if s.engine == nil || s.taskLog == nil {
		http.Error(w, "engine not started", http.StatusServiceUnavailable)

		return
	}

	if _, err := s.taskLog.PendingCount(probeCtx); err != nil {
		http.Error(w, "broker unavailable", http.StatusServiceUnavailable)

		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(healthzBody))
}

// Addr returns the bound API address; empty before Start.
func (s *Server) Addr() string {
	if s.listener == nil {
		return ""
	}

	return s.listener.Addr().String()
}

// Stop gracefully shuts the server down, honoring the context deadline:
// worker sessions drain first (releasing in-flight tasks for redelivery
// elsewhere), then the API listener and the engine stop, and the broker
// closes last. Stop is idempotent.
func (s *Server) Stop(ctx context.Context) error {
	s.logger.Info("conveyord stopping")

	var errs []error

	// The drain must complete before the engine stops: GoAkt rejects all
	// user messages once its stop sequence begins, after which gateways
	// can no longer release their in-flight tasks.
	if s.workerService != nil {
		if err := s.workerService.DrainSessions(ctx); err != nil {
			errs = append(errs, fmt.Errorf("draining worker sessions: %w", err))
		}
	}

	// Shutdown closes the listener immediately; the drained session
	// streams have already ended, so only short unary requests remain.
	httpDone := make(chan error, 1)

	go func() {
		httpDone <- s.http.Shutdown(ctx)
	}()

	if s.engine != nil {
		if err := s.engine.Stop(ctx); err != nil {
			errs = append(errs, fmt.Errorf("stopping engine: %w", err))
		} else {
			s.engine = nil
		}
	}

	if err := <-httpDone; err != nil {
		errs = append(errs, fmt.Errorf("stopping API server: %w", err))
	}

	if s.taskLog != nil {
		if err := s.taskLog.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing broker: %w", err))
		} else {
			s.taskLog = nil
		}
	}

	return errors.Join(errs...)
}

// NewLogger builds the process logger from the log configuration.
func NewLogger(config LogConfig) *slog.Logger {
	var level slog.Level

	switch config.Level {
	case LogLevelDebug:
		level = slog.LevelDebug
	case LogLevelWarn:
		level = slog.LevelWarn
	case LogLevelError:
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	options := &slog.HandlerOptions{Level: level}

	var handler slog.Handler
	if config.Format == LogFormatText {
		handler = slog.NewTextHandler(os.Stderr, options)
	} else {
		handler = slog.NewJSONHandler(os.Stderr, options)
	}

	return slog.New(handler)
}
