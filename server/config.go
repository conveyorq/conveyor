// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

// Package server assembles the conveyord application: configuration,
// broker, actor system, and API listeners. It is importable (the embedded
// mode reuses it) but is not part of the public SDK surface.
package server

import (
	"fmt"
	"math"
	"net/url"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/go-viper/mapstructure/v2"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/rawbytes"
	"github.com/knadh/koanf/providers/structs"
	"github.com/knadh/koanf/v2"
)

// Deployment modes. Embedded mode is a Go package, not a conveyord mode,
// so it does not appear here.
const (
	ModeStandalone = "standalone"
	ModeCluster    = "cluster"
	ModeKubernetes = "kubernetes"
)

// Broker drivers.
const (
	BrokerPostgres = "postgres"
	BrokerMemory   = "memory"
)

// Cluster discovery providers.
const (
	DiscoveryStatic     = "static"
	DiscoveryNATS       = "nats"
	DiscoveryConsul     = "consul"
	DiscoveryEtcd       = "etcd"
	DiscoveryMDNS       = "mdns"
	DiscoveryDNSSD      = "dnssd"
	DiscoveryKubernetes = "kubernetes"
)

// Log levels accepted by log.level.
const (
	LogLevelDebug = "debug"
	LogLevelInfo  = "info"
	LogLevelWarn  = "warn"
	LogLevelError = "error"
)

// Log output formats accepted by log.format.
const (
	LogFormatJSON = "json"
	LogFormatText = "text"
)

// envPrefix is the prefix for environment overrides; CONVEYOR_BROKER__DSN
// overrides broker.dsn ("__" separates nesting levels because key names
// themselves contain "_").
const (
	envPrefix       = "CONVEYOR_"
	envLevelDelim   = "__"
	configKeyDelim  = "."
	envListSepComma = ","
)

// koanfTag is the struct tag koanf reads configuration keys from.
const koanfTag = "koanf"

// maxPortNumber is the highest valid TCP port.
const maxPortNumber = 65535

// Configuration defaults applied before file and environment overrides.
const (
	defaultAPIListen     = ":8080"
	defaultBindAddr      = "127.0.0.1"
	defaultRemotingPort  = 9000
	defaultDiscoveryPort = 9001
	defaultPeersPort     = 9002
	// Kubernetes named container ports the discovery provider reads.
	defaultDiscoveryPortName = "gossip"
	defaultRemotingPortName  = "remoting"
	defaultPeersPortName     = "cluster"
	defaultLeaseTTL          = 60 * time.Second
	defaultLeaseBatchMax     = 100
	defaultResolverPoolSize  = 8
	defaultReapInterval      = 15 * time.Second
	defaultPromoteInterval   = time.Second
	defaultPassivateAfter    = 5 * time.Minute
	defaultMaxRetry          = 25
	defaultShutdownTimeout   = 30 * time.Second
	defaultGroupMaxSize      = 100
	defaultGroupMaxDelay     = time.Minute
	defaultGroupGracePeriod  = 10 * time.Second
	defaultGroupSweep        = time.Second
	defaultRateLimitEnabled  = true
	// defaultEventsEnabled is off: nothing consumes the stream out of the box, so
	// a production node pays no per-transition cost until an operator opts in
	// (a webhook or a live watcher). The --dev preset turns it on.
	defaultEventsEnabled   = false
	defaultOtelServiceName = "conveyord"
	// defaultMetricsListen is the OpenTelemetry Prometheus exporter's
	// conventional port; metrics bind here, kept off the public API listener.
	defaultMetricsListen = ":9464"
)

// Config is the full conveyord configuration.
// Precedence: flags > environment (CONVEYOR_*) > file > defaults.
type Config struct {
	// Mode selects the deployment mode: standalone, cluster, or kubernetes.
	Mode string `koanf:"mode"`
	// Broker configures the durable task log.
	Broker BrokerConfig `koanf:"broker"`
	// API configures the public ConnectRPC listener.
	API APIConfig `koanf:"api"`
	// Cluster configures GoAkt discovery and remoting.
	Cluster ClusterConfig `koanf:"cluster"`
	// Engine tunes dispatch, leasing, and maintenance loops.
	Engine EngineConfig `koanf:"engine"`
	// Log configures structured logging.
	Log LogConfig `koanf:"log"`
	// Otel configures OpenTelemetry export.
	Otel OtelConfig `koanf:"otel"`
	// Metrics configures the Prometheus metrics listener.
	Metrics MetricsConfig `koanf:"metrics"`
	// Events configures the task lifecycle event stream and optional webhook.
	Events EventsConfig `koanf:"events"`
}

// EventsConfig configures the task lifecycle event stream and the optional
// webhook sink.
type EventsConfig struct {
	// Enabled gates the whole event subsystem: the cluster-topic relay,
	// WatchEvents, and the webhook. It defaults on so the dashboard and external
	// watchers get a push channel; set it false to drop all event propagation.
	Enabled bool `koanf:"enabled"`
	// BufferSize is the per-watcher event buffer depth. A watcher that falls
	// further behind than this has events dropped rather than stalling dispatch.
	// Zero selects the built-in default.
	BufferSize int `koanf:"buffer_size"`
	// Webhook optionally posts every event to an HTTP endpoint.
	Webhook WebhookConfig `koanf:"webhook"`
}

// WebhookConfig configures the optional HTTP webhook sink.
type WebhookConfig struct {
	// URL is the endpoint events are POSTed to as JSON. Empty disables the sink.
	URL string `koanf:"url"`
	// Timeout bounds one delivery attempt; zero selects the default.
	Timeout time.Duration `koanf:"timeout"`
	// MaxRetries is the number of retries after a failed delivery; negative
	// selects the default.
	MaxRetries int `koanf:"max_retries"`
	// Secret, when set, is sent as an Authorization: Bearer header.
	Secret string `koanf:"secret"`
	// Queues restricts delivery to these queues; empty means every queue.
	Queues []string `koanf:"queues"`
	// EventTypes restricts delivery to these event types (proto enum names, e.g.
	// "TASK_EVENT_TYPE_ARCHIVED"); empty means every type.
	EventTypes []string `koanf:"event_types"`
}

// MetricsConfig configures the Prometheus metrics endpoint. Metrics are
// served on their own listener — never the public API listener — because the
// exposition includes internal topology (peer addresses, queue names) that
// should not ride on a client-facing, possibly internet-exposed port.
type MetricsConfig struct {
	// Listen is the address the /metrics endpoint binds, e.g. ":9464". An
	// empty value disables the endpoint and the meter provider entirely,
	// which embedded mode uses so it neither binds a port nor replaces the
	// host application's global OpenTelemetry provider.
	Listen string `koanf:"listen"`
}

// BrokerConfig selects and configures the broker driver.
type BrokerConfig struct {
	// Driver is the broker implementation: postgres or memory.
	Driver string `koanf:"driver"`
	// DSN is the database connection string (required for postgres).
	DSN string `koanf:"dsn"`
}

// TLSConfig points at a certificate/key pair; both fields are set or none.
type TLSConfig struct {
	// CertFile is the path to the PEM-encoded certificate.
	CertFile string `koanf:"cert_file"`
	// KeyFile is the path to the PEM-encoded private key.
	KeyFile string `koanf:"key_file"`
	// CAFile is the path to the PEM-encoded certificate authority bundle that
	// signs peer certificates. When set on cluster remoting it turns on mutual
	// TLS: each node verifies its peers against this CA. When empty, peer
	// certificates are verified against the host's system roots and client
	// certificates are not required.
	CAFile string `koanf:"ca_file"`
}

// APIConfig configures the public API listener.
type APIConfig struct {
	// Listen is the address the ConnectRPC server binds, e.g. ":8080".
	Listen string `koanf:"listen"`
	// TLS optionally enables TLS on the API port.
	TLS TLSConfig `koanf:"tls"`
	// AuthTokens are accepted bearer tokens. An empty list disables
	// authentication, which is intended for development only and logged
	// loudly at startup.
	AuthTokens []string `koanf:"auth_tokens"`
	// AllowUnauthenticated permits the API to run with authentication
	// disabled (no AuthTokens). It must be set explicitly: outside the
	// `--dev` preset, an empty AuthTokens without this flag fails validation,
	// so a deployment never serves an unauthenticated API by accident. Set it
	// only when another layer (a gateway, mTLS, or a private network) secures
	// the API.
	AllowUnauthenticated bool `koanf:"allow_unauthenticated"`
	// Dashboard serves the embedded read+write operations console at the API
	// root. It defaults on; set it false to expose the API without the UI
	// (for example when hosting the dashboard separately).
	Dashboard bool `koanf:"dashboard"`
	// CORSOrigins lists browser origins permitted to call the API
	// cross-origin, for hosting the dashboard on a different origin. Empty
	// disables CORS entirely (the secure default); an entry of "*" allows any
	// origin.
	CORSOrigins []string `koanf:"cors_origins"`
	// GrafanaURL, when set, is surfaced to the dashboard as a "Metrics" link to
	// the operator's Grafana. Empty hides the link.
	GrafanaURL string `koanf:"grafana_url"`
	// ReadOnly puts the admin API in read-only mode: inspection and listing
	// stay available, but every mutating operation (pause/resume, task and cron
	// actions) is rejected. The dashboard reads this flag and hides its action
	// controls. Task ingestion through the enqueue API is unaffected.
	ReadOnly bool `koanf:"read_only"`
}

// ClusterConfig configures GoAkt clustering.
type ClusterConfig struct {
	// Discovery selects the peer discovery provider.
	Discovery string `koanf:"discovery"`
	// BindAddr is the host the node binds and advertises to peers for
	// remoting, gossip, and the peers protocol. The default loopback serves
	// standalone (cluster-of-one) and dev; multi-node deployments must set a
	// routable address peers can dial.
	BindAddr string `koanf:"bind_addr"`
	// StaticPeers lists host:port peers for discovery=static; empty means
	// self-discovery (a cluster of one).
	StaticPeers []string `koanf:"static_peers"`
	// RemotingPort is the GoAkt remoting port.
	RemotingPort int `koanf:"remoting_port"`
	// DiscoveryPort is the gossip bootstrap port.
	DiscoveryPort int `koanf:"discovery_port"`
	// PeersPort is the cluster peers port.
	PeersPort int `koanf:"peers_port"`
	// TLS optionally enables mTLS on cluster remoting.
	TLS TLSConfig `koanf:"tls"`
	// Kubernetes configures the kubernetes discovery provider; required when
	// discovery is "kubernetes".
	Kubernetes KubernetesConfig `koanf:"kubernetes"`
	// Options carries free-form settings for a custom discovery provider
	// registered through RegisterDiscovery; the provider reads its own keys.
	Options map[string]string `koanf:"options"`
}

// KubernetesConfig configures GoAkt's Kubernetes discovery provider, which
// finds peers by listing pods. The port names must match the named container
// ports the node exposes for gossip, remoting, and the peers protocol.
type KubernetesConfig struct {
	// Namespace is the namespace the node's pods run in.
	Namespace string `koanf:"namespace"`
	// PodLabels selects the peer pods to discover; at least one is required.
	PodLabels map[string]string `koanf:"pod_labels"`
	// DiscoveryPortName is the named container port for gossip bootstrap.
	DiscoveryPortName string `koanf:"discovery_port_name"`
	// RemotingPortName is the named container port for remoting.
	RemotingPortName string `koanf:"remoting_port_name"`
	// PeersPortName is the named container port for the peers protocol.
	PeersPortName string `koanf:"peers_port_name"`
}

// EngineConfig tunes the dispatch and maintenance behavior.
type EngineConfig struct {
	// LeaseTTL is how long a task lease lives before the reaper may
	// reclaim it; workers extend leases via heartbeats.
	LeaseTTL time.Duration `koanf:"lease_ttl"`
	// LeaseBatchMax caps how many tasks one lease cycle may claim.
	LeaseBatchMax int `koanf:"lease_batch_max"`
	// ResolverPoolSize is the number of dependency-resolver routees per node.
	ResolverPoolSize int `koanf:"resolver_pool_size"`
	// ReapInterval is the cadence of the reaper singleton tick.
	ReapInterval time.Duration `koanf:"reap_interval"`
	// PromoteInterval is the cadence of scheduled-task promotion.
	PromoteInterval time.Duration `koanf:"promote_interval"`
	// PassivateAfter is the idle time before a queue grain deactivates.
	PassivateAfter time.Duration `koanf:"passivate_after"`
	// DefaultMaxRetry applies to tasks that do not set max_retry.
	DefaultMaxRetry int `koanf:"default_max_retry"`
	// ShutdownTimeout bounds graceful shutdown of the whole node.
	ShutdownTimeout time.Duration `koanf:"shutdown_timeout"`
	// GroupMaxSize fires an aggregation group once this many members
	// accumulate.
	GroupMaxSize int `koanf:"group_max_size"`
	// GroupMaxDelay fires a group this long after its first member.
	GroupMaxDelay time.Duration `koanf:"group_max_delay"`
	// GroupGracePeriod fires a group this long after its most recent member.
	GroupGracePeriod time.Duration `koanf:"group_grace_period"`
	// GroupSweepInterval is the cadence of the group-aggregation sweep.
	GroupSweepInterval time.Duration `koanf:"group_sweep_interval"`
	// RateLimitEnabled gates dispatch rate limiting; false disables it for
	// every queue regardless of any configured override.
	RateLimitEnabled bool `koanf:"rate_limit_enabled"`
	// RateLimitRatePerSec is the global default dispatch rate in tasks per
	// second; zero leaves queues unlimited unless they set an override.
	RateLimitRatePerSec float64 `koanf:"rate_limit_rate_per_sec"`
	// RateLimitBurst is the global default token-bucket depth.
	RateLimitBurst int `koanf:"rate_limit_burst"`
}

// LogConfig configures structured logging.
type LogConfig struct {
	// Level is one of debug, info, warn, error.
	Level string `koanf:"level"`
	// Format is json or text.
	Format string `koanf:"format"`
}

// OtelConfig configures OpenTelemetry export.
type OtelConfig struct {
	// Endpoint is the OTLP endpoint; empty disables export.
	Endpoint string `koanf:"endpoint"`
	// ServiceName overrides the reported service name.
	ServiceName string `koanf:"service_name"`
}

// DefaultConfig returns the standard defaults. The result does not validate
// as-is: the postgres driver requires a DSN, which has no sensible default.
func DefaultConfig() *Config {
	return &Config{
		Mode:   ModeStandalone,
		Broker: BrokerConfig{Driver: BrokerPostgres},
		API:    APIConfig{Listen: defaultAPIListen, Dashboard: true},
		Cluster: ClusterConfig{
			Discovery:     DiscoveryStatic,
			BindAddr:      defaultBindAddr,
			RemotingPort:  defaultRemotingPort,
			DiscoveryPort: defaultDiscoveryPort,
			PeersPort:     defaultPeersPort,
			Kubernetes: KubernetesConfig{
				DiscoveryPortName: defaultDiscoveryPortName,
				RemotingPortName:  defaultRemotingPortName,
				PeersPortName:     defaultPeersPortName,
			},
		},
		Engine: EngineConfig{
			LeaseTTL:           defaultLeaseTTL,
			LeaseBatchMax:      defaultLeaseBatchMax,
			ResolverPoolSize:   defaultResolverPoolSize,
			ReapInterval:       defaultReapInterval,
			PromoteInterval:    defaultPromoteInterval,
			PassivateAfter:     defaultPassivateAfter,
			DefaultMaxRetry:    defaultMaxRetry,
			ShutdownTimeout:    defaultShutdownTimeout,
			GroupMaxSize:       defaultGroupMaxSize,
			GroupMaxDelay:      defaultGroupMaxDelay,
			GroupGracePeriod:   defaultGroupGracePeriod,
			GroupSweepInterval: defaultGroupSweep,
			RateLimitEnabled:   defaultRateLimitEnabled,
		},
		Log:     LogConfig{Level: LogLevelInfo, Format: LogFormatJSON},
		Otel:    OtelConfig{ServiceName: defaultOtelServiceName},
		Metrics: MetricsConfig{Listen: defaultMetricsListen},
		Events:  EventsConfig{Enabled: defaultEventsEnabled},
	}
}

// DevConfig returns the `conveyord --dev` configuration: standalone mode,
// in-memory broker, authentication disabled, debug logging. Metrics bind an
// ephemeral port so repeated in-process starts (tests) never collide on the
// fixed default.
func DevConfig() *Config {
	config := DefaultConfig()
	config.Broker = BrokerConfig{Driver: BrokerMemory}
	config.Log = LogConfig{Level: LogLevelDebug, Format: LogFormatText}
	config.Metrics = MetricsConfig{Listen: "127.0.0.1:0"}
	// Dev runs without authentication by design; opt in explicitly so the
	// fail-closed auth check in Validate accepts it.
	config.API.AllowUnauthenticated = true
	// Dev turns the lifecycle event stream on so the local experience (and
	// `conveyor events`) is events-first; production leaves it off by default.
	config.Events.Enabled = true

	return config
}

// LoadConfig builds a Config from defaults, an optional YAML file, and
// CONVEYOR_* environment variables (in increasing precedence), then
// validates it. ${VAR} references inside the file are expanded from the
// environment before parsing.
func LoadConfig(path string) (*Config, error) {
	return loadConfig(DefaultConfig(), path)
}

// LoadDevConfig builds the `--dev` configuration: the DevConfig preset with
// CONVEYOR_* environment overrides still applied on top.
func LoadDevConfig() (*Config, error) {
	return loadConfig(DevConfig(), "")
}

// loadConfig layers an optional YAML file and CONVEYOR_* environment
// variables over the given base, then validates the result.
func loadConfig(base *Config, path string) (*Config, error) {
	k := koanf.New(configKeyDelim)

	if err := k.Load(structs.Provider(base, koanfTag), nil); err != nil {
		return nil, fmt.Errorf("loading defaults: %w", err)
	}

	if path != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("reading config file: %w", err)
		}

		expanded := []byte(os.ExpandEnv(string(raw)))
		if err := k.Load(rawbytes.Provider(expanded), yaml.Parser()); err != nil {
			return nil, fmt.Errorf("parsing config file %s: %w", path, err)
		}
	}

	if err := k.Load(env.Provider(envPrefix, configKeyDelim, envKeyToConfigKey), nil); err != nil {
		return nil, fmt.Errorf("loading environment overrides: %w", err)
	}

	config := &Config{}

	unmarshalConf := koanf.UnmarshalConf{
		Tag: koanfTag,
		DecoderConfig: &mapstructure.DecoderConfig{
			Result: config,
			DecodeHook: mapstructure.ComposeDecodeHookFunc(
				mapstructure.StringToTimeDurationHookFunc(),
				mapstructure.StringToSliceHookFunc(envListSepComma),
			),
			WeaklyTypedInput: true,
		},
	}

	if err := k.UnmarshalWithConf("", config, unmarshalConf); err != nil {
		return nil, fmt.Errorf("decoding config: %w", err)
	}

	if err := config.Validate(); err != nil {
		return nil, err
	}

	return config, nil
}

// envKeyToConfigKey maps CONVEYOR_BROKER__DSN to broker.dsn: strip the
// prefix, lowercase, and treat "__" as the nesting separator so key names
// containing "_" (e.g. auth_tokens) survive.
func envKeyToConfigKey(envKey string) string {
	key := strings.TrimPrefix(envKey, envPrefix)
	key = strings.ToLower(key)

	return strings.ReplaceAll(key, envLevelDelim, configKeyDelim)
}

// validateRateLimitDefault checks the global default dispatch-rate knobs: the
// rate must be a non-negative finite number, and a positive rate needs a burst
// of at least one (a zero burst would silently disable the default).
func validateRateLimitDefault(engine EngineConfig) error {
	if engine.RateLimitRatePerSec < 0 || math.IsNaN(engine.RateLimitRatePerSec) || math.IsInf(engine.RateLimitRatePerSec, 0) {
		return fmt.Errorf("engine.rate_limit_rate_per_sec: must be a non-negative finite number, got %v", engine.RateLimitRatePerSec)
	}

	if engine.RateLimitRatePerSec > 0 && engine.RateLimitBurst < 1 {
		return fmt.Errorf("engine.rate_limit_burst: must be at least 1 when a default rate is set, got %d", engine.RateLimitBurst)
	}

	return nil
}

// validateEvents checks the optional webhook endpoint: when set it must be a
// valid absolute http(s) URL, and the retry budget must not be negative.
func validateEvents(events EventsConfig) error {
	if events.Webhook.MaxRetries < 0 {
		return fmt.Errorf("events.webhook.max_retries: must not be negative, got %d", events.Webhook.MaxRetries)
	}

	raw := events.Webhook.URL
	if raw == "" {
		return nil
	}

	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return fmt.Errorf("events.webhook.url: must be a valid http(s) URL, got %q", raw)
	}

	return nil
}

// Validate checks the configuration for internal consistency and returns a
// descriptive error naming the offending key.
func (c *Config) Validate() error {
	modes := []string{ModeStandalone, ModeCluster, ModeKubernetes}
	if !slices.Contains(modes, c.Mode) {
		return fmt.Errorf("mode: %q is not one of %v", c.Mode, modes)
	}

	drivers := []string{BrokerPostgres, BrokerMemory}
	if !slices.Contains(drivers, c.Broker.Driver) {
		return fmt.Errorf("broker.driver: %q is not one of %v", c.Broker.Driver, drivers)
	}

	if c.Broker.Driver == BrokerPostgres && c.Broker.DSN == "" {
		return fmt.Errorf("broker.dsn: required when broker.driver is %q", BrokerPostgres)
	}

	if c.API.Listen == "" {
		return fmt.Errorf("api.listen: must not be empty")
	}

	if err := c.API.TLS.validate("api.tls"); err != nil {
		return err
	}

	if c.AuthDisabled() && !c.API.AllowUnauthenticated {
		return fmt.Errorf("api.auth_tokens: set at least one token, or set api.allow_unauthenticated to run the API without authentication (the --dev preset does this)")
	}

	providers := []string{
		DiscoveryStatic, DiscoveryNATS, DiscoveryConsul, DiscoveryEtcd,
		DiscoveryMDNS, DiscoveryDNSSD, DiscoveryKubernetes,
	}

	if !slices.Contains(providers, c.Cluster.Discovery) {
		if _, ok := lookupDiscovery(c.Cluster.Discovery); !ok {
			return fmt.Errorf("cluster.discovery: %q is not a built-in provider %v and no custom provider is registered under that name", c.Cluster.Discovery, providers)
		}
	}

	if c.Cluster.BindAddr == "" {
		return fmt.Errorf("cluster.bind_addr: must not be empty")
	}

	if c.Cluster.Discovery == DiscoveryKubernetes {
		if c.Cluster.Kubernetes.Namespace == "" {
			return fmt.Errorf("cluster.kubernetes.namespace: required when discovery is %q", DiscoveryKubernetes)
		}

		if len(c.Cluster.Kubernetes.PodLabels) == 0 {
			return fmt.Errorf("cluster.kubernetes.pod_labels: at least one label is required when discovery is %q", DiscoveryKubernetes)
		}
	}

	ports := map[string]int{
		"cluster.remoting_port":  c.Cluster.RemotingPort,
		"cluster.discovery_port": c.Cluster.DiscoveryPort,
		"cluster.peers_port":     c.Cluster.PeersPort,
	}

	for key, port := range ports {
		if port <= 0 || port > maxPortNumber {
			return fmt.Errorf("%s: %d is not a valid port", key, port)
		}
	}

	if err := c.Cluster.TLS.validate("cluster.tls"); err != nil {
		return err
	}

	durations := map[string]time.Duration{
		"engine.lease_ttl":            c.Engine.LeaseTTL,
		"engine.reap_interval":        c.Engine.ReapInterval,
		"engine.promote_interval":     c.Engine.PromoteInterval,
		"engine.passivate_after":      c.Engine.PassivateAfter,
		"engine.shutdown_timeout":     c.Engine.ShutdownTimeout,
		"engine.group_max_delay":      c.Engine.GroupMaxDelay,
		"engine.group_grace_period":   c.Engine.GroupGracePeriod,
		"engine.group_sweep_interval": c.Engine.GroupSweepInterval,
	}

	for key, d := range durations {
		if d <= 0 {
			return fmt.Errorf("%s: must be positive, got %s", key, d)
		}
	}

	if c.Engine.LeaseBatchMax <= 0 {
		return fmt.Errorf("engine.lease_batch_max: must be positive, got %d", c.Engine.LeaseBatchMax)
	}

	if c.Engine.GroupMaxSize <= 0 {
		return fmt.Errorf("engine.group_max_size: must be positive, got %d", c.Engine.GroupMaxSize)
	}

	if c.Engine.DefaultMaxRetry < 0 {
		return fmt.Errorf("engine.default_max_retry: must not be negative, got %d", c.Engine.DefaultMaxRetry)
	}

	if err := validateRateLimitDefault(c.Engine); err != nil {
		return err
	}

	if err := validateEvents(c.Events); err != nil {
		return err
	}

	levels := []string{LogLevelDebug, LogLevelInfo, LogLevelWarn, LogLevelError}
	if !slices.Contains(levels, c.Log.Level) {
		return fmt.Errorf("log.level: %q is not one of %v", c.Log.Level, levels)
	}

	formats := []string{LogFormatJSON, LogFormatText}
	if !slices.Contains(formats, c.Log.Format) {
		return fmt.Errorf("log.format: %q is not one of %v", c.Log.Format, formats)
	}

	return nil
}

// AuthDisabled reports whether the API accepts unauthenticated requests.
func (c *Config) AuthDisabled() bool {
	return len(c.API.AuthTokens) == 0
}

// validate checks that a TLS block names both halves of the pair or neither.
func (t *TLSConfig) validate(key string) error {
	if (t.CertFile == "") != (t.KeyFile == "") {
		return fmt.Errorf("%s: cert_file and key_file must be set together", key)
	}

	return nil
}
