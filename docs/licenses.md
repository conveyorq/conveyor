# Third-party license inventory

Conveyor is licensed under [Apache-2.0](../LICENSE). This file inventories the
licenses of its compiled dependencies and records that all of them are
compatible with redistribution under Apache-2.0.

Regenerate with:

```sh
make licenses
```

(which runs `go-licenses report` over the server, CLI, SDK, and embedded
packages). Update this file whenever the dependency set changes.

## Summary

| License      | Count | Apache-2.0 compatible                               |
|--------------|------:|-----------------------------------------------------|
| Apache-2.0   |    61 | yes                                                 |
| MIT          |    40 | yes                                                 |
| BSD-3-Clause |    24 | yes                                                 |
| MPL-2.0      |     7 | yes (file-level copyleft; redistributed unmodified) |
| BSD-2-Clause |     5 | yes                                                 |
| ISC          |     1 | yes                                                 |

All licenses are permissive or file-level weak-copyleft and impose no condition
that conflicts with distributing Conveyor under Apache-2.0. The seven MPL-2.0
modules are HashiCorp clustering libraries (`memberlist` and its dependencies,
pulled in transitively through GoAkt); MPL-2.0 governs its own files only and is
satisfied by redistributing those modules unmodified, which Conveyor does.

The dependencies named in the relicensing plan check out:
GoAkt (`github.com/tochemey/goakt/v4`) is MIT, ConnectRPC
(`connectrpc.com/connect`) is Apache-2.0, and pgx (`github.com/jackc/pgx/v5`)
is MIT.

## Inventory

| Module                                                                         | License      |
|--------------------------------------------------------------------------------|--------------|
| `connectrpc.com/connect`                                                       | Apache-2.0   |
| `github.com/RoaringBitmap/roaring`                                             | Apache-2.0   |
| `github.com/Workiva/go-datastructures/queue`                                   | Apache-2.0   |
| `github.com/andybalholm/brotli/flate`                                          | BSD-3-Clause |
| `github.com/andybalholm/brotli`                                                | MIT          |
| `github.com/armon/go-metrics`                                                  | MIT          |
| `github.com/beorn7/perks/quantile`                                             | MIT          |
| `github.com/bits-and-blooms/bitset`                                            | BSD-3-Clause |
| `github.com/bytedance/gopkg/lang/dirtmake`                                     | Apache-2.0   |
| `github.com/bytedance/sonic`                                                   | Apache-2.0   |
| `github.com/cenkalti/backoff/v5`                                               | MIT          |
| `github.com/cespare/xxhash/v2`                                                 | MIT          |
| `github.com/conveyorq/conveyor`                                                | Apache-2.0   |
| `github.com/davecgh/go-spew/spew`                                              | ISC          |
| `github.com/deckarep/golang-set/v2`                                            | MIT          |
| `github.com/emicklei/go-restful/v3`                                            | MIT          |
| `github.com/fatih/structs`                                                     | MIT          |
| `github.com/flowchartsman/retry`                                               | MIT          |
| `github.com/fxamacker/cbor/v2`                                                 | MIT          |
| `github.com/go-logr/logr`                                                      | Apache-2.0   |
| `github.com/go-logr/stdr`                                                      | Apache-2.0   |
| `github.com/go-openapi/jsonpointer`                                            | Apache-2.0   |
| `github.com/go-openapi/jsonreference`                                          | Apache-2.0   |
| `github.com/go-openapi/swag/cmdutils`                                          | Apache-2.0   |
| `github.com/go-openapi/swag/conv`                                              | Apache-2.0   |
| `github.com/go-openapi/swag/fileutils`                                         | Apache-2.0   |
| `github.com/go-openapi/swag/jsonname`                                          | Apache-2.0   |
| `github.com/go-openapi/swag/jsonutils`                                         | Apache-2.0   |
| `github.com/go-openapi/swag/loading`                                           | Apache-2.0   |
| `github.com/go-openapi/swag/mangling`                                          | Apache-2.0   |
| `github.com/go-openapi/swag/netutils`                                          | Apache-2.0   |
| `github.com/go-openapi/swag/stringutils`                                       | Apache-2.0   |
| `github.com/go-openapi/swag/typeutils`                                         | Apache-2.0   |
| `github.com/go-openapi/swag/yamlutils`                                         | Apache-2.0   |
| `github.com/go-openapi/swag`                                                   | Apache-2.0   |
| `github.com/go-viper/mapstructure/v2`                                          | MIT          |
| `github.com/google/btree`                                                      | Apache-2.0   |
| `github.com/google/gnostic-models`                                             | Apache-2.0   |
| `github.com/google/uuid`                                                       | BSD-3-Clause |
| `github.com/grpc-ecosystem/grpc-gateway/v2`                                    | BSD-3-Clause |
| `github.com/hashicorp/errwrap`                                                 | MPL-2.0      |
| `github.com/hashicorp/go-immutable-radix`                                      | MPL-2.0      |
| `github.com/hashicorp/go-metrics/compat`                                       | MIT          |
| `github.com/hashicorp/go-msgpack/v2/codec`                                     | MIT          |
| `github.com/hashicorp/go-multierror`                                           | MPL-2.0      |
| `github.com/hashicorp/go-sockaddr`                                             | MPL-2.0      |
| `github.com/hashicorp/golang-lru/simplelru`                                    | MPL-2.0      |
| `github.com/hashicorp/logutils`                                                | MPL-2.0      |
| `github.com/hashicorp/memberlist`                                              | MPL-2.0      |
| `github.com/jackc/pgpassfile`                                                  | MIT          |
| `github.com/jackc/pgservicefile`                                               | MIT          |
| `github.com/jackc/pgx/v5`                                                      | MIT          |
| `github.com/jackc/puddle/v2`                                                   | MIT          |
| `github.com/json-iterator/go`                                                  | MIT          |
| `github.com/klauspost/compress/internal/snapref`                               | BSD-3-Clause |
| `github.com/klauspost/compress/zstd/internal/xxhash`                           | MIT          |
| `github.com/klauspost/compress`                                                | Apache-2.0   |
| `github.com/knadh/koanf/maps`                                                  | MIT          |
| `github.com/knadh/koanf/parsers/yaml`                                          | MIT          |
| `github.com/knadh/koanf/providers/env`                                         | MIT          |
| `github.com/knadh/koanf/providers/rawbytes`                                    | MIT          |
| `github.com/knadh/koanf/providers/structs`                                     | MIT          |
| `github.com/knadh/koanf/v2`                                                    | MIT          |
| `github.com/miekg/dns`                                                         | BSD-3-Clause |
| `github.com/mitchellh/copystructure`                                           | MIT          |
| `github.com/mitchellh/reflectwalk`                                             | MIT          |
| `github.com/modern-go/concurrent`                                              | Apache-2.0   |
| `github.com/modern-go/reflect2`                                                | Apache-2.0   |
| `github.com/munnerz/goautoneg`                                                 | BSD-3-Clause |
| `github.com/oklog/ulid/v2`                                                     | Apache-2.0   |
| `github.com/pkg/errors`                                                        | BSD-2-Clause |
| `github.com/prometheus/client_golang/internal/github.com/golang/gddo/httputil` | BSD-3-Clause |
| `github.com/prometheus/client_golang/prometheus`                               | Apache-2.0   |
| `github.com/prometheus/client_model/go`                                        | Apache-2.0   |
| `github.com/prometheus/common`                                                 | Apache-2.0   |
| `github.com/prometheus/otlptranslator`                                         | Apache-2.0   |
| `github.com/redis/go-redis/v9`                                                 | BSD-2-Clause |
| `github.com/reugn/go-quartz`                                                   | MIT          |
| `github.com/sean-/seed`                                                        | MIT          |
| `github.com/spf13/cobra`                                                       | Apache-2.0   |
| `github.com/spf13/pflag`                                                       | BSD-3-Clause |
| `github.com/tidwall/btree`                                                     | MIT          |
| `github.com/tidwall/match`                                                     | MIT          |
| `github.com/tidwall/redcon`                                                    | MIT          |
| `github.com/tochemey/goakt/v4`                                                 | MIT          |
| `github.com/tochemey/olric/internal/consistent`                                | MIT          |
| `github.com/tochemey/olric`                                                    | Apache-2.0   |
| `github.com/vmihailenco/msgpack/v5`                                            | BSD-2-Clause |
| `github.com/vmihailenco/tagparser/v2`                                          | BSD-2-Clause |
| `github.com/x448/float16`                                                      | MIT          |
| `github.com/zeebo/xxh3`                                                        | BSD-2-Clause |
| `go.etcd.io/bbolt`                                                             | MIT          |
| `go.mongodb.org/mongo-driver`                                                  | Apache-2.0   |
| `go.opentelemetry.io/auto/sdk`                                                 | Apache-2.0   |
| `go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc`            | Apache-2.0   |
| `go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc`              | Apache-2.0   |
| `go.opentelemetry.io/otel/exporters/otlp/otlptrace`                            | Apache-2.0   |
| `go.opentelemetry.io/otel/exporters/prometheus`                                | Apache-2.0   |
| `go.opentelemetry.io/otel/metric`                                              | Apache-2.0   |
| `go.opentelemetry.io/otel/sdk/metric`                                          | Apache-2.0   |
| `go.opentelemetry.io/otel/sdk`                                                 | Apache-2.0   |
| `go.opentelemetry.io/otel/trace`                                               | Apache-2.0   |
| `go.opentelemetry.io/otel`                                                     | Apache-2.0   |
| `go.opentelemetry.io/proto/otlp`                                               | Apache-2.0   |
| `go.uber.org/atomic`                                                           | MIT          |
| `go.uber.org/multierr`                                                         | MIT          |
| `go.uber.org/zap`                                                              | MIT          |
| `go.yaml.in/yaml/v2`                                                           | Apache-2.0   |
| `go.yaml.in/yaml/v3`                                                           | MIT          |
| `golang.org/x/arch/x86/x86asm`                                                 | BSD-3-Clause |
| `golang.org/x/mod/semver`                                                      | BSD-3-Clause |
| `golang.org/x/net`                                                             | BSD-3-Clause |
| `golang.org/x/oauth2`                                                          | BSD-3-Clause |
| `golang.org/x/sync`                                                            | BSD-3-Clause |
| `golang.org/x/sys/unix`                                                        | BSD-3-Clause |
| `golang.org/x/term`                                                            | BSD-3-Clause |
| `golang.org/x/text`                                                            | BSD-3-Clause |
| `golang.org/x/time/rate`                                                       | BSD-3-Clause |
| `google.golang.org/genproto/googleapis/api/httpbody`                           | Apache-2.0   |
| `google.golang.org/genproto/googleapis/rpc`                                    | Apache-2.0   |
| `google.golang.org/grpc`                                                       | Apache-2.0   |
| `google.golang.org/protobuf`                                                   | BSD-3-Clause |
| `gopkg.in/evanphx/json-patch.v4`                                               | BSD-3-Clause |
| `gopkg.in/inf.v0`                                                              | BSD-3-Clause |
| `k8s.io/api`                                                                   | Apache-2.0   |
| `k8s.io/apimachinery/pkg`                                                      | Apache-2.0   |
| `k8s.io/apimachinery/third_party/forked/golang`                                | BSD-3-Clause |
| `k8s.io/client-go`                                                             | Apache-2.0   |
| `k8s.io/klog/v2`                                                               | Apache-2.0   |
| `k8s.io/kube-openapi/pkg/internal/third_party/go-json-experiment/json`         | BSD-3-Clause |
| `k8s.io/kube-openapi/pkg/validation/spec`                                      | Apache-2.0   |
| `k8s.io/kube-openapi/pkg`                                                      | Apache-2.0   |
| `k8s.io/utils/internal/third_party/forked/golang/net`                          | BSD-3-Clause |
| `k8s.io/utils`                                                                 | Apache-2.0   |
| `sigs.k8s.io/json`                                                             | Apache-2.0   |
| `sigs.k8s.io/randfill`                                                         | Apache-2.0   |
| `sigs.k8s.io/structured-merge-diff/v6`                                         | Apache-2.0   |
| `sigs.k8s.io/yaml`                                                             | Apache-2.0   |
