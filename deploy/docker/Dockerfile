# syntax=docker/dockerfile:1
#
# Production image for the Conveyor server (conveyord) and CLI (conveyor).
# Two stages: a static build on golang-alpine, then a distroless nonroot
# runtime that ships only the binaries and CA roots.
#
# Build from the repository root so the vendor tree is in context:
#   docker build -f deploy/docker/Dockerfile -t ghcr.io/conveyorq/conveyor:dev .

# The dashboard's built bundle is not committed; build it here so conveyord can
# embed it. Runs on the native build platform (static assets are arch-neutral).
FROM --platform=$BUILDPLATFORM node:22-alpine AS web

WORKDIR /web

# Install deps against the committed lockfile, then build the SPA into dist/.
COPY web/dashboard/package.json web/dashboard/package-lock.json ./
RUN npm ci
COPY web/dashboard/ ./
RUN npm run build

# The build stage runs on the native build platform and cross-compiles to the
# target, so multi-arch images build without emulating the Go toolchain.
FROM --platform=$BUILDPLATFORM golang:1.26.4-alpine AS build

WORKDIR /src

# Copy the whole tree (including vendor/) so the build is hermetic and
# never reaches the network for modules.
COPY . .

# Overlay the freshly built dashboard bundle (dist/ is git-ignored, so the
# context carries only a .gitkeep) before the Go build embeds it.
COPY --from=web /web/dist ./web/dashboard/dist

# Static, stripped binaries: CGO off so the result runs on distroless static,
# -mod=vendor so dependencies come from the vendored tree, GOOS/GOARCH from the
# target platform so one builder produces every arch.
ARG VERSION=dev
ARG TARGETOS
ARG TARGETARCH
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -mod=vendor -trimpath \
        -ldflags="-s -w" \
        -o /out/conveyord ./cmd/conveyord \
 && CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -mod=vendor -trimpath \
        -ldflags="-s -w" \
        -o /out/conveyor ./cmd/conveyor

FROM gcr.io/distroless/static:nonroot

# OCI metadata; VERSION is threaded through from the build stage's ARG.
ARG VERSION=dev
LABEL org.opencontainers.image.title="conveyor" \
      org.opencontainers.image.description="Conveyor distributed task queue server" \
      org.opencontainers.image.source="https://github.com/conveyorq/conveyor" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.version="${VERSION}"

COPY --from=build /out/conveyord /usr/local/bin/conveyord
COPY --from=build /out/conveyor /usr/local/bin/conveyor

# api (health + ConnectRPC), remoting, gossip bootstrap, peers protocol.
EXPOSE 8080 9000 9001 9002

# distroless nonroot runs as uid 65532; never run as root.
USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/conveyord"]
