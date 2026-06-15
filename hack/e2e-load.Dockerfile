# syntax=docker/dockerfile:1
#
# Test-only image for the kind rolling-restart e2e: the e2e-load workload
# driver (producer + worker) that runs in-cluster and asserts zero task loss
# across a server StatefulSet rollout. Not a shipped artifact.
#
# Build from the repository root so the vendor tree is in context:
#   docker build -f hack/e2e-load.Dockerfile -t conveyor-e2e-load:e2e .

FROM golang:1.26.4-alpine AS build

WORKDIR /src

# Copy the whole tree (including vendor/) so the build never reaches the
# network for modules, matching the production image's hermetic build.
COPY . .

RUN CGO_ENABLED=0 go build -mod=vendor -trimpath -ldflags="-s -w" \
        -o /out/e2e-load ./hack/e2e-load

FROM gcr.io/distroless/static:nonroot

COPY --from=build /out/e2e-load /usr/local/bin/e2e-load

USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/e2e-load"]
