# syntax=docker/dockerfile:1
#
# Optional image for running the Postmark example *inside* a cluster (e.g. the
# kind cluster from `make e2e`). It bundles both the worker and the producer;
# the Kubernetes manifests pick one with `command:`. The send is always
# simulated, so the image needs no SMTP credentials or network egress.
#
# Build from the repository root so the vendor tree is in context:
#   docker build -f examples/postmark/deploy/Dockerfile -t postmark:e2e .

FROM golang:1.26.4-alpine AS build

WORKDIR /src

# Copy the whole tree (including vendor/) so the build never reaches the
# network for modules, matching the production image's hermetic build.
COPY . .

RUN CGO_ENABLED=0 go build -mod=vendor -trimpath -ldflags="-s -w" \
        -o /out/worker ./examples/postmark/cmd/worker
RUN CGO_ENABLED=0 go build -mod=vendor -trimpath -ldflags="-s -w" \
        -o /out/producer ./examples/postmark/cmd/producer

FROM gcr.io/distroless/static:nonroot

COPY --from=build /out/worker /usr/local/bin/worker
COPY --from=build /out/producer /usr/local/bin/producer

USER nonroot:nonroot

# No default ENTRYPOINT: the manifests select /usr/local/bin/worker or
# /usr/local/bin/producer via the pod's command.
