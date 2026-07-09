# Email-delivery worker image for the Conveyor TypeScript example.
#
# Build from the REPO ROOT so the SDK (a local file: dependency) is in context:
#
#   docker build -f examples/typescript/Dockerfile -t conveyor-email-worker .
#
# docker-compose.yml builds it for you.
FROM node:22-alpine

# Enable pnpm via Corepack (bundled with Node 22). The npm bundled with the
# base image is unused here; drop it so its vulnerable transitive dependencies
# (e.g. undici, CVE-2026-12151) leave the image.
RUN corepack enable \
 && rm -rf /usr/local/lib/node_modules/npm /usr/local/bin/npm /usr/local/bin/npx

WORKDIR /app

# Build the SDK first; the example depends on it via file:../../sdks/typescript.
COPY sdks/typescript/package.json sdks/typescript/pnpm-lock.yaml* ./sdks/typescript/
COPY sdks/typescript/ ./sdks/typescript/
RUN cd sdks/typescript && pnpm install && pnpm run build

# Install and typecheck the example (tsx runs the TypeScript directly).
# pnpm-workspace.yaml carries the build-script approvals (allowBuilds) and
# must be present before pnpm install.
COPY examples/typescript/package.json examples/typescript/pnpm-lock.yaml examples/typescript/pnpm-workspace.yaml ./examples/typescript/
RUN cd examples/typescript && pnpm install
COPY examples/typescript/ ./examples/typescript/
RUN cd examples/typescript && pnpm run typecheck

WORKDIR /app/examples/typescript

# The worker drains in-flight tasks on SIGTERM, which `docker stop` sends.
CMD ["pnpm", "run", "worker"]
