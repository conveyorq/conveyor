# Email-delivery worker image for the Conveyor TypeScript example.
#
# Build from the REPO ROOT so the SDK (a local file: dependency) is in context:
#
#   docker build -f examples/typescript/Dockerfile -t conveyor-email-worker .
#
# docker-compose.yml builds it for you.
FROM node:22-alpine

# Enable pnpm via Corepack (bundled with Node 22).
RUN corepack enable

WORKDIR /app

# Build the SDK first; the example depends on it via file:../../sdks/typescript.
COPY sdks/typescript/package.json sdks/typescript/pnpm-lock.yaml* ./sdks/typescript/
COPY sdks/typescript/ ./sdks/typescript/
RUN cd sdks/typescript && pnpm install && pnpm run build

# Install and typecheck the example (tsx runs the TypeScript directly).
COPY examples/typescript/package.json ./examples/typescript/
RUN cd examples/typescript && pnpm install
COPY examples/typescript/ ./examples/typescript/
RUN cd examples/typescript && pnpm run typecheck

WORKDIR /app/examples/typescript

# The worker drains in-flight tasks on SIGTERM, which `docker stop` sends.
CMD ["pnpm", "run", "worker"]
