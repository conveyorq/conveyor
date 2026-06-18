// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

import type { Interceptor, Transport } from "@connectrpc/connect";
import { createGrpcTransport } from "@connectrpc/connect-node";

/**
 * createTransport builds the gRPC-over-HTTP/2 transport every service shares.
 * A plaintext `http://` base URL uses HTTP/2 cleartext (h2c); an `https://`
 * URL negotiates HTTP/2 via ALPN. gRPC is used because the worker session is a
 * bidirectional stream, which needs full-duplex HTTP/2.
 *
 * @internal
 */
export function createTransport(baseUrl: string, token: string | undefined): Transport {
  const interceptors: Interceptor[] = [];

  if (token) {
    interceptors.push((next) => (request) => {
      request.header.set("Authorization", `Bearer ${token}`);

      return next(request);
    });
  }

  return createGrpcTransport({ baseUrl, interceptors });
}
