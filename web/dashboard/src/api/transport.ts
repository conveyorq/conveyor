import { createConnectTransport } from "@connectrpc/connect-web";
import type { Interceptor, Transport } from "@connectrpc/connect";
import { getToken } from "./token.ts";

// apiBaseUrl resolves the conveyord API base URL. It defaults to the page's
// own origin (the embedded case), and can be overridden when the dashboard is
// hosted on a different origin: a global window.CONVEYOR_API_BASE (set by a
// config.js next to index.html) or an ?api=<url> query parameter.
export function apiBaseUrl(): string {
  const globalOverride = (window as { CONVEYOR_API_BASE?: string }).CONVEYOR_API_BASE;
  if (globalOverride) {
    return globalOverride;
  }

  const param = new URLSearchParams(window.location.search).get("api");
  if (param) {
    return param;
  }

  return window.location.origin;
}

// authInterceptor attaches the stored bearer token to every request, so the
// same transport works against an authenticated server and a --dev one.
const authInterceptor: Interceptor = (next) => (req) => {
  const token = getToken();
  if (token !== "") {
    req.header.set("Authorization", `Bearer ${token}`);
  }

  return next(req);
};

// createApiTransport builds the Connect transport the clients use.
export function createApiTransport(baseUrl: string = apiBaseUrl()): Transport {
  return createConnectTransport({ baseUrl, interceptors: [authInterceptor] });
}
