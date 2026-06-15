import { ConnectError } from "@connectrpc/connect";

// errorMessage normalizes any thrown value (Connect RPC errors included) into a
// human-readable string for display.
export function errorMessage(err: unknown): string {
  return ConnectError.from(err).message;
}
