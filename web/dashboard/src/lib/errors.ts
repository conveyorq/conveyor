import { ConnectError } from "@connectrpc/connect";

// errorMessage normalizes any thrown value into a human-readable string for
// display. A Connect RPC error keeps its code prefix (e.g. "[not_found] ...");
// a plain Error, such as a client-side validation failure, shows its message
// as written rather than being labeled with Connect's "unknown" code.
export function errorMessage(err: unknown): string {
  if (err instanceof Error && !(err instanceof ConnectError)) {
    return err.message;
  }

  return ConnectError.from(err).message;
}
