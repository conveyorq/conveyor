// tokenKey is the localStorage key holding the API bearer token. The token is
// kept client-side only and attached to API calls by the transport
// interceptor; it is never baked into the served assets.
const tokenKey = "conveyor.token";

// getToken returns the stored bearer token, or an empty string when unset or
// when storage is unavailable.
export function getToken(): string {
  try {
    return localStorage.getItem(tokenKey) ?? "";
  } catch {
    return "";
  }
}

// setToken stores the bearer token, or clears it when empty.
export function setToken(token: string): void {
  try {
    if (token === "") {
      localStorage.removeItem(tokenKey);
    } else {
      localStorage.setItem(tokenKey, token);
    }
  } catch {
    // Storage unavailable (private mode, disabled): the token simply does not
    // persist; calls fall back to unauthenticated.
  }
}

// tokenParam is the query-string parameter a deployment may use to hand the
// dashboard its bearer token on first load, so a link can open the UI already
// authenticated (the Postmark demo opens such a link). It mirrors the existing
// "?api=" override.
const tokenParam = "token";

// ingestTokenFromURL consumes a "?token=..." query parameter when present: it
// moves the token into client-side storage and strips the parameter from the
// URL, so the token is not left in the address bar, history, or a bookmark, and
// is still never baked into the served assets. A blank value clears the stored
// token. Call it once at startup, before the first API call. Other query
// parameters (for example "?api=") are preserved.
export function ingestTokenFromURL(): void {
  try {
    const url = new URL(window.location.href);

    const fromURL = url.searchParams.get(tokenParam);
    if (fromURL === null) {
      return;
    }

    setToken(fromURL);

    url.searchParams.delete(tokenParam);
    window.history.replaceState(null, "", url.pathname + url.search + url.hash);
  } catch {
    // A malformed URL or unavailable history API: fall back to a typed token.
  }
}
