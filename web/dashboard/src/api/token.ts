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
