import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import "./index.css";
import { App } from "./App.tsx";
import { ApiProvider, createApi } from "./api/context.tsx";
import { createApiTransport } from "./api/transport.ts";
import { ingestTokenFromURL } from "./api/token.ts";

// Adopt a token passed in the URL (e.g. an "open pre-authenticated" demo link)
// before anything reads it, then build the clients.
ingestTokenFromURL();

const api = createApi(createApiTransport());

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <ApiProvider api={api}>
      <App />
    </ApiProvider>
  </StrictMode>,
);
