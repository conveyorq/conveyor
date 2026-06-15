import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import "./index.css";
import { App } from "./App.tsx";
import { ApiProvider, createApi } from "./api/context.tsx";
import { createApiTransport } from "./api/transport.ts";

const api = createApi(createApiTransport());

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <ApiProvider api={api}>
      <App />
    </ApiProvider>
  </StrictMode>,
);
