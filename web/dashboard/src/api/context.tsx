import { createContext, useContext, type ReactNode } from "react";
import { createClient, type Client, type Transport } from "@connectrpc/connect";
import { AdminService, TaskService } from "../gen/conveyor/v1/service_pb.ts";

// Api bundles the typed Connect clients the dashboard calls.
export interface Api {
  admin: Client<typeof AdminService>;
  tasks: Client<typeof TaskService>;
}

// createApi builds the clients over a transport. Production passes the
// Connect-web transport; tests pass an in-memory router transport.
export function createApi(transport: Transport): Api {
  return {
    admin: createClient(AdminService, transport),
    tasks: createClient(TaskService, transport),
  };
}

const ApiContext = createContext<Api | null>(null);

// ApiProvider makes the clients available to the component tree.
export function ApiProvider({ api, children }: { api: Api; children: ReactNode }) {
  return <ApiContext.Provider value={api}>{children}</ApiContext.Provider>;
}

// useApi returns the clients from context, panicking if no provider is present.
export function useApi(): Api {
  const api = useContext(ApiContext);
  if (api === null) {
    throw new Error("useApi must be used within an ApiProvider");
  }

  return api;
}
