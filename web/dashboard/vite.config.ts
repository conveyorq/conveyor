/// <reference types="vitest/config" />
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

// The dashboard is embedded into conveyord via go:embed of dist/, and is also
// publishable as a standalone static bundle. Assets use relative paths so the
// same build works whether served from the API root or a sub-path on a CDN.
export default defineConfig({
  base: "./",
  plugins: [react(), tailwindcss()],
  build: {
    outDir: "dist",
    emptyOutDir: true,
    rollupOptions: {
      output: {
        // Split the rarely-changing vendor libraries into their own chunks so a
        // UI change does not bust the cache of React or the Connect runtime,
        // which (with the immutable asset caching the server sets) the browser
        // then keeps across deploys. Vite 8 (Rolldown) wants the function form.
        manualChunks(id: string) {
          if (!id.includes("node_modules")) {
            return undefined;
          }

          if (/[\\/]node_modules[\\/](react|react-dom|scheduler)[\\/]/.test(id)) {
            return "react";
          }

          if (/[\\/]node_modules[\\/](@connectrpc|@bufbuild)[\\/]/.test(id)) {
            return "connect";
          }

          return "vendor";
        },
      },
    },
  },
  test: {
    environment: "jsdom",
    globals: true,
    setupFiles: "./src/test-setup.ts",
    css: true,
  },
});
