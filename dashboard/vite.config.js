import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// During `npm run dev`, calls to /api and /health are forwarded to FastAPI on port 8000.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api": "http://localhost:8000",
      "/health": "http://localhost:8000",
    },
  },
});
