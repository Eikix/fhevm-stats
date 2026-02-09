import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import { defineConfig } from "vite";

const defaultAllowedHosts = ["localhost", "127.0.0.1", "::1"];
const extraAllowedHosts = (process.env.VITE_ALLOWED_HOSTS ?? "")
  .split(",")
  .map((value) => value.trim())
  .filter((value) => value.length > 0);
const allowedHosts = [
  ...new Set([...defaultAllowedHosts, ...extraAllowedHosts]),
];

// https://vite.dev/config/
export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    host: process.env.VITE_DEV_HOST ?? "127.0.0.1",
    allowedHosts,
  },
});
