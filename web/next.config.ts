import type { NextConfig } from "next";

const api = process.env.API_URL ?? "http://localhost:3001";

const config: NextConfig = {
  outputFileTracingRoot: import.meta.dirname,
  async rewrites() {
    return [{ source: "/api/:path*", destination: `${api}/:path*` }];
  },
};

export default config;
