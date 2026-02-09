import path from "path";

const nextConfig = {
  /* config options here */
  reactStrictMode: true,
  webpack: (config) => {
    config.resolve.alias = {
      ...config.resolve.alias,
      "@app": path.resolve(process.cwd(), "src"),
    };
    return config;
  },
};

export default nextConfig;
