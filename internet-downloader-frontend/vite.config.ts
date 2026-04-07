import { defineConfig, loadEnv } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'
import { tanstackRouter } from '@tanstack/router-plugin/vite'
import path from "path"
import { exit } from 'process'

const REQUIRED_ENVS = [
  'FRONTEND_URL',
  'BACKEND_URL'
];

// https://vite.dev/config/
export default defineConfig(({ mode }) => {
  const rootEnv = loadEnv(mode, path.resolve(process.cwd(), '..'), '');
  const frontendEnv = loadEnv(mode, process.cwd(), '');
  const env = { ...rootEnv, ...frontendEnv };

  const definedEnvVariables: Record<string, string> = {};
  const missingEnvVariables: string[] = [];

  for (const key of REQUIRED_ENVS) {
    if (!env[key]) {
      missingEnvVariables.push(key);
      continue;
    }
    
    // Add it to the define object dynamically
    definedEnvVariables[`import.meta.env.${key}`] = JSON.stringify(env[key]);
  }

  if (missingEnvVariables.length > 0) {
    console.error(`\x1b[31mThe following environmental variables are required but were not found. Please set them in the .env file: ${missingEnvVariables.join(', ')}\x1b[0m`);
    exit(1);
  }

  return {
    plugins: [
      tanstackRouter({
        target: 'react',
        autoCodeSplitting: true,
        routeFileIgnorePattern: 'components', 
      }),
      react(),
      tailwindcss(),
    ],
    resolve: {
      tsconfigPaths: true, 
      alias: {
        "@": path.resolve(__dirname, "./src"),
      },
    },
    define: definedEnvVariables,
  };
})
