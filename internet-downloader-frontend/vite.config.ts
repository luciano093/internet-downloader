import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react-swc'
import routes from '@generouted/react-router/plugin'
import tailwindcss from '@tailwindcss/vite'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react(), routes(), tailwindcss()],
})
