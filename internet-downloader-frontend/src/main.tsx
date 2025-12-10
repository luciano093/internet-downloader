import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import { Routes } from '@generouted/react-router'

document.documentElement.classList.add("dark");

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <div className="w-full min-h-screen bg-background text-foreground">
      <Routes />
    </div>
  </StrictMode>,
)
