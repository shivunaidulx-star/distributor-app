import { defineConfig } from '@playwright/test';

const baseURL = process.env.E2E_BASE_URL || 'http://localhost:8081';
const channel = process.env.PLAYWRIGHT_CHANNEL || undefined;
const executablePath = process.env.PLAYWRIGHT_EXECUTABLE_PATH || undefined;

export default defineConfig({
  testDir: './tests',
  timeout: 120000,
  expect: {
    timeout: 15000
  },
  use: {
    baseURL,
    headless: true,
    viewport: { width: 1280, height: 720 },
    channel,
    launchOptions: executablePath ? { executablePath } : undefined,
    trace: 'on-first-retry'
  },
  retries: process.env.CI ? 1 : 0
});
