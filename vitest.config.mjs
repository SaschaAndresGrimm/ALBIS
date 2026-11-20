import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    environment: "jsdom",
    include: ["frontend/tests/**/*.test.js"],
    globals: true,
    clearMocks: true,
    // Each file still gets a fresh module graph and a fresh document, but
    // inside a VM context in a long-lived worker rather than a new process
    // with a new jsdom: building those was 86% of the run (41 s -> 11 s).
    // `isolate: false` is faster still but lets files leak state into each
    // other, and several do. Two things behave differently in a VM context:
    // fake only the timers a test needs (`toFake`), and a test that builds
    // its own JSDOM runs in `@vitest-environment node`. See frontend/AGENTS.md.
    pool: "vmThreads",
  },
});
