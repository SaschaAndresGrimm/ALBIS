/**
 * Global client-side logging hooks.
 */

export function bindClientLogging({
  formatClientArg,
  logClient,
}) {
  const originalConsoleError = console.error;
  const originalConsoleWarn = console.warn;

  console.error = (...args) => {
    originalConsoleError(...args);
    logClient("error", args.map(formatClientArg).join(" "));
  };

  console.warn = (...args) => {
    originalConsoleWarn(...args);
    logClient("warning", args.map(formatClientArg).join(" "));
  };

  window.addEventListener("error", (event) => {
    logClient("error", event.message || "Unhandled error", {
      source: event.filename,
      line: event.lineno,
      column: event.colno,
      stack: event.error?.stack,
    });
  });

  window.addEventListener("unhandledrejection", (event) => {
    logClient("error", "Unhandled promise rejection", {
      reason: formatClientArg(event.reason),
    });
  });
}
