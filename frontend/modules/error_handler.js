/**
 * Global error handling and logging for ALBIS frontend.
 * 
 * This module captures:
 * - Uncaught JavaScript errors
 * - Unhandled promise rejections
 * - Manual error reports
 * 
 * All errors are logged to console and sent to the backend for centralized logging.
 */

import { API } from "./http.js";

let errorCount = 0;
const MAX_ERRORS_PER_SESSION = 50; // Prevent spam if something goes really wrong

/**
 * Send error to backend for logging
 */
async function logErrorToBackend(level, message, details = {}) {
  if (errorCount >= MAX_ERRORS_PER_SESSION) {
    return; // Stop sending if we've hit the limit
  }
  
  errorCount++;
  
  try {
    await fetch(`${API}/client-log`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        level: level,
        message: message,
        ...details,
        url: window.location.href,
        userAgent: navigator.userAgent,
        timestamp: new Date().toISOString(),
      }),
    });
  } catch (err) {
    // Silent fail if backend is unreachable
    console.warn("Failed to send error to backend:", err);
  }
}

/**
 * Handle global errors
 */
function handleGlobalError(event) {
  const error = event.error;
  const message = error?.message || event.message || "Unknown error";
  const stack = error?.stack || "";
  
  console.error("Global error caught:", error);
  
  logErrorToBackend("error", message, {
    stack: stack,
    filename: event.filename,
    lineno: event.lineno,
    colno: event.colno,
  });
  
  // Don't prevent default error handling
  return false;
}

/**
 * Handle unhandled promise rejections
 */
function handleUnhandledRejection(event) {
  const reason = event.reason;
  const message = reason?.message || String(reason) || "Unhandled promise rejection";
  const stack = reason?.stack || "";
  
  console.error("Unhandled promise rejection:", reason);
  
  logErrorToBackend("error", message, {
    stack: stack,
    type: "unhandledRejection",
  });
  
  // Prevent default (which would log to console again)
  event.preventDefault();
}

/**
 * Manually report an error
 */
export function reportError(message, level = "error", details = {}) {
  console[level](message, details);
  logErrorToBackend(level, message, details);
}

/**
 * Initialize error handling
 */
export function initErrorHandler() {
  window.addEventListener("error", handleGlobalError);
  window.addEventListener("unhandledrejection", handleUnhandledRejection);
  
  console.info("Error handler initialized");
}

/**
 * Get current error count (useful for debugging/testing)
 */
export function getErrorCount() {
  return errorCount;
}
