/**
 * Localized wording for SIMPLON connection failures.
 *
 * The backend classifies every transport/HTTP failure into a small code
 * vocabulary (`dns`, `refused`, `timeout`, `api_missing`, `http_error`,
 * `unreachable`). This module turns one of those diagnoses into a sentence that
 * names the fix, so the connection test and the live poll status report the
 * same specific reason instead of a generic "SIMPLON error".
 */

import { t } from "./i18n.js";

/**
 * @param {object|null|undefined} diagnosis Payload with `code` plus optional
 *   `port`, `http_status`, `api_version`, `url`, `timeout_s`.
 * @returns {string} Localized, actionable failure text.
 */
export function describeSimplonFailure(diagnosis) {
  const code = String(diagnosis?.code || "");
  const port = diagnosis?.port;
  const httpStatus = diagnosis?.http_status;
  const version = String(diagnosis?.api_version || "");
  const url = String(diagnosis?.url || "");
  const seconds = Number(diagnosis?.timeout_s);

  switch (code) {
    case "dns":
      return t("simplon.failure.dns");
    case "refused":
      return port
        ? t("simplon.failure.refused_port", { port })
        : t("simplon.failure.refused");
    case "timeout":
      return Number.isFinite(seconds) && seconds > 0
        ? t("simplon.failure.timeout_seconds", { seconds })
        : t("simplon.failure.timeout");
    case "api_missing":
      return version
        ? t("simplon.failure.api_missing_version", { version })
        : t("simplon.failure.api_missing");
    case "http_error":
      return httpStatus
        ? t("simplon.failure.http_status", { status: httpStatus })
        : t("simplon.failure.http");
    case "unreachable":
      return url
        ? t("simplon.failure.unreachable_url", { url })
        : t("simplon.failure.unreachable");
    default:
      return t("simplon.failure.unknown");
  }
}

/**
 * Pull the diagnosis out of a failed ALBIS API response body.
 *
 * FastAPI wraps our classified payload in `detail`; a plain-string detail (or a
 * non-JSON body from a proxy) yields null so callers fall back to generic text.
 *
 * @param {Response} response Failed `fetch` response.
 * @returns {Promise<object|null>} Diagnosis payload, or null when absent.
 */
export async function readSimplonFailure(response) {
  try {
    const payload = await response.json();
    const detail = payload?.detail;
    if (detail && typeof detail === "object" && detail.code) {
      return detail;
    }
  } catch {
    // Non-JSON or empty body — fall back to generic wording.
  }
  return null;
}
