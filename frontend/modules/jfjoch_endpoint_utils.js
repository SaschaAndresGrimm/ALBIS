/**
 * Normalization for the JUNGFRAUJOCH preview endpoint field.
 *
 * Same class of problem as the SIMPLON address — operators type `host:port` and
 * the connection fails deep inside ZeroMQ — but the rules differ: the transport
 * is `tcp://` rather than `http://`, and there is no default port to fall back
 * on, so a missing port is an error rather than an assumption.
 */

// Transports a ZeroMQ subscriber can use. Kept in step with
// backend/services/jungfraujoch_preview.py.
const ZMQ_SCHEMES = ["tcp", "ipc", "inproc", "pgm", "epgm"];
const SCHEME_PATTERN = /^([a-z][a-z0-9+.-]*):\/\//i;
const MALFORMED_SCHEME_PATTERN = new RegExp(`^(${ZMQ_SCHEMES.join("|")})(?::\\/*|\\/+)`, "i");

/**
 * Fold user input into a ZeroMQ-usable endpoint.
 *
 * Returns "" for empty input. A non-ZeroMQ scheme is passed through unchanged so
 * validation can reject it by name rather than silently rewriting it.
 *
 * @param {string} input Raw field value.
 * @returns {string} Canonical `tcp://host:port` (or another ZeroMQ transport).
 */
export function normalizeJfjochEndpoint(input) {
  let text = String(input ?? "").replace(/\s+/g, "");
  if (!text) return "";

  const scheme = text.match(SCHEME_PATTERN);
  if (scheme) {
    if (!ZMQ_SCHEMES.includes(scheme[1].toLowerCase())) return text;
    text = `${scheme[1].toLowerCase()}://${text.slice(scheme[0].length)}`;
  } else {
    const malformed = text.match(MALFORMED_SCHEME_PATTERN);
    text = malformed
      ? `${malformed[1].toLowerCase()}://${text.slice(malformed[0].length)}`
      : `tcp://${text}`;
  }

  const separator = text.indexOf("://") + 3;
  const transport = text.slice(0, separator - 3);
  const rest = text.slice(separator);
  if (transport !== "tcp") {
    // ipc/inproc addresses are paths: `ipc:///tmp/x` means /tmp/x, so the
    // slashes are content and must survive.
    return rest ? `${transport}://${rest}` : "";
  }
  const host = rest.replace(/^\/+/, "").replace(/\/+$/, "");
  return host ? `tcp://${host}` : "";
}

/**
 * Read an endpoint input, normalize it, and write the canonical form back so
 * the user sees exactly what will be connected.
 *
 * @param {HTMLInputElement|null|undefined} element Field holding the endpoint.
 * @returns {string} Canonical endpoint ("" when empty/missing).
 */
export function normalizeJfjochEndpointInput(element) {
  if (!element) return "";
  const normalized = normalizeJfjochEndpoint(element.value);
  if (element.value !== normalized) {
    element.value = normalized;
  }
  return normalized;
}

/**
 * Why an endpoint cannot be used, or "" when it looks usable.
 *
 * Mirrors the backend's rejections so the UI can flag them before a request.
 *
 * @param {string} endpoint Canonical endpoint from `normalizeJfjochEndpoint`.
 * @returns {""|"empty"|"transport"|"host"|"port"} Problem code.
 */
export function describeJfjochEndpointProblem(endpoint) {
  const value = String(endpoint ?? "").trim();
  if (!value) return "empty";
  const scheme = value.match(SCHEME_PATTERN);
  if (!scheme || !ZMQ_SCHEMES.includes(scheme[1].toLowerCase())) return "transport";
  if (scheme[1].toLowerCase() !== "tcp") return "";
  const rest = value.slice(scheme[0].length);
  const separator = rest.lastIndexOf(":");
  if (separator < 0) return "port";
  // Last colon wins, so `[::1]:31003` keeps its bracketed host intact.
  if (!rest.slice(0, separator)) return "host";
  const port = rest.slice(separator + 1);
  return /^\d+$/.test(port) && Number(port) > 0 ? "" : "port";
}
