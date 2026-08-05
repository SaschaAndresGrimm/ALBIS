/**
 * Normalization for the SIMPLON monitor base URL field.
 *
 * Operators rarely type a well-formed URL: they enter a bare detector IP or
 * hostname, paste a URL copied out of the SIMPLON docs (which carries the
 * `/monitor/api/<version>` path), or mistype the scheme separator. All of
 * those are folded into the canonical `http://host[:port]` form the backend
 * expects, so the field accepts what users actually type instead of failing
 * the poll with a generic error.
 *
 * The port is never rewritten: omitting it means the detector's default
 * (port 80), and an explicit port is always kept as entered.
 */

const SCHEME_PATTERN = /^[a-z][a-z0-9+.-]*:\/\//i;
// `http//host`, `http:/host`, `http:host`, `https:///host` — a scheme keyword
// followed by any mangled separator. Requires a `:` or `/` after the keyword so
// hostnames that merely start with "http" (e.g. `http-gw.local`) are untouched.
const MALFORMED_SCHEME_PATTERN = /^(https?)(?::\/*|\/+)/i;
// SIMPLON sub-API roots, e.g. `/monitor/api/1.8.0/images/monitor`.
const API_PATH_PATTERN = /\/(monitor|detector|stream|filewriter|system)\/api(\/|$)/i;
// A dangling sub-API segment without the `/api` part, e.g. `http://host/monitor`.
const TRAILING_API_ROOT_PATTERN = /\/(monitor|detector|stream|filewriter|system)\/*$/i;

/**
 * Fold user input into a canonical SIMPLON base URL.
 *
 * Returns "" for empty input. Input carrying a non-HTTP scheme is left as
 * entered so validation can reject it with a meaningful message rather than
 * silently rewriting what the user asked for.
 *
 * @param {string} input Raw field value.
 * @returns {string} Canonical `http(s)://host[:port][/prefix]` form.
 */
export function normalizeSimplonBaseUrl(input) {
  let text = String(input ?? "").replace(/\s+/g, "");
  if (!text) return "";

  if (SCHEME_PATTERN.test(text)) {
    const scheme = text.slice(0, text.indexOf("://"));
    if (!/^https?$/i.test(scheme)) {
      return text;
    }
    text = `${scheme.toLowerCase()}://${text.slice(scheme.length + 3)}`;
  } else {
    const malformed = text.match(MALFORMED_SCHEME_PATTERN);
    if (malformed) {
      text = `${malformed[1].toLowerCase()}://${text.slice(malformed[0].length)}`;
    } else {
      text = `http://${text}`;
    }
  }

  const separator = text.indexOf("://") + 3;
  const scheme = text.slice(0, separator);
  let rest = text.slice(separator).replace(/^\/+/, "");
  if (!rest) return "";

  const apiPath = rest.match(API_PATH_PATTERN);
  if (apiPath) {
    rest = rest.slice(0, apiPath.index);
  } else {
    rest = rest.replace(TRAILING_API_ROOT_PATTERN, "");
  }

  rest = rest.replace(/\/+$/, "").replace(/:$/, "");
  if (!rest) return "";
  return `${scheme}${rest}`;
}

/**
 * Read a URL input, normalize it, and write the canonical form back into the
 * field so users see exactly what will be requested.
 *
 * @param {HTMLInputElement|null|undefined} element Field holding the base URL.
 * @returns {string} Canonical base URL ("" when the field is empty/missing).
 */
export function normalizeSimplonUrlInput(element) {
  if (!element) return "";
  const normalized = normalizeSimplonBaseUrl(element.value);
  if (element.value !== normalized) {
    element.value = normalized;
  }
  return normalized;
}
