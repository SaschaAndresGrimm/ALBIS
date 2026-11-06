/**
 * What the footer status pill shows for a status message.
 *
 * The pill and the toasts split the work: the pill says what ALBIS is doing
 * now (loading, exporting, ready), a toast says that something happened (a
 * failure, a warning, a confirmation). A toned message is an event, so it goes
 * to a toast only, and the pill falls back to Ready or Idle. Repeating it in
 * the pill used to show the same failure twice, and the pill kept saying it
 * long after the toast had gone, until some unrelated message replaced it.
 *
 * Returns `{ key }` for one of the ambient labels, or `{ text }` to show the
 * message itself.
 */
export function statusPillContent(text, { tone = null, frameStatus = false, hasFrame = false } = {}) {
  const normalized = String(text || "").trim();
  if (frameStatus) return { key: "common.ready" };
  if (tone) return { key: hasFrame ? "common.ready" : "common.idle" };
  return normalized ? { text: normalized } : { key: "common.idle" };
}
