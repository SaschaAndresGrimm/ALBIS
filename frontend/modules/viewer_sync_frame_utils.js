/**
 * Frame linking between windows, as arithmetic: what a message carries and
 * which frame a receiving window goes to.
 *
 * A window sends its absolute frame, never a step. The link drops messages on
 * purpose (it is throttled, and a window still loading keeps only the newest
 * request), so a lost "+1" would leave the windows a frame apart for good. The
 * receiver instead learns its offset to each sender once, from the first
 * message after linking, and from then on goes to the sender's frame plus that
 * offset. Two windows linked at frames 10 and 12 stay two frames apart however
 * they are stepped or played, and a dropped message costs nothing.
 */

/** The frame message as it travels, or null when it is not one. */
export function normalizeFramePayload(payload) {
  if (!payload || typeof payload !== "object") return null;
  const index = Number(payload.index);
  const previousIndex = Number(payload.previousIndex);
  if (!Number.isInteger(index) || index < 0) return null;
  const playing = Boolean(payload.playing);
  return {
    index,
    previousIndex: Number.isInteger(previousIndex) && previousIndex >= 0 ? previousIndex : index,
    playing,
    wrap: playing || Boolean(payload.wrap),
  };
}

/**
 * This window's offset to a sender, learned from the sender's first message:
 * where this window is, minus where the sender was before it moved.
 */
export function frameOffsetFor(ownIndex, payload) {
  const own = Number(ownIndex);
  return (Number.isInteger(own) ? own : 0) - payload.previousIndex;
}

/**
 * The frame to go to: the sender's frame plus the offset. During playback it
 * wraps round like playback itself does; when stepping it stops at the ends,
 * so a window with fewer frames waits at its last one. The message that stops
 * playback wraps as well: the frames it carries come from a playback that
 * wrapped, and clamping them put a window on frame 1 where it belonged on 10.
 */
export function followFrameTarget({ index, offset, frameCount, wrap }) {
  const count = Math.max(1, Math.floor(Number(frameCount) || 1));
  const target = Number(index) + (Number(offset) || 0);
  if (wrap) {
    return ((target % count) + count) % count;
  }
  return Math.max(0, Math.min(count - 1, target));
}
