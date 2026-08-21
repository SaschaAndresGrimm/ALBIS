/**
 * Follow a series that is still being written.
 *
 * A detector filewriter holds its output open for the length of a run and
 * appends to it. ALBIS can read such a file -- that is what the SWMR retry in
 * `open_hdf5_read_only` is for -- but reading it once is not enough: the frame
 * count it had at the moment it was opened is not the frame count it will have
 * a second later, so the slider stops at wherever the acquisition happened to
 * be and the frames written since are unreachable without reopening the file.
 *
 * So while `/api/metadata` reports that a writer still holds the file, the count
 * is re-read on a timer. Only the count: the frame on screen, the playback
 * position and the mask are left exactly as they are, because someone watching
 * an acquisition is usually looking at something specific.
 *
 * The watch stops by itself when the writer lets go. That is the normal end of
 * a run, and it means a finished file costs nothing.
 */

const DEFAULT_INTERVAL_MS = 1000;

export function createLiveSeriesWatch({
  refreshFrameCount,
  callbacks = {},
  intervalMs = DEFAULT_INTERVAL_MS,
}) {
  const { onGrew, onFinished } = callbacks;
  let timer = null;
  let inFlight = false;

  function isRunning() {
    return timer !== null;
  }

  async function tick() {
    // One request at a time. A slow filesystem is the case this exists for, so
    // ticks that outpace the answer must not stack up.
    if (inFlight) return;
    inFlight = true;
    try {
      const result = await refreshFrameCount();
      if (!isRunning()) return;
      if (result?.changed) {
        onGrew?.(result.frameCount);
      }
      if (!result?.writerPresent) {
        stop();
        onFinished?.(result?.frameCount);
      }
    } catch (err) {
      console.error(err);
    } finally {
      inFlight = false;
    }
  }

  function start() {
    if (isRunning()) return;
    timer = window.setInterval(tick, intervalMs);
  }

  function stop() {
    if (!isRunning()) return;
    window.clearInterval(timer);
    timer = null;
  }

  /** Start or stop from what the last metadata read said about the writer. */
  function setWriterPresent(present) {
    if (present) {
      start();
    } else {
      stop();
    }
  }

  return { start, stop, setWriterPresent, isRunning, tick };
}
