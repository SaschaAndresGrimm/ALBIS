/**
 * Long paths and names in the export dialogs: shown by their end, which is
 * where they differ, with the whole text on hover when it does not fit.
 */

import { longestFitting, middleEllipsis } from "./text_fit_utils.js";

const overflows = (element) => element.scrollWidth > element.clientWidth + 1;

// A text field scrolled to the end of its value, the whole value in its
// tooltip while it does not fit. Unlaid out (hidden), it is left alone.
export function showEnd(input) {
  if (!input || !input.clientWidth) return;
  input.scrollLeft = input.scrollWidth;
  const value = String(input.value || "");
  if (overflows(input)) input.title = value;
  else input.removeAttribute("title");
}

// `text` in `element` on one line, cut in its middle to fit; the whole text in
// its tooltip when it was cut.
export function fitMiddle(element, text) {
  if (!element) return;
  const value = String(text ?? "");
  element.textContent = value;
  element.removeAttribute("title");
  if (!element.clientWidth || !overflows(element)) return;
  const keep = longestFitting(value.length - 1, (count) => {
    element.textContent = middleEllipsis(value, count);
    return !overflows(element);
  });
  element.textContent = middleEllipsis(value, keep);
  element.title = value;
}
