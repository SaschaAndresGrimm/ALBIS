/**
 * Marking a control unavailable so that the reason stays reachable.
 *
 * A natively `disabled` control fires neither pointer events nor clicks, so
 * both the tooltip explaining why it cannot be used and the guard that would
 * turn a click down out loud are unreachable: it sits there dead, which is the
 * dead end the greying was meant to replace. Controls whose unavailability
 * carries an explanation are therefore marked the way the File menu already
 * marks its entries -- `is-disabled` plus `aria-disabled` -- so they stay
 * hoverable and clickable, the reason shows on hover, and a click reaches the
 * command's own guard.
 *
 * The reason goes in `data-help-reason` rather than `title` because the help
 * tooltip system reads `data-help` ahead of `title` and strips `title` from
 * everything it maps: a reason in `title` would be outranked on the controls
 * that carry a hint, and then deleted on the next refresh.
 *
 * Use the native `disabled` attribute for controls that are simply inert with
 * nothing to say, and for checkboxes and radios, where a control that looks
 * disabled but still toggles is worse than one that cannot explain itself --
 * put their reason on the surrounding label instead.
 */
export function setControlAvailability(element, reason) {
  if (!element) return;
  element.classList.toggle("is-disabled", Boolean(reason));
  if (reason) {
    element.setAttribute("aria-disabled", "true");
    element.dataset.helpReason = reason;
  } else {
    element.removeAttribute("aria-disabled");
    delete element.dataset.helpReason;
  }
}
