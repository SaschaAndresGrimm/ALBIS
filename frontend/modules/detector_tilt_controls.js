/**
 * The Detector 2θ row of Data → Detector Geometry: the arm's angle, and the
 * direction it swings on the image.
 *
 * Built here, after the beam-centre field, rather than written into
 * index.html, which is past its line budget. The geometry params controller
 * owns the row once it exists, exactly as it owns the fields beside it.
 */

import { applyI18nToDom } from "./i18n.js";
import { attachNumberStepper } from "./number_stepper.js";

const MARKUP = `
  <span data-i18n="geometry.two_theta.label">Detector 2θ (°)</span>
  <div class="ring-center">
    <input id="geometry-two-theta" type="number" min="0" max="89.9" step="0.1" placeholder="—" />
    <select
      id="geometry-two-theta-direction"
      aria-label="Arm swings toward"
      data-i18n-aria-label="geometry.two_theta.direction"
      data-i18n-title="geometry.two_theta.direction"
    >
      <option value="" data-i18n="geometry.two_theta.direction.none">Direction</option>
      <option value="+x" data-i18n="geometry.two_theta.direction.px">+X (right)</option>
      <option value="-x" data-i18n="geometry.two_theta.direction.nx">−X (left)</option>
      <option value="+y" data-i18n="geometry.two_theta.direction.py">+Y (down)</option>
      <option value="-y" data-i18n="geometry.two_theta.direction.ny">−Y (up)</option>
    </select>
  </div>
  <div class="field-hint is-hidden" id="geometry-two-theta-hint"></div>
`;

/** The row, placed after `afterField`; null when there is nowhere to put it. */
export function createDetectorTiltControls(afterField) {
  if (!afterField?.parentElement || typeof document === "undefined") return null;
  const field = document.createElement("label");
  field.className = "field geometry-two-theta-field";
  field.innerHTML = MARKUP;
  afterField.after(field);
  applyI18nToDom(field);
  const input = field.querySelector("#geometry-two-theta");
  attachNumberStepper(input);
  return {
    field,
    input,
    select: field.querySelector("#geometry-two-theta-direction"),
    hint: field.querySelector("#geometry-two-theta-hint"),
  };
}
