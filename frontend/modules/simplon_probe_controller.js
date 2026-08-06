/**
 * SIMPLON "Test connection" control.
 *
 * Answers the question the Base URL field cannot: does a SIMPLON monitor API
 * actually answer at this address? A wrong port, an unknown host, and a wrong
 * API version all used to look identical once polling failed, so the result
 * here names which one it was — and on success it names the detector, proving
 * the address points at the intended instrument.
 */

import { t } from "./i18n.js";
import { describeSimplonFailure } from "./simplon_diagnostics.js";
import { recordSimplonHost, renderSimplonHostOptions } from "./simplon_host_history.js";
import { normalizeSimplonUrlInput } from "./simplon_url_utils.js";

export function createSimplonProbeController({ apiBase, state, elements, callbacks = {} }) {
  const { simplonUrl, simplonVersion, simplonTest, simplonProbeMessage, simplonUrlList } = elements;
  const { persistAutoloadSettings, logClient } = callbacks;

  let inFlight = false;

  function setMessage(text, variant = "") {
    if (!simplonProbeMessage) return;
    simplonProbeMessage.textContent = text || "";
    simplonProbeMessage.classList.toggle("is-hidden", !text);
    simplonProbeMessage.classList.toggle("is-busy", variant === "busy");
    simplonProbeMessage.classList.toggle("is-ok", variant === "ok");
    simplonProbeMessage.classList.toggle("is-error", variant === "error");
  }

  function describeSuccess(payload) {
    const detector = String(payload?.detector || "").trim();
    const serial = String(payload?.serial || "").trim();
    if (detector && serial) {
      return t("simplon.probe.ok_detector_serial", { detector, serial });
    }
    if (detector) {
      return t("simplon.probe.ok_detector", { detector });
    }
    return t("simplon.probe.ok");
  }

  async function probeSimplonConnection() {
    if (inFlight) return;
    const url = normalizeSimplonUrlInput(simplonUrl);
    if (!url) {
      setMessage(t("simplon.probe.enter_address"), "error");
      simplonUrl?.focus?.();
      return;
    }
    // The field may have been normalized just now; keep the stored value in step.
    persistAutoloadSettings?.();
    const version = simplonVersion?.value?.trim() || "1.8.0";

    inFlight = true;
    if (simplonTest) simplonTest.disabled = true;
    setMessage(t("simplon.probe.testing", { url }), "busy");
    try {
      const res = await fetch(
        `${apiBase}/simplon/probe?url=${encodeURIComponent(url)}&version=${encodeURIComponent(version)}`,
      );
      if (!res.ok) {
        // 400 means the address itself is unusable; anything else is our own
        // backend failing, which is not a detector diagnosis.
        setMessage(
          res.status === 400 ? t("simplon.probe.invalid_address") : t("simplon.probe.request_failed"),
          "error",
        );
        return;
      }
      const payload = await res.json();
      logClient?.("info", "SIMPLON probe result", payload);
      if (payload?.status === "ok") {
        // A working address is worth offering back next time.
        if (state && recordSimplonHost(state, url)) {
          renderSimplonHostOptions(simplonUrlList, state.autoload.simplonRecentHosts);
          persistAutoloadSettings?.();
        }
        const parts = [describeSuccess(payload)];
        const detected = String(payload.api_version || "").trim();
        // The configured version was absent but another one answered: adopt it,
        // so the fix is applied rather than merely described.
        if (payload.code === "ok_other_version" && detected) {
          if (simplonVersion) simplonVersion.value = detected;
          if (state?.autoload) state.autoload.simplonVersion = detected;
          persistAutoloadSettings?.();
          parts.push(t("simplon.probe.version_switched", { version: detected }));
        }
        setMessage(parts.join(" "), "ok");
      } else {
        setMessage(describeSimplonFailure(payload), "error");
      }
    } catch (err) {
      console.error(err);
      setMessage(t("simplon.probe.request_failed"), "error");
    } finally {
      inFlight = false;
      if (simplonTest) simplonTest.disabled = false;
    }
  }

  function clearSimplonProbeMessage() {
    setMessage("");
  }

  return { probeSimplonConnection, clearSimplonProbeMessage };
}
