/**
 * JUNGFRAUJOCH "Test" control for the preview endpoint.
 *
 * Deliberately modest in what it claims: a TCP connect proves the host resolves
 * and the port is open, which separates a typo'd host from a wrong port from a
 * firewall — the three things that actually go wrong. It cannot prove the peer
 * is a JUNGFRAUJOCH publisher, so the success wording says only that the
 * endpoint accepts connections, and the bridge status remains the authority on
 * whether frames arrive.
 */

import { t } from "./i18n.js";
import {
  describeJfjochEndpointProblem,
  normalizeJfjochEndpointInput,
} from "./jfjoch_endpoint_utils.js";

export function createJfjochProbeController({ apiBase, elements, callbacks = {} }) {
  const { jfjochEndpointInput, jfjochTest, jfjochProbeMessage } = elements;
  const { persistAutoloadSettings, logClient } = callbacks;

  let inFlight = false;

  function setMessage(text, variant = "") {
    if (!jfjochProbeMessage) return;
    jfjochProbeMessage.textContent = text || "";
    jfjochProbeMessage.classList.toggle("is-hidden", !text);
    jfjochProbeMessage.classList.toggle("is-busy", variant === "busy");
    jfjochProbeMessage.classList.toggle("is-ok", variant === "ok");
    jfjochProbeMessage.classList.toggle("is-error", variant === "error");
  }

  function describeFailure(payload) {
    const code = String(payload?.code || "");
    const host = String(payload?.host || "");
    const port = payload?.port;
    const seconds = Number(payload?.timeout_s);
    switch (code) {
      case "dns":
        return t("jfjoch.failure.dns");
      case "refused":
        return port ? t("jfjoch.failure.refused_port", { port }) : t("jfjoch.failure.refused");
      case "timeout":
        return Number.isFinite(seconds) && seconds > 0
          ? t("jfjoch.failure.timeout_seconds", { seconds })
          : t("jfjoch.failure.timeout");
      default:
        return host ? t("jfjoch.failure.unreachable_host", { host }) : t("jfjoch.failure.unreachable");
    }
  }

  async function probeJfjochEndpoint() {
    if (inFlight) return;
    const endpoint = normalizeJfjochEndpointInput(jfjochEndpointInput);
    persistAutoloadSettings?.();
    const problem = describeJfjochEndpointProblem(endpoint);
    if (problem) {
      setMessage(t(`jfjoch.probe.problem.${problem}`), "error");
      jfjochEndpointInput?.focus?.();
      return;
    }

    inFlight = true;
    if (jfjochTest) jfjochTest.disabled = true;
    setMessage(t("jfjoch.probe.testing", { endpoint }), "busy");
    try {
      const res = await fetch(`${apiBase}/jfjoch/probe?endpoint=${encodeURIComponent(endpoint)}`);
      if (!res.ok) {
        setMessage(
          res.status === 400
            ? t("jfjoch.probe.problem.transport")
            : t("jfjoch.probe.request_failed"),
          "error",
        );
        return;
      }
      const payload = await res.json();
      logClient?.("info", "JUNGFRAUJOCH probe result", payload);
      if (payload?.status === "ok") {
        const port = payload.port;
        setMessage(
          port ? t("jfjoch.probe.ok_port", { port }) : t("jfjoch.probe.ok"),
          "ok",
        );
      } else {
        setMessage(describeFailure(payload), "error");
      }
    } catch (err) {
      console.error(err);
      setMessage(t("jfjoch.probe.request_failed"), "error");
    } finally {
      inFlight = false;
      if (jfjochTest) jfjochTest.disabled = false;
    }
  }

  function clearJfjochProbeMessage() {
    setMessage("");
  }

  return { probeJfjochEndpoint, clearJfjochProbeMessage };
}
