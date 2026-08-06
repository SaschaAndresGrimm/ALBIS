import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const DICTIONARY = {
  "simplon.probe.testing": "Testing {{url}}…",
  "simplon.probe.ok": "Connected — the SIMPLON monitor API answered.",
  "simplon.probe.ok_detector": "Connected — {{detector}}",
  "simplon.probe.ok_detector_serial": "Connected — {{detector}} (S/N {{serial}})",
  "simplon.probe.enter_address": "Enter the detector hostname or IP address first.",
  "simplon.probe.invalid_address": "That address cannot be used.",
  "simplon.probe.request_failed": "The test could not run.",
  "simplon.probe.version_switched": "API version switched to {{version}}.",
  "simplon.failure.dns": "Host not found — check the hostname or IP address.",
  "simplon.failure.refused_port": "Connection refused on port {{port}} — SIMPLON normally listens on port 80.",
  "simplon.failure.timeout_seconds": "No response within {{seconds}} s.",
  "simplon.failure.api_missing_version": "Reachable, but no SIMPLON API {{version}} here.",
  "simplon.failure.http_status": "The detector answered with HTTP {{status}}.",
  "simplon.failure.unreachable_url": "Cannot reach {{url}}.",
  "simplon.failure.unknown": "The detector could not be contacted.",
};

function buildFetchMock(probeHandler) {
  return vi.fn(async (url) => {
    const text = String(url);
    if (text.match(/locales\/([^/]+)\.json/)) {
      return { ok: true, json: async () => DICTIONARY };
    }
    return probeHandler(text);
  });
}

function buildElements(value = "") {
  return {
    simplonUrl: { value, focus: vi.fn() },
    simplonVersion: { value: "1.8.0" },
    simplonTest: { disabled: false },
    simplonProbeMessage: {
      textContent: "",
      classList: {
        classes: new Set(),
        toggle(name, on) {
          if (on) this.classes.add(name);
          else this.classes.delete(name);
        },
        contains(name) {
          return this.classes.has(name);
        },
      },
    },
  };
}

async function loadController(probeHandler) {
  vi.resetModules();
  global.fetch = buildFetchMock(probeHandler);
  const i18n = await import("../modules/i18n.js");
  await i18n.initializeI18n({ backendLanguage: "en" });
  const { createSimplonProbeController } = await import("../modules/simplon_probe_controller.js");
  return createSimplonProbeController;
}

describe("simplon_probe_controller", () => {
  beforeEach(() => {
    localStorage.clear();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
  });

  it("normalizes the address before probing and reports the detector on success", async () => {
    const requests = [];
    const create = await loadController(async (url) => {
      requests.push(url);
      return {
        ok: true,
        status: 200,
        json: async () => ({
          status: "ok",
          code: "ok",
          detector: "EIGER2 CdTe 4M",
          serial: "E-32-0123",
        }),
      };
    });
    const elements = buildElements("192.168.1.10");
    const persistAutoloadSettings = vi.fn();
    const controller = create({
      apiBase: "/api",
      elements,
      callbacks: { persistAutoloadSettings },
    });

    await controller.probeSimplonConnection();

    expect(elements.simplonUrl.value).toBe("http://192.168.1.10");
    expect(requests[0]).toContain(`url=${encodeURIComponent("http://192.168.1.10")}`);
    expect(requests[0]).toContain("version=1.8.0");
    expect(persistAutoloadSettings).toHaveBeenCalled();
    expect(elements.simplonProbeMessage.textContent).toBe(
      "Connected — EIGER2 CdTe 4M (S/N E-32-0123)",
    );
    expect(elements.simplonProbeMessage.classList.contains("is-ok")).toBe(true);
    expect(elements.simplonProbeMessage.classList.contains("is-hidden")).toBe(false);
    expect(elements.simplonTest.disabled).toBe(false);
  });

  it("falls back to a generic success message when the detector is unnamed", async () => {
    const create = await loadController(async () => ({
      ok: true,
      status: 200,
      json: async () => ({ status: "ok", code: "ok", detector: "", serial: "" }),
    }));
    const elements = buildElements("det.local");
    const controller = create({ apiBase: "/api", elements, callbacks: {} });

    await controller.probeSimplonConnection();

    expect(elements.simplonProbeMessage.textContent).toBe(
      "Connected — the SIMPLON monitor API answered.",
    );
  });

  it.each([
    [
      { status: "error", code: "refused", port: 5000 },
      "Connection refused on port 5000 — SIMPLON normally listens on port 80.",
    ],
    [
      { status: "error", code: "dns" },
      "Host not found — check the hostname or IP address.",
    ],
    [
      { status: "error", code: "timeout", timeout_s: 3 },
      "No response within 3 s.",
    ],
    [
      { status: "error", code: "api_missing", api_version: "1.8.0" },
      "Reachable, but no SIMPLON API 1.8.0 here.",
    ],
    [
      { status: "error", code: "http_error", http_status: 500 },
      "The detector answered with HTTP 500.",
    ],
    [
      { status: "error", code: "unreachable", url: "http://det.local" },
      "Cannot reach http://det.local.",
    ],
    [
      { status: "error", code: "something-new" },
      "The detector could not be contacted.",
    ],
  ])("names the failure for %o", async (payload, expected) => {
    const create = await loadController(async () => ({
      ok: true,
      status: 200,
      json: async () => payload,
    }));
    const elements = buildElements("192.168.1.10");
    const controller = create({ apiBase: "/api", elements, callbacks: {} });

    await controller.probeSimplonConnection();

    expect(elements.simplonProbeMessage.textContent).toBe(expected);
    expect(elements.simplonProbeMessage.classList.contains("is-error")).toBe(true);
  });

  it("asks for an address instead of probing an empty field", async () => {
    const probe = vi.fn();
    const create = await loadController(probe);
    const elements = buildElements("   ");
    const controller = create({ apiBase: "/api", elements, callbacks: {} });

    await controller.probeSimplonConnection();

    expect(probe).not.toHaveBeenCalled();
    expect(elements.simplonProbeMessage.textContent).toBe(
      "Enter the detector hostname or IP address first.",
    );
    expect(elements.simplonUrl.focus).toHaveBeenCalled();
  });

  it("separates an unusable address (400) from a backend failure (500)", async () => {
    let create = await loadController(async () => ({ ok: false, status: 400 }));
    let elements = buildElements("tcp://det.local");
    await create({ apiBase: "/api", elements, callbacks: {} }).probeSimplonConnection();
    expect(elements.simplonProbeMessage.textContent).toBe("That address cannot be used.");

    create = await loadController(async () => ({ ok: false, status: 500 }));
    elements = buildElements("det.local");
    await create({ apiBase: "/api", elements, callbacks: {} }).probeSimplonConnection();
    expect(elements.simplonProbeMessage.textContent).toBe("The test could not run.");
  });

  it("reports a thrown request and re-enables the button", async () => {
    vi.spyOn(console, "error").mockImplementation(() => {});
    const create = await loadController(async () => {
      throw new Error("network down");
    });
    const elements = buildElements("det.local");
    const controller = create({ apiBase: "/api", elements, callbacks: {} });

    await controller.probeSimplonConnection();

    expect(elements.simplonProbeMessage.textContent).toBe("The test could not run.");
    expect(elements.simplonTest.disabled).toBe(false);
  });

  it("ignores a second click while a probe is in flight", async () => {
    let resolveProbe;
    const create = await loadController(
      () =>
        new Promise((resolve) => {
          resolveProbe = () => resolve({ ok: true, status: 200, json: async () => ({ status: "ok", code: "ok" }) });
        }),
    );
    const elements = buildElements("det.local");
    const controller = create({ apiBase: "/api", elements, callbacks: {} });

    const first = controller.probeSimplonConnection();
    expect(elements.simplonTest.disabled).toBe(true);
    expect(elements.simplonProbeMessage.classList.contains("is-busy")).toBe(true);
    await controller.probeSimplonConnection();
    resolveProbe();
    await first;

    expect(global.fetch.mock.calls.filter(([url]) => String(url).includes("/simplon/probe"))).toHaveLength(1);
    expect(elements.simplonTest.disabled).toBe(false);
  });

  it("adopts an API version the detector actually serves", async () => {
    const create = await loadController(async () => ({
      ok: true,
      status: 200,
      json: async () => ({
        status: "ok",
        code: "ok_other_version",
        api_version: "1.6.0",
        requested_version: "1.8.0",
        detector: "EIGER2 CdTe 1M",
      }),
    }));
    const elements = buildElements("det.local");
    const state = { autoload: { simplonRecentHosts: [], simplonVersion: "1.8.0" } };
    const persistAutoloadSettings = vi.fn();
    const controller = create({
      apiBase: "/api",
      state,
      elements,
      callbacks: { persistAutoloadSettings },
    });

    await controller.probeSimplonConnection();

    expect(elements.simplonVersion.value).toBe("1.6.0");
    expect(state.autoload.simplonVersion).toBe("1.6.0");
    expect(elements.simplonProbeMessage.textContent).toBe(
      "Connected — EIGER2 CdTe 1M API version switched to 1.6.0.",
    );
    expect(persistAutoloadSettings).toHaveBeenCalled();
  });

  it("leaves the configured version alone on a plain success", async () => {
    const create = await loadController(async () => ({
      ok: true,
      status: 200,
      json: async () => ({ status: "ok", code: "ok", api_version: "1.8.0" }),
    }));
    const elements = buildElements("det.local");
    const state = { autoload: { simplonRecentHosts: [], simplonVersion: "1.8.0" } };
    const controller = create({ apiBase: "/api", state, elements, callbacks: {} });

    await controller.probeSimplonConnection();

    expect(elements.simplonVersion.value).toBe("1.8.0");
    expect(elements.simplonProbeMessage.textContent).not.toContain("switched");
  });

  it("remembers a working address and offers it back", async () => {
    const create = await loadController(async () => ({
      ok: true,
      status: 200,
      json: async () => ({ status: "ok", code: "ok" }),
    }));
    const elements = buildElements("192.168.1.10");
    elements.simplonUrlList = document.createElement("datalist");
    const state = { autoload: { simplonRecentHosts: [] } };
    const persistAutoloadSettings = vi.fn();
    const controller = create({
      apiBase: "/api",
      state,
      elements,
      callbacks: { persistAutoloadSettings },
    });

    await controller.probeSimplonConnection();

    expect(state.autoload.simplonRecentHosts).toEqual(["http://192.168.1.10"]);
    expect([...elements.simplonUrlList.querySelectorAll("option")].map((o) => o.value)).toEqual([
      "http://192.168.1.10",
    ]);
    expect(persistAutoloadSettings).toHaveBeenCalled();
  });

  it("does not remember an address that failed", async () => {
    const create = await loadController(async () => ({
      ok: true,
      status: 200,
      json: async () => ({ status: "error", code: "dns" }),
    }));
    const elements = buildElements("typo.invalid");
    const state = { autoload: { simplonRecentHosts: [] } };
    const controller = create({ apiBase: "/api", state, elements, callbacks: {} });

    await controller.probeSimplonConnection();

    expect(state.autoload.simplonRecentHosts).toEqual([]);
  });

  it("clears a stale result when asked", async () => {
    const create = await loadController(async () => ({
      ok: true,
      status: 200,
      json: async () => ({ status: "ok", code: "ok" }),
    }));
    const elements = buildElements("det.local");
    const controller = create({ apiBase: "/api", elements, callbacks: {} });

    await controller.probeSimplonConnection();
    controller.clearSimplonProbeMessage();

    expect(elements.simplonProbeMessage.textContent).toBe("");
    expect(elements.simplonProbeMessage.classList.contains("is-hidden")).toBe(true);
  });
});
