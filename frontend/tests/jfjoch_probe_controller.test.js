import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import fs from "node:fs";
import path from "node:path";

function readLocale(language = "en") {
  return JSON.parse(
    fs.readFileSync(path.join(process.cwd(), "frontend", "locales", `${language}.json`), "utf8"),
  );
}

function buildElements(value = "") {
  return {
    jfjochEndpointInput: { value, focus: vi.fn() },
    jfjochTest: { disabled: false },
    jfjochProbeMessage: {
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
  const calls = [];
  global.fetch = vi.fn(async (url) => {
    const text = String(url);
    if (text.match(/locales\/([^/]+)\.json/)) {
      return { ok: true, json: async () => readLocale("en") };
    }
    calls.push(text);
    return probeHandler(text);
  });
  const i18n = await import("../modules/i18n.js");
  await i18n.initializeI18n({ backendLanguage: "en" });
  const { createJfjochProbeController } = await import("../modules/jfjoch_probe_controller.js");
  return { createJfjochProbeController, calls };
}

describe("jfjoch_probe_controller", () => {
  beforeEach(() => {
    localStorage.clear();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
  });

  it("normalizes the endpoint and reports an open port without overclaiming", async () => {
    const { createJfjochProbeController, calls } = await loadController(async () => ({
      ok: true,
      status: 200,
      json: async () => ({ status: "ok", code: "ok", host: "192.168.1.5", port: 31003 }),
    }));
    const elements = buildElements("192.168.1.5:31003");
    const persistAutoloadSettings = vi.fn();
    const controller = createJfjochProbeController({
      apiBase: "/api",
      elements,
      callbacks: { persistAutoloadSettings },
    });

    await controller.probeJfjochEndpoint();

    expect(elements.jfjochEndpointInput.value).toBe("tcp://192.168.1.5:31003");
    expect(calls[0]).toContain(encodeURIComponent("tcp://192.168.1.5:31003"));
    expect(elements.jfjochProbeMessage.textContent).toContain("accepts connections");
    // A TCP connect cannot prove the peer is JUNGFRAUJOCH, so the wording must
    // defer confirmation to the preview itself.
    expect(elements.jfjochProbeMessage.textContent).toContain("preview starts");
    expect(elements.jfjochProbeMessage.classList.contains("is-ok")).toBe(true);
    expect(persistAutoloadSettings).toHaveBeenCalled();
  });

  it.each([
    [{ code: "refused", port: 31003 }, "port 31003"],
    [{ code: "dns" }, "Host not found"],
    [{ code: "timeout", timeout_s: 2 }, "within 2 s"],
    [{ code: "unreachable", host: "jf.local" }, "jf.local"],
  ])("names the failure for %o", async (payload, expected) => {
    const { createJfjochProbeController } = await loadController(async () => ({
      ok: true,
      status: 200,
      json: async () => ({ status: "error", ...payload }),
    }));
    const elements = buildElements("jf.local:31003");
    const controller = createJfjochProbeController({ apiBase: "/api", elements, callbacks: {} });

    await controller.probeJfjochEndpoint();

    expect(elements.jfjochProbeMessage.textContent).toContain(expected);
    expect(elements.jfjochProbeMessage.classList.contains("is-error")).toBe(true);
  });

  it("refuses to probe an endpoint with no port, naming the missing piece", async () => {
    const { createJfjochProbeController, calls } = await loadController(async () => {
      throw new Error("must not be called");
    });
    const elements = buildElements("192.168.1.5");
    const controller = createJfjochProbeController({ apiBase: "/api", elements, callbacks: {} });

    await controller.probeJfjochEndpoint();

    expect(calls).toHaveLength(0);
    expect(elements.jfjochProbeMessage.textContent).toContain("Add the preview port");
    expect(elements.jfjochEndpointInput.focus).toHaveBeenCalled();
  });

  it("flags a non-ZeroMQ transport locally", async () => {
    const { createJfjochProbeController, calls } = await loadController(async () => {
      throw new Error("must not be called");
    });
    const elements = buildElements("http://jf.local:31003");
    const controller = createJfjochProbeController({ apiBase: "/api", elements, callbacks: {} });

    await controller.probeJfjochEndpoint();

    expect(calls).toHaveLength(0);
    expect(elements.jfjochProbeMessage.textContent).toContain("ZeroMQ transport");
  });

  it("asks for an endpoint when the field is empty", async () => {
    const { createJfjochProbeController } = await loadController(async () => ({ ok: true }));
    const elements = buildElements("  ");
    const controller = createJfjochProbeController({ apiBase: "/api", elements, callbacks: {} });

    await controller.probeJfjochEndpoint();

    expect(elements.jfjochProbeMessage.textContent).toContain("Enter the preview endpoint");
  });

  it("reports a backend failure and re-enables the button", async () => {
    vi.spyOn(console, "error").mockImplementation(() => {});
    const { createJfjochProbeController } = await loadController(async () => {
      throw new Error("network down");
    });
    const elements = buildElements("jf.local:31003");
    const controller = createJfjochProbeController({ apiBase: "/api", elements, callbacks: {} });

    await controller.probeJfjochEndpoint();

    expect(elements.jfjochProbeMessage.textContent).toContain("could not run");
    expect(elements.jfjochTest.disabled).toBe(false);
  });

  it("clears a stale result when asked", async () => {
    const { createJfjochProbeController } = await loadController(async () => ({
      ok: true,
      status: 200,
      json: async () => ({ status: "ok", code: "ok", port: 31003 }),
    }));
    const elements = buildElements("jf.local:31003");
    const controller = createJfjochProbeController({ apiBase: "/api", elements, callbacks: {} });

    await controller.probeJfjochEndpoint();
    controller.clearJfjochProbeMessage();

    expect(elements.jfjochProbeMessage.textContent).toBe("");
    expect(elements.jfjochProbeMessage.classList.contains("is-hidden")).toBe(true);
  });
});
