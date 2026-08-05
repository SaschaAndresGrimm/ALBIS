import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import fs from "node:fs";
import path from "node:path";

/**
 * Guards the wiring rather than the logic: the ids in index.html, the element
 * map, the click listener, and the i18n keys must line up, or the button is
 * inert in the real app while every unit test still passes.
 */
function readIndexHtml() {
  return fs.readFileSync(path.join(process.cwd(), "frontend", "index.html"), "utf8");
}

function readLocale(language) {
  return JSON.parse(
    fs.readFileSync(path.join(process.cwd(), "frontend", "locales", `${language}.json`), "utf8"),
  );
}

function extractSimplonGroup(html) {
  const start = html.indexOf('id="autoload-simplon"');
  const end = html.indexOf("autoload-status-primary-slot", start);
  const open = html.lastIndexOf("<div", start);
  return html.slice(open, end);
}

describe("SIMPLON connection test wiring", () => {
  beforeEach(() => {
    localStorage.clear();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
    document.body.innerHTML = "";
  });

  it("ships the markup the controller binds to", () => {
    const group = extractSimplonGroup(readIndexHtml());

    expect(group).toContain('id="simplon-url"');
    expect(group).toContain('id="simplon-test"');
    expect(group).toContain('id="simplon-probe-message"');
    expect(group).toContain('data-i18n="data_source.action.test"');
    expect(group).toContain('data-i18n-title="data_source.action.test_connection"');
    expect(group).toContain('data-i18n-placeholder="data_source.placeholder.simplon_url"');
    // The old default misled users to port 5000; SIMPLON serves on port 80.
    expect(group).not.toContain("5000");
  });

  it("keeps every key the probe and failure messages resolve in all locales", () => {
    const required = [
      "data_source.action.test",
      "data_source.action.test_connection",
      "data_source.placeholder.simplon_url",
      "hint.simplon.test",
      "hint.simplon.url",
      "autoload.status.simplon.error_reason",
      "simplon.probe.testing",
      "simplon.probe.ok",
      "simplon.probe.ok_detector",
      "simplon.probe.ok_detector_serial",
      "simplon.probe.enter_address",
      "simplon.probe.invalid_address",
      "simplon.probe.request_failed",
      "simplon.failure.dns",
      "simplon.failure.refused",
      "simplon.failure.refused_port",
      "simplon.failure.timeout",
      "simplon.failure.timeout_seconds",
      "simplon.failure.api_missing",
      "simplon.failure.api_missing_version",
      "simplon.failure.http",
      "simplon.failure.http_status",
      "simplon.failure.unreachable",
      "simplon.failure.unreachable_url",
      "simplon.failure.unknown",
    ];
    const languages = fs
      .readdirSync(path.join(process.cwd(), "frontend", "locales"))
      .filter((name) => name.endsWith(".json"))
      .map((name) => name.replace(/\.json$/, ""));

    expect(languages.length).toBeGreaterThan(1);
    languages.forEach((language) => {
      const dictionary = readLocale(language);
      required.forEach((key) => {
        expect(dictionary[key], `${language} is missing ${key}`).toBeTruthy();
      });
    });
  });

  it("probes on click through the real binding path", async () => {
    document.body.innerHTML = `
      <input id="simplon-url" type="text" />
      <button id="simplon-test" type="button"></button>
      <p id="simplon-probe-message" class="is-hidden"></p>
      <input id="simplon-version" type="text" value="1.8.0" />
    `;
    const simplonUrl = document.getElementById("simplon-url");
    const simplonTest = document.getElementById("simplon-test");
    const simplonProbeMessage = document.getElementById("simplon-probe-message");
    const simplonVersion = document.getElementById("simplon-version");
    simplonUrl.value = "192.168.1.10:5000";

    const probeCalls = [];
    vi.resetModules();
    global.fetch = vi.fn(async (url) => {
      const text = String(url);
      if (text.match(/locales\/([^/]+)\.json/)) {
        return { ok: true, json: async () => readLocale("en") };
      }
      if (text.includes("/simplon/probe")) {
        probeCalls.push(text);
        return {
          ok: true,
          status: 200,
          json: async () => ({ status: "error", code: "refused", port: 5000 }),
        };
      }
      throw new Error(`Unexpected fetch: ${text}`);
    });
    const i18n = await import("../modules/i18n.js");
    await i18n.initializeI18n({ backendLanguage: "en" });
    const { bindAutoloadControls } = await import("../modules/autoload_controls.js");

    const state = { autoload: { mode: "simplon", simplonUrl: "", running: false, types: {} } };
    const persistAutoloadSettings = vi.fn();
    bindAutoloadControls({
      apiBase: "/api",
      state,
      analysisState: {},
      backendIsLocal: true,
      elements: { simplonUrl, simplonTest, simplonProbeMessage, simplonVersion },
      callbacks: { persistAutoloadSettings, logClient: vi.fn() },
    });

    simplonTest.click();
    await vi.waitFor(() => expect(simplonProbeMessage.textContent).toContain("port 5000"));

    expect(probeCalls).toHaveLength(1);
    expect(simplonUrl.value).toBe("http://192.168.1.10:5000");
    expect(probeCalls[0]).toContain(encodeURIComponent("http://192.168.1.10:5000"));
    expect(simplonProbeMessage.classList.contains("is-error")).toBe(true);
    expect(simplonProbeMessage.classList.contains("is-hidden")).toBe(false);

    // Editing the address must retire a result that no longer describes it.
    simplonUrl.value = "det.local";
    simplonUrl.dispatchEvent(new Event("change"));
    expect(simplonProbeMessage.textContent).toBe("");
    expect(state.autoload.simplonUrl).toBe("http://det.local");
    expect(persistAutoloadSettings).toHaveBeenCalled();
  });
});
