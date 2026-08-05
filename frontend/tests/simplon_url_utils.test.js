import { describe, expect, it } from "vitest";

import { normalizeSimplonBaseUrl, normalizeSimplonUrlInput } from "../modules/simplon_url_utils.js";

describe("normalizeSimplonBaseUrl", () => {
  it("returns an empty string for blank input", () => {
    expect(normalizeSimplonBaseUrl("")).toBe("");
    expect(normalizeSimplonBaseUrl("   ")).toBe("");
    expect(normalizeSimplonBaseUrl(null)).toBe("");
    expect(normalizeSimplonBaseUrl(undefined)).toBe("");
    expect(normalizeSimplonBaseUrl("http://")).toBe("");
  });

  it("adds the http scheme to bare hosts", () => {
    expect(normalizeSimplonBaseUrl("192.168.1.10")).toBe("http://192.168.1.10");
    expect(normalizeSimplonBaseUrl("detector.example.com")).toBe("http://detector.example.com");
    expect(normalizeSimplonBaseUrl("det")).toBe("http://det");
  });

  it("keeps an explicit port instead of assuming one", () => {
    expect(normalizeSimplonBaseUrl("192.168.1.10:5000")).toBe("http://192.168.1.10:5000");
    expect(normalizeSimplonBaseUrl("det.local:80")).toBe("http://det.local:80");
    expect(normalizeSimplonBaseUrl("http://det.local:")).toBe("http://det.local");
  });

  it("repairs mistyped scheme separators", () => {
    expect(normalizeSimplonBaseUrl("http//192.168.1.10")).toBe("http://192.168.1.10");
    expect(normalizeSimplonBaseUrl("http:/192.168.1.10")).toBe("http://192.168.1.10");
    expect(normalizeSimplonBaseUrl("http:192.168.1.10")).toBe("http://192.168.1.10");
    expect(normalizeSimplonBaseUrl("https:///192.168.1.10")).toBe("https://192.168.1.10");
    expect(normalizeSimplonBaseUrl("HTTP://192.168.1.10")).toBe("http://192.168.1.10");
  });

  it("does not mistake a hostname starting with http for a scheme", () => {
    expect(normalizeSimplonBaseUrl("http-gw.local")).toBe("http://http-gw.local");
    expect(normalizeSimplonBaseUrl("https-det")).toBe("http://https-det");
  });

  it("strips SIMPLON API paths pasted from docs or a browser", () => {
    expect(normalizeSimplonBaseUrl("http://192.168.1.10/monitor/api/1.8.0")).toBe(
      "http://192.168.1.10",
    );
    expect(normalizeSimplonBaseUrl("http://192.168.1.10/monitor/api/1.8.0/images/monitor")).toBe(
      "http://192.168.1.10",
    );
    expect(normalizeSimplonBaseUrl("http://det.local/detector/api/1.8.0/config/description")).toBe(
      "http://det.local",
    );
    expect(normalizeSimplonBaseUrl("192.168.1.10/monitor/api/1.8.0")).toBe("http://192.168.1.10");
    expect(normalizeSimplonBaseUrl("http://det.local/monitor")).toBe("http://det.local");
  });

  it("preserves a reverse-proxy path prefix ahead of the API path", () => {
    expect(normalizeSimplonBaseUrl("http://gw.local/det1/monitor/api/1.8.0")).toBe(
      "http://gw.local/det1",
    );
  });

  it("trims whitespace and trailing slashes", () => {
    expect(normalizeSimplonBaseUrl("  http://192.168.1.10/  ")).toBe("http://192.168.1.10");
    expect(normalizeSimplonBaseUrl("http://192.168.1.10///")).toBe("http://192.168.1.10");
    expect(normalizeSimplonBaseUrl("http:// 192.168.1.10")).toBe("http://192.168.1.10");
  });

  it("passes through non-HTTP schemes for validation to reject", () => {
    expect(normalizeSimplonBaseUrl("tcp://192.168.1.10:31003")).toBe("tcp://192.168.1.10:31003");
  });

  it("is idempotent", () => {
    const once = normalizeSimplonBaseUrl("192.168.1.10/monitor/api/1.8.0");
    expect(normalizeSimplonBaseUrl(once)).toBe(once);
  });
});

describe("normalizeSimplonUrlInput", () => {
  it("writes the canonical value back into the field", () => {
    const input = { value: "192.168.1.10" };
    expect(normalizeSimplonUrlInput(input)).toBe("http://192.168.1.10");
    expect(input.value).toBe("http://192.168.1.10");
  });

  it("leaves an already canonical value untouched", () => {
    const input = { value: "http://det.local:8080" };
    expect(normalizeSimplonUrlInput(input)).toBe("http://det.local:8080");
    expect(input.value).toBe("http://det.local:8080");
  });

  it("tolerates a missing field", () => {
    expect(normalizeSimplonUrlInput(null)).toBe("");
    expect(normalizeSimplonUrlInput(undefined)).toBe("");
  });
});
