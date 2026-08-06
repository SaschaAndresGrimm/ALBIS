import { describe, expect, it } from "vitest";

import {
  describeJfjochEndpointProblem,
  normalizeJfjochEndpoint,
  normalizeJfjochEndpointInput,
} from "../modules/jfjoch_endpoint_utils.js";

describe("normalizeJfjochEndpoint", () => {
  it("returns an empty string for blank input", () => {
    expect(normalizeJfjochEndpoint("")).toBe("");
    expect(normalizeJfjochEndpoint("   ")).toBe("");
    expect(normalizeJfjochEndpoint(null)).toBe("");
    expect(normalizeJfjochEndpoint("tcp://")).toBe("");
  });

  it("adds the tcp transport to a bare host:port", () => {
    expect(normalizeJfjochEndpoint("192.168.1.5:31003")).toBe("tcp://192.168.1.5:31003");
    expect(normalizeJfjochEndpoint("jfjoch.example.com:31003")).toBe(
      "tcp://jfjoch.example.com:31003",
    );
  });

  it("repairs mistyped transport separators", () => {
    expect(normalizeJfjochEndpoint("tcp//192.168.1.5:31003")).toBe("tcp://192.168.1.5:31003");
    expect(normalizeJfjochEndpoint("tcp:/192.168.1.5:31003")).toBe("tcp://192.168.1.5:31003");
    expect(normalizeJfjochEndpoint("tcp:192.168.1.5:31003")).toBe("tcp://192.168.1.5:31003");
    expect(normalizeJfjochEndpoint("TCP://192.168.1.5:31003")).toBe("tcp://192.168.1.5:31003");
  });

  it("trims whitespace and trailing slashes", () => {
    expect(normalizeJfjochEndpoint("  tcp://host:31003/  ")).toBe("tcp://host:31003");
    expect(normalizeJfjochEndpoint("tcp://host:31003///")).toBe("tcp://host:31003");
  });

  it("keeps path-based transports intact", () => {
    // `ipc:///tmp/x` means the absolute path /tmp/x — the slashes are content.
    expect(normalizeJfjochEndpoint("ipc:///tmp/jf.sock")).toBe("ipc:///tmp/jf.sock");
    expect(normalizeJfjochEndpoint("inproc://preview")).toBe("inproc://preview");
  });

  it("passes a non-ZeroMQ transport through for validation to reject", () => {
    expect(normalizeJfjochEndpoint("http://host:31003")).toBe("http://host:31003");
  });

  it("is idempotent", () => {
    const once = normalizeJfjochEndpoint("192.168.1.5:31003");
    expect(normalizeJfjochEndpoint(once)).toBe(once);
  });
});

describe("describeJfjochEndpointProblem", () => {
  it("accepts a usable tcp endpoint", () => {
    expect(describeJfjochEndpointProblem("tcp://192.168.1.5:31003")).toBe("");
  });

  it("accepts path transports without demanding a port", () => {
    expect(describeJfjochEndpointProblem("ipc:///tmp/jf.sock")).toBe("");
    expect(describeJfjochEndpointProblem("inproc://preview")).toBe("");
  });

  it("names the missing piece", () => {
    expect(describeJfjochEndpointProblem("")).toBe("empty");
    // No default preview port exists, so a bare host is not usable.
    expect(describeJfjochEndpointProblem("tcp://192.168.1.5")).toBe("port");
    expect(describeJfjochEndpointProblem("tcp://192.168.1.5:abc")).toBe("port");
    expect(describeJfjochEndpointProblem("tcp://:31003")).toBe("host");
    expect(describeJfjochEndpointProblem("http://host:31003")).toBe("transport");
  });
});

describe("normalizeJfjochEndpointInput", () => {
  it("writes the canonical value back into the field", () => {
    const input = { value: "192.168.1.5:31003" };
    expect(normalizeJfjochEndpointInput(input)).toBe("tcp://192.168.1.5:31003");
    expect(input.value).toBe("tcp://192.168.1.5:31003");
  });

  it("tolerates a missing field", () => {
    expect(normalizeJfjochEndpointInput(null)).toBe("");
  });
});
