import { describe, expect, it } from "vitest";
import { dataSourceSectionState, missingSourceAddress, sourceChipIsModeOnly } from "../modules/data_source_utils.js";

describe("missingSourceAddress", () => {
  it("asks SIMPLON for the detector's address and JUNGFRAUJOCH for its endpoint", () => {
    expect(missingSourceAddress({ mode: "simplon", simplonUrl: "" })).toBe("simplon_address");
    expect(missingSourceAddress({ mode: "simplon", simplonUrl: "   " })).toBe("simplon_address");
    expect(missingSourceAddress({ mode: "jungfraujoch", jfjochEndpoint: "" })).toBe("jfjoch_endpoint");
  });

  it("asks nothing once the address is there, nor of a file or a remote stream", () => {
    expect(missingSourceAddress({ mode: "simplon", simplonUrl: "http://10.0.0.1:80" })).toBe("");
    expect(missingSourceAddress({ mode: "jungfraujoch", jfjochEndpoint: "tcp://host:5400" })).toBe("");
    expect(missingSourceAddress({ mode: "file" })).toBe("");
    expect(missingSourceAddress({ mode: "remote" })).toBe("");
    expect(missingSourceAddress(undefined)).toBe("");
  });
});

describe("dataSourceSectionState", () => {
  it("does not call a live source without its address active, even while it runs", () => {
    expect(dataSourceSectionState({ mode: "simplon", running: true, simplonUrl: "" }, false)).toEqual({
      tone: "empty",
      key: "data_source.state.simplon_needs_address",
    });
    expect(dataSourceSectionState({ mode: "jungfraujoch", running: true, jfjochEndpoint: "" }, false)).toEqual({
      tone: "empty",
      key: "data_source.state.jfjoch_needs_endpoint",
    });
  });

  it("calls a configured, running live source active", () => {
    expect(dataSourceSectionState({ mode: "simplon", running: true, simplonUrl: "http://d" }, false).key).toBe(
      "data_source.state.simplon_active",
    );
    expect(dataSourceSectionState({ mode: "jungfraujoch", running: true, jfjochEndpoint: "tcp://h:1" }, false).key).toBe(
      "data_source.state.jfjoch_active",
    );
    expect(dataSourceSectionState({ mode: "remote", running: true }, false)).toEqual({
      tone: "active",
      key: "data_source.state.remote_active",
    });
  });

  it("asks to configure a stopped live source that has its address", () => {
    expect(dataSourceSectionState({ mode: "simplon", running: false, simplonUrl: "http://d" }, false).key).toBe(
      "data_source.state.configure_simplon",
    );
    expect(dataSourceSectionState({ mode: "remote", running: false }, false).key).toBe("data_source.state.configure_remote");
  });

  it("says a folder is being watched, otherwise whether a file is open", () => {
    expect(dataSourceSectionState({ mode: "file", watchEnabled: true, running: true }, true)).toEqual({
      tone: "active",
      key: "data_source.state.watch_active",
    });
    expect(dataSourceSectionState({ mode: "file", watchEnabled: true, running: false }, false).key).toBe(
      "data_source.state.select_file_to_begin",
    );
    expect(dataSourceSectionState({ mode: "file" }, true)).toEqual({
      tone: "active",
      key: "data_source.state.file_mode_ready",
    });
  });
});

describe("sourceChipIsModeOnly", () => {
  it("is true only for a file source that watches no folder", () => {
    expect(sourceChipIsModeOnly({ mode: "file", watchEnabled: false })).toBe(true);
    expect(sourceChipIsModeOnly({})).toBe(true);
    expect(sourceChipIsModeOnly({ mode: "file", watchEnabled: true })).toBe(false);
    expect(sourceChipIsModeOnly({ mode: "simplon" })).toBe(false);
  });
});
