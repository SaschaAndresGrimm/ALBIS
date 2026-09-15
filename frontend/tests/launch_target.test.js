/**
 * Reading back the file ALBIS was launched to open.
 *
 * The launcher percent-encodes a local path into `#albis-open=`, and this is
 * the other end of that. The cases that matter are the ones a beamline share
 * produces without trying: spaces, ampersands, non-ASCII names. Each has its
 * own way of arriving truncated rather than absent, which is worse -- ALBIS
 * would open the wrong file, or report that a file it mis-parsed is missing.
 */

import { describe, expect, it } from "vitest";

import {
  LAUNCH_OPEN_HASH_PREFIX,
  readLaunchTargetFromHash,
} from "../modules/launch_target.js";

/** Encode a path the way albis_launcher._open_target_url does. */
function fragmentFor(path) {
  return `#${LAUNCH_OPEN_HASH_PREFIX}${encodeURIComponent(path)}`;
}

describe("readLaunchTargetFromHash", () => {
  it("reads back a plain absolute path", () => {
    const path = "/data/beamline/frame.h5";
    expect(readLaunchTargetFromHash(fragmentFor(path))).toBe(path);
  });

  it.each([
    ["a space", "/data/two words/frame.h5"],
    ["an ampersand", "/data/run&scan/frame.h5"],
    ["a percent sign", "/data/50%done/frame.h5"],
    ["a hash", "/data/hash#tag/frame.h5"],
    ["non-ASCII", "/data/röntgen/messung.h5"],
    ["a Windows path", "C:\\Users\\sascha\\Downloads\\master.h5"],
    ["a UNC path", "\\\\beamline\\share\\frame.cbf"],
  ])("survives %s", (_label, path) => {
    expect(readLaunchTargetFromHash(fragmentFor(path))).toBe(path);
  });

  it("tolerates a fragment with no leading hash", () => {
    // `window.location.hash` includes the "#", but a caller passing the raw
    // value should not silently get nothing.
    const path = "/data/frame.h5";
    expect(readLaunchTargetFromHash(`${LAUNCH_OPEN_HASH_PREFIX}${encodeURIComponent(path)}`)).toBe(
      path,
    );
  });

  it("finds the key alongside others in the fragment", () => {
    const path = "/data/frame.h5";
    const hash = `#albis-clone=abc123&${LAUNCH_OPEN_HASH_PREFIX}${encodeURIComponent(path)}`;
    expect(readLaunchTargetFromHash(hash)).toBe(path);
  });

  it("stops at the next key rather than swallowing it", () => {
    const path = "/data/frame.h5";
    const hash = `#${LAUNCH_OPEN_HASH_PREFIX}${encodeURIComponent(path)}&albis-clone=abc123`;
    expect(readLaunchTargetFromHash(hash)).toBe(path);
  });

  it("does not match a key that merely ends with the right name", () => {
    // `not-albis-open=` is a different key; the regex anchors on a boundary.
    expect(readLaunchTargetFromHash("#not-albis-open=%2Fdata%2Fframe.h5")).toBe("");
  });

  it.each([
    ["no fragment", ""],
    ["only a hash", "#"],
    ["a different key", "#albis-clone=abc123"],
    ["the key with no value", "#albis-open="],
    ["null", null],
    ["undefined", undefined],
  ])("returns nothing for %s", (_label, hash) => {
    expect(readLaunchTargetFromHash(hash)).toBe("");
  });

  it("returns nothing for a malformed escape instead of throwing", () => {
    // decodeURIComponent throws on a lone "%". Startup must not depend on the
    // fragment being well formed; the launcher has already logged the path.
    expect(() => readLaunchTargetFromHash("#albis-open=%ZZ")).not.toThrow();
    expect(readLaunchTargetFromHash("#albis-open=%ZZ")).toBe("");
  });

  it("trims surrounding whitespace", () => {
    expect(readLaunchTargetFromHash("#albis-open=%20%2Fdata%2Fframe.h5%20")).toBe(
      "/data/frame.h5",
    );
  });
});
