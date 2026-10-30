#!/usr/bin/env node
/**
 * Which screenshots changed between two runs of `ui:snapshots`, and where.
 *
 *   npm run ui:diff -- /tmp/albis-ui/before /tmp/albis-ui/after [--out /tmp/albis-ui/diff] [--tolerance 16]
 *
 * Prints every pair with the share of pixels that changed and the box around
 * them, and the shots found in only one run. For each pair that changed it
 * writes `<name>` into --out (default: a `diff` folder beside the after run):
 * the after picture faded, changed pixels in magenta, the box outlined.
 *
 * Advisory: it always exits 0 when it could compare. A changed pair is
 * something to look at, not a failure -- the change may be the point. See the
 * ui-check skill (.claude/skills/ui-check/SKILL.md) for the whole check.
 */

import { mkdirSync, readFileSync, readdirSync, writeFileSync } from "node:fs";
import path from "node:path";

import { decodePng, diffImages, encodePng, pairSnapshots } from "./ui_snapshot_diff_lib.mjs";

function arg(name, fallback) {
  const i = process.argv.indexOf(`--${name}`);
  return i > 0 && process.argv[i + 1] ? process.argv[i + 1] : fallback;
}

const positional = process.argv.slice(2).filter((value, i, all) => !value.startsWith("--") && !all[i - 1]?.startsWith("--"));
if (positional.length !== 2) {
  console.error("usage: npm run ui:diff -- <before folder> <after folder> [--out folder] [--tolerance 16]");
  process.exit(2);
}
const [beforeDir, afterDir] = positional.map((dir) => path.resolve(dir));
const outDir = path.resolve(arg("out", path.join(path.dirname(afterDir), "diff")));
const tolerance = Number(arg("tolerance", 16));

const { both, onlyBefore, onlyAfter } = pairSnapshots(readdirSync(beforeDir), readdirSync(afterDir));
const rows = both.map((name) => {
  const result = diffImages(
    decodePng(readFileSync(path.join(beforeDir, name))),
    decodePng(readFileSync(path.join(afterDir, name))),
    { tolerance },
  );
  if (result.image) {
    mkdirSync(outDir, { recursive: true });
    writeFileSync(path.join(outDir, name), encodePng(result.image));
  }
  return { name, ...result };
});

const changed = rows.filter((row) => row.changed).sort((a, b) => b.share - a.share);
for (const row of changed) {
  const { left, top, right, bottom } = row.box;
  const percent = (row.share * 100).toFixed(row.share < 0.001 ? 3 : 1).padStart(6);
  const size = `${right - left + 1}x${bottom - top + 1} at ${left},${top}`;
  console.info(`changed ${percent}%  ${row.name}  (${size}${row.resized ? ", resized" : ""})`);
}
for (const name of onlyBefore) console.info(`only before      ${name}`);
for (const name of onlyAfter) console.info(`only after       ${name}`);

const same = rows.length - changed.length;
console.info(
  `\n${changed.length} changed, ${same} unchanged, ${onlyBefore.length + onlyAfter.length} unpaired` +
    (changed.length ? `; diff images in ${outDir}` : ""),
);
if (onlyBefore.length || onlyAfter.length) {
  console.info("Unpaired shots: the runs differ in which windows opened, or were taken with different --file/--width.");
}
