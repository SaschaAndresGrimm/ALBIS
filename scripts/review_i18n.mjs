#!/usr/bin/env node

import path from "node:path";

import {
  buildI18nReviewReport,
  formatI18nReviewReport,
  getExitCodeForReview,
} from "./i18n_review_lib.mjs";

const strictUntranslated = process.argv.includes("--strict-untranslated");
const rootDir = process.cwd();
const report = buildI18nReviewReport(rootDir);

process.stdout.write(formatI18nReviewReport(report, { strictUntranslated }));

const exitCode = getExitCodeForReview(report, { strictUntranslated });
if (exitCode !== 0) {
  process.stderr.write(
    `\nStrict untranslated mode failed: ${report.untranslatedFindings.length} suspect carryovers found in ${path.basename(rootDir)}.\n`,
  );
}
process.exit(exitCode);
