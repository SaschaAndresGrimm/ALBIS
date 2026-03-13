import { describe, expect, it } from "vitest";

import {
  collectTechnicalReviewRows,
  collectUntranslatedFindings,
  getTechnicalBucket,
  scanHtmlForHardcodedText,
  scanJavaScriptForHardcodedText,
} from "../../scripts/i18n_review_lib.mjs";

describe("i18n review audit", () => {
  it("reports identical-to-English strings unless explicitly allowlisted", () => {
    const baseLocale = {
      "analysis.rings.beam_center": "Beam center (px)",
      "analysis.rings.placeholder.center_x": "X",
      "view.colormap.magma": "Magma",
      "series.ui.operation.median": "Median",
    };
    const localeEntries = [
      ["de", {
        "analysis.rings.beam_center": "Beam center (px)",
        "analysis.rings.placeholder.center_x": "X",
        "view.colormap.magma": "Magma",
        "series.ui.operation.median": "Median",
      }],
    ];

    const findings = collectUntranslatedFindings(baseLocale, localeEntries);

    expect(findings.map((finding) => finding.key)).toEqual([
      "analysis.rings.beam_center",
      "series.ui.operation.median",
    ]);
  });

  it("groups technical keys into review buckets", () => {
    expect(getTechnicalBucket("analysis.rings.beam_center")).toBe("analysis.rings");
    expect(getTechnicalBucket("data_source.meta.energy_ev")).toBe("data_source.meta");
    expect(getTechnicalBucket("series.ui.operation.median")).toBe("series");
    expect(getTechnicalBucket("status.series.started")).toBe("status.series");
    expect(getTechnicalBucket("hint.frame.threshold_channel")).toBe("hint.frame");
    expect(getTechnicalBucket("menu.file.open")).toBeNull();
  });

  it("builds grouped technical review rows with locale values", () => {
    const baseLocale = {
      "analysis.rings.beam_center": "Beam center (px)",
      "series.ui.operation.median": "Median",
      "status.series.started": "Series summing started",
    };
    const localeEntries = [
      ["de", {
        "analysis.rings.beam_center": "Strahlmitte (px)",
        "series.ui.operation.median": "Mittlere",
        "status.series.started": "Seriensummierung gestartet",
      }],
    ];

    const groups = collectTechnicalReviewRows(baseLocale, localeEntries);

    expect(groups.get("analysis.rings")).toEqual([
      {
        key: "analysis.rings.beam_center",
        values: {
          en: "Beam center (px)",
          de: "Strahlmitte (px)",
        },
      },
    ]);
    expect(groups.get("series")).toEqual([
      {
        key: "series.ui.operation.median",
        values: {
          en: "Median",
          de: "Mittlere",
        },
      },
    ]);
    expect(groups.get("status.series")).toEqual([
      {
        key: "status.series.started",
        values: {
          en: "Series summing started",
          de: "Seriensummierung gestartet",
        },
      },
    ]);
  });

  it("ignores HTML fallback text when the element is already bound to i18n", () => {
    const findings = scanHtmlForHardcodedText({
      filePath: "frontend/index.html",
      source: `
        <div>
          <button data-i18n="menu.file.open">Open...</button>
          <button>Review translations</button>
          <input aria-label="Frame step" />
          <input data-i18n-aria-label="toolbar.step" aria-label="Frame step" />
        </div>
      `,
    });

    expect(findings).toEqual([
      {
        filePath: "frontend/index.html",
        kind: "html-text",
        line: 4,
        text: "Review translations",
      },
      {
        filePath: "frontend/index.html",
        kind: "html-aria-label",
        line: 5,
        text: "Frame step",
      },
    ]);
  });

  it("reports hardcoded JavaScript UI literals with file locations", () => {
    const findings = scanJavaScriptForHardcodedText({
      filePath: "frontend/modules/example.js",
      source: `
        button.textContent = "Review translations";
        button.setAttribute("aria-label", "Open panel");
        window.prompt("Save output");
        setStatus("Series failed");
        button.textContent = t("menu.file.open");
        console.warn("debug only");
      `,
    });

    expect(findings).toEqual([
      {
        filePath: "frontend/modules/example.js",
        kind: "js-text",
        line: 2,
        text: "Review translations",
      },
      {
        filePath: "frontend/modules/example.js",
        kind: "js-attribute",
        line: 3,
        text: "Open panel",
      },
      {
        filePath: "frontend/modules/example.js",
        kind: "js-dialog",
        line: 4,
        text: "Save output",
      },
      {
        filePath: "frontend/modules/example.js",
        kind: "js-status",
        line: 5,
        text: "Series failed",
      },
    ]);
  });
});
