/**
 * One shape for every CSV ALBIS writes: a comment block, then one table.
 *
 * The ROI export used to stack its plots as sections -- a `# Title`, a header
 * pair, the rows, a blank line, then the next one. Each section was correct in
 * isolation and the file as a whole was not a table, so nothing read it as one:
 * Numbers imports the lot as a single sheet with the second plot's header
 * sitting in the data, and `pandas.read_csv` needs `skiprows`/`nrows` worked
 * out per section before it can start.
 *
 * Side by side instead, with each plot keeping its own pair of columns. That
 * matters because the plots do not share an x axis -- a line profile is
 * indexed along the ROI, a projection by pixel column or row, a histogram by
 * intensity in bins that are often fractional -- so one shared index column
 * beside several value columns would be a fiction. Two columns per plot states
 * each axis where it belongs, and a shorter series simply stops early, which
 * every CSV reader already handles.
 *
 * Comments only ever lead. A reader that skips a prefix (`comment="#"`, Origin's
 * skip-header) then sees a clean single-header table, which is the whole point
 * of the change.
 */

/** A value as CSV, quoted only where the text would otherwise break the row. */
export function csvField(value) {
  const text = value == null ? "" : String(value);
  if (/[",\r\n]/.test(text)) {
    return `"${text.replace(/"/g, '""')}"`;
  }
  return text;
}

export function formatCsvNumber(value) {
  return Number.isFinite(value) ? String(value) : "";
}

/**
 * Who made this file and what from, as leading comment lines.
 *
 * Same facts and nearly the same wording as the CBF and TIFF headers get from
 * `producer_string`/`_provenance_lines` in the backend, so a support question
 * about an exported number is answerable from the file whichever export it came
 * from. Deliberately not translated, unlike the column headers: this block is
 * for whoever has to reproduce the export, and a locale-dependent provenance
 * line is one more thing to normalize before two files can be compared.
 *
 * Commas are avoided rather than quoted. A comment line is not a CSV row, so
 * nothing quotes it, and a spreadsheet that shows the prefix as cells at least
 * keeps it in one of them.
 */
export function csvProvenanceLines(state, extra = []) {
  const version = state?.backendVersion || "";
  const commit = state?.backendCommit || "";
  const build = version ? `ALBIS ${version}${commit ? ` (${commit})` : ""}` : "ALBIS";
  const lines = [`# Produced by ${build}`];

  const source = String(state?.file || "")
    .split("/")
    .pop();
  if (source) {
    const parts = [`# Source: ${source}`];
    if (state?.dataset) {
      parts.push(String(state.dataset));
    }
    const frame = Number.isFinite(state?.frameIndex) ? state.frameIndex + 1 : null;
    if (frame !== null) {
      const count = Number.isFinite(state?.frameCount) ? state.frameCount : 0;
      parts.push(`frame ${frame}${count > 1 ? `/${count}` : ""}`);
    }
    if (Number.isFinite(state?.thresholdCount) && state.thresholdCount > 1) {
      parts.push(`threshold ${state.thresholdIndex + 1}/${state.thresholdCount}`);
    }
    lines.push(parts.join(" "));
  }

  extra.filter(Boolean).forEach((line) => lines.push(`# ${line}`));
  return lines;
}

/**
 * One header row and one body from columns that need not be the same length.
 *
 * `columns` is `[{ label, values }]`, already formatted: the caller knows
 * whether a figure is a count, a coordinate to three decimals or a blank, and
 * this only has to place it. Columns with no values at all are dropped, since a
 * header over nothing is worse than an absence.
 */
export function buildCsvTable(columns) {
  const present = (columns || []).filter(
    (column) => column && Array.isArray(column.values) && column.values.length > 0
  );
  if (!present.length) return [];
  const height = present.reduce((max, column) => Math.max(max, column.values.length), 0);
  if (!height) return [];

  const lines = [present.map((column) => csvField(column.label)).join(",")];
  for (let row = 0; row < height; row += 1) {
    lines.push(present.map((column) => csvField(column.values[row] ?? "")).join(","));
  }
  return lines;
}

/**
 * The two columns a plotted series contributes, from its data and plot meta.
 *
 * The x values are reconstructed from `xStart`/`xStep` rather than stored: a
 * plot keeps only its y values, because the x axis of every one of them is an
 * arithmetic sequence the renderer walks the same way.
 */
export function plotSeriesColumns({ title, data, meta, fallbackX = "x", fallbackY = "value" }) {
  if (!data || !data.length) return [];
  const xStart = Number.isFinite(meta?.xStart) ? meta.xStart : 0;
  const xStep = Number.isFinite(meta?.xStep) && meta.xStep !== 0 ? meta.xStep : 1;
  const xValues = Array.from(data, (_, idx) => formatCsvNumber(xStart + idx * xStep));
  const yValues = Array.from(data, (value) => formatCsvNumber(value));
  // Qualified by the plot's name because the axis labels collide on their own:
  // in box mode both projections call their y axis "Mean", and a line profile
  // measures "Intensity" on the axis a histogram counts it along.
  const label = (axis) => (title ? `${title}: ${axis}` : axis);
  return [
    { label: label(meta?.xLabel || fallbackX), values: xValues },
    { label: label(meta?.yLabel || fallbackY), values: yValues },
  ];
}
