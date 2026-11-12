import { describe, expect, it } from "vitest";

import {
  RECENT_COMMANDS_LIMIT,
  groupCommands,
  parseRecentCommandIds,
  rankCommands,
  rememberRecentCommand,
  splitCommandLabel,
} from "../modules/command_palette_utils.js";

const commands = [
  { id: "open-file", label: "File: Open…", search: "file open load", shortcut: "Ctrl+O" },
  { id: "new-window", label: "File: New Window", search: "file window" },
  { id: "preferences", label: "Settings: Preferences…", search: "settings preferences options" },
  { id: "panel-toggle", label: "Panel: Open Side Panel", search: "panel side toggle collapse" },
  { id: "tab-data", label: "Panel: Data Tab", search: "panel tab data" },
  { id: "export-data", label: "Export: Convert Dataset", search: "export convert dataset tiff cbf" },
  { id: "help-docs", label: "Help: Documentation", search: "help docs documentation", shortcut: "F1" },
  { id: "quit", label: "Quit" },
];

const ids = (list) => list.map((command) => command.id);

describe("splitCommandLabel", () => {
  it("splits at the first colon, in any locale's spacing", () => {
    expect(splitCommandLabel("File: Open…")).toEqual({ category: "File", name: "Open…" });
    expect(splitCommandLabel("Fichier : Ouvrir…")).toEqual({ category: "Fichier", name: "Ouvrir…" });
    expect(splitCommandLabel("文件：打开…")).toEqual({ category: "文件", name: "打开…" });
    expect(splitCommandLabel("File: Open Geometry (.expt): x")).toEqual({
      category: "File",
      name: "Open Geometry (.expt): x",
    });
  });

  it("leaves a label without a category whole", () => {
    expect(splitCommandLabel("Quit")).toEqual({ category: "", name: "Quit" });
    expect(splitCommandLabel(undefined)).toEqual({ category: "", name: "" });
  });
});

describe("recent command ids", () => {
  it("parses only a list of unique strings, capped", () => {
    expect(parseRecentCommandIds(null)).toEqual([]);
    expect(parseRecentCommandIds("not json")).toEqual([]);
    expect(parseRecentCommandIds('{"a":1}')).toEqual([]);
    expect(parseRecentCommandIds('["a", 3, "", "a", "b"]')).toEqual(["a", "b"]);
    const many = JSON.stringify(["a", "b", "c", "d", "e", "f", "g"]);
    expect(parseRecentCommandIds(many)).toHaveLength(RECENT_COMMANDS_LIMIT);
  });

  it("moves the command just run to the front and drops the oldest", () => {
    expect(rememberRecentCommand([], "a")).toEqual(["a"]);
    expect(rememberRecentCommand(["a", "b", "c"], "c")).toEqual(["c", "a", "b"]);
    expect(rememberRecentCommand(["a", "b", "c", "d", "e"], "f")).toEqual(["f", "a", "b", "c", "d"]);
    expect(rememberRecentCommand(["a"], "")).toEqual(["a"]);
  });
});

describe("groupCommands", () => {
  it("groups by category in first-seen order and shows the name without the prefix", () => {
    const groups = groupCommands(commands, []);
    expect(groups.map((group) => group.label)).toEqual(["File", "Settings", "Panel", "Export", "Help", ""]);
    expect(groups[0].items.map((item) => item.text)).toEqual(["Open…", "New Window"]);
    expect(groups.at(-1).items[0].text).toBe("Quit");
  });

  it("puts available recent commands first, most recent first, and not again below", () => {
    const groups = groupCommands(commands, ["help-docs", "gone", "open-file"]);
    expect(groups[0].id).toBe("recent");
    expect(groups[0].items.map((item) => item.command.id)).toEqual(["help-docs", "open-file"]);
    expect(groups[0].items[0].text).toBe("Help: Documentation");
    expect(groups.find((group) => group.label === "Help")).toBeUndefined();
    expect(groups.find((group) => group.label === "File").items.map((item) => item.command.id)).toEqual([
      "new-window",
    ]);
  });

  it("has no recent group when none of the recent commands is available", () => {
    expect(groupCommands(commands, ["gone"])[0].label).toBe("File");
  });
});

describe("rankCommands", () => {
  it("returns everything for an empty query", () => {
    expect(ids(rankCommands(commands, "  "))).toEqual(ids(commands));
  });

  it("ranks a name prefix above a word start above a mid-word match", () => {
    // "Data Tab" starts with "data"; "Dataset" has it at a word start of the
    // label; nothing has it mid-word except via keywords.
    expect(ids(rankCommands(commands, "data")).slice(0, 2)).toEqual(["tab-data", "export-data"]);
    // "open": the name "Open…" and "Open Side Panel" start with it.
    const open = ids(rankCommands(commands, "open"));
    expect(open.slice(0, 2)).toEqual(["open-file", "panel-toggle"]);
    // "set" is mid-word in "Dataset" but starts "Settings".
    expect(ids(rankCommands(commands, "set"))[0]).toBe("preferences");
  });

  it("still matches the full label, including the category", () => {
    expect(ids(rankCommands(commands, "file: open"))[0]).toBe("open-file");
    expect(ids(rankCommands(commands, "help doc"))).toEqual(["help-docs"]);
  });

  it("allows letters in order as a last resort", () => {
    expect(ids(rankCommands(commands, "nwwin"))).toEqual(["new-window"]);
    expect(rankCommands(commands, "zzz")).toEqual([]);
  });

  it("matches shortcuts", () => {
    expect(ids(rankCommands(commands, "f1"))).toEqual(["help-docs"]);
  });
});
