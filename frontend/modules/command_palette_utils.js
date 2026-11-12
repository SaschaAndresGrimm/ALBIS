/**
 * Command palette: the pure part. Splits a label into its category and name,
 * groups the commands for an empty query (recent first, then by category),
 * ranks them for a typed query, and keeps the list of recently run commands.
 */

export const RECENT_COMMANDS_LIMIT = 5;

// "File: Open…", "Fichier : Ouvrir…" and "文件：打开…" all split at the
// first colon; a label without one has no category.
const CATEGORY_PATTERN = /^\s*([^:：]+?)\s*[:：]\s*(\S.*)$/u;

/** Split "File: Open…" into { category: "File", name: "Open…" }. */
export function splitCommandLabel(label) {
  const text = String(label ?? "");
  const match = CATEGORY_PATTERN.exec(text);
  if (!match) return { category: "", name: text.trim() };
  return { category: match[1], name: match[2].trim() };
}

/** Read the stored recent ids; anything malformed reads as no recents. */
export function parseRecentCommandIds(raw) {
  if (typeof raw !== "string" || !raw) return [];
  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return [];
  }
  if (!Array.isArray(parsed)) return [];
  const ids = [];
  for (const id of parsed) {
    if (typeof id === "string" && id && !ids.includes(id)) ids.push(id);
    if (ids.length >= RECENT_COMMANDS_LIMIT) break;
  }
  return ids;
}

/** The recent ids after running `id`: it moves to the front, the list stays short. */
export function rememberRecentCommand(recentIds, id, limit = RECENT_COMMANDS_LIMIT) {
  const previous = Array.isArray(recentIds) ? recentIds : [];
  if (typeof id !== "string" || !id) return previous.slice(0, limit);
  return [id, ...previous.filter((other) => other !== id)].slice(0, limit);
}

/**
 * Groups for an empty query: "recent" with the recent commands that are still
 * available (most recent first), then one group per category in the order the
 * categories first appear. A recent command is not listed again in its category.
 * Each group is { id, label, items: [{ command, text }] }; `text` is what the
 * row shows. The recent group's label is left for the caller to translate.
 */
export function groupCommands(commands, recentIds = []) {
  const list = Array.isArray(commands) ? commands : [];
  const byId = new Map(list.filter((command) => command?.id).map((command) => [command.id, command]));
  const recent = [];
  for (const id of recentIds) {
    const command = byId.get(id);
    if (command && !recent.includes(command)) recent.push(command);
    if (recent.length >= RECENT_COMMANDS_LIMIT) break;
  }

  const groups = [];
  if (recent.length) {
    groups.push({
      id: "recent",
      label: "",
      items: recent.map((command) => ({ command, text: command.label })),
    });
  }
  const byCategory = new Map();
  for (const command of list) {
    if (!command || recent.includes(command)) continue;
    const { category, name } = splitCommandLabel(command.label);
    let group = byCategory.get(category);
    if (!group) {
      group = { id: `category-${byCategory.size}`, label: category, items: [] };
      byCategory.set(category, group);
      groups.push(group);
    }
    group.items.push({ command, text: category ? name : command.label });
  }
  return groups;
}

function isWordChar(char) {
  return /[\p{L}\p{N}]/u.test(char);
}

function isWordStart(text, index) {
  return index === 0 || !isWordChar(text[index - 1]);
}

function isSubsequence(token, text) {
  let at = 0;
  for (const char of text) {
    if (char === token[at]) at += 1;
    if (at === token.length) return true;
  }
  return token.length === 0;
}

/** Score of one token against one text: 0 no match, higher is better. */
function scoreText(token, text, { prefix, wordStart, inside }) {
  if (!text) return 0;
  if (text.startsWith(token)) return prefix;
  let index = text.indexOf(token);
  let best = 0;
  while (index !== -1) {
    if (isWordStart(text, index)) return wordStart;
    best = inside;
    index = text.indexOf(token, index + 1);
  }
  return best;
}

/**
 * Score one token against a command; 0 means the token does not match at all.
 * Prefix of the name or label > word start in the label > keyword match >
 * mid-word in the label > anywhere else > letters in order (fuzzy).
 */
export function scoreCommandToken(command, token) {
  const label = String(command?.label || "").toLowerCase();
  const name = splitCommandLabel(label).name;
  const search = String(command?.search || "").toLowerCase();
  const extra = `${command?.id || ""} ${command?.shortcut || ""}`.toLowerCase();
  return Math.max(
    scoreText(token, name, { prefix: 100, wordStart: 70, inside: 30 }),
    scoreText(token, label, { prefix: 90, wordStart: 60, inside: 30 }),
    scoreText(token, search, { prefix: 50, wordStart: 50, inside: 20 }),
    scoreText(token, extra, { prefix: 20, wordStart: 20, inside: 10 }),
    isSubsequence(token, label) ? 5 : 0,
  );
}

/** Commands that match every word of the query, best first; ties keep their order. */
export function rankCommands(commands, query) {
  const list = Array.isArray(commands) ? commands : [];
  const tokens = String(query || "")
    .toLowerCase()
    .split(/\s+/)
    .filter(Boolean);
  if (!tokens.length) return list.slice();
  const scored = [];
  list.forEach((command, index) => {
    let total = 0;
    for (const token of tokens) {
      const score = scoreCommandToken(command, token);
      if (!score) return;
      total += score;
    }
    scored.push({ command, index, total });
  });
  scored.sort((a, b) => b.total - a.total || a.index - b.index);
  return scored.map((entry) => entry.command);
}
