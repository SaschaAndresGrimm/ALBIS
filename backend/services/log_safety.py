"""Make a caller-influenced string safe to write as one line.

Named for the log, which is where it is mostly needed, but the guarantee --
one printable line, whatever the value contains -- is what the leading comment
block of a CSV export needs too, for the same reason and from the same values.

A log line is a line: the reader -- `journalctl`, an editor, or ALBIS's own
**View Log** dialog, which tails the file and shows it verbatim -- has nothing
to go on but the newlines. A value carrying one splits into two entries, and
the second can be given any timestamp and level its author likes, so a forged
entry sits in the record looking exactly like something the backend said.

The reachable case is narrow. Values that arrive as paths or names are checked
against the data directory or a fixed set long before they are logged, and an
HDF5 group path has to name a group that really exists in the opened file. But
HDF5 permits a newline in a group name, so a crafted file is enough, and the
file is the one thing on a beamline that comes from somewhere else.

Escaping rather than stripping, for the same reason `sanitize_header_value`
percent-encodes instead of dropping: a name that really does contain an odd
character should still be recognisable to whoever is reading the log, and a
value silently shortened is worse to debug than one that shows `\\n` where the
newline was.
"""

from __future__ import annotations

# Generous for a path or a name, short enough that one value cannot push the
# surrounding message out of a reader's view.
_LOG_VALUE_MAX = 512


def sanitize_log_value(value: object, limit: int = _LOG_VALUE_MAX) -> str:
    """Return `value` as one printable line, with control characters escaped."""
    # The line breaks are handled here, with `str.replace`, rather than in the
    # loop below that could do it in one pass. Two reasons, and the first is
    # the real one: these are the only two characters that can forge an entry,
    # so the step that stops it deserves to be legible on its own. The second
    # is that CodeQL's log-injection query recognizes `.replace("\n", ...)` and
    # nothing else as the barrier for this rule, so written any other way the
    # alert stands whatever the code actually does -- and an alert that cannot
    # be satisfied by fixing the problem gets dismissed instead of fixed.
    text = str(value).replace("\n", "\\n").replace("\r", "\\r")
    # `unicode_escape` would also mangle every non-ASCII character, which costs
    # a Japanese dataset name its readability to fix a problem it does not have.
    # Only the C0 range, DEL and the two Unicode line separators can break a
    # line, and the two above are already gone.
    out = []
    for char in text:
        code = ord(char)
        if char == "\t":
            out.append("\\t")
        elif code < 0x20 or code == 0x7F or code in (0x2028, 0x2029):
            out.append(f"\\x{code:02x}")
        else:
            out.append(char)
    escaped = "".join(out)
    if len(escaped) <= limit:
        return escaped
    return escaped[: max(0, limit - 3)] + "..."
