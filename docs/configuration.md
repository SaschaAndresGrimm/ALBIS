# ALBIS Configuration

ALBIS runtime settings are loaded from JSON and normalized by `backend/config.py`.

## Files

- Runtime config filename: `albis.config.json`
- JSON schema: `albis.config.schema.json`
- Commented template: `albis.config.example.jsonc`

Config lookup order:

1. `<cwd>/albis.config.json`
2. `<app-dir>/albis.config.json` (packaged/frozen mode)
3. `<repo-root>/albis.config.json`
4. `~/.config/albis/config.json`

In packaged mode, if no config exists, ALBIS writes defaults to `~/.config/albis/config.json`.

## Rules

- Unknown top-level sections are rejected.
- Unknown keys inside known sections are rejected.
- Values are normalized and clamped where applicable (for example port range and UI limits).
- Missing fields use defaults.

## Settings Reference

### `server`

- `host` (`string`, default `127.0.0.1`)
- `port` (`integer`, default `8000`, clamped `0..65535`)
- `reload` (`boolean`, default `false`)

### `launcher`

- `startup_timeout_sec` (`number`, default `5.0`, minimum `0.1`)
- `open_browser` (`boolean`, default `true`)
- `debug_macos_events` (`boolean`, default `false`)

### `data`

- `root` (`string`, default `""`)
- `allow_abs_paths` (`boolean`, default `true`)
- `scan_cache_sec` (`number`, default `2.0`, minimum `0.0`)
- `max_scan_depth` (`integer`, default `-1`, minimum `-1`)
- `max_upload_mb` (`integer`, default `0`, minimum `0`)

### `logging`

- `level` (`DEBUG|INFO|WARNING|ERROR|CRITICAL`, default `INFO`)
- `dir` (`string`, default `""`)

### `ui`

- `tool_hints` (`boolean`, default `false`)
- `pixel_label_min_cell_px` (`integer`, default `18`, clamped `8..64`)
- `pixel_label_max_labels` (`integer`, default `4000`, clamped `100..100000`)
- `pixel_label_format` (`auto|integer|scientific`, default `auto`)
- `pixel_label_show_during_drag` (`boolean`, default `false`)

## Example

Use `albis.config.example.jsonc` as a commented template, then save your real file as valid JSON in `albis.config.json`.
