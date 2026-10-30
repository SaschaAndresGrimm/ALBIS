# Documentation notes for coding agents

Read the root [AGENTS.md](../AGENTS.md) first. This covers the documents in
`docs/` and the root, the changelog, and the comments in code — ALBIS's prose
follows one house style, below.

## Which document is for whom

| Document | Reader | Holds |
| --- | --- | --- |
| `README.md` | Anyone arriving | What ALBIS is, formats, install, where to go next. |
| [USER_GUIDE.md](USER_GUIDE.md) | Users | Organised by task ("Make the image readable"), not by menu. Names controls exactly as the interface does. |
| [POWER_USER_GUIDE.md](POWER_USER_GUIDE.md) | Users running the server themselves | Run modes, every config key, environment variables, the Remote Stream API. |
| [NETWORK_AND_PRIVACY.md](NETWORK_AND_PRIVACY.md) | Users and the IT staff approving ALBIS | Every network request ALBIS makes, and what it never does. |
| [COMPATIBILITY.md](COMPATIBILITY.md) | Users and integrators | What a version number promises. |
| [DEVELOPER_GUIDE.md](DEVELOPER_GUIDE.md), [ARCHITECTURE.md](ARCHITECTURE.md), [CODE_MAP.md](CODE_MAP.md) | Contributors | Setup and gates; how the parts run together; which file owns what. |
| [API_CONTRACTS.md](API_CONTRACTS.md) | Anyone calling the HTTP API | The stable contract. |
| [UI_GUIDE.md](UI_GUIDE.md), [I18N_REVIEW_GUIDE.md](I18N_REVIEW_GUIDE.md) + `I18N_GLOSSARY.csv` | Contributors | Interface rules; translation rules and fixed terms. |
| [RELEASE_CHECKLIST.md](RELEASE_CHECKLIST.md), [WINDOWS_SIGNING.md](WINDOWS_SIGNING.md) | Maintainers | Cutting and signing releases. |
| `CHANGELOG.md`, `release-notes/` | Users deciding whether to update | What changed, in their terms. |
| `decisions/` | Contributors | Why a deliberate choice was made, so it is not undone by accident. |

## What to update when

| You changed | Update |
| --- | --- |
| Anything a user can see or do | `CHANGELOG.md` under `## [Unreleased]`; the user guide if a control's behaviour or name changed. |
| Something a picture in the user guide shows | Its picture: `npm run guide:screenshots -- --only <scene>` (see [UI_GUIDE.md](UI_GUIDE.md#the-user-guides-pictures)). |
| A config key or environment variable | `POWER_USER_GUIDE.md` and `albis.config.schema.json` (tests enforce both). |
| An endpoint | `API_CONTRACTS.md`. |
| A file format | `README.md`'s format list and the in-app help (a test enforces it). |
| An outgoing connection or external URL | `NETWORK_AND_PRIVACY.md`. |
| A check command or CI gate | `CONTRIBUTING.md`, `DEVELOPER_GUIDE.md`, `RELEASE_CHECKLIST.md`, the PR template, `scripts/check.sh` (a test compares them with `ci.yml`). |
| Where code lives, or a module boundary | `CODE_MAP.md` (a test fails on a module it does not name); `DEVELOPER_GUIDE.md` for a new pattern. |
| A rule agents should follow | The nearest `AGENTS.md`. |
| A deliberate, non-obvious choice | A record in `decisions/`. |

`tests/test_documentation_consistency.py` and
`tests/test_documented_check_commands.py` fail when the documents and the code
disagree; the locale tests fail when a language is missing a key.

## House style

The same voice runs through the documents, the changelog, commit messages and
the comments in code.

- **Say why, not only what.** A rule comes with its reason; a reason
  survives cases the rule did not foresee.
- **Name the failure that motivated it.** "The schema had no
  `launcher.startup_health_timeout_sec` … so the file ALBIS itself wrote was
  rejected by ALBIS's own schema" beats "keep the schema in sync".
- **Write for the reader of that document.** The user guide speaks of
  buttons and tasks, never of modules; the changelog says what changed for
  the user first, then why.
- **Be specific.** Numbers, names, file paths: "5 ms instead of 22 ms for a
  1 Mpx frame", not "faster".
- **Plain words, full sentences.** No marketing ("seamless", "powerful"), no
  filler. Use the interface's own names for controls, in bold in user docs.
- **Present tense for what is; the changelog says what changed.**

### Changelog entries

[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) sections (`Added`,
`Changed`, `Fixed`, …) under `## [Unreleased]`. Each entry starts with a bold
sentence a user understands, then the detail:

> - **Background and foreground take the value typed.** A value typed into
>   either field was clamped to the frame's own minimum and maximum, so a fixed
>   range across a series, or a foreground above anything in the current frame,
>   could not be set. …

### Comments in code

A comment explains what the code cannot: why it is this way, what broke
before, what the obvious alternative would get wrong. It does not narrate
what the next line does. Module docstrings say what the module is for and
the guarantees it keeps (see `backend/services/log_safety.py`); a test's
docstring says what would break without it.

### Decision records

`docs/decisions/NNNN-short-title.md`: **Status**, **Context** (the problem
and the forces), **Decision**, **Consequences** (what it costs, what it rules
out, what would make us revisit it). Short — a page at most. Add one when a
choice is deliberate and looks, to a newcomer, like something to fix.
