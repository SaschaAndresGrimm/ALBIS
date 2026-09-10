# Getting help with ALBIS

## Try these first

- Press **F1** inside ALBIS for the built-in help: interaction basics, keyboard
  shortcuts, data sources and troubleshooting.
- [User Guide](docs/USER_GUIDE.md) — opening data, contrast, ROI statistics,
  resolution rings, live sources, exporting.
- [Power User Guide](docs/POWER_USER_GUIDE.md) — server configuration, the
  Stream API, running from source.
- [Network Behaviour and Privacy](docs/NETWORK_AND_PRIVACY.md) — what leaves
  your machine, and how to stop it.

## Where to ask

**[GitHub issues](https://github.com/SaschaAndresGrimm/ALBIS/issues)** — the
only channel. There is no mailing list, chat or forum, and GitHub Discussions
is not enabled. Use the issue forms:

- **Bug report** for something that does not work.
- **Feature request** for something that should exist.

A question is a fine reason to open an issue. Someone else running the same
detector has probably wondered the same thing, and an issue is searchable in a
way that an email is not.

## What to include

The single most useful thing is **how to reproduce it**. After that:

- Your OS and version, and how you installed ALBIS (installer, AppImage,
  Docker, from source).
- The exact ALBIS build. **Help → About**, or the **Versions** button at the
  bottom right, names the version *and* the commit — the commit matters,
  because two builds of the same version are not necessarily the same code.
- What you expected, and what happened instead.
- The backend log. Its location is shown in **Help → Backend Log**.
- For a file that will not open: the detector, the writer that produced it, and
  — if you can share it — a small sample. Do not attach unpublished data; a
  cropped or synthetic file that reproduces the problem is better for both of
  us.

## What to expect

ALBIS has one maintainer working on it around a full-time job. Best effort, and
honestly:

- **Security reports** get priority. Use
  [private vulnerability reporting](SECURITY.md), not a public issue.
- **A crash, data corruption, or a wrong number** is next — a viewer that
  displays the wrong value is the worst thing this project can do.
- **Everything else** is handled when there is time. Some issues will sit for a
  while. An issue with no reply has not been rejected.

No service-level agreement is offered or implied. If you need one, ALBIS is
[MIT licensed](LICENSE) — you may fork it, vendor it, or pay someone to
maintain it for you. See [GOVERNANCE.md](GOVERNANCE.md).

## Not the right place

- **DECTRIS hardware, firmware or the detector's own software.** ALBIS reads
  what your detector writes; it cannot help with the instrument itself.
  Contact DECTRIS support for that, and note that ALBIS is not currently
  covered by a DECTRIS support commitment either — see
  [GOVERNANCE.md](GOVERNANCE.md).
- **Beamline-specific configuration.** Your local controls group knows your
  setup; ALBIS only knows the files and streams it is pointed at.
