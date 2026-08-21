# Network Behaviour and Privacy

This document states exactly what ALBIS sends over a network, so you do not
have to read the source to find out. It is written for the person who has to
approve ALBIS on a beamline workstation as much as for the person using it.

The short version: ALBIS collects no telemetry, has no analytics, and sends no
usage data, crash reports, file names or image data anywhere. It makes **one**
network request you did not ask for — a version check against GitHub — and that
request can be switched off in the interface.

## What ALBIS collects: nothing

There is no telemetry, no analytics, no crash reporting, no unique install
identifier, and no account. Nothing about you, your data, or your usage is
recorded off your machine. The frontend loads no fonts, scripts, stylesheets or
images from a CDN or any other third-party host — every asset ships inside the
application, so simply opening the interface contacts nothing but ALBIS itself.

## The one unsolicited request: the update check

When the interface starts, ALBIS asks GitHub whether a newer release exists:

```
GET https://api.github.com/repos/SaschaAndresGrimm/ALBIS/releases/latest
```

- **What is sent.** An ordinary HTTPS GET. The only thing ALBIS adds is a
  `User-Agent` of the form `ALBIS/0.11.0`, which tells GitHub the version being
  run. No file names, no paths, no configuration, no identifier of you or your
  machine. As with any HTTPS request, your IP address is visible to GitHub;
  that is a property of connecting, not something ALBIS transmits.
- **What comes back** is the latest release's tag and page URL, compared against
  the running version. You are notified only when a newer version exists.
- **How often.** Once per interface start, with the result cached for five
  minutes. The request times out after three seconds.
- **When it fails** — offline machine, firewall, no route to GitHub — the check
  gives up quietly and logs a warning. It never blocks startup and never
  retries in a loop. An air-gapped installation is fully functional; it simply
  never learns about updates.

### Turning it off

Uncheck **Settings → Connection → Check for updates on startup**, or set it in
`albis.config.json` before first launch:

```jsonc
{
  "ui": {
    "auto_check_updates": false
  }
}
```

With that set, ALBIS makes no outbound request of its own accord at all. For
managed or offline deployments, ship the configuration file with the setting
already `false` rather than relying on each user to change it.

## Everything else is an address you supplied

The remaining network activity exists because you pointed ALBIS at something.
None of it happens by default:

| Traffic | Destination | Starts when |
| --- | --- | --- |
| SIMPLON monitor polling | the detector control server you enter | you connect to a SIMPLON source |
| JUNGFRAUJOCH preview | the ZeroMQ endpoint you enter | you connect to a JFJoch preview source |
| Remote Stream API | inbound only — ALBIS receives, never calls out | an external producer posts frames to ALBIS |
| Health probe | `127.0.0.1` (the bundled launcher checking its own backend) | ALBIS starts |

These go to hosts on your instrument network, chosen by you. ALBIS does not
discover, scan or contact detectors on its own.

## Where your data and logs stay

Image data, file paths and the dataset structures you browse are read from disk
and served to your own browser. They are never uploaded anywhere.

The interface reports its own errors to the ALBIS backend at
`POST /api/client-log`, which writes them into the local log file alongside the
backend's own entries. Those entries can include a message, the page URL and
the browser's user-agent string. This is a request from your browser to ALBIS on
the same machine — it does not leave it, and nothing forwards the log onward.
The log lives where `logging.dir` points (by default under the data directory
for source runs, or `~/.config/albis/logs` for packaged builds), and you decide
whether to attach it to a bug report.

## Exposure and deployment

ALBIS has no authentication. It assumes the only person who can reach it is the
person sitting in front of it, and the checks described under *Reverse Proxies
and Remote Access* in the [Power User Guide](POWER_USER_GUIDE.md) exist to keep
a web page in your browser from acting on your behalf.

Running ALBIS on a trusted beamline or lab network is supported. **Exposing it
to the public internet is not a supported deployment mode** — there is nothing
to authenticate against, so anything that can reach the port can read whatever
the server can read. To report a security issue, see [SECURITY.md](../SECURITY.md).
