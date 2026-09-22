# Network Behaviour and Privacy

This document states exactly what ALBIS sends over a network, so you do not
have to read the source to find out. It is written for the person who has to
approve ALBIS on a beamline workstation as much as for the person using it.

The short version: ALBIS collects no telemetry, has no analytics, and sends no
usage data, crash reports, file names or image data anywhere. It makes **one**
network request you did not ask for — a version check against GitHub — and that
request can be switched off in the interface. If that check finds a newer
release, clicking **Download Update** fetches that one file from GitHub and
verifies it; nothing is fetched or installed without the click, and that step
can be switched off too.

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
- **What comes back** is the latest release's tag, its page URL and the list of
  files attached to it, compared against the running version. You are notified
  only when a newer version exists.
- **What ALBIS does with the answer.** It works out how this copy was installed
  — AppImage, Windows installer, Windows portable zip, macOS app, container, or
  a source checkout — and shows either the single file to download or the single
  command to run. That detection is entirely local: an environment variable set
  in the container image, whether this is a packaged build, the platform, the CPU
  architecture, and on Windows the installer's own uninstall entry. None of it is
  sent anywhere.
- **Nothing is fetched until you click.** There is no auto-update and no
  background downloader. What the click does depends on one setting, below.
- **How often.** Once per interface start, with the result cached for five
  minutes. The request times out after three seconds.
- **When it fails** — offline machine, firewall, no route to GitHub — the check
  gives up quietly and logs a warning. It never blocks startup and never
  retries in a loop. An air-gapped installation is fully functional; it simply
  never learns about updates.

### Downloading the update

When you click **Download Update**, ALBIS fetches that one release asset over
HTTPS and checks it before handing it over:

```
GET https://github.com/SaschaAndresGrimm/ALBIS/releases/download/<tag>/<asset>
GET https://github.com/SaschaAndresGrimm/ALBIS/releases/download/<tag>/SHA256SUMS.txt
GET https://github.com/SaschaAndresGrimm/ALBIS/releases/download/<tag>/SHA256SUMS.txt.sig
```

- **Why ALBIS does this rather than your browser.** Nobody verifies a browser
  download. ALBIS hashes the file as it arrives and compares it against the
  checksum the release published for it. A file that does not match is deleted,
  not offered. A release that publishes no checksum list is reported as
  unverified rather than withheld.
- **What is sent.** Three ordinary HTTPS GETs with no request body and no
  identifier. A redirect that is not HTTPS is refused. Only URLs under
  `https://github.com/` are ever fetched.
- **Where the file goes.** Your `Downloads` folder when you have one, otherwise
  the system temporary directory. The name is the release asset's own, and the
  interface shows you the full path.
- **The signature check runs where it can.** ALBIS verifies the GPG signature
  over `SHA256SUMS.txt` when `gpg` is present and the build ships the release
  public key — in practice, Linux. Being *unable* to check it is reported and
  nothing more: on macOS and Windows the operating system checks notarization
  and Authenticode itself when you open the installer. A signature that is
  present and does **not** verify is different, and fails the download: a
  checksum taken from a list that cannot be trusted is not evidence, so the
  file is deleted rather than handed over labelled "verified".
- **Installing it is a separate step, off by default.** By default the last
  thing ALBIS does is open the folder the file landed in, and you apply the
  update. See below for the setting that changes that.

To switch the download off and keep the dialog to a link only, uncheck
**Settings → Connection → Download and verify updates in ALBIS**, or set it in
`albis.config.json` before first launch:

```jsonc
{
  "ui": {
    "allow_update_download": false
  }
}
```

With that set, the button opens the release asset in your browser instead, and
ALBIS writes no installer to disk.

### Installing the update

Off unless switched on, in **Settings → Connection → Install updates from
ALBIS** or in `albis.config.json`. With `ui.allow_update_apply: true`, a
verified download gains an **Install Update** button, which makes no further
network request — it acts on the file already on disk:

- **On Linux** ALBIS replaces the AppImage it is running from, at the path the
  AppImage runtime reports in `APPIMAGE`, by staging a copy beside it and
  renaming it into place. That is atomic: either the old file or the new one is
  there, never half of either. The file's mode is carried over, not assumed.
- **On Windows** ALBIS runs the downloaded installer silently. The installer
  then does what it does for a manual install: it asks ALBIS to close, waits,
  and updates in place. Nothing new is invented for this.
- **Nowhere else.** macOS is excluded because replacing a running `.app` risks
  invalidating the notarization you are relying on; a container cannot replace
  its own image; a source checkout is your working tree. Those keep the
  download-and-show-the-folder behaviour whatever the setting says.

Three refusals are built in: an update whose checksum did not verify is never
installed, even though it is still offered as a download; nothing is installed
while a live watch, series sum or export is running; and ALBIS does not restart
itself afterwards — it closes, and you start it again.

```jsonc
{
  "ui": {
    "allow_update_apply": true
  }
}
```

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

With that set, ALBIS makes no outbound request of its own accord at all, and
nothing to download is ever offered. For managed or offline deployments, ship
the configuration file with the setting already `false` rather than relying on
each user to change it.

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
