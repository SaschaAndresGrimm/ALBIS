# Security Policy

If you discover a security issue, please report it responsibly.

## Reporting

Use **GitHub Private Vulnerability Reporting** for suspected vulnerabilities in public releases.
Do **not** open a public GitHub issue for security-sensitive reports.

If private reporting is temporarily unavailable, contact the maintainers directly and include
enough detail to reproduce and assess impact.

## Supported Versions

Security fixes are accepted and triaged for the **latest published release**, whatever its
version number. Fixes ship in a new release rather than as patches to older ones, so the
remedy for a reported issue is to update.

Older releases receive no security fixes, and pre-release or self-built artifacts are
best-effort only. If you are pinned to an older version and cannot update, say so in your
report — it affects how the fix is described, though not which release carries it.

ALBIS is currently in `0.x`. In line with semantic versioning, that means configuration keys
and API details may still change between minor releases; it does not weaken the commitment
above. What `1.0.0` will promise, and what it deliberately will not, is written down in
[Compatibility Policy](docs/COMPATIBILITY.md).

## Scope

ALBIS is intended for local desktop use and trusted lab or instrument networks. Public internet
exposure is not a supported deployment mode: ALBIS has no authentication, so anything able to
reach the port can read whatever the server can read. Reports amounting to "an unauthenticated
listener exposed to the internet is readable" describe a deployment choice rather than a
vulnerability.

Please highlight:
- Any data exposure issues
- Remote access or authentication bypass risks
- Unsafe default configurations

For what ALBIS sends over the network by default — and what it deliberately does not — see
[Network Behaviour and Privacy](docs/NETWORK_AND_PRIVACY.md).

## What is inside a download, and how it is watched

Every release carries a CycloneDX bill of materials per platform,
`ALBIS-<platform>-<version>.cdx.json`, listing what that download contains: the Python
packages, the Python interpreter, the C libraries inside both (OpenSSL, zlib, HDF5, libzmq
and others) with their versions, and the third-party files in the web interface. It is
covered by the signed `SHA256SUMS.txt`. The Docker image carries its own as a registry
attestation.

These are checked for known vulnerabilities with [grype](https://github.com/anchore/grype):

- **Before a release.** A known vulnerability rated Critical in what is about to ship stops
  the release, unless it was reviewed and found not to apply to ALBIS. Those decisions are
  recorded, with the reason, in [`.grype.yaml`](.grype.yaml).
- **Every week, after a release.** The bills of materials of the latest release are scanned
  again, so a vulnerability published after the release is noticed too.

The pinned Python dependencies are audited with `pip-audit`, and the web interface's with
`npm audit`, on every pull request.
