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

ALBIS is intended for local desktop use and trusted beamline/LAN environments. Public internet
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
