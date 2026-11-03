# 0007 — Windows releases are signed with Azure Artifact Signing

**Status:** Accepted, 2026-11-03, chosen by the maintainer.

## Context

An unsigned Windows installer shows "Unknown publisher" and gives no way to
tell whether it was altered after the build. The options for signing a
GitHub-released installer differ in cost, in how much they ask of the project
and in whether CI can do the signing unattended:

- **Microsoft Store with MSIX.** The only route that avoids SmartScreen
  download warnings entirely, because Microsoft signs Store packages. It means
  Store packaging and submission instead of plain GitHub release assets.
- **SignPath Foundation.** Free for eligible open-source projects, and ALBIS
  is MIT licensed. It needs project approval, a code signing policy, GitHub
  origin verification and usually a manual approval step per signing.
- **Azure Artifact Signing.** A low-cost paid service (the Basic plan was
  listed at $9.99 a month for 5,000 signatures) with managed certificates and
  HSM-backed keys. It needs identity validation and GitHub OIDC setup, and it
  limits Public Trust profiles to organizations and individuals in certain
  countries.
- **A traditional OV or EV certificate.** Since 1 June 2023 the CA/Browser
  Forum requires the private key of a new public certificate to live in
  hardware or a cloud HSM, so "put a PFX in CI" mostly no longer works. EV no
  longer bypasses SmartScreen, so it is not worth buying for that.

No option removes the first-run SmartScreen warning for a GitHub download:
reputation still has to build up.

## Decision

CI is wired for Azure Artifact Signing, through the SignTool integration and
not only `azure/artifact-signing-action`: Inno Setup has to call the signer
while it compiles the installer, or the generated `unins*.exe` stays unsigned.
It is the fastest route that keeps signing automated in GitHub Actions. The
PFX path stays in `scripts/sign_windows.ps1` for forks and for anyone with an
exportable certificate. Setup is in [WINDOWS_SIGNING.md](../WINDOWS_SIGNING.md).

## Consequences

- A tagged release needs a complete Azure or a complete PFX configuration, so
  an unsigned Windows build cannot reach a release by an empty secret.
- Public Trust eligibility depends on the country of the signing organization.
  It must be confirmed with Microsoft before the account is created and
  identity validation starts.
- **Revisit if** Azure eligibility or billing becomes a blocker. SignPath
  Foundation is the fallback; it uses the PFX wiring already in CI. Revisit
  the Store as well if the SmartScreen warning drives users away.
