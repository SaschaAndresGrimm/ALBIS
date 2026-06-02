# Windows Signing

ALBIS currently has an Authenticode signing path for Windows releases:

- `scripts/sign_windows.ps1` signs files with either Azure Artifact Signing or a PFX certificate and verifies the result with `signtool`.
- `scripts/setup_azure_artifact_signing.ps1` installs the Windows SDK Build Tools and Azure Artifact Signing client from NuGet for GitHub Actions.
- `.github/workflows/release.yml` signs `dist/ALBIS/ALBIS.exe`, rebuilds the portable zip, then lets Inno Setup sign the setup exe and generated uninstaller.
- `scripts/installer_windows.iss` enables Inno Setup signing only when the Windows signing variables are present.

## What signing can and cannot do

Signing prevents tampering warnings and replaces "Unknown publisher" with the certificate publisher, but it does not guarantee a clean first-run SmartScreen experience. Microsoft documents that even valid OV/EV signed apps can show an "unrecognized app" warning until the file hash or publisher identity has reputation. Microsoft also documents that EV certificates no longer bypass SmartScreen by default.

The only Microsoft-documented way to avoid SmartScreen download warnings entirely is Microsoft Store distribution, because Store apps are signed by Microsoft. For ALBIS, that means MSIX packaging is the useful Store path if the goal is avoiding a separate certificate purchase. Microsoft also supports listing existing EXE/MSI installers, but that path still requires the installer and PE files to be signed with a CA-trusted certificate.

References:

- Microsoft SmartScreen reputation: https://learn.microsoft.com/en-us/windows/apps/package-and-deploy/smartscreen-reputation
- Microsoft Store developer account setup: https://learn.microsoft.com/en-us/windows/apps/publish/partner-center/open-a-developer-account
- Microsoft Store Win32 distribution options: https://learn.microsoft.com/en-us/windows/apps/distribute-through-store/how-to-distribute-your-win32-app-through-microsoft-store
- Microsoft Store MSI/EXE signing requirements: https://learn.microsoft.com/en-gb/windows/apps/publish/publish-your-app/msi/app-package-requirements

## Azure Artifact Signing CI setup

The ALBIS CI uses the SignTool integration, not only `azure/artifact-signing-action`, because Inno Setup must call the signer while compiling the setup exe in order to sign the generated `unins*.exe`.

Create these Azure resources:

1. Azure Artifact Signing account.
2. Completed identity validation.
3. Public Trust certificate profile.
4. Microsoft Entra app registration or managed identity for GitHub OIDC.
5. Federated credential on that identity for this repository's release and artifact workflows.
6. `Artifact Signing Certificate Profile Signer` role assignment for that identity, scoped to the certificate profile or Artifact Signing account.

Microsoft currently limits Public Trust profiles to organizations in the USA, Canada, the European Union, and the United Kingdom, and individual developers in the USA and Canada. Confirm eligibility before relying on this path for a public release.

Set these GitHub repository variables:

- `AZURE_ARTIFACT_SIGNING_ENDPOINT`: region endpoint, for example `https://swn.codesigning.azure.net`.
- `AZURE_ARTIFACT_SIGNING_ACCOUNT`: Artifact Signing account name.
- `AZURE_ARTIFACT_SIGNING_CERT_PROFILE`: certificate profile name.

Set these GitHub repository secrets:

- `AZURE_CLIENT_ID`: client ID of the Entra app/managed identity used by GitHub OIDC.
- `AZURE_TENANT_ID`: Entra tenant ID.
- `AZURE_SUBSCRIPTION_ID`: Azure subscription ID.

Optional:

- `AZURE_ARTIFACT_SIGNING_TIMESTAMP_URL`: timestamp URL override. The default is `http://timestamp.acs.microsoft.com`.
- `AZURE_ARTIFACT_SIGNING_DLIB_PATH`: full path to `Azure.CodeSigning.Dlib.dll` for self-hosted runners.
- `WINDOWS_SIGNTOOL_PATH`: full path to `signtool.exe` for self-hosted runners.

When Azure variables are present, `scripts/sign_windows.ps1` prefers Azure Artifact Signing over the legacy PFX path. Tagged public releases require either a complete Azure Artifact Signing configuration or a complete PFX configuration.

The Windows build job authenticates with `azure/login` by using GitHub OIDC. The signing dlib then uses the Azure CLI session through `DefaultAzureCredential`, so no client secret is stored in GitHub.

References:

- Azure Artifact Signing quickstart: https://learn.microsoft.com/en-us/azure/artifact-signing/quickstart
- Azure Artifact Signing role assignment: https://learn.microsoft.com/en-us/azure/trusted-signing/tutorial-assign-roles
- Azure Artifact Signing SignTool integration: https://learn.microsoft.com/en-us/azure/artifact-signing/how-to-signing-integrations
- Azure Artifact Signing GitHub Action: https://github.com/Azure/artifact-signing-action

## Legacy PFX-based CI setup

Set these GitHub Actions secrets together:

- `WINDOWS_SIGN_CERT_B64`: base64 encoded `.pfx`/`.p12` certificate bundle containing the private key.
- `WINDOWS_SIGN_CERT_PASSWORD`: password for that bundle.
- `WINDOWS_SIGN_TIMESTAMP_URL`: RFC 3161 timestamp URL, usually from your certificate provider.

Create the base64 secret locally with PowerShell:

```powershell
[Convert]::ToBase64String([IO.File]::ReadAllBytes("C:\path\to\certificate.pfx"))
```

## Important limitation of the PFX path

The PFX path is only practical if you already have an exportable certificate, such as an older code signing certificate, an internal enterprise certificate, or a test/private certificate. For new public-trust code signing certificates, the CA/Browser Forum has required private keys to be generated, stored, and used in suitable hardware or cloud HSM-backed signing services since June 1, 2023. Traditional "put a PFX in CI" signing is therefore usually not available for newly issued public certificates.

References:

- CA/Browser Forum code signing baseline requirements: https://cabforum.org/uploads/Baseline-Requirements-for-the-Issuance-and-Management-of-Code-Signing.v3.6.pdf
- DigiCert summary of the 2023 private-key storage change: https://knowledge.digicert.com/general-information/new-private-key-storage-requirement-for-standard-code-signing-certificates-november-2022

## Cheapest practical options

1. Microsoft Store with MSIX: best user experience and now free through the new onboarding flow, but requires Store packaging/submission instead of only GitHub release assets.
2. SignPath Foundation: best free option for eligible open-source projects. ALBIS is MIT licensed, so this is worth applying for. It requires project approval, a code signing policy, GitHub origin verification, and usually a signing approval step.
3. Azure Artifact Signing: easiest low-cost paid option for non-Store GitHub releases. Microsoft lists the Basic plan at $9.99/month for up to 5,000 signatures. It uses managed certificates and HSM-backed keys, but requires Azure identity validation and GitHub OIDC setup.
4. Traditional OV certificate: usually more expensive and now hardware-token or cloud-HSM based. EV is not worth buying just for SmartScreen, because Microsoft says EV no longer gives an automatic SmartScreen bypass.

References:

- SignPath Foundation: https://signpath.org/
- SignPath GitHub integration: https://docs.signpath.io/trusted-build-systems/github
- Azure Artifact Signing pricing and overview: https://azure.microsoft.com/en-us/products/artifact-signing/
- Azure Artifact Signing SignTool integration: https://learn.microsoft.com/en-us/azure/artifact-signing/how-to-signing-integrations

## Selected ALBIS path

ALBIS CI is wired for Azure Artifact Signing because it is the fastest path that keeps signing automated in GitHub Actions and still signs the Inno Setup uninstaller.

SignPath Foundation remains the lowest-cost fallback if Azure Public Trust eligibility or billing becomes a blocker.
