# Windows Signing

This page is for maintainers, and for forks that sign their own builds. It describes how the Windows release is signed and the CI setup that needs. ALBIS has an Authenticode signing path for Windows releases:

- `scripts/sign_windows.ps1` signs files with either Azure Artifact Signing or a PFX certificate and verifies the result with `signtool`.
- `scripts/setup_azure_artifact_signing.ps1` installs the Windows SDK Build Tools and Azure Artifact Signing client from NuGet for GitHub Actions.
- `.github/workflows/release.yml` signs `dist/ALBIS/ALBIS.exe`, rebuilds the portable zip, then lets Inno Setup sign the setup exe and generated uninstaller.
- `scripts/installer_windows.iss` enables Inno Setup signing only when the Windows signing variables are present.

## What signing does for a user

A signature shows the publisher's name instead of "Unknown publisher" and
shows whether the file was changed after it was built. It does not guarantee
a quiet first run: Windows SmartScreen can still show an "unrecognized app"
warning for a signed file until that file or publisher has built up
reputation, and an EV certificate no longer bypasses this. If you see the
warning, check that the download came from the project's GitHub releases page,
then choose **More info**, **Run anyway**.

Microsoft Store distribution avoids the warning, because Microsoft signs Store
packages. ALBIS does not ship through the Store.

References:

- Microsoft SmartScreen reputation: https://learn.microsoft.com/en-us/windows/apps/package-and-deploy/smartscreen-reputation

## Azure Artifact Signing CI setup

The ALBIS CI uses the SignTool integration, not only `azure/artifact-signing-action`, because Inno Setup must call the signer while compiling the setup exe in order to sign the generated `unins*.exe`.

Create these Azure resources:

1. Azure Artifact Signing account.
2. Completed identity validation.
3. Public Trust certificate profile.
4. Microsoft Entra app registration or managed identity for GitHub OIDC.
5. Federated credential on that identity, scoped to the **`release` GitHub environment**:

   | Field | Value |
   | --- | --- |
   | Entity type | Environment |
   | Environment name | `release` |
   | Resulting subject | `repo:SaschaAndresGrimm/ALBIS:environment:release` |
   | Issuer | `https://token.actions.githubusercontent.com` |
   | Audience | `api://AzureADTokenExchange` |

   Scope it to the environment, not to a branch or a tag. An Azure federated
   credential matches its subject exactly. The release workflow runs from
   `refs/tags/v*`, so a ref-scoped credential would have to be recreated for
   every tag, and one scoped to `refs/heads/main` would never match a release
   at all — while `artifacts.yml`, which dispatches from `main`, would need a
   second credential. The Windows build job in both workflows therefore
   declares `environment: release`, which fixes the subject for both.

   The environment needs no protection rules. Adding required reviewers to it
   would pause every tagged release for manual approval, which is a decision
   worth making deliberately rather than by accident.

6. `Artifact Signing Certificate Profile Signer` role assignment for that identity, scoped to the certificate profile or Artifact Signing account.

Microsoft limits Public Trust profiles to organizations in a short list of countries (currently the USA, Canada, the European Union and the United Kingdom) and to individual developers in the USA and Canada. Check the current list before creating the account; identity validation is the slow step. A fork that cannot get a Public Trust profile can use the PFX path below with a certificate it already has.

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

The PFX path is only practical with an exportable certificate, such as an older code signing certificate, an internal enterprise certificate, or a test/private certificate. For new public-trust code signing certificates, the CA/Browser Forum has required private keys to be generated, stored, and used in suitable hardware or cloud HSM-backed signing services since June 1, 2023. Traditional "put a PFX in CI" signing is therefore usually not available for newly issued public certificates.

References:

- CA/Browser Forum code signing baseline requirements: https://cabforum.org/uploads/Baseline-Requirements-for-the-Issuance-and-Management-of-Code-Signing.v3.6.pdf
- DigiCert summary of the 2023 private-key storage change: https://knowledge.digicert.com/general-information/new-private-key-storage-requirement-for-standard-code-signing-certificates-november-2022
