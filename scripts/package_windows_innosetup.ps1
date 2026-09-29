$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

$versionInfo = python .\scripts\version_info.py --json | ConvertFrom-Json
$version = $versionInfo.version
$tag = $versionInfo.tag
$outBase = "ALBIS-Setup-" + $versionInfo.target + "-" + $tag

if (-not (Test-Path ".\\dist\\ALBIS")) {
  Write-Host "Missing dist\\ALBIS. Run .\\scripts\\build_windows.ps1 first."
  exit 1
}

$iscc = Get-Command iscc -ErrorAction SilentlyContinue
if (-not $iscc) {
  Write-Host "Inno Setup (ISCC) not found. Install it, then rerun."
  exit 1
}

# The Inno Setup version CI builds with. .github/workflows/release.yml and
# artifacts.yml install exactly this; tests/test_installer_images.py keeps the
# three in step. It is checked here too because `choco install` reports a
# copy already on the runner image as "already installed" and moves on, so
# the pin alone does not prove which compiler ran.
$pinnedInnoVersion = [version]"6.7.1"
# The wizard images and WizardStyle in installer_windows.iss need this: 6.6.0
# changed the image sizes and added the styles.
$minimumInnoVersion = [version]"6.6.0"

$isccInfo = (Get-Item $iscc.Source).VersionInfo
$foundInnoVersion = $null
foreach ($candidate in @($isccInfo.ProductVersion, $isccInfo.FileVersion)) {
  if ($candidate -match '^\d+(\.\d+){1,3}') {
    $foundInnoVersion = [version]$Matches[0]
    break
  }
}
if (-not $foundInnoVersion) {
  throw "Could not read the Inno Setup version from $($iscc.Source) (ProductVersion '$($isccInfo.ProductVersion)', FileVersion '$($isccInfo.FileVersion)')."
}
$foundInnoRelease = [version]::new($foundInnoVersion.Major, $foundInnoVersion.Minor, [Math]::Max(0, $foundInnoVersion.Build))
Write-Host "Using Inno Setup $foundInnoRelease ($($iscc.Source))"

if ($env:GITHUB_ACTIONS -eq "true") {
  # CI: exactly the pinned version, so a release is built the same way twice.
  if ($foundInnoRelease -ne $pinnedInnoVersion) {
    throw "CI must build with Inno Setup $pinnedInnoVersion, found $foundInnoRelease. Update the pin in this script and both workflows together."
  }
} else {
  # A local build: anything new enough works, but say so if it differs.
  if ($foundInnoRelease -lt $minimumInnoVersion) {
    throw "Inno Setup $minimumInnoVersion or later is required for the wizard styling, found $foundInnoRelease."
  }
  if ($foundInnoRelease -ne $pinnedInnoVersion) {
    Write-Warning "Building with Inno Setup $foundInnoRelease; CI uses $pinnedInnoVersion."
  }
}

function Get-ConfiguredSigningVarNames {
  param(
    [Parameter(Mandatory = $true)]
    [hashtable]$Vars
  )

  return @(
    $Vars.GetEnumerator() |
      Where-Object { -not [string]::IsNullOrWhiteSpace($_.Value) } |
      ForEach-Object { $_.Key }
  )
}

function Assert-CompleteSigningVarSet {
  param(
    [Parameter(Mandatory = $true)]
    [string]$Name,

    [Parameter(Mandatory = $true)]
    [hashtable]$Vars,

    [string[]]$ConfiguredVars = @()
  )

  $configuredCount = if ($null -eq $ConfiguredVars) { 0 } else { $ConfiguredVars.Count }
  if ($configuredCount -gt 0 -and $configuredCount -lt $Vars.Count) {
    $missing = @(
      $Vars.Keys |
        Where-Object { [string]::IsNullOrWhiteSpace($Vars[$_]) } |
        Sort-Object
    )
    throw "$Name signing is partially configured. Missing: $($missing -join ', ')"
  }
}

$pfxSigningVars = @{
  WINDOWS_SIGN_CERT_B64 = $env:WINDOWS_SIGN_CERT_B64
  WINDOWS_SIGN_CERT_PASSWORD = $env:WINDOWS_SIGN_CERT_PASSWORD
  WINDOWS_SIGN_TIMESTAMP_URL = $env:WINDOWS_SIGN_TIMESTAMP_URL
}
$azureSigningVars = @{
  AZURE_ARTIFACT_SIGNING_ENDPOINT = $env:AZURE_ARTIFACT_SIGNING_ENDPOINT
  AZURE_ARTIFACT_SIGNING_ACCOUNT = $env:AZURE_ARTIFACT_SIGNING_ACCOUNT
  AZURE_ARTIFACT_SIGNING_CERT_PROFILE = $env:AZURE_ARTIFACT_SIGNING_CERT_PROFILE
}
$configuredPfxSigningVars = Get-ConfiguredSigningVarNames -Vars $pfxSigningVars
$configuredAzureSigningVars = Get-ConfiguredSigningVarNames -Vars $azureSigningVars
Assert-CompleteSigningVarSet -Name "PFX" -Vars $pfxSigningVars -ConfiguredVars $configuredPfxSigningVars
Assert-CompleteSigningVarSet -Name "Azure Artifact Signing" -Vars $azureSigningVars -ConfiguredVars $configuredAzureSigningVars

$windowsSigningEnabled = (
  ($configuredPfxSigningVars.Count -eq $pfxSigningVars.Count) -or
  ($configuredAzureSigningVars.Count -eq $azureSigningVars.Count)
)

$isccArgs = @(
  "/DAppVersion=$version"
  "/DOutputBaseFilename=$outBase"
)

if ($windowsSigningEnabled) {
  $signScript = (Resolve-Path ".\\scripts\\sign_windows.ps1").Path
  $signToolCommand = 'powershell.exe -NoProfile -ExecutionPolicy Bypass -File $q' + $signScript + '$q -Files $q$f$q'
  $isccArgs += "/DWindowsSigningEnabled=1"
  $isccArgs += "/Salbis_sign=$signToolCommand"
  Write-Host "Windows signing enabled for setup and generated uninstaller."
}

$isccArgs += ".\\scripts\\installer_windows.iss"
& $iscc.Path @isccArgs
Write-Host ("Output: dist\\" + $outBase + ".exe")
