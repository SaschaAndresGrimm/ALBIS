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

    [Parameter(Mandatory = $true)]
    [string[]]$ConfiguredVars
  )

  if ($ConfiguredVars.Count -gt 0 -and $ConfiguredVars.Count -lt $Vars.Count) {
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
