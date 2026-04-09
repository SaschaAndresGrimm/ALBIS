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

$signingVars = @{
  WINDOWS_SIGN_CERT_B64 = $env:WINDOWS_SIGN_CERT_B64
  WINDOWS_SIGN_CERT_PASSWORD = $env:WINDOWS_SIGN_CERT_PASSWORD
  WINDOWS_SIGN_TIMESTAMP_URL = $env:WINDOWS_SIGN_TIMESTAMP_URL
}
$configuredSigningVars = @(
  $signingVars.GetEnumerator() |
    Where-Object { -not [string]::IsNullOrWhiteSpace($_.Value) } |
    ForEach-Object { $_.Key }
)
if ($configuredSigningVars.Count -gt 0 -and $configuredSigningVars.Count -lt $signingVars.Count) {
  throw "Windows signing is partially configured. Set WINDOWS_SIGN_CERT_B64, WINDOWS_SIGN_CERT_PASSWORD, and WINDOWS_SIGN_TIMESTAMP_URL together."
}

$isccArgs = @(
  "/DAppVersion=$version"
  "/DOutputBaseFilename=$outBase"
)

if ($configuredSigningVars.Count -eq $signingVars.Count) {
  $signScript = (Resolve-Path ".\\scripts\\sign_windows.ps1").Path
  $signToolCommand = 'powershell.exe -NoProfile -ExecutionPolicy Bypass -File $q' + $signScript + '$q -Files $f'
  $isccArgs += "/DWindowsSigningEnabled=1"
  $isccArgs += "/Salbis_sign=$signToolCommand"
  Write-Host "Windows signing enabled for setup and generated uninstaller."
}

$isccArgs += ".\\scripts\\installer_windows.iss"
& $iscc.Path @isccArgs
Write-Host ("Output: dist\\" + $outBase + ".exe")
