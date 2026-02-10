$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

$versionInfo = python .\scripts\version_info.py --json | ConvertFrom-Json
$version = $versionInfo.version
$tag = $versionInfo.tag
$outBase = "ALBIS-Setup-" + $tag

if (-not (Test-Path ".\\dist\\ALBIS")) {
  Write-Host "Missing dist\\ALBIS. Run .\\scripts\\build_windows.ps1 first."
  exit 1
}

$iscc = Get-Command iscc -ErrorAction SilentlyContinue
if (-not $iscc) {
  Write-Host "Inno Setup (ISCC) not found. Install it, then rerun."
  exit 1
}

iscc "/DAppVersion=$version" "/DOutputBaseFilename=$outBase" ".\\scripts\\installer_windows.iss"
Write-Host ("Output: dist\\" + $outBase + ".exe")
