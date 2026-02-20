$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

$versionInfo = python .\scripts\version_info.py --json | ConvertFrom-Json
$tag = $versionInfo.tag

try {
  $osVersion = (Get-CimInstance Win32_OperatingSystem -ErrorAction Stop).Version
} catch {
  $osVersion = [System.Environment]::OSVersion.Version.ToString()
}
$osTag = "windows-" + (($osVersion -replace '[^0-9\.]', '') -replace '\.', '_')

python -m pip install --upgrade pyinstaller

# Prefer curated ALBIS icon assets when available.
$assetIcon = Join-Path $root "albis_assets\\albis_256x256.png"
$fallbackIcon = Join-Path $root "frontend\\ressources\\icon.png"
if (Test-Path $assetIcon) {
  $env:ALBIS_ICON = $assetIcon
} elseif (Test-Path $fallbackIcon) {
  $env:ALBIS_ICON = $fallbackIcon
}

# Non-interactive build: never prompt to remove existing output directories.
python -m PyInstaller --noconfirm --clean ALBIS.spec

$zip = Join-Path $root ("dist\\ALBIS-" + $osTag + "-" + $tag + ".zip")
if (Test-Path $zip) {
  Remove-Item $zip
}
Compress-Archive -Path (Join-Path $root "dist\\ALBIS") -DestinationPath $zip -Force
Write-Host ("Output: dist\\ALBIS and " + $zip)
