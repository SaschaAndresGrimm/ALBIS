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
python -m PyInstaller ALBIS.spec

$zip = Join-Path $root ("dist\\ALBIS-" + $osTag + "-" + $tag + ".zip")
if (Test-Path $zip) {
  Remove-Item $zip
}
Compress-Archive -Path (Join-Path $root "dist\\ALBIS") -DestinationPath $zip -Force
Write-Host ("Output: dist\\ALBIS and " + $zip)
