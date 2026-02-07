$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

python -m pip install --upgrade pyinstaller
python -m PyInstaller ALBIS.spec

$zip = Join-Path $root "dist\\ALBIS-win.zip"
if (Test-Path $zip) {
  Remove-Item $zip
}
Compress-Archive -Path (Join-Path $root "dist\\ALBIS") -DestinationPath $zip -Force
Write-Host "Output: dist\\ALBIS and dist\\ALBIS-win.zip"
