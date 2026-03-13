$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

$bootstrapPython = if ($env:PYTHON_BIN) { $env:PYTHON_BIN } else { "python" }
$isolatedBuild = if ($env:ALBIS_BUILD_ISOLATED -eq "0") { $false } else { $true }
$buildVenv = if ($env:ALBIS_BUILD_VENV) { $env:ALBIS_BUILD_VENV } else { Join-Path $root ".build-venv-windows" }
$pythonExe = $bootstrapPython
$pyInstallerVersion = if ($env:ALBIS_PYINSTALLER_VERSION) { $env:ALBIS_PYINSTALLER_VERSION } else { "6.19.0" }

if ($isolatedBuild) {
  $cleanVenv = if ($env:ALBIS_BUILD_CLEAN_VENV -eq "0") { $false } else { $true }
  if ($cleanVenv -and (Test-Path $buildVenv)) {
    Remove-Item -Recurse -Force $buildVenv
  }
  $venvPython = Join-Path $buildVenv "Scripts\\python.exe"
  if (-not (Test-Path $venvPython)) {
    & $bootstrapPython -m venv $buildVenv
  }
  $pythonExe = $venvPython
}

$versionInfo = & $pythonExe .\scripts\version_info.py --json | ConvertFrom-Json
$tag = $versionInfo.tag

& $pythonExe -m pip install --upgrade pip
if ($isolatedBuild) {
  & $pythonExe -m pip install -r .\backend\requirements.txt
}
& $pythonExe -m pip install --upgrade ("pyinstaller==" + $pyInstallerVersion)

# Prefer curated ALBIS icon assets when available.
$distDir = Join-Path $root "dist"
New-Item -ItemType Directory -Force -Path $distDir | Out-Null
$generatedIcon = Join-Path $distDir "ALBIS.ico"
$assetIcon = Join-Path $root "albis_assets\\icon.ico"
$fallbackIcon = Join-Path $root "frontend\\ressources\\icon.ico"
$iconGenerator = Join-Path $root "scripts\\generate_windows_icon.py"

if (Test-Path $iconGenerator) {
  & $pythonExe $iconGenerator --output $generatedIcon
}

if (Test-Path $generatedIcon) {
  $env:ALBIS_ICON = $generatedIcon
} elseif (Test-Path $assetIcon) {
  $env:ALBIS_ICON = $assetIcon
} elseif (Test-Path $fallbackIcon) {
  $env:ALBIS_ICON = $fallbackIcon
}

# Non-interactive build: never prompt to remove existing output directories.
& $pythonExe -m PyInstaller --noconfirm --clean ALBIS.spec

$zip = Join-Path $root ("dist\\ALBIS-" + $versionInfo.target + "-" + $tag + ".zip")
if (Test-Path $zip) {
  Remove-Item $zip
}
Compress-Archive -Path (Join-Path $root "dist\\ALBIS") -DestinationPath $zip -Force
Write-Host ("Output: dist\\ALBIS and " + $zip)
