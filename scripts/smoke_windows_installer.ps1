param(
  [Parameter(Mandatory = $true)]
  [string]$InstallerPath,

  [double]$StartupTimeout = 60.0
)

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

$resolvedInstaller = (Resolve-Path $InstallerPath).Path
$installDir = Join-Path $env:RUNNER_TEMP "albis-installer-smoke"
if (Test-Path $installDir) {
  Remove-Item -Recurse -Force $installDir
}
New-Item -ItemType Directory -Force -Path $installDir | Out-Null

$installerLog = Join-Path $env:RUNNER_TEMP "albis-installer-smoke.log"
& $resolvedInstaller "/VERYSILENT" "/SUPPRESSMSGBOXES" "/NORESTART" "/DIR=$installDir" "/LOG=$installerLog"
if ($LASTEXITCODE -ne 0) {
  throw "Installer exited with code $LASTEXITCODE"
}

$exePath = Join-Path $installDir "ALBIS.exe"
if (-not (Test-Path $exePath)) {
  throw "Installed executable not found: $exePath"
}

python .\scripts\smoke_packaged_binary.py --binary $exePath --startup-timeout $StartupTimeout

$uninstaller = Get-ChildItem $installDir -File -Filter "unins*.exe" | Select-Object -First 1
if (-not $uninstaller) {
  throw "Uninstaller not found in $installDir"
}

& $uninstaller.FullName "/VERYSILENT" "/SUPPRESSMSGBOXES" "/NORESTART"
if ($LASTEXITCODE -ne 0) {
  throw "Uninstaller exited with code $LASTEXITCODE"
}

for ($attempt = 0; $attempt -lt 20; $attempt++) {
  if (-not (Test-Path $exePath)) {
    break
  }
  Start-Sleep -Milliseconds 250
}

if (Test-Path $exePath) {
  throw "Installed executable still present after uninstall: $exePath"
}
