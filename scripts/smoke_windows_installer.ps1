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
$installerArgs = @(
  "/VERYSILENT"
  "/SUPPRESSMSGBOXES"
  "/NORESTART"
  "/DIR=$installDir"
  "/LOG=$installerLog"
)
$installerProcess = Start-Process -FilePath $resolvedInstaller -ArgumentList $installerArgs -Wait -PassThru
if ($installerProcess.ExitCode -ne 0) {
  if (Test-Path $installerLog) {
    Write-Host "Installer log:"
    Get-Content $installerLog
  }
  throw "Installer exited with code $($installerProcess.ExitCode)"
}

$exePath = Join-Path $installDir "ALBIS.exe"
if (-not (Test-Path $exePath)) {
  if (Test-Path $installerLog) {
    Write-Host "Installer log:"
    Get-Content $installerLog
  }
  throw "Installed executable not found: $exePath"
}

python .\scripts\smoke_packaged_binary.py --binary $exePath --startup-timeout $StartupTimeout

$uninstaller = Get-ChildItem $installDir -File -Filter "unins*.exe" | Select-Object -First 1
if (-not $uninstaller) {
  throw "Uninstaller not found in $installDir"
}

if (-not [string]::IsNullOrWhiteSpace($env:WINDOWS_SIGN_CERT_B64)) {
  $uninstallerSig = Get-AuthenticodeSignature $uninstaller.FullName
  Write-Host "Uninstaller signature status: $($uninstallerSig.Status)"
  if ($uninstallerSig.Status -ne "Valid") {
    throw "Expected signed uninstaller, got status $($uninstallerSig.Status)"
  }
}

$uninstallProcess = Start-Process -FilePath $uninstaller.FullName -ArgumentList @(
  "/VERYSILENT"
  "/SUPPRESSMSGBOXES"
  "/NORESTART"
) -Wait -PassThru
if ($uninstallProcess.ExitCode -ne 0) {
  throw "Uninstaller exited with code $($uninstallProcess.ExitCode)"
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
