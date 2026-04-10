param(
  [Parameter(Mandatory = $true)]
  [string]$InstallerPath,

  [double]$StartupTimeout = 60.0
)

$ErrorActionPreference = "Stop"

function Get-TempRoot {
  if (-not [string]::IsNullOrWhiteSpace($env:RUNNER_TEMP)) {
    return $env:RUNNER_TEMP
  }
  return [System.IO.Path]::GetTempPath()
}

function Invoke-LoggedProcess {
  param(
    [Parameter(Mandatory = $true)]
    [string]$FilePath,

    [Parameter(Mandatory = $true)]
    [string[]]$ArgumentList,

    [Parameter(Mandatory = $true)]
    [string]$Label,

    [string]$LogPath
  )

  if ($LogPath -and (Test-Path $LogPath)) {
    Remove-Item -Force $LogPath
  }

  $process = Start-Process -FilePath $FilePath -ArgumentList $ArgumentList -Wait -PassThru
  if ($process.ExitCode -eq 0) {
    return
  }

  if ($LogPath -and (Test-Path $LogPath)) {
    Write-Host "$Label log:"
    Get-Content $LogPath
  }
  throw "$Label exited with code $($process.ExitCode)"
}

function Get-FreeTcpPort {
  $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, 0)
  $listener.Start()
  try {
    return ([System.Net.IPEndPoint]$listener.LocalEndpoint).Port
  }
  finally {
    $listener.Stop()
  }
}

function Wait-ForHealth {
  param(
    [Parameter(Mandatory = $true)]
    [int]$Port,

    [Parameter(Mandatory = $true)]
    [double]$TimeoutSec
  )

  $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSec)
  $uri = "http://127.0.0.1:$Port/api/health"
  while ([DateTime]::UtcNow -lt $deadline) {
    try {
      $response = Invoke-WebRequest -Uri $uri -TimeoutSec 2
      if ($response.StatusCode -eq 200) {
        return
      }
    }
    catch {
    }
    Start-Sleep -Milliseconds 250
  }
  throw "Timed out waiting for ALBIS health endpoint at $uri"
}

function Wait-ForProcessExit {
  param(
    [Parameter(Mandatory = $true)]
    [System.Diagnostics.Process]$Process,

    [Parameter(Mandatory = $true)]
    [double]$TimeoutSec,

    [Parameter(Mandatory = $true)]
    [string]$Label
  )

  if ($Process.HasExited) {
    return
  }
  if (-not $Process.WaitForExit([int]([Math]::Ceiling($TimeoutSec * 1000.0)))) {
    throw "$Label did not exit within $TimeoutSec seconds"
  }
}

function Start-AlbisBackgroundProcess {
  param(
    [Parameter(Mandatory = $true)]
    [string]$ExePath,

    [Parameter(Mandatory = $true)]
    [string]$ProfileDir,

    [Parameter(Mandatory = $true)]
    [double]$TimeoutSec
  )

  $configDir = Join-Path $ProfileDir ".config\albis"
  $logDir = Join-Path $configDir "logs"
  $dataDir = Join-Path $ProfileDir "data"
  New-Item -ItemType Directory -Force -Path $configDir | Out-Null
  New-Item -ItemType Directory -Force -Path $logDir | Out-Null
  New-Item -ItemType Directory -Force -Path $dataDir | Out-Null

  $port = Get-FreeTcpPort
  $configPath = Join-Path $configDir "config.json"
  $config = @{
    server = @{
      host = "127.0.0.1"
      port = $port
      reload = $false
    }
    launcher = @{
      startup_timeout_sec = 2.0
      startup_health_timeout_sec = [Math]::Max(5.0, [Math]::Min($TimeoutSec, 15.0))
      open_browser = $false
      debug_macos_events = $false
    }
    data = @{
      root = $dataDir
      allow_abs_paths = $true
    }
    logging = @{
      level = "INFO"
      dir = $logDir
    }
  }
  $config | ConvertTo-Json -Depth 6 | Set-Content -Path $configPath -Encoding UTF8

  $psi = [System.Diagnostics.ProcessStartInfo]::new($ExePath)
  $psi.WorkingDirectory = $ProfileDir
  $psi.UseShellExecute = $false
  $psi.CreateNoWindow = $true
  $psi.Environment["HOME"] = $ProfileDir
  $psi.Environment["USERPROFILE"] = $ProfileDir

  $process = [System.Diagnostics.Process]::Start($psi)
  if (-not $process) {
    throw "Failed to start ALBIS from $ExePath"
  }

  Wait-ForHealth -Port $port -TimeoutSec $TimeoutSec
  return @{
    Process = $process
    Port = $port
  }
}

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

$resolvedInstaller = (Resolve-Path $InstallerPath).Path
$tempRoot = Get-TempRoot
$installDir = Join-Path $tempRoot "albis-installer-smoke"
if (Test-Path $installDir) {
  Remove-Item -Recurse -Force $installDir
}
New-Item -ItemType Directory -Force -Path $installDir | Out-Null

$installerLog = Join-Path $tempRoot "albis-installer-smoke.log"
$installerArgs = @(
  "/VERYSILENT"
  "/SUPPRESSMSGBOXES"
  "/NORESTART"
  "/DIR=$installDir"
  "/LOG=$installerLog"
)
Invoke-LoggedProcess -FilePath $resolvedInstaller -ArgumentList $installerArgs -Label "Installer" -LogPath $installerLog

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

$runtimeProfile = Join-Path $tempRoot ("albis-installer-running-" + [Guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Force -Path $runtimeProfile | Out-Null
$runningState = $null

try {
  $runningState = Start-AlbisBackgroundProcess -ExePath $exePath -ProfileDir $runtimeProfile -TimeoutSec $StartupTimeout

  $upgradeLog = Join-Path $tempRoot "albis-installer-upgrade.log"
  $upgradeArgs = @(
    "/VERYSILENT"
    "/SUPPRESSMSGBOXES"
    "/NORESTART"
    "/DIR=$installDir"
    "/LOG=$upgradeLog"
  )
  Invoke-LoggedProcess -FilePath $resolvedInstaller -ArgumentList $upgradeArgs -Label "Upgrade installer" -LogPath $upgradeLog
  Wait-ForProcessExit -Process $runningState.Process -TimeoutSec 20 -Label "ALBIS after installer-driven shutdown"

  if (-not (Test-Path $exePath)) {
    throw "Installed executable missing after reinstall: $exePath"
  }

  $runningState = Start-AlbisBackgroundProcess -ExePath $exePath -ProfileDir $runtimeProfile -TimeoutSec $StartupTimeout

  $uninstaller = Get-ChildItem $installDir -File -Filter "unins*.exe" | Select-Object -First 1
  if (-not $uninstaller) {
    throw "Uninstaller not found in $installDir after reinstall"
  }

  $uninstallLog = Join-Path $tempRoot "albis-installer-uninstall.log"
  $uninstallArgs = @(
    "/VERYSILENT"
    "/SUPPRESSMSGBOXES"
    "/NORESTART"
    "/LOG=$uninstallLog"
  )
  Invoke-LoggedProcess -FilePath $uninstaller.FullName -ArgumentList $uninstallArgs -Label "Uninstaller" -LogPath $uninstallLog
  Wait-ForProcessExit -Process $runningState.Process -TimeoutSec 20 -Label "ALBIS after uninstall-driven shutdown"
}
finally {
  if ($runningState -and $runningState.Process -and -not $runningState.Process.HasExited) {
    $runningState.Process.Kill()
    $null = $runningState.Process.WaitForExit(5000)
  }
  if (Test-Path $runtimeProfile) {
    Remove-Item -Recurse -Force $runtimeProfile
  }
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
