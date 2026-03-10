param(
  [Parameter(Mandatory = $true)]
  [string[]]$Files
)

$ErrorActionPreference = "Stop"

$certBase64 = $env:WINDOWS_SIGN_CERT_B64
if ([string]::IsNullOrWhiteSpace($certBase64)) {
  Write-Host "[sign_windows] WINDOWS_SIGN_CERT_B64 not set; skipping signing."
  exit 0
}

$password = $env:WINDOWS_SIGN_CERT_PASSWORD
if ([string]::IsNullOrWhiteSpace($password)) {
  throw "[sign_windows] WINDOWS_SIGN_CERT_PASSWORD is required when WINDOWS_SIGN_CERT_B64 is set."
}

$timestampUrl = $env:WINDOWS_SIGN_TIMESTAMP_URL
if ([string]::IsNullOrWhiteSpace($timestampUrl)) {
  throw "[sign_windows] WINDOWS_SIGN_TIMESTAMP_URL is required when WINDOWS_SIGN_CERT_B64 is set."
}

$signtool = Get-Command signtool.exe -ErrorAction SilentlyContinue
if (-not $signtool) {
  throw "[sign_windows] signtool.exe not found on PATH."
}

$tmp = [System.IO.Path]::GetTempFileName()
$pfxPath = [System.IO.Path]::ChangeExtension($tmp, ".pfx")
Move-Item -Path $tmp -Destination $pfxPath -Force

try {
  [System.IO.File]::WriteAllBytes($pfxPath, [System.Convert]::FromBase64String($certBase64))

  foreach ($file in $Files) {
    if (-not (Test-Path $file)) {
      throw "[sign_windows] File not found: $file"
    }

    Write-Host "[sign_windows] Signing $file"
    & $signtool.Path sign /fd SHA256 /tr $timestampUrl /td SHA256 /f $pfxPath /p $password /a $file
    if ($LASTEXITCODE -ne 0) {
      throw "[sign_windows] signtool sign failed for $file"
    }

    & $signtool.Path verify /pa $file
    if ($LASTEXITCODE -ne 0) {
      throw "[sign_windows] signtool verify failed for $file"
    }
  }

  Write-Host "[sign_windows] Signing completed."
} finally {
  Remove-Item -Path $pfxPath -Force -ErrorAction SilentlyContinue
}
