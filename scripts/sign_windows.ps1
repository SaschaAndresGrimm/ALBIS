param(
  [Parameter(Mandatory = $true)]
  [string[]]$Files
)

$ErrorActionPreference = "Stop"

function Get-ConfiguredSigningVarNames {
  param(
    [Parameter(Mandatory = $true)]
    [hashtable]$Vars
  )

  return @(
    $Vars.GetEnumerator() |
      Where-Object { -not [string]::IsNullOrWhiteSpace($_.Value) } |
      ForEach-Object { $_.Key }
  )
}

function Assert-CompleteSigningVarSet {
  param(
    [Parameter(Mandatory = $true)]
    [string]$Name,

    [Parameter(Mandatory = $true)]
    [hashtable]$Vars,

    [Parameter(Mandatory = $true)]
    [string[]]$ConfiguredVars
  )

  if ($ConfiguredVars.Count -gt 0 -and $ConfiguredVars.Count -lt $Vars.Count) {
    $missing = @(
      $Vars.Keys |
        Where-Object { [string]::IsNullOrWhiteSpace($Vars[$_]) } |
        Sort-Object
    )
    throw "[sign_windows] $Name signing is partially configured. Missing: $($missing -join ', ')"
  }
}

function Get-SignToolPath {
  if (-not [string]::IsNullOrWhiteSpace($env:WINDOWS_SIGNTOOL_PATH)) {
    if (Test-Path -Path $env:WINDOWS_SIGNTOOL_PATH -PathType Leaf) {
      return (Resolve-Path $env:WINDOWS_SIGNTOOL_PATH).Path
    }
    throw "[sign_windows] WINDOWS_SIGNTOOL_PATH is set but does not point to a file: $env:WINDOWS_SIGNTOOL_PATH"
  }

  $signtool = Get-Command signtool.exe -ErrorAction SilentlyContinue
  if ($signtool) {
    return $signtool.Path
  }

  $programFilesX86 = [Environment]::GetFolderPath("ProgramFilesX86")
  if (-not [string]::IsNullOrWhiteSpace($programFilesX86)) {
    $kitsBin = Join-Path $programFilesX86 "Windows Kits\10\bin"
    if (Test-Path $kitsBin) {
      $sdkDirs = Get-ChildItem -Path $kitsBin -Directory -ErrorAction SilentlyContinue |
        Sort-Object -Property @{
          Expression = {
            try {
              [version]$_.Name
            } catch {
              [version]"0.0"
            }
          }
          Descending = $true
        }

      foreach ($sdkDir in $sdkDirs) {
        foreach ($arch in @("x64", "x86")) {
          $candidate = Join-Path $sdkDir.FullName "$arch\signtool.exe"
          if (Test-Path -Path $candidate -PathType Leaf) {
            return $candidate
          }
        }
      }
    }
  }

  throw "[sign_windows] signtool.exe not found. Install the Windows SDK or set WINDOWS_SIGNTOOL_PATH."
}

function Get-AzureArtifactSigningDlibPath {
  if (-not [string]::IsNullOrWhiteSpace($env:AZURE_ARTIFACT_SIGNING_DLIB_PATH)) {
    if (Test-Path -Path $env:AZURE_ARTIFACT_SIGNING_DLIB_PATH -PathType Leaf) {
      return (Resolve-Path $env:AZURE_ARTIFACT_SIGNING_DLIB_PATH).Path
    }
    throw "[sign_windows] AZURE_ARTIFACT_SIGNING_DLIB_PATH is set but does not point to a file: $env:AZURE_ARTIFACT_SIGNING_DLIB_PATH"
  }

  $candidateRoots = @()
  if (-not [string]::IsNullOrWhiteSpace($env:RUNNER_TEMP)) {
    $candidateRoots += (Join-Path $env:RUNNER_TEMP "azure-artifact-signing")
  }
  $localAppData = [Environment]::GetFolderPath("LocalApplicationData")
  if (-not [string]::IsNullOrWhiteSpace($localAppData)) {
    $candidateRoots += (Join-Path $localAppData "Microsoft\MicrosoftArtifactSigningClientTools")
  }
  $programFiles = [Environment]::GetFolderPath("ProgramFiles")
  if (-not [string]::IsNullOrWhiteSpace($programFiles)) {
    $candidateRoots += (Join-Path $programFiles "Microsoft\MicrosoftArtifactSigningClientTools")
  }
  $programFilesX86 = [Environment]::GetFolderPath("ProgramFilesX86")
  if (-not [string]::IsNullOrWhiteSpace($programFilesX86)) {
    $candidateRoots += (Join-Path $programFilesX86 "Microsoft\MicrosoftArtifactSigningClientTools")
  }

  foreach ($root in $candidateRoots) {
    if (-not (Test-Path $root)) {
      continue
    }

    $candidates = @(
      Get-ChildItem -Path $root -Filter "Azure.CodeSigning.Dlib.dll" -Recurse -File -ErrorAction SilentlyContinue
    )
    $preferredCandidate = $candidates |
      Where-Object { $_.FullName -match "\\x64\\Azure\.CodeSigning\.Dlib\.dll$" } |
      Select-Object -First 1
    if ($preferredCandidate) {
      return $preferredCandidate.FullName
    }
    if ($candidates.Count -gt 0) {
      return $candidates[0].FullName
    }
  }

  throw "[sign_windows] Azure Artifact Signing dlib not found. Run scripts\setup_azure_artifact_signing.ps1 or set AZURE_ARTIFACT_SIGNING_DLIB_PATH."
}

function New-AzureArtifactSigningMetadataFile {
  param(
    [Parameter(Mandatory = $true)]
    [hashtable]$AzureVars
  )

  $metadata = [ordered]@{
    Endpoint = $AzureVars["AZURE_ARTIFACT_SIGNING_ENDPOINT"]
    CodeSigningAccountName = $AzureVars["AZURE_ARTIFACT_SIGNING_ACCOUNT"]
    CertificateProfileName = $AzureVars["AZURE_ARTIFACT_SIGNING_CERT_PROFILE"]
  }

  $correlationId = $env:AZURE_ARTIFACT_SIGNING_CORRELATION_ID
  if ([string]::IsNullOrWhiteSpace($correlationId) -and -not [string]::IsNullOrWhiteSpace($env:GITHUB_RUN_ID)) {
    $attempt = if ($env:GITHUB_RUN_ATTEMPT) { $env:GITHUB_RUN_ATTEMPT } else { "1" }
    $correlationId = "github-$($env:GITHUB_RUN_ID)-$attempt"
  }
  if (-not [string]::IsNullOrWhiteSpace($correlationId)) {
    $metadata.CorrelationId = $correlationId
  }

  $tmp = [System.IO.Path]::GetTempFileName()
  $metadataPath = [System.IO.Path]::ChangeExtension($tmp, ".json")
  Move-Item -Path $tmp -Destination $metadataPath -Force
  $metadata | ConvertTo-Json -Depth 4 | Set-Content -Path $metadataPath -Encoding UTF8
  return $metadataPath
}

function Invoke-SignToolAndVerify {
  param(
    [Parameter(Mandatory = $true)]
    [string]$SignToolPath,

    [Parameter(Mandatory = $true)]
    [string[]]$SignArguments,

    [Parameter(Mandatory = $true)]
    [string]$File
  )

  if (-not (Test-Path $File)) {
    throw "[sign_windows] File not found: $File"
  }

  Write-Host "[sign_windows] Signing $File"
  & $SignToolPath @SignArguments $File
  if ($LASTEXITCODE -ne 0) {
    throw "[sign_windows] signtool sign failed for $File"
  }

  & $SignToolPath verify /pa $File
  if ($LASTEXITCODE -ne 0) {
    throw "[sign_windows] signtool verify failed for $File"
  }
}

$pfxSigningVars = @{
  WINDOWS_SIGN_CERT_B64 = $env:WINDOWS_SIGN_CERT_B64
  WINDOWS_SIGN_CERT_PASSWORD = $env:WINDOWS_SIGN_CERT_PASSWORD
  WINDOWS_SIGN_TIMESTAMP_URL = $env:WINDOWS_SIGN_TIMESTAMP_URL
}
$azureSigningVars = @{
  AZURE_ARTIFACT_SIGNING_ENDPOINT = $env:AZURE_ARTIFACT_SIGNING_ENDPOINT
  AZURE_ARTIFACT_SIGNING_ACCOUNT = $env:AZURE_ARTIFACT_SIGNING_ACCOUNT
  AZURE_ARTIFACT_SIGNING_CERT_PROFILE = $env:AZURE_ARTIFACT_SIGNING_CERT_PROFILE
}

$configuredPfxSigningVars = Get-ConfiguredSigningVarNames -Vars $pfxSigningVars
$configuredAzureSigningVars = Get-ConfiguredSigningVarNames -Vars $azureSigningVars
Assert-CompleteSigningVarSet -Name "PFX" -Vars $pfxSigningVars -ConfiguredVars $configuredPfxSigningVars
Assert-CompleteSigningVarSet -Name "Azure Artifact Signing" -Vars $azureSigningVars -ConfiguredVars $configuredAzureSigningVars

$useAzureSigning = $configuredAzureSigningVars.Count -eq $azureSigningVars.Count
$usePfxSigning = $configuredPfxSigningVars.Count -eq $pfxSigningVars.Count

if (-not $useAzureSigning -and -not $usePfxSigning) {
  Write-Host "[sign_windows] Windows signing variables not set; skipping signing."
  exit 0
}

$signtoolPath = Get-SignToolPath
Write-Host "[sign_windows] Using SignTool at $signtoolPath"

if ($useAzureSigning) {
  $dlibPath = Get-AzureArtifactSigningDlibPath
  $timestampUrl = if ($env:AZURE_ARTIFACT_SIGNING_TIMESTAMP_URL) {
    $env:AZURE_ARTIFACT_SIGNING_TIMESTAMP_URL
  } else {
    "http://timestamp.acs.microsoft.com"
  }
  Write-Host "[sign_windows] Using Azure Artifact Signing dlib at $dlibPath"
  $metadataPath = New-AzureArtifactSigningMetadataFile -AzureVars $azureSigningVars

  try {
    $signArguments = @(
      "sign"
      "/v"
      "/debug"
      "/fd"
      "SHA256"
      "/tr"
      $timestampUrl
      "/td"
      "SHA256"
      "/dlib"
      $dlibPath
      "/dmdf"
      $metadataPath
    )
    foreach ($file in $Files) {
      Invoke-SignToolAndVerify -SignToolPath $signtoolPath -SignArguments $signArguments -File $file
    }
  } finally {
    Remove-Item -Path $metadataPath -Force -ErrorAction SilentlyContinue
  }
} else {
  $certBase64 = $pfxSigningVars["WINDOWS_SIGN_CERT_B64"]
  $password = $pfxSigningVars["WINDOWS_SIGN_CERT_PASSWORD"]
  $timestampUrl = $pfxSigningVars["WINDOWS_SIGN_TIMESTAMP_URL"]

  $tmp = [System.IO.Path]::GetTempFileName()
  $pfxPath = [System.IO.Path]::ChangeExtension($tmp, ".pfx")
  Move-Item -Path $tmp -Destination $pfxPath -Force

  try {
    [System.IO.File]::WriteAllBytes($pfxPath, [System.Convert]::FromBase64String($certBase64))

    $signArguments = @(
      "sign"
      "/fd"
      "SHA256"
      "/tr"
      $timestampUrl
      "/td"
      "SHA256"
      "/f"
      $pfxPath
      "/p"
      $password
      "/a"
    )
    foreach ($file in $Files) {
      Invoke-SignToolAndVerify -SignToolPath $signtoolPath -SignArguments $signArguments -File $file
    }
  } finally {
    Remove-Item -Path $pfxPath -Force -ErrorAction SilentlyContinue
  }
}

Write-Host "[sign_windows] Signing completed."
