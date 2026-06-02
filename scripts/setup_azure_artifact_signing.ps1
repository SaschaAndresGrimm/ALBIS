$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"

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

$azureSigningVars = @{
  AZURE_ARTIFACT_SIGNING_ENDPOINT = $env:AZURE_ARTIFACT_SIGNING_ENDPOINT
  AZURE_ARTIFACT_SIGNING_ACCOUNT = $env:AZURE_ARTIFACT_SIGNING_ACCOUNT
  AZURE_ARTIFACT_SIGNING_CERT_PROFILE = $env:AZURE_ARTIFACT_SIGNING_CERT_PROFILE
}
$configuredAzureSigningVars = Get-ConfiguredSigningVarNames -Vars $azureSigningVars
if ($configuredAzureSigningVars.Count -eq 0) {
  Write-Host "[setup_azure_artifact_signing] Azure Artifact Signing variables not set; skipping setup."
  exit 0
}
if ($configuredAzureSigningVars.Count -lt $azureSigningVars.Count) {
  $missing = @(
    $azureSigningVars.Keys |
      Where-Object { [string]::IsNullOrWhiteSpace($azureSigningVars[$_]) } |
      Sort-Object
  )
  throw "[setup_azure_artifact_signing] Azure Artifact Signing is partially configured. Missing: $($missing -join ', ')"
}

$toolsDir = if ($env:AZURE_ARTIFACT_SIGNING_TOOLS_DIR) {
  $env:AZURE_ARTIFACT_SIGNING_TOOLS_DIR
} elseif ($env:RUNNER_TEMP) {
  Join-Path $env:RUNNER_TEMP "azure-artifact-signing"
} else {
  Join-Path ([System.IO.Path]::GetTempPath()) "azure-artifact-signing"
}
New-Item -ItemType Directory -Force -Path $toolsDir | Out-Null

$nuget = Join-Path $toolsDir "nuget.exe"
if (-not (Test-Path $nuget)) {
  Write-Host "[setup_azure_artifact_signing] Downloading nuget.exe"
  Invoke-WebRequest -Uri "https://dist.nuget.org/win-x86-commandline/latest/nuget.exe" -OutFile $nuget
}

$nugetSource = "https://api.nuget.org/v3/index.json"
Write-Host "[setup_azure_artifact_signing] Installing Windows SDK Build Tools"
& $nuget install Microsoft.Windows.SDK.BuildTools -OutputDirectory $toolsDir -ExcludeVersion -NonInteractive -Source $nugetSource
if ($LASTEXITCODE -ne 0) {
  throw "[setup_azure_artifact_signing] Failed to install Microsoft.Windows.SDK.BuildTools"
}

Write-Host "[setup_azure_artifact_signing] Installing Azure Artifact Signing client"
& $nuget install Microsoft.ArtifactSigning.Client -OutputDirectory $toolsDir -ExcludeVersion -NonInteractive -Source $nugetSource
if ($LASTEXITCODE -ne 0) {
  throw "[setup_azure_artifact_signing] Failed to install Microsoft.ArtifactSigning.Client"
}

$signtool = Get-ChildItem -Path (Join-Path $toolsDir "Microsoft.Windows.SDK.BuildTools") -Filter "signtool.exe" -Recurse -File |
  Where-Object { $_.FullName -match "\\x64\\signtool\.exe$" } |
  Sort-Object -Property FullName -Descending |
  Select-Object -First 1
if (-not $signtool) {
  throw "[setup_azure_artifact_signing] Could not find x64 signtool.exe in Microsoft.Windows.SDK.BuildTools."
}

$dlib = Get-ChildItem -Path (Join-Path $toolsDir "Microsoft.ArtifactSigning.Client") -Filter "Azure.CodeSigning.Dlib.dll" -Recurse -File |
  Where-Object { $_.FullName -match "\\x64\\Azure\.CodeSigning\.Dlib\.dll$" } |
  Select-Object -First 1
if (-not $dlib) {
  throw "[setup_azure_artifact_signing] Could not find x64 Azure.CodeSigning.Dlib.dll in Microsoft.ArtifactSigning.Client."
}

Write-Host "[setup_azure_artifact_signing] SignTool: $($signtool.FullName)"
Write-Host "[setup_azure_artifact_signing] Azure Artifact Signing dlib: $($dlib.FullName)"

$env:WINDOWS_SIGNTOOL_PATH = $signtool.FullName
$env:AZURE_ARTIFACT_SIGNING_DLIB_PATH = $dlib.FullName

if ($env:GITHUB_ENV) {
  Add-Content -Path $env:GITHUB_ENV -Value "WINDOWS_SIGNTOOL_PATH=$($signtool.FullName)"
  Add-Content -Path $env:GITHUB_ENV -Value "AZURE_ARTIFACT_SIGNING_DLIB_PATH=$($dlib.FullName)"
}

$dotnetRuntimes = @()
try {
  $dotnetRuntimes = & dotnet --list-runtimes
} catch {
}
if (-not ($dotnetRuntimes | Select-String -Pattern "^Microsoft\.NETCore\.App 8\." -Quiet)) {
  Write-Warning "[setup_azure_artifact_signing] .NET 8 runtime was not detected. GitHub windows-latest normally includes it; self-hosted runners must install it for Azure Artifact Signing."
}
