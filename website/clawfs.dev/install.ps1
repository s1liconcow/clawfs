#Requires -Version 5.1
<#
.SYNOPSIS
    Install the public ClawFS CLI for Windows.
.DESCRIPTION
    Downloads the latest (or specified) ClawFS Windows release and installs
    clawfs.exe to a local directory.
.EXAMPLE
    iwr https://clawfs.dev/install.ps1 -UseBasicParsing | iex
.EXAMPLE
    $env:CLAWFS_INSTALL_VERSION = "v0.3.0"; iwr https://clawfs.dev/install.ps1 -UseBasicParsing | iex
#>

$ErrorActionPreference = "Stop"

$Repo    = if ($env:CLAWFS_RELEASE_REPO)   { $env:CLAWFS_RELEASE_REPO }   else { "s1liconcow/clawfs" }
$Version = if ($env:CLAWFS_INSTALL_VERSION) { $env:CLAWFS_INSTALL_VERSION } else { "latest" }
$InstallDir = if ($env:CLAWFS_INSTALL_DIR)  { $env:CLAWFS_INSTALL_DIR }    else { Join-Path $env:LOCALAPPDATA "ClawFS\bin" }

# Resolve latest version tag
if ($Version -eq "latest") {
    $ApiUrl = "https://api.github.com/repos/$Repo/releases/latest"
    try {
        $Release = Invoke-RestMethod -Uri $ApiUrl -UseBasicParsing
        $Version = $Release.tag_name
    } catch {
        Write-Error "Failed to resolve latest ClawFS release tag from $ApiUrl"
        exit 1
    }
}

if (-not $Version) {
    Write-Error "Could not determine ClawFS version to install."
    exit 1
}

$ZipName = "clawfs-$Version-windows-x86_64.zip"
$Url = "https://github.com/$Repo/releases/download/$Version/$ZipName"

Write-Host "Installing ClawFS $Version from $Url"

# Download to temp
$TmpDir = Join-Path ([System.IO.Path]::GetTempPath()) ("clawfs-install-" + [guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $TmpDir -Force | Out-Null

try {
    $ZipPath = Join-Path $TmpDir $ZipName
    Invoke-WebRequest -Uri $Url -OutFile $ZipPath -UseBasicParsing

    # Extract
    Expand-Archive -Path $ZipPath -DestinationPath $TmpDir -Force

    # Install
    New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
    Copy-Item (Join-Path $TmpDir "clawfs.exe") $InstallDir -Force

    Write-Host ""
    Write-Host "Installed ClawFS to $InstallDir"
    Write-Host ""

    # Check if install dir is in PATH
    $UserPath = [Environment]::GetEnvironmentVariable("Path", "User")
    if ($UserPath -notlike "*$InstallDir*") {
        Write-Host "Adding $InstallDir to your user PATH..."
        [Environment]::SetEnvironmentVariable("Path", "$InstallDir;$UserPath", "User")
        $env:Path = "$InstallDir;$env:Path"
        Write-Host "Done. Restart your terminal for PATH changes to take effect."
    }

    Write-Host ""
    Write-Host "Next steps:"
    Write-Host "  clawfs --help"
    Write-Host "  clawfs --mount-path C:\clawfs-mnt --store-path C:\clawfs-store"
} finally {
    Remove-Item -Recurse -Force $TmpDir -ErrorAction SilentlyContinue
}
