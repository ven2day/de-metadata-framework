[CmdletBinding()]
param(
    [switch]$PrepareOnly,
    [switch]$SkipBuild
)

$ErrorActionPreference = 'Stop'
$projectDirectory = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
Set-Location -LiteralPath $projectDirectory

function Invoke-Docker {
    param([string[]]$DockerArguments)
    & docker @DockerArguments
    if ($LASTEXITCODE -ne 0) {
        throw "Docker command failed (exit $LASTEXITCODE). Check the error above."
    }
}

function New-LocalSecret {
    $secretBytes = New-Object byte[] 36
    $generator = [Security.Cryptography.RandomNumberGenerator]::Create()
    try { $generator.GetBytes($secretBytes) }
    finally { $generator.Dispose() }
    return [Convert]::ToBase64String($secretBytes)
}

$environmentPath = Join-Path $projectDirectory '.env'
if (-not (Test-Path -LiteralPath $environmentPath)) {
    $templatePath = Join-Path $projectDirectory '.env.example'
    $environmentText = [IO.File]::ReadAllText($templatePath)
    $generatedValues = @{
        FLASK_SECRET_KEY = New-LocalSecret
        ROOT_PASSWORD = 'Aa9!' + (New-LocalSecret)
        AUTH_DB_PASSWORD = New-LocalSecret
        MINIO_SECRET_KEY = New-LocalSecret
        SALT_KEY = New-LocalSecret
        SALT_2 = New-LocalSecret
    }
    foreach ($setting in $generatedValues.Keys) {
        $pattern = '(?m)^' + [regex]::Escape($setting) + '=.*$'
        $environmentText = [regex]::Replace(
            $environmentText, $pattern, $setting + '=' + $generatedValues[$setting]
        )
    }
    [IO.File]::WriteAllText($environmentPath, $environmentText, (New-Object Text.UTF8Encoding($false)))
    Write-Host 'Created .env with unique local passwords and signing/encryption secrets.'
    Write-Host 'Your root password is stored in ROOT_PASSWORD in .env; it is not printed here.'
}

$environmentText = [IO.File]::ReadAllText($environmentPath)
$unconfigured = @(
    [regex]::Matches($environmentText, '(?m)^([A-Z][A-Z0-9_]*)=REQUIRED[^\r\n]*') |
        ForEach-Object { $_.Groups[1].Value }
)
if ($unconfigured.Count -gt 0) {
    throw ('Replace placeholder values in .env before starting: ' + ($unconfigured -join ', '))
}

if ($PrepareOnly) {
    Write-Host 'Configuration prepared. Add Dhan settings to .env, then run this script again.'
    exit 0
}

Invoke-Docker -DockerArguments @('info', '--format', 'Docker engine: {{.ServerVersion}}')
Invoke-Docker -DockerArguments @('compose', 'config', '--quiet')
if (-not $SkipBuild) {
    Write-Host 'Building storage, Hive, and shared application images. First build needs Internet access.'
    Invoke-Docker -DockerArguments @('compose', 'build', 'minio', 'minio-init', 'hive-metastore', 'pipeline')
}

Write-Host 'Starting the local services and waiting for health checks...'
Invoke-Docker -DockerArguments @(
    'compose', 'up', '-d', '--no-build', '--wait', '--wait-timeout', '600',
    'minio', 'minio-init', 'postgres-meta', 'hive-metastore',
    'vault', 'vault-init', 'postgres-app', 'trino', 'spark-sql', 'ui'
)
Invoke-Docker -DockerArguments @('compose', 'ps', '--all')
Write-Host ''
Write-Host 'Application: http://localhost:5001'
Write-Host 'Users page:  http://localhost:5001/users'
Write-Host 'Sign in as root with ROOT_PASSWORD from .env.'
Write-Host 'Dhan setup: docs/DHAN_NSE.md (credentials stay on your machine).'
