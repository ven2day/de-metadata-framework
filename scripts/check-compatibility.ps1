[CmdletBinding()]
param([switch]$SkipBuild)

$ErrorActionPreference = 'Stop'
$projectDirectory = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
Set-Location -LiteralPath $projectDirectory

function Invoke-Docker {
    param([string[]]$DockerArguments)
    & docker @DockerArguments
    if ($LASTEXITCODE -ne 0) {
        throw "Compatibility check failed (exit $LASTEXITCODE). Test volumes were retained for diagnosis."
    }
}

$compatArguments = @(
    'compose', '-p', 'de-compat', '--env-file', '.env.example',
    '-f', 'docker-compose.yml', '-f', 'tests/docker-compose.compat.yml'
)
Invoke-Docker -DockerArguments @('info', '--format', 'Docker engine: {{.ServerVersion}}')
Invoke-Docker -DockerArguments ($compatArguments + @('config', '--quiet'))
if (-not $SkipBuild) {
    Invoke-Docker -DockerArguments ($compatArguments + @('build', 'minio', 'minio-init', 'hive-metastore', 'pipeline'))
}
Invoke-Docker -DockerArguments ($compatArguments + @(
    'up', '-d', '--no-build', '--wait', '--wait-timeout', '600',
    'minio', 'minio-init', 'postgres-meta', 'hive-metastore', 'vault', 'vault-init',
    'postgres-app', 'trino', 'spark-sql', 'ui'
))
Invoke-Docker -DockerArguments ($compatArguments + @(
    'run', '--rm', '--no-deps', '--entrypoint', 'spark-submit',
    'pipeline', '/checks/smoke_layers.py'
))
Invoke-Docker -DockerArguments ($compatArguments + @(
    'run', '--rm', '--no-deps', '--entrypoint', 'python',
    'pipeline', '/checks/smoke_ui.py', '--base-url', 'http://ui:5000'
))
Invoke-Docker -DockerArguments ($compatArguments + @(
    'run', '--rm', '--no-deps', '--entrypoint', 'python',
    'pipeline', '/checks/smoke_dhan_ui.py', '--base-url', 'http://ui:5000'
))
Invoke-Docker -DockerArguments ($compatArguments + @(
    'cp', 'tests/smoke_scheduler.py', 'ui:/tmp/check_local_gold_scheduler.py'
))
Invoke-Docker -DockerArguments ($compatArguments + @(
    'exec', '-T', 'ui', 'python', '/tmp/check_local_gold_scheduler.py'
))
Write-Host 'The synthetic-data local layer check passed. Real Dhan authentication still requires your account settings.'
Write-Host 'Test containers and small unique test tables remain in project de-compat for inspection.'
