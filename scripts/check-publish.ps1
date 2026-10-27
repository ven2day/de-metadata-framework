[CmdletBinding()]
param(
    [switch]$Staged,
    [switch]$AllowMissingEnv
)

# This gate never stages files, changes Git configuration, or contacts a broker.
# The only permitted download is the pinned scanner image, when it is missing.
$ErrorActionPreference = 'Stop'
$publishRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$scannerImage = 'ghcr.io/gitleaks/gitleaks:v8.30.1@sha256:c00b6bd0aeb3071cbcb79009cb16a60dd9e0a7c60e2be9ab65d25e6bc8abbb7f'
$snapshotRoot = $null
$snapshotOwned = $false
$snapshotLeaf = 'de-publish-' + [Guid]::NewGuid().ToString('N')
$temporaryRoot = [IO.Path]::GetFullPath([IO.Path]::GetTempPath()).TrimEnd([IO.Path]::DirectorySeparatorChar)
$pathComparison = if ([IO.Path]::DirectorySeparatorChar -eq '\') { [StringComparison]::OrdinalIgnoreCase } else { [StringComparison]::Ordinal }
$publishPhase = 'initialization'
$publishExit = 1
$strictUtf8 = New-Object Text.UTF8Encoding($false, $true)

function ConvertTo-NativeArgument {
    param([string]$Value)
    if ($Value -notmatch '[\s"]' -and $Value.Length -gt 0) { return $Value }
    $escaped = [regex]::Replace($Value, '(\\*)"', '$1$1\"')
    $escaped = [regex]::Replace($escaped, '(\\+)$', '$1$1')
    return '"' + $escaped + '"'
}

function Invoke-CapturedProcess {
    param([string]$Executable, [string[]]$Arguments)
    $startInfo = New-Object Diagnostics.ProcessStartInfo
    $startInfo.FileName = $Executable
    $startInfo.WorkingDirectory = $publishRoot
    $startInfo.UseShellExecute = $false
    $startInfo.CreateNoWindow = $true
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    if ($null -ne $startInfo.GetType().GetProperty('ArgumentList')) {
        foreach ($argument in $Arguments) { $startInfo.ArgumentList.Add($argument) }
    } else {
        $startInfo.Arguments = (($Arguments | ForEach-Object { ConvertTo-NativeArgument $_ }) -join ' ')
    }
    $process = New-Object Diagnostics.Process
    $process.StartInfo = $startInfo
    $outputBuffer = New-Object IO.MemoryStream
    try {
        [void]$process.Start()
        $errorTask = $process.StandardError.ReadToEndAsync()
        $process.StandardOutput.BaseStream.CopyTo($outputBuffer)
        $process.WaitForExit()
        # Captured stderr is inspected only for an absent image; never display it.
        return [pscustomobject]@{
            ExitCode = $process.ExitCode
            OutputBytes = $outputBuffer.ToArray()
            ErrorText = $errorTask.GetAwaiter().GetResult()
        }
    } finally {
        $outputBuffer.Dispose()
        $process.Dispose()
    }
}

function ConvertFrom-CapturedUtf8 {
    param($Result)
    return $strictUtf8.GetString($Result.OutputBytes)
}

function Invoke-GitRead {
    param([string[]]$GitArguments)
    $result = Invoke-CapturedProcess $gitExecutable (@('-C', $publishRoot) + $GitArguments)
    if ($result.ExitCode -ne 0) { throw 'A read-only Git command failed.' }
    return $result
}

function Write-FindingMetadata {
    param($Findings, [switch]$NodeFormat)
    $safeFindings = @()
    foreach ($finding in @($Findings)) {
        if ($null -eq $finding) { throw 'Invalid scanner finding.' }
        $file = $finding.file
        $rule = if ($NodeFormat) { $finding.category } else { $finding.rule }
        $commit = if ($null -eq $finding.commit) { '' } else { $finding.commit }
        $line = $finding.line
        if ($file -isnot [string] -or !$file -or $file -match '[\x00-\x1F\x7F]') { throw 'Invalid scanner filename.' }
        if ($rule -isnot [string] -or $rule -notmatch '^[A-Za-z0-9_:.-]+$') { throw 'Invalid scanner rule.' }
        if ($commit -isnot [string] -or ($commit -and $commit -notmatch '^[a-fA-F0-9]{40,64}$')) { throw 'Invalid scanner commit.' }
        if ($null -ne $line -and ([string]$line -notmatch '^[1-9][0-9]*$')) { throw 'Invalid scanner line.' }
        $safeFindings += [pscustomobject][ordered]@{ file = $file; line = $line; rule = $rule; commit = $commit }
    }
    # Validate the complete response before printing any of it. Never forward raw output.
    foreach ($finding in $safeFindings) { Write-Output ($finding | ConvertTo-Json -Compress) }
}

function Assert-SafeRelativePath {
    param([string]$RelativePath)
    if (!$RelativePath -or [IO.Path]::IsPathRooted($RelativePath) -or $RelativePath -match '[\x00-\x1F\x7F\\:<>"|?*]') {
        throw 'Unsupported Git filename.'
    }
    foreach ($part in $RelativePath.Split('/')) {
        if (!$part -or $part -eq '.' -or $part -eq '..' -or $part -ieq '.git' -or $part.EndsWith('.') -or $part.EndsWith(' ')) {
            throw 'Unsafe Git filename.'
        }
    }
}

function Get-ContainedPath {
    param([string]$BasePath, [string]$RelativePath)
    Assert-SafeRelativePath $RelativePath
    $fullPath = [IO.Path]::GetFullPath((Join-Path $BasePath $RelativePath))
    $prefix = $BasePath.TrimEnd([IO.Path]::DirectorySeparatorChar) + [IO.Path]::DirectorySeparatorChar
    if (!$fullPath.StartsWith($prefix, $pathComparison)) { throw 'Git filename escaped its allowed directory.' }
    return $fullPath
}

function Assert-RegularPath {
    param([string]$BasePath, [string]$RelativePath, [switch]$Directory)
    $current = $BasePath
    $rootItem = Get-Item -LiteralPath $BasePath -Force
    if (($rootItem.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) { throw 'Symbolic links are not supported.' }
    foreach ($part in $RelativePath.Split('/')) {
        $current = Join-Path $current $part
        $item = Get-Item -LiteralPath $current -Force
        if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) { throw 'Symbolic links are not supported.' }
    }
    if ($Directory) {
        if (!$item.PSIsContainer) { throw 'Expected a regular directory.' }
    } elseif ($item.PSIsContainer -or $item -isnot [IO.FileInfo]) {
        throw 'A publishing candidate is not a regular file.'
    }
}

function Get-SnapshotDestination {
    param([string]$RelativePath)
    $destination = Get-ContainedPath $snapshotRoot $RelativePath
    $parent = [IO.Path]::GetDirectoryName($destination)
    [void][IO.Directory]::CreateDirectory($parent)
    $parentRelative = [IO.Path]::GetDirectoryName($RelativePath)
    if ($parentRelative) { Assert-RegularPath $snapshotRoot ($parentRelative.Replace('\', '/')) -Directory }
    if (Test-Path -LiteralPath $destination) { throw 'Colliding publishing candidate filenames.' }
    return $destination
}

function New-PublishSnapshot {
    $names = New-Object 'Collections.Generic.HashSet[string]' ([StringComparer]::OrdinalIgnoreCase)
    if ($Staged) {
        # Read index blobs directly: no checkout filters, CRLF conversion, hooks,
        # index updates, credentials, or external smudge/LFS commands can run.
        $indexResult = Invoke-GitRead @('ls-files', '--stage', '-z')
        $entries = (ConvertFrom-CapturedUtf8 $indexResult).Split([char]0)
        foreach ($entry in $entries) {
            if (!$entry) { continue }
            if ($entry -notmatch '^([0-9]{6}) ([a-fA-F0-9]{40,64}) ([0-3])\t([^\x00]+)$') { throw 'Unsupported Git index entry.' }
            $mode = $Matches[1]
            $objectId = $Matches[2]
            $stage = $Matches[3]
            $relativePath = $Matches[4]
            if ($stage -ne '0' -or $mode -notin @('100644', '100755')) { throw 'Unmerged, symlink, or submodule index entries are unsupported.' }
            Assert-SafeRelativePath $relativePath
            if (!$names.Add($relativePath)) { throw 'Colliding Git index filenames.' }
            $destination = Get-SnapshotDestination $relativePath
            $blob = Invoke-GitRead @('cat-file', 'blob', $objectId)
            [IO.File]::WriteAllBytes($destination, $blob.OutputBytes)
        }
    } else {
        $indexResult = Invoke-GitRead @('ls-files', '--stage', '-z')
        foreach ($entry in (ConvertFrom-CapturedUtf8 $indexResult).Split([char]0)) {
            if (!$entry) { continue }
            if ($entry -notmatch '^([0-9]{6}) ([a-fA-F0-9]{40,64}) ([0-3])\t([^\x00]+)$' -or
                $Matches[3] -ne '0' -or $Matches[1] -notin @('100644', '100755')) {
                throw 'Unmerged, symlink, or submodule index entries are unsupported.'
            }
        }
        $fileResult = Invoke-GitRead @('ls-files', '--cached', '--others', '--exclude-standard', '-z')
        $relativePaths = (ConvertFrom-CapturedUtf8 $fileResult).Split([char]0)
        foreach ($relativePath in $relativePaths) {
            if (!$relativePath) { continue }
            Assert-SafeRelativePath $relativePath
            if (!$names.Add($relativePath)) { throw 'Colliding publishing candidate filenames.' }
            $source = Get-ContainedPath $publishRoot $relativePath
            Assert-RegularPath $publishRoot $relativePath
            $destination = Get-SnapshotDestination $relativePath
            Copy-Item -LiteralPath $source -Destination $destination -ErrorAction Stop
        }
    }
}

function Invoke-GitleaksScan {
    param([switch]$History)
    $dockerArguments = @(
        'run', '--rm', '--pull=never', '--network', 'none', '--read-only',
        '--mount', ('type=bind,source=' + $publishRoot + ',target=/repo,readonly'),
        '--env', 'GIT_CONFIG_COUNT=1',
        '--env', 'GIT_CONFIG_KEY_0=safe.directory',
        '--env', 'GIT_CONFIG_VALUE_0=/repo'
    )
    if (!$History) { $dockerArguments += @('--mount', ('type=bind,source=' + $snapshotRoot + ',target=/snapshot,readonly')) }
    $dockerArguments += $scannerImage
    if ($History) { $dockerArguments += @('git', '/repo', '--log-opts=--all') }
    else { $dockerArguments += @('dir', '/snapshot') }
    $dockerArguments += @(
        '--config=/repo/.gitleaks.toml', '--redact=100', '--no-banner', '--no-color', '--log-level=error',
        '--report-format=template', '--report-template=/repo/scripts/security/gitleaks-summary.tmpl',
        # Gitleaks treats '-' as stdout. /dev/stdout is unsafe here: its
        # report-path preflight can unlink/recreate it as an ordinary file.
        '--report-path=-', '--ignore-gitleaks-allow'
    )
    $diagnostic = [ordered]@{
        check = if ($History) { 'history' } else { 'snapshot' }
        code = 'scanner_start_failed'
        exit_code = $null
        report_byte_count = 0
        report_shape = 'not_received'
        finding_count = $null
        stderr_category = 'none'
    }
    try {
        $result = Invoke-CapturedProcess $dockerExecutable $dockerArguments
        $diagnostic.exit_code = $result.ExitCode
        $diagnostic.report_byte_count = $result.OutputBytes.Length
        # Classify only known operational failures. Never print native stderr.
        if ($result.ErrorText) {
            $diagnostic.stderr_category = switch -Regex ($result.ErrorText) {
                '(?i)invalid mount|mounts denied|not shared|bind source path|file sharing|permission denied' { 'mount_or_permission'; break }
                '(?i)cannot connect|daemon is not running|error during connect' { 'docker_unavailable'; break }
                '(?i)unknown flag|unknown command' { 'unsupported_arguments'; break }
                '(?i)read-only file system' { 'read_only_filesystem'; break }
                '(?i)template:' { 'template_error'; break }
                '(?i)failed to load config|failed to parse config|toml' { 'configuration_error'; break }
                default { 'unclassified_suppressed' }
            }
        }
        $diagnostic.code = 'report_decode_failed'
        $reportText = (ConvertFrom-CapturedUtf8 $result).Trim()
        $diagnostic.report_shape = if (!$reportText) { 'empty' } elseif ($reportText.StartsWith('[')) { 'array' } elseif ($reportText.StartsWith('{')) { 'object' } else { 'other' }
        if ($reportText) {
            $diagnostic.code = 'report_shape_invalid'
            if (!$reportText.StartsWith('[') -or !$reportText.EndsWith(']')) { throw 'Unexpected scanner report format.' }
            $diagnostic.code = 'report_json_invalid'
            # Assignment first also flattens JSON arrays correctly in Windows PowerShell 5.1.
            $parsedFindings = $reportText | ConvertFrom-Json -ErrorAction Stop
            $findings = @($parsedFindings)
            $diagnostic.finding_count = $findings.Count
            $diagnostic.code = 'finding_metadata_invalid'
            Write-FindingMetadata $findings
            if ($findings.Count -gt 0) {
                $diagnostic.code = 'findings_block_publish'
                throw 'Secret scanner findings block publishing.'
            }
        } else {
            $diagnostic.finding_count = 0
        }
        $diagnostic.code = 'scanner_exit_nonzero'
        if ($result.ExitCode -ne 0) { throw 'Secret scanner failed or found a secret.' }
    } catch {
        [Console]::Error.WriteLine(('Scanner diagnostic: ' + ($diagnostic | ConvertTo-Json -Compress)))
        throw 'Secret scanner did not complete cleanly.'
    }
}

try {
    $nodeExecutable = (Get-Command node -CommandType Application -ErrorAction Stop | Select-Object -First 1).Source
    $gitExecutable = (Get-Command git -CommandType Application -ErrorAction Stop | Select-Object -First 1).Source
    $publishPhase = 'local credential and history checks'
    $nodeArguments = @((Join-Path $PSScriptRoot 'check-local-secrets.js'), '--history')
    if ($Staged) { $nodeArguments += '--staged' }
    if ($AllowMissingEnv) { $nodeArguments += '--allow-missing-env' }
    $localResult = Invoke-CapturedProcess $nodeExecutable $nodeArguments
    $localReport = ConvertFrom-CapturedUtf8 $localResult | ConvertFrom-Json -ErrorAction Stop
    if ($localReport.ok -isnot [bool] -or $localReport.credential_source_present -isnot [bool]) {
        throw 'Invalid local credential check response.'
    }
    if ($null -ne $localReport.findings) { Write-FindingMetadata $localReport.findings -NodeFormat }
    if ($localResult.ExitCode -ne 0 -or !$localReport.ok -or @($localReport.findings).Count -gt 0) {
        throw 'Local credential checks block publishing.'
    }
    if ($localReport.credential_source_present -eq $false) {
        if (!$AllowMissingEnv) { throw 'Local credential source is required.' }
        Write-Output 'Local credential source unavailable: exact-value comparison was not performed. Pattern and history scans are still required and will run.'
    }

    $publishPhase = 'repository and scanner validation'
    $rootResult = Invoke-GitRead @('rev-parse', '--show-toplevel')
    $reportedRoot = [IO.Path]::GetFullPath((ConvertFrom-CapturedUtf8 $rootResult).TrimEnd([char]10, [char]13))
    if (!$reportedRoot.Equals($publishRoot, $pathComparison)) { throw 'Unexpected repository root.' }
    if ($publishRoot -match '[,\x00-\x1F\x7F]') { throw 'Repository path is unsupported by safe Docker mounts.' }
    Assert-RegularPath $publishRoot '.gitleaks.toml'
    Assert-RegularPath $publishRoot 'scripts/security/gitleaks-summary.tmpl'
    $dockerExecutable = (Get-Command docker -CommandType Application -ErrorAction Stop | Select-Object -First 1).Source
    $imageResult = Invoke-CapturedProcess $dockerExecutable @('image', 'inspect', $scannerImage)
    if ($imageResult.ExitCode -ne 0) {
        if ($imageResult.ErrorText -notmatch '(?i)no such image|image[^\r\n]*not found') { throw 'Docker image inspection failed.' }
        $publishPhase = 'pinned scanner image download'
        $pullResult = Invoke-CapturedProcess $dockerExecutable @('pull', $scannerImage)
        if ($pullResult.ExitCode -ne 0) { throw 'Pinned scanner image download failed.' }
    }

    $publishPhase = 'all-reference Git history scan'
    Invoke-GitleaksScan -History

    $publishPhase = if ($Staged) { 'staged index snapshot' } else { 'publishable worktree snapshot' }
    $snapshotRoot = [IO.Path]::GetFullPath((Join-Path $temporaryRoot $snapshotLeaf))
    if ($snapshotRoot -match '[,\x00-\x1F\x7F]' -or (Test-Path -LiteralPath $snapshotRoot)) { throw 'Unsafe temporary snapshot location.' }
    [void](New-Item -ItemType Directory -Path $snapshotRoot -ErrorAction Stop)
    $snapshotOwned = $true
    New-PublishSnapshot
    $publishPhase = 'publishable file secret scan'
    Invoke-GitleaksScan
    $publishExit = 0
} catch {
    # Never print exception messages, native stderr, matched lines, or raw reports.
    [Console]::Error.WriteLine(('Publishing check failed during {0}. Review redacted metadata above and check the required tools/configuration.' -f $publishPhase))
} finally {
    if ($snapshotOwned -and $snapshotRoot -and (Test-Path -LiteralPath $snapshotRoot)) {
        try {
            $cleanupPath = [IO.Path]::GetFullPath($snapshotRoot)
            $cleanupItem = Get-Item -LiteralPath $cleanupPath -Force
            if (![IO.Path]::GetDirectoryName($cleanupPath).Equals($temporaryRoot, $pathComparison) -or
                [IO.Path]::GetFileName($cleanupPath) -ne $snapshotLeaf -or !$snapshotLeaf.StartsWith('de-publish-') -or
                ($cleanupItem.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
                throw 'Temporary directory ownership check failed.'
            }
            Remove-Item -LiteralPath $cleanupPath -Recurse -Force -ErrorAction Stop
        } catch {
            $publishExit = 1
            [Console]::Error.WriteLine('Publishing check failed: its temporary snapshot could not be safely removed. No other directory was deleted.')
        }
    }
}

if ($publishExit -eq 0) { Write-Output 'Publishing checks passed. This does not certify GitHub permissions, dependency safety, or production readiness.' }
exit $publishExit
