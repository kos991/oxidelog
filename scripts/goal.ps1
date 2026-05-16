$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

function Stop-Fwlogd {
    param([System.Diagnostics.Process] $Process)
    if ($null -ne $Process -and -not $Process.HasExited) {
        Stop-Process -Id $Process.Id -Force
        $Process.WaitForExit(5000) | Out-Null
    }
}

if (-not (Get-Command cargo -ErrorAction SilentlyContinue)) {
    throw "cargo not found in PATH"
}

if (Test-Path data) {
    Remove-Item -LiteralPath data -Recurse -Force
}
New-Item -ItemType Directory -Force -Path data\spool, data\duckdb, data\export, data\parquet, data\frozen | Out-Null

cargo test --workspace
cargo build --workspace

$exe = Join-Path $repoRoot "target\debug\fwlogd.exe"
if (-not (Test-Path $exe)) {
    $exe = Join-Path $repoRoot "target\debug\fwlogd"
}

$stdoutPath = Join-Path $repoRoot "data\fwlogd.out.log"
$stderrPath = Join-Path $repoRoot "data\fwlogd.err.log"
$proc = $null

try {
    $proc = Start-Process -FilePath $exe `
        -ArgumentList @("--config", "config/local.toml") `
        -WorkingDirectory $repoRoot `
        -RedirectStandardOutput $stdoutPath `
        -RedirectStandardError $stderrPath `
        -WindowStyle Hidden `
        -PassThru

    $healthy = $false
    for ($i = 0; $i -lt 30; $i++) {
        try {
            $health = Invoke-RestMethod -Uri "http://127.0.0.1:18080/api/health" -TimeoutSec 1
            if ($health.status -eq "ok") {
                $healthy = $true
                break
            }
        } catch {
            Start-Sleep -Seconds 1
        }
    }

    if (-not $healthy) {
        throw "fwlogd did not become healthy within 30 seconds. See $stdoutPath and $stderrPath"
    }

    $client = [System.Net.Sockets.TcpClient]::new("127.0.0.1", 1514)
    try {
        $stream = $client.GetStream()
        $writer = [System.IO.StreamWriter]::new($stream)
        foreach ($line in Get-Content samples\sangfor.log) {
            $writer.WriteLine($line)
        }
        $writer.Flush()
    } finally {
        $client.Close()
    }

    Start-Sleep -Seconds 2

    $events = Invoke-RestMethod -Uri "http://127.0.0.1:18080/api/events?limit=20"
    $status = Invoke-RestMethod -Uri "http://127.0.0.1:18080/api/system/status"
    $csv = Invoke-WebRequest -Uri "http://127.0.0.1:18080/api/events/export.csv?limit=20"
    Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:18080/api/archive/parquet?limit=20" | Out-Null
    $archiveFiles = Invoke-RestMethod -Uri "http://127.0.0.1:18080/api/archive/files"
    $frozen = Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:18080/api/archive/frozen?limit=20"
    $frozenFiles = Invoke-RestMethod -Uri "http://127.0.0.1:18080/api/archive/frozen"
    $restorePath = [System.Uri]::EscapeDataString($frozen.path)
    $restored = Invoke-RestMethod -Uri "http://127.0.0.1:18080/api/archive/frozen/restore?path=$restorePath"
    $exportPath = Join-Path $repoRoot "data\export\events.csv"
    Set-Content -Path $exportPath -Value $csv.Content -Encoding UTF8

    $ingested = @($events).Count
    $parsed = @($events | Where-Object { $_.parse_status -eq "parsed" }).Count
    $failed = @($events | Where-Object { $_.parse_status -eq "failed" }).Count
    $archiveCount = @($archiveFiles).Count
    $frozenCount = @($frozenFiles).Count
    $restoredCount = @($restored).Count

    if ($ingested -lt 5 -or $parsed -lt 4 -or $failed -lt 1) {
        throw "unexpected goal counts: ingested=$ingested parsed=$parsed failed=$failed"
    }
    if ($status.service -ne "fwlogd") {
        throw "unexpected system status service: $($status.service)"
    }
    if ($status.events_total -lt $ingested -or $status.events_parsed -lt $parsed -or $status.events_failed -lt $failed) {
        throw "unexpected system status event counts: total=$($status.events_total) parsed=$($status.events_parsed) failed=$($status.events_failed)"
    }
    if ($archiveCount -lt 1) {
        throw "expected at least one archive file, got $archiveCount"
    }
    if ($frozenCount -lt 1) {
        throw "expected at least one frozen archive file, got $frozenCount"
    }
    if ($restoredCount -lt 5) {
        throw "expected at least five restored frozen lines, got $restoredCount"
    }

    Write-Host "OxideLog V3 local goal passed"
    Write-Host "API: http://127.0.0.1:18080"
    Write-Host "Ingested: $ingested"
    Write-Host "Parsed: $parsed"
    Write-Host "Failed: $failed"
    Write-Host "Export: data/export/events.csv"
    Write-Host "Archives: $archiveCount"
    Write-Host "Frozen archives: $frozenCount"
    Write-Host "Restored frozen lines: $restoredCount"
} finally {
    Stop-Fwlogd -Process $proc
}
