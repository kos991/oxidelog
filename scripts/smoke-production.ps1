param(
    [string] $ApiHost = "192.168.0.142",
    [int] $ApiPort = 18080,
    [string] $TcpHost = "192.168.0.142",
    [int] $TcpPort = 1514,
    [int] $Limit = 50,
    [int] $WaitSeconds = 3,
    [string] $OutputRoot = "",
    [string] $ApiToken = $env:OXIDELOG_API_TOKEN,
    [switch] $NoIngest
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
if ([string]::IsNullOrWhiteSpace($OutputRoot)) {
    $OutputRoot = Join-Path $repoRoot "smoke-production-output"
}

$stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$outputDir = Join-Path $OutputRoot $stamp
New-Item -ItemType Directory -Force -Path $outputDir | Out-Null

$baseUrl = "http://${ApiHost}:$ApiPort"
$ingestEnabled = -not $NoIngest.IsPresent
$headers = @{}
if (-not [string]::IsNullOrWhiteSpace($ApiToken)) {
    $headers["Authorization"] = "Bearer $ApiToken"
}

function Write-Step {
    param([string] $Message)
    Write-Host "[smoke] $Message"
}

function Invoke-SmokeJson {
    param(
        [string] $Path,
        [string] $OutFile,
        [string] $Method = "GET"
    )

    $uri = "$baseUrl$Path"
    Write-Step "$Method $uri"
    $response = Invoke-RestMethod -Method $Method -Uri $uri -Headers $headers -TimeoutSec 15
    ConvertTo-Json -InputObject $response -Depth 20 | Set-Content -Path $OutFile -Encoding UTF8
    return $response
}

function Assert-Condition {
    param(
        [bool] $Condition,
        [string] $Message
    )

    if (-not $Condition) {
        throw $Message
    }
}

function Get-SampleLines {
    $samplePath = Join-Path $repoRoot "samples\sangfor.log"
    if (Test-Path $samplePath) {
        return @(Get-Content -Path $samplePath | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    }

    $now = Get-Date -Format "yyyy-MM-ddTHH:mm:ssK"
    return @(
        "<134>1 $now smoke-production oxidelog - - - allow src=10.10.0.1 dst=10.10.0.2 action=allow",
        "<134>1 $now smoke-production oxidelog - - - deny src=10.10.0.3 dst=10.10.0.4 action=deny",
        "<134>1 $now smoke-production oxidelog - - - alert src=10.10.0.5 dst=10.10.0.6 action=alert",
        "<134>1 $now smoke-production oxidelog - - - pass src=10.10.0.7 dst=10.10.0.8 action=pass",
        "smoke-production malformed raw line"
    )
}

function Send-TcpLines {
    param([string[]] $Lines)

    Write-Step "TCP ingest $($Lines.Count) lines to ${TcpHost}:$TcpPort"
    $client = [System.Net.Sockets.TcpClient]::new()
    $client.Connect($TcpHost, $TcpPort)
    try {
        $stream = $client.GetStream()
        $writer = [System.IO.StreamWriter]::new($stream)
        foreach ($line in $Lines) {
            $writer.WriteLine($line)
        }
        $writer.Flush()
    } finally {
        $client.Close()
    }
}

Write-Step "artifact directory: $outputDir"

$health = Invoke-SmokeJson -Path "/api/health" -OutFile (Join-Path $outputDir "health.json")
Assert-Condition ($health.status -eq "ok") "unexpected health status: $($health.status)"

$status = Invoke-SmokeJson -Path "/api/system/status" -OutFile (Join-Path $outputDir "system-status.json")
Assert-Condition ($status.service -eq "fwlogd") "unexpected system status service: $($status.service)"

$sampleCount = 0
if ($ingestEnabled) {
    $sampleLines = Get-SampleLines
    $sampleCount = $sampleLines.Count
    Assert-Condition ($sampleCount -gt 0) "no sample log lines available"
    Set-Content -Path (Join-Path $outputDir "ingested-sample.log") -Value $sampleLines -Encoding UTF8
    Send-TcpLines -Lines $sampleLines
    Start-Sleep -Seconds $WaitSeconds
} else {
    Write-Step "TCP ingest skipped"
}

$events = Invoke-SmokeJson -Path "/api/events?limit=$Limit" -OutFile (Join-Path $outputDir "events.json")
$eventCount = @($events).Count
Assert-Condition ($eventCount -gt 0) "expected at least one event from /api/events"
if ($ingestEnabled) {
    Assert-Condition ($eventCount -ge [Math]::Min($sampleCount, $Limit)) "expected events after ingest, got $eventCount for $sampleCount sample lines"
}

$csvPath = Join-Path $outputDir "events.csv"
Write-Step "GET $baseUrl/api/events/export.csv?limit=$Limit"
$csv = Invoke-WebRequest -Uri "$baseUrl/api/events/export.csv?limit=$Limit" -Headers $headers -TimeoutSec 15
Set-Content -Path $csvPath -Value $csv.Content -Encoding UTF8
Assert-Condition ($csv.Content -match "event_id|raw|parse_status") "CSV export did not include expected event columns"

$parquet = Invoke-SmokeJson -Method "POST" -Path "/api/archive/parquet?limit=$Limit" -OutFile (Join-Path $outputDir "parquet-created.json")
Assert-Condition (($parquet.path -as [string]).EndsWith(".parquet")) "parquet archive path was not a .parquet file"

$parquetFiles = Invoke-SmokeJson -Path "/api/archive/files" -OutFile (Join-Path $outputDir "parquet-files.json")
$parquetCount = @($parquetFiles).Count
Assert-Condition ($parquetCount -gt 0) "expected at least one parquet archive file"

$frozen = Invoke-SmokeJson -Method "POST" -Path "/api/archive/frozen?limit=$Limit" -OutFile (Join-Path $outputDir "frozen-created.json")
Assert-Condition (($frozen.path -as [string]).EndsWith(".raw.zst")) "frozen archive path was not a .raw.zst file"

$frozenFiles = Invoke-SmokeJson -Path "/api/archive/frozen" -OutFile (Join-Path $outputDir "frozen-files.json")
$frozenCount = @($frozenFiles).Count
Assert-Condition ($frozenCount -gt 0) "expected at least one frozen archive file"

$restorePath = [System.Uri]::EscapeDataString($frozen.path)
$restored = Invoke-SmokeJson -Path "/api/archive/frozen/restore?path=$restorePath" -OutFile (Join-Path $outputDir "frozen-restored.json")
$restoredCount = @($restored).Count
Assert-Condition ($restoredCount -gt 0) "expected restored frozen archive lines"

Write-Host "OxideLog production smoke passed"
Write-Host "API: $baseUrl"
Write-Host "Auth header: $(if ($headers.Count -gt 0) { "enabled" } else { "not set" })"
Write-Host "TCP ingest: $(if ($ingestEnabled) { "${TcpHost}:$TcpPort ($sampleCount lines)" } else { "skipped" })"
Write-Host "Events checked: $eventCount"
Write-Host "CSV: $csvPath"
Write-Host "Parquet archives listed: $parquetCount"
Write-Host "Frozen archives listed: $frozenCount"
Write-Host "Restored frozen lines: $restoredCount"
Write-Host "Artifacts: $outputDir"
