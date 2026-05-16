param(
    [string] $HostName = "192.168.0.142",
    [string] $User = "root",
    [string] $RemoteDir = "/opt/oxidelog-src",
    [string] $RemoteInput = "/opt/sangfor_fw_log",
    [string] $DuckDb = "/var/lib/oxidelog/duckdb/oxidelog.duckdb",
    [int] $BatchSize = 500000,
    [string] $LocalInput = ""
)

$ErrorActionPreference = "Stop"

$target = "$User@$HostName"

if ($LocalInput -ne "") {
    $resolved = Resolve-Path $LocalInput
    $localPath = $resolved.Path
    $leaf = Split-Path -Leaf $localPath
    if ($RemoteInput -eq "/opt/sangfor_fw_log") {
        $RemoteInput = "/opt/$leaf"
    }
    scp -r $localPath "${target}:/opt/"
}

$remoteCommand = "set -e; cd '$RemoteDir'; git pull --ff-only origin main; chmod +x scripts/import-history-linux.sh; bash scripts/import-history-linux.sh '$RemoteInput' '$DuckDb' '$BatchSize'"
ssh $target $remoteCommand
