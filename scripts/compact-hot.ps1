param(
    [string] $HostName = "192.168.0.142",
    [string] $User = "root",
    [string] $RemoteDir = "/opt/oxidelog-src",
    [string] $DuckDb = "/var/lib/oxidelog/duckdb/oxidelog.duckdb",
    [int] $HotLimit = 100000
)

$ErrorActionPreference = "Stop"

$target = "$User@$HostName"
$remoteCommand = "set -e; cd '$RemoteDir'; git pull --ff-only origin main; chmod +x scripts/compact-hot-linux.sh; bash scripts/compact-hot-linux.sh '$DuckDb' '$HotLimit'"
ssh $target $remoteCommand
