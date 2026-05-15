param(
    [string] $HostName = "192.168.0.142",
    [string] $User = "root",
    [string] $Repo = "git@github.com:kos991/oxidelog.git",
    [string] $Branch = "main",
    [string] $RemoteDir = "/opt/oxidelog"
)

$ErrorActionPreference = "Stop"

$target = "$User@$HostName"

ssh $target "set -e; if [ ! -d '$RemoteDir/.git' ]; then if [ -e '$RemoteDir' ]; then echo '$RemoteDir exists but is not a git checkout' >&2; exit 1; fi; git clone --branch '$Branch' '$Repo' '$RemoteDir'; fi; cd '$RemoteDir'; git fetch origin '$Branch'; git checkout '$Branch'; git pull --ff-only origin '$Branch'; chmod +x scripts/goal-linux.sh; ./scripts/goal-linux.sh"
