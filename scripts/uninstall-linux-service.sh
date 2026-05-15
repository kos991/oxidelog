#!/usr/bin/env bash
set -euo pipefail

if [ "$(id -u)" -ne 0 ]; then
  echo "uninstall-linux-service.sh must be run as root." >&2
  exit 1
fi

if systemctl list-unit-files oxidelog.service >/dev/null 2>&1; then
  systemctl stop oxidelog.service || true
  systemctl disable oxidelog.service || true
fi

rm -f /etc/systemd/system/oxidelog.service
systemctl daemon-reload
systemctl reset-failed oxidelog.service >/dev/null 2>&1 || true

rm -f /opt/oxidelog/bin/fwlogd
rm -f /etc/oxidelog/config.toml

rmdir /opt/oxidelog/bin /opt/oxidelog /etc/oxidelog 2>/dev/null || true

echo "OxideLog service removed. Data under /var/lib/oxidelog was left intact."
