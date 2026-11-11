#!/usr/bin/env bash
set -euo pipefail
build_dir=${1:?Usage: sudo bash deploy/install-trader-shadow.sh BUILD_DIRECTORY}
test "$(id -u)" = 0
test -x "$build_dir/target/release/quant-trader"
test -f /opt/quant/config/collector-bbo.json
install -m 0755 "$build_dir/target/release/quant-trader" /opt/quant/bin/quant-trader
install -d -m 0700 -o quant -g quant /var/lib/quant-trader-shadow
sed 's#"../var/trader-shadow"#"/var/lib/quant-trader-shadow"#' \
  "$build_dir/config/trader-shadow.json" > /opt/quant/config/trader-shadow.json
chmod 0644 /opt/quant/config/trader-shadow.json
install -m 0644 "$build_dir/deploy/quant-trader-shadow.service" /etc/systemd/system/
install -m 0644 "$build_dir/deploy/quant-trader-shadow.timer" /etc/systemd/system/
/opt/quant/bin/quant-trader --config /opt/quant/config/trader-shadow.json --check
systemd-analyze verify /etc/systemd/system/quant-trader-shadow.service /etc/systemd/system/quant-trader-shadow.timer
systemctl daemon-reload
systemctl enable --now quant-trader-shadow.timer
systemctl list-timers quant-trader-shadow.timer --no-pager
