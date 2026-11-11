#!/usr/bin/env bash
# Run on the server after cargo build --release --locked --bin quant-collector.
set -euo pipefail
SOURCE=${1:?Usage: install-bbo.sh BUILD_DIRECTORY}
install -m 0755 "$SOURCE/target/release/quant-collector" /opt/quant/bin/quant-bbo
install -m 0644 "$SOURCE/config/collector-bbo.json" /opt/quant/config/collector-bbo.json
install -d -o quant -g quant -m 0700 /var/lib/quant-bbo
printf '%s\n' 'QUANT_SPOOL_DIR=/var/lib/quant-bbo' 'QUANT_S3_PREFIX=quant/bbo-v1' > /etc/quant/bbo.env
chmod 0600 /etc/quant/bbo.env
for component in collector archiver; do
    sed -e 's/quant-archiver.service/quant-bbo-archiver.service/g' \
        -e 's@/var/lib/quant-collector@/var/lib/quant-bbo@g' \
        -e 's/StateDirectory=quant-collector/StateDirectory=quant-bbo/' \
        -e 's@/opt/quant/bin/quant-collector@/opt/quant/bin/quant-bbo@g' \
        -e 's@/opt/quant/config/collector.json@/opt/quant/config/collector-bbo.json@g' \
        -e '/^ExecStart=/i EnvironmentFile=/etc/quant/bbo.env' \
        /etc/systemd/system/quant-$component.service > /etc/systemd/system/quant-bbo-$component.service
 done
systemctl daemon-reload
# Starting and enabling is intentionally separate, after a bounded smoke run.
