#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$PROJECT_ROOT/.venv"
PY_BIN="$VENV_DIR/bin/python3.11"
APP_FILE="$PROJECT_ROOT/bot.py"
SERVICE_NAME="airpollutionbot"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"

if [ "$EUID" -ne 0 ]; then
    echo "Error: must run as root (use sudo)"
    exit 1
fi

cat > "$SERVICE_FILE" <<EOF
[Unit]
Description=MAX Air Pollution Bot (Volgar)
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=$PROJECT_ROOT
Environment=PYTHONUNBUFFERED=1
ExecStart=$PY_BIN $APP_FILE
Restart=on-failure
RestartSec=10
KillMode=process

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable "$SERVICE_NAME"

# Убить любые старые процессы
pkill -9 -f 'bot.py' 2>/dev/null || true
sleep 1

systemctl start "$SERVICE_NAME"
sleep 2
systemctl status "$SERVICE_NAME" --no-pager

echo "=== Done ==="
echo "Status: systemctl status $SERVICE_NAME"
echo "Logs: journalctl -u $SERVICE_NAME -f"
