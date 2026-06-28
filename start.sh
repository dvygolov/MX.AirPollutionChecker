#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$PROJECT_ROOT/.venv"
PY_BIN="$VENV_DIR/bin/python3"
APP_FILE="$PROJECT_ROOT/bot.py"
LOG_FILE="$PROJECT_ROOT/airpollutionbot.log"

cd "$PROJECT_ROOT"

echo "=== $(date '+%Y-%m-%d %H:%M:%S') Starting AirPollutionBot ===" >> "$LOG_FILE"

exec "$PY_BIN" "$APP_FILE" >> "$LOG_FILE" 2>&1
