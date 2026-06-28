#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$PROJECT_ROOT/.venv"
PY_BIN="$VENV_DIR/bin/python3.11"
APP_FILE="$PROJECT_ROOT/bot.py"
LOG_FILE="$PROJECT_ROOT/airpollutionbot.log"
PID_FILE="$PROJECT_ROOT/bot.pid"

cd "$PROJECT_ROOT"

# Если бот уже запущен — не запускать двойник
if [ -f "$PID_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE" 2>/dev/null || true)
    if [ -n "$OLD_PID" ] && kill -0 "$OLD_PID" 2>/dev/null; then
        echo "=== $(date '+%Y-%m-%d %H:%M:%S') Bot already running (PID $OLD_PID), skipping ===" >> "$LOG_FILE"
        exit 0
    fi
fi

echo "=== $(date '+%Y-%m-%d %H:%M:%S') Starting AirPollutionBot ===" >> "$LOG_FILE"

# Запуск в фоне с PID-файлом
nohup "$PY_BIN" "$APP_FILE" >> "$LOG_FILE" 2>&1 &
echo $! > "$PID_FILE"
