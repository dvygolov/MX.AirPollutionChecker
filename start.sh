#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$PROJECT_ROOT/.venv"
PY_BIN="$VENV_DIR/bin/python3"
APP_FILE="$PROJECT_ROOT/bot_maxapi.py"
LOG_FILE="$PROJECT_ROOT/airpollutionbot.log"

# Use pyenv Python 3.11 if available
export PYENV_ROOT="${PYENV_ROOT:-$HOME/.pyenv}"
if [[ -d "$PYENV_ROOT/versions/3.11.9" ]]; then
    PY_BIN="$PYENV_ROOT/versions/3.11.9/bin/python3.11"
    export PATH="$PYENV_ROOT/versions/3.11.9/bin:$PATH"
fi

cd "$PROJECT_ROOT"

echo "=== $(date '+%Y-%m-%d %H:%M:%S') Starting AirPollutionBot ===" >> "$LOG_FILE"

exec "$PY_BIN" "$APP_FILE" >> "$LOG_FILE" 2>&1
