#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="/home/administrator/projects/polybot"
CONFIG_PATH="$PROJECT_DIR/config/supervisor-live.yaml"
LOG_PATH="$PROJECT_DIR/live-supervisor.log"
PID_FILE="$PROJECT_DIR/.live-supervisor.pid"
PATTERN="python -m polybot.live.supervisor --config $CONFIG_PATH"

cd "$PROJECT_DIR"

# If a tracked PID is alive and matches the supervisor command, nothing to do.
if [[ -f "$PID_FILE" ]]; then
  pid="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -n "${pid:-}" ]] && ps -p "$pid" -o args= 2>/dev/null | grep -Fq "$PATTERN"; then
    exit 0
  fi
fi

# If a supervisor is already alive but the PID file is stale/missing, adopt it.
existing_pid="$(pgrep -f "$PATTERN" | head -n 1 || true)"
if [[ -n "${existing_pid:-}" ]]; then
  echo "$existing_pid" > "$PID_FILE"
  exit 0
fi

# Start the live supervisor. It is safe to keep this always-on: per-strategy
# live execution is gated by the dashboard-controlled Postgres status.
set -a
source "$PROJECT_DIR/.env.live"
set +a
source "$PROJECT_DIR/.venv/bin/activate"

mkdir -p "$(dirname "$LOG_PATH")"
{
  echo "$(date -Is) watchdog starting live supervisor: $PATTERN"
} >> "$LOG_PATH"

nohup python -m polybot.live.supervisor --config "$CONFIG_PATH" >> "$LOG_PATH" 2>&1 &
echo "$!" > "$PID_FILE"
