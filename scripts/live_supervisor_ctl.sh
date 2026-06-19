#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="/home/administrator/projects/polybot"
CONFIG_PATH="$PROJECT_DIR/config/supervisor-live.yaml"
PID_FILE="$PROJECT_DIR/.live-supervisor.pid"
PATTERN="python -m polybot.live.supervisor --config $CONFIG_PATH"

case "${1:-status}" in
  start)
    "$PROJECT_DIR/scripts/live_supervisor_watchdog.sh"
    ;;
  stop)
    if [[ -f "$PID_FILE" ]]; then
      pid="$(cat "$PID_FILE" 2>/dev/null || true)"
      if [[ -n "${pid:-}" ]] && ps -p "$pid" -o args= 2>/dev/null | grep -Fq "$PATTERN"; then
        kill "$pid"
      fi
      rm -f "$PID_FILE"
    fi
    pkill -f "$PATTERN" 2>/dev/null || true
    ;;
  restart)
    "$0" stop
    sleep 1
    "$0" start
    ;;
  status)
    if pgrep -af "$PATTERN"; then
      exit 0
    fi
    echo "live supervisor is not running" >&2
    exit 1
    ;;
  *)
    echo "Usage: $0 {start|stop|restart|status}" >&2
    exit 2
    ;;
esac
