#!/usr/bin/env bash
# setup_cron.sh — Install a weekly cron job that auto-refreshes HereHack data caches.
#
# Usage:
#   chmod +x setup_cron.sh
#   ./setup_cron.sh           # install weekly cron (every Sunday at 03:00)
#   ./setup_cron.sh --remove  # remove the cron job
#   ./setup_cron.sh --status  # show installed cron jobs for this project

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV_PYTHON="$PROJECT_DIR/venv/bin/python"
SCRIPT="$PROJECT_DIR/refresh_data.py"
LOG_FILE="$PROJECT_DIR/logs/cron_refresh.log"
CRON_TAG="# herehack-auto-refresh"

# Fall back to system python if venv is missing
if [[ ! -f "$VENV_PYTHON" ]]; then
    VENV_PYTHON="$(which python3)"
fi

CRON_CMD="0 3 * * 0 cd \"$PROJECT_DIR\" && \"$VENV_PYTHON\" \"$SCRIPT\" --refresh >> \"$LOG_FILE\" 2>&1 $CRON_TAG"

usage() {
    echo "Usage: $0 [--remove | --status]"
    exit 1
}

install_cron() {
    # Remove any existing entry for this project
    (crontab -l 2>/dev/null | grep -v "$CRON_TAG") | crontab -
    # Append new entry
    (crontab -l 2>/dev/null; echo "$CRON_CMD") | crontab -
    echo "Installed cron job:"
    echo "  $CRON_CMD"
    echo ""
    echo "Data will be refreshed every Sunday at 03:00."
    echo "Logs → $LOG_FILE"
}

remove_cron() {
    (crontab -l 2>/dev/null | grep -v "$CRON_TAG") | crontab -
    echo "Removed HereHack auto-refresh cron job."
}

show_status() {
    echo "Current crontab entries for this project:"
    crontab -l 2>/dev/null | grep "$CRON_TAG" || echo "  (none installed)"
}

case "${1:-}" in
    --remove) remove_cron ;;
    --status) show_status ;;
    "")       install_cron ;;
    *)        usage ;;
esac
