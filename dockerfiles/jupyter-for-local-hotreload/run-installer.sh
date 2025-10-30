#!/bin/bash

set -euo pipefail

while [ ! -d /deepnote-mounts/s3fs ]; do
  echo "Waiting for /deepnote-mounts/s3fs to be created..."
  sleep 2
done

mkdir -p /datasets
ln -sf /deepnote-mounts/s3fs /datasets/_deepnote_work
mkdir -p /var/log/deepnote
touch /var/log/deepnote/helpers.log

handle_sigint() {
    echo "Received SIGINT, shutting down..."
    # Send TERM to all processes in our process group except ourselves
    echo "Sending TERM to all remaining child processes..."
    pkill -P $$ 2>/dev/null

    # Final cleanup - forcefully kill any remaining child processes
    sleep 2
    echo "Sending KILL to any remaining processes..."
    pkill -9 -P $$ 2>/dev/null
    exit 0
}

trap handle_sigint SIGINT

echo "Starting installer..."
if [ -n "${WITH_SERVER_LOGS:-}" ]; then
    poetry run python -m installer --venv-path "$(poetry env info --path)" &
else
    poetry run python -m installer --venv-path "$(poetry env info --path)" > /dev/null 2>&1 &
fi
installer_pid=$!

echo "Starting log tail..."
tail -f /var/log/deepnote/helpers.log &
tail_pid=$!

wait $installer_pid $tail_pid
