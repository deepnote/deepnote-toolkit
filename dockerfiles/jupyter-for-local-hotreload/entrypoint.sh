#!/bin/bash
set -e

# Entrypoint script for local development container
# Sets up filesystem mounts, installs toolkit in editable mode, and starts servers

echo "[local-toolkit] Starting local development environment..."

# Check if toolkit source is mounted
if [ ! -f "/toolkit/pyproject.toml" ]; then
    echo "[local-toolkit] ERROR: Toolkit source not found at /toolkit"
    echo "[local-toolkit] Make sure to mount the deepnote-toolkit directory to /toolkit"
    exit 1
fi

# Wait for s3fs mount to be available (created by localstack container)
echo "[local-toolkit] Waiting for /deepnote-mounts/s3fs to be created..."
while [ ! -d /deepnote-mounts/s3fs ]; do
    sleep 2
done
echo "[local-toolkit] /deepnote-mounts/s3fs is available"

mkdir -p /datasets
ln -sf /deepnote-mounts/s3fs /datasets/_deepnote_work
echo "[local-toolkit] Created /datasets/_deepnote_work symlink"

# Create /work symlink pointing to project-specific path
# In dev mode with PROJECT_ID, use project-specific path under s3fs
if [ -n "$PROJECT_ID" ]; then
    PROJECT_WORK_PATH="/datasets/_deepnote_work/projects/${PROJECT_ID}"
    mkdir -p "$PROJECT_WORK_PATH"
    ln -sf "$PROJECT_WORK_PATH" /work
    echo "[local-toolkit] Created /work -> $PROJECT_WORK_PATH symlink"
else
    ln -sf /datasets/_deepnote_work /work
    echo "[local-toolkit] Created /work -> /datasets/_deepnote_work symlink"
fi

cd /toolkit

# Install dependencies and toolkit in editable mode
echo "[local-toolkit] Installing toolkit in editable mode..."
poetry install --extras server --no-interaction

echo "[local-toolkit] Starting servers from /work directory..."

# Create log directory and start tailing the log file in background
# This makes toolkit logs visible in docker container output
LOG_FILE="/root/.local/state/deepnote-toolkit/logs/helpers.log"
mkdir -p "$(dirname "$LOG_FILE")"
touch "$LOG_FILE"
tail -f "$LOG_FILE" &
TAIL_PID=$!

# Clean up tail process on exit
cleanup() {
    if kill -0 "$TAIL_PID" 2>/dev/null; then
        kill "$TAIL_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT SIGINT SIGTERM

# Configure Jupyter to use /work as its root directory
# This is picked up by the config loader and passed to Jupyter's --ServerApp.root_dir
export DEEPNOTE_PATHS__NOTEBOOK_ROOT=/work

cd /work

# Run server in foreground (not exec) so trap can clean up tail process
poetry --directory /toolkit run deepnote-toolkit server "$@"
exit $?
