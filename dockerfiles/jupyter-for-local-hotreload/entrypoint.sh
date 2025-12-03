#!/bin/bash
set -e

# Entrypoint script for local development container
# Installs toolkit in editable mode and starts servers via deepnote-toolkit CLI

echo "[local-toolkit] Starting local development environment..."

# Check if toolkit source is mounted
if [ ! -f "/toolkit/pyproject.toml" ]; then
    echo "[local-toolkit] ERROR: Toolkit source not found at /toolkit"
    echo "[local-toolkit] Make sure to mount the deepnote-toolkit directory to /toolkit"
    exit 1
fi

cd /toolkit

# Mark git directory as safe (needed for poetry-dynamic-versioning)
git config --global --add safe.directory /toolkit

# Install dependencies and toolkit in editable mode
echo "[local-toolkit] Installing toolkit in editable mode..."
poetry install --extras server --no-interaction

echo "[local-toolkit] Starting servers..."

# Start servers using the toolkit CLI (handles Jupyter, LSP, config, etc.)
exec poetry run deepnote-toolkit server "$@"
