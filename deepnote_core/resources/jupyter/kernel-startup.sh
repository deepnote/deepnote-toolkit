#!/bin/sh
set -e

# Explicitly exclude server packages for kernels
export DEEPNOTE_INCLUDE_SERVER_PACKAGES=false

# Now Python won't load server packages
exec python -m ipykernel_launcher -f "$1"
