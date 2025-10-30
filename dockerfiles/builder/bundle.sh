#!/bin/bash

# This script is run in CI within the builder Docker image and the result is uploaded.
# We also use it in integration tests.

set -x
set -e

INSTALL_TOOLKIT_PACKAGE=${INSTALL_TOOLKIT_PACKAGE:-"true"}

# Mark git directory as safe (needed for poetry-dynamic-versioning in Docker)
git config --global --add safe.directory /deepnote-toolkit

# Poetry setup & export requirements.txt
poetry env use "$PY_VERSION"
poetry install $POETRY_INSTALL_ARGS
poetry run pip freeze > requirements.txt



# Remove local link to git repo
sed -i "/deepnote_toolkit/d" requirements.txt
sed -i "/deepnote-toolkit/d" requirements.txt

if [ "${INSTALL_TOOLKIT_PACKAGE}" = "true" ]; then
  # Build the project and update requirements.txt
  poetry build --format wheel
  # If sed replaced something, find the first .whl file and append it to requirements.txt
  find "$(pwd)" -maxdepth 2 -name "*.whl" -type f -print -quit >> requirements.txt
fi

if [ "${GENERATE_CONSTRAINTS_FILE}" = "true" ]; then
  # Generate constraints file
  poetry run generate-constraints --output ./dist/constraints"$PY_VERSION".txt
fi


# Output requirements.txt for debugging
cat requirements.txt

# Upgrade pip and uninstall all packages
python -m pip install --upgrade pip
python -m pip freeze | xargs python -m pip uninstall -y

# Safety check -- make sure no packages are currently installed
if [ $(python -m pip freeze | wc -c) -ne 0 ]; then
  exit 1
fi

mkdir -p ${PREFIX}

# Install all core libraries
python -m pip install --prefix ${PREFIX} -r requirements.txt
rm requirements.txt

# Replace shebangs with /usr/bin/env python to respect the python from current venv
find ${PREFIX}/bin -type f -exec sed -i '1 s|^#!.*python[0-9.]*|#!/usr/bin/env python|' {} +

cd "$PREFIX" || exit 1
