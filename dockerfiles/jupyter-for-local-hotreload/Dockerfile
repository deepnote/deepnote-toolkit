# Dockerfile for local development with hot-reload support
# This container expects the toolkit source to be mounted at /toolkit
# and installs it in editable mode for live code changes
#
# Build with:
#   docker build -t deepnote/jupyter-for-local:local -f dockerfiles/jupyter-for-local-hotreload/Dockerfile .

ARG FROM_PYTHON_TAG=3.12
FROM deepnote/python:${FROM_PYTHON_TAG}

ARG FROM_PYTHON_TAG

ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN apt-get update && \
    apt-get install --no-install-recommends -y \
    rsync \
    git \
    # Required for pymssql
    freetds-dev \
    # Required for database connectivity through ODBC
    unixodbc-dev \
    # Required for secure connections (SSL/TLS)
    libssl-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN pip install --no-cache-dir poetry==2.2.0

# Configure Poetry to create virtualenv outside the mounted source directory
# - virtualenvs.in-project false: Never use .venv from mounted host directory
# - virtualenvs.path: Store venvs in container-local directory
RUN poetry config virtualenvs.in-project false && \
    poetry config virtualenvs.path /opt/venvs

# Create toolkit directory (will be mounted over, but needed for initial setup)
RUN mkdir -p /toolkit /opt/venvs

WORKDIR /toolkit

# Environment variables for development mode
# POETRY_VIRTUALENVS_* ensures we never use host's .venv even if poetry.toml exists
ENV DEEPNOTE_RUNNING_IN_DEV_MODE=true \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=false \
    POETRY_VIRTUALENVS_PATH=/opt/venvs

# Copy the entrypoint script
COPY dockerfiles/jupyter-for-local-hotreload/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 8888

ENTRYPOINT ["/entrypoint.sh"]
