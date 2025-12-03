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

# Install Poetry and required plugins
RUN pip install --no-cache-dir poetry==2.2.0 && \
    poetry self add 'poetry-dynamic-versioning[plugin]>=1.0.0,<2.0.0'

# Configure Poetry to create virtualenv outside the mounted source directory
RUN poetry config virtualenvs.path /opt/venvs

# Create toolkit directory (will be mounted over, but needed for initial setup)
RUN mkdir -p /toolkit /opt/venvs

WORKDIR /toolkit

# Environment variables for development mode
ENV DEEPNOTE_RUNNING_IN_DEV_MODE=true \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Copy the entrypoint script
COPY dockerfiles/jupyter-for-local-hotreload/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 8888

ENTRYPOINT ["/entrypoint.sh"]
