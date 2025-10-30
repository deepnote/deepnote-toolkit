FROM quay.io/pypa/manylinux_2_28_x86_64:2025.05.28-1

ARG PY_VERSION
ENV PY_VERSION=$PY_VERSION

# Create a symbolic link for the specified Python version
RUN ln -s /usr/local/bin/python${PY_VERSION} /usr/local/bin/python

# Install system dependencies for building Python packages
RUN yum -y update && \
    yum -y install zip \
    # Required for pymssql - provides sqlfront.h header file
    freetds-devel \
    # Required for database connectivity through ODBC
    unixODBC-devel \
    # Required for secure connections (SSL/TLS)
    openssl-devel

# Install Poetry for Python dependency management and packaging
# (Poetry installer isn't versioned, pin by commit)
ARG POETRY_INSTALLER_URL="https://raw.githubusercontent.com/python-poetry/install.python-poetry.org/6027c8e3a3b723586f3f171d90d3cb74fb6a2018/install-poetry.py"
# Verification command:
#   curl -fsSL "${POETRY_INSTALLER_URL}" -o /tmp/install-poetry.py && shasum -a 256 /tmp/install-poetry.py
ARG POETRY_INSTALLER_SHA256="963d56703976ce9cdc6ff460c44a4f8fbad64c110dc447b86eeabb4a47ec2160"
RUN set -euo pipefail \
    && curl -fsSL "${POETRY_INSTALLER_URL}" -o /tmp/install-poetry.py \
    && echo "${POETRY_INSTALLER_SHA256}  /tmp/install-poetry.py" | sha256sum -c - \
    && python3 /tmp/install-poetry.py --version 2.2.0 --yes \
    && rm /tmp/install-poetry.py
ENV PATH="/root/.local/bin:${PATH}"

# Set up working directory
RUN mkdir -p /deepnote-toolkit
WORKDIR /deepnote-toolkit

# Default command to execute the bundle script
CMD ["bash", "-c", "./dockerfiles/builder/bundle.sh"]
