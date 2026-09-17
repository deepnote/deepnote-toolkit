FROM quay.io/pypa/manylinux_2_28_x86_64:2026.09.14-1@sha256:531d7aa844bbb0c131d4ab011d3db741c4abc8d498cd5ccc86121046f62303b4
RUN pipx install nox poetry==2.2.0

RUN dnf -y update && \
    dnf -y install java-17-openjdk-devel \
    # Required for pymssql - provides sqlfront.h header file
    freetds-devel \
    # Required for database connectivity through ODBC
    unixODBC-devel \
    # Required for secure connections (SSL/TLS)
    openssl-devel && \
    dnf clean all
