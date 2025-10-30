FROM quay.io/pypa/manylinux_2_28_x86_64:2025.05.28-1
RUN pipx install nox poetry==2.2.0

RUN dnf -y update && \
    dnf -y install java-11-openjdk-devel \
    # Required for pymssql - provides sqlfront.h header file
    freetds-devel \
    # Required for database connectivity through ODBC
    unixODBC-devel \
    # Required for secure connections (SSL/TLS)
    openssl-devel && \
    dnf clean all
