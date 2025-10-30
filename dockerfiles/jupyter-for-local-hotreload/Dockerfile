ARG FROM_PYTHON_TAG
FROM deepnote/python:${FROM_PYTHON_TAG}

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN pip install poetry==2.2.0

WORKDIR /deepnote-toolkit

ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_CREATE=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=0 \
    POETRY_CACHE_DIR=/tmp/poetry_cache

COPY pyproject.toml poetry.lock poetry.toml ./

RUN poetry install --no-interaction --no-ansi --with server --with dev

ENV PYTHONPATH=/deepnote-toolkit:/deepnote-toolkit/installer:$PYTHONPATH \
    TOOLKIT_BUNDLE_PATH=/deepnote-toolkit \
    TOOLKIT_VERSION="local-build" \
    USERNAME=user \
    PASSWORD=password \
    DEEPNOTE_RUNNING_IN_DEV_MODE=true \
    DEEPNOTE_WEBAPP_URL="http://host.docker.internal:3002"

COPY dockerfiles/jupyter-for-local-hotreload/run-installer.sh /usr/local/bin/run-installer.sh

ENTRYPOINT ["/usr/local/bin/run-installer.sh"]
