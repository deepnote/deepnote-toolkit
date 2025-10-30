ARG FROM_PYTHON_TAG=3.12
FROM deepnote/python:${FROM_PYTHON_TAG}

ARG BUNDLE_PATH="./dist"
ARG FROM_PYTHON_TAG

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
    apt-get install --no-install-recommends -y rsync && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy the compute-helpers files
COPY ${BUNDLE_PATH} /toolkit-bundle

RUN mkdir -p /tmp/toolkit-bundle && \
    tar -xf /toolkit-bundle/python${FROM_PYTHON_TAG}.tar -C /tmp/toolkit-bundle

ENV TOOLKIT_BUNDLE_PATH=/tmp/toolkit-bundle \
    DEEPNOTE_RUNNING_IN_DEV_MODE=true

EXPOSE 8888

CMD ["bash", "-c", "python /toolkit-bundle/installer.zip"]
