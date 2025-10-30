FROM alpine:3.22

RUN apk add --no-cache bash aws-cli python3

COPY cache-downloader.py /usr/bin/cache-downloader
RUN chmod 755 /usr/bin/cache-downloader

ARG RELEASE_NAME
ENV RELEASE_NAME=$RELEASE_NAME
RUN if [ -z "$RELEASE_NAME" ]; then echo "RELEASE_NAME argument not provided" && exit 1; fi

ARG PYTHON_VERSIONS
ENV PYTHON_VERSIONS=$PYTHON_VERSIONS
RUN if [ -z "$PYTHON_VERSIONS" ]; then echo "PYTHON_VERSIONS argument not provided" && exit 1; fi

ENV PYTHONUNBUFFERED=1

CMD [ "cache-downloader" ]