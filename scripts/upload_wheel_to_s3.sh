#!/bin/bash
#
# Upload a Python wheel to S3 and output the URLs
#
# Usage: ./upload_wheel_to_s3.sh <wheel_path> <version> <s3_bucket_prefix>
#
# Example: ./upload_wheel_to_s3.sh dist/pkg-1.0.0-py3-none-any.whl 1.0.0 s3://bucket/path
#
# Outputs (to stdout):
#   S3_URL=s3://bucket/path/version/wheel.whl
#   HTTP_URL=https://bucket.s3.amazonaws.com/path/version/wheel.whl

set -euo pipefail

if [ $# -ne 3 ]; then
    echo "Usage: $0 <wheel_path> <version> <s3_bucket_prefix>" >&2
    echo "Example: $0 dist/pkg-1.0.0.whl 1.0.0 s3://bucket/path" >&2
    exit 1
fi

WHEEL_PATH="$1"
VERSION="$2"
S3_PREFIX="$3"

# Validate inputs
if [ ! -f "$WHEEL_PATH" ]; then
    echo "Error: Wheel file not found: $WHEEL_PATH" >&2
    exit 1
fi

if [ -z "$VERSION" ]; then
    echo "Error: Version cannot be empty" >&2
    exit 1
fi

if [[ ! "$S3_PREFIX" =~ ^s3:// ]]; then
    echo "Error: S3 prefix must start with s3://" >&2
    exit 1
fi

# Extract wheel filename
WHEEL_NAME=$(basename "$WHEEL_PATH")

# URL-encode the version and wheel name for use in HTTP URLs
# Replace + with %2B and other special characters
URL_ENCODED_VERSION=$(printf '%s' "$VERSION" | sed 's/+/%2B/g')
URL_ENCODED_WHEEL_NAME=$(printf '%s' "$WHEEL_NAME" | sed 's/+/%2B/g')

# Upload to S3
echo "Uploading $WHEEL_NAME to S3..." >&2
aws s3 cp "$WHEEL_PATH" "${S3_PREFIX}/${VERSION}/" --acl public-read >&2

# Construct URLs
S3_URL="${S3_PREFIX}/${VERSION}/${WHEEL_NAME}"

# Parse S3 URL to construct HTTP URL
# Remove s3:// prefix
S3_WITHOUT_PREFIX="${S3_PREFIX#s3://}"
# Extract bucket name (everything before first /)
BUCKET="${S3_WITHOUT_PREFIX%%/*}"
# Extract path prefix (everything after first /)
if [[ "$S3_WITHOUT_PREFIX" == *"/"* ]]; then
    PATH_PREFIX="${S3_WITHOUT_PREFIX#*/}"
    HTTP_URL="https://${BUCKET}.s3.amazonaws.com/${PATH_PREFIX}/${URL_ENCODED_VERSION}/${URL_ENCODED_WHEEL_NAME}"
else
    # No path prefix, just bucket
    HTTP_URL="https://${BUCKET}.s3.amazonaws.com/${URL_ENCODED_VERSION}/${URL_ENCODED_WHEEL_NAME}"
fi

# Output URLs for local usage when not running in GitHub Actions
if [ -z "${GITHUB_OUTPUT:-}" ]; then
    echo "S3_URL=${S3_URL}"
    echo "HTTP_URL=${HTTP_URL}"
    echo "✓ Upload complete" >&2
    echo "  S3: ${S3_URL}" >&2
    echo "  HTTP: ${HTTP_URL}" >&2
else
    {
        echo "s3_url=${S3_URL}"
        echo "http_url=${HTTP_URL}"
    } >> "$GITHUB_OUTPUT"
    echo "✓ Upload complete" >&2
fi
