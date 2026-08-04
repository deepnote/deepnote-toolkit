"""Stand-ins for what the SQL cache object store answers with.

Shared by the tests for the cache flow and for the diagnostics taken off its
failures, so both see the same bodies and the same response semantics.
"""

from unittest import mock

# Signing parameters given values that are easy to search log output for
PRESIGNED_URL = (
    "https://bucket.s3.eu-west-1.amazonaws.com/ws/int/key"
    "?X-Amz-Algorithm=AWS4-HMAC-SHA256"
    "&X-Amz-Credential=CREDVALUE%2F20260729%2Feu-west-1%2Fs3%2Faws4_request"
    "&X-Amz-Date=20260729T120000Z&X-Amz-Expires=900"
    "&X-Amz-Security-Token=TOKENVALUE&X-Amz-Signature=SIGVALUE"
)

# What requests raises when it cannot reach the object store. The URL urllib3
# embeds is path-only - there is no scheme and no host in front of it.
URLLIB3_ERROR_MESSAGE = (
    "HTTPSConnectionPool(host='bucket.s3.eu-west-1.amazonaws.com', port=443): "
    "Max retries exceeded with url: /ws/int/key"
    "?X-Amz-Algorithm=AWS4-HMAC-SHA256"
    "&X-Amz-Credential=CREDVALUE%2F20260729%2Feu-west-1%2Fs3%2Faws4_request"
    "&X-Amz-Date=20260729T120000Z&X-Amz-Expires=900"
    "&X-Amz-Security-Token=TOKENVALUE&X-Amz-Signature=SIGVALUE "
    "(Caused by NameResolutionError('Failed to resolve host'))"
)

SECRETS = ("CREDVALUE", "TOKENVALUE", "SIGVALUE", "X-Amz-")

AWS_HEADERS = {
    "x-amz-request-id": "REQ123",
    "x-amz-id-2": "HOSTID456",
    "Date": "Wed, 29 Jul 2026 12:31:07 GMT",
}

ACCESS_DENIED_EXPIRED_BODY = (
    b'<?xml version="1.0" encoding="UTF-8"?>\n'
    b"<Error><Code>AccessDenied</Code><Message>Request has expired</Message>"
    b"<X-Amz-Expires>900</X-Amz-Expires>"
    b"<Expires>2026-07-29T12:15:00Z</Expires>"
    b"<ServerTime>2026-07-29T12:31:07Z</ServerTime>"
    b"<RequestId>REQ123</RequestId><HostId>HOSTID456</HostId></Error>"
)

# S3 rejecting credentials that died before the URL's own expiry
EXPIRED_TOKEN_BODY = (
    b'<?xml version="1.0" encoding="UTF-8"?>\n'
    b"<Error><Code>ExpiredToken</Code>"
    b"<Message>The provided token has expired.</Message>"
    b"<RequestId>REQ123</RequestId><HostId>HOSTID456</HostId></Error>"
)

# S3 echoes the canonical request - and with it the signed query string - when
# the signature does not match
SIGNATURE_MISMATCH_BODY = (
    b'<?xml version="1.0" encoding="UTF-8"?>\n'
    b"<Error><Code>SignatureDoesNotMatch</Code>"
    b"<Message>The request signature we calculated does not match the signature "
    b"you provided. Check your key and signing method.</Message>"
    b"<AWSAccessKeyId>CREDVALUE</AWSAccessKeyId>"
    b"<StringToSign>AWS4-HMAC-SHA256\n20260729T120000Z\n</StringToSign>"
    b"<CanonicalRequest>PUT\n/ws/int/key\n"
    b"X-Amz-Credential=CREDVALUE&amp;X-Amz-Security-Token=TOKENVALUE"
    b"&amp;X-Amz-Signature=SIGVALUE\nhost:bucket.s3.eu-west-1.amazonaws.com\n"
    b"</CanonicalRequest>"
    b"<RequestId>REQ123</RequestId><HostId>HOSTID456</HostId></Error>"
)

# A gateway that answered instead of S3 and echoed the request line back
PROXY_ECHO_BODY = (
    b"<html><head><title>502 Bad Gateway</title></head><body>\n"
    b"<h1>502 Bad Gateway</h1>\n"
    b"<p>Upstream failed for request: PUT "
    b"https://bucket.s3.eu-west-1.amazonaws.com/ws/int/key"
    b"?X-Amz-Algorithm=AWS4-HMAC-SHA256"
    b"&X-Amz-Credential=CREDVALUE%2F20260729%2Feu-west-1%2Fs3%2Faws4_request"
    b"&X-Amz-Date=20260729T120000Z&X-Amz-Expires=900"
    b"&X-Amz-Security-Token=TOKENVALUE&X-Amz-Signature=SIGVALUE</p>\n"
    b"</body></html>"
)


def s3_response(status_code, body=b"", headers=None):
    """Build a stand-in for a requests.Response from the object store."""
    response = mock.MagicMock(
        status_code=status_code, content=body, headers=headers or {}
    )
    response.__enter__.return_value = response
    # a real streamed body reads back empty once the block exits, silently
    response.__exit__.side_effect = lambda *_: setattr(response, "content", b"")
    # a chunked response yields one HTTP chunk per read however large a size is
    # asked for
    response.iter_content.side_effect = lambda size: iter(
        [body[i : i + 20] for i in range(0, len(body), 20)]
    )
    return response
