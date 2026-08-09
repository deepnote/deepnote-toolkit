import json
import time
import unittest

from parameterized import parameterized

from deepnote_toolkit.sql.sql_cache_diagnostics import (
    _URL_QUERY_PATTERN,
    describe_exception,
    describe_presigned_url,
    describe_s3_error,
    redact_sensitive,
    safe_url_path,
)

from .helpers.sql_cache_fixtures import (
    ACCESS_DENIED_EXPIRED_BODY,
    AWS_HEADERS,
    EXPIRED_TOKEN_BODY,
    PRESIGNED_URL,
    PROXY_ECHO_BODY,
    SECRETS,
    SIGNATURE_MISMATCH_BODY,
    URLLIB3_ERROR_MESSAGE,
    s3_response,
)


class TestRedactSensitive(unittest.TestCase):
    def test_strips_query_string_from_urlopen_style_message(self):
        redacted = redact_sensitive(URLLIB3_ERROR_MESSAGE)

        self.assertIn("bucket.s3.eu-west-1.amazonaws.com", redacted)
        self.assertIn("/ws/int/key?<redacted>", redacted)
        for secret in SECRETS:
            self.assertNotIn(secret, redacted)

    def test_query_string_strip_alone_redacts_urllib3_message(self):
        """URL query stripping must redact without the AWS-param backstop."""
        stripped = _URL_QUERY_PATTERN.sub(r"\1?<redacted>", URLLIB3_ERROR_MESSAGE)

        for secret in SECRETS:
            self.assertNotIn(secret, stripped)

    def test_blanks_aws_params_outside_a_url(self):
        redacted = redact_sensitive(
            "X-Amz-Credential=CREDVALUE&X-Amz-Security-Token=TOKENVALUE"
            "&X-Amz-Signature=SIGVALUE"
        )

        self.assertEqual(
            redacted,
            "X-Amz-Credential=<redacted>&X-Amz-Security-Token=<redacted>"
            "&X-Amz-Signature=<redacted>",
        )

    @parameterized.expand(
        [
            ("question_in_prose", "Is this ok? Yes it is."),
            ("bare_question", "what? nothing"),
            ("plain_sentence", "The provided token has expired."),
        ]
    )
    def test_leaves_ordinary_text_unchanged(self, _, text):
        self.assertEqual(redact_sensitive(text), text)


class TestDescribeException(unittest.TestCase):
    def test_large_message_is_bounded_and_redacted_in_bounded_time(self):
        """Truncate before redact: _URL_QUERY_PATTERN is quadratic on long strings."""
        message = URLLIB3_ERROR_MESSAGE + "&padding=" + "A" * 64_000

        started = time.monotonic()
        described = describe_exception(ValueError(message))
        elapsed = time.monotonic() - started

        self.assertEqual(described["error_type"], "ValueError")
        self.assertLessEqual(len(described["error_message"]), 500)
        self.assertIn("/ws/int/key?<redacted>", described["error_message"])
        for secret in SECRETS:
            self.assertNotIn(secret, described["error_message"])
        self.assertLess(elapsed, 2.0)


class TestDescribeS3Error(unittest.TestCase):
    def test_extracts_code_and_message_from_xml(self):
        diagnostics = describe_s3_error(
            s3_response(403, ACCESS_DENIED_EXPIRED_BODY, AWS_HEADERS)
        )

        self.assertEqual(diagnostics["status_code"], 403)
        self.assertEqual(diagnostics["s3_error_code"], "AccessDenied")
        self.assertIn("Request has expired", diagnostics["s3_error_message"])
        self.assertEqual(diagnostics["s3_expires"], "2026-07-29T12:15:00Z")
        self.assertEqual(diagnostics["s3_server_time"], "2026-07-29T12:31:07Z")

    def test_elements_absent_from_the_body_are_none(self):
        """This body carries no <Expires> or <ServerTime>."""
        diagnostics = describe_s3_error(s3_response(403, EXPIRED_TOKEN_BODY))

        self.assertEqual(diagnostics["s3_error_code"], "ExpiredToken")
        self.assertEqual(
            diagnostics["s3_error_message"], "The provided token has expired."
        )
        self.assertIsNone(diagnostics["s3_expires"])
        self.assertIsNone(diagnostics["s3_server_time"])

    def test_captures_aws_request_headers(self):
        diagnostics = describe_s3_error(
            s3_response(403, EXPIRED_TOKEN_BODY, AWS_HEADERS)
        )

        self.assertEqual(diagnostics["aws_request_id"], "REQ123")
        self.assertEqual(diagnostics["aws_host_id"], "HOSTID456")
        self.assertEqual(diagnostics["aws_date"], "Wed, 29 Jul 2026 12:31:07 GMT")

    def test_signature_mismatch_body_surfaces_only_code_and_message(self):
        """Field allowlist must ignore <CanonicalRequest> (signed query lives there)."""
        diagnostics = describe_s3_error(
            s3_response(403, SIGNATURE_MISMATCH_BODY, AWS_HEADERS)
        )

        self.assertEqual(diagnostics["s3_error_code"], "SignatureDoesNotMatch")
        self.assertNotIn("response_body_snippet", diagnostics)
        for value in diagnostics.values():
            for secret in SECRETS:
                self.assertNotIn(secret, str(value))

    def test_proxy_body_echoing_request_url_is_redacted(self):
        """Non-S3 bodies are not on the XML allowlist; snippet redaction must apply."""
        diagnostics = describe_s3_error(s3_response(502, PROXY_ECHO_BODY))

        self.assertIsNone(diagnostics["s3_error_code"])
        snippet = diagnostics["response_body_snippet"]
        self.assertIn("502 Bad Gateway", snippet)
        self.assertIn("/ws/int/key?<redacted>", snippet)
        for secret in SECRETS:
            self.assertNotIn(secret, snippet)

    def test_non_xml_body_yields_snippet_without_code(self):
        diagnostics = describe_s3_error(
            s3_response(502, b"<html><body>502 Bad Gateway</body></html>")
        )

        self.assertIsNone(diagnostics["s3_error_code"])
        self.assertIsNone(diagnostics["aws_request_id"])
        self.assertIn("502 Bad Gateway", diagnostics["response_body_snippet"])
        self.assertLessEqual(len(diagnostics["response_body_snippet"]), 500)

    def test_oversized_body_is_bounded(self):
        body = (
            b"<Error><Code>AccessDenied</Code><Message>"
            + b"x" * 1000
            + b"</Message>"
            + b"y" * 10_000
            + b"</Error>"
        )

        diagnostics = describe_s3_error(s3_response(403, body))

        self.assertEqual(diagnostics["s3_error_code"], "AccessDenied")
        for value in diagnostics.values():
            if isinstance(value, str):
                self.assertLessEqual(len(value), 500)

    def test_attributes_on_an_element_do_not_hide_it(self):
        """Tag matching must survive attributes, which push the '>' away."""
        diagnostics = describe_s3_error(
            s3_response(
                403,
                b'<Error><Code lang="en">AccessDenied</Code>'
                b"<Message>Request has expired</Message></Error>",
            )
        )

        self.assertEqual(diagnostics["s3_error_code"], "AccessDenied")
        self.assertEqual(diagnostics["s3_error_message"], "Request has expired")

    def test_empty_code_element_falls_back_to_the_snippet(self):
        diagnostics = describe_s3_error(
            s3_response(403, b"<Error><Code/><Message>Denied</Message></Error>")
        )

        self.assertIsNone(diagnostics["s3_error_code"])
        self.assertIn("Denied", diagnostics["response_body_snippet"])


class TestDescribePresignedUrl(unittest.TestCase):
    def test_returns_path_and_expiry(self):
        described = describe_presigned_url(PRESIGNED_URL)

        self.assertEqual(described["object_host"], "bucket.s3.eu-west-1.amazonaws.com")
        self.assertEqual(described["object_path"], "/ws/int/key")
        self.assertEqual(described["url_expires_in"], 900)

    @parameterized.expand(
        [
            ("absent", "https://example.com/x"),
            ("non_numeric", "https://example.com/x?X-Amz-Expires=abc"),
        ]
    )
    def test_unusable_expires_yields_none(self, _, url):
        described = describe_presigned_url(url)

        self.assertEqual(described["object_path"], "/x")
        self.assertIsNone(described["url_expires_in"])

    @parameterized.expand(
        [
            ("unterminated_ipv6", "https://[::1"),
            ("empty", ""),
            ("not_a_url", "not a url"),
            ("none", None),
            ("bytes", b"/ws/int/key"),
            ("dict", {"url": "https://example.com/x"}),
        ]
    )
    def test_malformed_url_does_not_raise_or_leak(self, _, url):
        described = describe_presigned_url(url)

        self.assertIsNone(described["url_expires_in"])
        for value in described.values():
            for secret in SECRETS:
                self.assertNotIn(secret, str(value))
        # Log extras must json.dumps; bytes values would drop the whole report.
        json.dumps(described)
        json.dumps(safe_url_path(url))

    @parameterized.expand(
        [
            (
                "separator_encoded",
                "%3FX-Amz-Credential=CREDVALUE&X-Amz-Security-Token=TOKENVALUE"
                "&X-Amz-Signature=SIGVALUE",
            ),
            (
                "separator_and_equals_encoded",
                "%3FX-Amz-Credential%3DCREDVALUE&X-Amz-Security-Token%3DTOKENVALUE"
                "&X-Amz-Signature%3DSIGVALUE",
            ),
            (
                "whole_query_encoded",
                "%3FX-Amz-Credential%3DCREDVALUE%26X-Amz-Security-Token%3DTOKENVALUE"
                "%26X-Amz-Signature%3DSIGVALUE",
            ),
            ("separator_is_a_semicolon", ";X-Amz-Credential=CREDVALUE"),
            ("no_separator_at_all", "X-Amz-Signature=SIGVALUE"),
        ]
    )
    def test_over_encoded_url_does_not_leak_signing_params_via_path(self, _, suffix):
        """urlsplit only splits on a literal '?'; signing params can land in path."""
        described = describe_presigned_url(
            "https://bucket.s3.eu-west-1.amazonaws.com/ws/int/key" + suffix
        )

        for value in described.values():
            for secret in ("CREDVALUE", "TOKENVALUE", "SIGVALUE"):
                self.assertNotIn(secret, str(value))
