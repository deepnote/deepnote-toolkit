from unittest.mock import Mock, patch

import pytest
import requests

from deepnote_toolkit.feature_flags import (
    _fetch_feature_flags,
    get_flag_variant,
    is_flag_enabled,
)


@pytest.fixture(autouse=True)
def clear_cache():
    """Clear the LRU cache before each test to ensure test isolation."""
    _fetch_feature_flags.cache_clear()
    yield
    _fetch_feature_flags.cache_clear()


class TestFetchFeatureFlags:
    """Tests for the _fetch_feature_flags function."""

    @patch("deepnote_toolkit.feature_flags.get_absolute_userpod_api_url")
    @patch("deepnote_toolkit.feature_flags.get_project_auth_headers")
    @patch("deepnote_toolkit.feature_flags.requests.get")
    def test_successful_fetch(self, mock_get, mock_get_headers, mock_get_url):
        """Test successful feature flags fetch."""
        mock_get_url.return_value = "http://test.com/api/toolkit/feature-flags"
        mock_get_headers.return_value = {"Authorization": "Bearer test"}

        mock_response = Mock()
        mock_response.json.return_value = {
            "enable_new_feature": True,
            "experiment_variant": "control",
            "max_retries": 5,
        }
        mock_get.return_value = mock_response

        result = _fetch_feature_flags()

        assert result == {
            "enable_new_feature": True,
            "experiment_variant": "control",
            "max_retries": 5,
        }
        mock_get.assert_called_once_with(
            "http://test.com/api/toolkit/feature-flags",
            headers={"Authorization": "Bearer test"},
            timeout=10,
        )

    @patch("deepnote_toolkit.feature_flags.get_absolute_userpod_api_url")
    @patch("deepnote_toolkit.feature_flags.get_project_auth_headers")
    @patch("deepnote_toolkit.feature_flags.requests.get")
    def test_fetch_with_empty_response(self, mock_get, mock_get_headers, mock_get_url):
        """Test fetch when API returns empty dict."""
        mock_get_url.return_value = "http://test.com/api"
        mock_get_headers.return_value = {}

        mock_response = Mock()
        mock_response.json.return_value = {}
        mock_get.return_value = mock_response

        result = _fetch_feature_flags()

        assert result == {}

    @patch("deepnote_toolkit.feature_flags.get_absolute_userpod_api_url")
    @patch("deepnote_toolkit.feature_flags.get_project_auth_headers")
    @patch("deepnote_toolkit.feature_flags.requests.get")
    def test_fetch_with_non_dict_response(
        self, mock_get, mock_get_headers, mock_get_url
    ):
        """Test fetch when API returns non-dict response."""
        mock_get_url.return_value = "http://test.com/api"
        mock_get_headers.return_value = {}

        mock_response = Mock()
        mock_response.json.return_value = ["not", "a", "dict"]
        mock_get.return_value = mock_response

        result = _fetch_feature_flags()

        assert result == {}

    @patch("deepnote_toolkit.feature_flags.get_absolute_userpod_api_url")
    @patch("deepnote_toolkit.feature_flags.get_project_auth_headers")
    @patch("deepnote_toolkit.feature_flags.requests.get")
    def test_fetch_with_http_error(self, mock_get, mock_get_headers, mock_get_url):
        """Test fetch when API returns HTTP error."""
        mock_get_url.return_value = "http://test.com/api"
        mock_get_headers.return_value = {}

        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("404 Not Found")
        mock_get.return_value = mock_response

        result = _fetch_feature_flags()

        assert result == {}

    @patch("deepnote_toolkit.feature_flags.get_absolute_userpod_api_url")
    @patch("deepnote_toolkit.feature_flags.get_project_auth_headers")
    @patch("deepnote_toolkit.feature_flags.requests.get")
    def test_fetch_with_connection_error(
        self, mock_get, mock_get_headers, mock_get_url
    ):
        """Test fetch when connection fails."""
        mock_get_url.return_value = "http://test.com/api"
        mock_get_headers.return_value = {}
        mock_get.side_effect = requests.ConnectionError("Network error")

        result = _fetch_feature_flags()

        assert result == {}

    @patch("deepnote_toolkit.feature_flags.get_absolute_userpod_api_url")
    @patch("deepnote_toolkit.feature_flags.get_project_auth_headers")
    @patch("deepnote_toolkit.feature_flags.requests.get")
    def test_fetch_with_timeout(self, mock_get, mock_get_headers, mock_get_url):
        """Test fetch when request times out."""
        mock_get_url.return_value = "http://test.com/api"
        mock_get_headers.return_value = {}
        mock_get.side_effect = requests.Timeout("Request timed out")

        result = _fetch_feature_flags()

        assert result == {}

    @patch("deepnote_toolkit.feature_flags.get_absolute_userpod_api_url")
    @patch("deepnote_toolkit.feature_flags.get_project_auth_headers")
    @patch("deepnote_toolkit.feature_flags.requests.get")
    def test_fetch_with_json_decode_error(
        self, mock_get, mock_get_headers, mock_get_url
    ):
        """Test fetch when response contains invalid JSON."""
        mock_get_url.return_value = "http://test.com/api"
        mock_get_headers.return_value = {}

        mock_response = Mock()
        mock_response.json.side_effect = ValueError("Invalid JSON")
        mock_get.return_value = mock_response

        result = _fetch_feature_flags()

        assert result == {}

    @patch("deepnote_toolkit.feature_flags.get_absolute_userpod_api_url")
    @patch("deepnote_toolkit.feature_flags.get_project_auth_headers")
    @patch("deepnote_toolkit.feature_flags.requests.get")
    def test_caching_behavior(self, mock_get, mock_get_headers, mock_get_url):
        """Test that feature flags are cached and only fetched once."""
        mock_get_url.return_value = "http://test.com/api"
        mock_get_headers.return_value = {}

        mock_response = Mock()
        mock_response.json.return_value = {"test_flag": True}
        mock_get.return_value = mock_response

        # First call
        result1 = _fetch_feature_flags()
        # Second call
        result2 = _fetch_feature_flags()

        assert result1 == result2 == {"test_flag": True}
        # Verify requests.get was only called once due to caching
        mock_get.assert_called_once()


class TestIsFlagEnabled:
    """Tests for the is_flag_enabled function."""

    @patch("deepnote_toolkit.feature_flags._fetch_feature_flags")
    def test_enabled_boolean_flag(self, mock_fetch):
        """Test flag that is enabled (True)."""
        mock_fetch.return_value = {"my_feature": True}

        result = is_flag_enabled("my_feature")

        assert result is True

    @patch("deepnote_toolkit.feature_flags._fetch_feature_flags")
    def test_disabled_boolean_flag(self, mock_fetch):
        """Test flag that is disabled (False)."""
        mock_fetch.return_value = {"my_feature": False}

        result = is_flag_enabled("my_feature")

        assert result is False

    @patch("deepnote_toolkit.feature_flags._fetch_feature_flags")
    def test_missing_flag_uses_default_false(self, mock_fetch):
        """Test that missing flag returns default value (False)."""
        mock_fetch.return_value = {"other_flag": True}

        result = is_flag_enabled("nonexistent_flag")

        assert result is False

    @patch("deepnote_toolkit.feature_flags._fetch_feature_flags")
    def test_missing_flag_uses_custom_default(self, mock_fetch):
        """Test that missing flag returns custom default value."""
        mock_fetch.return_value = {"other_flag": True}

        result = is_flag_enabled("nonexistent_flag", default=True)

        assert result is True

    @patch("deepnote_toolkit.feature_flags._fetch_feature_flags")
    def test_string_flag_returns_default(self, mock_fetch):
        """Test that string flag value returns default."""
        mock_fetch.return_value = {"my_feature": "enabled"}

        result = is_flag_enabled("my_feature", default=False)

        assert result is False

    @patch("deepnote_toolkit.feature_flags._fetch_feature_flags")
    def test_numeric_flag_returns_default(self, mock_fetch):
        """Test that numeric flag value returns default."""
        mock_fetch.return_value = {"my_feature": 1}

        result = is_flag_enabled("my_feature", default=False)

        assert result is False

    @patch("deepnote_toolkit.feature_flags._fetch_feature_flags")
    def test_none_flag_returns_default(self, mock_fetch):
        """Test that None flag value returns default."""
        mock_fetch.return_value = {"my_feature": None}

        result = is_flag_enabled("my_feature", default=True)

        assert result is True

    @patch("deepnote_toolkit.feature_flags._fetch_feature_flags")
    def test_empty_flags_dict(self, mock_fetch):
        """Test behavior when flags dict is empty."""
        mock_fetch.return_value = {}

        result = is_flag_enabled("any_flag", default=True)

        assert result is True


class TestGetFlagVariant:
    """Tests for the get_flag_variant function."""

    @patch("deepnote_toolkit.feature_flags._fetch_feature_flags")
    def test_string_variant(self, mock_fetch):
        """Test getting a string variant value."""
        mock_fetch.return_value = {"experiment": "variant_a"}

        result = get_flag_variant("experiment", default="control")

        assert result == "variant_a"

    @patch("deepnote_toolkit.feature_flags._fetch_feature_flags")
    def test_missing_variant_uses_default(self, mock_fetch):
        """Test that missing variant returns default value."""
        mock_fetch.return_value = {"other_flag": "value"}

        result = get_flag_variant("nonexistent_flag", default="fallback")

        assert result == "fallback"

    @patch("deepnote_toolkit.feature_flags._fetch_feature_flags")
    def test_boolean_variant_returns_default(self, mock_fetch):
        """Test that boolean variant value returns default."""
        mock_fetch.return_value = {"experiment": True}

        result = get_flag_variant("experiment", default="control")

        assert result == "control"

    @patch("deepnote_toolkit.feature_flags._fetch_feature_flags")
    def test_numeric_variant_returns_default(self, mock_fetch):
        """Test that numeric variant value returns default."""
        mock_fetch.return_value = {"experiment": 42}

        result = get_flag_variant("experiment", default="control")

        assert result == "control"

    @patch("deepnote_toolkit.feature_flags._fetch_feature_flags")
    def test_none_variant_returns_default(self, mock_fetch):
        """Test that None variant value returns default."""
        mock_fetch.return_value = {"experiment": None}

        result = get_flag_variant("experiment", default="control")

        assert result == "control"

    @patch("deepnote_toolkit.feature_flags._fetch_feature_flags")
    def test_empty_string_variant(self, mock_fetch):
        """Test getting an empty string variant value."""
        mock_fetch.return_value = {"experiment": ""}

        result = get_flag_variant("experiment", default="control")

        assert result == ""

    @patch("deepnote_toolkit.feature_flags._fetch_feature_flags")
    def test_empty_flags_dict(self, mock_fetch):
        """Test behavior when flags dict is empty."""
        mock_fetch.return_value = {}

        result = get_flag_variant("any_flag", default="default_value")

        assert result == "default_value"

    @patch("deepnote_toolkit.feature_flags._fetch_feature_flags")
    def test_multiple_string_variants(self, mock_fetch):
        """Test multiple variant flags in the same response."""
        mock_fetch.return_value = {
            "experiment_a": "variant_1",
            "experiment_b": "variant_2",
            "experiment_c": "control",
        }

        assert get_flag_variant("experiment_a", default="x") == "variant_1"
        assert get_flag_variant("experiment_b", default="x") == "variant_2"
        assert get_flag_variant("experiment_c", default="x") == "control"
