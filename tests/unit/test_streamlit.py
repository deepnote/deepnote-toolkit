import json
import logging
import os
import unittest
from unittest.mock import MagicMock, patch

from installer.module.streamlit import (
    fetch_integration_env_vars,
    fetch_streamlit_apps,
    set_integration_env_vars,
)


class TestFetchStreamlitApps(unittest.TestCase):
    def test_fetch_streamlit_apps(self):
        mock_data = {
            "streamlitApps": [
                {
                    "id": "3853c7f5-2048-4b57-946d-6c5592c3317e",
                    "entrypoint": "app.py",
                    "port": "8501",
                    "projectId": "d37eb9bb-07af-4ba5-bd27-fe6aaf52b740",
                }
            ]
        }

        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps(mock_data).encode("utf-8")

        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_urlopen.return_value.__enter__.return_value = mock_response

            test_logger = logging.getLogger("testLogger")

            streamlit_apps = fetch_streamlit_apps(test_logger)

            mock_urlopen.assert_called_once_with(
                "http://localhost:19456/userpod-api/streamlit-apps", timeout=3
            )

            self.assertEqual(
                streamlit_apps,
                [
                    {
                        "id": "3853c7f5-2048-4b57-946d-6c5592c3317e",
                        "entrypoint": "app.py",
                        "port": "8501",
                        "projectId": "d37eb9bb-07af-4ba5-bd27-fe6aaf52b740",
                    }
                ],
            )


class TestFetchIntegrationEnvVars(unittest.TestCase):
    """Tests for fetching integration env vars from the WebApp."""

    def test_fetch_integration_env_vars(self) -> None:
        """Returns the parsed list of env var dicts on success."""
        mock_data = [
            {"name": "SNOWFLAKE_USER", "value": "admin"},
            {"name": "SNOWFLAKE_PASSWORD", "value": "secret123"},
        ]

        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps(mock_data).encode("utf-8")

        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_urlopen.return_value.__enter__.return_value = mock_response

            test_logger = logging.getLogger("testLogger")
            variables = fetch_integration_env_vars(test_logger)

            mock_urlopen.assert_called_once_with(
                "http://localhost:19456/userpod-api/integrations/environment-variables",
                timeout=3,
            )

            self.assertEqual(variables, mock_data)

    def test_fetch_integration_env_vars_empty(self) -> None:
        """Returns an empty list when the endpoint returns no vars."""
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps([]).encode("utf-8")

        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_urlopen.return_value.__enter__.return_value = mock_response

            test_logger = logging.getLogger("testLogger")
            variables = fetch_integration_env_vars(test_logger)

            self.assertEqual(variables, [])

    def test_fetch_integration_env_vars_network_error(self) -> None:
        """Returns an empty list when a network error occurs."""
        import urllib.error

        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_urlopen.side_effect = urllib.error.URLError("connection refused")

            test_logger = logging.getLogger("testLogger")
            variables = fetch_integration_env_vars(test_logger)

            self.assertEqual(variables, [])

    def test_fetch_integration_env_vars_non_list_payload(self) -> None:
        """Returns an empty list when the payload is not a list."""
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({"unexpected": "shape"}).encode(
            "utf-8"
        )

        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_urlopen.return_value.__enter__.return_value = mock_response

            test_logger = logging.getLogger("testLogger")
            variables = fetch_integration_env_vars(test_logger)

            self.assertEqual(variables, [])


class TestSetIntegrationEnvVars(unittest.TestCase):
    """Tests for setting integration env vars in os.environ."""

    def test_set_integration_env_vars(self) -> None:
        """Valid env vars are set in os.environ."""
        self.addCleanup(os.environ.pop, "TEST_INT_VAR_A", None)
        self.addCleanup(os.environ.pop, "TEST_INT_VAR_B", None)

        mock_data = [
            {"name": "TEST_INT_VAR_A", "value": "value_a"},
            {"name": "TEST_INT_VAR_B", "value": "value_b"},
        ]

        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps(mock_data).encode("utf-8")

        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_urlopen.return_value.__enter__.return_value = mock_response

            test_logger = logging.getLogger("testLogger")
            set_integration_env_vars(test_logger)

            self.assertEqual(os.environ.get("TEST_INT_VAR_A"), "value_a")
            self.assertEqual(os.environ.get("TEST_INT_VAR_B"), "value_b")

    def test_set_integration_env_vars_skips_invalid_entries(self) -> None:
        """Invalid entries are skipped without affecting valid ones."""
        self.addCleanup(os.environ.pop, "TEST_INT_VAR_C", None)

        mock_data = [
            {"name": "TEST_INT_VAR_C", "value": "value_c"},
            {"name": None, "value": "orphan_value"},
            {"name": "TEST_INT_VAR_D", "value": None},
            {"name": "", "value": "empty_key"},
            {"name": "HAS=EQUALS", "value": "bad"},
            {"name": 123, "value": "non_string_key"},
            {"name": "GOOD_KEY", "value": 456},
            "not_a_dict_entry",
            42,
            {"name": "NULL_BYTE_VAL", "value": "bad\0value"},
            {"name": "BAD\0NAME", "value": "null_byte_name"},
        ]

        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps(mock_data).encode("utf-8")

        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_urlopen.return_value.__enter__.return_value = mock_response

            test_logger = logging.getLogger("testLogger")
            set_integration_env_vars(test_logger)

            self.assertEqual(os.environ.get("TEST_INT_VAR_C"), "value_c")
            self.assertNotIn("TEST_INT_VAR_D", os.environ)
            self.assertNotIn("HAS=EQUALS", os.environ)
            self.assertNotIn("GOOD_KEY", os.environ)
            self.assertNotIn("NULL_BYTE_VAL", os.environ)
            self.assertNotIn("BAD\0NAME", os.environ)
