import json
import logging
import unittest
from unittest.mock import MagicMock, patch

from installer.module.streamlit import fetch_streamlit_apps


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
