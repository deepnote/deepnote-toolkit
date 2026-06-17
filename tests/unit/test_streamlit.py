import json
import logging
import unittest
from unittest.mock import MagicMock, call, patch

from installer.module.streamlit import fetch_streamlit_apps, start_streamlit_servers


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


class TestStartStreamlitServers(unittest.TestCase):
    def _make_apps(self, *entrypoints: str) -> list:
        return [
            {"id": f"id-{i}", "entrypoint": ep, "port": str(8501 + i), "projectId": "p"}
            for i, ep in enumerate(entrypoints)
        ]

    def test_skips_app_when_directory_missing(self):
        """Apps whose parent directory does not exist are skipped without crashing."""
        apps = self._make_apps("deleted_folder/app.py", "existing_folder/app.py")
        mock_venv = MagicMock()
        mock_logger = MagicMock(spec=logging.Logger)

        def exists_side_effect(path: str) -> bool:
            return "deleted_folder" not in path

        with (
            patch("installer.module.streamlit.fetch_streamlit_apps", return_value=apps),
            patch("installer.module.streamlit.os.path.exists", side_effect=exists_side_effect),
        ):
            start_streamlit_servers(mock_venv, mock_logger)

        mock_logger.warning.assert_called_once()
        assert any("deleted_folder" in str(a) for a in mock_logger.warning.call_args[0])
        assert mock_venv.start_server.call_count == 1
        started_cmd = mock_venv.start_server.call_args[0][0]
        assert "existing_folder/app.py" in started_cmd

    def test_continues_after_skipped_app(self):
        """A missing-directory app does not prevent subsequent apps from starting."""
        apps = self._make_apps("gone/app.py", "also_gone/app.py", "present/app.py")
        mock_venv = MagicMock()
        mock_logger = MagicMock(spec=logging.Logger)

        def exists_side_effect(path: str) -> bool:
            return "present" in path

        with (
            patch("installer.module.streamlit.fetch_streamlit_apps", return_value=apps),
            patch("installer.module.streamlit.os.path.exists", side_effect=exists_side_effect),
        ):
            start_streamlit_servers(mock_venv, mock_logger)

        assert mock_logger.warning.call_count == 2
        assert mock_venv.start_server.call_count == 1
