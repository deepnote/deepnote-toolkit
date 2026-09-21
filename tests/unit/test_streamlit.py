import json
import logging
import unittest
from unittest.mock import MagicMock, patch

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
            patch(
                "installer.module.streamlit.os.path.exists",
                side_effect=exists_side_effect,
            ),
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
            patch(
                "installer.module.streamlit.os.path.exists",
                side_effect=exists_side_effect,
            ),
        ):
            start_streamlit_servers(mock_venv, mock_logger)

        assert mock_logger.warning.call_count == 2
        assert mock_venv.start_server.call_count == 1

    def test_passes_app_id_as_environment_data(self) -> None:
        """App IDs are data, never shell syntax; validation belongs to the SDK."""
        apps = [
            {
                "id": "11111111-2222-3333-4444-555555555555",
                "entrypoint": "a/app.py",
                "port": "8501",
            },
            {"id": "x; rm -rf /", "entrypoint": "b/app.py", "port": "8502"},
        ]
        mock_venv = MagicMock()

        with (
            patch("installer.module.streamlit.fetch_streamlit_apps", return_value=apps),
            patch("installer.module.streamlit.os.path.exists", return_value=True),
        ):
            start_streamlit_servers(mock_venv, MagicMock(spec=logging.Logger))

        calls = mock_venv.start_server.call_args_list
        assert calls[0].args[0].startswith("streamlit run /work/a/app.py ")
        assert calls[1].args[0].startswith("streamlit run /work/b/app.py ")
        assert calls[0].kwargs["env"] == {"DEEPNOTE_STREAMLIT_APP_ID": apps[0]["id"]}
        assert calls[1].kwargs["env"] == {"DEEPNOTE_STREAMLIT_APP_ID": apps[1]["id"]}

    def test_missing_app_id_warns_and_still_marks_process_as_hosted(self) -> None:
        """Missing app IDs warn without permitting local credential fallback."""
        app = {"entrypoint": "app.py", "port": "8501"}
        venv = MagicMock()
        logger = MagicMock(spec=logging.Logger)
        with (
            patch(
                "installer.module.streamlit.fetch_streamlit_apps", return_value=[app]
            ),
            patch("installer.module.streamlit.os.path.exists", return_value=True),
        ):
            start_streamlit_servers(venv, logger)
        logger.warning.assert_called_once()
        assert venv.start_server.call_args.kwargs["env"] == {
            "DEEPNOTE_STREAMLIT_APP_ID": ""
        }
