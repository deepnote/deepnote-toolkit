import json
import logging
import os
import tempfile
import unittest
from io import StringIO
from unittest import mock
from urllib.error import URLError

from deepnote_toolkit.logging import (
    LoggerManager,
    WebappErrorHandler,
    get_logger,
    report_error_to_webapp,
)


class TestLoggerManager(unittest.TestCase):
    """Test the LoggerManager class."""

    def setUp(self):
        # Create a temporary directory for log files
        self.temp_dir = tempfile.TemporaryDirectory()
        self.log_file = os.path.join(self.temp_dir.name, "test.log")

        # Reset singleton state between tests
        LoggerManager.reset()

    def tearDown(self):
        # Clean up the temporary directory
        self.temp_dir.cleanup()

        # Reset singleton state after tests
        LoggerManager.reset()

        # Clear environment variables that might affect tests
        if "CI" in os.environ:
            del os.environ["CI"]

    def test_singleton_pattern(self):
        """Test that LoggerManager implements the singleton pattern correctly."""
        manager1 = LoggerManager(log_file=self.log_file)
        manager2 = LoggerManager(log_file=self.log_file)

        # Both instances should be the same object
        self.assertIs(manager1, manager2)

    def test_get_logger_convenience_function(self):
        """Test that get_logger returns the same logger as LoggerManager().get_logger()."""
        logger1 = get_logger(log_file=self.log_file)
        logger2 = LoggerManager(log_file=self.log_file).get_logger()

        # Both loggers should be the same object
        self.assertIs(logger1, logger2)

    def test_file_logger_creation(self):
        """Test that a file logger is created correctly in non-CI environment."""
        # Ensure CI environment variable is not set
        if "CI" in os.environ:
            del os.environ["CI"]

        manager = LoggerManager(log_file=self.log_file)
        logger = manager.get_logger()

        # Log a test message
        test_message = "Test file logging"
        logger.info(test_message)

        # Check that the file was created and contains the message
        with open(self.log_file, "r") as f:
            log_content = f.read()
            self.assertIn(test_message, log_content)

    @mock.patch("sys.stdout", new_callable=StringIO)
    def test_stdout_logger_in_ci(self, mock_stdout):
        """Test that logs go to stdout in CI environment."""
        # Set CI environment variable
        os.environ["CI"] = "true"

        manager = LoggerManager(log_file=self.log_file)
        logger = manager.get_logger()

        # Log a test message
        test_message = "Test stdout logging in CI"
        logger.info(test_message)

        # Check that the message was printed to stdout
        self.assertIn(test_message, mock_stdout.getvalue())

        # Check that the file was not created
        self.assertFalse(os.path.exists(self.log_file))


class TestWebappErrorHandler(unittest.TestCase):
    """Test the WebappErrorHandler class."""

    def setUp(self):
        # Create a test logger with a unique name for each test to avoid handler accumulation
        self.logger_name = f"test_webapp_handler_{id(self)}"
        self.logger = logging.getLogger(self.logger_name)

        # Make sure we start with a clean logger
        self.logger.handlers = []
        self.logger.setLevel(logging.ERROR)

        # Add our test handler
        self.handler = WebappErrorHandler()
        self.logger.addHandler(self.handler)

    @mock.patch("deepnote_toolkit.logging.report_error_to_webapp")
    def test_error_reporting(self, mock_report):
        """Test that errors are reported to the webapp."""
        # Reset any previous calls to the mock
        mock_report.reset_mock()

        # Log an error
        error_message = "Test error message"
        self.logger.error(error_message)

        # Check that report_error_to_webapp was called with the right arguments
        self.assertEqual(
            mock_report.call_count,
            1,
            "report_error_to_webapp should be called exactly once",
        )
        args, kwargs = mock_report.call_args
        self.assertEqual(args[0], "TOOLKIT_RUNTIME_ERROR")
        self.assertIn(error_message, args[1])

    @mock.patch("deepnote_toolkit.logging.report_error_to_webapp")
    def test_extra_context_handling(self, mock_report):
        """Test that extra context is extracted and passed to report_error_to_webapp."""
        # Reset any previous calls to the mock
        mock_report.reset_mock()

        # Log an error with extra context
        error_message = "Test error message with context"
        extra_context = {"test_key": "test_value", "cause": "test_cause"}
        self.logger.error(error_message, extra=extra_context)

        # Check that report_error_to_webapp was called with the extra context
        self.assertEqual(
            mock_report.call_count,
            1,
            "report_error_to_webapp should be called exactly once",
        )
        # Verify the arguments
        args, kwargs = mock_report.call_args
        self.assertEqual(args[0], "TOOLKIT_RUNTIME_ERROR")
        self.assertIn(error_message, args[1])

        # In Python 3.12, a 'taskName' key is added to the context
        # Check that our extra context keys are present in the reported context
        for key, value in extra_context.items():
            self.assertIn(key, args[2])
            self.assertEqual(args[2][key], value)

    @mock.patch("deepnote_toolkit.logging.report_error_to_webapp")
    def test_exception_handling(self, mock_report):
        """Test that exceptions in the handler don't propagate."""
        # Reset any previous calls to the mock
        mock_report.reset_mock()

        # Make report_error_to_webapp raise an exception
        mock_report.side_effect = Exception("Test exception")

        # Log an error - this should not raise an exception
        try:
            self.logger.error("This should not raise an exception")
            # If we get here, no exception was raised
            success = True
        except Exception:
            success = False

        self.assertTrue(success, "Exception was not caught by the handler")

        # Verify the mock was called
        self.assertEqual(
            mock_report.call_count,
            1,
            "report_error_to_webapp should be called exactly once",
        )


class TestReportErrorToWebapp(unittest.TestCase):
    """Test the report_error_to_webapp function."""

    @mock.patch.dict(os.environ, {"DEEPNOTE_PROJECT_ID": "test-project-id"})
    @mock.patch("deepnote_toolkit.logging.urlopen")
    @mock.patch("deepnote_toolkit.get_webapp_url.get_absolute_userpod_api_url")
    @mock.patch("logging.warning")
    @mock.patch("logging.error")
    def test_successful_report(
        self, mock_log_error, mock_log_warning, mock_get_url, mock_urlopen
    ):
        """Test a successful error report to the webapp."""
        # Mock response
        mock_response = mock.MagicMock()
        mock_response.status = 200
        mock_urlopen.return_value.__enter__.return_value = mock_response

        # The function will call get_absolute_userpod_api_url("toolkit/errors")
        # Mock the return value to match the expected format
        mock_get_url.return_value = "http://localhost:19456/userpod-api/toolkit/errors"

        # Call the function
        error_type = "TEST_ERROR"
        error_message = "Test error message"
        report_error_to_webapp(error_type, error_message)

        # Check that the error was logged with warning level
        mock_log_warning.assert_called_once()
        log_message = f"[{error_type}] {error_message}"
        self.assertEqual(mock_log_warning.call_args[0][0], log_message)

        # Check that urlopen was called with the right arguments
        mock_urlopen.assert_called_once()

        # Check the request data
        request = mock_urlopen.call_args[0][0]
        # Check the URL is what we expect
        expected_url = mock_get_url.return_value
        self.assertEqual(request.full_url, expected_url)
        self.assertEqual(request.method, "POST")

        # Check the timeout is set to 5 seconds
        self.assertEqual(mock_urlopen.call_args[1]["timeout"], 5)

        # Decode and parse the request data
        request_data = json.loads(request.data.decode())
        self.assertEqual(request_data["type"], error_type)
        self.assertEqual(request_data["message"], error_message)

    @mock.patch.dict(os.environ, {"DEEPNOTE_PROJECT_ID": "test-project-id"})
    @mock.patch("deepnote_toolkit.logging.urlopen")
    @mock.patch("deepnote_toolkit.get_webapp_url.get_absolute_userpod_api_url")
    @mock.patch("logging.warning")
    @mock.patch("logging.error")
    def test_extra_context_inclusion(
        self, mock_log_error, mock_log_warning, mock_get_url, mock_urlopen
    ):
        """Test that extra context is included in the request."""
        # Mock response
        mock_response = mock.MagicMock()
        mock_response.status = 200
        mock_urlopen.return_value.__enter__.return_value = mock_response

        # The function will call get_absolute_userpod_api_url("toolkit/errors")
        # Mock the return value to match the expected format
        mock_get_url.return_value = "http://localhost:19456/userpod-api/toolkit/errors"

        # Call the function with extra context
        error_type = "TEST_ERROR"
        error_message = "Test error message"
        extra_context = {"test_key": "test_value", "cause": "test_cause"}
        report_error_to_webapp(error_type, error_message, extra_context)

        # Check the logging includes the context
        mock_log_warning.assert_called_once()
        log_message = f"[{error_type}] {error_message} | Context: {extra_context}"
        self.assertEqual(mock_log_warning.call_args[0][0], log_message)

        # Verify urlopen was called
        mock_urlopen.assert_called_once()

        # Check the request data
        request = mock_urlopen.call_args[0][0]
        request_data = json.loads(request.data.decode())
        self.assertEqual(request_data["context"], extra_context)

    # test_missing_project_id is no longer needed since project_id check was removed

    @mock.patch.dict(os.environ, {"DEEPNOTE_PROJECT_ID": "test-project-id"})
    @mock.patch("deepnote_toolkit.logging.urlopen")
    @mock.patch("deepnote_toolkit.get_webapp_url.get_absolute_userpod_api_url")
    @mock.patch("logging.warning")
    @mock.patch("logging.error")
    def test_error_response(
        self, mock_log_error, mock_log_warning, mock_get_url, mock_urlopen
    ):
        """Test handling of non-200 response."""
        # Mock error response
        mock_response = mock.MagicMock()
        mock_response.status = 500
        mock_response.reason = "Internal Server Error"
        mock_urlopen.return_value.__enter__.return_value = mock_response

        # The function will call get_absolute_userpod_api_url("toolkit/errors")
        # Mock the return value to match the expected format
        mock_get_url.return_value = "http://localhost:19456/userpod-api/toolkit/errors"

        # Call the function
        report_error_to_webapp("TEST_ERROR", "Test error message", retries=0)

        # Verify urlopen was called
        mock_urlopen.assert_called_once()

        # Check that the errors are logged with warning level
        mock_log_warning.assert_called()  # Base message is always logged

    @mock.patch.dict(os.environ, {"DEEPNOTE_PROJECT_ID": "test-project-id"})
    @mock.patch("deepnote_toolkit.logging.urlopen")
    @mock.patch("deepnote_toolkit.get_webapp_url.get_absolute_userpod_api_url")
    @mock.patch("logging.warning")
    @mock.patch("logging.error")
    def test_url_error(
        self, mock_log_error, mock_log_warning, mock_get_url, mock_urlopen
    ):
        """Test handling of URLError."""
        # Mock URLError
        mock_urlopen.side_effect = URLError("Connection refused")

        # The function will call get_absolute_userpod_api_url("toolkit/errors")
        # Mock the return value to match the expected format
        mock_get_url.return_value = "http://localhost:19456/userpod-api/toolkit/errors"

        # Call the function
        report_error_to_webapp("TEST_ERROR", "Test error message")

        # Check that the error was logged with warning level
        mock_log_warning.assert_called()  # Base message is always logged
        # Check for the specific warning message
        url_error_message = mock_log_warning.call_args_list[1][0][0]
        self.assertTrue(url_error_message.startswith("Failed"))
        self.assertIn("Failed to report error", url_error_message)

    @mock.patch.dict(os.environ, {"DEEPNOTE_PROJECT_ID": "test-project-id"})
    @mock.patch("deepnote_toolkit.logging.urlopen")
    @mock.patch("deepnote_toolkit.get_webapp_url.get_absolute_userpod_api_url")
    @mock.patch("logging.warning")
    @mock.patch("logging.error")
    def test_generic_exception_handling(
        self, mock_log_error, mock_log_warning, mock_get_url, mock_urlopen
    ):
        """Test handling of other unexpected exceptions."""
        # Mock a generic exception
        mock_urlopen.side_effect = Exception("Unexpected error")

        # The function will call get_absolute_userpod_api_url("toolkit/errors")
        # Mock the return value to match the expected format
        mock_get_url.return_value = "http://localhost:19456/userpod-api/toolkit/errors"

        # Call the function
        report_error_to_webapp("TEST_ERROR", "Test error message")

        # Check that the error was logged with warning level
        mock_log_warning.assert_called()  # Base message is always logged
        # Check for the specific warning message
        unexpected_error_message = mock_log_warning.call_args_list[1][0][0]
        self.assertTrue(unexpected_error_message.startswith("Failed"))
        self.assertIn("Failed to report error", unexpected_error_message)
