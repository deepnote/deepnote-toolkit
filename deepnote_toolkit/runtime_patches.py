from typing import Any, Dict, List, Optional, Union

from deepnote_toolkit.logging import LoggerManager

logger = LoggerManager().get_logger()


def _monkeypatch_trino_cancel_on_error():
    """Monkey patch Trino client to cancel queries on exceptions.

    When a query is running and an exception occurs (including KeyboardInterrupt
    from cell cancellation), the query will continue running on the Trino server
    unless explicitly cancelled. This patch wraps TrinoQuery.execute() and
    TrinoQuery.fetch() to automatically cancel the query when an exception occurs.
    """
    try:
        from trino import client as trino_client

        _original_execute = trino_client.TrinoQuery.execute
        _original_fetch = trino_client.TrinoQuery.fetch

        def _cancel_on_error(query: "trino_client.TrinoQuery") -> None:
            """Best-effort cancel when an error occurs."""
            if not query._cancelled and query._next_uri:
                try:
                    query.cancel()
                except (KeyboardInterrupt, Exception):
                    pass

        def _patched_execute(
            self: "trino_client.TrinoQuery",
            additional_http_headers: Optional[Dict[str, str]] = None,
        ) -> "trino_client.TrinoResult":
            try:
                return _original_execute(self, additional_http_headers)
            except (KeyboardInterrupt, Exception):
                _cancel_on_error(self)
                raise

        def _patched_fetch(
            self: "trino_client.TrinoQuery",
        ) -> List[Union[List[Any], Any]]:
            try:
                return _original_fetch(self)
            except (KeyboardInterrupt, Exception):
                _cancel_on_error(self)
                raise

        trino_client.TrinoQuery.execute = _patched_execute
        trino_client.TrinoQuery.fetch = _patched_fetch
        logger.debug(
            "Successfully monkeypatched trino.client.TrinoQuery.execute and fetch"
        )
    except ImportError:
        logger.warning(
            "Could not monkeypatch Trino cancel on error: trino not available"
        )
    except Exception as e:
        logger.warning("Failed to monkeypatch Trino cancel on error: %s", repr(e))


# TODO(BLU-5171): Temporary hack to allow cancelling BigQuery jobs on KeyboardInterrupt (e.g. when user cancels cell execution)
# Can be removed once
# 1. https://github.com/googleapis/python-bigquery/pull/2331 is merged and released
# 2. Dependencies updated for the toolkit. We don't depend on google-cloud-bigquery directly, but it's transitive
# dependency through sqlalchemy-bigquery
def _monkeypatch_bigquery_wait_or_cancel():
    try:
        import google.cloud.bigquery._job_helpers as _job_helpers
        from google.cloud.bigquery import job, table

        def _wait_or_cancel(
            job_obj: job.QueryJob,
            api_timeout: Optional[float],
            wait_timeout: Optional[Union[object, float]],
            retry: Optional[Any],
            page_size: Optional[int],
            max_results: Optional[int],
        ) -> table.RowIterator:
            try:
                return job_obj.result(
                    page_size=page_size,
                    max_results=max_results,
                    retry=retry,
                    timeout=wait_timeout,
                )
            except (KeyboardInterrupt, Exception):
                try:
                    job_obj.cancel(retry=retry, timeout=api_timeout)
                except (KeyboardInterrupt, Exception):
                    pass
                raise

        _job_helpers._wait_or_cancel = _wait_or_cancel
        logger.debug(
            "Successfully monkeypatched google.cloud.bigquery._job_helpers._wait_or_cancel"
        )
    except ImportError:
        logger.warning(
            "Could not monkeypatch BigQuery _wait_or_cancel: google.cloud.bigquery not available"
        )
    except Exception as e:
        logger.warning("Failed to monkeypatch BigQuery _wait_or_cancel: %s", repr(e))


def apply_runtime_patches():
    _monkeypatch_trino_cancel_on_error()
    _monkeypatch_bigquery_wait_or_cancel()
