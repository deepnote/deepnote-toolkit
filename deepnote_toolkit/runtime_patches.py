from typing import Any, Optional, Union

from deepnote_toolkit.logging import LoggerManager

logger = LoggerManager().get_logger()


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
    _monkeypatch_bigquery_wait_or_cancel()
