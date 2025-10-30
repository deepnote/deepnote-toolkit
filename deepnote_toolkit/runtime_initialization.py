""" This module contains functions to set up the Deepnote runtime environment. """

import builtins

import IPython.core.page
import psycopg2.extensions
import psycopg2.extras

from .dataframe_utils import add_formatters
from .execute_post_start_hooks import execute_post_start_hooks
from .logging import LoggerManager
from .output_middleware import add_output_middleware
from .set_integrations_env import set_integration_env
from .set_notebook_path import set_notebook_path
from .sql.spark_sql_magic import SparkSql


def init_deepnote_runtime():
    """
    This function initializes the runtime environment for Deepnote.
    """

    logger = LoggerManager().get_logger()

    logger.debug("Initializing Deepnote runtime environment started.")

    # Register sparksql magic
    try:
        IPython.get_ipython().register_magics(SparkSql)
    except Exception as e:
        logger.error("Failed to register sparksql magic with a error: %s", e)

    # Apply custom formatters for DataFrames
    try:
        logger.debug("Adding custom formatters for DataFrames.")
        add_formatters()
    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error("Failed to load custom dataFrame formatter with a error: %s", e)

    # Add output middleware
    try:
        logger.debug("Adding output middleware.")
        add_output_middleware()
    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error("Failed to add output middleware with a error: %s", e)

    # Set up psycopg2 to make long-running queries interruptible by SIGINT (interrupt kernel)
    try:
        logger.debug("Setting psycopg2.")
        psycopg2.extensions.set_wait_callback(psycopg2.extras.wait_select)
    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error("Failed to set psycopg2 with error: %s", e)

    # Set up IPython page printer
    try:
        logger.debug("Setting IPython page printer.")
        set_ipython_page_printer()
    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error("Failed to set IPython page printer with error: %s", e)

    # Set the notebook kernel's working directory
    try:
        logger.debug("Setting kernel working directory.")
        set_notebook_path()
    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error("Failed to set kernel working directory with error: %s", e)

    try:
        logger.debug("Setting integration environment.")
        set_integration_env()
    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error("Failed to set integration environment with error: %s", e)

    try:
        logger.debug("Executing post start hooks.")
        execute_post_start_hooks()
    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error("Failed to execute post start hooks with error: %s", e)

    logger.debug("Initializing Deepnote runtime environment finished.")


def set_ipython_page_printer():
    """
    This code changes how IPython handles the display of specific output, e.g. when running some ipython magic command such as `print?`.
    More info here: https://stackoverflow.com/questions/53498226/what-is-the-meaning-of-exclamation-and-question-marks-in-jupyter-notebook

    Instead of the default behavior (which in Jupyter opens a sidebar UI), it just prints the entire content directly to the console.
    """

    def page_printer(data, start=0, screen_lines=0, pager_cmd=None):
        if builtins.isinstance(data, builtins.dict):
            data = data["text/plain"]
        builtins.print(data)

    IPython.core.page.page = page_printer
