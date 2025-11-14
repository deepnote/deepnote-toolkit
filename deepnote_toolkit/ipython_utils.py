from typing import Optional

from IPython import get_ipython

# also defined in https://github.com/deepnote/deepnote/blob/a9f36659f50c84bd85aeba8ee2d3d4458f2f4998/libs/shared/src/constants.ts#L47
DEEPNOTE_SQL_METADATA_MIME_TYPE = "application/vnd.deepnote.sql-output-metadata+json"
DEEPNOTE_EXECUTION_METADATA_MIME_TYPE = (
    "application/vnd.deepnote.execution-metadata+json"
)


def output_display_data(mime_bundle):
    """
    Outputs a display_data MIME bundle, which will be added to the execution's outputs.
    """

    if get_ipython() is not None:
        get_ipython().display_pub.publish(data=mime_bundle)


def output_sql_metadata(metadata: dict):
    """
    Outputs SQL metadata to the notebook. Used for e.g. reporting on hit/miss of a SQL cache. or reporting the compiled query

    Args:
        metadata (dict): A dictionary containing SQL metadata.

    Returns:
        None
    """
    output_display_data({DEEPNOTE_SQL_METADATA_MIME_TYPE: metadata})


def publish_execution_metadata(
    execution_count: int,
    duration: float,
    success: bool,
    error_type: Optional[str] = None,
) -> None:
    """
    Publish execution metadata to the webapp via display_pub.

    This function publishes structured metadata about cell execution that can be
    consumed by the webapp for monitoring, debugging, and analytics purposes.

    Args:
        execution_count (int): The execution count of the cell
        duration (float): Execution duration in seconds
        success (bool): Whether the execution completed successfully
        error_type (str, optional): The type of error if execution failed

    Returns:
        None
    """
    metadata = {
        "execution_count": execution_count,
        "duration_seconds": duration,
        "success": success,
        "timestamp": duration,  # Using duration as timestamp for now
    }

    if error_type:
        metadata["error_type"] = error_type

    output_display_data({DEEPNOTE_EXECUTION_METADATA_MIME_TYPE: metadata})
