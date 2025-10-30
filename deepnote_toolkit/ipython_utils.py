from IPython import get_ipython

# also defined in https://github.com/deepnote/deepnote/blob/a9f36659f50c84bd85aeba8ee2d3d4458f2f4998/libs/shared/src/constants.ts#L47
DEEPNOTE_SQL_METADATA_MIME_TYPE = "application/vnd.deepnote.sql-output-metadata+json"


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
