import warnings

# Ignore warning coming from here:
# https://github.com/jupyter/jupyter_client/pull/708/files#diff-f89a7d6e5a8a1910d98e3a31ae8ff90c4d745d16bcef4a11d969e5459dcb7e50R112-R117
warnings.filterwarnings(
    "ignore",
    message=r"[\S\s]*Supporting this message is deprecated in jupyter-client 7, please make sure your message is JSON-compliant[\S\s]*",
)
