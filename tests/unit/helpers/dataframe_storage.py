"""Frames and execute requests shared by the DataFrame storage tests."""

import io
import uuid
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

NAME = "revenue"
BLOCK_ID = "blk1"
REPORT_URL = "http://userpod/toolkit/errors"


def probe_failure(frame: Any) -> Optional[type]:
    """The exception type pandas itself raises for this frame, if any."""
    try:
        frame.to_feather(io.BytesIO())
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return type(exc)
    return None


def dictionary_concat() -> pd.DataFrame:
    dictionary_type = pd.ArrowDtype(pa.dictionary(pa.int32(), pa.string()))
    first = pd.DataFrame({"c": pd.Series(["x", "y"], dtype=dictionary_type)})
    second = pd.DataFrame({"c": pd.Series(["z", "w"], dtype=dictionary_type)})
    return pd.concat([first, second])


ALWAYS_FAILING = [
    pytest.param(lambda: pd.DataFrame({"a": [1, "two"]}), id="mixed-types"),
    pytest.param(
        lambda: pd.DataFrame([[1, 2]], columns=["a", "a"]), id="duplicate-labels"
    ),
    pytest.param(lambda: pd.DataFrame({"a": [2**70]}), id="beyond-int64"),
    pytest.param(lambda: pd.DataFrame({"a": np.array([1 + 2j])}), id="complex"),
    pytest.param(lambda: pd.DataFrame({"a": ["\ud800"]}), id="lone-surrogate"),
]
VERSION_DEPENDENT = [
    pytest.param(lambda: pd.DataFrame({"a": [uuid.UUID(int=1)]}), 1, id="uuid"),
    pytest.param(dictionary_concat, 4, id="dictionary-concat"),
]


def storage_request(**overrides: Any) -> Dict[str, Any]:
    """Return the request message that asks the kernel to store a block's result.

    `overrides` replace fields of `deepnote.dataframeStorage`.
    """
    return {
        "metadata": {
            "deepnote": {
                "dataframeStorage": {"name": NAME, "blockId": BLOCK_ID, **overrides}
            }
        }
    }
