# Build a Streamlit app from a Deepnote notebook

Install `deepnote-toolkit` and `streamlit`, export your notebook as a `.deepnote`
file, and put it next to your app. Use the same notebook ID when reading inputs
and running the notebook, especially in projects with several notebooks.

```python
import streamlit as st
from deepnote_toolkit.notebooks import DeepnoteDocument, RunnerError
from deepnote_toolkit.streamlit import StreamlitCloudRunner, render_inputs

notebook_id = "your-notebook-id"
document = DeepnoteDocument.load("report.deepnote", notebook_id=notebook_id)
runner = StreamlitCloudRunner(notebook_id)
values = render_inputs(document.inputs, st.sidebar)

if st.button("Run"):
    try:
        result = runner.run(values)
        if not result.success:
            st.error(result.error or "The run failed.")
        elif result.snapshot_status == "pending":
            st.info("The run finished, but its outputs are not available yet.")
        elif (table := result.first_dataframe()) is not None:
            st.dataframe(table.records(include_index=False))
        else:
            st.write(result.text())
    except RunnerError as error:
        st.error(str(error))
```

## Authentication and local development

On Deepnote, `StreamlitCloudRunner` runs the notebook with the current viewer's
permissions. If the app ID, viewer cookie or token exchange is unavailable, the
call fails instead of falling back to an owner token. Call it on the Streamlit
script thread, not a worker thread.

For a locally hosted Streamlit app, opt into local credentials explicitly:

```python
import os

runner = StreamlitCloudRunner(
    notebook_id, local=True, token=os.environ["DEEPNOTE_TOKEN"]
)
```

`token_provider=` can supply a renewable token instead. On Deepnote, `local=True`
and explicit tokens are ignored and the viewer is used.

To call other API endpoints as the viewer, `current_user_api_credentials()`
returns the viewer's short-lived token and the API origin it is valid at. It
raises `CurrentUserApiTokenError` outside a hosted request.

For Python code outside Streamlit, use `DeepnoteCloudRunner` from
`deepnote_toolkit.notebooks`. It accepts `token=`, `token_provider=`, or
`DEEPNOTE_TOKEN`. For a local `@deepnote/local-runner` sidecar, use
`DeepnoteLocalRunner(base_url="http://127.0.0.1:8787")`.

## Inputs and outputs

`render_inputs()` keeps saved defaults, including `0` and `False`. A select
without a valid saved choice starts empty. Unselected single selects and
incomplete date-range selections are left out of the returned dictionary, so
disable your Run button until the required values are present. An omitted input
runs with the notebook's saved value. A saved open-ended date range renders as
separate start and end fields. Stale multi-select choices and slider defaults
outside the bounds are adjusted with a warning. Invalid slider bounds and
duplicate variable names raise `ValueError`. File inputs render as text paths;
this helper does not upload files.

`runner.info().matches_inputs(document.inputs)` compares static input definitions:
unique names, types, single/multiple selection, options, and slider bounds/steps.
It is a drift check, not a guarantee that every submitted value will be accepted.
Options populated from a variable cannot be checked against the saved file.

Cloud results contain outputs from the executed notebook. `result.text()` returns
text and `result.first_dataframe()` returns the first table, if present. Tables
contain a preview page; check `row_count` and `is_truncated` before treating them as
complete data. Non-numeric cells, including booleans, can arrive as strings.
`DeepnoteDocument.load("report.snapshot.deepnote")` can display saved outputs
without network access.

## Execution settings

Streamlit runs use `storage_mode="readonly"`: the notebook can read the project's
files but not change them. Pass `storage_mode="read_write"` when the app needs to
write them. `DeepnoteCloudRunner` leaves the choice to the API.

`timeout` (600 seconds by default) bounds the whole run, from creating it to
reading its outputs. Outputs can arrive after the run finishes; `snapshot_timeout`
(10 seconds) is how long to wait for them, and a result whose `snapshot_status` is
still `pending` has none.

Pass `session=requests.Session()` to configure proxies or HTTP adapters. On
`DeepnoteCloudRunner`, `credentials=` accepts any callable that takes a `timeout`
keyword and returns `ApiCredentials(token=..., api_origin=...)`. Supported names
are listed in each package's `__all__`.
