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

On Deepnote, `StreamlitCloudRunner` uses the current viewer's permissions.
The hosting environment must support viewer-token exchange. If the app ID,
viewer cookie, or exchange is unavailable, the call fails; it does not fall back
to an owner token. Call it on the Streamlit script thread, not a worker thread.

For a locally hosted Streamlit app, opt into local credentials explicitly:

```python
import os

runner = StreamlitCloudRunner(
    notebook_id, local=True, token=os.environ["DEEPNOTE_TOKEN"]
)
```

`token_provider=` can supply a renewable token instead. A Deepnote app or project
marker overrides `local=True` and explicit tokens. Older launchers can also be
recognized by the request host or viewer cookie. Do not set `local=True` in an
unmarked hosting environment: that is an explicit choice to use local credentials.

For Python code outside Streamlit, use `DeepnoteCloudRunner` from
`deepnote_toolkit.notebooks`. It accepts `token=`, `token_provider=`, or
`DEEPNOTE_TOKEN`. For a local `@deepnote/local-runner` sidecar, use
`DeepnoteLocalRunner(base_url="http://127.0.0.1:8787")`.
`StreamlitCloudRunner` requires explicit local mode and credentials even when
called outside the Streamlit runtime.

## Inputs and outputs

`render_inputs()` preserves saved defaults, including `0` and `False`. A select
without a valid saved choice starts empty. Unselected single selects and incomplete
selections in the date-range picker are omitted from the returned dictionary;
disable your Run button until required fields are present. An omitted input uses
the notebook's value according to the API. Saved open-ended ranges use separate
start/end fields so their chosen endpoint is preserved. Stale multi-select choices
produce a warning. Invalid slider bounds/defaults and duplicate variable names
raise `ValueError`. File inputs render as text paths; this helper does not upload files.

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

Streamlit runs are detached and use `storage_mode="readonly"`: they can read
persistent project files but cannot modify them. Use `storage_mode="read_write"`
only when the app intentionally needs to change those files. The general cloud
runner leaves storage mode to the API.

`timeout` (600 seconds by default) is the elapsed-time budget for creation,
authentication, polling, and output retrieval. Each HTTP request and sleep is
limited to the remaining budget. Output retrieval also has its own
`snapshot_timeout` (10 seconds); only an explicitly pending snapshot is polled.
Requests uses socket timeouts, so OS DNS resolution or a server streaming bytes
can exceed a request budget; this is not hard cancellation of a running notebook.
Run-status GET polls retry transient failures up to five consecutive times;
snapshot GET polls retry within the snapshot budget. Creating a run is never
automatically retried.

Pass `session=requests.Session()` to configure proxies or HTTP adapters. A custom
`credentials=` provider on `DeepnoteCloudRunner` receives a `timeout` keyword and
returns `ApiCredentials(token=..., api_origin=...)`. Providers should honor that
budget. Resolved bearer credentials take precedence over `.netrc` and session
authentication. API clients, HTTP helpers, and wire schemas are internal;
supported names are listed in each package's `__all__`.

The existing `streamlit_data_apps` module handles database federation. Notebook
execution uses its viewer-cookie reader and does not replace its database APIs.
