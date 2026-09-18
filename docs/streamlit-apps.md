# Build Streamlit apps from Deepnote notebooks

Deepnote Toolkit provides a small typed layer for custom Streamlit apps backed by
`.deepnote` source files and snapshots.

```python
from pathlib import Path

import streamlit as st
from deepnote_toolkit.notebooks import DeepnoteDocument
from deepnote_toolkit.streamlit import StreamlitCloudRunner, render_inputs

document = DeepnoteDocument.load(Path("report.deepnote"))
values = render_inputs(document.inputs, st.sidebar)

if st.button("Run"):
    result = StreamlitCloudRunner("your-notebook-id").run(values)
    st.dataframe(result.first_dataframe().records())
```

## Two packages

`deepnote_toolkit.notebooks` has no Streamlit dependency and works in any Python
program:

- `DeepnoteDocument` reads typed input definitions and structured outputs from a
  `.deepnote` source or snapshot file. In a project with several notebooks, pass
  the notebook you run so the inputs match it:
  `DeepnoteDocument.load(path, notebook_id="your-notebook-id")`.
- `DeepnoteCloudRunner` runs an existing notebook in Deepnote Cloud and returns
  its outputs as a `RunResult`.
- `DeepnoteRunner` does the same through a local `@deepnote/local-runner` sidecar
  at `http://127.0.0.1:8787`.
- `Runner` is the interface both runners implement, for code that accepts either.

`deepnote_toolkit.streamlit` holds the Streamlit-specific parts:

- `render_inputs` maps Deepnote input blocks to native Streamlit widgets and
  returns values ready to submit to a runner.
- `StreamlitCloudRunner` is a `DeepnoteCloudRunner` that runs notebooks as the
  person viewing the app when Deepnote hosts it.

A static app only loads a committed snapshot with `DeepnoteDocument`. It requires
no token or network access.

## Authentication

A Streamlit app hosted by Deepnote needs no token configuration.
`StreamlitCloudRunner` runs the notebook as the current viewer, with that viewer's
access, and never as the app's owner. A viewer who loses access to the project
can no longer run it.

Call the runner from the Streamlit script thread. A worker thread has no viewer
request, so the runner raises there instead of using `DEEPNOTE_TOKEN`.

For local development, pass an API token explicitly or set `DEEPNOTE_TOKEN`:

```python
runner = StreamlitCloudRunner("your-notebook-id", token="your-api-token")
```

A callable `token_provider=` can supply a renewable token. It is invoked for every
request.

For another Deepnote API client inside a hosted app,
`current_user_api_credentials()` returns a short-lived token for the current
viewer together with the API origin to send it to.

## Runs

Cloud runs are detached, which keeps viewer-triggered work out of the shared
project session.

The cloud runner retries a poll that fails with a timeout, a network error, HTTP
429 or a 5xx, up to five times in a row. After the run finishes it waits briefly
for the outputs, which can arrive after the final status.

Use `runner.info().accepts_inputs(document.inputs)` before submitting values to
verify that the deployed notebook still has matching input names and block types.
