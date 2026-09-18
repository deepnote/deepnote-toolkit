# Build Streamlit apps from Deepnote notebooks

Deepnote Toolkit provides a small typed layer for custom Streamlit apps backed by
`.deepnote` source files and snapshots.

```python
from pathlib import Path

import streamlit as st
from deepnote_toolkit.streamlit import (
    DeepnoteCloudRunner,
    DeepnoteDocument,
    render_inputs,
)

document = DeepnoteDocument.load(Path("report.deepnote"))
values = render_inputs(document.inputs, st.sidebar)

if st.button("Run"):
    result = DeepnoteCloudRunner("your-notebook-id").run(values)
    st.dataframe(result.first_dataframe().records())
```

`DeepnoteDocument` reads typed input definitions and structured notebook outputs.
In a project with several notebooks, pass the notebook you run so the inputs match
it: `DeepnoteDocument.load(path, notebook_id="your-notebook-id")`.
`render_inputs` maps Deepnote input blocks to native Streamlit widgets.
`DeepnoteCloudRunner` calls the same public notebooks and runs API used by the
Deepnote CLI. `DeepnoteRunner` is available for the local-runner sidecar.

## Authentication modes

A hosted Deepnote Streamlit app needs no token configuration. The cloud runner:

1. reads the current viewer's opaque `streamlit-token` cookie;
2. resolves the app ID from `x-original-host`, falling back to `host`;
3. exchanges the cookie through the project's userpod API at
   `POST /userpod-api/streamlit-apps/{appId}/api-token`; and
4. calls the returned `apiOrigin` with the short-lived token as a bearer.

Cloud runs explicitly request `detached: true`, keeping viewer-triggered work out
of the shared project session. Hosted app tokens receive sanitized
`snapshotBlocks` containing the executed notebook's outputs, not the raw
project snapshot. API-key clients remain compatible with inline
`snapshotContent` responses.

The opaque cookie is never sent to the public API. The exchanged credentials are kept
in the viewer's own Streamlit session state and reused until a minute before they
expire. They are never kept in process globals or shared between sessions, and a
hosted request never falls back to a shared environment token. Call the runner
from the Streamlit script thread: a worker thread has no viewer request, so the
runner raises instead of using `DEEPNOTE_TOKEN`. Deepnote rechecks
the viewer's access on every API request, so a reused bearer stops working as soon
as access is revoked.

The exchange endpoint must return `token`, `apiOrigin`, and
`expiresAtSeconds`. The request goes through the same userpod API route as the
Toolkit's other webapp calls, so a hosted app needs no extra configuration.

For another public API client, use both values returned by
`current_user_api_credentials()`. `current_user_api_token()` is a token-provider
convenience for clients whose API origin is configured separately.

For local development, pass a user's API token explicitly or set
`DEEPNOTE_TOKEN`:

```python
runner = DeepnoteCloudRunner("your-notebook-id", token="your-api-token")
```

A callable `token_provider=` can supply a renewable token. It is invoked for every
request. `DeepnoteRunner` can instead call a local `@deepnote/local-runner`
sidecar at `http://127.0.0.1:8787`.

Static apps only load a committed snapshot with `DeepnoteDocument`; they require
no token or network access.

## Synchronize at deployment

Runtime requests only read and run the existing cloud notebook. Synchronize source
in an explicit deployment step:

```bash
deepnote run report.deepnote --cloud --notebook-id "$DEEPNOTE_NOTEBOOK_ID" --push --dry-run
deepnote run report.deepnote --cloud --notebook-id "$DEEPNOTE_NOTEBOOK_ID" --push --yes
```

Use `RunnerInfo.accepts_inputs(document.inputs)` before submitting values to
verify that the deployed notebook still has matching input names and block types.

The cloud runner retries a poll that fails with a timeout, a network error, HTTP
429 or a 5xx, up to five times in a row. After the run finishes it waits briefly
for the outputs, which can arrive after the final status.
