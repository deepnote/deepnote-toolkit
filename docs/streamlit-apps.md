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
`render_inputs` maps Deepnote input blocks to native Streamlit widgets.
`DeepnoteCloudRunner` calls the same public notebooks and runs API used by the
Deepnote CLI. `DeepnoteRunner` is available for the local-runner sidecar.

## Authentication modes

A hosted Deepnote Streamlit app needs no token configuration. For each API request,
the cloud runner:

1. reads the current viewer's opaque `streamlit-token` cookie;
2. resolves the app ID from `x-original-host`, falling back to `host`;
3. exchanges the cookie at
   `POST /api/streamlit-apps/{appId}/api-token`; and
4. calls the returned `apiOrigin` with the short-lived token as a bearer.

The opaque cookie is never sent to the public API. Credentials are not cached in
process globals or Streamlit session state, and a hosted request never falls back to
a shared environment token.

The exchange endpoint must return `token`, `apiOrigin`, and
`expiresAtSeconds`. Deployments must provide `DEEPNOTE_WEBAPP_URL` through the
Toolkit runtime configuration.

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
