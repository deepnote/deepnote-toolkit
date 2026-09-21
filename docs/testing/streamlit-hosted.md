# Hosted Streamlit merge gate

Run this on the **final PR commit** in a Deepnote test environment that supports
viewer API-token exchange. Local unit tests and Streamlit AppTest do not verify
platform permissions, storage mounts, or the deployed launcher.

Record the commit, environment, app ID, notebook ID, test date, and results in the
PR before merging. Do not record tokens or cookies.

1. Start the app through the deployed launcher. Confirm the app process sees its
   UUID in `DEEPNOTE_STREAMLIT_APP_ID`.
2. As both owner and a workspace viewer, run a notebook with text and dataframe
   outputs using `StreamlitCloudRunner`. Confirm success, available snapshot, and
   outputs from only the executed notebook.
3. Have the notebook read a known project-storage file. Attempt a write to a
   disposable path on the persistent storage mount and catch the expected
   permission error. Confirm the default readonly run can read but cannot write.
4. Confirm each account cannot fetch the other account's run. An outsider must
   not obtain a viewer token.
5. Repeat with a bogus `DEEPNOTE_TOKEN`, explicit `token=`, and a `token_provider`
   that raises if called. Hosted execution must still use the viewer.
6. Disable project API access, retry, and confirm the server's reason is shown
   without a fallback request. Restore the original project setting afterward.
7. Invoke the same runner from a worker thread. Confirm it raises before HTTP.
8. Stop the app, remove its app ID/cookie/host context in a local AppTest, and
   confirm it fails closed unless local development is explicitly configured.

The readonly literal is defined by the public API's
`DetachedRunStorageModeSchema` in `apps/webapp/server/public-api/v2/contracts/runs.ts`
(`read_write | readonly`) and is also sent by
`apps/webapp-client/src/features/static-files-app-client/connect.ts`.
