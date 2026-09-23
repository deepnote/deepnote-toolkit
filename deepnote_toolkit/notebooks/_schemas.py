"""Consumed fields of the v2 API contracts (contracts/runs.ts and notebooks.ts).

Extra fields are ignored.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, StrictBool, StrictFloat, StrictInt, StrictStr

from .api_types import RunStatus, SnapshotStatus


class ApiInput(BaseModel):
    name: StrictStr
    type: StrictStr
    value: StrictStr | StrictBool | list[StrictStr] | None = None
    label: StrictStr | None = None
    options: list[StrictStr] = Field(default_factory=list)
    multiple: StrictBool = False
    min: StrictInt | StrictFloat | None = None
    max: StrictInt | StrictFloat | None = None
    step: StrictInt | StrictFloat | None = None


class ApiNotebook(BaseModel):
    name: StrictStr = "Untitled notebook"
    inputs: list[ApiInput] = Field(default_factory=list)


class NotebookResponse(BaseModel):
    notebook: ApiNotebook


class ApiRun(BaseModel):
    run_id: StrictStr = Field(alias="runId", min_length=1)
    status: RunStatus
    snapshot_status: SnapshotStatus | None = Field(default=None, alias="snapshotStatus")
    snapshot_blocks: list[dict[str, Any]] | None = Field(
        default=None, alias="snapshotBlocks"
    )
    error: StrictStr | None = None


class GetRunResponse(BaseModel):
    """The response envelope returned when fetching an existing run."""

    run: ApiRun


class ViewerTokenResponse(BaseModel):
    token: StrictStr = Field(min_length=1)
    api_origin: StrictStr = Field(alias="apiOrigin")
    expires_at_seconds: StrictInt | StrictFloat = Field(alias="expiresAtSeconds")
