"""Runtime action specifications for server orchestration."""

from typing import Annotated, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, Field


class JupyterServerSpec(BaseModel):
    """Specification for Jupyter server instance."""

    class Config:
        extra = "forbid"

    kind: Literal["jupyter"] = "jupyter"
    host: str = Field(default="0.0.0.0", description="Server bind address")
    port: int = Field(ge=1, le=65535, description="Server port")
    no_browser: bool = Field(default=True, description="Disable browser auto-open")
    allow_root: bool = Field(default=False, description="Allow root execution")
    enable_terminals: bool = Field(default=True, description="Enable terminal support")
    extra_args: List[str] = Field(
        default_factory=list, description="Additional arguments"
    )


class PythonLSPSpec(BaseModel):
    """Specification for Python Language Server."""

    class Config:
        extra = "forbid"

    kind: Literal["lsp"] = "lsp"
    host: str = Field(default="0.0.0.0", description="Server bind address")
    port: int = Field(ge=1, le=65535, description="Server port")
    verbose: bool = Field(default=True, description="Enable verbose logging")


class StreamlitSpec(BaseModel):
    """Specification for Streamlit application."""

    class Config:
        extra = "forbid"

    kind: Literal["streamlit"] = "streamlit"
    script: str = Field(min_length=1, description="Path to Streamlit script")
    port: Optional[int] = Field(
        default=None, ge=1, le=65535, description="Server port (auto if None)"
    )
    args: List[str] = Field(default_factory=list, description="Script arguments")


class ExtraServerSpec(BaseModel):
    """Specification for custom server processes."""

    class Config:
        extra = "forbid"

    kind: Literal["extra"] = "extra"
    command: List[str] = Field(description="Command and arguments")
    env: Dict[str, str] = Field(
        default_factory=dict, description="Environment variables"
    )


class EnableJupyterTerminalsAction(BaseModel):
    """Action to enable Jupyter terminals extension."""

    class Config:
        extra = "forbid"

    kind: Literal["enable_jupyter_terminals"] = "enable_jupyter_terminals"


RuntimeAction = Annotated[
    Union[
        JupyterServerSpec,
        PythonLSPSpec,
        StreamlitSpec,
        ExtraServerSpec,
        EnableJupyterTerminalsAction,
    ],
    Field(discriminator="kind"),
]
