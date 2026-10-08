"""A Streamlit runtime whose answers a test sets directly."""

from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class FakeStreamlitRuntime:
    request: bool = True
    worker: bool = False
    app: Optional[str] = None
    cookie: Optional[str] = None
    state: Optional[dict[str, Any]] = field(default_factory=dict)

    def has_request(self) -> bool:
        return self.request

    def is_worker_thread(self) -> bool:
        return self.worker

    def app_id(self) -> Optional[str]:
        return self.app

    def viewer_cookie(self) -> Optional[str]:
        return self.cookie

    def session_state(self) -> Optional[dict[str, Any]]:
        return self.state
