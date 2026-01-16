#!/usr/bin/env python3
"""Lightweight resource metrics server."""

import json
import logging
import os
import threading
import time
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Optional

try:
    import psutil

    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False


class ResourceMonitor:
    """Monitors system resources via cgroups or psutil."""

    def __init__(self, root_path: str = "/sys/fs/cgroup") -> None:
        self.root = Path(root_path)
        self.backend = self._detect_backend()
        self._last_cpu_sec: Optional[float] = None
        self._last_time: Optional[float] = None
        self._lock = threading.Lock()

    def _detect_backend(self) -> str:
        if (self.root / "cgroup.controllers").exists():
            return "cgroupv2"
        if (self.root / "memory/memory.limit_in_bytes").exists():
            return "cgroupv1"
        if HAS_PSUTIL:
            return "psutil"
        return "none"

    def _read_file(self, path: Path) -> Optional[str]:
        try:
            return path.read_text().strip()
        except (FileNotFoundError, PermissionError, OSError):
            return None

    def get_memory(self) -> tuple[int, Optional[int]]:
        """Returns (used_bytes, limit_bytes). Limit is None if unlimited."""
        if self.backend == "cgroupv2":
            current = self._parse_int(self.root / "memory.current", 0)
            inactive_file = self._parse_memory_stat("inactive_file")
            used = current - inactive_file
            limit_str = self._read_file(self.root / "memory.max")
            limit = self._parse_limit(limit_str, "max")
            return used, limit

        if self.backend == "cgroupv1":
            used = self._parse_int(self.root / "memory/memory.usage_in_bytes", 0)
            limit_str = self._read_file(self.root / "memory/memory.limit_in_bytes")
            limit = self._parse_limit(limit_str, threshold=1_000_000_000_000_000)
            return used, limit

        if self.backend == "psutil":
            proc = psutil.Process()
            return proc.memory_info().rss, psutil.virtual_memory().total

        return 0, None

    def get_cpu(self) -> tuple[Optional[float], Optional[float]]:
        """Returns (usage_percent, limit_cores)."""
        usage_sec = self._get_cpu_seconds()
        percent = self._calc_percent(usage_sec)
        limit = self._get_cpu_limit()
        return percent, limit

    def _parse_int(self, path: Path, default: int = 0) -> int:
        content = self._read_file(path)
        if content:
            try:
                return int(content)
            except ValueError:
                pass
        return default

    def _parse_limit(
        self,
        value: Optional[str],
        unlimited_marker: Optional[str] = None,
        threshold: Optional[int] = None,
    ) -> Optional[int]:
        if not value:
            return None
        if unlimited_marker and value == unlimited_marker:
            return None
        try:
            parsed = int(value)
            if threshold and parsed >= threshold:
                return None
            return parsed
        except ValueError:
            return None

    def _parse_memory_stat(self, key: str) -> int:
        """Parse a value from memory.stat file."""
        content = self._read_file(self.root / "memory.stat")
        if not content:
            return 0
        for line in content.splitlines():
            if line.startswith(key):
                parts = line.split()
                if len(parts) >= 2:
                    try:
                        return int(parts[1])
                    except ValueError:
                        pass
        return 0

    def _get_cpu_seconds(self) -> float:
        if self.backend == "cgroupv2":
            content = self._read_file(self.root / "cpu.stat")
            if content:
                for line in content.splitlines():
                    if line.startswith("usage_usec"):
                        parts = line.split()
                        if len(parts) >= 2:
                            return int(parts[1]) / 1_000_000.0
            return 0.0

        if self.backend == "cgroupv1":
            content = self._read_file(self.root / "cpuacct/cpuacct.usage")
            if content:
                return int(content) / 1_000_000_000.0
            return 0.0

        if self.backend == "psutil":
            times = psutil.Process().cpu_times()
            return times.user + times.system

        return 0.0

    def _get_cpu_limit(self) -> Optional[float]:
        if self.backend == "cgroupv2":
            content = self._read_file(self.root / "cpu.max")
            if not content:
                return None
            parts = content.split()
            if len(parts) < 2 or parts[0] == "max":
                return None
            try:
                return int(parts[0]) / int(parts[1])
            except (ValueError, ZeroDivisionError):
                return None

        if self.backend == "cgroupv1":
            quota = self._parse_int(self.root / "cpu/cpu.cfs_quota_us", -1)
            period = self._parse_int(self.root / "cpu/cpu.cfs_period_us", 0)
            if quota > 0 and period > 0:
                return quota / period
            return None

        if self.backend == "psutil":
            cpu_count = psutil.cpu_count(logical=True)
            return float(cpu_count) if cpu_count is not None else None

        return None

    def _calc_percent(self, current_sec: float) -> Optional[float]:
        now = time.monotonic()
        with self._lock:
            if self._last_cpu_sec is None or self._last_time is None:
                self._last_cpu_sec = current_sec
                self._last_time = now
                return None

            time_delta = now - self._last_time
            cpu_delta = current_sec - self._last_cpu_sec
            self._last_cpu_sec = current_sec
            self._last_time = now

        if time_delta <= 0:
            return 0.0
        return (cpu_delta / time_delta) * 100.0


class MetricsHandler(BaseHTTPRequestHandler):
    """HTTP handler for resource metrics."""

    def __init__(self, monitor: ResourceMonitor, *args: Any, **kwargs: Any) -> None:
        self.monitor = monitor
        super().__init__(*args, **kwargs)

    def do_GET(self) -> None:
        if self.path in ("/", "/resource-usage"):
            self._send_metrics()
        elif self.path == "/health":
            self._send_text(200, "ok")
        else:
            self.send_error(404)

    def _send_metrics(self) -> None:
        mem_used, mem_limit = self.monitor.get_memory()
        cpu_percent, cpu_limit = self.monitor.get_cpu()

        env_limit = os.environ.get("MEM_LIMIT")
        if env_limit:
            try:
                mem_limit = int(env_limit)
            except ValueError:
                pass

        mem_util = None
        if mem_limit and mem_limit > 0:
            mem_util = round((mem_used / mem_limit) * 100, 2)

        cpu_sat = None
        if cpu_percent is not None and cpu_limit:
            cpu_sat = round(cpu_percent / cpu_limit, 2)

        data = {
            "meta": {"backend": self.monitor.backend, "timestamp": time.time()},
            "memory": {
                "used_bytes": mem_used,
                "limit_bytes": mem_limit,
                "usage_percent": mem_util,
            },
            "cpu": {
                "usage_percent": (
                    round(cpu_percent, 2) if cpu_percent is not None else None
                ),
                "limit_cores": cpu_limit,
                "saturation_percent": cpu_sat,
            },
        }
        self._send_json(200, data)

    def _send_json(self, code: int, data: dict[str, Any]) -> None:
        body = json.dumps(data, indent=2).encode()
        self._send_response(code, body, "application/json")

    def _send_text(self, code: int, text: str) -> None:
        self._send_response(code, text.encode(), "text/plain")

    def _send_response(self, code: int, body: bytes, content_type: str) -> None:
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, msg_format: str, *args: Any) -> None:
        logging.info(f"{self.address_string()} - {msg_format % args}")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    port = int(os.environ.get("RESOURCE_USAGE_METRICS_PORT", 9104))
    monitor = ResourceMonitor()
    monitor.get_cpu()  # Initialize CPU baseline

    handler = partial(MetricsHandler, monitor)
    server = ThreadingHTTPServer(("0.0.0.0", port), handler)

    logging.info(f"Starting server on port {port} (backend: {monitor.backend})")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logging.info("Shutting down...")
        server.server_close()


if __name__ == "__main__":
    main()
