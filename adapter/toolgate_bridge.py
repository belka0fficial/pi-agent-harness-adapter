from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any


def _toolgate_root() -> Path:
    return Path(__file__).resolve().parents[2] / "toolgate"


def _ensure_toolgate_import_path() -> None:
    root = _toolgate_root()
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))


class ToolGateBridge:
    def __init__(self, container_name: str = "toolgate-api") -> None:
        self.container_name = container_name
        self._control_plane = None

    def _cp(self):
        if self._control_plane is None:
            _ensure_toolgate_import_path()
            from toolgate.core import control_plane

            self._control_plane = control_plane
        return self._control_plane

    def _container_call(self, action: str, payload: dict[str, Any]) -> dict[str, Any] | None:
        script = (
            "import json\n"
            "from toolgate.core import control_plane\n"
            f"payload = json.loads({json.dumps(json.dumps(payload))})\n"
            "result = None\n"
            f"if {json.dumps(action)} == 'get':\n"
            "    result = control_plane.get('request', payload['request_id'])\n"
            "elif "
            f"{json.dumps(action)} == 'decide':\n"
            "    result = control_plane.decide_request(payload['request_id'], payload['status'], 'pi-adapter', payload.get('note', ''))\n"
            "print(json.dumps(result))\n"
        )
        result = subprocess.run(
            ["docker", "exec", "-i", self.container_name, "python3", "-c", script],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
        value = result.stdout.strip()
        return json.loads(value) if value else None

    def get_request(self, request_id: str) -> dict[str, Any] | None:
        try:
            record = self._container_call("get", {"request_id": request_id})
            return record if isinstance(record, dict) else None
        except (OSError, subprocess.SubprocessError, json.JSONDecodeError):
            record = self._cp().get("request", request_id)
            return record if isinstance(record, dict) else None

    def decide_request(self, request_id: str, decision: str, note: str = "") -> dict[str, Any]:
        status = "approved" if decision == "approved" else "rejected"
        try:
            record = self._container_call("decide", {"request_id": request_id, "status": status, "note": note})
        except (OSError, subprocess.SubprocessError, json.JSONDecodeError):
            record = self._cp().decide_request(request_id, status, "pi-adapter", note)
        if not record:
            raise ValueError("request not found")
        return record
