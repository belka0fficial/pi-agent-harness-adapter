from __future__ import annotations

import json
import os
import re
import urllib.error
import urllib.request
from typing import Any


def _mcp_tool_name(tool_id: str, all_ids: list[str] | None = None) -> str:
    if os.environ.get("TOOLGATE_MCP_PRESERVE_IDS") == "1":
        return tool_id
    name = re.sub(r"[^A-Za-z0-9_-]", "_", tool_id).strip("_") or "toolgate_tool"
    if not re.match(r"^[A-Za-z_]", name):
        name = f"tool_{name}"
    name = name[:64]
    if all_ids:
        collisions = [item for item in all_ids if re.sub(r"[^A-Za-z0-9_-]", "_", item).strip("_")[:64] == name]
        if len(collisions) > 1:
            name = f"{name[:55]}_{abs(hash(tool_id)) % (10 ** 8):08d}"
    return name


class ToolGateBridge:
    def __init__(
        self,
        base_url: str | None = None,
        *,
        admin_key: str | None = None,
        execution_key: str | None = None,
    ) -> None:
        self.base_url = (base_url or os.environ.get("TOOLGATE_URL", "http://toolgate-api:8010")).rstrip("/")
        self.admin_key = admin_key or os.environ.get("TOOLGATE_ADMIN_KEY", "")
        self.execution_key = execution_key or os.environ.get("TOOLGATE_EXECUTION_KEY", "")

    def _request(
        self,
        method: str,
        path: str,
        *,
        payload: dict[str, Any] | None = None,
        admin: bool = False,
        agent: bool = False,
    ) -> Any:
        headers = {"Accept": "application/json"}
        data = None
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        if admin:
            if not self.admin_key:
                raise ValueError("TOOLGATE_ADMIN_KEY is not configured")
            headers["X-ToolGate-Key"] = self.admin_key
        if agent:
            if not self.execution_key:
                raise ValueError("TOOLGATE_EXECUTION_KEY is not configured")
            headers["X-ToolGate-Execution-Key"] = self.execution_key
        request = urllib.request.Request(f"{self.base_url}{path}", data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                body = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(detail or f"ToolGate request failed with status {exc.code}") from exc
        except OSError as exc:
            raise RuntimeError(f"ToolGate request failed: {exc}") from exc
        if not body:
            return None
        return json.loads(body)

    def list_active_tools(self) -> list[dict[str, Any]]:
        tools = self._request("GET", "/v2/agent/tools", agent=True)
        return list(tools) if isinstance(tools, list) else []

    def resolve_tool(self, mcp_name: str) -> dict[str, Any] | None:
        tools = self.list_active_tools()
        all_ids = [str(tool.get("id") or "") for tool in tools]
        for tool in tools:
            tool_id = str(tool.get("id") or "")
            if tool_id == mcp_name or _mcp_tool_name(tool_id, all_ids) == mcp_name:
                return tool
        return None

    def invoke_tool(self, tool_id: str, arguments: dict[str, Any]) -> dict[str, Any]:
        args = dict(arguments)
        approval_request_id = args.pop("approval_request_id", None)
        payload: dict[str, Any] = {"args": args}
        if approval_request_id:
            payload["approval_request_id"] = approval_request_id
        value = self._request("POST", f"/v2/tools/{tool_id}/invoke", payload=payload, agent=True)
        return value if isinstance(value, dict) else {"code": "REQUEST_FAILED", "message": "ToolGate response was not an object"}

    def get_request(self, request_id: str) -> dict[str, Any] | None:
        requests = self._request("GET", "/v2/requests", admin=True)
        if not isinstance(requests, list):
            return None
        for record in requests:
            if isinstance(record, dict) and record.get("id") == request_id:
                return record
        return None

    def decide_request(self, request_id: str, decision: str, note: str = "") -> dict[str, Any]:
        status = "approved" if decision == "approved" else "rejected"
        value = self._request(
            "POST",
            f"/v2/requests/{request_id}/decision",
            payload={"status": status, "note": note},
            admin=True,
        )
        if not isinstance(value, dict):
            raise ValueError("request not found")
        return value
