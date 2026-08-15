from __future__ import annotations

import json
import os
import re
import sys
from typing import Any

from .toolgate_bridge import ToolGateBridge

SERVER_NAME = "toolgate-http"
SERVER_VERSION = "0.1.0"


def _json_type(field_type: str) -> str:
    return field_type if field_type in {"string", "integer", "number", "boolean", "array", "object"} else "string"


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


def _schema_for_field(field: dict[str, Any]) -> dict[str, Any]:
    schema: dict[str, Any] = {"type": _json_type(str(field.get("type", "string")))}
    if field.get("description"):
        schema["description"] = field["description"]
    if "default" in field:
        schema["default"] = field["default"]
    if field.get("allowed_values"):
        schema["enum"] = list(field["allowed_values"])
    if schema["type"] == "string":
        if field.get("min_length") is not None:
            schema["minLength"] = field["min_length"]
        if field.get("max_length") is not None:
            schema["maxLength"] = field["max_length"]
        if field.get("pattern"):
            schema["pattern"] = field["pattern"]
    if schema["type"] in {"integer", "number"}:
        if field.get("minimum") is not None:
            schema["minimum"] = field["minimum"]
        if field.get("maximum") is not None:
            schema["maximum"] = field["maximum"]
    if schema["type"] == "array":
        item_schema: dict[str, Any] = {}
        if field.get("item_type"):
            item_schema["type"] = _json_type(str(field["item_type"]))
        if field.get("item_pattern"):
            item_schema["pattern"] = field["item_pattern"]
        if item_schema:
            schema["items"] = item_schema
        if field.get("min_items") is not None:
            schema["minItems"] = field["min_items"]
        if field.get("max_items") is not None:
            schema["maxItems"] = field["max_items"]
        if field.get("unique_items") is not None:
            schema["uniqueItems"] = bool(field["unique_items"])
    return schema


def _tool_input_schema(tool: dict[str, Any]) -> dict[str, Any]:
    properties: dict[str, Any] = {}
    required: list[str] = []
    for field in tool.get("inputs", []):
        name = field.get("name")
        if not name:
            continue
        properties[name] = _schema_for_field(field)
        if field.get("required"):
            required.append(name)
    properties["approval_request_id"] = {
        "type": "string",
        "description": "Optional ToolGate approval request id for retrying an owner-approved action.",
    }
    schema: dict[str, Any] = {"type": "object", "properties": properties, "additionalProperties": False}
    if required:
        schema["required"] = required
    return schema


class MCPServer:
    def __init__(self, bridge: ToolGateBridge | None = None) -> None:
        self.bridge = bridge or ToolGateBridge()

    def list_tools(self) -> list[dict[str, Any]]:
        visible = self.bridge.list_active_tools()
        all_ids = [str(tool["id"]) for tool in visible if tool.get("id")]
        tools = []
        for tool in visible:
            tool_id = str(tool.get("id") or "")
            description = tool.get("description") or f"Invoke ToolGate tool '{tool_id}'."
            if tool.get("authorization") == "owner_confirmation":
                description += " Owner approval may be required."
            description += f" ToolGate id: {tool_id}."
            tools.append(
                {
                    "name": _mcp_tool_name(tool_id, all_ids),
                    "description": description,
                    "inputSchema": _tool_input_schema(tool),
                }
            )
        tools.append(
            {
                "name": "toolgate_request_status",
                "description": "Check the status of a ToolGate request or approval.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "request_id": {"type": "string", "description": "The ToolGate request id to inspect."},
                    },
                    "required": ["request_id"],
                    "additionalProperties": False,
                },
            }
        )
        return tools

    def invoke(self, tool_name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        if tool_name == "toolgate_request_status":
            request_id = str(arguments["request_id"])
            value = self.bridge.get_request(request_id)
            if not value:
                raise RuntimeError(f"ToolGate request '{request_id}' was not found")
            return value
        tool = self.bridge.resolve_tool(tool_name)
        if not tool:
            raise RuntimeError(f"Tool '{tool_name}' was not found")
        value = self.bridge.invoke_tool(str(tool["id"]), arguments)
        if isinstance(value, dict):
            return value
        raise RuntimeError("ToolGate response was not an object")


def respond(message_id: Any, result: Any | None = None, error: Exception | str | None = None) -> None:
    body = {"jsonrpc": "2.0", "id": message_id}
    if error is None:
        body["result"] = result
    else:
        body["error"] = {"code": -32000, "message": str(error)}
    print(json.dumps(body), flush=True)


def main() -> int:
    server = MCPServer()
    for line in sys.stdin:
        request = None
        try:
            request = json.loads(line)
            method = request.get("method")
            params = request.get("params", {})
            if method == "initialize":
                respond(
                    request.get("id"),
                    {
                        "protocolVersion": params.get("protocolVersion", "2025-06-18"),
                        "capabilities": {"tools": {}},
                        "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
                    },
                )
                continue
            if method == "tools/list":
                respond(request.get("id"), {"tools": server.list_tools()})
                continue
            if method == "tools/call":
                name = params.get("name")
                if not name:
                    raise RuntimeError("tool name is required")
                value = server.invoke(str(name), params.get("arguments", {}))
                respond(request.get("id"), {"content": [{"type": "text", "text": json.dumps(value)}]})
                continue
            if "id" in request:
                respond(request.get("id"), {})
        except Exception as exc:  # pragma: no cover - protocol boundary
            respond(request.get("id") if isinstance(request, dict) else None, error=exc)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
