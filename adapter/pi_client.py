from __future__ import annotations

import asyncio
import json
import os
import re
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any


@dataclass
class PiEvent:
    event: str
    data: dict[str, Any]


def _pi_text(message: dict[str, Any]) -> str:
    chunks: list[str] = []
    for item in message.get("content", []):
        if item.get("type") == "text" and item.get("text"):
            chunks.append(str(item["text"]))
    return "".join(chunks)


def _thinking_level(options: dict[str, Any] | None) -> str | None:
    if not options:
        return None
    model_options = options.get("model_options")
    if isinstance(model_options, dict):
        effort = model_options.get("reasoning_effort")
        if isinstance(effort, str) and effort:
            return effort
    return None


def _build_command(
    command: str,
    prompt: str,
    *,
    session_id: str,
    options: dict[str, Any] | None,
) -> list[str]:
    args = [command, "--mode", "json", "--approve", "--session-id", _pi_session_id(session_id)]
    default_provider = os.environ.get("PI_PROVIDER")
    default_model = os.environ.get("PI_MODEL")
    if options:
        provider = options.get("provider") or default_provider
        model = options.get("model") or default_model
        instructions = options.get("instructions")
        if isinstance(provider, str) and provider:
            args.extend(["--provider", provider])
        if isinstance(model, str) and model:
            args.extend(["--model", model])
        thinking = _thinking_level(options)
        if thinking:
            args.extend(["--thinking", thinking])
        if isinstance(instructions, str) and instructions:
            args.extend(["--append-system-prompt", instructions])
    else:
        if default_provider:
            args.extend(["--provider", default_provider])
        if default_model:
            args.extend(["--model", default_model])
    args.append(prompt)
    return args


def _pi_session_id(session_id: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "-", session_id).strip("-.")
    return normalized or "pi-session"


def translate_pi_item(
    item: dict[str, Any],
    *,
    run_id: str,
    text_state: dict[str, Any],
) -> list[PiEvent]:
    kind = str(item.get("type") or "")
    events: list[PiEvent] = []

    if kind == "session":
        events.append(
            PiEvent(
                "run.started",
                {
                    "run_id": run_id,
                    "session_id": item.get("id"),
                    "cwd": item.get("cwd"),
                    "timestamp": item.get("timestamp"),
                },
            )
        )
        return events

    if kind == "message_update":
        assistant_event = item.get("assistantMessageEvent")
        if not isinstance(assistant_event, dict):
            return events
        event_type = str(assistant_event.get("type") or "")
        if event_type == "text_delta":
            delta = str(assistant_event.get("delta") or "")
            if delta:
                text_state["seen_delta"] = True
                events.append(PiEvent("message.delta", {"run_id": run_id, "delta": delta}))
        return events

    if kind == "tool_execution_start":
        tool_name = str(item.get("toolName") or "tool")
        events.append(
            PiEvent(
                "tool.started",
                {
                    "run_id": run_id,
                    "tool_name": tool_name,
                    "name": tool_name,
                    "args": item.get("args"),
                },
            )
        )
        return events

    if kind == "tool_execution_end":
        tool_name = str(item.get("toolName") or "tool")
        events.append(
            PiEvent(
                "tool.completed",
                {
                    "run_id": run_id,
                    "tool_name": tool_name,
                    "name": tool_name,
                    "is_error": bool(item.get("isError")),
                    "result": item.get("result"),
                },
            )
        )
        return events

    if kind in {"message_end", "turn_end"}:
        message = item.get("message")
        if not isinstance(message, dict) or message.get("role") != "assistant":
            return events
        text = _pi_text(message)
        if text and not text_state["seen_delta"]:
            events.append(PiEvent("message.delta", {"run_id": run_id, "delta": text}))
        stop_reason = str(message.get("stopReason") or "")
        if stop_reason == "aborted" or message.get("errorMessage"):
            events.append(
                PiEvent(
                    "run.failed",
                    {
                        "run_id": run_id,
                        "message": str(message.get("errorMessage") or "Pi run aborted"),
                    },
                )
            )
        elif kind == "message_end":
            events.append(
                PiEvent(
                    "message.completed",
                    {
                        "run_id": run_id,
                        "message_id": f"msg_{uuid.uuid4().hex[:12]}",
                    },
                )
            )
        return events

    return events


class PiClient:
    def __init__(self, command: str | None = None):
        self.command = command or os.environ.get("PI_COMMAND", "pi")

    async def stream(self, prompt: str, *, session_id: str, options: dict[str, Any] | None = None) -> AsyncIterator[PiEvent]:
        run_id = f"run_{uuid.uuid4().hex[:12]}"
        text_state = {"seen_delta": False}
        process = await asyncio.create_subprocess_exec(
            *_build_command(self.command, prompt, session_id=session_id, options=options),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        assert process.stdout is not None
        async for raw in process.stdout:
            line = raw.decode("utf-8", errors="replace").strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                yield PiEvent("message.delta", {"run_id": run_id, "delta": line})
                continue
            for event in translate_pi_item(item, run_id=run_id, text_state=text_state):
                yield event

        code = await process.wait()
        stderr = b""
        if process.stderr is not None:
            stderr = await process.stderr.read()
        if code != 0:
            yield PiEvent(
                "run.failed",
                {
                    "run_id": run_id,
                    "message": stderr.decode("utf-8", errors="replace")[-1000:] or f"Pi exited with status {code}",
                },
            )


def event_to_sse(event: PiEvent) -> bytes:
    return f"event: {event.event}\ndata: {json.dumps(event.data, ensure_ascii=True)}\n\n".encode("utf-8")
