from __future__ import annotations

import asyncio
import json
import os
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any


@dataclass
class PiEvent:
    event: str
    data: dict[str, Any]


class PiClient:
    def __init__(self, command: str | None = None):
        self.command = command or os.environ.get("PI_COMMAND", "pi")

    async def stream(self, prompt: str, *, session_id: str, options: dict[str, Any] | None = None) -> AsyncIterator[PiEvent]:
        run_id = f"run_{uuid.uuid4().hex[:12]}"
        yield PiEvent("run.started", {"run_id": run_id, "session_id": session_id})

        process = await asyncio.create_subprocess_exec(
            self.command,
            "--mode",
            "json",
            "--approve",
            prompt,
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
                yield PiEvent("message.delta", {"delta": line})
                continue
            event = str(item.get("event") or item.get("type") or "message.delta")
            data = item.get("data") if isinstance(item.get("data"), dict) else item
            if event in {"assistant_message", "message", "delta"}:
                event = "message.delta"
            yield PiEvent(event, data)
        code = await process.wait()
        if code == 0:
            yield PiEvent("message.completed", {"message_id": f"msg_{uuid.uuid4().hex[:12]}", "run_id": run_id})
        else:
            stderr = await process.stderr.read() if process.stderr else b""
            yield PiEvent("run.failed", {"message": stderr.decode("utf-8", errors="replace")[-1000:], "run_id": run_id})


def event_to_sse(event: PiEvent) -> bytes:
    return f"event: {event.event}\ndata: {json.dumps(event.data, ensure_ascii=True)}\n\n".encode("utf-8")

