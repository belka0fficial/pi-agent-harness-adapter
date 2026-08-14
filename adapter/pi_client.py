from __future__ import annotations

import asyncio
import json
import os
import re
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .toolgate_bridge import ToolGateBridge


@dataclass
class PiEvent:
    event: str
    data: dict[str, Any]


@dataclass
class PendingApproval:
    request_id: str
    tool_name: str
    args: dict[str, Any]
    expires_at: str | None
    request: dict[str, Any] | None = None


@dataclass
class RunContext:
    run_id: str
    session_id: str
    queue: asyncio.Queue[PiEvent | None] = field(default_factory=asyncio.Queue)
    text_state: dict[str, Any] = field(default_factory=lambda: {"seen_delta": False})
    pending_approval: PendingApproval | None = None
    stop_requested: bool = False
    completed: bool = False
    terminal_sent: bool = False
    resuming_after_approval: bool = False


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


def _pi_session_id(session_id: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "-", session_id).strip("-.")
    return normalized or "pi-session"


def _soul_block() -> str:
    text = os.environ.get("PI_SOUL_TEXT", "").strip()
    file_path = os.environ.get("PI_SOUL_FILE", "").strip()
    if text:
        return text
    if file_path:
        path = Path(file_path)
        if path.exists():
            return path.read_text(encoding="utf-8").strip()
    return ""


def _runtime_config(options: dict[str, Any] | None) -> dict[str, Any]:
    provider = None
    model = None
    thinking = None
    if options:
        provider = options.get("provider")
        model = options.get("model")
        thinking = _thinking_level(options)
    return {
        "provider": provider or os.environ.get("PI_PROVIDER"),
        "model": model or os.environ.get("PI_MODEL"),
        "thinking": thinking or os.environ.get("PI_THINKING"),
        "soul": _soul_block(),
    }


def _build_rpc_command(
    command: str,
    *,
    session_id: str,
    config: dict[str, Any],
    fork_from: str | None = None,
) -> list[str]:
    args = [command, "--mode", "rpc", "--approve", "--session-id", _pi_session_id(session_id)]
    if fork_from:
        args.extend(["--fork", fork_from])
    if isinstance(config.get("provider"), str) and config["provider"]:
        args.extend(["--provider", config["provider"]])
    if isinstance(config.get("model"), str) and config["model"]:
        args.extend(["--model", config["model"]])
    if isinstance(config.get("thinking"), str) and config["thinking"]:
        args.extend(["--thinking", config["thinking"]])
    if isinstance(config.get("soul"), str) and config["soul"]:
        args.extend(["--append-system-prompt", config["soul"]])
    return args


def _compose_prompt(prompt: str, options: dict[str, Any] | None) -> str:
    instructions = ""
    if options and isinstance(options.get("instructions"), str):
        instructions = options["instructions"].strip()
    if not instructions:
        return prompt
    return f"Session instruction for this turn only:\n{instructions}\n\nUser request:\n{prompt}"


def _extract_tool_text(result: dict[str, Any] | None) -> str:
    if not isinstance(result, dict):
        return ""
    content = result.get("content")
    if not isinstance(content, list):
        return ""
    parts: list[str] = []
    for item in content:
        if isinstance(item, dict) and item.get("type") == "text" and item.get("text"):
            parts.append(str(item["text"]))
    return "".join(parts)


def _parse_toolgate_confirmation(item: dict[str, Any]) -> dict[str, Any] | None:
    if str(item.get("toolName") or "").find("toolgate") == -1:
        return None
    payload = _extract_tool_text(item.get("result"))
    if not payload:
        return None
    try:
        value = json.loads(payload)
    except json.JSONDecodeError:
        return None
    if isinstance(value, dict) and value.get("code") == "CONFIRMATION_REQUIRED" and value.get("request_id"):
        return value
    return None


def _approval_event(run_id: str, approval: PendingApproval) -> PiEvent:
    request = approval.request or {}
    return PiEvent(
        "approval.required",
        {
            "run_id": run_id,
            "approval_id": approval.request_id,
            "request_id": approval.request_id,
            "id": approval.request_id,
            "expires_at": approval.expires_at,
            "tool_name": approval.tool_name,
            "name": approval.tool_name,
            "summary": {
                "title": request.get("title") or f"Run {approval.tool_name}",
                "details": request.get("details") or "Owner confirmation is required for this ToolGate action.",
                "action": request.get("payload") or {},
                "severity": request.get("severity") or "warning",
            },
        },
    )


def translate_pi_item(
    item: dict[str, Any],
    *,
    run_id: str,
    text_state: dict[str, Any],
) -> list[PiEvent]:
    kind = str(item.get("type") or "")
    events: list[PiEvent] = []

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


class SessionRuntime:
    def __init__(self, session_id: str, client: PiClient):
        self.session_id = session_id
        self.client = client
        self.process: asyncio.subprocess.Process | None = None
        self.reader_task: asyncio.Task[None] | None = None
        self.pending: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self.lock = asyncio.Lock()
        self.current_run: RunContext | None = None
        self.current_config: dict[str, Any] | None = None
        self.session_file: str | None = None

    async def ensure_started(self, options: dict[str, Any] | None, *, fork_from: str | None = None) -> None:
        config = _runtime_config(options)
        if self.process and self.process.returncode is None and self.current_config == config and not fork_from:
            return
        if self.process and self.process.returncode is None:
            self.process.terminate()
            try:
                await asyncio.wait_for(self.process.wait(), timeout=5)
            except asyncio.TimeoutError:
                self.process.kill()
                await self.process.wait()
        self.current_config = config
        self.process = await asyncio.create_subprocess_exec(
            *_build_rpc_command(self.client.command, session_id=self.session_id, config=config, fork_from=fork_from),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        self.reader_task = asyncio.create_task(self._reader())
        state = await self.send_command({"type": "get_state"})
        self.session_file = state.get("data", {}).get("sessionFile")

    async def send_command(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not self.process or not self.process.stdin or self.process.returncode is not None:
            raise RuntimeError("Pi RPC process is not available")
        request_id = str(payload.get("id") or f"rpc-{uuid.uuid4().hex[:12]}")
        payload = {**payload, "id": request_id}
        future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self.pending[request_id] = future
        self.process.stdin.write((json.dumps(payload, ensure_ascii=True) + "\n").encode("utf-8"))
        await self.process.stdin.drain()
        response = await future
        if not response.get("success", False):
            raise RuntimeError(str(response.get("error") or response.get("message") or "Pi RPC command failed"))
        return response

    async def start_run(self, run: RunContext, prompt: str, options: dict[str, Any] | None) -> None:
        async with self.lock:
            if self.current_run and not self.current_run.completed:
                raise RuntimeError("A run is already active for this session")
            await self.ensure_started(options)
            self.current_run = run
            response = await self.send_command({"type": "get_state"})
            data = response.get("data", {})
            self.session_file = data.get("sessionFile") or self.session_file
            await run.queue.put(
                PiEvent(
                    "run.started",
                    {
                        "run_id": run.run_id,
                        "session_id": data.get("sessionId") or self.session_id,
                        "session_file": data.get("sessionFile"),
                    },
                )
            )
            await self.send_command({"type": "prompt", "message": _compose_prompt(prompt, options)})

    async def stop_run(self, run: RunContext) -> dict[str, Any]:
        run.stop_requested = True
        return await self.send_command({"type": "abort"})

    async def decide_approval(self, run: RunContext, decision: str) -> dict[str, Any]:
        if not run.pending_approval:
            raise ValueError("run has no pending approval")
        record = self.client.toolgate.decide_request(run.pending_approval.request_id, decision)
        resume_message = _resume_message(run.pending_approval, decision)
        run.pending_approval = None
        run.resuming_after_approval = True
        await self.ensure_started(None)
        await self.send_command({"type": "prompt", "message": resume_message})
        return record

    async def fork_into(self, new_session_id: str) -> str | None:
        if not self.session_file:
            return None
        runtime = self.client._get_or_create_runtime(new_session_id)
        await runtime.ensure_started(None, fork_from=self.session_file)
        return runtime.session_file

    async def _reader(self) -> None:
        assert self.process and self.process.stdout
        try:
            async for raw in self.process.stdout:
                line = raw.decode("utf-8", errors="replace").strip()
                if not line:
                    continue
                try:
                    item = json.loads(line)
                except json.JSONDecodeError:
                    if self.current_run:
                        await self.current_run.queue.put(PiEvent("message.delta", {"run_id": self.current_run.run_id, "delta": line}))
                    continue
                if item.get("type") == "response" and item.get("id") in self.pending:
                    future = self.pending.pop(str(item["id"]))
                    if not future.done():
                        future.set_result(item)
                    continue
                await self._dispatch_event(item)
        finally:
            for future in self.pending.values():
                if not future.done():
                    future.set_exception(RuntimeError("Pi RPC process exited"))
            self.pending.clear()
            if self.current_run and not self.current_run.completed and not self.current_run.pending_approval:
                await self.current_run.queue.put(
                    PiEvent("run.failed", {"run_id": self.current_run.run_id, "message": "Pi RPC process exited unexpectedly"})
                )
                await self.current_run.queue.put(None)
                self.current_run.completed = True
            self.process = None

    async def _dispatch_event(self, item: dict[str, Any]) -> None:
        run = self.current_run
        if not run:
            return
        if item.get("type") in {"tool_execution_start", "tool_execution_end", "message_update"} and run.resuming_after_approval:
            run.resuming_after_approval = False
        confirmation = None
        if item.get("type") == "tool_execution_end":
            confirmation = _parse_toolgate_confirmation(item)
            if confirmation:
                request_id = str(confirmation["request_id"])
                request = self.client.toolgate.get_request(request_id)
                run.pending_approval = PendingApproval(
                    request_id=request_id,
                    tool_name=str(item.get("toolName") or "tool"),
                    args=item.get("args") if isinstance(item.get("args"), dict) else {},
                    expires_at=confirmation.get("expires_at"),
                    request=request,
                )
                await run.queue.put(_approval_event(run.run_id, run.pending_approval))
                asyncio.create_task(self.send_command({"type": "abort"}))
                return

        for event in translate_pi_item(item, run_id=run.run_id, text_state=run.text_state):
            if event.event == "run.failed":
                if run.pending_approval:
                    continue
                if run.resuming_after_approval and "aborted" in str(event.data.get("message") or "").lower():
                    continue
                if run.terminal_sent:
                    continue
                if run.stop_requested:
                    event = PiEvent("run.stopped", {"run_id": run.run_id, "message": "Run stopped by owner"})
                run.terminal_sent = True
                run.completed = True
            if event.event == "message.completed":
                run.completed = True
            await run.queue.put(event)

        if item.get("type") == "agent_settled":
            if run.pending_approval:
                return
            await run.queue.put(None)
            run.completed = True
            self.current_run = None


def _resume_message(approval: PendingApproval, decision: str) -> str:
    if decision == "approved":
        return (
            f"The owner approved ToolGate request {approval.request_id}. "
            f"Retry the exact same ToolGate tool call now with approval_request_id=\"{approval.request_id}\" "
            "and then continue the task."
        )
    return (
        f"The owner rejected ToolGate request {approval.request_id}. "
        "Do not retry that tool call. Continue by explaining the rejection and offering a safe next step."
    )


class PiClient:
    def __init__(self, command: str | None = None, toolgate: ToolGateBridge | None = None):
        self.command = command or os.environ.get("PI_COMMAND", "pi")
        self.toolgate = toolgate or ToolGateBridge()
        self._sessions: dict[str, SessionRuntime] = {}
        self._runs: dict[str, tuple[RunContext, SessionRuntime]] = {}

    def _get_or_create_runtime(self, session_id: str) -> SessionRuntime:
        runtime = self._sessions.get(session_id)
        if not runtime:
            runtime = SessionRuntime(session_id, self)
            self._sessions[session_id] = runtime
        return runtime

    async def stream(self, prompt: str, *, session_id: str, options: dict[str, Any] | None = None) -> AsyncIterator[PiEvent]:
        run = RunContext(run_id=f"run_{uuid.uuid4().hex[:12]}", session_id=session_id)
        runtime = self._get_or_create_runtime(session_id)
        self._runs[run.run_id] = (run, runtime)
        try:
            await runtime.start_run(run, prompt, options)
            while True:
                event = await run.queue.get()
                if event is None:
                    break
                yield event
        finally:
            run.completed = True
            self._runs.pop(run.run_id, None)
            if runtime.current_run is run:
                runtime.current_run = None

    async def stop_run(self, run_id: str) -> dict[str, Any]:
        run, runtime = self._require_run(run_id)
        return await runtime.stop_run(run)

    async def approve_run(self, run_id: str, decision: str) -> dict[str, Any]:
        run, runtime = self._require_run(run_id)
        return await runtime.decide_approval(run, decision)

    async def fork_session(self, source_session_id: str, new_session_id: str) -> str | None:
        source = self._get_or_create_runtime(source_session_id)
        return await source.fork_into(new_session_id)

    def session_state(self, session_id: str) -> dict[str, Any]:
        runtime = self._sessions.get(session_id)
        return {"session_file": runtime.session_file if runtime else None}

    def _require_run(self, run_id: str) -> tuple[RunContext, SessionRuntime]:
        record = self._runs.get(run_id)
        if not record:
            raise ValueError("run not found")
        return record


def event_to_sse(event: PiEvent) -> bytes:
    return f"event: {event.event}\ndata: {json.dumps(event.data, ensure_ascii=True)}\n\n".encode("utf-8")
