from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any

import subprocess

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from .pi_client import PiClient, event_to_sse


class ChatInput(BaseModel):
    input: str = Field(min_length=1)
    provider: str | None = None
    model: str | None = None
    model_options: dict[str, Any] | None = None
    instructions: str | None = None


app = FastAPI(title="Pi Agent Harness Adapter", version="0.1.0")
app.state.sessions = {}
app.state.messages = {}
app.state.pi = PiClient()


def now() -> str:
    return datetime.now(UTC).isoformat()


@app.get("/health")
def health():
    return {"status": "ok", "service": "pi-agent-harness-adapter"}


@app.get("/health/detailed")
def detailed_health():
    return {"status": "ok", "service": "pi-agent-harness-adapter", "pi": "configured"}


@app.post("/api/sessions")
def create_session(payload: dict[str, Any]):
    session_id = f"sess_{uuid.uuid4().hex[:12]}"
    item = {"id": session_id, "session_id": session_id, "title": payload.get("title") or "New chat", "created_at": now(), "updated_at": now()}
    app.state.sessions[session_id] = item
    app.state.messages[session_id] = []
    return item


@app.get("/api/sessions")
def list_sessions():
    return {"sessions": list(app.state.sessions.values())}


@app.get("/api/sessions/{session_id}")
def get_session(session_id: str):
    item = app.state.sessions.get(session_id)
    if not item:
        raise HTTPException(404, "session not found")
    return item


@app.patch("/api/sessions/{session_id}")
def update_session(session_id: str, payload: dict[str, Any]):
    item = app.state.sessions.get(session_id)
    if not item:
        raise HTTPException(404, "session not found")
    if isinstance(payload.get("title"), str) and payload["title"].strip():
        item["title"] = payload["title"].strip()
    item["updated_at"] = now()
    return item


@app.delete("/api/sessions/{session_id}")
def delete_session(session_id: str):
    if session_id not in app.state.sessions:
        raise HTTPException(404, "session not found")
    app.state.sessions.pop(session_id, None)
    app.state.messages.pop(session_id, None)
    return {"deleted": True}


@app.get("/api/sessions/{session_id}/messages")
def messages(session_id: str):
    return {"messages": app.state.messages.get(session_id, [])}


@app.post("/api/sessions/{session_id}/fork")
async def fork_session(session_id: str, payload: dict[str, Any]):
    source = app.state.sessions.get(session_id)
    if not source:
        raise HTTPException(404, "session not found")
    new_session_id = f"sess_{uuid.uuid4().hex[:12]}"
    item = {
        "id": new_session_id,
        "session_id": new_session_id,
        "title": payload.get("title") or f"Fork of {source.get('title') or session_id}",
        "created_at": now(),
        "updated_at": now(),
        "parent_session_id": session_id,
    }
    app.state.sessions[new_session_id] = item
    app.state.messages[new_session_id] = list(app.state.messages.get(session_id, []))
    await app.state.pi.fork_session(session_id, new_session_id)
    return item


@app.post("/api/sessions/{session_id}/chat/stream")
async def chat_stream(session_id: str, payload: ChatInput, request: Request):
    if session_id not in app.state.sessions:
        app.state.sessions[session_id] = {"id": session_id, "session_id": session_id, "title": "Imported chat", "created_at": now(), "updated_at": now()}
        app.state.messages[session_id] = []
    user_message = {"id": f"msg_{uuid.uuid4().hex[:12]}", "role": "user", "content": payload.input, "created_at": now()}
    app.state.messages[session_id].append(user_message)

    async def events() -> AsyncIterator[bytes]:
        collected = []
        options = {"provider": payload.provider, "model": payload.model, "model_options": payload.model_options, "instructions": payload.instructions}
        async for event in request.app.state.pi.stream(payload.input, session_id=session_id, options=options):
            if event.event == "message.delta":
                collected.append(str(event.data.get("delta") or event.data.get("text") or event.data.get("content") or ""))
            yield event_to_sse(event)
        if collected:
            request.app.state.messages[session_id].append({"id": f"msg_{uuid.uuid4().hex[:12]}", "role": "assistant", "content": "".join(collected), "created_at": now()})

    return StreamingResponse(events(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.get("/api/model/options")
def models():
    command = app.state.pi.command
    try:
        result = subprocess.run([command, "--list-models"], check=True, capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.SubprocessError):
        return {"models": [], "providers": []}
    models = []
    for line in result.stdout.splitlines():
        value = line.strip()
        if value and not value.lower().startswith("provider"):
            models.append({"id": value, "name": value})
    return {"models": models, "providers": sorted({value.split("/", 1)[0] for value in [item["id"] for item in models] if "/" in value})}


@app.get("/v1/capabilities")
def capabilities():
    return {"skills": False, "toolsets": False, "runs": True, "jobs": True}


@app.get("/v1/{kind}")
def capability_kind(kind: str):
    if kind in {"skills", "toolsets"}:
        return []
    raise HTTPException(404, "capability kind not found")


@app.post("/v1/runs/{run_id}/stop")
async def stop_run(run_id: str):
    try:
        await app.state.pi.stop_run(run_id)
    except ValueError:
        raise HTTPException(404, "run not found")
    return {"run_id": run_id, "status": "stopping"}


@app.post("/v1/runs/{run_id}/approval")
async def approve_run(run_id: str, payload: dict[str, Any]):
    decision = str(payload.get("decision") or "").strip().lower()
    if decision not in {"approved", "rejected"}:
        raise HTTPException(422, "decision must be approved or rejected")
    try:
        record = await app.state.pi.approve_run(run_id, decision)
    except ValueError as exc:
        raise HTTPException(404 if "run not found" in str(exc) else 409, str(exc))
    return {"run_id": run_id, "decision": decision, "request_id": record["id"], "status": record["status"]}
