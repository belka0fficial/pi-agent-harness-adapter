from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any

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


app = FastAPI(title="Brain Adapter", version="0.1.0")
app.state.sessions = {}
app.state.messages = {}
app.state.pi = PiClient()


def now() -> str:
    return datetime.now(UTC).isoformat()


def not_ready(name: str):
    raise HTTPException(501, f"TODO: Brain adapter contract stub for {name}")


@app.get("/health")
def health():
    return {"status": "ok", "service": "brain-adapter"}


@app.get("/health/detailed")
def detailed_health():
    return {"status": "ok", "service": "brain-adapter", "pi": "configured"}


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
    return app.state.sessions.get(session_id) or not_ready("get session")


@app.patch("/api/sessions/{session_id}")
def update_session(session_id: str, payload: dict[str, Any]):
    return not_ready("update session")


@app.delete("/api/sessions/{session_id}")
def delete_session(session_id: str):
    return not_ready("delete session")


@app.get("/api/sessions/{session_id}/messages")
def messages(session_id: str):
    return {"messages": app.state.messages.get(session_id, [])}


@app.post("/api/sessions/{session_id}/fork")
def fork_session(session_id: str, payload: dict[str, Any]):
    return not_ready("fork session")


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
    return not_ready("model options")


@app.get("/v1/capabilities")
def capabilities():
    return {"skills": False, "toolsets": False, "runs": False, "jobs": True}


@app.get("/v1/{kind}")
def capability_kind(kind: str):
    if kind in {"skills", "toolsets"}:
        return []
    return not_ready(kind)


@app.post("/v1/runs/{run_id}/stop")
def stop_run(run_id: str):
    return not_ready("stop run")


@app.post("/v1/runs/{run_id}/approval")
def approve_run(run_id: str, payload: dict[str, Any]):
    return not_ready("run approval")

