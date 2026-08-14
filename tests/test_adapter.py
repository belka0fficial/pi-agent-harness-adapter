from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field

from fastapi.testclient import TestClient

from adapter.main import app
from adapter.pi_client import PiEvent, _build_rpc_command, _pi_session_id, translate_pi_item


@dataclass
class FakePi:
    async def stream(self, prompt: str, *, session_id: str, options: dict | None = None):
        yield PiEvent("run.started", {"run_id": "run-test", "session_id": session_id})
        yield PiEvent("message.delta", {"delta": f"hello {prompt}"})
        yield PiEvent("message.completed", {"message_id": "msg-test"})


@dataclass
class ControlledPi:
    stop_event: threading.Event = field(default_factory=threading.Event)
    approved: dict[str, str] = field(default_factory=dict)

    async def stream(self, prompt: str, *, session_id: str, options: dict | None = None):
        if prompt == "needs-stop":
            yield PiEvent("run.started", {"run_id": "run-stop", "session_id": session_id})
            while not self.stop_event.is_set():
                time.sleep(0.01)
            yield PiEvent("run.stopped", {"run_id": "run-stop", "message": "Run stopped by owner"})
            return
        if prompt == "needs-approval":
            yield PiEvent("run.started", {"run_id": "run-approval", "session_id": session_id})
            yield PiEvent("tool.started", {"run_id": "run-approval", "tool_name": "mcp_toolgate_test"})
            yield PiEvent(
                "approval.required",
                {
                    "run_id": "run-approval",
                    "approval_id": "req-1",
                    "request_id": "req-1",
                    "id": "req-1",
                    "expires_at": "2026-08-14T20:30:00+00:00",
                    "summary": {"title": "Run test tool"},
                },
            )
            while "run-approval" not in self.approved:
                time.sleep(0.01)
            decision = self.approved["run-approval"]
            yield PiEvent("message.delta", {"run_id": "run-approval", "delta": f"{decision}: resolved"})
            yield PiEvent("message.completed", {"run_id": "run-approval", "message_id": "msg-approval"})
            return
        yield PiEvent("run.started", {"run_id": "run-default", "session_id": session_id})
        yield PiEvent("message.delta", {"run_id": "run-default", "delta": f"default {prompt}"})
        yield PiEvent("message.completed", {"run_id": "run-default", "message_id": "msg-default"})

    async def stop_run(self, run_id: str):
        self.stop_event.set()
        return {"run_id": run_id}

    async def approve_run(self, run_id: str, decision: str):
        self.approved[run_id] = decision
        return {"id": "req-1", "status": decision}


def test_round_trip_chat_against_mocked_pi():
    app.state.sessions = {}
    app.state.messages = {}
    app.state.pi = FakePi()
    with TestClient(app) as client:
        created = client.post("/api/sessions", json={"title": "Test"}).json()
        session_id = created["id"]
        with client.stream("POST", f"/api/sessions/{session_id}/chat/stream", json={"input": "world"}) as response:
            body = response.read().decode()

        assert response.status_code == 200
        assert "event: run.started" in body
        assert "event: message.delta" in body
        assert "hello world" in body
        messages = client.get(f"/api/sessions/{session_id}/messages").json()["messages"]
        assert messages[-1]["role"] == "assistant"
        assert messages[-1]["content"] == "hello world"


def test_stop_endpoint_terminates_stream_and_session_remains_usable():
    app.state.sessions = {}
    app.state.messages = {}
    app.state.pi = ControlledPi()
    with TestClient(app) as client:
        created = client.post("/api/sessions", json={"title": "Stop"}).json()
        session_id = created["id"]

        def stop_later():
            time.sleep(0.05)
            with TestClient(app) as other:
                result = other.post("/v1/runs/run-stop/stop")
                assert result.status_code == 200

        thread = threading.Thread(target=stop_later)
        thread.start()
        with client.stream("POST", f"/api/sessions/{session_id}/chat/stream", json={"input": "needs-stop"}) as response:
            body = response.read().decode()
        thread.join(timeout=2)

        assert "event: run.stopped" in body
        follow_up = client.post(f"/api/sessions/{session_id}/chat/stream", json={"input": "world"})
        assert follow_up.status_code == 200


def test_approval_endpoint_supports_approve_and_reject_paths():
    app.state.sessions = {}
    app.state.messages = {}
    app.state.pi = ControlledPi()
    with TestClient(app) as client:
        created = client.post("/api/sessions", json={"title": "Approve"}).json()
        session_id = created["id"]

        def approve_later(decision: str):
            time.sleep(0.05)
            with TestClient(app) as other:
                result = other.post("/v1/runs/run-approval/approval", json={"decision": decision})
                assert result.status_code == 200

        approve_thread = threading.Thread(target=approve_later, args=("approved",))
        approve_thread.start()
        with client.stream("POST", f"/api/sessions/{session_id}/chat/stream", json={"input": "needs-approval"}) as response:
            approve_body = response.read().decode()
        approve_thread.join(timeout=2)

        assert "event: approval.required" in approve_body
        assert "approved: resolved" in approve_body

        app.state.pi = ControlledPi()
        reject_thread = threading.Thread(target=approve_later, args=("rejected",))
        reject_thread.start()
        with client.stream("POST", f"/api/sessions/{session_id}/chat/stream", json={"input": "needs-approval"}) as response:
            reject_body = response.read().decode()
        reject_thread.join(timeout=2)

        assert "event: approval.required" in reject_body
        assert "rejected: resolved" in reject_body


def test_translate_real_pi_text_and_tool_events():
    run_id = "run-real"
    text_state = {"seen_delta": False}

    tool_start_events = translate_pi_item(
        {
            "type": "tool_execution_start",
            "toolName": "ls",
            "args": {"path": ".", "limit": 2},
        },
        run_id=run_id,
        text_state=text_state,
    )
    delta_events = translate_pi_item(
        {
            "type": "message_update",
            "assistantMessageEvent": {"type": "text_delta", "contentIndex": 0, "delta": "OK"},
        },
        run_id=run_id,
        text_state=text_state,
    )
    completed_events = translate_pi_item(
        {
            "type": "message_end",
            "message": {
                "role": "assistant",
                "content": [{"type": "text", "text": "OK"}],
                "stopReason": "stop",
            },
        },
        run_id=run_id,
        text_state=text_state,
    )

    assert tool_start_events[0].event == "tool.started"
    assert tool_start_events[0].data["tool_name"] == "ls"
    assert delta_events == [PiEvent("message.delta", {"run_id": run_id, "delta": "OK"})]
    assert completed_events[0].event == "message.completed"


def test_translate_real_pi_aborted_turn_into_run_failed():
    events = translate_pi_item(
        {
            "type": "turn_end",
            "message": {
                "role": "assistant",
                "content": [],
                "stopReason": "aborted",
                "errorMessage": "Request was aborted",
            },
        },
        run_id="run-abort",
        text_state={"seen_delta": False},
    )

    assert events == [PiEvent("run.failed", {"run_id": "run-abort", "message": "Request was aborted"})]


def test_pi_session_id_normalizes_invalid_characters():
    assert _pi_session_id("job:abc/123") == "job-abc-123"
    assert _pi_session_id("...") == "pi-session"


def test_build_rpc_command_includes_feature_flagged_soul_block(monkeypatch, tmp_path):
    soul = tmp_path / "soul.txt"
    soul.write_text("You are Hermes with a durable soul block.", encoding="utf-8")
    monkeypatch.setenv("PI_SOUL_FILE", str(soul))
    command = _build_rpc_command(
        "pi",
        session_id="sess_123",
        config={"provider": "openai-codex", "model": None, "thinking": None, "soul": soul.read_text(encoding="utf-8")},
    )

    assert "--append-system-prompt" in command
    assert "You are Hermes with a durable soul block." in command
