from __future__ import annotations

from dataclasses import dataclass

from fastapi.testclient import TestClient

from adapter.main import app
from adapter.pi_client import PiEvent, _pi_session_id, translate_pi_item


@dataclass
class FakePi:
    async def stream(self, prompt: str, *, session_id: str, options: dict | None = None):
        yield PiEvent("run.started", {"run_id": "run-test", "session_id": session_id})
        yield PiEvent("message.delta", {"delta": f"hello {prompt}"})
        yield PiEvent("message.completed", {"message_id": "msg-test"})


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


def test_unimplemented_contract_returns_501():
    with TestClient(app) as client:
        assert client.get("/api/model/options").status_code == 501


def test_translate_real_pi_text_and_tool_events():
    run_id = "run-real"
    text_state = {"seen_delta": False}

    session_events = translate_pi_item(
        {
            "type": "session",
            "id": "01-session",
            "cwd": "/tmp/project",
            "timestamp": "2026-08-14T19:48:10.880Z",
        },
        run_id=run_id,
        text_state=text_state,
    )
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

    assert session_events[0].event == "run.started"
    assert session_events[0].data["session_id"] == "01-session"
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
