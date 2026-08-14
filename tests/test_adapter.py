from __future__ import annotations

from dataclasses import dataclass

from fastapi.testclient import TestClient

from adapter.main import app
from adapter.pi_client import PiEvent


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

