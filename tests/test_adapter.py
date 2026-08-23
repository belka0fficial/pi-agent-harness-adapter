from __future__ import annotations

import asyncio
import threading
import time

import httpx
from dataclasses import dataclass, field

from fastapi.testclient import TestClient

from adapter import main
from adapter.main import app
from adapter.pi_client import PiEvent, _build_rpc_command, _pi_session_id, _pi_subprocess_env, translate_pi_item


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
                await asyncio.sleep(0.01)
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
                    "tool_name": "mcp_toolgate_test",
                    "expires_at": "2026-08-14T20:30:00+00:00",
                    "summary": {"title": "Run test tool"},
                },
            )
            while "run-approval" not in self.approved:
                await asyncio.sleep(0.01)
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


def test_session_context_is_used_by_chat_stream_without_turn_override():
    app.state.sessions = {}
    app.state.messages = {}
    app.state.agents = {}
    app.state.teams = {}
    main._ensure_registry_seeded()
    app.state.agents["agent_pi_operator"]["memory_scopes"] = []
    app.state.teams["team_core"]["memory_scopes"] = []
    app.state.pi = FakePi()
    with TestClient(app) as client:
        created = client.post(
            "/api/sessions",
            json={
                "title": "Core team room",
                "agent_id": "agent_pi_operator",
                "team_id": "team_core",
            },
        ).json()
        session_id = created["id"]
        with client.stream("POST", f"/api/sessions/{session_id}/chat/stream", json={"input": "team hello"}) as response:
            response.read()
        session = client.get(f"/api/sessions/{session_id}").json()

    assert created["agent_id"] == "agent_pi_operator"
    assert created["team_id"] == "team_core"
    assert response.status_code == 200
    assert session["agent_id"] == "agent_pi_operator"
    assert session["team_id"] == "team_core"


def test_session_context_update_validates_team_membership():
    app.state.sessions = {}
    app.state.messages = {}
    app.state.agents = {}
    app.state.teams = {}
    main._ensure_registry_seeded()
    with TestClient(app) as client:
        app.state.teams["team_empty"] = {
            "id": "team_empty",
            "name": "Empty Team",
            "purpose": "No members.",
            "orchestrator_agent_id": "",
            "member_agent_ids": [],
            "memory_scopes": [],
            "tool_ids": [],
            "skill_ids": [],
        }
        created = client.post("/api/sessions", json={"title": "Scoped"}).json()
        denied = client.patch(
            f"/api/sessions/{created['id']}",
            json={"agent_id": "agent_pi_operator", "team_id": "team_empty"},
        )

    assert denied.status_code == 403
    assert "not a member" in denied.text


def test_stop_endpoint_terminates_stream_and_session_remains_usable():
    async def scenario():
        app.state.sessions = {}
        app.state.messages = {}
        app.state.pi = ControlledPi()
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            created = (await client.post("/api/sessions", json={"title": "Stop"})).json()
            session_id = created["id"]
            stream_task = asyncio.create_task(
                client.post(f"/api/sessions/{session_id}/chat/stream", json={"input": "needs-stop"})
            )
            await asyncio.sleep(0.05)
            result = await client.post("/v1/runs/run-stop/stop")
            assert result.status_code == 200
            response = await asyncio.wait_for(stream_task, timeout=2)
            assert "event: run.stopped" in response.text
            follow_up = await client.post(f"/api/sessions/{session_id}/chat/stream", json={"input": "world"})
            assert follow_up.status_code == 200

    asyncio.run(scenario())


def test_approval_endpoint_supports_approve_and_reject_paths():
    async def run_decision(client: httpx.AsyncClient, session_id: str, decision: str):
        app.state.pi = ControlledPi()
        stream_task = asyncio.create_task(
            client.post(f"/api/sessions/{session_id}/chat/stream", json={"input": "needs-approval"})
        )
        await asyncio.sleep(0.05)
        result = await client.post("/v1/runs/run-approval/approval", json={"decision": decision})
        assert result.status_code == 200
        response = await asyncio.wait_for(stream_task, timeout=2)
        assert "event: approval.required" in response.text
        assert f"{decision}: resolved" in response.text

    async def scenario():
        app.state.sessions = {}
        app.state.messages = {}
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            created = (await client.post("/api/sessions", json={"title": "Approve"})).json()
            await run_decision(client, created["id"], "approved")
            await run_decision(client, created["id"], "rejected")

    asyncio.run(scenario())


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


def test_model_discovery_does_not_treat_no_models_help_text_as_models(monkeypatch):
    class Result:
        stdout = "No models available. Use /login to log into a provider via OAuth or API key. See:\n  /docs/providers.md\n  /docs/models.md\n"

    monkeypatch.setattr("adapter.main.subprocess.run", lambda *args, **kwargs: Result())
    app.state.pi = FakePi()
    app.state.pi.command = "pi"
    with TestClient(app) as client:
        payload = client.get("/api/model/options").json()
    assert payload == {"models": [], "providers": []}


def test_model_discovery_parses_pi_table_output(monkeypatch):
    class Result:
        stdout = (
            "provider      model                context  max-out  thinking  images\n"
            "openai-codex  gpt-5.6-luna         272K     128K     yes       yes\n"
            "openai-codex  gpt-5.6-sol          272K     128K     yes       yes\n"
        )

    monkeypatch.setattr("adapter.main.subprocess.run", lambda *args, **kwargs: Result())
    app.state.pi = FakePi()
    app.state.pi.command = "pi"
    with TestClient(app) as client:
        payload = client.get("/api/model/options").json()
    assert payload["providers"] == ["openai-codex"]
    assert payload["models"][0] == {
        "id": "openai-codex/gpt-5.6-luna",
        "provider": "openai-codex",
        "model": "gpt-5.6-luna",
        "name": "gpt-5.6-luna",
        "context": "272K",
        "max_output": "128K",
        "thinking": True,
        "images": True,
    }


class FailingPi:
    async def stream(self, prompt: str, *, session_id: str, options=None):
        if False:
            yield None
        raise RuntimeError("No API key found for the selected model")


def test_chat_stream_converts_pi_startup_errors_into_run_failed_sse():
    app.state.sessions = {}
    app.state.messages = {}
    app.state.pi = FailingPi()
    with TestClient(app) as client:
        created = client.post("/api/sessions", json={"title": "Failure"}).json()
        response = client.post(f"/api/sessions/{created['id']}/chat/stream", json={"input": "hello"})
    assert response.status_code == 200
    assert "event: run.failed" in response.text
    assert "No API key found" in response.text


def test_pi_subprocess_environment_excludes_gate_admin_credentials(monkeypatch):
    monkeypatch.setenv("TOOLGATE_ADMIN_KEY", "owner-secret")
    monkeypatch.setenv("MEMORYGATE_ADMIN_KEY", "memory-owner-secret")
    monkeypatch.setenv("SYSTEMGATE_ADMIN_KEY", "system-owner-secret")
    monkeypatch.setenv("TOOLGATE_EXECUTION_KEY", "scoped-agent-key")
    child_env = _pi_subprocess_env()
    assert "TOOLGATE_ADMIN_KEY" not in child_env
    assert "MEMORYGATE_ADMIN_KEY" not in child_env
    assert "SYSTEMGATE_ADMIN_KEY" not in child_env
    assert child_env["TOOLGATE_EXECUTION_KEY"] == "scoped-agent-key"


class DecisionGates:
    def __init__(self):
        self.decisions = []

    def decide_approval(self, request_id: str, decision: str):
        self.decisions.append((request_id, decision))
        return {"id": request_id, "status": decision, "decision": decision}


def test_agentgate_approval_decision_resumes_matching_paused_run():
    async def scenario():
        app.state.sessions = {}
        app.state.messages = {}
        app.state.agents = {}
        app.state.teams = {}
        main._ensure_registry_seeded()
        app.state.agents["agent_pi_operator"]["tool_ids"] = ["mcp_toolgate_test"]
        pi = ControlledPi()
        app.state.pi = pi
        app.state.gates = DecisionGates()
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            created = (await client.post("/api/sessions", json={"title": "Approve via facade"})).json()
            session_id = created["id"]
            stream_task = asyncio.create_task(
                client.post(f"/api/sessions/{session_id}/chat/stream", json={"input": "needs-approval"})
            )
            await asyncio.sleep(0.05)
            result = await client.post("/api/approvals/req-1/decision", json={"decision": "approved"})
            assert result.status_code == 200
            response = await asyncio.wait_for(stream_task, timeout=2)
            assert "event: approval.required" in response.text
            assert "approved: resolved" in response.text

    asyncio.run(scenario())


def test_agentgate_approval_decision_rejects_disallowed_runtime_tool():
    async def scenario():
        app.state.sessions = {}
        app.state.messages = {}
        app.state.agents = {}
        app.state.teams = {}
        main._ensure_registry_seeded()
        app.state.agents["agent_pi_operator"]["tool_ids"] = []
        pi = ControlledPi()
        app.state.pi = pi
        app.state.gates = DecisionGates()
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            created = (await client.post("/api/sessions", json={"title": "Approve via facade"})).json()
            session_id = created["id"]
            stream_task = asyncio.create_task(
                client.post(f"/api/sessions/{session_id}/chat/stream", json={"input": "needs-approval"})
            )
            await asyncio.sleep(0.05)
            result = await client.post("/api/approvals/req-1/decision", json={"decision": "approved"})
            assert result.status_code == 403
            assert "not allowed" in result.text
            await client.post("/api/approvals/req-1/decision", json={"decision": "rejected"})
            await asyncio.wait_for(stream_task, timeout=2)

    asyncio.run(scenario())


def test_agentgate_session_stop_endpoint_stops_current_server_side_run():
    async def scenario():
        app.state.sessions = {}
        app.state.messages = {}
        app.state.pi = ControlledPi()
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            created = (await client.post("/api/sessions", json={"title": "Stop facade"})).json()
            session_id = created["id"]
            stream_task = asyncio.create_task(
                client.post(f"/api/sessions/{session_id}/chat/stream", json={"input": "needs-stop"})
            )
            await asyncio.sleep(0.05)
            result = await client.post(f"/api/sessions/{session_id}/runs/current/stop")
            assert result.status_code == 200
            response = await asyncio.wait_for(stream_task, timeout=2)
            assert "event: run.stopped" in response.text

    asyncio.run(scenario())
