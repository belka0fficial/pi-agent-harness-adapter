from __future__ import annotations

from fastapi.testclient import TestClient

from adapter import main
from adapter.main import app


class FakeGates:
    def health(self):
        return {"toolgate": {"status": "ok"}, "memorygate": {"status": "ok"}, "systemgate": {"status": "ok"}}

    def approvals(self, *, history: bool = False):
        if history:
            return [{"id": "req-decided", "source": "ToolGate", "severity": "low", "title": "Completed request", "details": "Already reviewed", "binding": {"type": "tool", "id": "echo", "version": "1", "digest": "abc"}, "decision": "approved", "decided_at": "2026-01-01T00:00:00+00:00", "decided_by": "Owner", "created_at": "2026-01-01T00:00:00+00:00"}]
        return [{"id": "req-pending", "source": "ToolGate", "severity": "medium", "title": "Run echo", "details": "Approval required", "binding": {"type": "tool", "id": "echo", "version": "1", "digest": "def"}, "created_at": "2026-01-02T00:00:00+00:00"}]

    def memory_records(self):
        return [{"id": "mem-1", "title": "Owner prefers concise updates", "kind": "preference", "confidence": "high", "updated_at": "2026-01-03T00:00:00+00:00"}]

    def system_overview(self):
        return {"vitals": {"cpu_percent": 12, "memory": {"percent": 34}, "disk": {"percent": 56}, "cpu_count": 8}, "containers": [], "errors": [], "packages": [], "backups": {"latest": None}}

    def tools(self):
        return [{"id": "echo", "name": "Echo", "description": "Test tool", "status": "active", "authorization": "auto"}]

    def skills(self):
        return [{"id": "skill-1", "title": "Reply clearly", "version": "1", "active": True, "linked_tools": ["echo"]}]

    def decide_approval(self, request_id: str, decision: str):
        return {"id": request_id, "decision": decision}

    def memory_context(self, query: str, *, agent_id: str | None = None):
        self.memory_agent_id = agent_id
        return {"memories": [{"text": "Owner prefers concise answers", "confidence": "high"}], "entities": []}

    def record_transcript(self, session_id: str, messages: list[dict], *, agent_id: str | None = None):
        self.recorded = {"session_id": session_id, "messages": messages, "agent_id": agent_id}
        return {"status": "ok"}

    def write_memory_candidate(self, candidate: dict):
        self.memory_candidate = candidate
        return {"status": "ok", "id": "mem-approved", "memory_type": candidate.get("memory_type") or "context"}


def reset_state():
    app.state.sessions = {"sess-1": {"id": "sess-1", "title": "Real session", "created_at": "2026-01-01T00:00:00+00:00", "updated_at": "2026-01-02T00:00:00+00:00"}}
    app.state.messages = {"sess-1": [{"id": "u1", "role": "user", "content": "hello", "created_at": "2026-01-01T00:00:00+00:00"}, {"id": "a1", "role": "assistant", "content": "hi", "created_at": "2026-01-01T00:01:00+00:00"}]}
    app.state.jobs = {"job-1": {"id": "job-1", "name": "Briefing", "schedule": "0 8 * * *", "prompt": "brief me", "paused": False, "last_run_at": None, "next_run_at": "2026-01-04T08:00:00+00:00"}}
    app.state.gates = FakeGates()
    app.state.agents = {}
    app.state.teams = {}


def test_agentgate_facade_exposes_real_sessions_messages_and_jobs():
    reset_state()
    with TestClient(app) as client:
        app.state.jobs = {"job-1": {"id": "job-1", "name": "Briefing", "schedule": "0 8 * * *", "prompt": "brief me", "paused": False, "last_run_at": None, "next_run_at": "2026-01-04T08:00:00+00:00"}}
        chats = client.get("/api/chats").json()["sessions"]
        messages = client.get("/api/chats/sess-1/messages").json()["messages"]
        automations = client.get("/api/automations").json()["automations"]
    assert chats[0]["id"] == "sess-1"
    assert chats[0]["preview"] == "hi"
    assert chats[0]["message_count"] == 2
    assert [message["role"] for message in messages] == ["owner", "agent"]
    assert automations[0]["id"] == "job-1"
    assert automations[0]["status"] == "active"


def test_agentgate_facade_aggregates_gates_without_exposing_credentials():
    reset_state()
    with TestClient(app) as client:
        home = client.get("/api/home").json()
        system = client.get("/api/system").json()
        pending = client.get("/api/approvals").json()
        history = client.get("/api/approvals/history").json()
        memory = client.get("/api/gates/memorygate").json()
        suggestions = client.get("/api/suggestions").json()
        character = client.get("/api/character").json()
    assert home["health"]["pi"]["status"] == "ok"
    assert home["health"]["toolgate"]["status"] == "ok"
    assert home["pending_verifications"][0]["id"] == "req-pending"
    assert system["vitals"]["cpu_percent"] == 12
    assert pending[0]["id"] == "req-pending"
    assert history[0]["decision"] == "approved"
    assert memory["memories"][0]["id"] == "mem-1"
    assert suggestions == {"suggestions": []}
    assert character["name"]
    assert "key" not in str(home).lower()


def test_agentgate_facade_decides_toolgate_approval():
    reset_state()
    with TestClient(app) as client:
        response = client.post("/api/approvals/req-pending/decision", json={"decision": "approved"})
    assert response.status_code == 200
    assert response.json() == {"id": "req-pending", "decision": "approved"}


def test_agent_registry_persists_to_sqlite(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    app.state.agents = {}
    app.state.teams = {}

    with TestClient(app) as client:
        created = client.post(
            "/api/agents",
            json={
                "name": "Education Coach",
                "title": "Learning agent",
                "purpose": "Help the owner grow skills with scoped memory and tools.",
                "memory_scopes": ["education"],
                "tool_ids": ["approval.test-echo"],
            },
        )
    assert created.status_code == 200
    agent_id = created.json()["id"]

    app.state.agents = {}
    app.state.teams = {}
    main._load_registry()

    assert app.state.agents[agent_id]["name"] == "Education Coach"
    assert app.state.agents[agent_id]["memory_scopes"] == ["education"]
    assert app.state.agents[agent_id]["tool_ids"] == ["approval.test-echo"]

    with TestClient(app) as client:
        deleted = client.delete(f"/api/agents/{agent_id}")
    assert deleted.status_code == 200
    assert deleted.json() == {"deleted": True}

    app.state.agents = {}
    app.state.teams = {}
    main._load_registry()

    assert agent_id not in app.state.agents


def test_automation_jobs_persist_to_sqlite(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    app.state.agents = {}
    app.state.teams = {}
    app.state.jobs = {}

    with TestClient(app) as client:
        created = client.post(
            "/api/jobs",
            json={
                "name": "Morning Brief Probe",
                "schedule": "0 9 * * *",
                "prompt": "Harmless persistence test.",
            },
        )
    assert created.status_code == 200
    job_id = created.json()["id"]

    app.state.jobs = {}
    main._load_registry()

    assert app.state.jobs[job_id]["name"] == "Morning Brief Probe"
    assert app.state.jobs[job_id]["prompt"] == "Harmless persistence test."
    assert app.state.jobs[job_id]["paused"] is False

    with TestClient(app) as client:
        paused = client.post(f"/api/jobs/{job_id}/pause")
    assert paused.status_code == 200

    app.state.jobs = {}
    main._load_registry()

    assert app.state.jobs[job_id]["paused"] is True

    with TestClient(app) as client:
        deleted = client.delete(f"/api/jobs/{job_id}")
    assert deleted.status_code == 200

    app.state.jobs = {}
    main._load_registry()

    assert job_id not in app.state.jobs


def test_automation_jobs_validate_agent_team_assignment():
    reset_state()
    main._ensure_registry_seeded()
    app.state.agents["agent_pi_operator"]["team_ids"] = ["team_core"]
    app.state.teams["team_core"]["member_agent_ids"] = ["agent_pi_operator"]
    with TestClient(app) as client:
        created = client.post(
            "/api/jobs",
            json={
                "name": "Assigned Job",
                "schedule": "0 9 * * *",
                "prompt": "assigned safely",
                "agent_id": "agent_pi_operator",
                "team_id": "team_core",
            },
        )
        missing_agent = client.post(
            "/api/jobs",
            json={
                "name": "Missing Agent",
                "schedule": "0 9 * * *",
                "prompt": "bad actor",
                "agent_id": "agent_missing",
            },
        )
    assert created.status_code == 200
    assert created.json()["agent_id"] == "agent_pi_operator"
    assert created.json()["team_id"] == "team_core"
    assert missing_agent.status_code == 404



def test_gate_clients_normalize_upstream_contracts(monkeypatch):
    from adapter.gates import GateClients

    gates = GateClients()
    responses = {
        ("GET", "toolgate", "/health"): {"status": "ok"},
        ("GET", "memorygate", "/health"): {"status": "ok"},
        ("GET", "systemgate", "/health"): {"status": "ok"},
        ("GET", "toolgate", "/v2/requests"): [
            {"id": "pending", "kind": "tool_approval", "title": "Run echo", "details": "Needs approval", "severity": "warning", "status": "pending", "payload": {"subject_type": "tool", "subject_id": "echo", "subject_version": "2", "argument_digest": "sha256:123"}, "created_at": "2026-01-01T00:00:00+00:00"},
            {"id": "done", "kind": "tool_approval", "title": "Run fetch", "details": "Reviewed", "severity": "info", "status": "approved", "decision": {"actor": "admin", "at": "2026-01-02T00:00:00+00:00"}, "payload": {}, "created_at": "2026-01-01T00:00:00+00:00"},
        ],
        ("GET", "memorygate", "/memory"): {"results": [{"id": "m1", "text": "Remember this", "memory_type": "fact", "confidence": "high", "updated_at": "2026-01-01T00:00:00+00:00"}]},
        ("GET", "systemgate", "/vitals"): {"cpu_percent": 5, "memory": {"percent": 10}, "disk": {"percent": 20}, "cpu_count": 4},
        ("GET", "systemgate", "/containers"): {"containers": []},
        ("GET", "systemgate", "/logs/errors"): {"text": ""},
        ("GET", "systemgate", "/packages"): {"packages": []},
        ("GET", "systemgate", "/backups"): {"backups": []},
    }

    def fake_request(service, path, *, method="GET", payload=None, timeout=8):
        if method == "POST":
            return {"id": path.split("/")[-2], "status": payload["status"]}
        return responses[(method, service, path)]

    monkeypatch.setattr(gates, "_request", fake_request)

    assert gates.health()["toolgate"]["status"] == "ok"
    assert gates.approvals()[0]["binding"]["digest"] == "sha256:123"
    assert gates.approvals(history=True)[0]["decision"] == "approved"
    assert gates.memory_records()[0]["title"] == "Remember this"
    assert gates.system_overview()["vitals"]["cpu_percent"] == 5
    assert gates.decide_approval("pending", "approved")["status"] == "approved"


def test_system_overview_degrades_when_one_systemgate_endpoint_times_out(monkeypatch):
    from adapter.gates import GateClients

    gates = GateClients()

    def fake_request(service, path, *, method="GET", payload=None, timeout=8):
        assert service == "systemgate"
        if path == "/vitals":
            return {"cpu_percent": 5, "memory": {"percent": 10}, "disk": {"percent": 20}, "cpu_count": 4}
        if path == "/containers":
            raise RuntimeError("systemgate is unavailable: timeout")
        return {}

    monkeypatch.setattr(gates, "_request", fake_request)

    overview = gates.system_overview()

    assert overview["vitals"]["cpu_percent"] == 5
    assert overview["containers"] == []
    assert overview["packages"] == []
    assert overview["backups"] == {"latest": None}
    assert overview["sources"]["containers"]["status"] == "unavailable"
    assert "containers" in overview["errors"][0]["service"]


def test_agentgate_facade_exposes_toolgate_tools_and_memorygate_skills():
    reset_state()
    with TestClient(app) as client:
        tools = client.get("/api/tools")
        skills = client.get("/api/skills")
    assert tools.status_code == 200
    assert skills.status_code == 200
    assert tools.json()["tools"][0]["id"] == "echo"
    assert skills.json()["skills"][0]["id"] == "skill-1"


def test_pi_discovery_exposes_memorygate_skills_and_toolgate_capabilities():
    reset_state()
    with TestClient(app) as client:
        capabilities = client.get("/v1/capabilities").json()
        skills = client.get("/v1/skills").json()
    assert capabilities["skills"] is True
    assert capabilities["toolsets"] is True
    assert skills[0]["id"] == "skill-1"


class CapturingPi:
    def __init__(self):
        self.options = None

    async def stream(self, prompt: str, *, session_id: str, options=None):
        from adapter.pi_client import PiEvent
        self.options = options
        yield PiEvent("run.started", {"run_id": "run-context"})
        yield PiEvent("message.delta", {"delta": "context-aware"})
        yield PiEvent("message.completed", {"message_id": "done"})


class FailingJobPi:
    async def stream(self, prompt: str, *, session_id: str, options=None):
        from adapter.pi_client import PiEvent
        yield PiEvent("run.started", {"run_id": "run-failed"})
        yield PiEvent("run.failed", {"message": "simulated failure"})


def test_chat_retrieves_memorygate_context_and_records_completed_transcript():
    reset_state()
    pi = CapturingPi()
    app.state.pi = pi
    with TestClient(app) as client:
        response = client.post("/api/sessions/sess-1/chat/stream", json={"input": "What matters?", "memory_enabled": True})
    assert response.status_code == 200
    assert "Owner prefers concise answers" in pi.options["instructions"]
    assert app.state.gates.recorded["session_id"] == "sess-1"
    assert app.state.gates.recorded["agent_id"] == "agent_pi_operator"
    assert app.state.gates.memory_agent_id == "agent_pi_operator"
    assert app.state.gates.recorded["messages"][-1]["content"] == "context-aware"


def test_chat_defaults_to_no_memory_side_effects():
    reset_state()
    pi = CapturingPi()
    app.state.pi = pi
    app.state.gates.context_called = False
    app.state.gates.recorded = None

    def fail_context(query: str):
        app.state.gates.context_called = True
        raise AssertionError("memory_context should be explicit opt-in")

    def fail_record(session_id: str, messages: list[dict]):
        raise AssertionError("record_transcript should be explicit opt-in")

    app.state.gates.memory_context = fail_context
    app.state.gates.record_transcript = fail_record
    with TestClient(app) as client:
        response = client.post("/api/sessions/sess-1/chat/stream", json={"input": "default turn"})
    assert response.status_code == 200
    assert app.state.gates.context_called is False
    assert "MemoryGate reference context" not in str(pi.options.get("instructions"))


def test_chat_rejects_memory_when_agent_has_no_memory_scopes():
    reset_state()
    app.state.pi = CapturingPi()
    with TestClient(app) as client:
        main._ensure_registry_seeded()
        app.state.agents["agent_pi_operator"]["memory_scopes"] = []
        app.state.teams["team_core"]["memory_scopes"] = []
        response = client.post("/api/sessions/sess-1/chat/stream", json={"input": "What matters?", "memory_enabled": True})
    assert response.status_code == 403
    assert "memorygate" in response.text.lower()


def test_automation_rejects_webhooks_and_too_frequent_cron():
    reset_state()
    with TestClient(app) as client:
        webhook = client.post(
            "/api/jobs",
            json={
                "name": "Webhook Probe",
                "schedule": "0 9 * * *",
                "prompt": "no external delivery",
                "webhook_url": "https://example.com/hook",
            },
        )
        frequent = client.post(
            "/api/jobs",
            json={
                "name": "Too Frequent",
                "schedule": "*/1 * * * *",
                "prompt": "too often",
            },
        )
        acceptable_step = client.post(
            "/api/jobs",
            json={
                "name": "Ten Minute Probe",
                "schedule": "*/10 * * * *",
                "prompt": "reasonable cadence",
                "timezone": "UTC",
            },
        )
        invalid_timezone = client.post(
            "/api/jobs",
            json={
                "name": "Bad Timezone",
                "schedule": "0 9 * * *",
                "prompt": "bad timezone",
                "timezone": "Mars/Olympus_Mons",
            },
        )
    assert webhook.status_code == 422
    assert "disabled" in webhook.text.lower()
    assert frequent.status_code == 422
    assert "5 minutes" in frequent.text
    assert acceptable_step.status_code == 200
    assert acceptable_step.json()["timezone"] == "UTC"
    assert len(acceptable_step.json()["schedule_preview"]) == 3
    assert invalid_timezone.status_code == 422
    assert "timezone" in invalid_timezone.text.lower()


def test_automation_run_persists_safe_result_summary_only():
    reset_state()
    app.state.pi = CapturingPi()
    with TestClient(app) as client:
        created = client.post(
            "/api/jobs",
            json={
                "name": "Safe Result",
                "schedule": "0 9 * * *",
                "prompt": "Summarize private notes",
            },
        ).json()
        ran = client.post(f"/api/jobs/{created['id']}/run").json()
    result = ran["last_result"]
    assert result["status"] == "ok"
    assert result["output_summary"] == "context-aware"
    assert result["output_chars"] == len("context-aware")
    assert result["completed_at"]
    assert "output" not in result
    assert "prompt" not in result
    assert ran["runs"] == 1
    assert ran["history"].endswith("s")
    assert ran["run_history"][0]["status"] == "ok"
    assert ran["run_history"][0]["output_summary"] == "context-aware"
    assert "prompt" not in ran["run_history"][0]


def test_automation_pauses_after_three_failures():
    reset_state()
    app.state.pi = FailingJobPi()
    with TestClient(app) as client:
        created = client.post(
            "/api/jobs",
            json={
                "name": "Failure Probe",
                "schedule": "0 9 * * *",
                "prompt": "fail safely",
            },
        ).json()
        for _ in range(3):
            ran = client.post(f"/api/jobs/{created['id']}/run").json()
    assert ran["failure_count"] == 3
    assert ran["paused"] is True
    assert "3 consecutive" in ran["quarantine_reason"]


def test_chat_memory_disabled_skips_context_and_transcript_ingest():
    reset_state()
    pi = CapturingPi()
    app.state.pi = pi
    app.state.gates.context_called = False
    app.state.gates.recorded = None

    def fail_context(query: str):
        app.state.gates.context_called = True
        raise AssertionError("memory_context should not be called when memory is disabled")

    def fail_record(session_id: str, messages: list[dict]):
        raise AssertionError("record_transcript should not be called when memory is disabled")

    app.state.gates.memory_context = fail_context
    app.state.gates.record_transcript = fail_record
    with TestClient(app) as client:
        response = client.post("/api/sessions/sess-1/chat/stream", json={"input": "private turn", "memory_enabled": False})
    assert response.status_code == 200
    assert app.state.gates.context_called is False
    assert "MemoryGate reference context" not in str(pi.options.get("instructions"))



def test_agentgate_memory_candidate_approval_writes_owner_approved_memory():
    reset_state()
    with TestClient(app) as client:
        response = client.post("/api/memory/candidates", json={
            "text": "Owner wants AgentGate to stay local-first.",
            "session_id": "sess-1",
            "source_message_id": "a1",
            "memory_type": "context",
            "confidence": "high",
            "tags": ["agentgate"],
            "approved": True,
            "candidate_id": "memcand_test",
            "source_role": "agent",
        })
    assert response.status_code == 200
    assert response.json()["id"] == "mem-approved"
    candidate = app.state.gates.memory_candidate
    assert candidate["text"] == "Owner wants AgentGate to stay local-first."
    assert candidate["source_type"] == "agentgate_owner_approved"
    assert candidate["memory_type"] == "context"
    assert candidate["confidence"] == "high"
    assert candidate["do_not_generalize"] is True
    assert "agentgate" in candidate["tags"]
    assert "source:chat" in candidate["tags"]
    assert "role:agent" in candidate["tags"]
    assert "untrusted-selected-text" in candidate["tags"]
    assert "candidate:memcand_test" in candidate["tags"]
    assert "session:sess-1" in candidate["tags"]
    assert candidate["evidence"]["session_id"] == "sess-1"
    assert candidate["evidence"]["source_message_id"] == "a1"
    assert candidate["evidence"]["source_role"] == "agent"
    assert candidate["evidence"]["candidate_id"] == "memcand_test"


def test_agentgate_memory_candidate_requires_explicit_owner_approval():
    reset_state()
    with TestClient(app) as client:
        response = client.post("/api/memory/candidates", json={"text": "save me", "session_id": "sess-1"})
    assert response.status_code == 422
    assert "approval" in response.text.lower()


def test_agentgate_memory_candidate_requires_existing_source_message():
    reset_state()
    with TestClient(app) as client:
        response = client.post("/api/memory/candidates", json={
            "text": "save me",
            "session_id": "sess-1",
            "source_message_id": "missing",
            "approved": True,
        })
    assert response.status_code == 404
    assert "source message" in response.text.lower()


def test_agentgate_memory_candidate_rejects_empty_text():
    reset_state()
    with TestClient(app) as client:
        response = client.post("/api/memory/candidates", json={"text": "   ", "session_id": "sess-1"})
    assert response.status_code == 422
