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

    def operations_summary(self, *, pending=None):
        return {
            "service_health": {},
            "pending_approvals": len(pending or []),
            "action_counts_24h": {"approved": 1, "rejected": 1, "expired": 0, "failed": 0},
            "recent_verification_events": [
                {"time": "2026-01-02T00:00:00+00:00", "risk": "low", "source": "ToolGate", "action_summary": "request decided request req-decided"}
            ],
            "toolgate_counts": {"success": 2, "failure": 0},
            "memorygate_counts": {"reads": 3, "writes": 1},
        }

    def memory_records(self):
        return [{"id": "mem-1", "title": "Owner prefers concise updates", "kind": "preference", "confidence": "high", "updated_at": "2026-01-03T00:00:00+00:00"}]

    def system_overview(self):
        return {
            "vitals": {"cpu_percent": 12, "memory": {"percent": 34}, "disk": {"percent": 56}, "cpu_count": 8},
            "containers": [],
            "errors": [],
            "packages": [],
            "backups": {"latest": {"name": "backup-probe", "created_at": 1760000000.0}},
            "sources": {"backups": {"status": "ok"}},
        }

    def tools(self):
        return [
            {"id": "echo", "name": "Echo", "description": "Test tool", "status": "active", "authorization": "auto"},
            {"id": "danger.write", "name": "Danger Write", "description": "Should stay hidden unless granted", "status": "active", "authorization": "approval"},
        ]

    def skills(self):
        return [
            {"id": "skill-1", "title": "Reply clearly", "version": "1", "active": True, "linked_tools": ["echo"]},
            {"id": "skill-secret", "title": "Secret workflow", "version": "1", "active": True, "linked_tools": ["danger.write"]},
        ]

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

    def update_toolgate_execution_scopes(self, scopes: list[str]):
        self.synced_toolgate_scopes = scopes
        self.synced_toolgate_scope_history = [*getattr(self, "synced_toolgate_scope_history", []), scopes]
        return {"id": "agent-key", "scopes": scopes}

    def toolgate_execution_status(self):
        return {"id": "agent-key", "scopes": getattr(self, "synced_toolgate_scopes", [])}


def reset_state():
    app.state.sessions = {"sess-1": {"id": "sess-1", "title": "Real session", "created_at": "2026-01-01T00:00:00+00:00", "updated_at": "2026-01-02T00:00:00+00:00"}}
    app.state.messages = {"sess-1": [{"id": "u1", "role": "user", "content": "hello", "created_at": "2026-01-01T00:00:00+00:00"}, {"id": "a1", "role": "assistant", "content": "hi", "created_at": "2026-01-01T00:01:00+00:00"}]}
    app.state.jobs = {"job-1": {"id": "job-1", "name": "Briefing", "schedule": "0 8 * * *", "prompt": "brief me", "paused": False, "last_run_at": None, "next_run_at": "2026-01-04T08:00:00+00:00"}}
    app.state.gates = FakeGates()
    app.state.agents = {}
    app.state.teams = {}
    app.state.approval_runs = {}


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
    assert home["operations"]["pending_approvals"] == 1
    assert home["operations"]["toolgate_counts"] == {"success": 2, "failure": 0}
    assert home["operations"]["memorygate_counts"] == {"reads": 3, "writes": 1}
    assert home["model_summary"]["runtime"]["status"] == "ok"
    assert home["model_summary"]["default_route"]["agent_id"] == "agent_pi_operator"
    assert home["model_summary"]["providers"][0]["id"] == "pi"
    assert home["backup_summary"]["status"] == "ok"
    assert "base_url" not in str(home["model_summary"])
    assert "token" not in str(home).lower()
    assert "secret" not in str(home).lower()
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


def test_agent_tool_grants_sync_to_native_toolgate_scopes(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        main._ensure_registry_seeded()
        response = client.patch(
            "/api/agents/agent_pi_operator",
            json={"tool_ids": ["research.scan-competition", "tool:filesystem.read", "*"]},
        )
        cleared = client.patch("/api/agents/agent_pi_operator", json={"tool_ids": []})

    assert response.status_code == 200
    assert app.state.gates.synced_toolgate_scope_history[0] == [
        "tool:*",
        "tool:filesystem.read",
        "tool:research.scan-competition",
    ]
    assert cleared.status_code == 200
    assert app.state.gates.synced_toolgate_scope_history[-1] == []


def test_tool_health_probe_checks_registry_and_toolgate_scope(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        main._ensure_registry_seeded()
        granted = client.patch("/api/agents/agent_pi_operator", json={"tool_ids": ["echo"]})
        ok = client.post(
            "/api/tools/echo/health",
            json={"agent_id": "agent_pi_operator", "team_id": "team_core"},
        )
        denied = client.post(
            "/api/tools/danger.write/health",
            json={"agent_id": "agent_pi_operator", "team_id": "team_core"},
        )
        activity = client.get("/api/agents/agent_pi_operator/activity").json()["activity"]

    assert granted.status_code == 200
    assert ok.status_code == 200
    assert ok.json()["status"] == "ok"
    assert ok.json()["registry_allowed"] is True
    assert ok.json()["execution_scope_allowed"] is True
    assert ok.json()["required_scope"] == "tool:echo"
    assert denied.status_code == 403
    assert "tool.health_checked" in [item["event_type"] for item in activity]
    assert "danger.write" not in str(activity)


def test_team_membership_updates_agent_team_ids(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        main._ensure_registry_seeded()
        created_agent = client.post(
            "/api/agents",
            json={
                "name": "Research Scout",
                "purpose": "Find safe public evidence for the owner.",
            },
        )
        agent_id = created_agent.json()["id"]
        created_team = client.post(
            "/api/teams",
            json={
                "name": "Research Cell",
                "purpose": "Coordinate bounded research tasks.",
                "orchestrator_agent_id": agent_id,
                "member_agent_ids": [],
            },
        )
        team_id = created_team.json()["id"]
        team_ids_after_create = list(app.state.agents[agent_id]["team_ids"])
        cleared = client.patch(
            f"/api/teams/{team_id}",
            json={"orchestrator_agent_id": "", "member_agent_ids": []},
        )

    assert created_agent.status_code == 200
    assert created_team.status_code == 200
    assert created_team.json()["member_agent_ids"] == [agent_id]
    assert team_id in team_ids_after_create
    assert cleared.status_code == 200
    assert app.state.agents[agent_id]["team_ids"] == []


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


def test_automation_jobs_validate_required_capabilities(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        grant = client.patch(
            "/api/agents/agent_pi_operator",
            json={"tool_ids": ["echo"], "memory_scopes": ["briefing"]},
        )
        created = client.post(
            "/api/jobs",
            json={
                "name": "Scoped Job",
                "schedule": "0 9 * * *",
                "prompt": "assigned safely",
                "agent_id": "agent_pi_operator",
                "team_id": "team_core",
                "required_tool_ids": ["echo"],
                "required_memory_scopes": ["briefing"],
            },
        )
        missing_tool = client.post(
            "/api/jobs",
            json={
                "name": "Missing Tool Job",
                "schedule": "0 9 * * *",
                "prompt": "bad tool",
                "agent_id": "agent_pi_operator",
                "team_id": "team_core",
                "required_tool_ids": ["danger.write"],
            },
        )
        missing_memory = client.patch(
            f"/api/jobs/{created.json()['id']}",
            json={"required_memory_scopes": ["private-journal"]},
        )
    assert grant.status_code == 200
    assert created.status_code == 200
    assert created.json()["required_tool_ids"] == ["echo"]
    assert created.json()["required_memory_scopes"] == ["briefing"]
    assert missing_tool.status_code == 403
    assert missing_memory.status_code == 403



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


def test_agentgate_filters_tools_and_skills_by_agent_team_grants():
    reset_state()

    with TestClient(app) as client:
        main._ensure_registry_seeded()
        app.state.agents["agent_pi_operator"]["tool_ids"] = ["echo"]
        app.state.agents["agent_pi_operator"]["skill_ids"] = []
        app.state.teams["team_core"]["tool_ids"] = []
        app.state.teams["team_core"]["skill_ids"] = ["skill-1"]
        tools = client.get("/api/tools?agent_id=agent_pi_operator&team_id=team_core")
        skills = client.get("/api/skills?agent_id=agent_pi_operator&team_id=team_core")

    assert tools.status_code == 200
    assert skills.status_code == 200
    assert tools.json()["scope"] == "agent-effective"
    assert tools.json()["total"] == 2
    assert tools.json()["visible"] == 1
    assert [tool["id"] for tool in tools.json()["tools"]] == ["echo"]
    assert [skill["id"] for skill in skills.json()["skills"]] == ["skill-1"]
    assert "skill-secret" not in [skill["id"] for skill in skills.json()["skills"]]


def test_pi_discovery_exposes_memorygate_skills_and_toolgate_capabilities():
    reset_state()
    with TestClient(app) as client:
        main._ensure_registry_seeded()
        app.state.agents["agent_pi_operator"]["tool_ids"] = ["echo"]
        app.state.teams["team_core"]["skill_ids"] = ["skill-1"]
        capabilities = client.get("/v1/capabilities").json()
        skills = client.get("/v1/skills").json()
        toolsets = client.get("/v1/toolsets").json()
    assert capabilities["skills"] is True
    assert capabilities["toolsets"] is True
    assert skills[0]["id"] == "skill-1"
    assert toolsets[0]["id"] == "echo"
    assert "skill-secret" not in [skill["id"] for skill in skills]
    assert "danger.write" not in [tool["id"] for tool in toolsets]


def test_pi_discovery_defaults_to_empty_without_registry_grants():
    reset_state()
    with TestClient(app) as client:
        main._ensure_registry_seeded()
        app.state.agents["agent_pi_operator"]["tool_ids"] = []
        app.state.agents["agent_pi_operator"]["skill_ids"] = []
        app.state.teams["team_core"]["tool_ids"] = []
        app.state.teams["team_core"]["skill_ids"] = []
        skills = client.get("/v1/skills").json()
        toolsets = client.get("/v1/toolsets").json()

    assert skills == []
    assert toolsets == []


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


class BlockedJobPi:
    async def stream(self, prompt: str, *, session_id: str, options=None):
        raise AssertionError("Pi should not run when job requirements are missing")
        yield


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


def test_chat_uses_selected_agent_model_defaults_when_payload_omits_model():
    reset_state()
    pi = CapturingPi()
    app.state.pi = pi

    with TestClient(app) as client:
        main._ensure_registry_seeded()
        app.state.agents["agent_pi_operator"]["primary_provider"] = "openai-codex"
        app.state.agents["agent_pi_operator"]["primary_model"] = "gpt-5.6-luna"
        response = client.post("/api/sessions/sess-1/chat/stream", json={"input": "Use default model"})

    assert response.status_code == 200
    assert pi.options["provider"] == "openai-codex"
    assert pi.options["model"] == "gpt-5.6-luna"


def test_agent_activity_records_safe_chat_and_job_metadata(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()
    pi = CapturingPi()
    app.state.pi = pi

    with TestClient(app) as client:
        chat = client.post(
            "/api/sessions/sess-activity/chat/stream",
            json={"input": "Sensitive prompt must not be stored in activity"},
        )
        assert chat.status_code == 200
        created = client.post(
            "/api/jobs",
            json={"name": "Activity Probe", "schedule": "0 8 * * *", "prompt": "Sensitive job prompt"},
        )
        assert created.status_code == 200
        ran = client.post(f"/api/jobs/{created.json()['id']}/run")
        assert ran.status_code == 200
        activity = client.get("/api/agents/agent_pi_operator/activity").json()["activity"]

    event_types = [item["event_type"] for item in activity]
    assert "chat.started" in event_types
    assert "chat.completed" in event_types
    assert "job.created" in event_types
    assert "job.completed" in event_types
    assert "Sensitive" not in str(activity)
    assert all("summary" in item and "created_at" in item for item in activity)


def test_team_activity_rollup_uses_safe_metadata(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()
    app.state.pi = CapturingPi()

    with TestClient(app) as client:
        created = client.post(
            "/api/sessions",
            json={
                "title": "Team activity probe",
                "agent_id": "agent_pi_operator",
                "team_id": "team_core",
            },
        )
        assert created.status_code == 200
        chat = client.post(
            f"/api/sessions/{created.json()['id']}/chat/stream",
            json={"input": "Sensitive team prompt must not be stored in activity"},
        )
        assert chat.status_code == 200
        teams = client.get("/api/teams").json()["teams"]
        team = client.get("/api/teams/team_core").json()
        activity = client.get("/api/teams/team_core/activity").json()["activity"]

    core_rollup = next(item for item in teams if item["id"] == "team_core")
    assert core_rollup["recent_activity"]
    assert team["recent_activity"]
    assert [item["team_id"] for item in activity]
    assert all(item["team_id"] == "team_core" for item in activity)
    assert "chat.started" in [item["event_type"] for item in activity]
    assert "chat.completed" in [item["event_type"] for item in activity]
    assert "Sensitive" not in str(activity)


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
        facade_job = next(
            item for item in client.get("/api/automations").json()["automations"] if item["id"] == created["id"]
        )
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
    assert facade_job["last_result"]["status"] == "ok"
    assert facade_job["last_result"]["output_summary"] == "context-aware"
    assert "output" not in facade_job["last_result"]
    assert "prompt" not in facade_job["last_result"]


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


def test_automation_run_blocks_cleanly_when_requirements_are_revoked(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()
    app.state.pi = BlockedJobPi()

    with TestClient(app) as client:
        client.patch(
            "/api/agents/agent_pi_operator",
            json={"tool_ids": ["echo"], "memory_scopes": ["briefing"]},
        )
        created = client.post(
            "/api/jobs",
            json={
                "name": "Revoked Requirement",
                "schedule": "0 9 * * *",
                "prompt": "should not reach pi",
                "agent_id": "agent_pi_operator",
                "team_id": "team_core",
                "required_tool_ids": ["echo"],
                "required_memory_scopes": ["briefing"],
            },
        ).json()
        client.patch(
            "/api/agents/agent_pi_operator",
            json={"tool_ids": [], "memory_scopes": []},
        )
        ran = client.post(f"/api/jobs/{created['id']}/run")

    assert ran.status_code == 200
    body = ran.json()
    assert body["paused"] is True
    assert body["quarantine_reason"] == "paused after missing required grants"
    assert body["last_result"]["status"] == "blocked"
    assert body["last_result"]["output_chars"] == 0
    assert body["run_history"][0]["status"] == "blocked"
    assert "echo" in body["last_result"]["error"]


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
