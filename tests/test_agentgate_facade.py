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
            {
                "id": "echo",
                "name": "Echo",
                "description": "Test tool",
                "status": "active",
                "authorization": "auto",
                "policy": {"usage_limits": {"max_per_minute": 12}},
            },
            {
                "id": "danger.write",
                "name": "Danger Write",
                "description": "Should stay hidden unless granted",
                "status": "active",
                "authorization": "owner_confirmation",
                "policy": {"usage_limits": {"max_per_minute": 1}},
            },
        ]

    def update_tool_policy(self, tool_id: str, *, authorization: str, usage_limits: dict[str, int]):
        self.updated_tool_policy = {
            "tool_id": tool_id,
            "authorization": authorization,
            "usage_limits": usage_limits,
        }
        return {
            "id": tool_id,
            "authorization": authorization,
            "policy": {"usage_limits": usage_limits},
            "execution": {"type": "echo"},
        }

    def skills(self):
        return [
            {"id": "skill-1", "title": "Reply clearly", "version": "1", "active": True, "linked_tools": ["echo"]},
            {"id": "skill-secret", "title": "Secret workflow", "version": "1", "active": True, "linked_tools": ["danger.write"]},
        ]

    def decide_approval(self, request_id: str, decision: str):
        record = getattr(self, "requests", {}).get(request_id, {"id": request_id, "payload": {}})
        record = {**record, "status": decision, "decision": {"actor": "admin"}}
        self.requests = {**getattr(self, "requests", {}), request_id: record}
        return record

    def create_admin_request(self, *, kind: str, title: str, details: str, payload: dict, severity: str = "warning"):
        request_id = f"req-{len(getattr(self, 'requests', {})) + 1}"
        record = {
            "id": request_id,
            "kind": kind,
            "title": title,
            "details": details,
            "payload": payload,
            "severity": severity,
            "status": "pending",
        }
        self.requests = {**getattr(self, "requests", {}), request_id: record}
        return record

    def request_status(self, request_id: str):
        return getattr(self, "requests", {}).get(request_id)

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
    app.state.tool_drafts = {}
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
    assert response.json()["id"] == "req-pending"
    assert response.json()["status"] == "approved"


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


def test_agent_identity_profile_is_bounded_and_sanitized(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        created = client.post(
            "/api/agents",
            json={
                "name": "Character Coach",
                "title": "Persona agent",
                "purpose": "Shape safe character profiles.",
                "voice": "Warm\x00, precise, and short.\\u0000",
                "personality": ["kind", "kind", "evidence-first", ""],
                "appearance": {
                    "mode": "character",
                    "style": "clean cel-shaded profile card",
                    "height": "170 cm",
                    "body_type": "athletic",
                    "palette": "warm neutrals",
                    "raw_asset_path": "should-not-persist",
                },
                "story": "A studio guide for agent identity drafts.",
            },
        )
        patched = client.patch(
            f"/api/agents/{created.json()['id']}",
            json={
                "personality": ["steady", "steady", "playful"],
                "appearance": {
                    "visual_summary": "Simple professional portrait with no generated asset yet.",
                    "avatar_hint": "future optional avatar sidecar",
                    "raw_tool_args": {"not": "stored"},
                },
            },
        )

    assert created.status_code == 200
    assert created.json()["voice"] == "Warm, precise, and short."
    assert created.json()["personality"] == ["kind", "evidence-first"]
    assert created.json()["appearance"] == {
        "mode": "character",
        "style": "clean cel-shaded profile card",
        "height": "170 cm",
        "body_type": "athletic",
        "palette": "warm neutrals",
    }
    assert patched.status_code == 200
    assert patched.json()["personality"] == ["steady", "playful"]
    assert patched.json()["appearance"] == {
        "visual_summary": "Simple professional portrait with no generated asset yet.",
        "avatar_hint": "future optional avatar sidecar",
    }
    assert "raw" not in str(patched.json()).lower()


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


def test_tool_policy_update_is_limited_to_authorization_and_usage_limits(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        response = client.patch(
            "/api/tools/echo/policy",
            json={
                "authorization": "owner_confirmation",
                "usage_limits": {
                    "max_per_minute": 3,
                    "max_per_hour": 12,
                    "cooldown_seconds": 2,
                    "max_runtime_seconds": 10,
                },
                "execution": {"type": "dangerous"},
            },
        )
        invalid = client.patch(
            "/api/tools/echo/policy",
            json={"authorization": "auto", "usage_limits": {"raw_args": 1}},
        )
        activity = client.get("/api/activity").json()["activity"]

    assert response.status_code == 200
    assert invalid.status_code == 422
    assert app.state.gates.updated_tool_policy == {
        "tool_id": "echo",
        "authorization": "owner_confirmation",
        "usage_limits": {
            "max_per_minute": 3,
            "max_per_hour": 12,
            "cooldown_seconds": 2,
            "max_runtime_seconds": 10,
        },
    }
    assert response.json()["tool"]["execution"] == {"type": "echo"}
    assert "tool.policy_updated" in [item["event_type"] for item in activity]
    assert "dangerous" not in str(activity)


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


def test_team_templates_create_metadata_only_team(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        templates = client.get("/api/team-templates").json()["templates"]
        created = client.post("/api/team-templates/security/create").json()
        updated_templates = client.get("/api/team-templates").json()["templates"]

    security_template = next(
        item for item in templates if item["id"] == "security"
    )
    updated_security = next(
        item for item in updated_templates if item["id"] == "security"
    )
    assert security_template["tool_ids"] == []
    assert security_template["skill_ids"] == []
    assert created["name"] == "Security Team"
    assert created["orchestrator_agent_id"] == "agent_pi_operator"
    assert created["member_agent_ids"] == ["agent_pi_operator"]
    assert created["tool_ids"] == []
    assert created["skill_ids"] == []
    assert created["memory_scopes"] == ["system-summary"]
    assert created["id"] in app.state.agents["agent_pi_operator"]["team_ids"]
    assert updated_security["already_created"] is True


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
        ("GET", "systemgate", "/containers"): {"results": [{"id": "abc123", "name": "agentgate", "image": "agentgate:test", "status": "running", "created": "2026-01-01T00:00:00Z", "stats": {"raw": "hidden"}}]},
        ("GET", "systemgate", "/logs/errors"): {"text": ""},
        ("GET", "systemgate", "/packages"): {"apt": {"ok": True, "output": ""}, "pip": {"ok": True, "output": "[]"}, "npm": {"ok": True, "output": '{}'}},
        ("GET", "systemgate", "/backups"): {"results": [{"name": "backup-1.tar.zst", "path": "hidden-upstream-path", "created_at": 1770000000.0}]},
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
    overview = gates.system_overview()
    assert overview["vitals"]["cpu_percent"] == 5
    assert overview["containers"][0]["name"] == "agentgate"
    assert "stats" not in overview["containers"][0]
    assert overview["backups"]["latest"]["name"] == "backup-1.tar.zst"
    assert "path" not in overview["backups"]["latest"]
    assert overview["packages"][0]["name"] == "apt"
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
    assert [pkg["name"] for pkg in overview["packages"]] == ["apt", "pip", "npm"]
    assert overview["backups"] == {"latest": None, "count": 0, "results": []}
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


def test_agentgate_skills_report_missing_linked_tool_grants():
    reset_state()

    with TestClient(app) as client:
        main._ensure_registry_seeded()
        app.state.agents["agent_pi_operator"]["tool_ids"] = []
        app.state.agents["agent_pi_operator"]["skill_ids"] = ["skill-1"]
        app.state.teams["team_core"]["tool_ids"] = []
        app.state.teams["team_core"]["skill_ids"] = []
        missing = client.get(
            "/api/skills?agent_id=agent_pi_operator&team_id=team_core"
        )
        app.state.teams["team_core"]["tool_ids"] = ["echo"]
        ready = client.get(
            "/api/skills?agent_id=agent_pi_operator&team_id=team_core"
        )

    assert missing.status_code == 200
    assert ready.status_code == 200
    assert missing.json()["allowed_tool_ids"] == []
    assert missing.json()["skills"][0]["linked_tools"] == ["echo"]
    assert missing.json()["skills"][0]["missing_linked_tools"] == ["echo"]
    assert missing.json()["skills"][0]["linked_tools_ready"] is False
    assert ready.json()["allowed_tool_ids"] == ["echo"]
    assert ready.json()["skills"][0]["missing_linked_tools"] == []
    assert ready.json()["skills"][0]["linked_tools_ready"] is True


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


class MultiSpeakerPi:
    def __init__(self):
        self.calls = []

    async def stream(self, prompt: str, *, session_id: str, options=None):
        from adapter.pi_client import PiEvent
        agent_hint = (options or {}).get("instructions") or ""
        self.calls.append({"prompt": prompt, "session_id": session_id, "options": options})
        speaker = "speaker"
        if "Pi Agent" in agent_hint:
            speaker = "operator"
        if "Group Teammate" in agent_hint:
            speaker = "teammate"
        yield PiEvent("run.started", {"run_id": f"run-{len(self.calls)}"})
        yield PiEvent("message.delta", {"delta": f"{speaker} response"})
        yield PiEvent("message.completed", {"message_id": f"done-{len(self.calls)}"})


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


def test_global_activity_feed_uses_safe_metadata(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()
    app.state.pi = CapturingPi()

    with TestClient(app) as client:
        chat = client.post(
            "/api/sessions/sess-global-activity/chat/stream",
            json={"input": "Private global prompt must not be stored"},
        )
        assert chat.status_code == 200
        home = client.get("/api/home").json()
        activity = client.get("/api/activity").json()["activity"]

    assert home["activity_feed"]
    assert home["activity"]
    assert activity
    assert "chat.started" in [item["event_type"] for item in activity]
    assert "chat.completed" in [item["event_type"] for item in activity]
    assert "Private global prompt" not in str(home)
    assert "Private global prompt" not in str(activity)
    required_keys = {"event_type", "status", "source", "summary", "created_at"}
    assert all(required_keys <= set(item) for item in activity)


def test_audit_timeline_merges_redacted_approvals_and_activity(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    def approvals(*, history: bool = False):
        if history:
            return [{
                "id": "req-decided",
                "source": "ToolGate",
                "severity": "low",
                "title": "Completed request",
                "details": "Already reviewed",
                "binding": {"type": "tool", "id": "echo", "version": "1", "digest": "abc"},
                "decision": "approved",
                "decided_at": "2026-01-01T00:00:00+00:00",
                "decided_by": "Owner",
                "created_at": "2026-01-01T00:00:00+00:00",
            }]
        return [{
            "id": "req-pending",
            "source": "ToolGate",
            "severity": "medium",
            "title": "Run echo token=abc123",
            "details": "Approval required with https://private.example/path",
            "binding": {
                "type": "tool",
                "id": "echo",
                "version": "1",
                "digest": "sha256:abc",
            },
            "created_at": "2026-01-02T00:00:00+00:00",
        }]

    monkeypatch.setattr(app.state.gates, "approvals", approvals)
    main._record_activity(
        "agent_pi_operator",
        event_type="tool.arguments",
        status="failed",
        source="ToolGate",
        summary="raw tool arguments token=abc123 https://private.example/path",
        team_id="team_core",
        ref_type="tool",
        ref_id="echo",
    )

    with TestClient(app) as client:
        audit = client.get("/api/audit").json()["events"]

    assert audit
    assert {"time", "risk", "source", "status", "event_type", "action_summary"} <= set(audit[0])
    joined = str(audit)
    assert "token=abc123" not in joined
    assert "https://private.example" not in joined
    assert "raw tool arguments" not in joined.lower()
    assert any(item["event_type"] == "approval.pending" for item in audit)
    assert any(item["event_type"] == "approval.decided" for item in audit)
    assert any(item["event_type"] == "tool.arguments" for item in audit)


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


def test_workroom_snapshot_aggregates_safe_team_metadata(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()
    app.state.pi = CapturingPi()

    with TestClient(app) as client:
        main._ensure_registry_seeded()
        app.state.sessions["sess-team"] = {
            "id": "sess-team",
            "title": "Core workroom",
            "agent_id": "agent_pi_operator",
            "team_id": "team_core",
            "created_at": "2026-01-01T00:00:00+00:00",
            "updated_at": "2026-01-01T00:05:00+00:00",
        }
        app.state.messages["sess-team"] = [
            {
                "id": "u-secret",
                "role": "user",
                "content": "secret project prompt",
                "created_at": "2026-01-01T00:00:00+00:00",
            }
        ]
        app.state.jobs["job-team"] = {
            "id": "job-team",
            "name": "Team briefing",
            "schedule": "0 8 * * *",
            "timezone": "UTC",
            "prompt": "secret automation prompt",
            "agent_id": "agent_pi_operator",
            "team_id": "team_core",
            "paused": False,
            "approval_status": "not_required",
            "required_tool_ids": [],
            "required_memory_scopes": [],
            "runs": 0,
            "history": "------------",
            "run_history": [],
        }
        snapshot = client.get("/api/workrooms/team_core").json()
        listing = client.get("/api/workrooms").json()["workrooms"]

    assert snapshot["team"]["id"] == "team_core"
    assert snapshot["orchestrator"]["id"] == "agent_pi_operator"
    assert snapshot["members"][0]["id"] == "agent_pi_operator"
    assert snapshot["sessions"][0]["id"] == "sess-team"
    assert snapshot["sessions"][0]["message_count"] == 1
    assert snapshot["automations"][0]["id"] == "job-team"
    assert listing[0]["id"] == "team_core"
    assert "secret project prompt" not in str(snapshot)
    assert "secret automation prompt" not in str(snapshot)
    assert "prompt" not in str(snapshot).lower()


def test_workroom_session_creation_validates_team_membership(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        main._ensure_registry_seeded()
        created = client.post(
            "/api/workrooms/team_core/sessions",
            json={"title": "Core room"},
        )
        outsider = client.post(
            "/api/agents",
            json={
                "name": "Outside Agent",
                "purpose": "Not a member of the core team.",
            },
        ).json()
        rejected = client.post(
            "/api/workrooms/team_core/sessions",
            json={"agent_id": outsider["id"]},
        )
        activity = client.get("/api/workrooms/team_core").json()["activity"]

    assert created.status_code == 200
    assert created.json()["team_id"] == "team_core"
    assert rejected.status_code == 403
    assert "workroom.session_created" in [item["event_type"] for item in activity]


def test_delegated_task_queue_uses_safe_metadata_and_registry_grants(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        main._ensure_registry_seeded()
        created = client.post(
            "/api/tasks",
            json={
                "title": "Review workroom surface",
                "summary": "Check the team room UI for safe metadata only.",
                "agent_id": "agent_pi_operator",
                "team_id": "team_core",
                "priority": "high",
                "risk": "medium",
                "source_session_id": "sess-secret",
                "source_message_id": "msg-secret",
            },
        )
        task_id = created.json()["id"]
        listed = client.get("/api/tasks?team_id=team_core").json()["tasks"]
        workroom = client.get("/api/workrooms/team_core").json()
        updated = client.patch(
            f"/api/tasks/{task_id}",
            json={"status": "done"},
        )
        activity = client.get("/api/activity?team_id=team_core").json()["activity"]

    assert created.status_code == 200
    assert created.json()["status"] == "queued"
    assert created.json()["priority"] == "high"
    assert created.json()["risk"] == "medium"
    assert listed[0]["id"] == task_id
    assert workroom["tasks"][0]["id"] == task_id
    assert workroom["readiness"]["task_count"] == 1
    assert updated.json()["status"] == "done"
    assert updated.json()["completed_at"]
    assert "task.created" in [item["event_type"] for item in activity]
    assert "task.updated" in [item["event_type"] for item in activity]
    assert "password" not in str(workroom).lower()
    assert "api_key" not in str(workroom).lower()
    assert "token" not in str(workroom).lower()


def test_delegated_task_session_creation_and_membership_validation(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        main._ensure_registry_seeded()
        task = client.post(
            "/api/tasks",
            json={
                "title": "Open task room",
                "summary": "Create a session but do not run Pi.",
                "agent_id": "agent_pi_operator",
                "team_id": "team_core",
            },
        ).json()
        opened = client.post(f"/api/tasks/{task['id']}/session")
        outsider = client.post(
            "/api/agents",
            json={"name": "Task Outsider", "purpose": "No core team access."},
        ).json()
        rejected = client.post(
            "/api/tasks",
            json={
                "title": "Invalid delegated task",
                "summary": "Should fail because the agent is not in the team.",
                "agent_id": outsider["id"],
                "team_id": "team_core",
            },
        )

    assert opened.status_code == 200
    assert opened.json()["task"]["status"] == "in_progress"
    assert opened.json()["task"]["session_id"].startswith("sess_")
    assert opened.json()["session"]["team_id"] == "team_core"
    assert app.state.messages[opened.json()["session"]["id"]] == []
    assert rejected.status_code == 403


def test_delegated_task_activity_tracks_task_session_chat(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()
    app.state.pi = CapturingPi()

    with TestClient(app) as client:
        main._ensure_registry_seeded()
        task = client.post(
            "/api/tasks",
            json={
                "title": "Track task progress",
                "summary": "Safe task activity probe.",
                "agent_id": "agent_pi_operator",
                "team_id": "team_core",
            },
        ).json()
        opened = client.post(f"/api/tasks/{task['id']}/session").json()
        response = client.post(
            f"/api/sessions/{opened['session']['id']}/chat/stream",
            json={"input": "Sensitive task prompt must not appear in history"},
        )
        task_after_chat = client.get(f"/api/tasks/{task['id']}").json()
        redacted = client.patch(
            f"/api/tasks/{task['id']}",
            json={"execution_summary": "Manual summary token=abc123 https://example.com/private raw command arguments"},
        ).json()
        activity = client.get(f"/api/tasks/{task['id']}/activity").json()["activity"]

    assert response.status_code == 200
    assert opened["session"]["task_id"] == task["id"]
    assert task_after_chat["status"] == "in_progress"
    assert task_after_chat["history"]
    assert task_after_chat["execution_summary"].startswith("Last task turn ok:")
    assert task_after_chat["execution_history"][0]["status"] == "ok"
    assert task_after_chat["execution_history"][0]["agent_id"] == "agent_pi_operator"
    assert task_after_chat["execution_history"][0]["output_chars"] > 0
    assert "token=abc123" not in redacted["execution_summary"]
    assert "https://" not in redacted["execution_summary"]
    assert "raw command" not in redacted["execution_summary"]
    event_types = [item["event_type"] for item in activity]
    assert "task.session_created" in event_types
    assert "task.chat_started" in event_types
    assert "task.chat_completed" in event_types
    assert "Sensitive task prompt" not in str(activity)
    assert "Sensitive task prompt" not in str(task_after_chat)
    assert "context-aware" not in str(activity)
    assert "context-aware" not in str(task_after_chat)


def test_delegated_task_dependencies_and_owner_checkpoint_gate_sessions(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        main._ensure_registry_seeded()
        dependency = client.post(
            "/api/tasks",
            json={
                "title": "Dependency task",
                "summary": "Must finish first.",
                "agent_id": "agent_pi_operator",
                "team_id": "team_core",
            },
        ).json()
        task = client.post(
            "/api/tasks",
            json={
                "title": "Checkpoint task",
                "summary": "Waits for dependency and owner review.",
                "agent_id": "agent_pi_operator",
                "team_id": "team_core",
                "depends_on_task_ids": [dependency["id"]],
                "owner_checkpoint": True,
                "checkpoint_note": "Review safe plan only.",
            },
        ).json()
        missing_dependency = client.post(
            "/api/tasks",
            json={
                "title": "Missing dependency",
                "agent_id": "agent_pi_operator",
                "team_id": "team_core",
                "depends_on_task_ids": ["task_missing"],
            },
        )
        self_dependency = client.patch(
            f"/api/tasks/{task['id']}",
            json={"depends_on_task_ids": [task["id"]]},
        )
        blocked_by_dependency = client.post(f"/api/tasks/{task['id']}/session")
        client.patch(f"/api/tasks/{dependency['id']}", json={"status": "done"})
        blocked_by_checkpoint = client.post(f"/api/tasks/{task['id']}/session")
        approved = client.patch(f"/api/tasks/{task['id']}", json={"checkpoint_status": "approved"}).json()
        opened = client.post(f"/api/tasks/{task['id']}/session")
        activity = client.get(f"/api/tasks/{task['id']}/activity").json()["activity"]

    assert task["owner_checkpoint"] is True
    assert task["checkpoint_status"] == "pending"
    assert task["blocked_dependencies"][0]["id"] == dependency["id"]
    assert missing_dependency.status_code == 404
    assert self_dependency.status_code == 422
    assert blocked_by_dependency.status_code == 409
    assert "dependencies" in blocked_by_dependency.json()["detail"]
    assert blocked_by_checkpoint.status_code == 409
    assert "checkpoint" in blocked_by_checkpoint.json()["detail"]
    assert approved["checkpoint_status"] == "approved"
    assert opened.status_code == 200
    assert opened.json()["session"]["task_id"] == task["id"]
    event_types = [item["event_type"] for item in activity]
    assert "task.checkpoint_requested" in event_types
    assert "task.checkpoint_approved" in event_types
    assert "password" not in str(opened.json()).lower()
    assert "api_key" not in str(opened.json()).lower()
    assert "token" not in str(opened.json()).lower()


def test_group_session_roster_and_speaker_are_enforced(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()
    app.state.pi = CapturingPi()

    with TestClient(app) as client:
        main._ensure_registry_seeded()
        teammate = client.post(
            "/api/agents",
            json={
                "name": "Group Speaker",
                "purpose": "Participate in scoped group rooms.",
                "team_ids": ["team_core"],
            },
        ).json()
        team = client.get("/api/teams/team_core").json()
        client.patch(
            "/api/teams/team_core",
            json={"member_agent_ids": [*team["member_agent_ids"], teammate["id"]]},
        )
        session = client.post(
            "/api/sessions",
            json={
                "title": "Group proof",
                "agent_id": "agent_pi_operator",
                "team_id": "team_core",
                "participant_agent_ids": ["agent_pi_operator", teammate["id"]],
            },
        ).json()
        switched = client.patch(
            f"/api/sessions/{session['id']}",
            json={"current_speaker_id": teammate["id"]},
        ).json()
        response = client.post(
            f"/api/sessions/{session['id']}/chat/stream",
            json={"input": "Group room turn", "agent_id": teammate["id"], "team_id": "team_core"},
        )
        outsider = client.post(
            "/api/agents",
            json={"name": "Outside Speaker", "purpose": "No group access."},
        ).json()
        rejected = client.post(
            f"/api/sessions/{session['id']}/chat/stream",
            json={"input": "Should not run", "agent_id": outsider["id"], "team_id": None},
        )
        messages = client.get(f"/api/chats/{session['id']}/messages").json()["messages"]
        listed = client.get("/api/chats").json()["sessions"]

    assert session["mode"] == "group"
    assert teammate["id"] in session["participant_agent_ids"]
    assert switched["current_speaker_id"] == teammate["id"]
    assert response.status_code == 200
    assert rejected.status_code == 403
    assistant_messages = [item for item in messages if item["role"] == "agent"]
    assert assistant_messages[-1]["agent_id"] == teammate["id"]
    row = next(item for item in listed if item["id"] == session["id"])
    assert row["mode"] == "group"
    assert row["participants"][1]["id"] == teammate["id"]
    assert "Group room turn" not in str(row)
    assert "token" not in str(row).lower()


def test_group_round_runs_each_roster_speaker_once(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()
    app.state.pi = MultiSpeakerPi()

    with TestClient(app) as client:
        teammate = client.post(
            "/api/agents",
            json={
                "name": "Group Teammate",
                "purpose": "Participate in scoped group rounds.",
                "memory_scopes": ["project-context"],
            },
        ).json()
        team = client.post(
            "/api/teams",
            json={
                "name": "Group Round Team",
                "purpose": "Test group rounds.",
                "orchestrator_agent_id": "agent_pi_operator",
                "member_agent_ids": ["agent_pi_operator", teammate["id"]],
                "memory_scopes": ["project-context"],
            },
        ).json()
        session = client.post(
            "/api/sessions",
            json={
                "title": "Group Round",
                "agent_id": "agent_pi_operator",
                "team_id": team["id"],
                "participant_agent_ids": ["agent_pi_operator", teammate["id"]],
            },
        ).json()
        result = client.post(
            f"/api/sessions/{session['id']}/group-round",
            json={"input": "Everyone give one short view.", "memory_enabled": True},
        )
        messages = client.get(f"/api/chats/{session['id']}/messages").json()["messages"]
        activity = client.get(f"/api/activity?team_id={team['id']}").json()["activity"]

    assert result.status_code == 200
    payload = result.json()
    assert payload["round"]["speaker_count"] == 2
    assert [item["agent_id"] for item in payload["round"]["responses"]] == [
        "agent_pi_operator",
        teammate["id"],
    ]
    assert len(app.state.pi.calls) == 2
    assert [message["role"] for message in messages] == ["owner", "agent", "agent"]
    assert messages[1]["agent_id"] == "agent_pi_operator"
    assert messages[2]["agent_id"] == teammate["id"]
    assert {message["content"] for message in messages[1:]} == {
        "operator response",
        "teammate response",
    }
    event_types = [item["event_type"] for item in activity]
    assert event_types.count("group.speaker_started") == 2
    assert event_types.count("group.speaker_completed") == 2
    assert "Everyone give one short view" not in str(activity)


def test_tool_draft_artifacts_are_metadata_only(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        created = client.post(
            "/api/tool-drafts",
            json={
                "title": "Create a cleanup helper",
                "purpose": "Draft a reviewed local helper. Never include token=abc123 or raw command arguments.",
                "proposed_tool_id": "Local Cleanup Helper!",
                "risk": "medium",
                "source_session_id": "sess-1",
                "source_message_id": "u1",
                "source_role": "owner",
            },
        ).json()
        listed = client.get("/api/tool-drafts").json()["drafts"]
        reviewed = client.patch(
            f"/api/tool-drafts/{created['id']}",
            json={"status": "needs_toolgate_review"},
        ).json()
        deleted = client.delete(f"/api/tool-drafts/{created['id']}")

    assert created["proposed_tool_id"] == "local.cleanup.helper"
    assert created["status"] == "draft"
    assert created["review_state"] == "needs_owner_review"
    assert listed[0]["id"] == created["id"]
    assert reviewed["status"] == "needs_toolgate_review"
    assert reviewed["review_state"] == "ready_for_toolgate_review"
    assert deleted.status_code == 200
    payload = str({**created, **reviewed}).lower()
    assert "token=abc123" not in payload
    assert "raw command" not in payload
    assert "code" not in payload
    assert "args" not in payload


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


def test_automation_delivery_policy_stores_safe_metadata_only():
    reset_state()
    with TestClient(app) as client:
        created = client.post(
            "/api/jobs",
            json={
                "name": "Desktop Notify Plan",
                "schedule": "0 9 * * *",
                "prompt": "private delivery prompt",
                "delivery_policy": "allowlisted",
                "delivery_targets": ["desktop-main", "phone personal"],
            },
        ).json()
        unsafe_url = client.post(
            "/api/jobs",
            json={
                "name": "Bad Delivery",
                "schedule": "0 10 * * *",
                "prompt": "do not store endpoint",
                "delivery_policy": "allowlisted",
                "delivery_targets": ["https://example.com/hook"],
            },
        )
        unsafe_secret = client.post(
            "/api/jobs",
            json={
                "name": "Bad Secret Delivery",
                "schedule": "0 11 * * *",
                "prompt": "do not store token",
                "delivery_policy": "owner_confirmation",
                "delivery_targets": ["telegram token abc"],
            },
        )
        listed = next(
            item for item in client.get("/api/automations").json()["automations"] if item["id"] == created["id"]
        )

    assert created["delivery_policy"] == "allowlisted"
    assert created["delivery_targets"] == ["desktop-main", "phone personal"]
    assert created["delivery_target_count"] == 2
    assert listed["delivery_targets"] == ["desktop-main", "phone personal"]
    assert "private delivery prompt" not in str(listed)
    assert unsafe_url.status_code == 422
    assert unsafe_secret.status_code == 422


def test_automation_owner_confirmation_uses_toolgate_request_without_raw_prompt():
    reset_state()
    app.state.pi = BlockedJobPi()
    with TestClient(app) as client:
        created = client.post(
            "/api/jobs",
            json={
                "name": "Approval Required",
                "schedule": "0 9 * * *",
                "prompt": "private automation prompt",
                "approval_policy": "owner_confirmation",
            },
        ).json()
        facade_pending = next(
            item for item in client.get("/api/automations").json()["automations"] if item["id"] == created["id"]
        )
        blocked = client.post(f"/api/jobs/{created['id']}/run").json()
        request = app.state.gates.requests[created["approval_request_id"]]
        decision = client.post(
            f"/api/approvals/{created['approval_request_id']}/decision",
            json={"decision": "approved"},
        ).json()
        facade_active = next(
            item for item in client.get("/api/automations").json()["automations"] if item["id"] == created["id"]
        )

    assert created["approval_status"] == "pending"
    assert created["paused"] is True
    assert facade_pending["status"] == "pending_approval"
    assert blocked["last_result"]["status"] == "blocked"
    assert request["kind"] == "automation_schedule"
    assert request["payload"]["subject_id"] == created["id"]
    assert request["payload"]["prompt_digest"]
    assert "private automation prompt" not in str(request)
    assert decision["automation_status"] == "scheduled"
    assert facade_active["status"] == "active"
    assert facade_active["approval_status"] == "approved"
    assert facade_active["next"] != "—"


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
