from __future__ import annotations

import asyncio
import json
import sqlite3
import threading

from fastapi.testclient import TestClient

from adapter import main
from adapter.gates import GateClients
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
            {
                "id": "approval.test-echo",
                "name": "Approval Test Echo",
                "description": "Harmless owner-confirmation echo drill",
                "status": "active",
                "authorization": "owner_confirmation",
                "inputs": [{"name": "value", "type": "string", "required": True}],
                "outputs": [{"name": "result", "type": "object"}],
                "execution": {"type": "local_echo"},
                "policy": {"usage_limits": {"max_per_minute": 3}},
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
            "provider_url": "https://private.example/tool",
            "credentials": {"api_key": "abc123"},
            "raw_args": {"token": "abc123"},
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

    def invoke_tool(self, tool_id: str, *, args: dict, execution_key: str, approval_request_id: str | None = None):
        self.invoked_tool = {
            "tool_id": tool_id,
            "args": args,
            "execution_key": execution_key,
            "approval_request_id": approval_request_id,
        }
        if not approval_request_id:
            request = self.create_admin_request(
                kind="tool_verification",
                title=f"Run {tool_id}",
                details="Owner confirmation is required for this exact immutable tool invocation.",
                payload={
                    "subject_type": "tool",
                    "subject_id": tool_id,
                    "argument_digest": "fake-digest",
                },
                severity="warning",
            )
            return {
                "code": "CONFIRMATION_REQUIRED",
                "request_id": request["id"],
                "expires_at": "2026-01-02T00:01:00+00:00",
            }
        request = self.request_status(approval_request_id)
        if not request or request.get("status") != "approved":
            raise RuntimeError("approval invalid")
        return {"code": "OK", "message": "Tool completed", "result": {"result": args}}

    def memory_context(self, query: str, *, agent_id: str | None = None, team_id: str | None = None):
        self.memory_agent_id = agent_id
        self.memory_team_id = team_id
        self.memory_actor_id = self._memory_store_id(agent_id, team_id)
        return {"memories": [{"text": "Owner prefers concise answers", "confidence": "high"}], "entities": []}

    def record_transcript(self, session_id: str, messages: list[dict], *, agent_id: str | None = None, team_id: str | None = None):
        self.recorded = {
            "session_id": session_id,
            "messages": messages,
            "agent_id": agent_id,
            "team_id": team_id,
            "memory_actor_id": self._memory_store_id(agent_id, team_id),
        }
        return {"status": "ok"}

    def write_memory_candidate(self, candidate: dict):
        self.memory_candidate = candidate
        return {"status": "ok", "id": "mem-approved", "memory_type": candidate.get("memory_type") or "context"}

    def update_toolgate_execution_scopes(self, scopes: list[str]):
        self.synced_toolgate_scopes = scopes
        self.synced_toolgate_scope_history = [*getattr(self, "synced_toolgate_scope_history", []), scopes]
        return {"id": "agent-key", "scopes": scopes}

    @staticmethod
    def _toolgate_store_id(agent_id: str | None, team_id: str | None = None) -> str:
        if not agent_id:
            return ""
        return f"{agent_id}@{team_id}" if team_id else agent_id

    def toolgate_execution_status(self, agent_id: str | None = None, team_id: str | None = None):
        self.status_agent_id = agent_id
        self.status_team_id = team_id
        store_id = self._toolgate_store_id(agent_id, team_id)
        scopes = getattr(self, "toolgate_private_key_scopes", {}).get(store_id, getattr(self, "synced_toolgate_scopes", []))
        return {"id": "agent-key", "scopes": scopes}

    def toolgate_agent_keys(self):
        return getattr(self, "toolgate_keys", [])

    def ensure_toolgate_agent_execution_key(self, agent_id: str, scopes: list[str], team_id: str | None = None):
        store_id = self._toolgate_store_id(agent_id, team_id)
        label = f"AgentGate:{agent_id}@{team_id}" if team_id else f"AgentGate:{agent_id}"
        self.ensured_toolgate = {"agent_id": agent_id, "team_id": team_id, "scopes": scopes}
        self.toolgate_private_keys = {
            **getattr(self, "toolgate_private_keys", {}),
            store_id: f"tgx_fake_private_key_{store_id}_1234567890",
        }
        self.toolgate_private_key_scopes = {
            **getattr(self, "toolgate_private_key_scopes", {}),
            store_id: scopes,
        }
        self.toolgate_keys = [
            row for row in getattr(self, "toolgate_keys", []) if row.get("name") != label
        ] + [{"id": f"tg-{store_id}", "name": label, "status": "active", "scopes": scopes}]
        return {"status": "cached", "agent_id": agent_id, "team_id": team_id or ""}

    def toolgate_agent_execution_key(self, agent_id: str | None, team_id: str | None = None):
        return getattr(self, "toolgate_private_keys", {}).get(self._toolgate_store_id(agent_id, team_id), "")

    def has_toolgate_agent_execution_key(self, agent_id: str, team_id: str | None = None):
        return self._toolgate_store_id(agent_id, team_id) in getattr(self, "toolgate_private_keys", {"agent_pi_operator": "tgx_fake"})

    def forget_toolgate_agent_execution_key(self, agent_id: str, team_id: str | None = None):
        store_id = self._toolgate_store_id(agent_id, team_id)
        self.forgot_toolgate_agent_id = store_id
        self.toolgate_private_keys = {
            key: value for key, value in getattr(self, "toolgate_private_keys", {}).items() if key != store_id
        }

    def forget_toolgate_agent_execution_keys_for_agent(self, agent_id: str):
        self.forgot_toolgate_agent_id = agent_id
        prefix = f"{agent_id}@"
        self.toolgate_private_keys = {
            key: value
            for key, value in getattr(self, "toolgate_private_keys", {}).items()
            if key != agent_id and not key.startswith(prefix)
        }

    def revoke_toolgate_agent_key(self, key_id: str):
        self.revoked_toolgate_key_ids = [*getattr(self, "revoked_toolgate_key_ids", []), key_id]
        self.toolgate_keys = [
            {**row, "status": "revoked"} if row.get("id") == key_id else row
            for row in getattr(self, "toolgate_keys", [])
        ]
        return {"ok": True}

    def memorygate_agent_keys(self):
        return getattr(self, "memorygate_keys", [])

    @staticmethod
    def _memory_store_id(agent_id: str | None, team_id: str | None = None) -> str:
        if not agent_id:
            return ""
        return f"{agent_id}@{team_id}" if team_id else agent_id

    def ensure_memorygate_agent_read_key(self, agent_id: str, team_id: str | None = None):
        store_id = self._memory_store_id(agent_id, team_id)
        self.ensured_memorygate_agent_id = store_id
        self.memorygate_private_keys = {
            *getattr(self, "memorygate_private_keys", set()),
            store_id,
        }
        self.memorygate_keys = [
            row for row in getattr(self, "memorygate_keys", []) if row.get("label") != f"AgentGate:{store_id}"
        ] + [{"id": f"mg-{store_id}", "label": f"AgentGate:{store_id}", "agent_id": store_id, "revoked": False}]
        return {"status": "cached", "agent_id": agent_id, "team_id": team_id or "", "memory_actor_id": store_id}

    def has_memorygate_agent_read_key(self, agent_id: str, team_id: str | None = None):
        return self._memory_store_id(agent_id, team_id) in getattr(self, "memorygate_private_keys", {"agent_pi_operator"})

    def forget_memorygate_agent_read_key(self, agent_id: str, team_id: str | None = None):
        store_id = self._memory_store_id(agent_id, team_id)
        self.memorygate_private_keys = {
            item for item in getattr(self, "memorygate_private_keys", set()) if item != store_id
        }

    def forget_memorygate_agent_read_keys_for_agent(self, agent_id: str):
        prefix = f"{agent_id}@"
        self.memorygate_private_keys = {
            item
            for item in getattr(self, "memorygate_private_keys", set())
            if item != agent_id and not item.startswith(prefix)
        }

    def revoke_memorygate_agent_key(self, key_id: str):
        self.revoked_memorygate_key_ids = [*getattr(self, "revoked_memorygate_key_ids", []), key_id]
        self.memorygate_keys = [
            {**row, "revoked": True} if row.get("id") == key_id else row
            for row in getattr(self, "memorygate_keys", [])
        ]
        return {"status": "ok"}


def reset_state():
    app.state.sessions = {"sess-1": {"id": "sess-1", "title": "Real session", "created_at": "2026-01-01T00:00:00+00:00", "updated_at": "2026-01-02T00:00:00+00:00"}}
    app.state.messages = {"sess-1": [{"id": "u1", "role": "user", "content": "hello", "created_at": "2026-01-01T00:00:00+00:00"}, {"id": "a1", "role": "assistant", "content": "hi", "created_at": "2026-01-01T00:01:00+00:00"}]}
    app.state.jobs = {"job-1": {"id": "job-1", "name": "Briefing", "schedule": "0 8 * * *", "prompt": "brief me", "paused": False, "last_run_at": None, "next_run_at": "2026-01-04T08:00:00+00:00"}}
    app.state.gates = FakeGates()
    app.state.agents = {}
    app.state.teams = {}
    app.state.tool_drafts = {}
    app.state.app_workspaces = {}
    app.state.app_artifacts = {}
    app.state.notification_channels = {}
    app.state.memory_candidates = {}
    app.state.active_job_runs = {}
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


def test_registry_export_import_is_metadata_only(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        created = client.post(
            "/api/agents",
            json={
                "name": "Portable Worker",
                "purpose": "Safe metadata export probe.",
                "soul": "Never reveal token=abc123 or https://private.example/path.",
                "tool_ids": ["echo"],
                "memory_scopes": ["project-context"],
            },
        ).json()
        team = client.post(
            "/api/teams",
            json={
                "name": "Portable Team",
                "purpose": "Safe team export probe.",
                "orchestrator_agent_id": created["id"],
                "member_agent_ids": [created["id"]],
                "tool_ids": ["echo"],
                "memory_scopes": ["project-context"],
            },
        ).json()
        exported = client.get("/api/registry/export").json()
        preview = client.post("/api/registry/import", json={**exported, "apply": False}).json()

    assert exported["contents"]["agents"] == 2
    assert exported["contents"]["teams"] == 2
    assert "raw gate keys" in exported["excluded"]
    assert "automation prompts" in exported["excluded"]
    assert "chat transcripts" in exported["excluded"]
    assert preview["summary"]["updates"] == 4
    assert "_agent_profiles" not in preview
    assert "_team_profiles" not in preview
    assert "token=abc123" not in str(exported)
    assert "https://private.example" not in str(exported)
    assert "tgx_" not in str(exported)
    assert "mg_read_" not in str(exported)
    assert "api_key" not in str(exported).lower()

    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "restore.sqlite3")
    reset_state()
    with TestClient(app) as client:
        applied = client.post("/api/registry/import", json={**exported, "apply": True}).json()
        agents = client.get("/api/agents").json()["agents"]
        teams = client.get("/api/teams").json()["teams"]

    assert applied["summary"]["creates"] == 2
    assert applied["summary"]["updates"] == 2
    assert any(agent["id"] == created["id"] for agent in agents)
    assert any(row["id"] == team["id"] for row in teams)
    restored = next(agent for agent in agents if agent["id"] == created["id"])
    assert restored["tool_ids"] == ["echo"]
    assert team["id"] in restored["team_ids"]


def test_app_workspace_registry_create_list_patch_delete(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        main._ensure_registry_seeded()
        client.patch(
            "/api/agents/agent_pi_operator",
            json={"tool_ids": ["echo"], "memory_scopes": ["project-context"]},
        )
        created = client.post(
            "/api/app-workspaces",
            json={
                "name": "Budget Tracker",
                "purpose": "Metadata-only planning record.",
                "app_type": "dashboard",
                "required_tool_ids": ["echo"],
                "required_memory_scopes": ["project-context"],
            },
        )
        workspace = created.json()
        listed = client.get("/api/app-workspaces").json()
        patched = client.patch(
            f"/api/app-workspaces/{workspace['id']}",
            json={"status": "planning", "progress_summary": "Wireframe reviewed."},
        )

    assert created.status_code == 200
    assert workspace["status"] == "draft"
    assert workspace["required_tool_ids"] == ["echo"]
    assert workspace["required_memory_scopes"] == ["project-context"]
    assert listed["summary"]["total"] >= 1
    assert listed["summary"]["active"] >= 1
    assert any(row["id"] == workspace["id"] for row in listed["workspaces"])
    assert listed["safety"]["mode"] == "metadata_only"
    assert patched.status_code == 200
    assert patched.json()["status"] == "planning"
    assert patched.json()["progress_summary"] == "Wireframe reviewed."

    app.state.app_workspaces = {}
    main._load_registry()
    assert app.state.app_workspaces[workspace["id"]]["name"] == "Budget Tracker"

    with TestClient(app) as client:
        deleted = client.delete(f"/api/app-workspaces/{workspace['id']}")
    assert deleted.status_code == 200
    assert deleted.json() == {"deleted": True}

    app.state.app_workspaces = {}
    main._load_registry()
    assert workspace["id"] not in app.state.app_workspaces


def test_app_workspace_rejects_ungranted_requirements(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        main._ensure_registry_seeded()
        response = client.post(
            "/api/app-workspaces",
            json={
                "name": "Unsafe App",
                "required_tool_ids": ["danger.write"],
                "required_memory_scopes": ["private-journal"],
            },
        )

    assert response.status_code == 403
    assert "missing tool grants" in response.text
    assert "missing memory scopes" in response.text


def test_app_workspace_responses_redact_prompts_secrets_paths_and_urls(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        main._ensure_registry_seeded()
        response = client.post(
            "/api/app-workspaces",
            json={
                "name": "Private Build",
                "purpose": "raw prompt token=abc123 https://private.example/app path=/home/private/app",
                "app_type": "internal tool",
                "progress_summary": "secret bearer abc123 file=/tmp/generated-app",
                "raw_prompt": "should not persist",
                "secret": "should not persist",
                "host_path": "/home/private/nope",
            },
        )
        listed = client.get("/api/app-workspaces")

    assert response.status_code == 200
    body = json.dumps({"created": response.json(), "listed": listed.json()}).lower()
    for forbidden in [
        "raw prompt",
        "should not persist",
        "abc123",
        "https://private.example",
        "/home/private",
        "/tmp/generated-app",
        "bearer abc123",
    ]:
        assert forbidden not in body
    assert "[redacted" in body


def test_app_workspace_artifact_registry_create_list_patch_delete(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        main._ensure_registry_seeded()
        workspace = client.post(
            "/api/app-workspaces",
            json={"name": "Gallery Host", "purpose": "Metadata-only artifact gallery."},
        ).json()
        created = client.post(
            f"/api/app-workspaces/{workspace['id']}/artifacts",
            json={
                "name": "Dashboard Spec",
                "artifact_type": "spec",
                "status": "draft",
                "risk_level": "medium",
                "summary": "Reviewer-visible metadata only.",
            },
        )
        artifact = created.json()["artifacts"][0]
        listed = client.get(f"/api/app-workspaces/{workspace['id']}/artifacts")
        patched = client.patch(
            f"/api/app-workspaces/{workspace['id']}/artifacts/{artifact['id']}",
            json={"status": "review_ready", "review_status": "needs_review", "summary": "Ready for metadata review."},
        )
        drilldown = client.get(f"/api/workstream/refs/app_artifact/{artifact['id']}")

    assert created.status_code == 200
    assert created.json()["summary"]["total"] == 1
    assert artifact["workspace_id"] == workspace["id"]
    assert artifact["artifact_type"] == "spec"
    assert artifact["created_by_agent_id"] == "agent_pi_operator"
    assert created.json()["safety"]["mode"] == "metadata_only"
    assert created.json()["safety"]["files_created"] is False
    assert created.json()["safety"]["file_contents_included"] is False
    assert created.json()["safety"]["code_executed"] is False
    assert listed.json()["summary"]["draft"] == 1
    assert patched.status_code == 200
    assert patched.json()["summary"]["review_ready"] == 1
    assert patched.json()["artifacts"][0]["summary"] == "Ready for metadata review."
    assert drilldown.status_code == 200
    assert drilldown.json()["detail"]["id"] == artifact["id"]
    assert drilldown.json()["detail"]["workspace_id"] == workspace["id"]
    assert drilldown.json()["safety"]["mode"] == "metadata_only"

    app.state.app_artifacts = {}
    main._load_registry()
    assert app.state.app_artifacts[artifact["id"]]["name"] == "Dashboard Spec"

    with TestClient(app) as client:
        deleted = client.delete(f"/api/app-workspaces/{workspace['id']}/artifacts/{artifact['id']}")
    assert deleted.status_code == 200
    assert deleted.json()["deleted"] is True
    assert deleted.json()["summary"]["total"] == 0

    app.state.app_artifacts = {}
    main._load_registry()
    assert artifact["id"] not in app.state.app_artifacts


def test_app_workspace_artifacts_missing_workspace_404(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        main._ensure_registry_seeded()
        listed = client.get("/api/app-workspaces/appws_missing/artifacts")
        created = client.post(
            "/api/app-workspaces/appws_missing/artifacts",
            json={"name": "Missing", "artifact_type": "spec"},
        )

    assert listed.status_code == 404
    assert created.status_code == 404


def test_app_workspace_artifacts_redact_tokens_urls_paths_and_raw_code(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        main._ensure_registry_seeded()
        workspace = client.post(
            "/api/app-workspaces",
            json={"name": "Safe Gallery", "purpose": "Metadata only."},
        ).json()
        response = client.post(
            f"/api/app-workspaces/{workspace['id']}/artifacts",
            json={
                "name": "Preview https://private.example/mockup token=abc123 path=/home/private/app",
                "artifact_type": "preview_stub",
                "summary": "raw_code=print(secret) file=/tmp/app.py bearer abc123 ```const token = 'abc123'```",
                "review_status": "needs_review",
                "raw_code": "print('should not persist')",
                "host_path": "/home/private/nope",
                "url": "https://private.example/nope",
            },
        )
        listed = client.get(f"/api/app-workspaces/{workspace['id']}/artifacts")

    assert response.status_code == 200
    body = json.dumps({"created": response.json(), "listed": listed.json()}).lower()
    for forbidden in [
        "abc123",
        "https://private.example",
        "/home/private",
        "/tmp/app.py",
        "print('should not persist')",
        "print(secret)",
        "const token",
        "bearer abc123",
    ]:
        assert forbidden not in body
    assert "[redacted" in body
    assert response.json()["safety"]["host_paths_accepted"] is False
    assert response.json()["safety"]["raw_code_included"] is False


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
                "voice_profile": {
                    "tone": "warm guide",
                    "pace": "measured",
                    "formality": "casual professional",
                    "interaction_style": "asks one clear question when blocked",
                    "tts_hint": "sample=/home/private/voice.wav",
                    "call_behavior": "token=abc123 should disappear",
                    "raw_sample_path": "/tmp/nope.wav",
                },
                "expression_profile": {
                    "sidecar_mode": "metadata_only",
                    "voice_sidecar": "local voice presenter",
                    "avatar_sidecar": "prometheus-avatar",
                    "read_aloud": "owner_triggered",
                    "call_mode": "push_to_talk",
                    "mic_policy": "push_to_talk",
                    "camera_policy": "owner_started",
                    "expression_analysis": "metadata_only",
                    "idle_animation": "subtle",
                    "safety_notes": "No always-on mic. credential=abc123 and https://private.example should redact.",
                    "asset_path": "/home/private/avatar.glb",
                },
                "personality": ["kind", "kind", "evidence-first", ""],
                "appearance": {
                    "mode": "character",
                    "style": "clean cel-shaded profile card",
                    "height": "170 cm",
                    "body_type": "athletic",
                    "palette": "warm neutrals",
                    "age_range": "adult-coded",
                    "attire": "simple jacket",
                    "distinguishing_features": "bright eyes, file=/home/private/avatar.png",
                    "expression_style": "calm focus",
                    "motion_style": "small idle gestures",
                    "raw_asset_path": "should-not-persist",
                },
                "profile_provenance": {
                    "origin_mode": "owner_notes",
                    "review_status": "owner_reviewed",
                    "source_type": "character_reference",
                    "source_confidence": "owner_verified",
                    "usage_policy": "transformative",
                    "asset_review_status": "approved_metadata",
                    "source_labels": ["owner seed", "https://private.example/profile"],
                    "notes_summary": "Drafted from token=abc123 owner notes.",
                    "review_checklist": [
                        "No copied source text",
                        "URL https://private.example/lore redacted",
                        "token=abc123 redacted",
                    ],
                    "raw_source_page": "should-not-persist",
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
                    "avatar_hint": "future optional avatar sidecar at https://private.example/avatar.png",
                    "raw_tool_args": {"not": "stored"},
                },
                "voice_profile": {
                    "tone": "steady",
                    "tts_hint": "voiceprint=abc123",
                },
                "expression_profile": {
                    "sidecar_mode": "unsafe-mode",
                    "mic_policy": "always_on",
                    "camera_policy": "always_on",
                    "read_aloud": "owner_triggered",
                    "safety_notes": "sample=/home/private/sample.wav",
                },
            },
        )

    assert created.status_code == 200
    assert created.json()["voice"] == "Warm, precise, and short."
    assert created.json()["voice_profile"] == {
        "tone": "warm guide",
        "pace": "measured",
        "formality": "casual professional",
        "interaction_style": "asks one clear question when blocked",
        "tts_hint": "sample=[redacted]",
        "call_behavior": "token=[redacted] should disappear",
    }
    assert created.json()["expression_profile"] == {
        "sidecar_mode": "metadata_only",
        "voice_sidecar": "local voice presenter",
        "avatar_sidecar": "prometheus-avatar",
        "read_aloud": "owner_triggered",
        "call_mode": "push_to_talk",
        "mic_policy": "push_to_talk",
        "camera_policy": "owner_started",
        "expression_analysis": "metadata_only",
        "idle_animation": "subtle",
        "safety_notes": "No always-on mic. credential=[redacted] and [redacted-url] should redact.",
    }
    assert created.json()["personality"] == ["kind", "evidence-first"]
    assert created.json()["appearance"] == {
        "mode": "character",
        "style": "clean cel-shaded profile card",
        "height": "170 cm",
        "body_type": "athletic",
        "palette": "warm neutrals",
        "age_range": "adult-coded",
        "attire": "simple jacket",
        "distinguishing_features": "bright eyes, file=[redacted]",
        "expression_style": "calm focus",
        "motion_style": "small idle gestures",
    }
    assert created.json()["profile_provenance"]["review_status"] == "owner_reviewed"
    assert created.json()["profile_provenance"]["source_type"] == "character_reference"
    assert created.json()["profile_provenance"]["source_confidence"] == "owner_verified"
    assert created.json()["profile_provenance"]["usage_policy"] == "transformative"
    assert created.json()["profile_provenance"]["asset_review_status"] == "approved_metadata"
    assert created.json()["profile_provenance"]["source_labels"] == ["owner seed", "[redacted-url]"]
    assert created.json()["profile_provenance"]["review_checklist"] == [
        "No copied source text",
        "URL [redacted-url] redacted",
        "token=[redacted] redacted",
    ]
    assert "token=abc123" not in created.json()["profile_provenance"]["notes_summary"]
    assert "raw_source_page" not in created.json()["profile_provenance"]
    assert patched.status_code == 200
    assert patched.json()["personality"] == ["steady", "playful"]
    assert patched.json()["appearance"] == {
        "visual_summary": "Simple professional portrait with no generated asset yet.",
        "avatar_hint": "future optional avatar sidecar at [redacted-url]",
    }
    assert patched.json()["voice_profile"] == {
        "tone": "steady",
        "tts_hint": "voiceprint=[redacted]",
    }
    assert patched.json()["expression_profile"] == {
        "sidecar_mode": "disabled",
        "mic_policy": "disabled",
        "camera_policy": "disabled",
        "read_aloud": "owner_triggered",
        "safety_notes": "sample=[redacted]",
        "call_mode": "disabled",
        "expression_analysis": "disabled",
        "idle_animation": "disabled",
    }
    assert patched.json()["profile_readiness"]["review_status"] == "owner_reviewed"
    payload = str(patched.json()).lower()
    assert "raw" not in payload
    assert "https://private.example" not in payload
    assert "token=abc123" not in payload


def test_agent_profile_readiness_public_projection_redacts_provenance(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        created = client.post(
            "/api/agents",
            json={
                "name": "Readiness Probe",
                "title": "Agent",
                "purpose": "Check readiness metadata.",
                "soul": "Stay inside granted scopes.",
                "voice": "Brief and careful.",
                "personality": ["safe"],
                "appearance": {"style": "clean"},
                "primary_provider": "openai-codex",
                "primary_model": "gpt-test",
                "memory_scopes": ["project-context"],
                "profile_provenance": {
                    "origin_mode": "search_notes",
                    "review_status": "not-a-real-status",
                    "source_labels": ["api_key=abc", "clean label"],
                    "notes_summary": "No bearer abc123 or https://private.example/path should leak.",
                },
            },
        ).json()
        listed = client.get("/api/agents").json()["agents"]
        fetched = client.get(f"/api/agents/{created['id']}").json()

    row = next(agent for agent in listed if agent["id"] == created["id"])
    assert row["profile_readiness"]["score"] >= 75
    assert row["profile_readiness"]["review_status"] == "unreviewed"
    assert "source_review" in row["profile_readiness"]["missing_fields"]
    assert fetched["profile_provenance"]["review_status"] == "unreviewed"
    assert fetched["profile_provenance"]["source_labels"] == ["api_key=[redacted]", "clean label"]
    combined = f"{row} {fetched}".lower()
    assert "bearer abc123" not in combined
    assert "https://private.example" not in combined
    assert "tgx_" not in combined
    assert "mg_read_" not in combined
    assert "password" not in combined
    assert "secret" not in combined


def test_sidecar_readiness_is_metadata_only_agent_projection(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        created = client.post(
            "/api/agents",
            json={
                "name": "Sidecar Probe",
                "title": "Expression agent",
                "purpose": "Check sidecar readiness metadata.",
                "soul": "Never expose private prompt text token=soul-secret.",
                "voice": "Brief and calm.",
                "personality": ["steady"],
                "appearance": {"style": "simple profile card"},
                "primary_provider": "openai-codex",
                "primary_model": "gpt-test",
                "memory_scopes": ["project-context"],
                "expression_profile": {
                    "sidecar_mode": "local_sidecar",
                    "voice_sidecar": "https://provider.example/private-voice",
                    "avatar_sidecar": "asset=/home/private/avatar.glb",
                    "read_aloud": "owner_triggered",
                    "call_mode": "push_to_talk",
                    "mic_policy": "push_to_talk",
                    "camera_policy": "owner_started",
                    "expression_analysis": "metadata_only",
                    "idle_animation": "subtle",
                    "safety_notes": "token=abc123 path=/home/private/sample.wav",
                },
                "profile_provenance": {
                    "review_status": "needs_review",
                    "source_confidence": "low",
                    "usage_policy": "needs_review",
                    "asset_review_status": "needs_review",
                    "source_labels": ["https://private.example/profile", "api_key=abc"],
                    "notes_summary": "Private notes with bearer abc123.",
                },
            },
        ).json()
        response = client.get("/api/sidecars/readiness")

    assert response.status_code == 200
    body = response.json()
    row = next(item for item in body["agents"] if item["agent_id"] == created["id"])
    assert body["summary"]["total_agents"] >= 1
    assert body["summary"]["enabled_candidates"] >= 1
    assert body["summary"]["review_needed"] >= 1
    assert body["summary"]["blocked"] >= 1
    assert body["safety"] == {
        "mode": "metadata_only",
        "media_included": False,
        "assets_included": False,
        "prompts_included": False,
        "memory_contents_included": False,
        "raw_tool_args_included": False,
        "provider_urls_included": False,
        "host_paths_included": False,
    }
    assert row == {
        "agent_id": created["id"],
        "name": "Sidecar Probe",
        "status": "draft",
        "sidecar_mode": "local_sidecar",
        "read_aloud": "owner_triggered",
        "call_mode": "push_to_talk",
        "mic_policy": "push_to_talk",
        "camera_policy": "owner_started",
        "expression_analysis": "metadata_only",
        "idle_animation": "subtle",
        "readiness": row["readiness"],
        "risk_notes": row["risk_notes"],
        "review_needed": True,
    }
    assert row["readiness"]["review_status"] == "needs_review"
    assert "source_review_pending" in row["risk_notes"]
    assert "expression_sidecar_review_pending" in row["risk_notes"]
    joined = json.dumps(body).lower()
    for forbidden in [
        "soul-secret",
        "abc123",
        "https://provider.example",
        "https://private.example",
        "/home/private",
        "private prompt",
        "source_labels",
        "notes_summary",
        "voice_sidecar",
        "avatar_sidecar",
        "safety_notes",
        "memory_scopes",
        "primary_model",
    ]:
        assert forbidden not in joined


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


def test_system_access_boundaries_report_native_key_readiness(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()
    app.state.gates.toolgate_keys = [
        {
            "id": "tg-ready",
            "name": "AgentGate:agent_pi_operator",
            "scopes": ["tool:echo"],
            "status": "active",
        },
        {
            "id": "tg-ready-team",
            "name": "AgentGate:agent_pi_operator@team_core",
            "scopes": ["tool:echo"],
            "status": "active",
        },
    ]
    app.state.gates.toolgate_private_keys = {
        "agent_pi_operator": "tgx_fake_private_key_agent_pi_operator_1234567890",
        "agent_pi_operator@team_core": "tgx_fake_private_key_agent_pi_operator_team_core_1234567890",
    }
    app.state.gates.toolgate_private_key_scopes = {
        "agent_pi_operator": ["tool:echo"],
        "agent_pi_operator@team_core": ["tool:echo"],
    }
    app.state.gates.memorygate_keys = [
        {
            "id": "mg-ready",
            "label": "AgentGate:agent_pi_operator",
            "agent_id": "agent_pi_operator",
            "revoked": False,
        },
        {
            "id": "mg-ready-team",
            "label": "AgentGate:agent_pi_operator@team_core",
            "agent_id": "agent_pi_operator@team_core",
            "revoked": False,
        },
    ]
    app.state.gates.memorygate_private_keys = {"agent_pi_operator", "agent_pi_operator@team_core"}

    with TestClient(app) as client:
        client.patch(
            "/api/agents/agent_pi_operator",
            json={"tool_ids": ["echo"], "memory_scopes": ["briefing"]},
        )
        created = client.post(
            "/api/agents",
            json={
                "name": "Missing Keys",
                "purpose": "Probe missing native key readiness.",
                "tool_ids": ["danger.write"],
                "memory_scopes": ["private-journal"],
            },
        ).json()
        system = client.get("/api/system").json()

    boundaries = system["access_boundaries"]
    ready = next(row for row in boundaries["agents"] if row["agent_id"] == "agent_pi_operator")
    missing = next(row for row in boundaries["agents"] if row["agent_id"] == created["id"])
    assert boundaries["summary"]["agents"] == 2
    assert ready["status"] == "ready"
    assert ready["toolgate_key_status"] == "ready"
    assert ready["memorygate_key_status"] == "ready"
    assert ready["memorygate_adapter_credential_status"] == "ready"
    assert ready["expected_tool_scope_count"] == 1
    assert ready["expected_memory_scope_count"] == 3
    ready_contexts = [
        row
        for row in boundaries["toolgate_contexts"]
        if row["agent_id"] == "agent_pi_operator"
    ]
    assert boundaries["summary"]["toolgate_contexts"] >= 2
    assert {row["team_id"] for row in ready_contexts} >= {None, "team_core"}
    assert all(row["status"] == "ready" for row in ready_contexts)
    ready_memory_contexts = [
        row
        for row in boundaries["memorygate_contexts"]
        if row["agent_id"] == "agent_pi_operator"
    ]
    assert boundaries["summary"]["memorygate_contexts"] >= 2
    assert {row["team_id"] for row in ready_memory_contexts} >= {None, "team_core"}
    assert all(row["status"] == "ready" for row in ready_memory_contexts)
    assert missing["status"] == "drift"
    assert missing["toolgate_key_status"] == "ready"
    assert missing["toolgate_adapter_credential_status"] == "ready"
    assert missing["memorygate_key_status"] == "missing"
    assert missing["memorygate_adapter_credential_status"] == "missing"
    assert "raw" not in str(boundaries).lower()
    assert "tgx_" not in str(boundaries)
    assert "mg_" + "read_" not in str(boundaries)


def test_system_access_boundary_repair_syncs_native_gate_contexts_without_keys(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()
    app.state.gates.toolgate_keys = []
    app.state.gates.memorygate_keys = []
    app.state.gates.toolgate_private_keys = {}
    app.state.gates.toolgate_private_key_scopes = {}
    app.state.gates.memorygate_private_keys = set()

    with TestClient(app) as client:
        main._ensure_registry_seeded()
        app.state.agents["agent_pi_operator"]["tool_ids"] = ["echo"]
        app.state.agents["agent_pi_operator"]["memory_scopes"] = ["briefing"]
        app.state.teams["team_core"]["tool_ids"] = ["approval.test-echo"]
        app.state.teams["team_core"]["memory_scopes"] = ["project-context"]
        before = client.get("/api/system").json()["access_boundaries"]["summary"]
        repaired = client.post("/api/system/access-boundaries/repair").json()
        after = client.get("/api/system").json()["access_boundaries"]["summary"]

    assert before["drift"] == 1
    assert repaired["metadata_only"] is True
    assert repaired["status"] == "ok"
    assert repaired["repair"]["toolgate_contexts_checked"] == 2
    assert repaired["repair"]["memorygate_contexts_checked"] == 2
    assert repaired["repair"]["toolgate_contexts_repaired"] == 2
    assert repaired["repair"]["memorygate_contexts_repaired"] == 2
    assert repaired["after"]["drift"] == 0
    assert after["drift"] == 0
    assert app.state.gates.toolgate_private_keys
    assert app.state.gates.memorygate_private_keys
    assert not any(
        word in str(repaired).lower()
        for word in ["tgx_", "mg_read_", "api_key", "password", "secret", "bearer"]
    )


def test_system_access_boundary_orphan_cleanup_is_metadata_only_and_conservative(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()
    app.state.gates.toolgate_keys = [
        {"id": "tg-orphan", "name": "AgentGate:agent_old", "status": "active", "scopes": ["tool:old"]},
        {"id": "tg-dup-1", "name": "AgentGate:agent_duplicate", "status": "active", "scopes": ["tool:a"]},
        {"id": "tg-dup-2", "name": "AgentGate:agent_duplicate", "status": "active", "scopes": ["tool:b"]},
        {"id": "tg-bootstrap", "name": "AgentGate Pi", "status": "active", "scopes": ["tool:*"]},
    ]
    app.state.gates.memorygate_keys = [
        {"id": "mg-orphan", "label": "AgentGate:agent_old", "agent_id": "agent_old", "revoked": False},
        {"id": "mg-unsafe", "label": "AgentGate:agent_bad", "agent_id": "other_actor", "revoked": False},
        {"id": "mg-revoked", "label": "AgentGate:agent_revoked", "agent_id": "agent_revoked", "revoked": True},
    ]
    app.state.gates.toolgate_private_keys = {"agent_old": "tgx_fake_private_key_agent_old_1234567890"}
    app.state.gates.memorygate_private_keys = {"agent_old"}

    with TestClient(app) as client:
        main._ensure_registry_seeded()
        system = client.get("/api/system").json()["access_boundaries"]
        dry = client.post(
            "/api/system/access-boundaries/orphans/cleanup",
            json={"scope": "all", "dry_run": True},
        ).json()
        dry_revoked_toolgate = getattr(app.state.gates, "revoked_toolgate_key_ids", [])
        dry_revoked_memorygate = getattr(app.state.gates, "revoked_memorygate_key_ids", [])
        live = client.post(
            "/api/system/access-boundaries/orphans/cleanup",
            json={"scope": "all", "dry_run": False},
        ).json()
        after = client.get("/api/system").json()["access_boundaries"]

    assert system["summary"]["orphaned_keys"] == 2
    assert system["summary"]["unsafe_to_touch"] == 3
    assert all("key_id" not in row for row in system["orphaned_keys"])
    assert not any(row["label"] == "AgentGate Pi" for row in system["orphaned_keys"])
    assert dry["summary"]["would_clean"] == 2
    assert dry_revoked_toolgate == []
    assert dry_revoked_memorygate == []
    assert live["summary"]["cleaned"] == 2
    assert app.state.gates.revoked_toolgate_key_ids == ["tg-orphan"]
    assert app.state.gates.revoked_memorygate_key_ids == ["mg-orphan"]
    assert "agent_old" not in app.state.gates.toolgate_private_keys
    assert "agent_old" not in app.state.gates.memorygate_private_keys
    assert after["summary"]["orphaned_keys"] == 0
    assert after["summary"]["unsafe_to_touch"] == 3
    assert not any(
        word in str(live).lower()
        for word in ["tgx_", "mg_read_", "api_key", "password", "secret", "bearer"]
    )


def test_memorygate_boundary_requires_adapter_read_credential(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()
    app.state.gates.memorygate_keys = [
        {
            "id": "mg-orphaned",
            "label": "AgentGate:agent_pi_operator",
            "agent_id": "agent_pi_operator",
            "revoked": False,
        },
    ]
    app.state.gates.memorygate_private_keys = set()

    def fail_bootstrap(agent_id: str, team_id: str | None = None):
        raise RuntimeError("native key exists but raw read credential is not recoverable")

    app.state.gates.ensure_memorygate_agent_read_key = fail_bootstrap

    with TestClient(app) as client:
        main._ensure_registry_seeded()
        client.patch("/api/agents/agent_pi_operator", json={"memory_scopes": ["briefing"]})
        system = client.get("/api/system").json()
        response = client.post(
            "/api/sessions/sess-1/chat/stream",
            json={"input": "What matters?", "memory_enabled": True},
        )

    row = next(item for item in system["access_boundaries"]["agents"] if item["agent_id"] == "agent_pi_operator")
    assert row["status"] == "drift"
    assert row["memorygate_key_count"] == 1
    assert row["memorygate_adapter_credential_status"] == "missing"
    assert "one or more MemoryGate team contexts are not ready" in row["issues"]
    context_issues = [
        issue
        for context in system["access_boundaries"]["memorygate_contexts"]
        for issue in context.get("issues", [])
    ]
    assert "adapter MemoryGate read credential unavailable" in context_issues
    assert response.status_code == 503
    assert "mg_" + "read_" not in str(system)


def test_gate_client_bootstraps_agent_memory_key_privately(monkeypatch, tmp_path):
    monkeypatch.setenv("ADAPTER_DATA_DIR", str(tmp_path))

    class RecordingGateClients(GateClients):
        def __init__(self):
            super().__init__()
            self.created_payloads = []

        def _request(self, service: str, path: str, *, method: str = "GET", payload=None, timeout: float = 8):
            read_prefix = "mg_" + "read_"
            if service == "memorygate" and path == "/auth/agent-keys" and method == "GET":
                return {"results": []}
            if service == "memorygate" and path == "/auth/agent-keys" and method == "POST":
                self.created_payloads.append(payload)
                return {
                    "id": "mg-agent-a",
                    "label": payload["label"],
                    "agent_id": payload["agent_id"],
                    "key": f"{read_prefix}test_private_key_1234567890",
                }
            raise AssertionError(f"unexpected request {service} {method} {path}")

    gates = RecordingGateClients()
    result = gates.ensure_memorygate_agent_read_key("agent_alpha")

    assert result == {
        "status": "created",
        "agent_id": "agent_alpha",
        "team_id": "",
        "memory_actor_id": "agent_alpha",
        "memorygate_key_id": "mg-agent-a",
    }
    assert gates.created_payloads == [{"label": "AgentGate:agent_alpha", "agent_id": "agent_alpha"}]
    assert gates.has_memorygate_agent_read_key("agent_alpha")
    assert gates.memorygate_agent_read_key("agent_alpha").startswith("mg_" + "read_")
    assert gates.agent_memory_key_path.exists()
    assert oct(gates.agent_memory_key_path.stat().st_mode & 0o777) == "0o600"


def test_gate_client_scopes_agent_memorygate_keys_by_team(monkeypatch, tmp_path):
    monkeypatch.setenv("ADAPTER_DATA_DIR", str(tmp_path))

    class RecordingGateClients(GateClients):
        def __init__(self):
            super().__init__()
            self.created_payloads = []

        def _request(self, service: str, path: str, *, method: str = "GET", payload=None, timeout: float = 8):
            read_prefix = "mg_" + "read_"
            if service == "memorygate" and path == "/auth/agent-keys" and method == "GET":
                return {"results": []}
            if service == "memorygate" and path == "/auth/agent-keys" and method == "POST":
                self.created_payloads.append(payload)
                return {
                    "id": f"mg-{payload['agent_id']}",
                    "label": payload["label"],
                    "agent_id": payload["agent_id"],
                    "key": f"{read_prefix}test_private_key_{payload['agent_id']}_1234567890",
                }
            raise AssertionError(f"unexpected request {service} {method} {path}")

    gates = RecordingGateClients()
    direct = gates.ensure_memorygate_agent_read_key("agent_alpha")
    team = gates.ensure_memorygate_agent_read_key("agent_alpha", team_id="team_core")

    assert direct["memory_actor_id"] == "agent_alpha"
    assert team["memory_actor_id"] == "agent_alpha@team_core"
    assert gates.created_payloads == [
        {"label": "AgentGate:agent_alpha", "agent_id": "agent_alpha"},
        {"label": "AgentGate:agent_alpha@team_core", "agent_id": "agent_alpha@team_core"},
    ]
    assert gates.memorygate_agent_read_key("agent_alpha") != gates.memorygate_agent_read_key(
        "agent_alpha",
        team_id="team_core",
    )
    assert gates.has_memorygate_agent_read_key("agent_alpha")
    assert gates.has_memorygate_agent_read_key("agent_alpha", team_id="team_core")

    gates.forget_memorygate_agent_read_keys_for_agent("agent_alpha")

    assert not gates.has_memorygate_agent_read_key("agent_alpha")
    assert not gates.has_memorygate_agent_read_key("agent_alpha", team_id="team_core")


def test_gate_client_bootstraps_agent_toolgate_key_privately(monkeypatch, tmp_path):
    monkeypatch.setenv("ADAPTER_DATA_DIR", str(tmp_path))

    class RecordingGateClients(GateClients):
        def __init__(self):
            super().__init__()
            self.created_payloads = []

        def _request(self, service: str, path: str, *, method: str = "GET", payload=None, timeout: float = 8):
            if service == "toolgate" and path == "/v2/agent-keys" and method == "GET":
                return {"results": []}
            if service == "toolgate" and path == "/v2/agent-keys" and method == "POST":
                self.created_payloads.append(payload)
                return {
                    "key": "tgx_test_private_key_1234567890",
                    "record": {
                        "id": "tg-agent-a",
                        "name": payload["name"],
                        "status": "active",
                        "scopes": payload["scopes"],
                    },
                }
            if service == "toolgate" and path == "/v2/agent-keys/tg-agent-a/scopes" and method == "PATCH":
                return {"id": "tg-agent-a", "scopes": payload["scopes"]}
            raise AssertionError(f"unexpected request {service} {method} {path}")

    gates = RecordingGateClients()
    result = gates.ensure_toolgate_agent_execution_key("agent_alpha", ["tool:echo"])
    cached = gates.ensure_toolgate_agent_execution_key("agent_alpha", ["tool:echo", "tool:notes.*"])

    assert result == {"status": "created", "agent_id": "agent_alpha", "team_id": "", "toolgate_key_id": "tg-agent-a"}
    assert cached == {"status": "cached", "agent_id": "agent_alpha", "team_id": "", "toolgate_key_id": "tg-agent-a"}
    assert gates.created_payloads == [{"name": "AgentGate:agent_alpha", "scopes": ["tool:echo"]}]
    assert gates.has_toolgate_agent_execution_key("agent_alpha")
    assert gates.toolgate_agent_execution_key("agent_alpha").startswith("tgx_")
    assert gates.agent_toolgate_key_path.exists()
    assert oct(gates.agent_toolgate_key_path.stat().st_mode & 0o777) == "0o600"


def test_gate_client_scopes_agent_toolgate_keys_by_team(monkeypatch, tmp_path):
    monkeypatch.setenv("ADAPTER_DATA_DIR", str(tmp_path))

    class RecordingGateClients(GateClients):
        def __init__(self):
            super().__init__()
            self.created_payloads = []
            self.updated_payloads = []

        def _request(self, service: str, path: str, *, method: str = "GET", payload=None, timeout: float = 8):
            if service == "toolgate" and path == "/v2/agent-keys" and method == "GET":
                return {"results": []}
            if service == "toolgate" and path == "/v2/agent-keys" and method == "POST":
                self.created_payloads.append(payload)
                suffix = str(payload["name"]).split("@", 1)[-1]
                return {
                    "key": f"tgx_test_private_key_{suffix}_1234567890",
                    "record": {
                        "id": f"tg-agent-{suffix}",
                        "name": payload["name"],
                        "status": "active",
                        "scopes": payload["scopes"],
                    },
                }
            if service == "toolgate" and method == "PATCH":
                self.updated_payloads.append({"path": path, "payload": payload})
                return {"id": path.rsplit("/", 2)[-2], "scopes": payload["scopes"]}
            raise AssertionError(f"unexpected request {service} {method} {path}")

    gates = RecordingGateClients()
    core = gates.ensure_toolgate_agent_execution_key("agent_alpha", ["tool:echo"], team_id="team_core")
    ops = gates.ensure_toolgate_agent_execution_key("agent_alpha", ["tool:danger.write"], team_id="team_ops")

    assert core["team_id"] == "team_core"
    assert ops["team_id"] == "team_ops"
    assert gates.created_payloads == [
        {"name": "AgentGate:agent_alpha@team_core", "scopes": ["tool:echo"]},
        {"name": "AgentGate:agent_alpha@team_ops", "scopes": ["tool:danger.write"]},
    ]
    assert gates.toolgate_agent_execution_key("agent_alpha", team_id="team_core") != gates.toolgate_agent_execution_key(
        "agent_alpha",
        team_id="team_ops",
    )
    assert gates.has_toolgate_agent_execution_key("agent_alpha", team_id="team_core")
    assert gates.has_toolgate_agent_execution_key("agent_alpha", team_id="team_ops")

    gates.forget_toolgate_agent_execution_keys_for_agent("agent_alpha")

    assert not gates.has_toolgate_agent_execution_key("agent_alpha", team_id="team_core")
    assert not gates.has_toolgate_agent_execution_key("agent_alpha", team_id="team_ops")


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
    assert app.state.gates.status_agent_id == "agent_pi_operator"
    assert app.state.gates.status_team_id == "team_core"
    assert "tool.health_checked" in [item["event_type"] for item in activity]
    assert "danger.write" not in str(activity)


def test_safe_toolgate_echo_drill_requires_grant_and_executes_after_approval(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        main._ensure_registry_seeded()
        denied = client.post(
            "/api/tools/approval.test-echo/drill",
            json={"agent_id": "agent_pi_operator", "team_id": "team_core"},
        )
        client.patch("/api/agents/agent_pi_operator", json={"tool_ids": ["approval.test-echo"]})
        queued = client.post(
            "/api/tools/approval.test-echo/drill",
            json={
                "agent_id": "agent_pi_operator",
                "team_id": "team_core",
                "value": "safe proof token=abc123 https://private.example/path",
            },
        )
        request_id = queued.json()["request_id"]
        approved = client.post(f"/api/approvals/{request_id}/decision", json={"decision": "approved"})
        executed = client.post(
            "/api/tools/approval.test-echo/drill",
            json={
                "agent_id": "agent_pi_operator",
                "team_id": "team_core",
                "value": "safe proof token=abc123 https://private.example/path",
                "approval_request_id": request_id,
            },
        )
        activity = client.get("/api/activity").json()["activity"]

    assert denied.status_code == 403
    assert queued.status_code == 200
    assert queued.json()["status"] == "pending_approval"
    assert queued.json()["request_id"]
    assert approved.status_code == 200
    assert executed.status_code == 200
    assert executed.json()["status"] == "executed"
    assert executed.json()["output_digest"]
    assert app.state.gates.invoked_tool["execution_key"] == "tgx_fake_private_key_agent_pi_operator@team_core_1234567890"
    combined = json.dumps({"queued": queued.json(), "executed": executed.json(), "activity": activity}).lower()
    for forbidden in ["token=abc123", "https://private.example", "safe proof"]:
        assert forbidden not in combined
    assert "tool.drill_approval_requested" in [item["event_type"] for item in activity]
    assert "tool.drill_executed" in [item["event_type"] for item in activity]


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
    body = response.json()
    assert "tool" not in body
    assert body["policy_summary"] == {
        "tool_id": "echo",
        "authorization": "owner_confirmation",
        "usage_limits": {
            "max_per_minute": 3,
            "max_per_hour": 12,
            "cooldown_seconds": 2,
            "max_runtime_seconds": 10,
        },
        "policy_status": "saved",
        "updated_at": body["policy_summary"]["updated_at"],
    }
    assert "tool.policy_updated" in [item["event_type"] for item in activity]
    combined = f"{body} {activity}".lower()
    assert "dangerous" not in combined
    assert "execution" not in combined
    assert "raw_args" not in combined
    assert "credentials" not in combined
    assert "https://private.example" not in combined
    assert "api_key" not in combined
    assert "token" not in combined


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


def test_team_orchestration_readiness_redacts_public_surfaces(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        main._ensure_registry_seeded()
        created = client.post(
            "/api/teams",
            json={
                "name": "Ops token=abc123 Cell",
                "purpose": "Coordinate https://private.example/path and bearer abc123 safely.",
                "orchestrator_agent_id": "agent_pi_operator",
                "member_agent_ids": ["agent_pi_operator"],
                "memory_scopes": ["project-context", "secret=https://memory.example"],
                "tool_ids": ["approval.test-echo", "api_key=abc"],
                "skill_ids": ["safe-skill", "token=skillsecret"],
                "orchestrator_policy": {
                    "handoff_mode": "not-real",
                    "approval_mode": "not-real",
                    "review_status": "not-real",
                    "max_parallel_tasks": 999,
                    "escalation_summary": "Stop before https://private.example or bearer xyz.",
                },
            },
        ).json()
        patched = client.patch(
            f"/api/teams/{created['id']}",
            json={
                "orchestrator_policy": {
                    "handoff_mode": "owner_confirmed",
                    "approval_mode": "toolgate_required",
                    "review_status": "owner_reviewed",
                    "max_parallel_tasks": 2,
                    "escalation_summary": "Escalate risky handoffs before running tools.",
                }
            },
        ).json()
        listed = client.get("/api/teams").json()["teams"]
        fetched = client.get(f"/api/teams/{created['id']}").json()
        workrooms = client.get("/api/workrooms").json()["workrooms"]
        workroom = client.get(f"/api/workrooms/{created['id']}").json()
        exported = client.get("/api/registry/export").json()

    team = next(item for item in listed if item["id"] == created["id"])
    room = next(item for item in workrooms if item["id"] == created["id"])
    assert created["orchestrator_policy"]["handoff_mode"] == "manual"
    assert created["orchestrator_policy"]["approval_mode"] == "toolgate_required"
    assert created["orchestrator_policy"]["review_status"] == "unreviewed"
    assert created["orchestrator_policy"]["max_parallel_tasks"] == 8
    assert patched["orchestration_readiness"]["review_status"] == "owner_reviewed"
    assert patched["orchestration_readiness"]["ready"] is True
    assert team["orchestration_readiness"]["approval_mode"] == "toolgate_required"
    assert fetched["orchestrator_policy"]["max_parallel_tasks"] == 2
    assert room["orchestration_readiness"]["member_count"] == 1
    assert workroom["readiness"]["orchestrator_is_member"] is True
    combined = f"{created} {patched} {team} {fetched} {room} {workroom} {exported}".lower()
    assert "token=abc123" not in combined
    assert "skillsecret" not in combined
    assert "api_key=abc" not in combined
    assert "https://private.example" not in combined
    assert "bearer abc123" not in combined
    assert "bearer xyz" not in combined


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


def test_automation_job_registry_outage_quarantines_without_raising(monkeypatch):
    reset_state()
    main._ensure_registry_seeded()
    app.state.pi = BlockedJobPi()
    job_id = "job-registry-outage"
    app.state.jobs[job_id] = {
        "id": job_id,
        "job_id": job_id,
        "name": "Registry Outage",
        "schedule": "0 9 * * *",
        "timezone": "UTC",
        "prompt": "should block before pi",
        "agent_id": "agent_pi_operator",
        "team_id": "team_core",
        "required_tool_ids": ["missing.tool"],
        "required_memory_scopes": [],
        "paused": False,
    }

    def fail_save(kind, item):
        raise sqlite3.OperationalError("unable to open database file")

    monkeypatch.setattr(main, "_save_registry_item", fail_save)

    for _ in range(3):
        asyncio.run(main.run_job(job_id))

    item = app.state.jobs[job_id]
    assert item["last_result"]["status"] == "blocked"
    assert item["persistence_failure_count"] >= 3
    assert item["paused"] is True
    assert item["next_run_at"] is None
    assert "registry persistence" in item["quarantine_reason"]
    assert getattr(app.state, "persistence_failures")


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
        ("GET", "systemgate", "/services"): {"results": [{
            "id": "pid-1-agentgate",
            "pid": 1,
            "name": "agentgate",
            "kind": "process-listener",
            "status": "listening",
            "listeners": ["loopback:8080", "127.0.0.1:9999", "/var/run/docker.sock", "0.0.0.0:80"],
            "cmdline": "hidden",
            "env": {"TOKEN": "hidden"},
            "path": "/home/alexeybe1kin/private",
        }]},
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
    assert overview["containers"][0]["id"] == "service-001"
    assert overview["containers"][0]["listeners"] == ["loopback:8080"]
    assert overview["containers"][0]["source"] == "systemgate-services"
    assert "stats" not in overview["containers"][0]
    assert "pid" not in overview["containers"][0]
    assert "cmdline" not in overview["containers"][0]
    assert "env" not in overview["containers"][0]
    assert "/home/" not in str(overview["containers"])
    assert "/var/run/docker.sock" not in str(overview["containers"])
    assert "127.0.0.1" not in str(overview["containers"])
    assert "0.0.0.0" not in str(overview["containers"])
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
        if path == "/services":
            raise RuntimeError("systemgate services unavailable: timeout")
        if path == "/containers":
            return {"results": [{"id": "abc123", "name": "fallback", "image": "fallback:test", "status": "running"}]}
        return {}

    monkeypatch.setattr(gates, "_request", fake_request)

    overview = gates.system_overview()

    assert overview["vitals"]["cpu_percent"] == 5
    assert overview["containers"][0]["name"] == "fallback"
    assert overview["containers"][0]["source"] == "systemgate-containers"
    assert [pkg["name"] for pkg in overview["packages"]] == ["apt", "pip", "npm"]
    assert overview["backups"] == {"latest": None, "count": 0, "results": []}
    assert overview["sources"]["services"]["status"] == "unavailable"
    assert overview["sources"]["containers"]["status"] == "ok"
    assert "services" in overview["errors"][0]["service"]


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
    assert tools.json()["total"] == 3
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


class StoppableJobPi:
    def __init__(self):
        self.started = threading.Event()
        self.stop_requested = threading.Event()

    async def stream(self, prompt: str, *, session_id: str, options=None):
        from adapter.pi_client import PiEvent
        yield PiEvent("run.started", {"run_id": "run-private-job", "session_id": session_id})
        self.started.set()
        while not self.stop_requested.is_set():
            await asyncio.sleep(0.01)
        yield PiEvent("run.stopped", {"run_id": "run-private-job", "message": "Run stopped by owner"})

    async def stop_run(self, run_id: str):
        if run_id != "run-private-job":
            raise ValueError("run not found")
        self.stop_requested.set()
        return {"run_id": run_id}


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


def test_chat_uses_team_scoped_memorygate_actor_for_context_and_transcript():
    reset_state()
    pi = CapturingPi()
    app.state.pi = pi

    with TestClient(app) as client:
        main._ensure_registry_seeded()
        team = client.post(
            "/api/teams",
            json={
                "name": "Memory Team",
                "purpose": "Team-scoped memory probe.",
                "orchestrator_agent_id": "agent_pi_operator",
                "member_agent_ids": ["agent_pi_operator"],
                "memory_scopes": ["team-journal"],
                "tool_ids": [],
                "skill_ids": [],
            },
        ).json()
        response = client.post(
            "/api/sessions/sess-1/chat/stream",
            json={"input": "What does the team remember?", "team_id": team["id"], "memory_enabled": True},
        )

    assert response.status_code == 200
    assert app.state.gates.ensured_memorygate_agent_id == f"agent_pi_operator@{team['id']}"
    assert app.state.gates.memory_agent_id == "agent_pi_operator"
    assert app.state.gates.memory_team_id == team["id"]
    assert app.state.gates.memory_actor_id == f"agent_pi_operator@{team['id']}"
    assert app.state.gates.recorded["agent_id"] == "agent_pi_operator"
    assert app.state.gates.recorded["team_id"] == team["id"]
    assert app.state.gates.recorded["memory_actor_id"] == f"agent_pi_operator@{team['id']}"


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


def test_workstream_merges_safe_metadata_without_private_payloads(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()
    app.state.pi = CapturingPi()

    with TestClient(app) as client:
        chat = client.post(
            "/api/sessions/sess-workstream/chat/stream",
            json={"input": "private prompt token=chat-secret https://chat.example/path"},
        )
        assert chat.status_code == 200
        job = client.post(
            "/api/jobs",
            json={
                "name": "Morning proof",
                "schedule": "0 8 * * *",
                "prompt": "job secret token=job-secret https://job.example/path",
            },
        )
        assert job.status_code == 200
        task = client.post(
            "/api/tasks",
            json={
                "title": "Safe task title",
                "summary": "task secret token=task-secret https://task.example/path",
            },
        )
        assert task.status_code == 200
        draft = client.post(
            "/api/tool-drafts",
            json={
                "title": "Debug helper",
                "purpose": "tool secret token=tool-secret https://tool.example/path",
                "proposed_tool_id": "debug.helper",
                "risk": "high",
            },
        )
        assert draft.status_code == 200
        memory = client.post(
            "/api/memory/candidates",
            json={
                "text": "memory secret token=memory-secret https://memory.example/path",
                "session_id": "sess-1",
                "source_message_id": "a1",
                "memory_type": "preference",
                "confidence": "high",
                "candidate_id": "memcand_workstream",
            },
        )
        assert memory.status_code == 200
        main._record_activity(
            "agent_pi_operator",
            event_type="automation",
            status="scheduled",
            source="AgentGate",
            summary="Automation job created: Retired proof",
            ref_type="job",
            ref_id="job_missing_from_runtime",
        )
        response = client.get("/api/workstream?limit=50")
        job_detail = client.get(f"/api/workstream/refs/job/{job.json()['id']}")
        memory_detail = client.get("/api/workstream/refs/memory_candidate/memcand_workstream")
        ghost_detail = client.get("/api/workstream/refs/job/job_missing_from_runtime")
        agent_detail = client.get("/api/workstream/refs/agent/agent_pi_operator")
        team_detail = client.get("/api/workstream/refs/team/team_core")

    assert response.status_code == 200
    assert job_detail.status_code == 200
    assert memory_detail.status_code == 200
    assert ghost_detail.status_code == 200
    assert agent_detail.status_code == 200
    assert team_detail.status_code == 200
    body = response.json()
    events = body["events"]
    kinds = {item["kind"] for item in events}
    assert {"chat", "automation", "task", "tool", "memory", "approval"} <= kinds
    assert body["counts"]["total"] == len(events)
    assert body["safety"]["mode"] == "metadata_only"
    joined = json.dumps(body).lower()
    for forbidden in [
        "chat-secret",
        "job-secret",
        "task-secret",
        "tool-secret",
        "memory-secret",
        "https://chat.example",
        "https://job.example",
        "https://task.example",
        "https://tool.example",
        "https://memory.example",
        "private prompt",
        "job secret",
        "memory secret",
        "tool secret",
    ]:
        assert forbidden not in joined
        assert forbidden not in job_detail.text.lower()
        assert forbidden not in memory_detail.text.lower()
        assert forbidden not in agent_detail.text.lower()
        assert forbidden not in team_detail.text.lower()
    job_body = job_detail.json()
    assert job_body["ref_type"] == "job"
    assert job_body["detail"]["name"] == "Morning proof"
    assert job_body["detail"]["failure_policy"]["automatic_retries"] is False
    assert job_body["safety"]["mode"] == "metadata_only"
    memory_body = memory_detail.json()
    assert memory_body["ref_type"] == "memory_candidate"
    assert memory_body["detail"]["memory_type"] == "preference"
    assert memory_body["detail"]["confidence"] == "high"
    assert "text" not in memory_body["detail"]
    assert memory_body["safety"]["mode"] == "metadata_only"
    ghost_body = ghost_detail.json()
    assert ghost_body["ref_type"] == "job"
    assert ghost_body["detail"]["available"] is False
    assert ghost_body["detail"]["state"] == "audit_only"
    assert ghost_body["events"][0]["ref_id"] == "job_missing_from_runtime"
    agent_body = agent_detail.json()
    assert agent_body["ref_type"] == "agent"
    assert agent_body["detail"]["id"] == "agent_pi_operator"
    assert "profile_readiness" in agent_body["detail"]
    assert agent_body["safety"]["mode"] == "metadata_only"
    team_body = team_detail.json()
    assert team_body["ref_type"] == "team"
    assert team_body["detail"]["id"] == "team_core"
    assert "orchestration_readiness" in team_body["detail"]
    assert team_body["safety"]["mode"] == "metadata_only"


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


def test_workroom_handoff_plan_creates_checkpointed_safe_task_shells(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        main._ensure_registry_seeded()
        helper = client.post(
            "/api/agents",
            json={
                "name": "Helper Agent",
                "purpose": "Assist with team handoffs.",
            },
        ).json()
        client.patch(
            "/api/teams/team_core",
            json={
                "member_agent_ids": ["agent_pi_operator", helper["id"]],
                "orchestrator_policy": {
                    "handoff_mode": "bounded_auto",
                    "approval_mode": "owner_checkpoint",
                    "max_parallel_tasks": 2,
                },
            },
        )
        plan = client.post(
            "/api/workrooms/team_core/handoff-plan",
            json={
                "objective": "Coordinate api_key=abc123 via https://private.invalid with raw tool arguments",
                "max_tasks": 4,
                "risk": "medium",
                "priority": "high",
                "owner_checkpoint": False,
            },
        ).json()
        outsider = client.post(
            "/api/agents",
            json={"name": "Outsider", "purpose": "Not a team member."},
        ).json()
        rejected = client.post(
            "/api/workrooms/team_core/handoff-plan",
            json={
                "objective": "bad target",
                "target_agent_ids": [outsider["id"]],
            },
        )
        workroom = client.get("/api/workrooms/team_core").json()
        activity = client.get("/api/activity?team_id=team_core").json()["activity"]

    assert plan["metadata_only"] is True
    assert plan["task_count"] == 2
    assert len(plan["objective_digest"]) == 64
    assert plan["policy"]["handoff_mode"] == "bounded_auto"
    assert all(task["owner_checkpoint"] for task in plan["tasks"])
    assert all(task["checkpoint_status"] == "pending" for task in plan["tasks"])
    assert {task["agent_id"] for task in plan["tasks"]} == {"agent_pi_operator", helper["id"]}
    assert workroom["readiness"]["task_count"] == 2
    assert rejected.status_code == 403
    combined = f"{plan} {workroom}".lower()
    assert "abc123" not in combined
    assert "api_key" not in combined
    assert "https://private.invalid" not in combined
    assert "raw tool arguments" not in combined
    assert "workroom.handoff_planned" in [item["event_type"] for item in activity]
    assert "workroom.handoff_task_created" in [item["event_type"] for item in activity]


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


def test_group_round_stream_emits_live_speaker_events(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()
    app.state.pi = MultiSpeakerPi()

    with TestClient(app) as client:
        teammate = client.post(
            "/api/agents",
            json={"name": "Group Teammate", "purpose": "Participate in live group rounds."},
        ).json()
        team = client.post(
            "/api/teams",
            json={
                "name": "Group Stream Team",
                "purpose": "Test live group round events.",
                "orchestrator_agent_id": "agent_pi_operator",
                "member_agent_ids": ["agent_pi_operator", teammate["id"]],
            },
        ).json()
        session = client.post(
            "/api/sessions",
            json={
                "title": "Group Stream",
                "agent_id": "agent_pi_operator",
                "team_id": team["id"],
                "participant_agent_ids": ["agent_pi_operator", teammate["id"]],
            },
        ).json()
        with client.stream(
            "POST",
            f"/api/sessions/{session['id']}/group-round/stream",
            json={"input": "Live safe group update."},
        ) as response:
            body = "".join(response.iter_text())
        messages = client.get(f"/api/chats/{session['id']}/messages").json()["messages"]

    assert response.status_code == 200
    assert "event: group.round.started" in body
    assert body.count("event: group.speaker.started") == 2
    assert body.count("event: message.delta") == 2
    assert body.count("event: group.speaker.completed") == 2
    assert "event: group.round.completed" in body
    assert "operator response" in body
    assert "teammate response" in body
    assert "Live safe group update" not in body
    assert [message["role"] for message in messages] == ["owner", "agent", "agent"]


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
    assert reviewed["review_state"] == "toolgate_pending"
    assert reviewed["toolgate_status"] == "pending"
    assert reviewed["toolgate_request_id"]
    assert deleted.status_code == 200
    payload = str({**created, **reviewed}).lower()
    assert "token=abc123" not in payload
    assert "raw command" not in payload
    assert "code" not in payload
    assert "args" not in payload


def test_tool_draft_review_creates_toolgate_request(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        created = client.post(
            "/api/tool-drafts",
            json={
                "title": "Create a reviewed helper",
                "purpose": "Summarize local files. Never include token=abc123 or https://private.example/path.",
                "proposed_tool_id": "Reviewed Helper",
                "risk": "high",
                "source_session_id": "sess-1",
                "source_message_id": "u1",
            },
        ).json()
        review = client.post(f"/api/tool-drafts/{created['id']}/toolgate-review").json()
        listed = client.get("/api/tool-drafts").json()["drafts"]
        app.state.gates.decide_approval(review["toolgate_request_id"], "approved")
        refreshed = client.get("/api/tool-drafts").json()["drafts"]

    request = app.state.gates.requests[review["toolgate_request_id"]]
    assert review["status"] == "needs_toolgate_review"
    assert review["review_state"] == "toolgate_pending"
    assert review["toolgate_status"] == "pending"
    assert request["kind"] == "tool_draft_review"
    assert request["severity"] == "critical"
    assert request["payload"]["subject_type"] == "tool_draft"
    assert request["payload"]["subject_id"] == created["id"]
    assert request["payload"]["metadata_only"] is True
    assert "purpose_digest" in request["payload"]
    assert "token=abc123" not in str(request)
    assert "https://private.example" not in str(request)
    assert listed[0]["toolgate_request_id"] == review["toolgate_request_id"]
    assert refreshed[0]["toolgate_status"] == "approved"
    assert refreshed[0]["review_state"] == "toolgate_approved"


def test_tool_draft_package_proposal_requires_toolgate_approval(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        created = client.post(
            "/api/tool-drafts",
            json={
                "title": "Create a safe helper",
                "purpose": "Prepare a reviewed helper package.",
                "proposed_tool_id": "Safe Helper",
                "risk": "medium",
            },
        ).json()
        blocked = client.post(f"/api/tool-drafts/{created['id']}/package-proposal")

    assert blocked.status_code == 409
    assert "approval" in blocked.text.lower()


def test_tool_draft_package_proposal_is_metadata_only_after_approval(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        created = client.post(
            "/api/tool-drafts",
            json={
                "title": "Create a safe helper",
                "purpose": "Summarize local notes. Never include token=abc123 or https://private.example/path.",
                "proposed_tool_id": "Safe Helper",
                "risk": "medium",
                "source_session_id": "sess-1",
                "source_message_id": "u1",
            },
        ).json()
        review = client.post(f"/api/tool-drafts/{created['id']}/toolgate-review").json()
        app.state.gates.decide_approval(review["toolgate_request_id"], "approved")
        proposal = client.post(f"/api/tool-drafts/{created['id']}/package-proposal").json()

    assert proposal["status"] == "package_proposed"
    assert proposal["review_state"] == "package_proposal_ready"
    package = proposal["package_proposal"]
    manifest = package["manifest"]
    assert package["digest"]
    assert manifest["schema_version"] == "agentgate.tool_package_proposal.v1"
    assert manifest["proposed_tool_id"] == "safe.helper"
    assert manifest["install_policy"] == "manual_toolgate_owned"
    assert manifest["executable_included"] is False
    assert manifest["raw_arguments_included"] is False
    assert manifest["approval"]["toolgate_request_id"] == review["toolgate_request_id"]
    assert manifest["approval"]["toolgate_status"] == "approved"
    payload = str(proposal).lower()
    assert "token=abc123" not in payload
    assert "https://private.example" not in payload
    assert "mg_" + "read_" not in payload
    assert "tgx_" not in payload
    assert "api_key" not in payload


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
                "delivery_targets": ["desktop-main", "phone-personal"],
            },
        ).json()
        unknown_label = client.post(
            "/api/jobs",
            json={
                "name": "Unknown Delivery",
                "schedule": "0 9 * * *",
                "prompt": "do not store endpoint",
                "delivery_policy": "allowlisted",
                "delivery_targets": ["kitchen display"],
            },
        )
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
        unsafe_phone = client.post(
            "/api/jobs",
            json={
                "name": "Bad Phone Delivery",
                "schedule": "0 12 * * *",
                "prompt": "do not store phone",
                "delivery_policy": "owner_confirmation",
                "delivery_targets": ["+1 555 010 1234"],
            },
        )
        listed = next(
            item for item in client.get("/api/automations").json()["automations"] if item["id"] == created["id"]
        )

    assert created["delivery_policy"] == "allowlisted"
    assert created["delivery_targets"] == ["desktop-main", "phone-personal"]
    assert created["delivery_target_count"] == 2
    assert created["approval_status"] == "pending"
    assert created["paused"] is True
    assert "delivery_policy" in created["approval_reasons"]
    assert "delivery_targets" in created["approval_reasons"]
    assert listed["delivery_targets"] == ["desktop-main", "phone-personal"]
    assert "private delivery prompt" not in str(listed)
    assert unknown_label.status_code == 422
    assert "configured" in unknown_label.text
    assert unsafe_url.status_code == 422
    assert unsafe_secret.status_code == 422
    assert unsafe_phone.status_code == 422


def test_notification_channels_are_metadata_only_and_reject_sensitive_targets(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()
    with TestClient(app) as client:
        seeded = client.get("/api/notification-channels").json()
        created = client.post(
            "/api/notification-channels",
            json={
                "label": "workstation speakers",
                "kind": "desktop",
                "status": "needs_setup",
                "description": "safe label only",
                "requires_owner_confirmation": True,
            },
        ).json()
        duplicate = client.post(
            "/api/notification-channels",
            json={"label": "workstation speakers", "kind": "desktop"},
        )
        unsafe_url = client.post(
            "/api/notification-channels",
            json={"label": "https://private.invalid/hook", "kind": "manual"},
        )
        unsafe_email = client.post(
            "/api/notification-channels",
            json={"label": "owner@example.invalid", "kind": "mobile"},
        )
        unsafe_phone = client.post(
            "/api/notification-channels",
            json={"label": "+1 555 010 9999", "kind": "mobile"},
        )
        unsafe_secret = client.post(
            "/api/notification-channels",
            json={"label": "telegram bearer token", "kind": "manual"},
        )
        unsafe_description = client.post(
            "/api/notification-channels",
            json={
                "label": "safe desktop label",
                "kind": "desktop",
                "description": "send to https://private.invalid/hook",
            },
        )
        updated = client.patch(
            f"/api/notification-channels/{created['id']}",
            json={
                "label": "workstation alerts",
                "kind": "desktop",
                "status": "available",
                "description": "safe updated label only",
            },
        ).json()
        created_job = client.post(
            "/api/jobs",
            json={
                "name": "Uses Workstation Alerts",
                "schedule": "0 9 * * *",
                "prompt": "private delivery prompt",
                "delivery_policy": "allowlisted",
                "delivery_targets": ["workstation alerts"],
            },
        ).json()
        disable_in_use = client.patch(
            f"/api/notification-channels/{created['id']}",
            json={"status": "disabled"},
        )
        delete_in_use = client.delete(f"/api/notification-channels/{created['id']}")
        listed = client.get("/api/notification-channels").json()

    assert seeded["summary"]["metadata_only"] is True
    assert {row["label"] for row in seeded["channels"]} >= {"local dashboard inbox", "desktop-main", "phone-personal"}
    assert created["label"] == "workstation speakers"
    assert created["metadata_only"] is True
    assert "endpoint" not in created
    assert "url" not in created
    assert "token" not in str(created).lower()
    assert updated["label"] == "workstation alerts"
    assert updated["status"] == "available"
    assert created_job["delivery_targets"] == ["workstation alerts"]
    assert disable_in_use.status_code == 409
    assert disable_in_use.json()["detail"]["job_count"] == 1
    assert delete_in_use.status_code == 409
    assert delete_in_use.json()["detail"]["job_ids"] == [created_job["id"]]
    assert any(row["label"] == "workstation alerts" for row in listed["channels"])
    assert not any(row["label"] == "workstation speakers" for row in listed["channels"])
    assert duplicate.status_code == 409
    assert unsafe_url.status_code == 422
    assert unsafe_email.status_code == 422
    assert unsafe_phone.status_code == 422
    assert unsafe_secret.status_code == 422
    assert unsafe_description.status_code == 422
    assert "https://private.invalid" not in str(listed)


def test_automation_auto_policy_requires_toolgate_for_tools_memory_and_delivery(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()
    with TestClient(app) as client:
        client.patch(
            "/api/agents/agent_pi_operator",
            json={"tool_ids": ["echo"], "memory_scopes": ["briefing"]},
        )
        created = client.post(
            "/api/jobs",
            json={
                "name": "Scoped Risky Auto",
                "schedule": "0 9 * * *",
                "prompt": "private risky automation prompt",
                "required_tool_ids": ["echo"],
                "required_memory_scopes": ["briefing"],
                "delivery_policy": "allowlisted",
                "delivery_targets": ["desktop-main"],
            },
        ).json()
        request = app.state.gates.requests[created["approval_request_id"]]

    assert created["approval_policy"] == "auto"
    assert created["approval_status"] == "pending"
    assert created["paused"] is True
    assert created["next"] == "—"
    assert set(created["approval_reasons"]) == {"tool_access", "memory_access", "delivery_policy", "delivery_targets"}
    assert request["payload"]["approval_reasons"] == created["approval_reasons"]
    assert request["payload"]["required_tool_count"] == 1
    assert request["payload"]["required_memory_scope_count"] == 1
    assert request["payload"]["delivery_target_count"] == 1
    assert "private risky automation prompt" not in str(request)
    assert request["payload"]["prompt_digest"]


def test_automation_risky_update_requires_fresh_toolgate_approval(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()
    with TestClient(app) as client:
        created = client.post(
            "/api/jobs",
            json={
                "name": "Approved Delivery",
                "schedule": "0 9 * * *",
                "prompt": "first private prompt",
                "delivery_policy": "allowlisted",
                "delivery_targets": ["desktop-main"],
            },
        ).json()
        first_request_id = created["approval_request_id"]
        approved = client.post(
            f"/api/approvals/{first_request_id}/decision",
            json={"decision": "approved"},
        )
        updated = client.patch(
            f"/api/jobs/{created['id']}",
            json={
                "schedule": "15 9 * * *",
                "prompt": "second private prompt",
            },
        ).json()
        second_request = app.state.gates.requests[updated["approval_request_id"]]

    assert approved.status_code == 200
    assert updated["approval_status"] == "pending"
    assert updated["paused"] is True
    assert updated["approval_request_id"] != first_request_id
    assert updated["next"] == "—"
    assert "delivery_policy" in updated["approval_reasons"]
    assert second_request["payload"]["prompt_digest"]
    assert "first private prompt" not in str(second_request)
    assert "second private prompt" not in str(second_request)


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


def test_automation_active_run_can_be_stopped_without_exposing_pi_run_id():
    reset_state()
    pi = StoppableJobPi()
    app.state.pi = pi

    with TestClient(app) as client:
        created = client.post(
            "/api/jobs",
            json={
                "name": "Stoppable Job",
                "schedule": "0 9 * * *",
                "prompt": "private long-running automation prompt",
            },
        ).json()
        result_holder = {}

        def run_job_now():
            result_holder["response"] = client.post(f"/api/jobs/{created['id']}/run")

        thread = threading.Thread(target=run_job_now)
        thread.start()
        assert pi.started.wait(timeout=3)

        running = next(
            item for item in client.get("/api/automations").json()["automations"] if item["id"] == created["id"]
        )
        stopped = client.post(f"/api/jobs/{created['id']}/stop")
        thread.join(timeout=3)
        completed = result_holder["response"].json()
        listed = next(
            item for item in client.get("/api/automations").json()["automations"] if item["id"] == created["id"]
        )

    assert stopped.status_code == 200
    assert running["status"] == "running"
    assert running["is_running"] is True
    assert running["active_run"]["status"] == "running"
    assert "run_id" not in running["active_run"]
    assert stopped.json()["status"] == "stopping"
    assert stopped.json()["active_run"]["status"] == "stopping"
    assert "run_id" not in stopped.text
    assert "run-private-job" not in stopped.text
    assert "private long-running automation prompt" not in stopped.text
    assert completed["last_result"]["status"] == "stopped"
    assert completed["is_running"] is False
    assert completed["active_run"] is None
    assert completed["failure_count"] == 0
    assert listed["status"] == "active"
    assert listed["last_result"]["status"] == "stopped"
    assert "run-private-job" not in str(completed)
    assert "private long-running automation prompt" not in str(completed["last_result"])


def test_automation_stop_without_active_run_fails_safely():
    reset_state()
    with TestClient(app) as client:
        created = client.post(
            "/api/jobs",
            json={
                "name": "Idle Job",
                "schedule": "0 9 * * *",
                "prompt": "private idle prompt",
            },
        ).json()
        stopped = client.post(f"/api/jobs/{created['id']}/stop")

    assert stopped.status_code == 404
    assert "run_id" not in stopped.text
    assert "private idle prompt" not in stopped.text


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
    assert ran["failure_policy"]["max_consecutive_failures"] == 3
    assert ran["failure_policy_status"]["remaining_before_terminal"] == 0
    assert ran["failure_policy_status"]["automatic_retries"] is False


def test_automation_failure_policy_is_owner_visible_and_bounded():
    reset_state()
    app.state.pi = FailingJobPi()
    with TestClient(app) as client:
        created = client.post(
            "/api/jobs",
            json={
                "name": "Fast Failure Probe",
                "schedule": "0 9 * * *",
                "prompt": "fail safely",
                "failure_policy": {
                    "max_consecutive_failures": 2,
                    "failure_window_hours": 12,
                    "terminal_action": "pause",
                    "retry_strategy": "none",
                },
            },
        ).json()
        first = client.post(f"/api/jobs/{created['id']}/run").json()
        second = client.post(f"/api/jobs/{created['id']}/run").json()
        rejected = client.post(
            "/api/jobs",
            json={
                "name": "Bad Failure Policy",
                "schedule": "0 9 * * *",
                "prompt": "fail safely",
                "failure_policy": {"max_consecutive_failures": 0},
            },
        )

    assert created["failure_policy"]["max_consecutive_failures"] == 2
    assert created["failure_policy"]["automatic_retries"] is False
    assert first["failure_policy_status"]["remaining_before_terminal"] == 1
    assert second["failure_count"] == 2
    assert second["paused"] is True
    assert "2 consecutive" in second["quarantine_reason"]
    assert rejected.status_code == 422


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
        client.post(
            f"/api/approvals/{created['approval_request_id']}/decision",
            json={"decision": "approved"},
        )
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


def test_chat_passes_per_agent_toolgate_execution_key_to_pi():
    reset_state()
    pi = CapturingPi()
    app.state.pi = pi

    with TestClient(app) as client:
        main._ensure_registry_seeded()
        response = client.post("/api/sessions/sess-1/chat/stream", json={"input": "use scoped key"})

    assert response.status_code == 200
    assert pi.options["toolgate_execution_key"] == "tgx_fake_private_key_agent_pi_operator@team_core_1234567890"
    assert app.state.gates.ensured_toolgate == {"agent_id": "agent_pi_operator", "team_id": "team_core", "scopes": []}


def test_chat_uses_team_scoped_toolgate_execution_key():
    reset_state()
    pi = CapturingPi()
    app.state.pi = pi

    with TestClient(app) as client:
        main._ensure_registry_seeded()
        team = client.post(
            "/api/teams",
            json={
                "name": "Ops",
                "purpose": "Restricted ops test",
                "orchestrator_agent_id": "agent_pi_operator",
                "member_agent_ids": ["agent_pi_operator"],
                "tool_ids": ["danger.write"],
                "memory_scopes": [],
                "skill_ids": [],
            },
        ).json()
        client.patch("/api/agents/agent_pi_operator", json={"tool_ids": ["echo"], "team_ids": ["team_core", team["id"]]})
        response = client.post(
            "/api/sessions/sess-1/chat/stream",
            json={"input": "use ops tool boundary", "team_id": team["id"]},
        )

    assert response.status_code == 200
    assert pi.options["toolgate_execution_key"] == f"tgx_fake_private_key_agent_pi_operator@{team['id']}_1234567890"
    assert app.state.gates.ensured_toolgate == {
        "agent_id": "agent_pi_operator",
        "team_id": team["id"],
        "scopes": ["tool:danger.write", "tool:echo"],
    }



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


def test_agentgate_memory_candidate_queue_requires_review_before_write():
    reset_state()
    with TestClient(app) as client:
        queued = client.post("/api/memory/candidates", json={
            "text": "Owner wants memory candidates reviewed first.",
            "session_id": "sess-1",
            "source_message_id": "a1",
            "memory_type": "context",
            "confidence": "high",
            "candidate_id": "memcand_pending",
            "source_role": "agent",
        })
        listed = client.get("/api/memory/candidates").json()
        assert queued.status_code == 200
        assert queued.json()["status"] == "pending"
        assert not hasattr(app.state.gates, "memory_candidate")
        assert listed["candidates"][0]["id"] == "memcand_pending"
        assert listed["candidates"][0]["status"] == "pending"
        approved = client.post("/api/memory/candidates/memcand_pending/approve")

    assert approved.status_code == 200
    assert approved.json()["candidate"]["status"] == "approved"
    assert app.state.gates.memory_candidate["text"] == "Owner wants memory candidates reviewed first."
    assert "owner-approved" in app.state.gates.memory_candidate["tags"]


def test_agentgate_memory_candidate_reject_does_not_write_memory():
    reset_state()
    with TestClient(app) as client:
        queued = client.post("/api/memory/candidates", json={
            "text": "Do not save this.",
            "session_id": "sess-1",
            "source_message_id": "a1",
            "candidate_id": "memcand_reject",
        })
        rejected = client.post("/api/memory/candidates/memcand_reject/reject")
        approved_after_reject = client.post("/api/memory/candidates/memcand_reject/approve")

    assert queued.status_code == 200
    assert rejected.status_code == 200
    assert rejected.json()["status"] == "rejected"
    assert approved_after_reject.status_code == 409
    assert not hasattr(app.state.gates, "memory_candidate")


def test_agentgate_memory_candidate_delete_removes_rejected_review_record():
    reset_state()
    with TestClient(app) as client:
        client.post("/api/memory/candidates", json={
            "text": "Remove this rejected candidate.",
            "session_id": "sess-1",
            "source_message_id": "a1",
            "candidate_id": "memcand_delete",
        })
        client.post("/api/memory/candidates/memcand_delete/reject")
        deleted = client.delete("/api/memory/candidates/memcand_delete")
        listed = client.get("/api/memory/candidates?status=all").json()

    assert deleted.status_code == 200
    assert deleted.json() == {"deleted": True, "id": "memcand_delete"}
    assert all(item.get("id") != "memcand_delete" for item in listed["candidates"])


def test_agentgate_memory_candidate_requires_source_binding():
    reset_state()
    with TestClient(app) as client:
        response = client.post("/api/memory/candidates", json={"text": "save me", "session_id": "sess-1"})
    assert response.status_code == 422
    assert "source message" in response.text.lower()


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
