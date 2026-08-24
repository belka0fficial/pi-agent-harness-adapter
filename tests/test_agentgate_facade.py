from __future__ import annotations

import asyncio
import json
import re
import sqlite3
import threading

from fastapi.testclient import TestClient

from adapter import main
from adapter.gates import GateClients
from adapter.main import app
from adapter.pi_client import PiClient, PiEvent


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


class LocalNotificationPi:
    async def stream(self, prompt: str, *, session_id: str, options: dict | None = None):
        yield PiEvent("run.started", {"run_id": "run-local-notification", "session_id": session_id})
        yield PiEvent(
            "message.delta",
            {
                "delta": (
                    "brief ready without secrets "
                    "api_key=abc123 https://private.invalid/raw"
                )
            },
        )
        yield PiEvent("message.completed", {"message_id": "msg-local-notification"})


class ApprovalRequiredPi:
    async def stream(self, prompt: str, *, session_id: str, options: dict | None = None):
        yield PiEvent("run.started", {"run_id": "run-secret-123", "session_id": session_id})
        yield PiEvent(
            "approval.required",
            {
                "run_id": "run-secret-123",
                "request_id": "req-pending",
                "tool_name": "echo",
                "summary": {
                    "action": "token=abc123 https://private.example/path /home/alexey/private memory text",
                    "raw_args": {"secret": "abc123"},
                },
            },
        )
        yield PiEvent("message.completed", {"message_id": "msg-approval"})


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
    app.state.app_preview_proposals = {}
    app.state.auxiliary_model_routes = {}
    app.state.notification_channels = {}
    app.state.notification_deliveries = {}
    app.state.memory_candidates = {}
    app.state.character_sources = {}
    app.state.sidecar_runtimes = {}
    app.state.active_job_runs = {}
    app.state.approval_runs = {}
    app.state.owner_sessions = {}


def approve_team_policy(client: TestClient, team_id: str) -> dict:
    review = client.post(f"/api/teams/{team_id}/policy-review", json={}).json()
    assert review["team_policy_review"]["status"] == "pending"
    decided = client.post(
        f"/api/approvals/{review['toolgate_request']['id']}/decision",
        json={"decision": "approved"},
    ).json()
    assert decided["team_policy_status"] == "owner_reviewed"
    return client.get(f"/api/teams/{team_id}").json()


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


def test_session_approvals_are_session_scoped_and_redacted():
    reset_state()
    app.state.sessions["sess-2"] = {"id": "sess-2", "title": "Other session", "created_at": "2026-01-01T00:00:00+00:00", "updated_at": "2026-01-01T00:00:00+00:00"}
    app.state.approval_runs = {
        "req-pending": {
            "run_id": "run-secret-123",
            "session_id": "sess-1",
            "agent_id": "agent_pi_operator",
            "team_id": "team_core",
            "tool_id": "echo",
            "tool_ids": ["echo"],
        },
        "req-other": {
            "run_id": "run-other-secret",
            "session_id": "sess-2",
            "agent_id": "agent_pi_operator",
            "team_id": "team_core",
            "tool_id": "echo",
            "tool_ids": ["echo"],
        },
    }

    with TestClient(app) as client:
        response = client.get("/api/sessions/sess-1/approvals")

    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 1
    assert payload["approvals"][0]["id"] == "req-pending"
    assert payload["approvals"][0]["details"] == "Stored in ToolGate"
    assert payload["approvals"][0]["metadata_only"] is True
    assert payload["raw_run_ids_included"] is False
    serialized = json.dumps(payload).lower()
    for forbidden in [
        "run-secret",
        "run-other-secret",
        "req-other",
        "token=abc123",
        "https://private.example",
        "/home/alexey",
        "raw_args",
        "secret",
        "memory text",
    ]:
        assert forbidden not in serialized


def test_chat_stream_sanitizes_approval_required_event():
    reset_state()
    app.state.pi = ApprovalRequiredPi()
    app.state.agents = {
        "agent_pi_operator": {
            "id": "agent_pi_operator",
            "name": "Pi",
            "tool_ids": ["echo"],
            "skill_ids": [],
            "memory_scopes": [],
            "team_ids": ["team_core"],
        }
    }
    app.state.teams = {
        "team_core": {
            "id": "team_core",
            "name": "Core",
            "member_agent_ids": ["agent_pi_operator"],
            "tool_ids": [],
            "skill_ids": [],
            "memory_scopes": [],
        }
    }

    with TestClient(app) as client:
        response = client.post(
            "/api/sessions/sess-1/chat/stream",
            json={"input": "please run the gated echo", "agent_id": "agent_pi_operator", "team_id": "team_core"},
        )
        session_approvals = client.get("/api/sessions/sess-1/approvals")

    assert response.status_code == 200
    assert "event: approval.required" in response.text
    assert '"metadata_only":true' in response.text.replace(" ", "")
    assert '"raw_run_id_included":false' in response.text.replace(" ", "")
    assert session_approvals.status_code == 200
    assert session_approvals.json()["approvals"][0]["id"] == "req-pending"
    serialized = f"{response.text} {json.dumps(session_approvals.json())}".lower()
    for forbidden in [
        "run-secret",
        "token=abc123",
        "https://private.example",
        "/home/alexey",
        "raw_args",
        "secret",
        "memory text",
    ]:
        assert forbidden not in serialized


def test_owner_auth_fails_closed_when_not_configured(monkeypatch):
    reset_state()
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.delenv("AGENTGATE_OWNER_TOKEN", raising=False)

    with TestClient(app) as client:
        health = client.get("/health").json()
        response = client.get("/api/home")

    assert health["owner_auth"] == "missing"
    assert response.status_code == 503
    assert response.json() == {
        "detail": "owner authentication is not configured",
        "status": "unavailable",
    }


def test_owner_auth_requires_matching_bearer_without_echoing_secret(monkeypatch):
    reset_state()
    owner_secret = "a" * 40
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setenv("AGENTGATE_OWNER_TOKEN", owner_secret)

    with TestClient(app) as client:
        health = client.get("/health").json()
        rejected = client.get("/api/home", headers={"Authorization": "Bearer wrong"})
        accepted = client.get("/api/home", headers={"Authorization": f"Bearer {owner_secret}"})

    assert health["owner_auth"] == "configured"
    assert rejected.status_code == 401
    assert rejected.json() == {"detail": "owner authentication required"}
    assert accepted.status_code == 200
    combined = f"{health} {rejected.text} {accepted.text}"
    assert owner_secret not in combined


def test_owner_auth_session_validates_bearer_without_echoing_secret(monkeypatch):
    reset_state()
    owner_secret = "session-owner-token-" + "d" * 32
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setenv("AGENTGATE_OWNER_TOKEN", owner_secret)

    with TestClient(app) as client:
        accepted = client.get(
            "/api/auth/session",
            headers={"Authorization": f"Bearer {owner_secret}"},
        )
        rejected = client.get(
            "/api/auth/session",
            headers={"Authorization": "Bearer wrong-token"},
        )

    assert accepted.status_code == 200
    assert accepted.json() == {
        "status": "ok",
        "owner_authenticated": True,
        "auth_mode": "owner_bearer",
        "token_storage": "legacy_bearer",
        "metadata_only": True,
        "credentials_included": False,
        "token_included": False,
        "token_length_included": False,
        "owner_token_included": False,
        "csrf_required": False,
        "csrf_token": None,
        "session_expires_at": None,
    }
    assert rejected.status_code == 401
    combined = f"{accepted.text} {rejected.text}"
    assert owner_secret not in combined
    assert "wrong-token" not in combined


def test_owner_auth_login_sets_http_only_session_cookie_without_echoing_secret(monkeypatch):
    reset_state()
    owner_secret = "login-owner-token-" + "e" * 32
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setenv("AGENTGATE_OWNER_TOKEN", owner_secret)

    with TestClient(app) as client:
        rejected = client.post("/api/auth/login", json={"owner_token": "wrong-token"})
        accepted = client.post("/api/auth/login", json={"owner_token": owner_secret})
        session = client.get("/api/auth/session")

    assert rejected.status_code == 401
    assert rejected.json() == {"detail": "owner authentication required"}
    assert accepted.status_code == 200
    assert session.status_code == 200
    accepted_body = accepted.json()
    session_body = session.json()
    assert accepted_body["auth_mode"] == "owner_session"
    assert accepted_body["token_storage"] == "http_only_cookie"
    assert accepted_body["csrf_required"] is True
    assert isinstance(accepted_body["csrf_token"], str)
    assert len(accepted_body["csrf_token"]) >= 32
    assert session_body["csrf_token"] == accepted_body["csrf_token"]
    set_cookie = accepted.headers.get("set-cookie", "").lower()
    assert "agentgate_owner_session=" in set_cookie
    assert "httponly" in set_cookie
    assert "samesite=lax" in set_cookie
    combined = f"{rejected.text} {accepted.text} {session.text} {set_cookie}"
    assert owner_secret not in combined
    assert "wrong-token" not in combined


def test_owner_session_requires_csrf_for_mutating_requests_but_allows_reads(monkeypatch):
    reset_state()
    owner_secret = "csrf-owner-token-" + "f" * 32
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setenv("AGENTGATE_OWNER_TOKEN", owner_secret)

    with TestClient(app) as client:
        login = client.post("/api/auth/login", json={"owner_token": owner_secret})
        csrf_token = login.json()["csrf_token"]
        read = client.get("/api/home")
        blocked = client.post("/api/sessions", json={"title": "blocked"})
        wrong = client.post("/api/sessions", json={"title": "wrong"}, headers={"X-AgentGate-CSRF": "wrong"})
        accepted = client.post("/api/sessions", json={"title": "accepted"}, headers={"X-AgentGate-CSRF": csrf_token})

    assert read.status_code == 200
    assert blocked.status_code == 403
    assert blocked.json() == {"detail": "owner csrf token required"}
    assert wrong.status_code == 403
    assert accepted.status_code == 200
    assert accepted.json()["title"] == "accepted"
    combined = f"{login.text} {read.text} {blocked.text} {wrong.text} {accepted.text}"
    assert owner_secret not in combined


def test_legacy_bearer_still_allows_mutating_requests_without_csrf(monkeypatch):
    reset_state()
    owner_secret = "legacy-owner-token-" + "g" * 32
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setenv("AGENTGATE_OWNER_TOKEN", owner_secret)

    with TestClient(app) as client:
        accepted = client.post(
            "/api/sessions",
            json={"title": "legacy cli"},
            headers={"Authorization": f"Bearer {owner_secret}"},
        )

    assert accepted.status_code == 200
    assert accepted.json()["title"] == "legacy cli"
    assert owner_secret not in accepted.text


def test_owner_session_requires_csrf_for_job_control_endpoints(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()
    app.state.pi = CapturingPi()
    owner_secret = "job-csrf-owner-token-" + "k" * 32
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setenv("AGENTGATE_OWNER_TOKEN", owner_secret)

    with TestClient(app) as client:
        login = client.post("/api/auth/login", json={"owner_token": owner_secret})
        csrf_token = login.json()["csrf_token"]
        created = client.post(
            "/api/jobs",
            json={
                "name": "CSRF protected job",
                "schedule": "0 9 * * *",
                "prompt": "private command job prompt",
            },
            headers={"X-AgentGate-CSRF": csrf_token},
        ).json()
        job_id = created["id"]
        blocked = [
            client.post(f"/api/jobs/{job_id}/pause"),
            client.post(f"/api/jobs/{job_id}/resume"),
            client.post(f"/api/jobs/{job_id}/run"),
            client.post(f"/api/jobs/{job_id}/stop"),
        ]
        wrong = client.post(
            f"/api/jobs/{job_id}/pause",
            headers={"X-AgentGate-CSRF": "wrong"},
        )
        accepted_pause = client.post(
            f"/api/jobs/{job_id}/pause",
            headers={"X-AgentGate-CSRF": csrf_token},
        )
        accepted_resume = client.post(
            f"/api/jobs/{job_id}/resume",
            headers={"X-AgentGate-CSRF": csrf_token},
        )
        accepted_run = client.post(
            f"/api/jobs/{job_id}/run",
            headers={"X-AgentGate-CSRF": csrf_token},
        )
        accepted_stop = client.post(
            f"/api/jobs/{job_id}/stop",
            headers={"X-AgentGate-CSRF": csrf_token},
        )

    assert login.status_code == 200
    assert created["status"] == "active"
    assert [response.status_code for response in blocked] == [403, 403, 403, 403]
    assert all(response.json() == {"detail": "owner csrf token required"} for response in blocked)
    assert wrong.status_code == 403
    assert accepted_pause.status_code == 200
    assert accepted_pause.json()["status"] == "paused"
    assert accepted_resume.status_code == 200
    assert accepted_resume.json()["status"] == "active"
    assert accepted_run.status_code == 200
    assert accepted_run.json()["last_result"]["status"] == "ok"
    assert accepted_stop.status_code == 404
    assert accepted_stop.json() == {"detail": "active job run not found"}
    combined = " ".join(
        [
            login.text,
            str(created),
            *(response.text for response in blocked),
            wrong.text,
            accepted_pause.text,
            accepted_resume.text,
            accepted_run.text,
            accepted_stop.text,
        ]
    )
    assert owner_secret not in combined
    assert "private command job prompt" not in combined


def test_legacy_bearer_allows_job_controls_without_csrf(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()
    app.state.pi = CapturingPi()
    owner_secret = "legacy-job-owner-token-" + "l" * 32
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setenv("AGENTGATE_OWNER_TOKEN", owner_secret)
    headers = {"Authorization": f"Bearer {owner_secret}"}

    with TestClient(app) as client:
        created = client.post(
            "/api/jobs",
            json={
                "name": "Legacy job controls",
                "schedule": "0 9 * * *",
                "prompt": "private legacy job prompt",
            },
            headers=headers,
        ).json()
        job_id = created["id"]
        paused = client.post(f"/api/jobs/{job_id}/pause", headers=headers)
        resumed = client.post(f"/api/jobs/{job_id}/resume", headers=headers)
        ran = client.post(f"/api/jobs/{job_id}/run", headers=headers)
        stopped = client.post(f"/api/jobs/{job_id}/stop", headers=headers)

    assert created["status"] == "active"
    assert paused.status_code == 200
    assert paused.json()["status"] == "paused"
    assert resumed.status_code == 200
    assert resumed.json()["status"] == "active"
    assert ran.status_code == 200
    assert ran.json()["last_result"]["status"] == "ok"
    assert stopped.status_code == 404
    assert stopped.json() == {"detail": "active job run not found"}
    combined = f"{created} {paused.text} {resumed.text} {ran.text} {stopped.text}"
    assert owner_secret not in combined
    assert "private legacy job prompt" not in combined


def test_owner_logout_removes_session_cookie_and_server_session(monkeypatch):
    reset_state()
    owner_secret = "logout-owner-token-" + "h" * 32
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setenv("AGENTGATE_OWNER_TOKEN", owner_secret)

    with TestClient(app) as client:
        login = client.post("/api/auth/login", json={"owner_token": owner_secret})
        csrf_token = login.json()["csrf_token"]
        assert len(app.state.owner_sessions) == 1
        logout = client.post("/api/auth/logout", headers={"X-AgentGate-CSRF": csrf_token})
        after = client.get("/api/auth/session")

    assert logout.status_code == 200
    assert logout.json()["owner_authenticated"] is False
    assert len(app.state.owner_sessions) == 0
    assert "agentgate_owner_session=" in logout.headers.get("set-cookie", "").lower()
    assert after.status_code == 401
    combined = f"{login.text} {logout.text} {after.text}"
    assert owner_secret not in combined


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


def test_workstream_app_workspace_detail_is_digest_count_only(monkeypatch, tmp_path):
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
                "name": "Private Workspace",
                "purpose": "build with token=workspace-secret https://private.example/app path=/home/alexey/private",
                "app_type": "dashboard",
                "required_tool_ids": ["echo"],
                "required_memory_scopes": ["project-context"],
                "progress_summary": "progress bearer workspace-secret file=/tmp/private-workspace",
            },
        )
        workspace = created.json()
        drilldown = client.get(f"/api/workstream/refs/app_workspace/{workspace['id']}")
        workstream = client.get("/api/workstream")

    assert created.status_code == 200
    assert drilldown.status_code == 200
    detail = drilldown.json()["detail"]
    assert detail["schema"] == "agentgate.app_workspace_ref_detail.v1"
    assert detail["id"] == workspace["id"]
    assert detail["purpose_present"] is True
    assert detail["purpose_digest"]
    assert detail["purpose_chars"] > 0
    assert detail["progress_summary_present"] is True
    assert detail["progress_summary_digest"]
    assert detail["progress_summary_chars"] > 0
    assert detail["required_tool_count"] == 1
    assert detail["required_memory_scope_count"] == 1
    assert detail["artifact_count"] == 0
    assert detail["preview_proposal_count"] == 0
    assert drilldown.json()["insight"]["controls"]["schema"] == "agentgate.app_workspace_controls.v1"
    assert drilldown.json()["insight"]["controls"]["executes_from_drilldown"] is False
    serialized = json.dumps({"drilldown": drilldown.json(), "workstream": workstream.json()}).lower()
    for forbidden in [
        "workspace-secret",
        "https://private.example",
        "/home/alexey/private",
        "/tmp/private-workspace",
        "bearer workspace-secret",
        "build with token",
        "progress bearer",
        "required_tool_ids",
        "required_memory_scopes",
        "project-context",
    ]:
        assert forbidden not in serialized


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
    assert drilldown.json()["detail"]["schema"] == "agentgate.app_artifact_ref_detail.v1"
    assert drilldown.json()["detail"]["id"] == artifact["id"]
    assert drilldown.json()["detail"]["workspace_ref_present"] is True
    assert "workspace_id" not in drilldown.json()["detail"]
    assert "summary" not in drilldown.json()["detail"]
    assert drilldown.json()["detail"]["summary_digest"]
    assert drilldown.json()["detail"]["summary_chars"] > 0
    assert drilldown.json()["insight"]["controls"]["schema"] == "agentgate.app_artifact_controls.v1"
    assert drilldown.json()["insight"]["controls"]["executes_from_drilldown"] is False
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


def test_workstream_app_artifact_detail_is_digest_count_only(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        main._ensure_registry_seeded()
        workspace = client.post(
            "/api/app-workspaces",
            json={"name": "Artifact Host", "purpose": "Metadata-only artifact host."},
        ).json()
        created = client.post(
            f"/api/app-workspaces/{workspace['id']}/artifacts",
            json={
                "name": "Private Spec",
                "artifact_type": "spec",
                "summary": "raw_code=print('artifact-secret') token=artifact-secret https://private.example/spec path=/home/alexey/spec",
            },
        )
        artifact = created.json()["artifacts"][0]
        drilldown = client.get(f"/api/workstream/refs/app_artifact/{artifact['id']}")
        workstream = client.get("/api/workstream")

    assert created.status_code == 200
    assert drilldown.status_code == 200
    detail = drilldown.json()["detail"]
    assert detail["schema"] == "agentgate.app_artifact_ref_detail.v1"
    assert detail["id"] == artifact["id"]
    assert detail["workspace_ref_present"] is True
    assert detail["summary_present"] is True
    assert detail["summary_digest"]
    assert detail["summary_chars"] > 0
    assert detail["linked_preview_proposal_count"] == 0
    assert drilldown.json()["insight"]["controls"]["schema"] == "agentgate.app_artifact_controls.v1"
    assert drilldown.json()["insight"]["controls"]["executes_from_drilldown"] is False
    serialized = json.dumps({"drilldown": drilldown.json(), "workstream": workstream.json()}).lower()
    for forbidden in [
        "artifact-secret",
        "raw_code",
        "https://private.example",
        "/home/alexey/spec",
        "print('artifact-secret')",
        "workspace_id",
    ]:
        assert forbidden not in serialized


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
                "summary": "raw_code=print(secret) file=/tmp/app.py bearer abc123 token equals abc123 ```const token = 'abc123'```",
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


def test_app_workspace_preview_proposal_registry_create_list_patch_delete(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        main._ensure_registry_seeded()
        workspace = client.post(
            "/api/app-workspaces",
            json={"name": "Preview Desk", "purpose": "Metadata-only proposal desk."},
        ).json()
        artifact = client.post(
            f"/api/app-workspaces/{workspace['id']}/artifacts",
            json={"name": "Review Note", "artifact_type": "review_note"},
        ).json()["artifacts"][0]
        created = client.post(
            f"/api/app-workspaces/{workspace['id']}/preview-proposals",
            json={
                "name": "Static Preview Package",
                "proposal_type": "static_preview",
                "status": "draft",
                "risk_level": "medium",
                "summary": "Metadata-only proposal for owner review.",
                "linked_artifact_ids": [artifact["id"]],
            },
        )
        proposal = created.json()["proposals"][0]
        listed = client.get(f"/api/app-workspaces/{workspace['id']}/preview-proposals")
        patched = client.patch(
            f"/api/app-workspaces/{workspace['id']}/preview-proposals/{proposal['id']}",
            json={"status": "review_ready", "review_status": "needs_review", "proposal_type": "review_bundle"},
        )
        drilldown = client.get(f"/api/workstream/refs/app_preview_proposal/{proposal['id']}")

    assert created.status_code == 200
    assert created.json()["summary"]["total"] == 1
    assert proposal["workspace_id"] == workspace["id"]
    assert proposal["proposal_type"] == "static_preview"
    assert proposal["linked_artifact_ids"] == [artifact["id"]]
    assert proposal["created_by_agent_id"] == "agent_pi_operator"
    assert created.json()["safety"]["mode"] == "metadata_only"
    assert created.json()["safety"]["files_created"] is False
    assert created.json()["safety"]["source_code_stored"] is False
    assert created.json()["safety"]["previews_run"] is False
    assert created.json()["safety"]["packages_built"] is False
    assert created.json()["safety"]["apps_published"] is False
    assert created.json()["safety"]["toolgate_called"] is False
    assert listed.json()["summary"]["draft"] == 1
    assert patched.status_code == 200
    assert patched.json()["summary"]["review_ready"] == 1
    assert patched.json()["proposals"][0]["proposal_type"] == "review_bundle"
    assert drilldown.status_code == 200
    assert drilldown.json()["detail"]["id"] == proposal["id"]
    assert drilldown.json()["detail"]["workspace_id"] == workspace["id"]
    assert drilldown.json()["detail"]["schema"] == "agentgate.app_preview_proposal_ref_detail.v1"
    assert "summary" not in drilldown.json()["detail"]
    assert drilldown.json()["detail"]["summary_digest"]
    assert drilldown.json()["detail"]["linked_artifact_count"] == 1
    assert drilldown.json()["safety"]["mode"] == "metadata_only"
    assert drilldown.json()["insight"]["controls"]["schema"] == "agentgate.app_preview_proposal_controls.v1"

    app.state.app_preview_proposals = {}
    main._load_registry()
    assert app.state.app_preview_proposals[proposal["id"]]["name"] == "Static Preview Package"

    with TestClient(app) as client:
        deleted = client.delete(f"/api/app-workspaces/{workspace['id']}/preview-proposals/{proposal['id']}")
    assert deleted.status_code == 200
    assert deleted.json()["deleted"] is True
    assert deleted.json()["summary"]["total"] == 0

    app.state.app_preview_proposals = {}
    main._load_registry()
    assert proposal["id"] not in app.state.app_preview_proposals


def test_app_workspace_preview_proposal_linked_artifact_validation(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        main._ensure_registry_seeded()
        first = client.post("/api/app-workspaces", json={"name": "First Workspace"}).json()
        second = client.post("/api/app-workspaces", json={"name": "Second Workspace"}).json()
        foreign_artifact = client.post(
            f"/api/app-workspaces/{second['id']}/artifacts",
            json={"name": "Foreign Spec", "artifact_type": "spec"},
        ).json()["artifacts"][0]
        missing = client.post(
            f"/api/app-workspaces/{first['id']}/preview-proposals",
            json={"name": "Missing Link", "linked_artifact_ids": ["appart_missing"]},
        )
        foreign = client.post(
            f"/api/app-workspaces/{first['id']}/preview-proposals",
            json={"name": "Foreign Link", "linked_artifact_ids": [foreign_artifact["id"]]},
        )

    assert missing.status_code == 422
    assert "linked artifact not found" in missing.text
    assert foreign.status_code == 422
    assert "does not belong" in foreign.text


def test_app_workspace_preview_proposals_missing_workspace_404(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        main._ensure_registry_seeded()
        listed = client.get("/api/app-workspaces/appws_missing/preview-proposals")
        created = client.post(
            "/api/app-workspaces/appws_missing/preview-proposals",
            json={"name": "Missing", "proposal_type": "static_preview"},
        )

    assert listed.status_code == 404
    assert created.status_code == 404


def test_app_workspace_preview_proposals_redact_tokens_urls_paths_and_raw_code(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        main._ensure_registry_seeded()
        workspace = client.post(
            "/api/app-workspaces",
            json={"name": "Proposal Safety", "purpose": "Metadata only."},
        ).json()
        response = client.post(
            f"/api/app-workspaces/{workspace['id']}/preview-proposals",
            json={
                "name": "Package https://private.example/app token=abc123 path=/home/private/app",
                "proposal_type": "tool_package",
                "summary": "raw_code=print(secret) file=/tmp/app.py bearer abc123 ```const token = 'abc123'```",
                "raw_code": "print('should not persist')",
                "host_path": "/home/private/nope",
                "url": "https://private.example/nope",
            },
        )
        listed = client.get(f"/api/app-workspaces/{workspace['id']}/preview-proposals")

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


def test_app_workspace_preview_proposal_promotion_approval_queues_redacted_toolgate_request(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        main._ensure_registry_seeded()
        workspace = client.post(
            "/api/app-workspaces",
            json={"name": "Promotion Desk", "purpose": "Metadata-only promotion review."},
        ).json()
        created = client.post(
            f"/api/app-workspaces/{workspace['id']}/preview-proposals",
            json={
                "name": "Dashboard Plugin token=abc123 path=/home/private/app",
                "proposal_type": "dashboard_plugin",
                "status": "review_ready",
                "risk_level": "medium",
                "summary": "Owner package review. url=https://private.example/app raw_code=print(secret)",
            },
        ).json()["proposals"][0]
        response = client.post(
            f"/api/app-workspaces/{workspace['id']}/preview-proposals/{created['id']}/promotion-approval",
            json={
                "target_kind": "dashboard_plugin",
                "owner_note": "Review this package at https://private.example/app token=abc123 path=/tmp/app.py ```console.log(secret)```",
                "requested_by_agent_id": "agent_pi_operator",
                "requested_by_team_id": None,
                "raw_tool_args": {"token": "abc123"},
                "package_manifest": {"scripts": {"postinstall": "curl https://private.example"}},
            },
        )

    assert response.status_code == 200
    body = json.dumps(
        {
            "response": response.json(),
            "toolgate_request": app.state.gates.requests[response.json()["approval"]["approval_request_id"]],
        },
        sort_keys=True,
    ).lower()
    for forbidden in [
        "abc123",
        "https://private.example",
        "/home/private",
        "/tmp/app.py",
        "print(secret)",
        "console.log(secret)",
        "postinstall",
        "\"raw_tool_args\": {",
    ]:
        assert forbidden not in body
    request = app.state.gates.requests[response.json()["approval"]["approval_request_id"]]
    proposal = response.json()["proposals"][0]
    assert request["kind"] == "app_preview_promotion_review"
    assert request["payload"]["subject_type"] == "app_preview_proposal"
    assert request["payload"]["target_kind"] == "dashboard_plugin"
    assert request["payload"]["metadata_only"] is True
    assert "owner_note_digest" in request["payload"]
    assert proposal["approval_request_id"] == request["id"]
    assert proposal["approval_status"] == "pending"
    assert proposal["approval_target_kind"] == "dashboard_plugin"
    assert response.json()["safety"]["toolgate_approval_queued"] is True
    assert response.json()["safety"]["toolgate_execution_called"] is False
    assert response.json()["safety"]["packages_installed"] is False
    assert response.json()["safety"]["apps_published"] is False
    assert response.json()["safety"]["plugins_promoted"] is False
    assert response.json()["safety"]["raw_tool_args_stored"] is False
    assert not hasattr(app.state.gates, "invoked_tool")


def test_app_workspace_preview_proposal_promotion_approval_archived_fails_closed(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        main._ensure_registry_seeded()
        workspace = client.post("/api/app-workspaces", json={"name": "Archived Promotion Desk"}).json()
        proposal = client.post(
            f"/api/app-workspaces/{workspace['id']}/preview-proposals",
            json={"name": "Archived Package", "status": "archived", "proposal_type": "tool_package"},
        ).json()["proposals"][0]
        response = client.post(
            f"/api/app-workspaces/{workspace['id']}/preview-proposals/{proposal['id']}/promotion-approval",
            json={"target_kind": "tool_package", "owner_note": "please review"},
        )

    assert response.status_code == 409
    assert "not eligible" in response.text
    assert getattr(app.state.gates, "requests", {}) == {}


def test_app_workspace_preview_proposal_promotion_approval_decision_syncs_metadata(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        main._ensure_registry_seeded()
        workspace = client.post("/api/app-workspaces", json={"name": "Decision Sync Desk"}).json()
        proposal = client.post(
            f"/api/app-workspaces/{workspace['id']}/preview-proposals",
            json={"name": "Rejected Package", "status": "review_ready", "proposal_type": "tool_package"},
        ).json()["proposals"][0]
        queued = client.post(
            f"/api/app-workspaces/{workspace['id']}/preview-proposals/{proposal['id']}/promotion-approval",
            json={"target_kind": "tool_package", "owner_note": "metadata review only"},
        ).json()
        rejected = client.post(
            f"/api/approvals/{queued['approval']['approval_request_id']}/decision",
            json={"decision": "rejected"},
        )
        listed = client.get(f"/api/app-workspaces/{workspace['id']}/preview-proposals").json()["proposals"][0]

    assert rejected.status_code == 200
    assert rejected.json()["app_preview_promotion_status"] == "rejected"
    assert listed["approval_status"] == "rejected"
    assert listed["approval_target_kind"] == "tool_package"
    assert listed["review_status"] == "blocked"
    assert "approval_decided_at" not in listed
    assert "package_manifest" not in json.dumps(listed).lower()


def test_app_workspace_preview_proposal_promotion_approval_approved_stays_metadata_only(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        main._ensure_registry_seeded()
        workspace = client.post("/api/app-workspaces", json={"name": "Approved Metadata Desk"}).json()
        proposal = client.post(
            f"/api/app-workspaces/{workspace['id']}/preview-proposals",
            json={"name": "Approved Metadata", "status": "review_ready", "proposal_type": "dashboard_plugin"},
        ).json()["proposals"][0]
        queued = client.post(
            f"/api/app-workspaces/{workspace['id']}/preview-proposals/{proposal['id']}/promotion-approval",
            json={"target_kind": "dashboard_plugin", "owner_note": "metadata review only"},
        ).json()
        approved = client.post(
            f"/api/approvals/{queued['approval']['approval_request_id']}/decision",
            json={"decision": "approved"},
        )
        listed_response = client.get(f"/api/app-workspaces/{workspace['id']}/preview-proposals").json()
        listed = listed_response["proposals"][0]

    assert approved.status_code == 200
    assert approved.json()["app_preview_promotion_status"] == "approved_metadata"
    assert listed["approval_status"] == "approved"
    assert listed["approval_target_kind"] == "dashboard_plugin"
    assert listed["review_status"] == "approved_metadata"
    assert listed_response["safety"]["previews_run"] is False
    assert listed_response["safety"]["packages_installed"] is False
    assert listed_response["safety"]["apps_published"] is False
    assert listed_response["safety"]["plugins_promoted"] is False
    assert listed_response["safety"]["toolgate_called"] is False


def test_app_workspace_preview_proposal_promotion_approval_missing_workspace_or_proposal_404(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        main._ensure_registry_seeded()
        workspace = client.post("/api/app-workspaces", json={"name": "Missing Promotion Desk"}).json()
        missing_workspace = client.post(
            "/api/app-workspaces/appws_missing/preview-proposals/appprop_missing/promotion-approval",
            json={"target_kind": "static_preview"},
        )
        missing_proposal = client.post(
            f"/api/app-workspaces/{workspace['id']}/preview-proposals/appprop_missing/promotion-approval",
            json={"target_kind": "static_preview"},
        )

    assert missing_workspace.status_code == 404
    assert missing_proposal.status_code == 404


def test_app_preview_proposal_workstream_controls_are_metadata_only(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        main._ensure_registry_seeded()
        workspace = client.post(
            "/api/app-workspaces",
            json={"name": "Command App Review Desk", "purpose": "Metadata-only app proposal proof."},
        ).json()
        created = client.post(
            f"/api/app-workspaces/{workspace['id']}/preview-proposals",
            json={
                "name": "Command Preview",
                "proposal_type": "dashboard_plugin",
                "status": "review_ready",
                "risk_level": "high",
                "summary": "Never expose token=app-secret https://app.example/path raw_code=print(secret) path=/home/private/app",
                "linked_artifact_ids": [],
            },
        ).json()["proposals"][0]
        before_drilldown_requests = set(getattr(app.state.gates, "requests", {}).keys())
        draft_detail = client.get(f"/api/workstream/refs/app_preview_proposal/{created['id']}")
        after_drilldown_requests = set(getattr(app.state.gates, "requests", {}).keys())
        queued = client.post(
            f"/api/app-workspaces/{workspace['id']}/preview-proposals/{created['id']}/promotion-approval",
            json={
                "target_kind": "dashboard_plugin",
                "owner_note": "metadata review only token=note-secret https://note.example/path",
            },
        ).json()
        pending_detail = client.get(f"/api/workstream/refs/app_preview_proposal/{created['id']}")
        client.post(
            f"/api/approvals/{queued['approval']['approval_request_id']}/decision",
            json={"decision": "approved"},
        )
        approved_detail = client.get(f"/api/workstream/refs/app_preview_proposal/{created['id']}")
        archived = client.post(
            f"/api/app-workspaces/{workspace['id']}/preview-proposals",
            json={
                "name": "Archived Preview",
                "proposal_type": "static_preview",
                "status": "archived",
                "summary": "Archived private token=archived-secret https://archived.example/path",
            },
        ).json()["proposals"][0]
        archived_detail = client.get(f"/api/workstream/refs/app_preview_proposal/{archived['id']}")

    assert draft_detail.status_code == 200
    assert before_drilldown_requests == after_drilldown_requests
    body = draft_detail.json()
    detail = body["detail"]
    controls = body["insight"]["controls"]
    assert detail["schema"] == "agentgate.app_preview_proposal_ref_detail.v1"
    assert "summary" not in detail
    assert "linked_artifact_ids" not in detail
    assert detail["summary_present"] is True
    assert detail["summary_digest"]
    assert detail["summary_chars"] > 0
    assert detail["linked_artifact_count"] == 0
    assert detail["approval_request_present"] is False
    assert controls["schema"] == "agentgate.app_preview_proposal_controls.v1"
    assert controls["metadata_only"] is True
    assert controls["executes_from_drilldown"] is False
    assert controls["promotion_readiness"]["enabled"] is True
    assert controls["promotion_readiness"]["reason_code"] == "ready_for_toolgate_review"
    assert controls["approval_boundary"]["enabled"] is False
    assert controls["approval_boundary"]["reason_code"] == "no_pending_approval"
    assert controls["lifecycle_boundary"]["enabled"] is True
    joined = json.dumps(body).lower()
    for forbidden in [
        "app-secret",
        "https://app.example",
        "raw_code",
        "print(secret)",
        "/home/private",
        "/api/app-workspaces",
        "/v2/",
        "package_manifest",
        "raw_tool_args",
        "postinstall",
    ]:
        assert forbidden not in joined

    pending_controls = pending_detail.json()["insight"]["controls"]
    assert pending_controls["promotion_readiness"]["enabled"] is False
    assert pending_controls["promotion_readiness"]["reason_code"] == "approval_already_pending"
    assert pending_controls["approval_boundary"]["enabled"] is True
    assert pending_controls["approval_boundary"]["reason_code"] == "pending_owner_review"
    assert pending_controls["lifecycle_boundary"]["enabled"] is False
    assert pending_controls["lifecycle_boundary"]["reason_code"] == "pending_approval"

    approved_controls = approved_detail.json()["insight"]["controls"]
    assert approved_controls["promotion_readiness"]["enabled"] is False
    assert approved_controls["promotion_readiness"]["reason_code"] == "approved_metadata"
    assert approved_controls["approval_boundary"]["enabled"] is False
    assert approved_controls["approval_boundary"]["reason_code"] == "approved_metadata"
    assert approved_controls["lifecycle_boundary"]["enabled"] is False
    assert approved_controls["lifecycle_boundary"]["reason_code"] == "approved_audit_history"

    archived_controls = archived_detail.json()["insight"]["controls"]
    assert archived_controls["promotion_readiness"]["enabled"] is False
    assert archived_controls["promotion_readiness"]["reason_code"] == "archived"
    assert archived_controls["approval_boundary"]["enabled"] is False
    assert archived_controls["lifecycle_boundary"]["enabled"] is True


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


def test_character_source_reviews_are_metadata_only(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        agent = client.post(
            "/api/agents",
            json={
                "name": "Character Source Agent",
                "purpose": "Review character source metadata.",
            },
        ).json()
        created = client.post(
            "/api/character/sources",
            json={
                "title": "Naruto-like courage notes https://private.example/ref",
                "source_type": "character_reference",
                "target_agent_id": agent["id"],
                "summary": "Owner notes with token=abc123 and no copied source page.",
                "visual_notes": "Orange palette, file=/home/private/image.png, no actual upload.",
                "usage_policy": "transformative",
                "source_confidence": "owner_verified",
                "asset_review_status": "approved_metadata",
                "review_status": "owner_reviewed",
                "source_labels": ["owner seed", "https://private.example/source"],
                "review_checklist": ["No copied text", "bearer abc123 removed"],
            },
        ).json()
        listed = client.get(f"/api/character/sources?target_agent_id={agent['id']}").json()
        reviewed = client.get("/api/character/sources?review_status=owner_reviewed").json()
        activity = client.get(f"/api/agents/{agent['id']}/activity").json()["activity"]
        deleted = client.delete(f"/api/character/sources/{created['id']}")
        after_delete = client.get(f"/api/character/sources?target_agent_id={agent['id']}").json()

    assert created["source_type"] == "character_reference"
    assert created["usage_policy"] == "transformative"
    assert created["source_confidence"] == "owner_verified"
    assert created["asset_review_status"] == "approved_metadata"
    assert created["review_status"] == "owner_reviewed"
    assert created["source_labels"] == ["owner seed", "[redacted-url]"]
    assert created["review_checklist"] == ["No copied text", "bearer [redacted] removed"]
    assert created["safety"]["metadata_only"] is True
    assert listed["summary"]["total"] == 1
    assert listed["summary"]["owner_reviewed"] == 1
    assert reviewed["sources"][0]["id"] == created["id"]
    assert "character.source_created" in [item["event_type"] for item in activity]
    assert deleted.status_code == 200
    assert deleted.json()["deleted"] is True
    assert after_delete["sources"] == []
    combined = f"{created} {listed} {reviewed} {activity} {after_delete}".lower()
    assert "https://private.example" not in combined
    assert "token=abc123" not in combined
    assert "/home/private" not in combined


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
        "voice_runtime_status": "unregistered",
        "avatar_runtime_status": "unregistered",
        "runtime_ready": False,
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


def test_sidecar_runtime_registry_is_dormant_metadata_only(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        created = client.post(
            "/api/sidecars/runtimes",
            json={
                "label": "Local narrator",
                "runtime_kind": "tts",
                "status": "installed",
                "health_status": "manual_ok",
                "owner_review_status": "owner_reviewed",
                "local_only": True,
                "capabilities": ["read aloud", "short replies"],
                "description": "Owner-reviewed local-only TTS presenter.",
            },
        )
        listed = client.get("/api/sidecars/runtimes")
        updated = client.patch(
            f"/api/sidecars/runtimes/{created.json()['id']}",
            json={
                "status": "disabled",
                "health_status": "unknown",
                "description": "Dormant until the runtime is installed manually.",
            },
        )
        deleted = client.delete(f"/api/sidecars/runtimes/{created.json()['id']}")

    assert created.status_code == 200
    body = created.json()
    assert body["runtime_kind"] == "tts"
    assert body["runtime_ready"] is True
    assert body["safety"] == {
        "metadata_only": True,
        "local_only": True,
        "execution_enabled": False,
        "start_stop_supported": False,
        "media_included": False,
        "assets_included": False,
        "credentials_included": False,
        "provider_urls_included": False,
        "host_paths_included": False,
        "ports_included": False,
        "raw_config_included": False,
    }
    assert listed.status_code == 200
    assert listed.json()["summary"]["total"] == 1
    assert listed.json()["summary"]["ready"] == 1
    assert listed.json()["safety"]["execution_enabled"] is False
    assert updated.status_code == 200
    assert updated.json()["status"] == "disabled"
    assert updated.json()["runtime_ready"] is False
    assert deleted.status_code == 200
    assert deleted.json() == {
        "deleted": True,
        "id": body["id"],
        "metadata_only": True,
        "execution_stopped": False,
        "files_removed": False,
        "media_removed": False,
    }
    joined = json.dumps([body, listed.json(), updated.json(), deleted.json()]).lower()
    for forbidden in ["start_enabled", "stop_enabled", "install_enabled", "provider_url=", "endpoint=", "port:", "/home", "token=", "secret="]:
        assert forbidden not in joined


def test_sidecar_runtime_registry_rejects_private_runtime_details(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    unsafe_payloads = [
        {"label": "https://voice.example/private", "runtime_kind": "tts"},
        {"label": "Local voice", "description": "endpoint=http://127.0.0.1:9000 token=abc123"},
        {"label": "Local avatar", "capabilities": ["asset=/home/private/avatar.glb"]},
        {"label": "Remote bridge", "local_only": False},
    ]

    with TestClient(app) as client:
        responses = [client.post("/api/sidecars/runtimes", json=payload) for payload in unsafe_payloads]
        listed = client.get("/api/sidecars/runtimes")

    assert [response.status_code for response in responses] == [422, 422, 422, 422]
    assert listed.status_code == 200
    assert listed.json()["summary"]["total"] == 0
    joined = " ".join(response.text for response in responses).lower()
    assert "abc123" not in joined
    assert "voice.example" not in joined
    assert "/home/private" not in joined


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
        "agent_pi_operator": "toolgate-private-test-placeholder",
        "agent_pi_operator@team_core": "toolgate-team-private-test-placeholder",
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
    app.state.auxiliary_model_routes["summary"] = {
        "provider": "openrouter",
        "model": "safe-helper",
        "enabled": True,
        "risk_policy": "low_risk_only",
        "owner_review_status": "owner_reviewed",
    }

    def fail_subprocess(*_args, **_kwargs):
        raise AssertionError("verification snapshot must not execute local commands")

    monkeypatch.setattr(main.subprocess, "run", fail_subprocess)

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


def test_verification_snapshot_uses_safe_metadata_only(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    owner_secret = "b" * 40
    monkeypatch.setenv("AGENTGATE_OWNER_TOKEN", owner_secret)
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
        "agent_pi_operator": "toolgate-private-test-placeholder",
        "agent_pi_operator@team_core": "toolgate-team-private-test-placeholder",
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
        snapshot = client.get("/api/verification/snapshot").json()
        system = client.get("/api/system").json()

    assert snapshot["schema"] == "agentgate.verification_snapshot.v1"
    assert snapshot["safety"]["metadata_only"] is True
    assert snapshot["safety"]["commands_executed"] is False
    assert snapshot["safety"]["docker_socket_access"] is False
    assert snapshot["summary"]["total"] >= 6
    check_ids = {item["id"] for item in snapshot["checks"]}
    assert {
        "service-health",
        "owner-authentication",
        "listener-scope",
        "access-boundaries",
        "team-execution-policy-boundary",
        "auxiliary-model-routes",
        "model-provider-metadata",
        "free-model-gateway-boundary",
        "automation-approval-boundary",
        "open-loop-boundary",
    } <= check_ids
    owner_auth = next(item for item in snapshot["checks"] if item["id"] == "owner-authentication")
    assert owner_auth["status"] == "pass"
    assert owner_auth["detail"]["configured"] is True
    assert owner_secret not in str(owner_auth)
    access = next(item for item in snapshot["checks"] if item["id"] == "access-boundaries")
    assert access["detail"]["drift"] == 0
    assert system["verification"]["schema"] == snapshot["schema"]
    forbidden = re.compile(
        r"tgx_|mg_read_|api[_-]?key|token=|secret=|password=|bearer\s+|/home/|/srv/|docker\\.sock|https?://",
        re.I,
    )
    assert not forbidden.search(str(snapshot))


def test_verification_snapshot_reports_open_loop_boundary_without_loop_payloads(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    def approvals(*, history: bool = False):
        if history:
            return []
        return [{
            "id": "req-open-loop-secret",
            "source": "ToolGate",
            "severity": "high",
            "title": "Approval title token=open-loop-title-secret",
            "details": "Approval details api_key=open-loop-detail-secret https://open-loop.example/path",
            "binding": {
                "type": "tool",
                "id": "approval.test-echo token=open-loop-binding-secret",
                "version": "1",
                "digest": "sha256:openloop",
            },
            "created_at": "2026-01-02T00:00:00+00:00",
        }]

    monkeypatch.setattr(app.state.gates, "approvals", approvals)
    with TestClient(app) as client:
        app.state.memory_candidates = {
            "mem-open-loop-secret": {
                "id": "mem-open-loop-secret",
                "text": "memory token=open-loop-memory-secret https://memory-open-loop.example/path /home/alexey/private",
                "status": "pending",
                "memory_type": "preference",
                "confidence": "high",
                "created_at": "2026-01-03T00:00:00+00:00",
            }
        }
        snapshot = client.get("/api/verification/snapshot").json()

    check = next(item for item in snapshot["checks"] if item["id"] == "open-loop-boundary")
    assert check["status"] == "warn"
    assert check["severity"] == "warning"
    assert check["detail"]["schema"] == "agentgate.open_loop_boundary.v1"
    assert check["detail"]["total"] >= 2
    assert check["detail"]["needs_approval"] == 1
    assert check["detail"]["owner_review"] >= 1
    assert check["detail"]["metadata_only"] is True
    assert check["detail"]["actions_executed"] is False
    assert check["detail"]["approvals_decided"] is False
    assert check["detail"]["memory_written"] is False
    assert check["detail"]["ref_ids_included"] is False
    assert check["detail"]["titles_included"] is False
    assert check["detail"]["evidence_included"] is False
    assert "toolgate-approval" in check["detail"]["by_source_kind"]
    assert "/approvals" in check["detail"]["by_target_path"]
    assert "high" in check["detail"]["by_priority"]

    joined = json.dumps(check).lower()
    for forbidden in [
        "open-loop-title-secret",
        "open-loop-detail-secret",
        "open-loop-binding-secret",
        "open-loop-memory-secret",
        "req-open-loop-secret",
        "mem-open-loop-secret",
        "approval.test-echo",
        "https://open-loop.example",
        "https://memory-open-loop.example",
        "/home/alexey",
        "api_key=open-loop",
        "\"title\":",
        "\"evidence\":",
        "\"ref_id\":",
        "\"text\":",
        "\"content\":",
        "\"run_id\":",
        "\"provider_url\":",
    ]:
        assert forbidden not in joined


def test_verification_snapshot_accepts_scoped_listener_labels_with_ports(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    def system_overview():
        return {
            "vitals": {"cpu_percent": 1, "memory": {"percent": 2}, "disk": {"percent": 3}, "cpu_count": 4},
            "containers": [
                {"name": "agentgate", "status": "listening", "listeners": ["loopback:8080"]},
                {"name": "systemgate", "status": "listening", "listeners": ["container-internal:8040"]},
                {"name": "tailnet", "status": "listening", "listeners": ["tailscale:443"]},
            ],
            "errors": [],
            "packages": [],
            "backups": {"latest": {"name": "safe-backup", "created_at": 1760000000.0}},
            "sources": {"backups": {"status": "ok"}},
        }

    monkeypatch.setattr(app.state.gates, "system_overview", system_overview)
    with TestClient(app) as client:
        snapshot = client.get("/api/verification/snapshot").json()

    check = next(item for item in snapshot["checks"] if item["id"] == "listener-scope")
    assert check["status"] == "pass"
    assert check["detail"]["service_count"] == 3
    assert check["detail"]["unsafe_listener_count"] == 0


def test_verification_snapshot_warns_on_unscoped_listener_labels(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    def system_overview():
        return {
            "vitals": {"cpu_percent": 1, "memory": {"percent": 2}, "disk": {"percent": 3}, "cpu_count": 4},
            "containers": [
                {"name": "unsafe", "status": "listening", "listeners": ["lan:8080"]},
                {"name": "unsafe-any", "status": "listening", "listeners": ["public:0.0.0.0:8080"]},
            ],
            "errors": [],
            "packages": [],
            "backups": {"latest": {"name": "safe-backup", "created_at": 1760000000.0}},
            "sources": {"backups": {"status": "ok"}},
        }

    monkeypatch.setattr(app.state.gates, "system_overview", system_overview)
    with TestClient(app) as client:
        snapshot = client.get("/api/verification/snapshot").json()

    check = next(item for item in snapshot["checks"] if item["id"] == "listener-scope")
    assert check["status"] == "warn"
    assert check["detail"]["service_count"] == 2
    assert check["detail"]["unsafe_listener_count"] == 2


def test_verification_snapshot_reports_free_model_gateway_boundary_without_secrets(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    monkeypatch.setenv("AGENTGATE_OWNER_TOKEN", "c" * 40)
    monkeypatch.setenv("FREE_LLM_API_URL", "http://freellmapi.internal:3001")
    monkeypatch.setenv("FREE_LLM_API_KEY", "free-gateway-secret")
    reset_state()
    calls = []

    class Response:
        def __init__(self, status_code, payload=None):
            self.status_code = status_code
            self._payload = payload or {}

        def json(self):
            return self._payload

    def fake_get(url, **kwargs):
        calls.append({"url": url, "headers": kwargs.get("headers") or {}})
        if url.endswith("/health"):
            return Response(200, {"status": "ok"})
        if url.endswith("/v1/models"):
            assert kwargs.get("headers", {}).get("Authorization") == "Bearer free-gateway-secret"
            return Response(
                200,
                {
                    "data": [
                        {
                            "id": "stealth/ox-alpha",
                            "owned_by": "StealthProvider",
                            "context_window": "128k https://private.invalid/context?token=secret-value",
                            "modalities": ["text", "api_key=abc123"],
                            "capabilities": ["reasoning", "bearer hidden-token"],
                        }
                    ]
                },
            )
        return Response(404, {})

    monkeypatch.setattr(main.httpx, "get", fake_get)

    with TestClient(app) as client:
        snapshot = client.get("/api/verification/snapshot").json()

    check = next(item for item in snapshot["checks"] if item["id"] == "free-model-gateway-boundary")
    assert check["status"] == "pass"
    assert check["detail"]["gateway_status"] == "ok"
    assert check["detail"]["gateway_configured"] is True
    assert check["detail"]["gateway_models_visible"] is True
    assert check["detail"]["candidate_count"] == 1
    assert check["detail"]["provider_id"] == "freellmapi"
    assert check["detail"]["auth_status"] == "ok"
    assert check["detail"]["credentials_included"] is False
    assert check["detail"]["provider_urls_included"] is False
    assert sum(1 for item in calls if item["url"].endswith("/v1/models")) == 1
    visible = json.dumps(snapshot).lower()
    assert "free-gateway-secret" not in visible
    assert "freellmapi.internal" not in visible
    assert "https://private.invalid" not in visible
    assert "token=secret-value" not in visible
    assert "api_key=abc123" not in visible
    assert "bearer hidden-token" not in visible
    assert "authorization" not in visible


def test_verification_snapshot_reports_missing_gateway_key_as_safe_warning(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    monkeypatch.setenv("AGENTGATE_OWNER_TOKEN", "d" * 40)
    monkeypatch.setenv("FREE_LLM_API_URL", "http://freellmapi.internal:3001")
    monkeypatch.delenv("FREE_LLM_API_KEY", raising=False)
    monkeypatch.delenv("FREELLMAPI_API_KEY", raising=False)
    reset_state()

    class Response:
        def __init__(self, status_code, payload=None):
            self.status_code = status_code
            self._payload = payload or {}

        def json(self):
            return self._payload

    def fake_get(url, **kwargs):
        if url.endswith("/health"):
            return Response(200, {"status": "ok"})
        if url.endswith("/v1/models"):
            assert kwargs.get("headers") == {}
            return Response(401, {"error": "missing key api_key=secret https://private.invalid/auth"})
        return Response(404, {})

    monkeypatch.setattr(main.httpx, "get", fake_get)

    with TestClient(app) as client:
        snapshot = client.get("/api/verification/snapshot").json()

    check = next(item for item in snapshot["checks"] if item["id"] == "free-model-gateway-boundary")
    assert check["status"] == "warn"
    assert check["detail"]["gateway_status"] == "auth_required"
    assert check["detail"]["gateway_configured"] is False
    assert check["detail"]["auth_status"] == "missing"
    assert check["detail"]["candidate_count"] == 0
    visible = json.dumps(snapshot).lower()
    assert "freellmapi.internal" not in visible
    assert "api_key=secret" not in visible
    assert "https://private.invalid" not in visible
    assert "authorization" not in visible


def test_verification_snapshot_reports_capability_grant_boundary_clean(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    monkeypatch.setenv("AGENTGATE_OWNER_TOKEN", "g" * 40)
    reset_state()

    with TestClient(app) as client:
        main._ensure_registry_seeded()
        app.state.agents["agent_pi_operator"]["tool_ids"] = ["echo"]
        app.state.teams["team_core"]["skill_ids"] = ["skill-1"]
        snapshot = client.get("/api/verification/snapshot").json()

    check = next(item for item in snapshot["checks"] if item["id"] == "capability-grant-boundary")
    assert check["status"] == "pass"
    assert check["detail"]["catalog_status"] == "ok"
    assert check["detail"]["tool_catalog_count"] == 3
    assert check["detail"]["skill_catalog_count"] == 2
    assert check["detail"]["unknown_tool_grants"] == 0
    assert check["detail"]["unknown_skill_grants"] == 0
    assert check["detail"]["wildcard_tool_grants"] == 0
    assert check["detail"]["wildcard_skill_grants"] == 0
    assert check["detail"]["effective_skill_missing_linked_tool_refs"] == 0
    assert check["detail"]["metadata_only"] is True
    assert check["detail"]["raw_tool_arguments_included"] is False
    assert check["detail"]["credentials_included"] is False
    assert check["detail"]["provider_urls_included"] is False
    visible = json.dumps(check).lower()
    assert "echo" not in visible
    assert "skill-1" not in visible


def test_verification_snapshot_warns_on_capability_grant_drift_without_ids(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    monkeypatch.setenv("AGENTGATE_OWNER_TOKEN", "h" * 40)
    reset_state()

    with TestClient(app) as client:
        main._ensure_registry_seeded()
        app.state.agents["agent_pi_operator"]["tool_ids"] = []
        app.state.agents["agent_pi_operator"]["skill_ids"] = ["skill-1"]
        app.state.agents["agent_wildcard_probe"] = {
            "id": "agent_wildcard_probe",
            "name": "Wildcard Probe",
            "tool_ids": ["*"],
            "skill_ids": [],
            "memory_scopes": [],
            "team_ids": [],
        }
        app.state.teams["team_core"]["tool_ids"] = ["ghost.tool"]
        app.state.teams["team_core"]["skill_ids"] = ["ghost-skill", "*"]
        snapshot = client.get("/api/verification/snapshot").json()

    check = next(item for item in snapshot["checks"] if item["id"] == "capability-grant-boundary")
    assert check["status"] == "warn"
    assert check["severity"] == "warning"
    assert check["detail"]["catalog_status"] == "ok"
    assert check["detail"]["unknown_tool_grants"] == 1
    assert check["detail"]["unknown_skill_grants"] == 1
    assert check["detail"]["wildcard_tool_grants"] == 1
    assert check["detail"]["wildcard_skill_grants"] == 1
    assert check["detail"]["effective_skill_missing_linked_tool_refs"] == 1
    assert check["detail"]["warning_count"] >= 5
    assert check["detail"]["metadata_only"] is True
    assert check["detail"]["raw_tool_arguments_included"] is False
    assert check["detail"]["credentials_included"] is False
    assert check["detail"]["provider_urls_included"] is False
    visible = json.dumps(check).lower()
    assert "ghost.tool" not in visible
    assert "ghost-skill" not in visible
    assert "skill-1" not in visible
    assert "echo" not in visible
    assert "danger.write" not in visible


def test_capability_grant_review_applies_tool_grant_only_after_approval(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    owner_secret = "c" * 40
    monkeypatch.setenv("AGENTGATE_OWNER_TOKEN", owner_secret)
    reset_state()

    with TestClient(app) as client:
        csrf_token = client.post("/api/auth/login", json={"owner_token": owner_secret}).json()["csrf_token"]
        main._ensure_registry_seeded()
        app.state.agents["agent_pi_operator"]["tool_ids"] = []
        queued = client.post(
            "/api/capability-grants/review",
            json={
                "target": "agent",
                "target_id": "agent_pi_operator",
                "kind": "tool",
                "action": "grant",
                "capability_ids": ["approval.test-echo"],
            },
            headers={"X-AgentGate-CSRF": csrf_token},
        )
        assert queued.status_code == 200
        payload = queued.json()
        request = app.state.gates.requests[payload["approval_request_id"]]

        assert payload["status"] == "pending_approval"
        assert payload["metadata_only"] is True
        assert payload["safety"]["applies_on_approval_only"] is True
        assert app.state.agents["agent_pi_operator"]["tool_ids"] == []
        assert request["kind"] == "capability_grant_change"
        assert request["payload"]["subject_type"] == "capability_grant"
        assert request["payload"]["raw_arguments_included"] is False
        assert request["payload"]["credentials_included"] is False
        assert request["payload"]["next_ids"] == ["approval.test-echo"]

        decided = client.post(
            f"/api/approvals/{payload['approval_request_id']}/decision",
            json={"decision": "approved"},
            headers={"X-AgentGate-CSRF": csrf_token},
        )

    assert decided.status_code == 200
    assert decided.json()["capability_grant_status"] == "applied"
    assert app.state.agents["agent_pi_operator"]["tool_ids"] == ["approval.test-echo"]
    assert app.state.gates.synced_toolgate_scope_history[-1] == ["tool:approval.test-echo"]
    visible = json.dumps(payload).lower() + json.dumps(request).lower() + decided.text.lower()
    assert "token=" not in visible
    assert "password" not in visible
    assert "raw_args" not in visible


def test_capability_grant_review_rejects_without_mutation_and_blocks_wildcards(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    owner_secret = "d" * 40
    monkeypatch.setenv("AGENTGATE_OWNER_TOKEN", owner_secret)
    reset_state()

    with TestClient(app) as client:
        csrf_token = client.post("/api/auth/login", json={"owner_token": owner_secret}).json()["csrf_token"]
        main._ensure_registry_seeded()
        app.state.teams["team_core"]["skill_ids"] = ["skill-1"]
        queued = client.post(
            "/api/capability-grants/review",
            json={
                "target": "team",
                "target_id": "team_core",
                "kind": "skill",
                "action": "revoke",
                "capability_ids": ["skill-1"],
            },
            headers={"X-AgentGate-CSRF": csrf_token},
        ).json()
        rejected = client.post(
            f"/api/approvals/{queued['approval_request_id']}/decision",
            json={"decision": "rejected"},
            headers={"X-AgentGate-CSRF": csrf_token},
        )
        wildcard = client.post(
            "/api/capability-grants/review",
            json={
                "target": "agent",
                "target_id": "agent_pi_operator",
                "kind": "tool",
                "action": "grant",
                "capability_ids": ["*"],
            },
            headers={"X-AgentGate-CSRF": csrf_token},
        )

    assert rejected.status_code == 200
    assert rejected.json()["capability_grant_status"] == "rejected"
    assert app.state.teams["team_core"]["skill_ids"] == ["skill-1"]
    assert wildcard.status_code == 422


def test_verification_snapshot_reports_notification_delivery_boundary(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    monkeypatch.setenv("AGENTGATE_OWNER_TOKEN", "e" * 40)
    reset_state()
    app.state.pi = LocalNotificationPi()

    with TestClient(app) as client:
        created = client.post(
            "/api/jobs",
            json={
                "name": "Snapshot Local Notify",
                "schedule": "0 9 * * *",
                "prompt": "private prompt token=secret-value https://private.invalid/hook",
                "delivery_policy": "allowlisted",
                "delivery_targets": ["local dashboard inbox"],
            },
        ).json()
        client.post(
            f"/api/approvals/{created['approval_request_id']}/decision",
            json={"decision": "approved"},
        )
        client.post(f"/api/jobs/{created['id']}/resume")
        client.post(f"/api/jobs/{created['id']}/run")
        snapshot = client.get("/api/verification/snapshot").json()

    check = next(item for item in snapshot["checks"] if item["id"] == "notification-delivery-boundary")
    assert check["status"] == "pass"
    assert check["detail"]["channel_count"] >= 3
    assert check["detail"]["local_log_available"] >= 1
    assert check["detail"]["delivery_count"] >= 1
    assert check["detail"]["local_delivery_count"] >= 1
    assert check["detail"]["nonlocal_delivery_records"] == 0
    assert check["detail"]["suspicious_external_deliveries"] == 0
    assert check["detail"]["metadata_only"] is True
    assert check["detail"]["external_sender_configured"] is False
    assert check["detail"]["external_delivery_enabled"] is False
    assert check["detail"]["credentials_included"] is False
    assert check["detail"]["provider_urls_included"] is False
    visible = json.dumps(check).lower()
    assert "private prompt" not in visible
    assert "secret-value" not in visible
    assert "https://private.invalid" not in visible


def test_verification_snapshot_reports_sidecar_runtime_boundary(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    monkeypatch.setenv("AGENTGATE_OWNER_TOKEN", "i" * 40)
    reset_state()

    with TestClient(app) as client:
        created = client.post(
            "/api/sidecars/runtimes",
            json={
                "label": "Local narrator",
                "runtime_kind": "tts",
                "status": "installed",
                "health_status": "manual_ok",
                "owner_review_status": "owner_reviewed",
                "local_only": True,
                "capabilities": ["read aloud"],
                "description": "Owner-reviewed local-only presenter.",
            },
        )
        assert created.status_code == 200
        snapshot = client.get("/api/verification/snapshot").json()

    check = next(item for item in snapshot["checks"] if item["id"] == "sidecar-runtime-boundary")
    assert check["status"] == "pass"
    assert check["detail"]["runtime_count"] == 1
    assert check["detail"]["ready_count"] == 1
    assert check["detail"]["installed_count"] == 1
    assert check["detail"]["warning_count"] == 0
    assert check["detail"]["metadata_only"] is True
    assert check["detail"]["local_only"] is True
    assert check["detail"]["execution_enabled"] is False
    assert check["detail"]["start_stop_supported"] is False
    assert check["detail"]["install_supported"] is False
    assert check["detail"]["probe_supported"] is False
    assert check["detail"]["media_included"] is False
    assert check["detail"]["assets_included"] is False
    assert check["detail"]["credentials_included"] is False
    assert check["detail"]["provider_urls_included"] is False
    assert check["detail"]["host_paths_included"] is False
    assert check["detail"]["ports_included"] is False
    assert check["detail"]["raw_config_included"] is False
    visible = json.dumps(check).lower()
    assert "local narrator" not in visible
    assert "read aloud" not in visible
    assert "owner-reviewed" not in visible


def test_verification_snapshot_warns_on_unsafe_sidecar_runtime_without_leaking_values(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    monkeypatch.setenv("AGENTGATE_OWNER_TOKEN", "j" * 40)
    reset_state()

    with TestClient(app) as client:
        app.state.sidecar_runtimes = {
            "unsafe-runtime": {
                "id": "unsafe-runtime",
                "label": "Unsafe runtime https://private.invalid/voice",
                "runtime_kind": "tts",
                "status": "installed",
                "health_status": "manual_ok",
                "owner_review_status": "owner_reviewed",
                "local_only": False,
                "capabilities": ["asset=/home/private/avatar.glb"],
                "description": "endpoint=http://127.0.0.1:9999 token=secret-value",
                "created_at": main.now(),
                "updated_at": main.now(),
            }
        }
        snapshot = client.get("/api/verification/snapshot").json()

    check = next(item for item in snapshot["checks"] if item["id"] == "sidecar-runtime-boundary")
    assert check["status"] == "warn"
    assert check["severity"] == "warning"
    assert check["detail"]["runtime_count"] == 1
    assert check["detail"]["public_runtime_count"] == 0
    assert check["detail"]["unsafe_record_count"] == 1
    assert check["detail"]["nonlocal_claims"] == 1
    assert check["detail"]["warning_count"] >= 2
    assert check["detail"]["metadata_only"] is True
    assert check["detail"]["execution_enabled"] is False
    visible = json.dumps(check).lower()
    assert "unsafe runtime" not in visible
    assert "private.invalid" not in visible
    assert "127.0.0.1:9999" not in visible
    assert "secret-value" not in visible
    assert "/home/private" not in visible
    assert "avatar.glb" not in visible


def test_verification_snapshot_reports_pi_runtime_concurrency_boundary(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    monkeypatch.setenv("AGENTGATE_OWNER_TOKEN", "k" * 40)
    monkeypatch.delenv("PI_MAX_CONCURRENT_RUNS", raising=False)
    reset_state()
    app.state.pi = PiClient(command="fake-pi")

    with TestClient(app) as client:
        snapshot = client.get("/api/verification/snapshot").json()

    check = next(item for item in snapshot["checks"] if item["id"] == "pi-runtime-concurrency")
    assert check["status"] == "pass"
    assert check["severity"] == "info"
    assert check["detail"]["metadata_only"] is True
    assert check["detail"]["limit_source"] == "PI_MAX_CONCURRENT_RUNS"
    assert check["detail"]["run_limit"] == 1
    assert check["detail"]["default_serialized"] is True
    assert check["detail"]["semaphore_enabled"] is True
    assert check["detail"]["active_run_count"] == 0
    assert check["detail"]["active_session_count"] == 0
    assert check["detail"]["active_rpc_process_count"] == 0
    assert check["detail"]["warning_count"] == 0
    assert check["detail"]["run_ids_included"] is False
    assert check["detail"]["session_ids_included"] is False
    assert check["detail"]["prompts_included"] is False
    assert check["detail"]["process_args_included"] is False
    assert check["detail"]["process_ids_included"] is False
    assert check["detail"]["session_files_included"] is False
    assert check["detail"]["environment_included"] is False
    assert check["detail"]["credentials_included"] is False
    assert check["detail"]["provider_urls_included"] is False
    assert check["detail"]["host_paths_included"] is False
    visible = json.dumps(check).lower()
    assert "run_secret" not in visible
    assert "/home/" not in visible
    assert "--append-system-prompt" not in visible
    assert "toolgate_execution_key" not in visible
    assert "api_key" not in visible
    assert "token=" not in visible


def test_verification_snapshot_warns_on_pi_runtime_concurrency_over_limit_without_leaking_values(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    monkeypatch.setenv("AGENTGATE_OWNER_TOKEN", "l" * 40)
    monkeypatch.setenv("PI_MAX_CONCURRENT_RUNS", "1")
    reset_state()
    pi = PiClient(command="fake-pi")

    class ProcessProbe:
        returncode = None
        pid = 424242
        args = ["pi", "--append-system-prompt", "token=secret"]

    class RuntimeProbe:
        process = ProcessProbe()
        session_file = "/home/private/pi-session.json"
        current_config = {"toolgate_execution_key": "secret", "provider": "https://private.invalid"}

    pi._sessions = {"sess_secret_path": RuntimeProbe(), "sess_two": RuntimeProbe()}  # type: ignore[assignment]
    pi._runs = {"run_secret_one": object(), "run_secret_two": object()}  # type: ignore[assignment]
    app.state.pi = pi

    with TestClient(app) as client:
        snapshot = client.get("/api/verification/snapshot").json()

    check = next(item for item in snapshot["checks"] if item["id"] == "pi-runtime-concurrency")
    assert check["status"] == "warn"
    assert check["severity"] == "warning"
    assert check["detail"]["metadata_only"] is True
    assert check["detail"]["run_limit"] == 1
    assert check["detail"]["active_run_count"] == 2
    assert check["detail"]["active_session_count"] == 2
    assert check["detail"]["active_rpc_process_count"] == 2
    assert check["detail"]["active_run_over_limit"] is True
    assert check["detail"]["active_rpc_process_over_limit"] is True
    assert check["detail"]["warning_count"] >= 2
    assert check["detail"]["run_ids_included"] is False
    assert check["detail"]["session_ids_included"] is False
    assert check["detail"]["prompts_included"] is False
    assert check["detail"]["process_args_included"] is False
    assert check["detail"]["process_ids_included"] is False
    assert check["detail"]["session_files_included"] is False
    assert check["detail"]["environment_included"] is False
    assert check["detail"]["credentials_included"] is False
    assert check["detail"]["provider_urls_included"] is False
    assert check["detail"]["host_paths_included"] is False
    visible = json.dumps(check).lower()
    assert "run_secret" not in visible
    assert "sess_secret" not in visible
    assert "424242" not in visible
    assert "/home/private" not in visible
    assert "--append-system-prompt" not in visible
    assert "token=secret" not in visible
    assert "toolgate_execution_key" not in visible
    assert "private.invalid" not in visible
    assert "current_config" not in visible
    assert "processprobe" not in visible


def test_verification_snapshot_warns_on_available_nonlocal_notification_channel(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    monkeypatch.setenv("AGENTGATE_OWNER_TOKEN", "f" * 40)
    reset_state()

    with TestClient(app) as client:
        channel = client.post(
            "/api/notification-channels",
            json={
                "label": "studio desktop",
                "kind": "desktop",
                "status": "available",
                "description": "safe label only",
            },
        ).json()
        queued = client.post(
            f"/api/notification-channels/{channel['id']}/setup-approval",
            json={"summary": "safe desktop readiness label"},
        ).json()
        snapshot = client.get("/api/verification/snapshot").json()

    check = next(item for item in snapshot["checks"] if item["id"] == "notification-delivery-boundary")
    assert check["status"] == "warn"
    assert check["severity"] == "warning"
    assert check["detail"]["external_channels_marked_available"] == 1
    assert check["detail"]["pending_setup_reviews"] == 1
    assert check["detail"]["nonlocal_delivery_records"] == 0
    assert check["detail"]["suspicious_external_deliveries"] == 0
    assert check["detail"]["external_sender_configured"] is False
    assert check["detail"]["external_delivery_enabled"] is False
    assert check["detail"]["credentials_included"] is False
    assert check["detail"]["provider_urls_included"] is False
    assert queued["approval_status"] == "pending"
    visible = json.dumps(check).lower()
    assert "studio desktop" not in visible
    assert "safe desktop readiness label" not in visible
    assert "https://" not in visible
    assert "token=" not in visible


def test_verification_snapshot_reports_team_execution_policy_counts(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        agent = client.post(
            "/api/agents",
            json={
                "name": "Snapshot Team Agent",
                "purpose": "Participate in policy snapshot checks.",
            },
        ).json()
        reviewed_team = client.post(
            "/api/teams",
            json={
                "name": "Snapshot Reviewed Team",
                "purpose": "Reviewed group execution boundary.",
                "orchestrator_agent_id": "agent_pi_operator",
                "member_agent_ids": ["agent_pi_operator", agent["id"]],
                "orchestrator_policy": {
                    "approval_mode": "toolgate_required",
                    "review_status": "owner_reviewed",
                },
            },
        ).json()
        review = client.post(f"/api/teams/{reviewed_team['id']}/policy-review", json={}).json()
        client.post(
            f"/api/approvals/{review['toolgate_request']['id']}/decision",
            json={"decision": "approved"},
        )
        client.post(
            "/api/teams",
            json={
                "name": "Snapshot Pending Team",
                "purpose": "Private marker must not appear in verification details.",
                "orchestrator_agent_id": agent["id"],
                "member_agent_ids": ["agent_pi_operator", agent["id"]],
                "orchestrator_policy": {
                    "approval_mode": "metadata_only",
                    "review_status": "unreviewed",
                    "escalation_summary": "Private marker stays out of counts.",
                },
            },
        )
        snapshot = client.get("/api/verification/snapshot").json()

    check = next(item for item in snapshot["checks"] if item["id"] == "team-execution-policy-boundary")
    assert check["status"] == "warn"
    assert check["severity"] == "warning"
    assert check["detail"] == {
        "total_teams": 3,
        "multi_member_teams": 2,
        "owner_reviewed": 1,
        "toolgate_required": 1,
        "invalid_orchestrator": 0,
        "review_needed": 1,
    }
    assert "Private marker" not in str(check)
    assert "Snapshot Pending Team" not in str(check)


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


def test_system_access_boundary_repair_dry_run_does_not_create_keys(monkeypatch, tmp_path):
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
        preview = client.post(
            "/api/system/access-boundaries/repair",
            json={"scope": "all", "dry_run": True},
        ).json()
        after = client.get("/api/system").json()["access_boundaries"]["summary"]

    assert before["drift"] == 1
    assert preview["status"] == "dry_run"
    assert preview["dry_run"] is True
    assert preview["metadata_only"] is True
    assert preview["credentials_included"] is False
    assert preview["before"] == before
    assert preview["after"] == before
    assert after == before
    assert preview["repair"]["toolgate_contexts_checked"] == 2
    assert preview["repair"]["memorygate_contexts_checked"] == 2
    assert preview["repair"]["toolgate_contexts_repaired"] == 0
    assert preview["repair"]["memorygate_contexts_repaired"] == 0
    assert {row["toolgate"] for row in preview["contexts"] if row["toolgate"] != "not_requested"} == {"would_repair"}
    assert {row["memorygate"] for row in preview["contexts"] if row["memorygate"] != "not_requested"} == {"would_repair"}
    assert app.state.gates.toolgate_private_keys == {}
    assert app.state.gates.toolgate_private_key_scopes == {}
    assert app.state.gates.memorygate_private_keys == set()
    assert not any(
        word in str(preview).lower()
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
        patched = approve_team_policy(client, created["id"])
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


def test_auxiliary_model_routes_are_metadata_only(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        initial = client.get("/api/model/auxiliary-routes").json()
        saved = client.patch(
            "/api/model/auxiliary-routes/summary",
            json={
                "provider": "openai-codex",
                "model": "gpt-5.6-luna",
                "enabled": True,
                "purpose": "Summaries only; do not store token=abc123 or https://private.example/raw.",
                "risk_policy": "low_risk_only",
                "owner_review_status": "needs_review",
            },
        )
        missing = client.patch(
            "/api/model/auxiliary-routes/unknown",
            json={"provider": "openai-codex", "model": "gpt-5.6-luna"},
        )
        listed = client.get("/api/model/auxiliary-routes").json()

    assert initial["summary"]["total"] >= 4
    assert initial["safety"]["metadata_only"] is True
    assert saved.status_code == 200
    saved_body = saved.json()
    assert saved_body["task_id"] == "summary"
    assert saved_body["safety"]["metadata_only"] is True
    assert saved_body["safety"]["execution_enabled"] is False
    assert saved_body["safety"]["automatic_prompt_routing"] is False
    assert saved_body["route"]["provider"] == "openai-codex"
    assert saved_body["route"]["model"] == "gpt-5.6-luna"
    assert "token=abc123" not in str(saved_body)
    assert "private.example" not in str(saved_body)
    assert missing.status_code == 404
    summary_route = next(item for item in listed["routes"] if item["task_id"] == "summary")
    assert summary_route["provider"] == "openai-codex"
    assert listed["summary"]["enabled"] >= 1
    assert listed["safety"]["execution_enabled"] is False
    assert "token=abc123" not in str(listed)
    assert "private.example" not in str(listed)


def test_model_route_labels_reject_urls_and_credentials_before_toolgate(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        main._ensure_registry_seeded()
        before_proposals = len(app.state.model_route_proposals)
        response = client.post(
            "/api/model/routes/agent_pi_operator/save",
            json={
                "primary_provider": "freellmapi",
                "primary_model": "https://private.invalid/model?token=secret-value",
                "fallback_provider": "bearer secret-value",
                "fallback_model": "",
            },
        )
        aux = client.patch(
            "/api/model/auxiliary-routes/summary",
            json={
                "provider": "freellmapi",
                "model": "https://private.invalid/model?token=secret-value",
                "enabled": False,
            },
        )
        probe = client.post(
            "/api/model/route-check",
            json={"provider": "bearer secret-value", "model": "stealth/ox-alpha"},
        )

    assert response.status_code == 422
    assert aux.status_code == 422
    assert probe.status_code == 422
    assert len(app.state.model_route_proposals) == before_proposals
    visible = json.dumps({
        "route": response.json(),
        "aux": aux.json(),
        "probe": probe.json(),
        "proposals": app.state.model_route_proposals,
    }).lower()
    assert "https://private.invalid" not in visible
    assert "token=secret-value" not in visible
    assert "bearer secret-value" not in visible


def test_gateway_candidates_skip_hostile_model_labels():
    skipped = main._safe_gateway_model({
        "id": "https://private.invalid/model?token=secret-value",
        "owned_by": "bearer secret-value",
    })
    safe = main._safe_gateway_model({
        "id": "stealth/ox-alpha",
        "owned_by": "StealthProvider",
        "modalities": ["text"],
    })

    assert skipped is None
    assert safe
    assert safe["model"] == "stealth/ox-alpha"
    assert safe["provider"] == "freellmapi"
    assert "https://private.invalid" not in json.dumps(safe).lower()
    assert "token=secret-value" not in json.dumps(safe).lower()


def test_gateway_candidates_redact_hostile_metadata_fields():
    safe = main._safe_gateway_model({
        "id": "stealth/ox-alpha",
        "owned_by": "StealthProvider",
        "context_window": "128k https://private.invalid/context?token=secret-value",
        "modalities": ["text", "api_key=abc123", "bearer hidden-token"],
        "capabilities": ["reasoning", "tool_url=https://private.invalid/tool", "password=abc123"],
    })

    assert safe
    visible = json.dumps(safe).lower()
    assert "https://private.invalid" not in visible
    assert "token=secret-value" not in visible
    assert "api_key=abc123" not in visible
    assert "bearer hidden-token" not in visible
    assert "password=abc123" not in visible
    assert "private-" in visible


def test_model_providers_use_gateway_key_without_leaking_it(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    monkeypatch.setenv("FREE_LLM_API_URL", "http://freellmapi.internal:3001")
    monkeypatch.setenv("FREE_LLM_API_KEY", "free-gateway-secret")
    reset_state()
    calls = []

    class Response:
        def __init__(self, status_code, payload=None):
            self.status_code = status_code
            self._payload = payload or {}

        def json(self):
            return self._payload

    def fake_get(url, **kwargs):
        calls.append({"url": url, "headers": kwargs.get("headers") or {}})
        if url.endswith("/health"):
            return Response(200, {"status": "ok"})
        if url.endswith("/v1/models"):
            if kwargs.get("headers", {}).get("Authorization") == "Bearer free-gateway-secret":
                return Response(200, {"data": [{"id": "stealth/ox-alpha", "owned_by": "StealthProvider"}]})
            return Response(401, {"error": "missing key"})
        return Response(404, {})

    monkeypatch.setattr(main.httpx, "get", fake_get)

    with TestClient(app) as client:
        providers = client.get("/api/model/providers")
        candidates = client.get("/api/model/gateway-candidates")

    assert providers.status_code == 200
    assert candidates.status_code == 200
    free_provider = next(item for item in providers.json()["providers"] if item["id"] == "freellmapi")
    assert free_provider["status"] == "ok"
    assert free_provider["configured"] is True
    assert free_provider["models_visible"] is True
    assert free_provider["model_count"] == 1
    assert candidates.json()["gateway"]["configured"] is True
    assert candidates.json()["candidate_count"] == 1
    model_calls = [item for item in calls if item["url"].endswith("/v1/models")]
    assert model_calls
    assert all(item["headers"].get("Authorization") == "Bearer free-gateway-secret" for item in model_calls)
    visible = json.dumps({"providers": providers.json(), "candidates": candidates.json()}).lower()
    assert "free-gateway-secret" not in visible
    assert "freellmapi.internal" not in visible
    assert "authorization" not in visible


def test_model_route_approval_applies_and_rejects_metadata_only(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        main._ensure_registry_seeded()
        original = client.get("/api/agents/agent_pi_operator").json()
        queued = client.post(
            "/api/model/routes/agent_pi_operator/save",
            json={
                "primary_provider": "freellmapi",
                "primary_model": "stealth/ox-alpha",
                "fallback_provider": "",
                "fallback_model": "",
                "reason": "try https://private.invalid with token=secret-value",
            },
        ).json()
        request = app.state.gates.requests[queued["request_id"]]
        approved = client.post(
            f"/api/approvals/{queued['request_id']}/decision",
            json={"decision": "approved"},
        ).json()
        after_approval = client.get("/api/agents/agent_pi_operator").json()
        rejected = client.post(
            "/api/model/routes/agent_pi_operator/save",
            json={
                "primary_provider": "freellmapi",
                "primary_model": "stealth/reject-me",
                "fallback_provider": "",
                "fallback_model": "",
                "reason": "reject this one",
            },
        ).json()
        reject_decision = client.post(
            f"/api/approvals/{rejected['request_id']}/decision",
            json={"decision": "rejected"},
        ).json()
        after_reject = client.get("/api/agents/agent_pi_operator").json()

    visible = json.dumps({"queued": queued, "request": request, "approved": approved}).lower()
    assert original["primary_model"] != "stealth/ox-alpha"
    assert queued["status"] == "pending_approval"
    assert queued["safe_metadata_only"] is True
    assert queued["credentials_included"] is False
    assert queued["raw_prompts_included"] is False
    assert queued["upstream_details_included"] is False
    assert request["payload"]["metadata_only"] is True
    assert request["payload"]["route_digest"]
    assert request["payload"]["current_route_digest"]
    assert "owner_reason_digest" in request["payload"]
    assert "https://private.invalid" not in visible
    assert "token=secret-value" not in visible
    assert approved["model_route_status"] == "applied"
    assert after_approval["primary_provider"] == "freellmapi"
    assert after_approval["primary_model"] == "stealth/ox-alpha"
    assert reject_decision["model_route_status"] == "rejected"
    assert after_reject["primary_model"] == "stealth/ox-alpha"


def test_model_route_approval_goes_stale_after_agent_route_change(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        main._ensure_registry_seeded()
        app.state.agents["agent_pi_operator"]["primary_provider"] = "openai-codex"
        app.state.agents["agent_pi_operator"]["primary_model"] = "gpt-5.6-luna"
        queued = client.post(
            "/api/model/routes/agent_pi_operator/save",
            json={
                "primary_provider": "freellmapi",
                "primary_model": "stealth/stale-route",
                "fallback_provider": "",
                "fallback_model": "",
            },
        ).json()
        app.state.agents["agent_pi_operator"]["primary_model"] = "gpt-5.6-terra"
        stale = client.post(
            f"/api/approvals/{queued['request_id']}/decision",
            json={"decision": "approved"},
        ).json()
        after = client.get("/api/agents/agent_pi_operator").json()

    assert stale["model_route_status"] == "stale"
    assert stale["model_route_stale_reason"]
    assert after["primary_provider"] == "openai-codex"
    assert after["primary_model"] == "gpt-5.6-terra"


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
        activity_response = client.get("/api/agents/agent_pi_operator/activity").json()
        activity = activity_response["activity"]
        filtered_response = client.get(
            "/api/agents/agent_pi_operator/activity?status=ok&event_type=job.completed&limit=5"
        ).json()

    event_types = [item["event_type"] for item in activity]
    assert "chat.started" in event_types
    assert "chat.completed" in event_types
    assert "job.created" in event_types
    assert "job.completed" in event_types
    assert activity_response["summary"]["total_recent"] >= 4
    assert activity_response["summary"]["status_counts"]["ok"] >= 1
    assert activity_response["summary"]["event_type_counts"]["job.completed"] == 1
    assert "Pi adapter" in activity_response["available_filters"]["sources"]
    assert activity_response["safety"]["metadata_only"] is True
    assert filtered_response["summary"]["filtered_count"] == 1
    assert filtered_response["activity"][0]["event_type"] == "job.completed"
    assert filtered_response["filters"] == {"status": "ok", "event_type": "job.completed"}
    assert "Sensitive" not in str(activity_response)
    assert "Sensitive" not in str(filtered_response)
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
    pending = next(item for item in audit if item["event_type"] == "approval.pending")
    assert pending["ref_type"] == "approval"
    assert pending["ref_id"] == "req-pending"
    joined = str(audit)
    assert "token=abc123" not in joined
    assert "https://private.example" not in joined


def test_workstream_approval_ref_is_metadata_only(monkeypatch, tmp_path):
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
                "details": "Already reviewed token=old-secret https://old.example/path",
                "binding": {"type": "tool", "id": "echo", "version": "1", "digest": "sha256:old"},
                "decision": "approved",
                "decided_at": "2026-01-01T00:00:00+00:00",
                "decided_by": "Owner",
                "created_at": "2026-01-01T00:00:00+00:00",
            }]
        return [{
            "id": "req-pending",
            "source": "ToolGate",
            "severity": "high",
            "title": "Run echo token=approval-secret",
            "details": "Raw prompt and tool arguments api_key=abc123 https://private.example/path",
            "binding": {
                "type": "tool",
                "id": "echo token=binding-secret",
                "version": "1",
                "digest": "sha256:abc",
            },
            "created_at": "2026-01-02T00:00:00+00:00",
        }]

    monkeypatch.setattr(app.state.gates, "approvals", approvals)

    with TestClient(app) as client:
        response = client.get("/api/workstream?limit=20")
        detail = client.get("/api/workstream/refs/approval/req-pending")
        decided_detail = client.get("/api/workstream/refs/approval/req-decided")

    assert response.status_code == 200
    approval_event = next(item for item in response.json()["events"] if item["ref_id"] == "req-pending")
    assert approval_event["ref_type"] == "approval"
    assert detail.status_code == 200
    body = detail.json()
    assert body["ref_type"] == "approval"
    assert body["detail"]["schema"] == "agentgate.approval_ref_detail.v1"
    assert body["detail"]["details"] == "Stored in ToolGate"
    assert body["detail"]["details_chars"] > 0
    assert body["detail"]["request_fields_redacted"] is True
    assert body["insight"]["controls"]["schema"] == "agentgate.approval_controls.v1"
    assert body["insight"]["controls"]["metadata_only"] is True
    assert body["insight"]["controls"]["executes_from_drilldown"] is False
    assert body["insight"]["controls"]["approve"]["enabled"] is True
    assert body["insight"]["controls"]["approve"]["reason_code"] == "pending_owner_review"
    assert body["insight"]["controls"]["reject"]["enabled"] is True
    assert body["insight"]["controls"]["reject"]["reason_code"] == "pending_owner_review"
    decided_controls = decided_detail.json()["insight"]["controls"]
    assert decided_controls["approve"]["enabled"] is False
    assert decided_controls["approve"]["reason_code"] == "already_decided"
    assert decided_controls["reject"]["enabled"] is False
    joined = json.dumps(body).lower()
    for forbidden in [
        "approval-secret",
        "binding-secret",
        "old-secret",
        "api_key=abc123",
        "https://private.example",
        "raw prompt",
        "tool arguments",
        "payload",
        "/api/",
        "/v2/",
    ]:
        assert forbidden not in joined
    assert "raw tool arguments" not in joined.lower()


def test_session_workstream_detail_is_digest_count_only(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()
    app.state.pi = CapturingPi()

    with TestClient(app) as client:
        created = client.post(
            "/api/sessions",
            json={
                "title": "private room token=session-title-secret https://session.example/path",
            },
        )
        assert created.status_code == 200
        session_id = created.json()["id"]
        chat = client.post(
            f"/api/sessions/{session_id}/chat/stream",
            json={"input": "session prompt token=session-secret https://session.example/prompt"},
        )
        assert chat.status_code == 200
        app.state.active_runs[session_id] = "run_session_secret_123"
        drilldown = client.get(f"/api/workstream/refs/session/{session_id}")
        workstream = client.get("/api/workstream?limit=50")
        app.state.active_runs.pop(session_id, None)

    assert drilldown.status_code == 200
    detail = drilldown.json()["detail"]
    assert detail["schema"] == "agentgate.session_ref_detail.v1"
    assert detail["id"] == session_id
    assert detail["title_present"] is True
    assert detail["title_digest"]
    assert detail["title_chars"] > 0
    assert detail["message_count"] == 2
    assert detail["message_role_counts"]["user"] == 1
    assert detail["message_role_counts"]["assistant"] == 1
    assert detail["active_run_present"] is True
    assert "title" not in detail
    assert "agent_id" not in detail
    assert "team_id" not in detail
    assert "current_speaker_id" not in detail
    assert drilldown.json()["insight"]["controls"]["schema"] == "agentgate.session_controls.v1"
    assert drilldown.json()["insight"]["controls"]["executes_from_drilldown"] is False
    assert drilldown.json()["insight"]["controls"]["session_stop_boundary"]["enabled"] is True
    joined = json.dumps({"drilldown": drilldown.json(), "workstream": workstream.json()}).lower()
    for forbidden in [
        "session-title-secret",
        "session-secret",
        "https://session.example",
        "private room token",
        "session prompt token",
        "run_session_secret_123",
        "current_speaker_id",
        "participant_agent_ids",
        "\"content\":",
        "/api/sessions",
        "/v1/runs",
    ]:
        assert forbidden not in joined


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
        task_detail = client.get(f"/api/workstream/refs/task/{task.json()['id']}")
        memory_detail = client.get("/api/workstream/refs/memory_candidate/memcand_workstream")
        tool_detail = client.get(f"/api/workstream/refs/tool_draft/{draft.json()['id']}")
        ghost_detail = client.get("/api/workstream/refs/job/job_missing_from_runtime")
        agent_detail = client.get("/api/workstream/refs/agent/agent_pi_operator")
        team_detail = client.get("/api/workstream/refs/team/team_core")

    assert response.status_code == 200
    assert job_detail.status_code == 200
    assert task_detail.status_code == 200
    assert memory_detail.status_code == 200
    assert tool_detail.status_code == 200
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
        assert forbidden not in task_detail.text.lower()
        assert forbidden not in memory_detail.text.lower()
        assert forbidden not in tool_detail.text.lower()
        assert forbidden not in agent_detail.text.lower()
        assert forbidden not in team_detail.text.lower()
    job_body = job_detail.json()
    assert job_body["ref_type"] == "job"
    assert job_body["detail"]["name"] == "Morning proof"
    assert job_body["detail"]["failure_policy"]["automatic_retries"] is False
    assert job_body["insight"]["schema"] == "agentgate.workstream_ref_insight.v1"
    assert job_body["insight"]["safety"]["metadata_only"] is True
    assert job_body["insight"]["safety"]["actions_executed"] is False
    assert job_body["insight"]["safety"]["jobs_started"] is False
    assert job_body["insight"]["signal_counts"]["runs"] == 0
    assert "Open Automations" in job_body["insight"]["owner_next_step"]
    controls = job_body["insight"]["controls"]
    assert controls["schema"] == "agentgate.job_controls.v1"
    assert controls["metadata_only"] is True
    assert controls["executes_from_drilldown"] is False
    assert controls["pause"]["enabled"] is True
    assert controls["pause"]["reason_code"] == "schedule_active"
    assert controls["run_now"]["enabled"] is True
    assert controls["run_now"]["reason_code"] == "ready"
    assert controls["stop"]["enabled"] is False
    assert controls["stop"]["reason_code"] == "no_active_run"
    assert job_body["safety"]["mode"] == "metadata_only"
    task_body = task_detail.json()
    assert task_body["ref_type"] == "task"
    assert task_body["detail"]["title"] == "Safe task title"
    assert task_body["detail"]["summary"] == "Stored server-side"
    assert "summary_digest" in task_body["detail"]
    assert "task-secret" not in json.dumps(task_body).lower()
    assert task_body["insight"]["safety"]["actions_executed"] is False
    assert "Open Tasks" in task_body["insight"]["owner_next_step"]
    task_controls = task_body["insight"]["controls"]
    assert task_controls["schema"] == "agentgate.task_controls.v1"
    assert task_controls["metadata_only"] is True
    assert task_controls["executes_from_drilldown"] is False
    assert task_controls["checkpoint_review"]["enabled"] is False
    assert task_controls["checkpoint_review"]["reason_code"] == "checkpoint_not_required"
    assert task_controls["open_scoped_room"]["enabled"] is True
    assert task_controls["open_scoped_room"]["reason_code"] == "ready_no_checkpoint"
    memory_body = memory_detail.json()
    assert memory_body["ref_type"] == "memory_candidate"
    assert memory_body["detail"]["memory_type"] == "preference"
    assert memory_body["detail"]["confidence"] == "high"
    assert memory_body["insight"]["safety"]["memory_written"] is False
    memory_controls = memory_body["insight"]["controls"]
    assert memory_controls["schema"] == "agentgate.memory_candidate_controls.v1"
    assert memory_controls["metadata_only"] is True
    assert memory_controls["executes_from_drilldown"] is False
    assert memory_controls["approve_memory"]["enabled"] is True
    assert memory_controls["approve_memory"]["reason_code"] == "pending_owner_review"
    assert memory_controls["reject_memory"]["enabled"] is True
    assert memory_controls["reject_memory"]["reason_code"] == "pending_owner_review"
    assert memory_controls["delete_candidate"]["enabled"] is True
    assert memory_controls["delete_candidate"]["reason_code"] == "pending_or_rejected_record"
    assert "Open Memory review" in memory_body["insight"]["owner_next_step"]
    assert "text" not in memory_body["detail"]
    assert memory_body["safety"]["mode"] == "metadata_only"
    tool_body = tool_detail.json()
    assert tool_body["ref_type"] == "tool_draft"
    assert tool_body["insight"]["safety"]["tools_installed"] is False
    tool_controls = tool_body["insight"]["controls"]
    assert tool_controls["schema"] == "agentgate.tool_draft_controls.v1"
    assert tool_controls["metadata_only"] is True
    assert tool_controls["executes_from_drilldown"] is False
    assert tool_controls["review_readiness"]["enabled"] is True
    assert tool_controls["review_readiness"]["reason_code"] == "ready_for_toolgate_review"
    assert tool_controls["package_readiness"]["enabled"] is False
    assert tool_controls["package_readiness"]["reason_code"] == "needs_toolgate_review"
    assert tool_controls["lifecycle_boundary"]["enabled"] is True
    assert tool_controls["lifecycle_boundary"]["reason_code"] == "safe_non_approved_record"
    assert "Open Tools" in tool_body["insight"]["owner_next_step"]
    ghost_body = ghost_detail.json()
    assert ghost_body["ref_type"] == "job"
    assert ghost_body["detail"]["available"] is False
    assert ghost_body["detail"]["state"] == "audit_only"
    assert ghost_body["insight"]["available"] is False
    assert "audit-only" in ghost_body["insight"]["badges"]
    assert ghost_body["events"][0]["ref_id"] == "job_missing_from_runtime"
    agent_body = agent_detail.json()
    assert agent_body["ref_type"] == "agent"
    assert agent_body["detail"]["id"] == "agent_pi_operator"
    assert agent_body["detail"]["schema"] == "agentgate.agent_ref_detail.v1"
    assert "profile_readiness" in agent_body["detail"]
    assert "soul" not in agent_body["detail"]
    assert "purpose" not in agent_body["detail"]
    assert agent_body["insight"]["controls"]["schema"] == "agentgate.agent_controls.v1"
    assert "Open the Agent profile" in agent_body["insight"]["owner_next_step"]
    assert agent_body["safety"]["mode"] == "metadata_only"
    team_body = team_detail.json()
    assert team_body["ref_type"] == "team"
    assert team_body["detail"]["id"] == "team_core"
    assert team_body["detail"]["schema"] == "agentgate.team_ref_detail.v1"
    assert "orchestration_readiness" in team_body["detail"]
    assert "purpose" not in team_body["detail"]
    assert "member_agent_ids" not in team_body["detail"]
    assert team_body["insight"]["controls"]["schema"] == "agentgate.team_controls.v1"
    assert "Open the Team workroom" in team_body["insight"]["owner_next_step"]
    assert team_body["safety"]["mode"] == "metadata_only"


def test_open_loop_radar_uses_only_safe_workstream_metadata(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    def approvals(*, history: bool = False):
        if history:
            return []
        return [{
            "id": "req-radar",
            "source": "ToolGate",
            "severity": "high",
            "title": "Review echo token=approval-title-secret",
            "details": "Do not expose raw prompt api_key=approval-detail-secret https://approval.example/path",
            "binding": {
                "type": "tool",
                "id": "echo token=approval-binding-secret",
                "version": "1",
                "digest": "sha256:radar",
            },
            "created_at": "2026-01-02T00:00:00+00:00",
        }]

    monkeypatch.setattr(app.state.gates, "approvals", approvals)
    with TestClient(app) as client:
        app.state.memory_candidates = {
            "mem-radar": {
                "id": "mem-radar",
                "text": "private memory token=memory-secret https://memory.example/path /home/alexey/private",
                "status": "pending",
                "memory_type": "preference",
                "confidence": "high",
                "tags": ["safe"],
                "evidence": {
                    "session_id": "sess-1",
                    "source_message_id": "msg-secret",
                    "source_role": "user",
                },
                "created_at": "2026-01-03T00:00:00+00:00",
            }
        }
        response = client.get("/api/open-loops?limit=8")

    assert response.status_code == 200
    body = response.json()
    assert body["schema"] == "agentgate.open_loops.v1"
    assert body["summary"]["total"] >= 2
    assert body["summary"]["needs_approval"] == 1
    assert body["summary"]["by_status"]["needs-approval"] == 1
    assert body["summary"]["by_target_path"]["/approvals"] == 1
    assert body["summary"]["by_target_path"]["/memory"] >= 1
    assert body["summary"]["by_source_kind"]["toolgate-approval"] == 1
    assert body["summary"]["warning_count"] == body["summary"]["total"]
    assert body["safety"]["metadata_only"] is True
    assert body["safety"]["actions_executed"] is False
    assert body["safety"]["approvals_decided"] is False
    assert body["safety"]["memory_written"] is False
    assert body["safety"]["tool_arguments_included"] is False
    assert body["safety"]["credentials_included"] is False
    approval_loop = next(item for item in body["loops"] if item["source"]["ref_type"] == "approval")
    memory_loop = next(item for item in body["loops"] if item["source"]["ref_type"] == "memory_candidate")
    assert approval_loop["status"] == "needs-approval"
    assert approval_loop["approval_required"] is True
    assert approval_loop["target_path"] == "/approvals"
    assert approval_loop["signal"] == "ToolGate request is waiting for owner decision."
    assert memory_loop["status"] == "owner-review"
    assert memory_loop["approval_required"] is False
    assert memory_loop["target_path"] == "/memory"
    assert "enabled controls" in " ".join(memory_loop["evidence"])

    joined = json.dumps(body).lower()
    for forbidden in [
        "approval-title-secret",
        "approval-detail-secret",
        "approval-binding-secret",
        "memory-secret",
        "https://approval.example",
        "https://memory.example",
        "/home/alexey",
        "api_key=approval",
        "raw prompt",
        "tool arguments api",
        "\"text\":",
        "\"content\":",
        "\"provider_url\":",
        "\"run_id\":",
        "/api/sessions",
        "/v1/runs",
    ]:
        assert forbidden not in joined


def test_workstream_agent_team_details_are_digest_only(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        main._ensure_registry_seeded()
        agent = client.post(
            "/api/agents",
            json={
                "name": "Private Coach",
                "title": "Private title token=title-secret",
                "purpose": "Never expose purpose token=purpose-secret https://agent.example/path",
                "soul": "Never expose soul token=soul-secret bearer abc123 raw prompt notes.",
                "voice": "Never expose voice password=voice-secret https://voice.example/path",
                "story": "Never expose story secret=story-secret /home/private/story.md",
                "primary_provider": "openai",
                "primary_model": "model-safe",
                "tool_ids": ["tool-secret=https://tool.example"],
                "skill_ids": ["skill-token=skill-secret"],
                "memory_scopes": ["memory-secret=memory-secret"],
            },
        ).json()
        team = client.post(
            "/api/teams",
            json={
                "name": "Private Team",
                "purpose": "Never expose team purpose token=team-secret https://team.example/path",
                "orchestrator_agent_id": agent["id"],
                "member_agent_ids": [agent["id"]],
                "tool_ids": ["team-tool-token=tool-secret"],
                "skill_ids": ["team-skill-token=skill-secret"],
                "memory_scopes": ["team-memory-token=memory-secret"],
                "orchestrator_policy": {
                    "handoff_mode": "manual",
                    "approval_mode": "toolgate_required",
                    "review_status": "unreviewed",
                    "notes": "Never expose policy token=policy-secret https://policy.example/path",
                },
            },
        ).json()
        agent_detail = client.get(f"/api/workstream/refs/agent/{agent['id']}")
        team_detail = client.get(f"/api/workstream/refs/team/{team['id']}")

    assert agent_detail.status_code == 200
    agent_body = agent_detail.json()
    agent_safe = agent_body["detail"]
    assert agent_safe["schema"] == "agentgate.agent_ref_detail.v1"
    assert agent_safe["purpose_present"] is True
    assert agent_safe["purpose_digest"]
    assert agent_safe["soul_present"] is True
    assert agent_safe["soul_digest"]
    assert agent_safe["voice_present"] is True
    assert agent_safe["story_present"] is True
    assert agent_safe["tool_count"] == 1
    assert agent_safe["skill_count"] == 1
    assert agent_safe["memory_scope_count"] == 1
    assert agent_body["insight"]["controls"]["schema"] == "agentgate.agent_controls.v1"
    assert agent_body["insight"]["controls"]["metadata_only"] is True
    assert agent_body["insight"]["controls"]["executes_from_drilldown"] is False
    assert agent_body["insight"]["controls"]["profile_readiness"]["reason_code"] == "needs_owner_review"
    assert agent_body["insight"]["controls"]["access_boundary"]["reason_code"] == "grants_present"
    assert agent_body["insight"]["controls"]["model_route_boundary"]["reason_code"] == "route_present"

    assert team_detail.status_code == 200
    team_body = team_detail.json()
    team_safe = team_body["detail"]
    assert team_safe["schema"] == "agentgate.team_ref_detail.v1"
    assert team_safe["purpose_present"] is True
    assert team_safe["purpose_digest"]
    assert team_safe["member_count"] == 1
    assert team_safe["tool_count"] == 1
    assert team_safe["skill_count"] == 1
    assert team_safe["memory_scope_count"] == 1
    assert team_body["insight"]["controls"]["schema"] == "agentgate.team_controls.v1"
    assert team_body["insight"]["controls"]["metadata_only"] is True
    assert team_body["insight"]["controls"]["executes_from_drilldown"] is False
    assert team_body["insight"]["controls"]["policy_readiness"]["reason_code"] == "needs_owner_review"
    assert team_body["insight"]["controls"]["group_execution_boundary"]["reason_code"] == "group_execution_blocked"
    assert team_body["insight"]["controls"]["access_boundary"]["reason_code"] == "shared_grants_present"
    detail_joined = json.dumps({"agent": agent_safe, "team": team_safe}).lower()
    for forbidden_field in [
        '"member_agent_ids"',
        '"tool_ids"',
        '"skill_ids"',
        '"memory_scopes"',
        '"orchestrator_policy"',
    ]:
        assert forbidden_field not in detail_joined

    joined = json.dumps({"agent": agent_body, "team": team_body}).lower()
    for forbidden in [
        "purpose-secret",
        "soul-secret",
        "voice-secret",
        "story-secret",
        "title-secret",
        "team-secret",
        "policy-secret",
        "skill-secret",
        "memory-secret",
        "tool.example",
        "agent.example",
        "voice.example",
        "team.example",
        "policy.example",
        "/home/private",
        "raw prompt",
        "bearer abc123",
        "tool-secret=https",
    ]:
        assert forbidden not in joined


def test_workstream_memory_candidate_controls_are_metadata_only(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        pending = client.post(
            "/api/memory/candidates",
            json={
                "text": "private memory token=memory-secret https://memory.example/path raw transcript",
                "session_id": "sess-1",
                "source_message_id": "a1",
                "candidate_id": "memcand_control_pending",
                "memory_type": "preference",
                "confidence": "high",
            },
        )
        assert pending.status_code == 200
        pending_detail = client.get("/api/workstream/refs/memory_candidate/memcand_control_pending")
        approved = client.post(
            "/api/memory/candidates",
            json={
                "text": "approved private memory token=approved-secret https://approved.example/path",
                "session_id": "sess-1",
                "source_message_id": "a1",
                "candidate_id": "memcand_control_approved",
                "approved": True,
            },
        )
        assert approved.status_code == 200
        approved_detail = client.get("/api/workstream/refs/memory_candidate/memcand_control_approved")
        rejected = client.post(
            "/api/memory/candidates",
            json={
                "text": "rejected private memory token=rejected-secret https://rejected.example/path",
                "session_id": "sess-1",
                "source_message_id": "a1",
                "candidate_id": "memcand_control_rejected",
            },
        )
        assert rejected.status_code == 200
        client.post("/api/memory/candidates/memcand_control_rejected/reject")
        rejected_detail = client.get("/api/workstream/refs/memory_candidate/memcand_control_rejected")

    assert pending_detail.status_code == 200
    body = pending_detail.json()
    controls = body["insight"]["controls"]
    assert controls["schema"] == "agentgate.memory_candidate_controls.v1"
    assert controls["metadata_only"] is True
    assert controls["executes_from_drilldown"] is False
    assert controls["approve_memory"]["enabled"] is True
    assert controls["approve_memory"]["reason_code"] == "pending_owner_review"
    assert controls["reject_memory"]["enabled"] is True
    assert controls["reject_memory"]["reason_code"] == "pending_owner_review"
    assert controls["delete_candidate"]["enabled"] is True
    assert controls["delete_candidate"]["reason_code"] == "pending_or_rejected_record"
    joined = json.dumps(body).lower()
    for forbidden in [
        "memory-secret",
        "https://memory.example",
        "raw transcript",
        "private memory",
        "/api/memory",
        "/v2/",
    ]:
        assert forbidden not in joined
    approved_controls = approved_detail.json()["insight"]["controls"]
    assert approved_controls["approve_memory"]["enabled"] is False
    assert approved_controls["approve_memory"]["reason_code"] == "already_approved"
    assert approved_controls["reject_memory"]["enabled"] is False
    assert approved_controls["delete_candidate"]["enabled"] is False
    assert approved_controls["delete_candidate"]["reason_code"] == "approved_audit_history"
    rejected_controls = rejected_detail.json()["insight"]["controls"]
    assert rejected_controls["approve_memory"]["enabled"] is False
    assert rejected_controls["approve_memory"]["reason_code"] == "already_rejected"
    assert rejected_controls["reject_memory"]["enabled"] is False
    assert rejected_controls["delete_candidate"]["enabled"] is True
    assert rejected_controls["delete_candidate"]["reason_code"] == "pending_or_rejected_record"


def test_workstream_tool_draft_controls_are_metadata_only(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        pending = client.post(
            "/api/tool-drafts",
            json={
                "title": "Private shell helper token=title-secret password=bad /home/alexeybe1kin/private raw_command=rm",
                "purpose": "Never expose token=tool-secret https://tool.example/path raw command arguments.",
                "proposed_tool_id": "Private Shell Helper",
                "risk": "high",
            },
        ).json()
        draft_detail = client.get(f"/api/workstream/refs/tool_draft/{pending['id']}")
        review = client.post(f"/api/tool-drafts/{pending['id']}/toolgate-review").json()
        pending_detail = client.get(f"/api/workstream/refs/tool_draft/{pending['id']}")
        app.state.gates.decide_approval(review["toolgate_request_id"], "approved")
        approved_detail = client.get(f"/api/workstream/refs/tool_draft/{pending['id']}")
        package = client.post(f"/api/tool-drafts/{pending['id']}/package-proposal")
        package_detail = client.get(f"/api/workstream/refs/tool_draft/{pending['id']}")

    assert draft_detail.status_code == 200
    controls = draft_detail.json()["insight"]["controls"]
    detail = draft_detail.json()["detail"]
    assert detail["schema"] == "agentgate.tool_draft_ref_detail.v1"
    assert "purpose" not in detail
    assert "package_proposal" not in detail
    assert detail["purpose_present"] is True
    assert detail["purpose_chars"] > 0
    assert detail["purpose_digest"]
    assert controls["schema"] == "agentgate.tool_draft_controls.v1"
    assert controls["metadata_only"] is True
    assert controls["executes_from_drilldown"] is False
    assert controls["review_readiness"]["enabled"] is True
    assert controls["review_readiness"]["reason_code"] == "ready_for_toolgate_review"
    assert controls["package_readiness"]["enabled"] is False
    assert controls["package_readiness"]["reason_code"] == "needs_toolgate_review"
    assert controls["lifecycle_boundary"]["enabled"] is True
    joined = json.dumps(draft_detail.json()).lower()
    for forbidden in [
        "tool-secret",
        "title-secret",
        "password=bad",
        "https://tool.example",
        "/home/alexeybe1kin",
        "raw_command",
        "raw command",
        "/api/tool-drafts",
        "/v2/",
        "register",
    ]:
        assert forbidden not in joined

    pending_controls = pending_detail.json()["insight"]["controls"]
    assert pending_controls["review_readiness"]["enabled"] is False
    assert pending_controls["review_readiness"]["reason_code"] == "review_already_pending"
    assert pending_controls["package_readiness"]["enabled"] is False
    assert pending_controls["package_readiness"]["reason_code"] == "waiting_for_toolgate_approval"
    assert pending_controls["lifecycle_boundary"]["enabled"] is True

    approved_controls = approved_detail.json()["insight"]["controls"]
    assert approved_controls["review_readiness"]["enabled"] is False
    assert approved_controls["review_readiness"]["reason_code"] == "already_approved"
    assert approved_controls["package_readiness"]["enabled"] is True
    assert approved_controls["package_readiness"]["reason_code"] == "toolgate_approved"
    assert approved_controls["lifecycle_boundary"]["enabled"] is False
    assert approved_controls["lifecycle_boundary"]["reason_code"] == "approved_audit_history"

    assert package.status_code == 200
    package_controls = package_detail.json()["insight"]["controls"]
    package_detail_body = package_detail.json()["detail"]
    assert package_detail_body["package_proposal_present"] is True
    assert package_detail_body["package_proposal_digest"]
    assert "package_proposal" not in package_detail_body
    assert package_controls["package_readiness"]["enabled"] is False
    assert package_controls["package_readiness"]["reason_code"] == "package_already_prepared"


def test_workstream_job_controls_hide_raw_active_run_id(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        created = client.post(
            "/api/jobs",
            json={
                "name": "Running proof",
                "schedule": "0 9 * * *",
                "prompt": "private job prompt token=job-secret https://job.example/path",
            },
        ).json()
        app.state.active_job_runs[created["id"]] = {
            "run_id": "pi_raw_run_secret_123",
            "started_at": main.now(),
        }
        detail = client.get(f"/api/workstream/refs/job/{created['id']}")

    assert detail.status_code == 200
    body = detail.json()
    controls = body["insight"]["controls"]
    assert controls["stop"]["enabled"] is True
    assert controls["stop"]["reason_code"] == "active_run"
    assert controls["run_now"]["enabled"] is False
    assert controls["run_now"]["reason_code"] == "already_running"
    assert controls["pause"]["enabled"] is True
    joined = json.dumps(body).lower()
    assert "pi_raw_run_secret_123" not in joined
    assert "run_id" not in json.dumps(controls).lower()
    assert "job-secret" not in joined
    assert "https://job.example" not in joined
    assert "private job prompt" not in joined


def test_workstream_task_controls_are_metadata_only_for_owner_checkpoint(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        task = client.post(
            "/api/tasks",
            json={
                "title": "Owner checkpoint proof",
                "summary": "private task summary token=task-secret https://task.example/path",
                "owner_checkpoint": True,
                "checkpoint_note": "checkpoint note password=note-secret https://note.example/path",
            },
        ).json()
        detail = client.get(f"/api/workstream/refs/task/{task['id']}")

    assert detail.status_code == 200
    body = detail.json()
    controls = body["insight"]["controls"]
    assert controls["schema"] == "agentgate.task_controls.v1"
    assert controls["metadata_only"] is True
    assert controls["executes_from_drilldown"] is False
    assert controls["checkpoint_review"]["enabled"] is True
    assert controls["checkpoint_review"]["reason_code"] == "ready_for_review"
    assert controls["open_scoped_room"]["enabled"] is False
    assert controls["open_scoped_room"]["reason_code"] == "checkpoint_pending"
    joined = json.dumps(body).lower()
    assert "task-secret" not in joined
    assert "note-secret" not in joined
    assert "https://task.example" not in joined
    assert "https://note.example" not in joined
    assert body["detail"]["summary"] == "Stored server-side"
    assert body["detail"]["checkpoint_note"] == "Stored server-side"
    assert body["detail"]["summary_chars"] > 0
    assert body["detail"]["checkpoint_note_chars"] > 0
    assert body["detail"]["summary_digest"] != body["detail"]["checkpoint_note_digest"]
    assert "/api/tasks" not in joined
    assert "private task summary" not in joined
    assert "checkpoint note password" not in joined


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
        activity_body = client.get(
            "/api/teams/team_core/activity",
            params={"event_type": "chat.completed", "limit": 40},
        ).json()
        activity = activity_body["activity"]
        lifecycle_team = client.post(
            "/api/teams",
            json={
                "name": "Lifecycle Lens Team",
                "purpose": "Purpose with token=abc123 and https://private.example omitted from activity.",
                "orchestrator_agent_id": "agent_pi_operator",
                "member_agent_ids": ["agent_pi_operator"],
            },
        ).json()
        lifecycle_activity_body = client.get(
            f"/api/teams/{lifecycle_team['id']}/activity",
            params={"event_type": "team.created", "limit": 10},
        ).json()
        client.delete(f"/api/teams/{lifecycle_team['id']}")

    core_rollup = next(item for item in teams if item["id"] == "team_core")
    assert core_rollup["recent_activity"]
    assert team["recent_activity"]
    assert [item["team_id"] for item in activity]
    assert all(item["team_id"] == "team_core" for item in activity)
    assert "chat.completed" in [item["event_type"] for item in activity]
    assert activity_body["summary"]["filtered_count"] >= 1
    assert activity_body["summary"]["event_type_counts"]["chat.completed"] >= 1
    assert activity_body["filters"] == {"event_type": "chat.completed"}
    assert activity_body["safety"]["metadata_only"] is True
    assert "Sensitive" not in str(activity)
    assert lifecycle_activity_body["summary"]["filtered_count"] == 1
    assert lifecycle_activity_body["activity"][0]["event_type"] == "team.created"
    assert lifecycle_activity_body["activity"][0]["team_id"] == lifecycle_team["id"]
    assert lifecycle_activity_body["safety"]["metadata_only"] is True
    assert "token=abc123" not in str(lifecycle_activity_body)
    assert "private.example" not in str(lifecycle_activity_body)


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
        direct_bypass = client.patch(f"/api/tasks/{task['id']}", json={"checkpoint_status": "approved"})
        disable_bypass = client.patch(f"/api/tasks/{task['id']}", json={"owner_checkpoint": False})
        queued = client.post(f"/api/tasks/{task['id']}/checkpoint-approval")
        request_id = queued.json()["approval_request_id"]
        approved_decision = client.post(f"/api/approvals/{request_id}/decision", json={"decision": "approved"})
        approved = client.get(f"/api/tasks/{task['id']}").json()
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
    assert direct_bypass.status_code == 409
    assert disable_bypass.status_code == 409
    assert queued.status_code == 200
    assert queued.json()["safety"]["metadata_only"] is True
    assert queued.json()["safety"]["raw_summary_included"] is False
    assert queued.json()["task"]["checkpoint_approval_status"] == "pending"
    assert app.state.gates.request_status(request_id)["kind"] == "task_checkpoint_review"
    assert app.state.gates.request_status(request_id)["payload"]["summary_digest"]
    assert "Waits for dependency" not in str(app.state.gates.request_status(request_id))
    assert approved_decision.status_code == 200
    assert approved["checkpoint_status"] == "approved"
    assert approved["checkpoint_approval_status"] == "approved"
    assert opened.status_code == 200
    assert opened.json()["session"]["task_id"] == task["id"]
    event_types = [item["event_type"] for item in activity]
    assert "task.checkpoint_requested" in event_types
    assert "task.checkpoint_approval_requested" in event_types
    assert "task.checkpoint_approved" in event_types
    assert "password" not in str(opened.json()).lower()
    assert "api_key" not in str(opened.json()).lower()
    assert "token" not in str(opened.json()).lower()


def test_task_checkpoint_toolgate_review_goes_stale_after_task_change(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        main._ensure_registry_seeded()
        task = client.post(
            "/api/tasks",
            json={
                "title": "Mutable checkpoint",
                "summary": "private summary token=task-secret https://task.example/path",
                "agent_id": "agent_pi_operator",
                "team_id": "team_core",
                "owner_checkpoint": True,
            },
        ).json()
        queued = client.post(f"/api/tasks/{task['id']}/checkpoint-approval").json()
        request = app.state.gates.request_status(queued["approval_request_id"])
        changed = client.patch(f"/api/tasks/{task['id']}", json={"risk": "high"}).json()
        decided = client.post(
            f"/api/approvals/{queued['approval_request_id']}/decision",
            json={"decision": "approved"},
        )
        after = client.get(f"/api/tasks/{task['id']}").json()
        blocked = client.post(f"/api/tasks/{task['id']}/session")

    assert request["payload"]["task_fingerprint"]
    assert "task-secret" not in str(request)
    assert "https://task.example" not in str(request)
    assert "task-secret" not in str(queued)
    assert "https://task.example" not in str(queued)
    assert changed["checkpoint_approval_status"] == "stale"
    assert decided.status_code == 200
    assert after["checkpoint_status"] == "pending"
    assert after["checkpoint_approval_status"] == "stale"
    assert "changed after ToolGate review" in after["checkpoint_approval_stale_reason"]
    assert blocked.status_code == 409


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
                "orchestrator_policy": {
                    "approval_mode": "toolgate_required",
                    "review_status": "owner_reviewed",
                },
            },
        ).json()
        team = approve_team_policy(client, team["id"])
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


def test_group_round_requires_owner_reviewed_team_policy(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()
    app.state.pi = MultiSpeakerPi()

    with TestClient(app) as client:
        teammate = client.post(
            "/api/agents",
            json={"name": "Group Teammate", "purpose": "Participate only after team policy review."},
        ).json()
        team = client.post(
            "/api/teams",
            json={
                "name": "Unreviewed Group Team",
                "purpose": "Should not run before review.",
                "orchestrator_agent_id": "agent_pi_operator",
                "member_agent_ids": ["agent_pi_operator", teammate["id"]],
            },
        ).json()
        session = client.post(
            "/api/sessions",
            json={
                "title": "Blocked Group Round",
                "agent_id": "agent_pi_operator",
                "team_id": team["id"],
                "participant_agent_ids": ["agent_pi_operator", teammate["id"]],
            },
        ).json()
        result = client.post(
            f"/api/sessions/{session['id']}/group-round",
            json={"input": "private marker should not be echoed in the block."},
        )

    assert result.status_code == 409
    detail = result.json()["detail"]
    assert detail["reason"] == "team_policy_review_required"
    assert detail["team_id"] == team["id"]
    assert detail["review_status"] == "unreviewed"
    assert "policy_review" in detail["missing_fields"]
    assert app.state.pi.calls == []
    assert "private marker" not in str(detail)


def test_team_policy_review_requires_toolgate_decision(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        teammate = client.post(
            "/api/agents",
            json={"name": "Policy Teammate", "purpose": "Participate after review."},
        ).json()
        team = client.post(
            "/api/teams",
            json={
                "name": "Policy Review Team",
                "purpose": "Needs ToolGate policy review before group runs.",
                "orchestrator_agent_id": "agent_pi_operator",
                "member_agent_ids": ["agent_pi_operator", teammate["id"]],
                "orchestrator_policy": {
                    "approval_mode": "toolgate_required",
                    "review_status": "owner_reviewed",
                },
            },
        ).json()
        patched = client.patch(
            f"/api/teams/{team['id']}",
            json={
                "orchestrator_policy": {
                    "approval_mode": "toolgate_required",
                    "review_status": "owner_reviewed",
                }
            },
        ).json()
        review = client.post(
            f"/api/teams/{team['id']}/policy-review",
            json={"owner_note": "Approve bounded execution. token=policy-secret https://policy.example/path"},
        ).json()
        decided = client.post(
            f"/api/approvals/{review['toolgate_request']['id']}/decision",
            json={"decision": "approved"},
        ).json()
        fetched = client.get(f"/api/teams/{team['id']}").json()

    assert team["orchestrator_policy"]["review_status"] == "needs_review"
    assert patched["orchestrator_policy"]["review_status"] != "owner_reviewed"
    assert review["orchestrator_policy"]["review_status"] == "needs_review"
    assert review["team_policy_review"]["status"] == "pending"
    assert decided["team_policy_status"] == "owner_reviewed"
    assert fetched["orchestrator_policy"]["review_status"] == "owner_reviewed"
    assert fetched["orchestration_readiness"]["ready"] is True
    forbidden = re.compile(r"policy-secret|https://policy\\.example|token=", re.I)
    assert not forbidden.search(str(review))


def test_team_policy_review_goes_stale_after_policy_change(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()

    with TestClient(app) as client:
        teammate = client.post(
            "/api/agents",
            json={"name": "Stale Policy Teammate", "purpose": "Participate after review."},
        ).json()
        team = client.post(
            "/api/teams",
            json={
                "name": "Stale Policy Team",
                "purpose": "Needs fresh policy review after edits.",
                "orchestrator_agent_id": "agent_pi_operator",
                "member_agent_ids": ["agent_pi_operator", teammate["id"]],
                "orchestrator_policy": {"approval_mode": "toolgate_required"},
            },
        ).json()
        review = client.post(f"/api/teams/{team['id']}/policy-review", json={}).json()
        changed = client.patch(
            f"/api/teams/{team['id']}",
            json={
                "orchestrator_policy": {
                    "approval_mode": "toolgate_required",
                    "turn_order": "reverse_roster",
                }
            },
        ).json()
        decided = client.post(
            f"/api/approvals/{review['toolgate_request']['id']}/decision",
            json={"decision": "approved"},
        ).json()
        fetched = client.get(f"/api/teams/{team['id']}").json()

    assert changed["team_policy_review"]["status"] == "stale"
    assert decided["team_policy_status"] == "stale"
    assert fetched["orchestrator_policy"]["review_status"] == "needs_review"
    assert fetched["orchestration_readiness"]["ready"] is False


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
                "orchestrator_policy": {
                    "approval_mode": "toolgate_required",
                    "review_status": "owner_reviewed",
                },
            },
        ).json()
        team = approve_team_policy(client, team["id"])
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


def test_group_sequence_runs_bounded_rounds_for_each_speaker(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()
    app.state.pi = MultiSpeakerPi()

    with TestClient(app) as client:
        teammate = client.post(
            "/api/agents",
            json={"name": "Group Teammate", "purpose": "Participate in bounded group sequences."},
        ).json()
        team = client.post(
            "/api/teams",
            json={
                "name": "Group Sequence Team",
                "purpose": "Test bounded group sequences.",
                "orchestrator_agent_id": "agent_pi_operator",
                "member_agent_ids": ["agent_pi_operator", teammate["id"]],
                "orchestrator_policy": {
                    "approval_mode": "toolgate_required",
                    "review_status": "owner_reviewed",
                },
            },
        ).json()
        team = approve_team_policy(client, team["id"])
        session = client.post(
            "/api/sessions",
            json={
                "title": "Group Sequence",
                "agent_id": "agent_pi_operator",
                "team_id": team["id"],
                "participant_agent_ids": ["agent_pi_operator", teammate["id"]],
            },
        ).json()
        result = client.post(
            f"/api/sessions/{session['id']}/group-sequence",
            json={"input": "Discuss the release plan safely.", "rounds": 2},
        )
        messages = client.get(f"/api/chats/{session['id']}/messages").json()["messages"]
        activity = client.get(f"/api/activity?team_id={team['id']}").json()["activity"]

    assert result.status_code == 200
    payload = result.json()
    assert payload["sequence"] == {
        "status": "ok",
        "round_count": 2,
        "requested_rounds": 2,
        "max_sequence_rounds": 3,
        "speaker_count": 2,
    }
    assert len(payload["rounds"]) == 2
    assert [round_item["round_index"] for round_item in payload["rounds"]] == [1, 2]
    assert len(app.state.pi.calls) == 4
    assert [message["role"] for message in messages] == ["owner", "agent", "agent", "owner", "agent", "agent"]
    assert messages[0]["content"].startswith("Discuss the release plan safely.")
    assert messages[3]["content"].startswith("Discuss the release plan safely.")
    assert all("round_index" in response for item in payload["rounds"] for response in item["responses"])
    assert "Discuss the release plan safely" not in str(activity)
    assert "group.sequence_completed" in [item["event_type"] for item in activity]


def test_group_sequence_requires_toolgate_approval_boundary(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()
    app.state.pi = MultiSpeakerPi()

    with TestClient(app) as client:
        teammate = client.post(
            "/api/agents",
            json={"name": "Group Teammate", "purpose": "Participate only with ToolGate boundary."},
        ).json()
        team = client.post(
            "/api/teams",
            json={
                "name": "Metadata Boundary Team",
                "purpose": "Should not run under metadata-only approval.",
                "orchestrator_agent_id": "agent_pi_operator",
                "member_agent_ids": ["agent_pi_operator", teammate["id"]],
                "orchestrator_policy": {
                    "approval_mode": "metadata_only",
                    "review_status": "owner_reviewed",
                },
            },
        ).json()
        session = client.post(
            "/api/sessions",
            json={
                "title": "Blocked Group Sequence",
                "agent_id": "agent_pi_operator",
                "team_id": team["id"],
                "participant_agent_ids": ["agent_pi_operator", teammate["id"]],
            },
        ).json()
        result = client.post(
            f"/api/sessions/{session['id']}/group-sequence",
            json={"input": "Discuss the private plan.", "rounds": 2},
        )

    assert result.status_code == 409
    detail = result.json()["detail"]
    assert detail["reason"] == "team_policy_review_required"
    assert detail["approval_mode"] == "metadata_only"
    assert "toolgate_boundary" in detail["missing_fields"]
    assert app.state.pi.calls == []
    assert "private plan" not in str(detail).lower()


def test_group_sequence_obeys_team_turn_policy(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()
    app.state.pi = MultiSpeakerPi()

    with TestClient(app) as client:
        teammate_a = client.post(
            "/api/agents",
            json={"name": "Group Teammate A", "purpose": "Participate in bounded group sequences."},
        ).json()
        teammate_b = client.post(
            "/api/agents",
            json={"name": "Group Teammate B", "purpose": "Participate in bounded group sequences."},
        ).json()
        team = client.post(
            "/api/teams",
            json={
                "name": "Policy Sequence Team",
                "purpose": "Test policy-bounded group sequences.",
                "orchestrator_agent_id": "agent_pi_operator",
                "member_agent_ids": ["agent_pi_operator", teammate_a["id"], teammate_b["id"]],
                "orchestrator_policy": {
                    "approval_mode": "toolgate_required",
                    "review_status": "owner_reviewed",
                    "turn_order": "reverse_roster",
                    "max_sequence_rounds": 1,
                    "max_speakers_per_round": 2,
                },
            },
        ).json()
        team = approve_team_policy(client, team["id"])
        session = client.post(
            "/api/sessions",
            json={
                "title": "Policy Group Sequence",
                "agent_id": "agent_pi_operator",
                "team_id": team["id"],
                "participant_agent_ids": ["agent_pi_operator", teammate_a["id"], teammate_b["id"]],
            },
        ).json()
        result = client.post(
            f"/api/sessions/{session['id']}/group-sequence",
            json={"input": "Discuss the bounded policy safely.", "rounds": 3, "max_speakers": 12},
        )

    assert result.status_code == 200
    payload = result.json()
    assert team["orchestrator_policy"]["turn_order"] == "reverse_roster"
    assert payload["sequence"]["round_count"] == 1
    assert payload["sequence"]["requested_rounds"] == 3
    assert payload["sequence"]["max_sequence_rounds"] == 1
    assert payload["sequence"]["speaker_count"] == 2
    assert payload["rounds"][0]["requested_speaker_count"] == 3
    assert payload["rounds"][0]["max_speakers_per_round"] == 2
    assert payload["rounds"][0]["turn_order"] == "reverse_roster"
    assert [item["agent_id"] for item in payload["rounds"][0]["responses"]] == [teammate_b["id"], teammate_a["id"]]
    assert len(app.state.pi.calls) == 2


def test_group_sequence_stream_emits_sequence_metadata(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()
    app.state.pi = MultiSpeakerPi()

    with TestClient(app) as client:
        teammate = client.post(
            "/api/agents",
            json={"name": "Group Teammate", "purpose": "Participate in live group sequences."},
        ).json()
        team = client.post(
            "/api/teams",
            json={
                "name": "Group Sequence Stream Team",
                "purpose": "Test live group sequence events.",
                "orchestrator_agent_id": "agent_pi_operator",
                "member_agent_ids": ["agent_pi_operator", teammate["id"]],
                "orchestrator_policy": {
                    "approval_mode": "toolgate_required",
                    "review_status": "owner_reviewed",
                },
            },
        ).json()
        team = approve_team_policy(client, team["id"])
        session = client.post(
            "/api/sessions",
            json={
                "title": "Group Sequence Stream",
                "agent_id": "agent_pi_operator",
                "team_id": team["id"],
                "participant_agent_ids": ["agent_pi_operator", teammate["id"]],
            },
        ).json()
        with client.stream(
            "POST",
            f"/api/sessions/{session['id']}/group-sequence/stream",
            json={"input": "Live sequence update.", "rounds": 2},
        ) as response:
            body = "".join(response.iter_text())

    assert response.status_code == 200
    assert "event: group.sequence.started" in body
    assert body.count("event: group.round.started") == 2
    assert body.count("event: group.speaker.started") == 4
    assert body.count("event: group.speaker.completed") == 4
    assert "event: group.sequence.completed" in body
    assert '"round_index": 1' in body
    assert '"round_index": 2' in body
    assert "Live sequence update" not in body


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


def test_local_notification_delivery_records_safe_automation_inbox(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()
    app.state.pi = LocalNotificationPi()
    with TestClient(app) as client:
        created = client.post(
            "/api/jobs",
            json={
                "name": "Local Inbox Proof",
                "schedule": "0 9 * * *",
                "prompt": "private prompt with token=secret-value",
                "delivery_policy": "allowlisted",
                "delivery_targets": ["local dashboard inbox"],
            },
        ).json()
        app.state.gates.decide_approval(created["approval_request_id"], "approved")
        resumed = client.post(f"/api/jobs/{created['id']}/resume").json()
        ran = client.post(f"/api/jobs/{created['id']}/run").json()
        inbox = client.get("/api/notification-deliveries").json()
        deleted = client.delete(f"/api/notification-deliveries/{inbox['deliveries'][0]['id']}").json()
        cleaned = client.get("/api/notification-deliveries").json()

    assert resumed["approval_status"] == "approved"
    assert ran["last_result"]["notification_delivery_count"] == 1
    assert ran["last_result"]["notification_channels"] == ["local dashboard inbox"]
    assert "https://private.invalid" not in json.dumps(ran["last_result"]).lower()
    assert "api_key=abc123" not in json.dumps(ran["last_result"]).lower()
    assert inbox["summary"]["metadata_only"] is True
    assert inbox["summary"]["local_only"] is True
    assert inbox["summary"]["external_delivery"] is False
    assert inbox["deliveries"][0]["channel_kind"] == "local_log"
    assert inbox["deliveries"][0]["status"] == "delivered"
    assert inbox["deliveries"][0]["external_delivery"] is False
    assert inbox["deliveries"][0]["result_output_chars"] > 0
    visible = json.dumps(inbox).lower()
    assert "private prompt" not in visible
    assert "secret-value" not in visible
    assert "https://private.invalid" not in visible
    assert "api_key=abc123" not in visible
    assert deleted["deleted"] is True
    assert deleted["metadata_only"] is True
    assert cleaned["summary"]["total"] == 0


def test_notification_channel_test_send_approval_is_metadata_only_and_redacted(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()
    forbidden = [
        "https://private.invalid/hook",
        "token=secret-value",
        "webhook",
        "+1 555 010 9999",
    ]
    with TestClient(app) as client:
        channels = client.get("/api/notification-channels").json()["channels"]
        channel = next(row for row in channels if row["label"] == "local dashboard inbox")
        queued = client.post(
            f"/api/notification-channels/{channel['id']}/setup-approval",
            json={
                "summary": "Send test to https://private.invalid/hook with token=secret-value webhook +1 555 010 9999",
                "requested_by_agent_id": "agent_pi_operator",
            },
        ).json()
        public_channel = next(
            row for row in client.get("/api/notification-channels").json()["channels"]
            if row["id"] == channel["id"]
        )
        request = app.state.gates.requests[queued["approval_request_id"]]

    visible = json.dumps({"queued": queued, "request": request, "channel": public_channel}).lower()
    assert queued["approval_status"] == "pending"
    assert queued["channel_id"] == channel["id"]
    assert queued["metadata_only"] is True
    assert queued["external_delivery"] is False
    assert queued["test_send"]["metadata_only"] is True
    assert queued["test_send"]["external_delivery"] is False
    assert queued["test_send"]["raw_args_included"] is False
    assert public_channel["setup_approval_request_id"] == queued["approval_request_id"]
    assert public_channel["setup_approval_status"] == "pending"
    assert request["kind"] == "notification_test_send"
    assert request["payload"]["channel_label"] == "local dashboard inbox"
    assert request["payload"]["channel_kind"] == "local_log"
    assert request["payload"]["channel_status"] == "available"
    assert request["payload"]["channel_fingerprint"]
    assert request["payload"]["metadata_only"] is True
    assert request["payload"]["external_delivery"] is False
    assert request["payload"]["raw_args_included"] is False
    assert request["payload"]["summary_digest"]
    assert "summary" not in request["payload"]
    for value in forbidden:
        assert value.lower() not in visible


def test_notification_channel_setup_approval_goes_stale_after_channel_change(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()
    with TestClient(app) as client:
        channel = next(
            row for row in client.get("/api/notification-channels").json()["channels"]
            if row["label"] == "local dashboard inbox"
        )
        queued = client.post(
            f"/api/notification-channels/{channel['id']}/setup-approval",
            json={"summary": "Safe local dashboard readiness check."},
        ).json()
        client.patch(
            f"/api/notification-channels/{channel['id']}",
            json={"status": "needs_setup"},
        )
        decision = client.post(
            f"/api/approvals/{queued['approval_request_id']}/decision",
            json={"decision": "approved"},
        ).json()
        after = client.get("/api/notification-deliveries").json()
        public_channel = next(
            row for row in client.get("/api/notification-channels").json()["channels"]
            if row["id"] == channel["id"]
        )

    assert decision["notification_test_status"] == "stale"
    assert decision["notification_test_stale_reason"]
    assert "notification_delivery" not in decision
    assert after["summary"]["total"] == 0
    assert public_channel["setup_approval_status"] == "stale"


def test_notification_channel_test_send_approval_records_local_log_after_approval(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()
    with TestClient(app) as client:
        channel = next(
            row for row in client.get("/api/notification-channels").json()["channels"]
            if row["label"] == "local dashboard inbox"
        )
        queued = client.post(
            f"/api/notification-channels/{channel['id']}/test-send-approval",
            json={"summary": "Safe local dashboard readiness check."},
        ).json()
        before = client.get("/api/notification-deliveries").json()
        decision = client.post(
            f"/api/approvals/{queued['approval_request_id']}/decision",
            json={"decision": "approved"},
        ).json()
        after = client.get("/api/notification-deliveries").json()

    assert before["summary"]["total"] == 0
    assert decision["notification_test_status"] == "delivered_local_log"
    assert decision["notification_delivery"]["channel_kind"] == "local_log"
    assert decision["notification_delivery"]["status"] == "delivered"
    assert decision["notification_delivery"]["source"] == "channel_test"
    assert decision["notification_delivery"]["external_delivery"] is False
    assert after["summary"]["total"] == 1
    assert after["deliveries"][0]["status"] == "delivered"
    assert after["deliveries"][0]["metadata_only"] is True


def test_notification_channel_test_send_reject_and_desktop_approval_do_not_send_external(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "REGISTRY_DB", tmp_path / "registry.sqlite3")
    reset_state()
    with TestClient(app) as client:
        channels = client.get("/api/notification-channels").json()["channels"]
        local_channel = next(row for row in channels if row["label"] == "local dashboard inbox")
        desktop_channel = next(row for row in channels if row["label"] == "desktop-main")
        rejected = client.post(
            f"/api/notification-channels/{local_channel['id']}/test-send-approval",
            json={"summary": "Reject this readiness check."},
        ).json()
        reject_decision = client.post(
            f"/api/approvals/{rejected['approval_request_id']}/decision",
            json={"decision": "rejected"},
        ).json()
        after_reject = client.get("/api/notification-deliveries").json()
        desktop = client.post(
            f"/api/notification-channels/{desktop_channel['id']}/test-send-approval",
            json={"summary": "Desktop readiness intent only."},
        ).json()
        desktop_decision = client.post(
            f"/api/approvals/{desktop['approval_request_id']}/decision",
            json={"decision": "approved"},
        ).json()
        after_desktop = client.get("/api/notification-deliveries").json()

    assert reject_decision["notification_test_status"] == "rejected"
    assert "notification_delivery" not in reject_decision
    assert after_reject["summary"]["total"] == 0
    assert desktop_decision["notification_test_status"] == "needs_setup"
    assert desktop_decision["notification_delivery"]["channel_kind"] == "desktop"
    assert desktop_decision["notification_delivery"]["status"] == "needs_setup"
    assert desktop_decision["notification_delivery"]["external_delivery"] is False
    assert after_desktop["summary"]["total"] == 1
    assert after_desktop["deliveries"][0]["status"] == "needs_setup"


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
    assert "output" not in facade_job
    assert "output" not in facade_job["last_result"]
    assert "prompt" not in facade_job["last_result"]


def test_public_automation_projection_drops_legacy_raw_output_fields():
    reset_state()

    with TestClient(app) as client:
        app.state.jobs = {
            "job-legacy-output": {
                "id": "job-legacy-output",
                "name": "Legacy Output Probe",
                "schedule": "0 9 * * *",
                "prompt": "private prompt token=hidden",
                "last_result": {
                    "job_id": "job-legacy-output",
                    "status": "ok",
                    "output": "raw automation output token=hidden https://private.invalid",
                    "output_summary": "safe summary",
                    "output_chars": 61,
                    "completed_at": "2026-08-24T09:00:00+00:00",
                },
                "run_history": [
                    {
                        "status": "ok",
                        "output": "raw historical output password=hidden",
                        "output_summary": "historical summary",
                        "output_chars": 37,
                        "completed_at": "2026-08-24T09:00:00+00:00",
                    }
                ],
            }
        }
        job = client.get("/api/jobs").json()["jobs"][0]

    combined = json.dumps(job).lower()
    assert "output" not in job
    assert "output" not in job["last_result"]
    assert "prompt" not in job["last_result"]
    assert "output" not in job["run_history"][0]
    assert "token=hidden" not in combined
    assert "password=hidden" not in combined
    assert "https://private.invalid" not in combined
    assert job["last_result"]["output_summary"] == "safe summary"
    assert job["run_history"][0]["output_summary"] == "historical summary"


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
