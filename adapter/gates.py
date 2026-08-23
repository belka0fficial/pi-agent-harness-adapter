from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any


class GateClients:
    """Server-side adapters for the three gates used by AgentGate's UI facade."""

    def __init__(self) -> None:
        self.services = {
            "toolgate": (
                os.environ.get("TOOLGATE_URL", "http://127.0.0.1:8010").rstrip("/"),
                "X-ToolGate-Key",
                os.environ.get("TOOLGATE_ADMIN_KEY", ""),
            ),
            "memorygate": (
                os.environ.get("MEMORYGATE_URL", "http://127.0.0.1:8020").rstrip("/"),
                "X-MemoryGate-Key",
                os.environ.get("MEMORYGATE_ADMIN_KEY", ""),
            ),
            "systemgate": (
                os.environ.get("SYSTEMGATE_URL", "http://127.0.0.1:8040").rstrip("/"),
                "X-SystemGate-Key",
                os.environ.get("SYSTEMGATE_ADMIN_KEY", ""),
            ),
        }
        self.memory_counts = {"reads": 0, "writes": 0}
        self.toolgate_execution_key = os.environ.get("TOOLGATE_EXECUTION_KEY", "")
        self.agent_memory_key_path = Path(os.environ.get("ADAPTER_DATA_DIR", "/app/data")) / "agent_memory_read_keys.json"

    @staticmethod
    def _parse_time(value: Any) -> datetime | None:
        if not value:
            return None
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed

    @staticmethod
    def _safe_label(value: Any) -> str:
        text = str(value or "").replace("_", " ").replace("-", " ").strip()
        text = " ".join(text.split())
        text = text.replace("key", "credential").replace("Key", "Credential")
        return text[:140]

    @staticmethod
    def _safe_service_listener(value: Any) -> str | None:
        text = str(value or "").strip()
        if ":" not in text:
            return None
        scope, port_text = text.rsplit(":", 1)
        allowed_scopes = {
            "loopback",
            "tailscale",
            "private-lan",
            "container-internal",
            "all-interfaces",
            "public-or-host",
        }
        if scope not in allowed_scopes:
            return None
        if not port_text.isdigit():
            return None
        port = int(port_text)
        if port < 1 or port > 65535:
            return None
        return f"{scope}:{port}"

    @classmethod
    def _safe_service_row(cls, row: dict[str, Any], index: int) -> dict[str, Any]:
        listeners = []
        if isinstance(row.get("listeners"), list):
            for item in row["listeners"][:12]:
                listener = cls._safe_service_listener(item)
                if listener and listener not in listeners:
                    listeners.append(listener)
        status = str(row.get("status") or "unknown").strip().lower()
        if status not in {"listening", "running", "healthy", "ok", "unknown", "unavailable"}:
            status = "unknown"
        return {
            "id": f"service-{index:03d}",
            "name": cls._safe_label(row.get("name") or "service")[:80] or "service",
            "image": cls._safe_label(row.get("image") or row.get("kind") or "service-listener")[:80],
            "status": status,
            "created": None,
            "listeners": listeners,
            "source": "systemgate-services",
        }

    @classmethod
    def _redacted_event(cls, event: dict[str, Any]) -> dict[str, Any]:
        event_type = str(event.get("event_type") or "event")
        severity = str(event.get("severity") or "info").lower()
        risk = {
            "critical": "high",
            "error": "high",
            "warning": "medium",
            "warn": "medium",
            "info": "low",
        }.get(severity, "low")
        subject_type = cls._safe_label(event.get("subject_type") or "event")
        subject_id = cls._safe_label(event.get("subject_id") or "")
        summary = cls._safe_label(f"{event_type} {subject_type} {subject_id}")
        return {
            "time": str(event.get("created_at") or ""),
            "risk": risk,
            "source": "ToolGate",
            "action_summary": summary,
        }

    def _request(self, service: str, path: str, *, method: str = "GET", payload: dict[str, Any] | None = None, timeout: float = 8):
        base_url, header_name, key = self.services[service]
        headers = {"Accept": "application/json"}
        if key:
            headers[header_name] = key
        data = None
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(f"{base_url}{path}", data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                body = response.read()
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")[:500]
            raise RuntimeError(f"{service} returned HTTP {exc.code}: {detail}") from exc
        except (TimeoutError, urllib.error.URLError) as exc:
            raise RuntimeError(f"{service} is unavailable") from exc
        return json.loads(body.decode("utf-8")) if body else {}

    def _toolgate_execution_request(self, path: str, *, timeout: float = 8):
        base_url = self.services["toolgate"][0]
        if not self.toolgate_execution_key:
            raise RuntimeError("toolgate execution key is unavailable")
        request = urllib.request.Request(
            f"{base_url}{path}",
            headers={"Accept": "application/json", "X-ToolGate-Execution-Key": self.toolgate_execution_key},
            method="GET",
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                body = response.read()
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")[:500]
            raise RuntimeError(f"toolgate returned HTTP {exc.code}: {detail}") from exc
        except (TimeoutError, urllib.error.URLError) as exc:
            raise RuntimeError("toolgate is unavailable") from exc
        return json.loads(body.decode("utf-8")) if body else {}

    def _memory_read_request(
        self,
        path: str,
        *,
        agent_id: str | None,
        method: str = "GET",
        payload: dict[str, Any] | None = None,
        timeout: float = 8,
    ):
        base_url = self.services["memorygate"][0]
        read_key = self.memorygate_agent_read_key(agent_id) if agent_id else ""
        if not read_key and agent_id == "agent_pi_operator":
            read_key = os.environ.get("MEMORYGATE_READ_KEY", "")
        if agent_id and not read_key:
            raise RuntimeError("memorygate read key is unavailable for agent")
        if not read_key:
            return self._request("memorygate", path, method=method, payload=payload, timeout=timeout)
        headers = {"Accept": "application/json", "X-MemoryGate-Key": read_key}
        if agent_id:
            headers["X-Agent-Id"] = agent_id
        data = None
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(f"{base_url}{path}", data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                body = response.read()
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")[:500]
            raise RuntimeError(f"memorygate returned HTTP {exc.code}: {detail}") from exc
        except (TimeoutError, urllib.error.URLError) as exc:
            raise RuntimeError("memorygate is unavailable") from exc
        return json.loads(body.decode("utf-8")) if body else {}

    def _load_agent_memory_keys(self) -> dict[str, dict[str, str]]:
        try:
            payload = json.loads(self.agent_memory_key_path.read_text())
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            return {}
        return payload if isinstance(payload, dict) else {}

    def _save_agent_memory_keys(self, payload: dict[str, dict[str, str]]) -> None:
        self.agent_memory_key_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.agent_memory_key_path.with_suffix(".tmp")
        tmp_path.write_text(json.dumps(payload, sort_keys=True))
        os.replace(tmp_path, self.agent_memory_key_path)
        try:
            self.agent_memory_key_path.chmod(0o600)
        except OSError:
            pass

    def memorygate_agent_read_key(self, agent_id: str | None) -> str:
        if not agent_id:
            return ""
        item = self._load_agent_memory_keys().get(agent_id)
        return str((item or {}).get("key") or "")

    def has_memorygate_agent_read_key(self, agent_id: str) -> bool:
        return bool(self.memorygate_agent_read_key(agent_id))

    def forget_memorygate_agent_read_key(self, agent_id: str) -> None:
        store = self._load_agent_memory_keys()
        if agent_id not in store:
            return
        store.pop(agent_id, None)
        self._save_agent_memory_keys(store)

    def ensure_memorygate_agent_read_key(self, agent_id: str) -> dict[str, Any]:
        agent_id = str(agent_id or "").strip()
        if not agent_id:
            raise RuntimeError("agent id is required")
        cached = self.memorygate_agent_read_key(agent_id)
        if cached:
            return {"status": "cached", "agent_id": agent_id}
        label = f"AgentGate:{agent_id}"
        keys = self.memorygate_agent_keys()
        if any(
            str(row.get("agent_id") or "") == agent_id
            and str(row.get("label") or "").strip().lower() == label.lower()
            and not bool(row.get("revoked"))
            for row in keys
        ):
            raise RuntimeError("memorygate native key exists but adapter read credential is unavailable")
        created = self._request(
            "memorygate",
            "/auth/agent-keys",
            method="POST",
            payload={"label": label, "agent_id": agent_id},
        )
        raw_key = str((created or {}).get("key") or "")
        if not raw_key.startswith("mg_read_"):
            raise RuntimeError("memorygate did not return a read key")
        store = self._load_agent_memory_keys()
        store[agent_id] = {
            "key": raw_key,
            "label": label,
            "memorygate_key_id": str(created.get("id") or ""),
            "created_at": datetime.now(UTC).isoformat(),
        }
        self._save_agent_memory_keys(store)
        return {
            "status": "created",
            "agent_id": agent_id,
            "memorygate_key_id": str(created.get("id") or ""),
        }

    def health(self) -> dict[str, dict[str, str]]:
        result: dict[str, dict[str, str]] = {}
        for service in self.services:
            try:
                payload = self._request(service, "/health")
                result[service] = {"status": str(payload.get("status") or "ok")}
            except RuntimeError:
                result[service] = {"status": "unavailable"}
        return result

    @staticmethod
    def _approval(record: dict[str, Any]) -> dict[str, Any]:
        payload = record.get("payload") if isinstance(record.get("payload"), dict) else {}
        status = str(record.get("status") or "pending")
        severity = str(record.get("severity") or "low")
        severity = {"warning": "medium", "info": "low", "critical": "high"}.get(severity, severity)
        if severity not in {"high", "medium", "low"}:
            severity = "low"
        decision = record.get("decision") if isinstance(record.get("decision"), dict) else {}
        item = {
            "id": str(record.get("id") or ""),
            "source": "ToolGate",
            "severity": severity,
            "title": str(record.get("title") or record.get("kind") or "ToolGate request"),
            "details": str(record.get("details") or ""),
            "binding": {
                "type": str(payload.get("subject_type") or record.get("kind") or "request"),
                "id": str(payload.get("subject_id") or record.get("id") or ""),
                "version": str(payload.get("subject_version") or ""),
                "digest": str(payload.get("argument_digest") or payload.get("digest") or ""),
            },
            "created_at": str(record.get("created_at") or record.get("updated_at") or ""),
        }
        if status in {"approved", "rejected", "dismissed"}:
            item.update({
                "decision": "rejected" if status == "dismissed" else status,
                "decided_at": str(decision.get("at") or record.get("updated_at") or ""),
                "decided_by": str(decision.get("actor") or "Owner"),
            })
        return item

    def approvals(self, *, history: bool = False) -> list[dict[str, Any]]:
        payload = self._request("toolgate", "/v2/requests")
        rows = payload if isinstance(payload, list) else payload.get("results", payload.get("requests", []))
        wanted = {"approved", "rejected", "dismissed"} if history else {"pending"}
        return [self._approval(row) for row in rows if str(row.get("status") or "pending") in wanted]

    def decide_approval(self, request_id: str, decision: str):
        return self._request(
            "toolgate",
            f"/v2/requests/{request_id}/decision",
            method="POST",
            payload={"status": decision, "note": "Decided in AgentGate"},
        )

    def create_admin_request(
        self,
        *,
        kind: str,
        title: str,
        details: str,
        payload: dict[str, Any],
        severity: str = "warning",
    ) -> dict[str, Any]:
        result = self._request(
            "toolgate",
            "/v2/admin/requests",
            method="POST",
            payload={
                "kind": kind,
                "title": title,
                "details": details,
                "payload": payload,
                "severity": severity,
            },
        )
        return result if isinstance(result, dict) else {}

    def request_status(self, request_id: str) -> dict[str, Any] | None:
        payload = self._request("toolgate", "/v2/requests")
        rows = payload if isinstance(payload, list) else payload.get("results", payload.get("requests", []))
        for row in rows:
            if row.get("id") == request_id:
                return row
        return None

    def toolgate_execution_status(self) -> dict[str, Any]:
        payload = self._toolgate_execution_request("/v2/agent/status")
        return payload if isinstance(payload, dict) else {}

    def toolgate_agent_keys(self) -> list[dict[str, Any]]:
        payload = self._request("toolgate", "/v2/agent-keys")
        rows = payload if isinstance(payload, list) else payload.get("results", payload.get("keys", []))
        return [row for row in rows if isinstance(row, dict)]

    def update_toolgate_execution_scopes(self, scopes: list[str]) -> dict[str, Any]:
        status = self.toolgate_execution_status()
        key_id = str(status.get("id") or "")
        if not key_id:
            raise RuntimeError("toolgate execution key id is unavailable")
        payload = self._request(
            "toolgate",
            f"/v2/agent-keys/{key_id}/scopes",
            method="PATCH",
            payload={"scopes": scopes},
        )
        return payload if isinstance(payload, dict) else {}

    def memorygate_agent_keys(self) -> list[dict[str, Any]]:
        payload = self._request("memorygate", "/auth/agent-keys")
        rows = payload if isinstance(payload, list) else payload.get("results", payload.get("keys", []))
        return [row for row in rows if isinstance(row, dict)]

    def memorygate_audit_metrics(self, *, hours: int = 24) -> dict[str, Any]:
        payload = self._request("memorygate", f"/audit/metrics?hours={max(1, min(int(hours or 24), 168))}")
        if not isinstance(payload, dict):
            return {"reads": self.memory_counts["reads"], "writes": self.memory_counts["writes"], "source": "adapter-observed"}
        return {
            "reads": int(payload.get("reads") or 0),
            "writes": int(payload.get("writes") or 0),
            "source": "memorygate-audit",
            "window_hours": int(payload.get("window_hours") or hours),
        }

    def memory_context(self, query: str, *, agent_id: str | None = None) -> dict[str, Any]:
        self.memory_counts["reads"] += 1
        payload = self._memory_read_request(
            "/runtime/context",
            method="POST",
            payload={"query": query, "max_items": 10, "include_evidence": False, "agent_id": agent_id},
            agent_id=agent_id,
        )
        return payload if isinstance(payload, dict) else {}

    def record_transcript(self, session_id: str, messages: list[dict[str, Any]], *, agent_id: str | None = None):
        self.memory_counts["writes"] += 1
        transcript = "\n\n".join(
            f"{str(message.get('role') or 'unknown').upper()}: {str(message.get('content') or '')}"
            for message in messages
        )
        return self._request(
            "memorygate",
            "/transcripts",
            method="POST",
            payload={"session_id": session_id, "transcript": transcript, "agent_id": agent_id},
        )

    def write_memory_candidate(self, candidate: dict[str, Any]):
        self.memory_counts["writes"] += 1
        payload = {
            "text": candidate["text"],
            "source_type": candidate.get("source_type", "agentgate_owner_approved"),
            "memory_type": candidate.get("memory_type"),
            "confidence": candidate.get("confidence"),
            "do_not_generalize": candidate.get("do_not_generalize", True),
            "tags": candidate.get("tags", []),
            "evidence": candidate.get("evidence"),
        }
        return self._request("memorygate", "/memory/write", method="POST", payload=payload)

    def memory_records(self) -> list[dict[str, Any]]:
        self.memory_counts["reads"] += 1
        payload = self._request("memorygate", "/memory")
        rows = payload if isinstance(payload, list) else payload.get("results", payload.get("memories", []))
        return [{
            "id": str(row.get("id") or ""),
            "title": str(row.get("text") or row.get("summary") or ""),
            "kind": str(row.get("memory_type") or row.get("type") or "context"),
            "confidence": str(row.get("confidence") or "medium"),
            "updated_at": str(row.get("updated_at") or row.get("created_at") or ""),
        } for row in rows]

    def tools(self) -> list[dict[str, Any]]:
        payload = self._request("toolgate", "/v2/tools")
        return payload if isinstance(payload, list) else payload.get("tools", payload.get("results", []))

    def update_tool_policy(
        self,
        tool_id: str,
        *,
        authorization: str,
        usage_limits: dict[str, int],
    ) -> dict[str, Any]:
        current = self._request("toolgate", f"/v2/tools/{tool_id}")
        if not isinstance(current, dict) or not current.get("id"):
            raise RuntimeError("toolgate tool was not found")
        policy = {**(current.get("policy") or {})}
        policy["usage_limits"] = usage_limits
        payload = {
            "id": current["id"],
            "name": current.get("name"),
            "description": current.get("description") or "",
            "service_id": current.get("service_id"),
            "category": current.get("category") or "controlled",
            "inputs": current.get("inputs") or [],
            "outputs": current.get("outputs") or [],
            "execution": current.get("execution") or {},
            "policy": policy,
            "authorization": authorization,
            "version": int(current.get("version") or 1),
            "status": current.get("status") or "active",
        }
        result = self._request("toolgate", f"/v2/tools/{tool_id}", method="PUT", payload=payload)
        return result if isinstance(result, dict) else {}

    def skills(self) -> list[dict[str, Any]]:
        payload = self._request("memorygate", "/skills")
        return payload if isinstance(payload, list) else payload.get("skills", payload.get("results", []))

    def system_overview(self) -> dict[str, Any]:
        sources: dict[str, dict[str, str]] = {}

        def read_system(name: str, path: str, default: dict[str, Any]) -> dict[str, Any]:
            try:
                payload = self._request("systemgate", path, timeout=1.5)
                sources[name] = {"status": "ok"}
                return payload if isinstance(payload, dict) else default
            except RuntimeError as exc:
                sources[name] = {"status": "unavailable", "message": str(exc)[:200]}
                return default

        vitals = read_system("vitals", "/vitals", {})
        services = read_system("services", "/services", {"results": []})
        containers = {"results": []}
        if not services.get("results"):
            containers = read_system("containers", "/containers", {"containers": []})
        errors = read_system("errors", "/logs/errors", {"errors": []})
        packages = read_system("packages", "/packages", {"packages": []})
        backups = read_system("backups", "/backups", {"backups": []})
        error_rows = errors.get("errors", [])
        if not error_rows and errors.get("text"):
            error_rows = [{"at": "", "service": "system", "level": "error", "message": line} for line in str(errors["text"]).splitlines() if line.strip()]
        for source_name, source in sources.items():
            if source.get("status") == "unavailable":
                error_rows.append({"at": "", "service": f"systemgate/{source_name}", "level": "warning", "message": source.get("message", "unavailable")})
        container_rows = containers.get("containers", containers.get("results", containers if isinstance(containers, list) else []))
        service_rows = services.get("services", services.get("results", services if isinstance(services, list) else []))
        backup_rows = backups.get("backups", backups.get("results", backups if isinstance(backups, list) else []))
        safe_backups = []
        for row in backup_rows:
            if not isinstance(row, dict):
                continue
            safe_backups.append({
                "name": str(row.get("name") or ""),
                "created_at": row.get("created_at"),
            })
        package_rows = packages.get("packages")
        if package_rows is None:
            package_rows = []
            for name in ("apt", "pip", "npm"):
                payload = packages.get(name) if isinstance(packages.get(name), dict) else {}
                output = str(payload.get("output") or "")
                state = "current"
                if name == "apt" and "\n" in output.strip():
                    state = "updates_available"
                elif name in {"pip", "npm"} and output.strip() not in {"", "{}", "[]"}:
                    state = "updates_available"
                package_rows.append({
                    "name": name,
                    "current": "installed",
                    "latest": "approved channel",
                    "state": state,
                    "ok": bool(payload.get("ok", True)),
                })
        return {
            "vitals": vitals,
            "containers": [
                self._safe_service_row(row, index)
                for index, row in enumerate(
                    [item for item in service_rows if isinstance(item, dict)],
                    start=1,
                )
            ] or [{
                "id": str(row.get("id") or ""),
                "name": str(row.get("name") or ""),
                "image": str(row.get("image") or ""),
                "status": str(row.get("status") or "unknown"),
                "created": row.get("created"),
                "listeners": [],
                "source": "systemgate-containers",
            } for row in container_rows if isinstance(row, dict)],
            "errors": [{
                "at": str(row.get("at") or ""),
                "service": str(row.get("service") or "system"),
                "level": str(row.get("level") or "warning"),
                "message": str(row.get("message") or "")[:240],
            } for row in error_rows[:12] if isinstance(row, dict)],
            "packages": package_rows,
            "backups": {
                "latest": safe_backups[0] if safe_backups else None,
                "count": len(safe_backups),
                "results": safe_backups[:10],
            },
            "sources": sources,
        }

    def operations_summary(self, *, pending: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        cutoff = datetime.now(UTC) - timedelta(hours=24)
        try:
            requests_payload = self._request("toolgate", "/v2/requests")
            requests = requests_payload if isinstance(requests_payload, list) else requests_payload.get("results", requests_payload.get("requests", []))
        except RuntimeError:
            requests = []
        try:
            events_payload = self._request("toolgate", "/v2/events?limit=100")
            events = events_payload if isinstance(events_payload, list) else events_payload.get("results", events_payload.get("events", []))
        except RuntimeError:
            events = []

        recent_requests = []
        for record in requests:
            decision = record.get("decision") if isinstance(record.get("decision"), dict) else {}
            observed_at = self._parse_time(decision.get("at") or record.get("updated_at") or record.get("created_at"))
            if observed_at and observed_at >= cutoff:
                recent_requests.append(record)

        action_counts = {"approved": 0, "rejected": 0, "expired": 0, "failed": 0}
        now_dt = datetime.now(UTC)
        for record in recent_requests:
            status = str(record.get("status") or "pending")
            if status == "approved":
                action_counts["approved"] += 1
            elif status in {"rejected", "dismissed"}:
                action_counts["rejected"] += 1
            payload = record.get("payload") if isinstance(record.get("payload"), dict) else {}
            binding = payload.get("binding") if isinstance(payload.get("binding"), dict) else payload
            expires_at = self._parse_time(binding.get("expires_at"))
            if status == "pending" and expires_at and expires_at < now_dt:
                action_counts["expired"] += 1

        recent_events = []
        tool_success = 0
        tool_failure = 0
        for event in events:
            created = self._parse_time(event.get("created_at"))
            if not created or created < cutoff:
                continue
            event_type = str(event.get("event_type") or "")
            subject_type = str(event.get("subject_type") or "")
            severity = str(event.get("severity") or "").lower()
            if subject_type == "tool" and event_type == "tool_executed":
                payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
                if payload.get("ok") is False:
                    tool_failure += 1
                else:
                    tool_success += 1
            elif subject_type == "tool" and (severity in {"warning", "critical", "error"} or "failed" in event_type or "blocked" in event_type):
                tool_failure += 1
            if severity in {"warning", "critical", "error"} or "failed" in event_type or "blocked" in event_type:
                action_counts["failed"] += 1
            if event_type.startswith("verification") or event_type.startswith("request_") or subject_type in {"request", "verification_method"}:
                recent_events.append(self._redacted_event(event))

        try:
            memory_counts = self.memorygate_audit_metrics(hours=24)
        except RuntimeError:
            memory_counts = {**dict(self.memory_counts), "source": "adapter-observed"}

        return {
            "service_health": {},
            "pending_approvals": len(pending or []),
            "action_counts_24h": action_counts,
            "recent_verification_events": recent_events[:8],
            "toolgate_counts": {"success": tool_success, "failure": tool_failure},
            "memorygate_counts": memory_counts,
        }
