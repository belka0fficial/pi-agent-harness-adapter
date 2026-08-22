from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
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
        read_key = os.environ.get("MEMORYGATE_READ_KEY", "")
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

    def memory_context(self, query: str, *, agent_id: str | None = None) -> dict[str, Any]:
        payload = self._memory_read_request(
            "/runtime/context",
            method="POST",
            payload={"query": query, "max_items": 10, "include_evidence": False, "agent_id": agent_id},
            agent_id=agent_id,
        )
        return payload if isinstance(payload, dict) else {}

    def record_transcript(self, session_id: str, messages: list[dict[str, Any]], *, agent_id: str | None = None):
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
        backup_rows = backups.get("backups", backups.get("results", []))
        return {
            "vitals": vitals,
            "containers": containers.get("containers", containers if isinstance(containers, list) else []),
            "errors": error_rows,
            "packages": packages.get("packages", packages if isinstance(packages, list) else []),
            "backups": {"latest": backup_rows[0] if backup_rows else None},
            "sources": sources,
        }
