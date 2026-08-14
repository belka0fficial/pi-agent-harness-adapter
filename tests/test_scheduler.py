from __future__ import annotations

from fastapi.testclient import TestClient

from scheduler.main import app
from adapter.pi_client import PiEvent


class FakePi:
    async def stream(self, prompt: str, *, session_id: str, options: dict | None = None):
        yield PiEvent("run.started", {"run_id": "job-run", "session_id": session_id})
        yield PiEvent("message.delta", {"delta": f"done: {prompt}"})
        yield PiEvent("message.completed", {"message_id": "job-message"})


def test_scheduler_contract_shape():
    app.state.jobs = {}
    app.state.pi = FakePi()
    with TestClient(app) as client:
        created = client.post("/api/jobs", json={"name": "Brief", "schedule": "0 9 * * *", "prompt": "Summarize logs"}).json()
        job_id = created["id"]

        assert created["job_id"] == job_id
        assert client.get("/api/jobs").json()["jobs"][0]["id"] == job_id
        assert client.post(f"/api/jobs/{job_id}/pause").json()["paused"] is True
        assert client.post(f"/api/jobs/{job_id}/resume").json()["paused"] is False
        ran = client.post(f"/api/jobs/{job_id}/run").json()
        assert ran["last_run_at"] is not None
        assert ran["last_result"]["status"] == "ok"
        assert ran["last_result"]["output"] == "done: Summarize logs"
        assert client.delete(f"/api/jobs/{job_id}").json()["deleted"] is True
