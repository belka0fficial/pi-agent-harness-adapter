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
        assert ran["last_result"]["output_summary"] == "done: Summarize logs"
        assert ran["last_result"]["output_chars"] == len("done: Summarize logs")
        assert "prompt" not in ran["last_result"]
        assert "output" not in ran["last_result"]
        assert client.delete(f"/api/jobs/{job_id}").json()["deleted"] is True


def test_scheduler_blocks_webhooks_and_fast_cron_by_default():
    app.state.jobs = {}
    app.state.pi = FakePi()
    with TestClient(app) as client:
        webhook = client.post(
            "/api/jobs",
            json={
                "name": "Webhook",
                "schedule": "0 9 * * *",
                "prompt": "Send somewhere",
                "webhook_url": "https://example.test/hook",
            },
        )
        fast = client.post(
            "/api/jobs",
            json={"name": "Fast", "schedule": "*/1 * * * *", "prompt": "Too often"},
        )

    assert webhook.status_code == 422
    assert "webhooks are disabled" in webhook.text
    assert fast.status_code == 422
    assert "more often than every 5 minutes" in fast.text
