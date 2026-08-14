from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from adapter.pi_client import PiClient


class JobInput(BaseModel):
    name: str = Field(min_length=1)
    schedule: str = Field(min_length=1)
    prompt: str = Field(min_length=1)
    deliver: str = "local"
    webhook_url: str | None = None


app = FastAPI(title="Pi Agent Harness Scheduler", version="0.1.0")
app.state.scheduler = AsyncIOScheduler()
app.state.jobs = {}
app.state.pi = PiClient()


@app.on_event("startup")
def start_scheduler():
    app.state.scheduler.start(paused=False)


@app.on_event("shutdown")
def stop_scheduler():
    app.state.scheduler.shutdown(wait=False)


def now() -> str:
    return datetime.now(UTC).isoformat()


async def run_job(job_id: str):
    item = app.state.jobs.get(job_id)
    if not item:
        return
    item["last_run_at"] = now()
    chunks = []
    status = "ok"
    error = None
    async for event in app.state.pi.stream(item["prompt"], session_id=f"job:{job_id}", options={"headless": True, "deliver": item.get("deliver")}):
        if event.event == "message.delta":
            chunks.append(str(event.data.get("delta") or event.data.get("text") or event.data.get("content") or ""))
        elif event.event == "run.failed":
            status = "failed"
            error = event.data.get("message") or "Pi run failed"
    result = {"job_id": job_id, "status": status, "prompt": item["prompt"], "output": "".join(chunks), "error": error}
    item["last_result"] = result
    if item.get("webhook_url"):
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(item["webhook_url"], json=result)


def _sync_scheduler(job_id: str):
    item = app.state.jobs[job_id]
    if app.state.scheduler.get_job(job_id):
        app.state.scheduler.remove_job(job_id)
    if item.get("paused"):
        return
    app.state.scheduler.add_job(run_job, "cron", id=job_id, args=[job_id], replace_existing=True, **_cron_kwargs(item["schedule"]))


def _cron_kwargs(schedule: str) -> dict[str, Any]:
    parts = schedule.split()
    if len(parts) != 5:
        raise HTTPException(422, "schedule must be five-field cron syntax")
    minute, hour, day, month, day_of_week = parts
    return {"minute": minute, "hour": hour, "day": day, "month": month, "day_of_week": day_of_week}


@app.get("/api/jobs")
def list_jobs():
    return {"jobs": list(app.state.jobs.values())}


@app.post("/api/jobs")
def create_job(payload: JobInput):
    job_id = f"job_{uuid.uuid4().hex[:12]}"
    item = {"id": job_id, "job_id": job_id, **payload.model_dump(), "paused": False, "created_at": now(), "updated_at": now(), "last_run_at": None, "next_run_at": None}
    app.state.jobs[job_id] = item
    _sync_scheduler(job_id)
    scheduled = app.state.scheduler.get_job(job_id)
    item["next_run_at"] = scheduled.next_run_time.isoformat() if scheduled and scheduled.next_run_time else None
    return item


@app.patch("/api/jobs/{job_id}")
def update_job(job_id: str, payload: dict[str, Any]):
    if job_id not in app.state.jobs:
        raise HTTPException(404, "job not found")
    app.state.jobs[job_id].update({key: value for key, value in payload.items() if key in {"name", "schedule", "prompt", "deliver", "webhook_url"}})
    app.state.jobs[job_id]["updated_at"] = now()
    _sync_scheduler(job_id)
    return app.state.jobs[job_id]


@app.delete("/api/jobs/{job_id}")
def delete_job(job_id: str):
    if job_id not in app.state.jobs:
        raise HTTPException(404, "job not found")
    if app.state.scheduler.get_job(job_id):
        app.state.scheduler.remove_job(job_id)
    app.state.jobs.pop(job_id)
    return {"deleted": True}


@app.post("/api/jobs/{job_id}/pause")
def pause_job(job_id: str):
    if job_id not in app.state.jobs:
        raise HTTPException(404, "job not found")
    app.state.jobs[job_id]["paused"] = True
    _sync_scheduler(job_id)
    return app.state.jobs[job_id]


@app.post("/api/jobs/{job_id}/resume")
def resume_job(job_id: str):
    if job_id not in app.state.jobs:
        raise HTTPException(404, "job not found")
    app.state.jobs[job_id]["paused"] = False
    _sync_scheduler(job_id)
    return app.state.jobs[job_id]


@app.post("/api/jobs/{job_id}/run")
async def run_now(job_id: str):
    if job_id not in app.state.jobs:
        raise HTTPException(404, "job not found")
    await run_job(job_id)
    return app.state.jobs[job_id]
