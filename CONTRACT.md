# AgentGate-Hermes Compatibility Contract

Frozen from `agentgate/api/agentgate/main.py` on this overnight run. `brain/adapter` must expose this surface before AgentGate can be pointed at Brain instead of Hermes.

## Health

- `GET /health`
  - AgentGate dependency check expects any 2xx JSON response.
- `GET /health/detailed`
  - AgentGate home consumes a small JSON object; unknown keys are tolerated.

## Sessions

- `GET /api/sessions?limit=<int>&offset=<int>&include_children=true`
  - AgentGate accepts either a list or `{ "sessions": [...] }` / `{ "items": [...] }`.
- `POST /api/sessions`
  - Request: arbitrary JSON; `title` is used when present.
  - Response must contain `id` or `session_id`.
- `GET /api/sessions/{session_id}`
  - Response: session object.
- `PATCH /api/sessions/{session_id}`
  - Request: arbitrary JSON, usually `{ "title": "..." }`.
  - Response: updated session object.
- `DELETE /api/sessions/{session_id}`
  - Response: deletion acknowledgement.
- `GET /api/sessions/{session_id}/messages`
  - AgentGate accepts either a list or `{ "messages": [...] }`.
  - Message shape consumed by UI: `{ "id"?, "role": "user"|"assistant"|string, "content" | "text" | "message" }`.
- `POST /api/sessions/{session_id}/fork`
  - Request: arbitrary JSON; `title` optional.
  - Response must contain `id` or `session_id`.

## Chat Stream

- `POST /api/sessions/{session_id}/chat/stream`
  - Request from AgentGate:
    - `input`: string, required.
    - `provider`: optional string.
    - `model`: optional string.
    - `model_options.reasoning_effort`: optional string mapped from AgentGate intensity.
    - `instructions`: optional string used for memory-incognito turns.
  - Response: `text/event-stream`.
  - AgentGate forwards upstream SSE line-for-line and parses:
    - Any `event:` line containing `tool` or `subagent` is shown in live activity.
    - Any `event:` line containing `approval` is captured into AgentGate's local approvals table.
    - `data:` JSON may include `run_id`, `delta`, `text`, `content`, `name`, `tool_name`, `summary`, `approval_id`, `id`, `request_id`, `expires_at`.
  - Required adapter event shapes:
    - `event: run.started`, `data: {"run_id":"..."}`
    - `event: message.delta`, `data: {"delta":"..."}`
    - `event: message.completed`, `data: {"message_id":"..."}`
    - `event: run.failed`, `data: {"message":"..."}`
  - Optional pass-through shapes:
    - `tool.started`, `tool.completed`, `subagent.started`, `subagent.completed`, `approval.required`.

## Runs

- `POST /v1/runs/{run_id}/stop`
  - Used by Chat stop button.
- `POST /v1/runs/{run_id}/approval`
  - Used for Hermes approval decisions.
  - Request is arbitrary JSON, usually `{ "decision": "approved"|"rejected" }`.

## Models And Capabilities

- `GET /api/model/options`
- `GET /v1/capabilities`
- `GET /v1/skills`
- `GET /v1/toolsets`

These are read-only discovery calls. AgentGate tolerates unavailable features by hiding or disabling related UI.

## Cron Jobs

- `GET /api/jobs`
  - AgentGate accepts either a list or `{ "jobs": [...] }`.
- `POST /api/jobs`
  - Request fields currently used by UI: `name`, `schedule`, `prompt`, `deliver`.
- `PATCH /api/jobs/{job_id}`
- `DELETE /api/jobs/{job_id}`
- `POST /api/jobs/{job_id}/pause`
- `POST /api/jobs/{job_id}/resume`
- `POST /api/jobs/{job_id}/run`

Job fields rendered by AgentGate:

- `id` or `job_id`
- `name`
- `schedule`
- `prompt`
- `deliver` or `delivery`
- `paused`
- `next_run_at`
- `last_run_at`

## Pi Adapter Notes

Pi supports non-interactive `--mode json` and `--mode rpc`, and project trust for non-interactive modes is controlled with `--approve` / `--no-approve` or global `defaultProjectTrust`. Pi's MCP support is extension-based through `pi-mcp-extension`, which reads `~/.pi/agent/mcp.json` or project `.pi/mcp.json`.

For tonight, Brain includes a project `.pi/mcp.json` registering the ToolGate stdio bridge. If Pi's native RPC event schema differs from the assumed JSON-line events in `adapter/pi_client.py`, the next pass should replace that small parser only; the AgentGate contract should stay unchanged.

