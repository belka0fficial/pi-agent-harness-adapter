# AgentGate-Hermes Compatibility Contract

Frozen from `agentgate/api/agentgate/main.py` on this overnight run. `pi-agent-harness-adapter/adapter` must expose this surface before AgentGate can be pointed at the Pi adapter instead of Hermes.

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
  - Raw compatibility/runtime stop API. The caller already knows the Pi `run_id`.
  - Browser and AgentGate UI code must prefer scoped facades that do not return raw run ids:
    - `POST /api/sessions/{session_id}/runs/current/stop`
    - `POST /api/jobs/{job_id}/stop`
- `POST /v1/runs/{run_id}/approval`
  - Used for Hermes approval decisions.
  - Request is arbitrary JSON, usually `{ "decision": "approved"|"rejected" }`.

## Models And Capabilities

- `GET /api/model/options`
- `GET /v1/capabilities`
- `GET /v1/skills`
- `GET /v1/toolsets`

These are read-only discovery calls. AgentGate tolerates unavailable features by hiding or disabling related UI.

Current adapter behavior:

- `/api/model/options` returns a best-effort `pi --list-models` snapshot and falls back to empty arrays.
- `/v1/skills` and `/v1/toolsets` return valid empty arrays instead of `501`.

## Cron Jobs

- `GET /api/jobs`
  - AgentGate accepts either a list or `{ "jobs": [...] }`.
- `POST /api/jobs`
  - Request fields currently used by UI: `name`, `schedule`, `timezone`, `prompt`, `agent_id`, `team_id`, `deliver`, `required_tool_ids`, `required_memory_scopes`, `approval_policy`, `delivery_policy`, `delivery_targets`, `failure_policy`.
- `PATCH /api/jobs/{job_id}`
- `DELETE /api/jobs/{job_id}`
- `POST /api/jobs/{job_id}/pause`
- `POST /api/jobs/{job_id}/resume`
- `POST /api/jobs/{job_id}/run`
- `GET /api/notification-channels`
  - Returns metadata-only delivery labels for automation planning. Responses must not include URLs, phone numbers, webhook targets, credentials, or provider secrets.
- `POST /api/notification-channels`
  - Creates a metadata-only label with `label`, `kind`, `status`, `description`, and `requires_owner_confirmation`. Real external delivery remains out of scope until ToolGate owns a sender integration.

Job fields rendered by AgentGate:

- `id` or `job_id`
- `name`
- `schedule`
- `prompt`
- `deliver` or `delivery`
- `paused`
- `next_run_at`
- `last_run_at`
- `approval_policy`, `approval_status`, `approval_reasons`, `approval_request_id`
- `required_tool_ids`, `required_memory_scopes`
- `delivery_policy`, `delivery_targets`, `delivery_target_count`
- `failure_policy`, `failure_policy_status`

Delivery target labels are public metadata. The adapter rejects URLs, emails, phone-number-like strings, webhook labels, and secret-like words before saving them.

## Pi Adapter Notes

Pi supports non-interactive `--mode json` and `--mode rpc`, and project trust for non-interactive modes is controlled with `--approve` / `--no-approve` or global `defaultProjectTrust`. Pi's MCP support is extension-based through `pi-mcp-extension`, which reads `~/.pi/agent/mcp.json` or project `.pi/mcp.json`.

The adapter now uses long-lived Pi RPC sessions per active chat session, keyed by `--session-id`.

Project `.pi/mcp.json` uses a stdio bridge into the running `toolgate-api` container via `docker exec -i toolgate-api python3 /app/toolgate/mcp/toolgate_mcp.py` so approvals and request consumption operate on the live writable ToolGate state.

The session-start SOUL block is feature-flagged through `PI_SOUL_TEXT` or `PI_SOUL_FILE` and is injected with `--append-system-prompt` when the Pi RPC session is created.
