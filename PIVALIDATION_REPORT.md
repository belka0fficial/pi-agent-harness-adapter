# Pi Validation Report

Validated on August 14, 2026 against real Pi CLI `0.84.2` with the `openai-codex` provider login already authorized on this host.

## Assumptions Vs Reality

| Area | Adapter Assumption | Observed Reality | Outcome |
| --- | --- | --- | --- |
| Non-interactive JSON mode | `pi --mode json --approve <prompt>` emits guessed event names like `delta` | `--mode json` emits JSONL with `session`, `message_update`, `tool_execution_start`, `message_end`, `agent_settled`, and nested `assistantMessageEvent` payloads | Client parser updated |
| RPC mode | not clearly modeled | `--mode rpc` is a command/event JSONL stream; `prompt` returns an ack first, then async lifecycle events continue on stdout | documented; recommended for long-lived control |
| Session handling | local `run_id` only; session continuity mostly ignored | Pi supports `--session-id` and rejects invalid characters like `:` | client now uses `--session-id` and normalizes IDs |
| Project MCP | Pi core was assumed to read `.pi/mcp.json` directly | project MCP works only after installing `pi-mcp-extension`; package enablement lives in `.pi/settings.json` | documented and configured |
| ToolGate bridge command | `python ../toolgate/.../toolgate_mcp.py` | host has no `python` shim; bridge also needs `python-dotenv` | `.pi/mcp.json` now points to repo-local `.venv-toolgate-mcp/bin/python` |
| Tool discovery | unverified | real Pi listed ToolGate-backed functions such as `functions.mcp_toolgate_research_search` | confirmed |

## What Changed

- Added `docs/pi-observed-behavior.md` with raw command shapes, exit behavior, session paths, and MCP findings.
- Enabled project-local Pi MCP through `.pi/settings.json`.
- Fixed `.pi/mcp.json` to use a runnable interpreter for the ToolGate stdio bridge on this host.
- Updated `adapter/pi_client.py` to:
  - invoke real Pi with `--mode json --approve --session-id`
  - honor `PI_PROVIDER` / `PI_MODEL` defaults
  - translate real Pi text/tool events into the frozen AgentGate SSE contract
  - normalize invalid session IDs before calling Pi
- Updated tests to mirror real Pi event shapes and added checkout-safe test bootstrap.

## Test Results

- Unit tests: `6 passed`
- Real Pi `--print`: returned `OK`, exit `0`
- Real Pi `--mode json`: matched documented JSONL lifecycle and tool execution shapes
- Real Pi `--mode rpc`: matched command/ack + async event stream model
- MCP with ToolGate: confirmed; ToolGate-backed tool names appeared in Pi output
- Real adapter API:
  - created session via `POST /api/sessions`
  - streamed SSE via `POST /api/sessions/{id}/chat/stream`
  - stored final assistant message `ADAPTER_OK`
- Real scheduler API:
  - created job via `POST /api/jobs`
  - executed immediate run via `POST /api/jobs/{id}/run`
  - returned final output `SCHED_OK`

Artifacts committed from the live run:

- `docs/e2e-session.json`
- `docs/e2e-chat-stream.sse`
- `docs/e2e-messages.json`
- `docs/e2e-job-create.json`
- `docs/e2e-job-run.json`

## RPC Recommendation

RPC looks better suited than one-process-per-prompt JSON mode for the eventual Hermes swap because it gives:

- explicit `get_state` / `prompt` commands
- stable session metadata including `sessionFile`
- a single long-lived stream for commands and events

This pass keeps the adapter on JSON mode because it was the smallest safe fix for the existing client abstraction. For the swap itself, RPC is the stronger target.

## MCP Status With ToolGate

- Status: working
- Mechanism: `pi-mcp-extension` installed project-locally through `.pi/settings.json`
- Config source: project `.pi/mcp.json`
- Result: ToolGate-backed functions were visible to real Pi in this repo

## Go / No-Go For Hermes Swap

Recommendation: `NO-GO` for replacing Hermes today.

Reason:

- the Pi-backed adapter is now proven for basic chat streaming and scheduler execution
- but the repo still has contract gaps outside the client fix scope, including unimplemented stop/approval flows and several `501` endpoints that AgentGate may need for a full replacement
- approval/tool governance behavior was not validated end-to-end through ToolGate decision loops in this session

Recommended next blockers to clear before a swap:

- implement and validate `/v1/runs/{run_id}/stop`
- implement and validate `/v1/runs/{run_id}/approval`
- decide whether to migrate the adapter to Pi RPC mode for long-lived sessions
- exercise a real ToolGate approval-required tool through the adapter stream
- fill or intentionally stub remaining discovery endpoints used by AgentGate UI
