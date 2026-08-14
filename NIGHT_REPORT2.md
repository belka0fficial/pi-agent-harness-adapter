# NIGHT_REPORT2

Date: August 14, 2026

Repo: `~/repos/pi-agent-harness-adapter`

## Task Status

### TASK 1 — Migrate client to RPC mode

Status: `PASS`

Completed:

- Replaced one-process-per-prompt JSON execution with a long-lived Pi RPC runtime per active session.
- Added session-bound run tracking, RPC command/response handling, session restart support, and prompt-level per-turn instruction injection.
- Added project-level SOUL injection support at session start through `PI_SOUL_TEXT` or `PI_SOUL_FILE`.

Acceptance evidence:

- Unit tests pass.
- Real RPC transcript saved: [docs/e2e-rpc-chat.sse](/home/alexeybe1kin/repos/pi-agent-harness-adapter/docs/e2e-rpc-chat.sse)
- Supporting artifacts:
  - [docs/e2e-rpc-session.json](/home/alexeybe1kin/repos/pi-agent-harness-adapter/docs/e2e-rpc-session.json)
  - [docs/e2e-rpc-messages.json](/home/alexeybe1kin/repos/pi-agent-harness-adapter/docs/e2e-rpc-messages.json)

Design choice:

- One long-lived Pi RPC process per active session.
- Reason: Pi RPC exposes stable session state and `abort`, but does not expose a clean multi-session routing protocol over a shared process.

### TASK 2 — Implement `/v1/runs/{run_id}/stop`

Status: `PASS`

Completed:

- Mapped stop to Pi RPC `abort`.
- Added run registry and stop endpoint wiring.
- Suppressed duplicate abort-terminal events caused by Pi emitting both aborted `message_end` and aborted `turn_end`.

Acceptance evidence:

- Automated stop contract test passes.
- Real stop transcript saved: [docs/e2e-stop.sse](/home/alexeybe1kin/repos/pi-agent-harness-adapter/docs/e2e-stop.sse)
- Recovery transcript saved: [docs/e2e-stop-recovery.sse](/home/alexeybe1kin/repos/pi-agent-harness-adapter/docs/e2e-stop-recovery.sse)
- Stop decision response saved: [docs/e2e-stop-decision.json](/home/alexeybe1kin/repos/pi-agent-harness-adapter/docs/e2e-stop-decision.json)

Observed behavior:

- Real stop now emits exactly one `run.stopped`.
- Same session remained usable afterward; recovery answer streamed as split deltas spelling `STOP_RECOVERY_OK`.

### TASK 3 — Implement `/v1/runs/{run_id}/approval`

Status: `PASS`

Completed:

- Added approval-required ToolGate detection by parsing ToolGate MCP tool results for `CONFIRMATION_REQUIRED`.
- Added `approval.required` SSE emission with the request id, expiry, and ToolGate summary payload.
- Added approval decision endpoint wiring.
- On approval decision, the adapter now decides the ToolGate request and resumes Pi by prompting it to retry the exact tool call with `approval_request_id`.
- On rejection, the adapter resumes Pi with a rejection message so it can continue the turn safely.

Acceptance evidence:

- Automated approve/reject contract test passes.

Important implementation note:

- ToolGate request lookup and decision now go through `docker exec -i toolgate-api ...` so the adapter acts on the live writable ToolGate state, not the host-side read-only view.

### TASK 4 — Real ToolGate approval end-to-end

Status: `PASS`

Completed:

- Added one minimal live ToolGate test tool in the running ToolGate control plane:
  - id: `approval.test-echo`
  - execution: `echo`
  - authorization: `owner_confirmation`
- Confirmed Pi sees it as `mcp_toolgate_approval_test_echo`.
- Ran the full loop:
  - chat
  - Pi tool call
  - `approval.required` SSE event
  - owner decision through adapter API
  - approved retry with `approval_request_id`
  - final answer returned through Pi

Acceptance evidence:

- Approve transcript: [docs/e2e-approval.sse](/home/alexeybe1kin/repos/pi-agent-harness-adapter/docs/e2e-approval.sse)
- Request record reference: [docs/e2e-approval-request.json](/home/alexeybe1kin/repos/pi-agent-harness-adapter/docs/e2e-approval-request.json)
- Approval response: [docs/e2e-approval-decision.json](/home/alexeybe1kin/repos/pi-agent-harness-adapter/docs/e2e-approval-decision.json)
- Reject path exercised and saved:
  - [docs/e2e-approval-reject.sse](/home/alexeybe1kin/repos/pi-agent-harness-adapter/docs/e2e-approval-reject.sse)
  - [docs/e2e-approval-reject-decision.json](/home/alexeybe1kin/repos/pi-agent-harness-adapter/docs/e2e-approval-reject-decision.json)

### TASK 5 — Resolve remaining 501s

Status: `PASS`

Result:

- No AgentGate frontend-called adapter path returns `501` now.

Endpoint table:

| Endpoint | Frontend calls it | Status |
| --- | --- | --- |
| `GET /api/sessions` | yes | implemented |
| `POST /api/sessions` | yes | implemented |
| `GET /api/sessions/{id}` | yes | implemented |
| `PATCH /api/sessions/{id}` | yes | implemented |
| `DELETE /api/sessions/{id}` | yes | implemented |
| `GET /api/sessions/{id}/messages` | yes | implemented |
| `POST /api/sessions/{id}/fork` | yes | implemented with message copy plus Pi session fork best-effort |
| `POST /api/sessions/{id}/chat/stream` | yes | implemented |
| `GET /api/model/options` | yes | implemented as best-effort `pi --list-models` snapshot |
| `GET /v1/capabilities` | yes | implemented |
| `GET /v1/skills` | potentially | stubbed valid empty array |
| `GET /v1/toolsets` | potentially | stubbed valid empty array |
| `POST /v1/runs/{run_id}/stop` | yes | implemented |
| `POST /v1/runs/{run_id}/approval` | yes | implemented |
| `GET /api/jobs` | yes | already implemented |
| `POST /api/jobs` | yes | already implemented |
| `PATCH /api/jobs/{id}` | yes | already implemented |
| `DELETE /api/jobs/{id}` | yes | already implemented |
| `POST /api/jobs/{id}/pause` | yes | already implemented |
| `POST /api/jobs/{id}/resume` | yes | already implemented |
| `POST /api/jobs/{id}/run` | yes | already implemented |

### TASK 6 — Character/system-prompt injection

Status: `PASS`

Completed:

- Added session-start SOUL injection through `PI_SOUL_TEXT` or `PI_SOUL_FILE`.
- Injected via Pi startup flag `--append-system-prompt`.
- Added unit coverage proving the flag is added when configured.

Acceptance evidence:

- Unit test `test_build_rpc_command_includes_feature_flagged_soul_block`

## Test Summary

- Unit tests: `8 passed`
- Real RPC chat: passed
- Real stop: passed
- Real stop recovery: passed
- Real ToolGate approval approve path: passed
- Real ToolGate approval reject path: exercised successfully
- Real fork endpoint sanity check: passed

## Open Notes

- The live ToolGate approval test tool was added to the running ToolGate control-plane state, not to ToolGate repo code.
- The adapter still keeps session/message metadata locally in memory; process restart persistence beyond Pi’s own session files remains limited.
- `scheduler/main.py` still uses FastAPI `on_event`, which emits deprecation warnings but is not blocking swap prep.

## Swap Verdict

Verdict: `GO`

Reason:

- The blockers named in `PIVALIDATION_REPORT.md` were closed:
  - RPC migration completed
  - stop endpoint completed
  - approval endpoint completed
  - real ToolGate approval loop proven
  - frontend-called 501s removed
  - SOUL injection added

Remaining risks:

- The approval resume flow currently relies on a follow-up prompt that tells Pi to retry the exact ToolGate tool call with `approval_request_id`; this is working in the validated case, but it is still a model-mediated resume rather than a lower-level forced tool replay primitive.
- Session metadata and approval bookkeeping are still in-memory inside the adapter process.
- The live approval verifier tool should be removed or formalized before a production-hardening pass.

## Swap-Day Checklist

1. Keep Hermes running for rollback.
2. Point AgentGate’s Hermes upstream target to this adapter’s loopback address instead of Hermes.
3. Verify:
   - chat stream
   - stop
   - approval inbox
   - cron run
4. Run one approval-gated ToolGate action from AgentGate.
5. If any critical regression appears, roll back in one step by restoring AgentGate’s upstream target back to Hermes.

Next-session rollback path:

- Change only the AgentGate upstream Hermes base URL back to the existing Hermes service.
