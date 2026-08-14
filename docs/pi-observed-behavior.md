# Pi Observed Behavior

Observed on August 14, 2026 with:

- Pi CLI `0.84.2`
- provider `openai-codex`
- repo cwd `/home/alexeybe1kin/repos/pi-agent-harness-adapter`

## Install And Auth Reality

- Package install path that worked here: `@earendil-works/pi-coding-agent`
- Login is not a standalone `pi login` command.
- `pi auth --help` exposes `print-api-key`, `print-bearer-token`, and `check`.
- OpenAI/Codex subscription login is handled through Pi's interactive flow, not through a separate non-interactive CLI subcommand.

## Real CLI Flags

From `pi --help`:

- print mode: `--print` or `-p`
- machine-readable modes: `--mode json` and `--mode rpc`
- session flags: `--continue`, `--resume`, `--session`, `--session-id`, `--fork`, `--session-dir`, `--no-session`
- project trust flags: `--approve`, `--no-approve`

## `--print` Mode

Command used:

```bash
pi --provider openai-codex --no-tools --approve -p "Reply with exactly OK."
```

Observed stdout:

```text
OK
```

Observed stderr:

```text
(empty)
```

Observed exit code:

```text
0
```

## `--mode json`

Command used:

```bash
pi --provider openai-codex --tools ls --approve --mode json \
  "List the first two files in the current directory, then answer in one sentence."
```

Observed properties:

- stdout is JSON Lines, not plain SSE.
- first line is a session header:

```json
{"type":"session","version":3,"id":"01a001ce-20b0-75f0-8e9f-b0df784468f7","timestamp":"2026-08-14T19:44:41.136Z","cwd":"/home/alexeybe1kin/repos/pi-agent-harness-adapter"}
```

- stream then emits top-level lifecycle events such as:
  - `agent_start`
  - `turn_start`
  - `message_start`
  - `message_update`
  - `message_end`
  - `tool_execution_start`
  - `tool_execution_end`
  - `turn_end`
  - `agent_end`
  - `agent_settled`
- incremental assistant text arrives inside:

```json
{"type":"message_update","assistantMessageEvent":{"type":"text_delta","contentIndex":0,"delta":"The"}}
```

- tool call construction arrives inside:

```json
{"type":"message_update","assistantMessageEvent":{"type":"toolcall_delta","contentIndex":0,"delta":"{\""}}
```

- tool execution is emitted separately from assistant deltas:

```json
{"type":"tool_execution_start","toolCallId":"...","toolName":"ls","args":{"path":".","limit":2}}
{"type":"tool_execution_end","toolCallId":"...","toolName":"ls","result":{"content":[{"type":"text","text":".git/\n.gitignore\n\n[2 entries limit reached. Use limit=4 for more]"}],"details":{"entryLimitReached":2}},"isError":false}
```

- final assistant message is a full structured `message_end`, not a guessed `{"event":"delta"}` shape:

```json
{"type":"message_end","message":{"role":"assistant","content":[{"type":"text","text":"The first two entries in the current directory are `.git/` and `.gitignore`."}],"stopReason":"stop"}}
```

Observed stderr:

```text
(empty)
```

Observed exit code:

```text
0
```

## `--mode rpc`

Command shape:

```bash
pi --provider openai-codex --no-tools --approve --mode rpc
```

Observed stdin commands:

```json
{"id":"state-1","type":"get_state"}
{"id":"prompt-1","type":"prompt","message":"Reply with exactly OK."}
```

Observed stdout behavior:

- stdout is also JSON Lines.
- command acknowledgements and asynchronous agent events share the same stream.
- the `prompt` command returns an acknowledgement first, then the actual agent lifecycle events continue afterward.

Observed `get_state` response shape:

```json
{"id":"state-1","type":"response","command":"get_state","success":true,"data":{"sessionFile":"/home/alexeybe1kin/.pi/agent/sessions/--home-alexeybe1kin-repos-pi-agent-harness-adapter--/2026-08-14T19-48-36-633Z_01a001d1-b899-70d5-9067-907a5e3359fd.jsonl","sessionId":"01a001d1-b899-70d5-9067-907a5e3359fd"}}
```

Observed prompt acknowledgement:

```json
{"id":"prompt-1","type":"response","command":"prompt","success":true}
```

Observed agent event continuation after the acknowledgement:

```json
{"type":"agent_start"}
{"type":"turn_start"}
{"type":"message_start","message":{"role":"user","content":[{"type":"text","text":"Reply with exactly OK."}]}}
{"type":"message_update","assistantMessageEvent":{"type":"text_delta","contentIndex":0,"delta":"OK"}}
{"type":"message_end","message":{"role":"assistant","content":[{"type":"text","text":"OK"}],"stopReason":"stop"}}
{"type":"agent_settled"}
```

Observed stderr:

```text
(empty)
```

Observed exit code:

```text
0
```

Implication:

- RPC is a better fit for long-lived session control than spawning one `--mode json` process per prompt.
- JSON mode is still useful for simple fire-and-stream subprocess integration.

## Session Storage Reality

- default session files live under `~/.pi/agent/sessions/`
- `get_state` reports the concrete `sessionFile` and `sessionId`
- `--session-dir` overrides the storage directory for subprocess runs

## MCP Reality

The adapter's old assumption was incomplete.

- Core Pi does not load project MCP config by itself.
- MCP support is provided through the `pi-mcp-extension` package.
- Project-local package registration is stored in `.pi/settings.json`.
- Once the extension is installed, project `.pi/mcp.json` becomes active.

Local package registration used here:

```json
{
  "packages": [
    "npm:pi-mcp-extension"
  ]
}
```

## ToolGate MCP Registration

Working project-local config:

- package enablement: `.pi/settings.json`
- server config: `.pi/mcp.json`

Important local fix:

- `command: "python"` failed because this host has no `python` shim.
- `python3` alone still failed because the ToolGate MCP bridge needs `python-dotenv`.
- the working repo-local bridge command is `.venv-toolgate-mcp/bin/python ../toolgate/toolgate/mcp/toolgate_mcp.py`

## Tool Discovery Through Real Pi

Prompt used:

```bash
pi --approve --mode json "List available tools and say whether any tool name includes toolgate."
```

Observed answer included ToolGate-backed functions such as:

- `functions.mcp_toolgate_research_search`
- `functions.mcp_toolgate_research_fetch`
- `functions.mcp_toolgate_toolgate_request_status`

Conclusion:

- ToolGate tools do appear to Pi once `pi-mcp-extension` is installed and `.pi/mcp.json` points at a runnable bridge interpreter.
