# Brain

Pi-backed adapter scaffold for the AgentGate/Hermes contract.

This repo does **not** replace Hermes yet. It freezes the API surface AgentGate currently consumes and provides a small compatibility adapter that can be tested with Pi mocked.

## Run Adapter

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
uvicorn adapter.main:app --host 127.0.0.1 --port 8642
```

## Run Scheduler

```bash
uvicorn scheduler.main:app --host 127.0.0.1 --port 8643
```

## Pi MCP

Install Pi and the MCP extension, then use `.pi/mcp.json`:

```bash
npm install -g --ignore-scripts @earendil-works/pi-coding-agent
pi install npm:pi-mcp-extension
```

Pi supports JSON/RPC modes and project MCP config through `pi-mcp-extension`; see `CONTRACT.md` for notes.

