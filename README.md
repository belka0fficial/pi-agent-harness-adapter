# Pi Agent Harness Adapter

Pi-backed brain adapter for the AgentGate runtime contract.

This repo is now the active upstream behind AgentGate in the Conker stack.

## Run Adapter

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
uvicorn adapter.main:app --host 127.0.0.1 --port 8644
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

Pi auth must already exist at `~/.pi/agent/auth.json` before running the containerized stack.
