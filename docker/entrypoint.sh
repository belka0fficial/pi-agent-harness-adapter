#!/usr/bin/env bash
set -Eeuo pipefail

export HOME="${HOME:-/app/data/home}"
export PI_PROVIDER="${PI_PROVIDER:-openai-codex}"

PI_RUNTIME_DIR="$HOME/.pi/agent"
PI_HOST_DIR="${PI_HOST_DIR:-/pi-host/agent}"

mkdir -p "$PI_RUNTIME_DIR/sessions" "$PI_RUNTIME_DIR/bin"

copy_if_present() {
  local name="$1"
  if [[ -f "$PI_HOST_DIR/$name" && ! -f "$PI_RUNTIME_DIR/$name" ]]; then
    cp "$PI_HOST_DIR/$name" "$PI_RUNTIME_DIR/$name"
  fi
}

copy_if_present auth.json
copy_if_present models-store.json
copy_if_present settings.json

if [[ -d "$PI_HOST_DIR/bin" ]]; then
  cp -R "$PI_HOST_DIR/bin/." "$PI_RUNTIME_DIR/bin/" 2>/dev/null || true
fi

cd /app
exec "$@"
