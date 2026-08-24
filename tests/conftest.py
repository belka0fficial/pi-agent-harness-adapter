from __future__ import annotations

import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture(autouse=True)
def isolated_adapter_data_dir(monkeypatch, tmp_path):
    """Keep local test runs from touching the container default /app/data."""
    from adapter import main

    data_dir = tmp_path / "adapter-data"
    monkeypatch.setenv("ADAPTER_DATA_DIR", str(data_dir))
    monkeypatch.setattr(main, "DATA_DIR", data_dir)
    monkeypatch.setattr(main, "REGISTRY_DB", data_dir / "registry.sqlite3")
