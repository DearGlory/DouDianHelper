from __future__ import annotations

import json
from pathlib import Path
from typing import Any


RUNTIME_STATE_VERSION = 1


def get_runtime_state_path(config: dict[str, Any]) -> Path:
    config_path = Path(str(config.get("config_path", "config.json")))
    return config_path.with_name(f"{config_path.name}.pause_state.json")


def load_runtime_state(config: dict[str, Any]) -> dict[str, Any] | None:
    path = get_runtime_state_path(config)
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def save_runtime_state(config: dict[str, Any], state: dict[str, Any]) -> Path:
    path = get_runtime_state_path(config)
    payload = {"version": RUNTIME_STATE_VERSION, **state}
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(path)
    return path


def clear_runtime_state(config: dict[str, Any]) -> None:
    path = get_runtime_state_path(config)
    if path.exists():
        path.unlink()
