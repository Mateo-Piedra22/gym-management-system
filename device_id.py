import os
import uuid
from typing import Optional


def _config_dir() -> str:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    cfg_dir = os.path.join(base_dir, "config")
    os.makedirs(cfg_dir, exist_ok=True)
    return cfg_dir


def _device_id_path() -> str:
    return os.path.join(_config_dir(), "device_id.txt")


def get_device_id() -> str:
    """Return a persistent device ID (UUID4) for this desktop instance.

    Stored at config/device_id.txt. Created on first use.
    """
    path = _device_id_path()
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                val = (f.read() or "").strip()
                if val:
                    return val
    except Exception:
        pass

    new_id = str(uuid.uuid4())
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(new_id)
    except Exception:
        # If write fails, still return in-memory ID
        pass
    return new_id


def set_device_id(value: str) -> Optional[str]:
    """Force-set device ID (mostly for testing). Returns the value written."""
    if not value:
        return None
    path = _device_id_path()
    with open(path, "w", encoding="utf-8") as f:
        f.write(value)
    return value