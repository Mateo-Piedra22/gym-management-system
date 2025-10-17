# -*- coding: utf-8 -*-
"""
Script CLI para ejecutar el uploader de sincronización una vez.
Uso:
  python scripts/run_sync_uploader.py
Opcional:
  Establecer `SYNC_UPLOAD_URL` y `SYNC_UPLOAD_TOKEN` en el entorno
  o `config/sync_uploader.json` con {"url": "...", "auth_token": "..."}.
"""

import json
import os
import sys

# Permitir ejecutar desde repos raíz
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

from sync_uploader import SyncUploader  # noqa: E402


def main() -> int:
    uploader = SyncUploader()
    sent, deleted = uploader.flush_once()
    print(json.dumps({"sent": sent, "deleted": deleted}, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())