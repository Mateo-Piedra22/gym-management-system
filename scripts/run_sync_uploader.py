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
    # Persistir token en config/config.json si está en entorno
    try:
        token = os.getenv('SYNC_UPLOAD_TOKEN', '').strip()
        if token:
            cfg_path = os.path.join(BASE_DIR, 'config', 'config.json')
            os.makedirs(os.path.dirname(cfg_path), exist_ok=True)
            data = {}
            if os.path.exists(cfg_path):
                try:
                    with open(cfg_path, 'r', encoding='utf-8') as f:
                        data = json.load(f) or {}
                    if not isinstance(data, dict):
                        data = {}
                except Exception:
                    data = {}
            cur = str(data.get('sync_upload_token') or '').strip()
            if cur != token:
                data['sync_upload_token'] = token
                try:
                    with open(cfg_path, 'w', encoding='utf-8') as f:
                        json.dump(data, f, ensure_ascii=False, indent=2)
                except Exception:
                    pass
    except Exception:
        pass
    uploader = SyncUploader()
    sent, deleted = uploader.flush_once()
    print(json.dumps({"sent": sent, "deleted": deleted}, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())