# -*- coding: utf-8 -*-
"""
Script CLI para ejecutar un flush puntual del outbox (public.sync_outbox).
Uso:
  python scripts/run_outbox_flush_once.py

Notas:
- Usa DatabaseManager para conectarse a la base local según config/config.json.
- Lee un lote desde public.sync_outbox y lo envía a /api/sync/upload_outbox.
- El token se toma de utils.get_sync_upload_token (persistiendo desde ENV si aplica).
- Imprime un JSON con el resultado y termina con exit code 0.
"""

import json
import os
import sys

# Permitir ejecutar desde raíz del repo
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)


def main() -> int:
    try:
        from database import DatabaseManager  # type: ignore
        from utils_modules.outbox_poller import OutboxPoller  # type: ignore
    except Exception as e:
        print(json.dumps({"ok": False, "error": f"imports_failed: {e}"}, ensure_ascii=False))
        return 2

    # Instanciar DB manager y poller
    try:
        dbm = DatabaseManager()
        poller = OutboxPoller(dbm)
    except Exception as e:
        print(json.dumps({"ok": False, "error": f"init_failed: {e}"}, ensure_ascii=False))
        return 3

    # Ejecutar una vuelta de envío
    try:
        res = poller.flush_once() or {}
        # Normalizar salida
        out = {
            "ok": True,
            "pending": int(res.get("pending", 0)) if isinstance(res.get("pending"), int) else res.get("pending"),
            "sent": int(res.get("sent", 0)) if isinstance(res.get("sent"), int) else res.get("sent"),
            "acked": int(res.get("acked", 0)) if isinstance(res.get("acked"), int) else res.get("acked"),
            "auth": res.get("auth"),
            "error": res.get("error"),
        }
        print(json.dumps(out, ensure_ascii=False))
        return 0
    except Exception as e:
        print(json.dumps({"ok": False, "error": f"flush_failed: {e}"}, ensure_ascii=False))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())