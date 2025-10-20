import json
import os
import sys
from typing import Any, Dict

# Asegurar que el directorio raíz del proyecto esté en sys.path
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from database import DatabaseManager  # type: ignore


def _safe_call(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except Exception as e:
        return e


def main() -> int:
    old_id = 2
    new_id = 3
    db = DatabaseManager()

    # Pre-checks
    exists_old = bool(_safe_call(db.usuario_id_existe, old_id))
    exists_new = bool(_safe_call(db.usuario_id_existe, new_id))

    result: Dict[str, Any] = {
        "old_id": old_id,
        "new_id": new_id,
        "precheck": {
            "exists_old": exists_old,
            "exists_new": exists_new,
        },
        "changed": False,
        "error": None,
        "postcheck": {},
    }

    if not exists_old:
        result["error"] = f"El usuario {old_id} no existe"
        print(json.dumps(result, ensure_ascii=False))
        return 1
    if exists_new:
        result["error"] = f"El ID {new_id} ya está en uso"
        print(json.dumps(result, ensure_ascii=False))
        return 1

    # Intentar cambio de ID
    change_err = _safe_call(db.cambiar_usuario_id, old_id, new_id)
    if isinstance(change_err, Exception):
        result["error"] = f"Error en cambiar_usuario_id: {change_err}"
        print(json.dumps(result, ensure_ascii=False))
        return 1

    # Post-checks
    exists_old_after = bool(_safe_call(db.usuario_id_existe, old_id))
    exists_new_after = bool(_safe_call(db.usuario_id_existe, new_id))
    summary_new = _safe_call(db.obtener_resumen_referencias_usuario, new_id)
    summary_old = _safe_call(db.obtener_resumen_referencias_usuario, old_id)

    result["changed"] = True
    result["postcheck"] = {
        "exists_old": exists_old_after,
        "exists_new": exists_new_after,
        "summary_new": summary_new if not isinstance(summary_new, Exception) else {"error": str(summary_new)},
        "summary_old": summary_old if not isinstance(summary_old, Exception) else {"error": str(summary_old)},
    }

    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())