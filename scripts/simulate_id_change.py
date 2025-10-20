import json
import os
import sys
# Asegurar que el directorio raíz del proyecto esté en sys.path
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)
from database import DatabaseManager


def main():
    db = DatabaseManager()
    old_id = 3
    new_id = 2
    try:
        exists_old = db.usuario_id_existe(old_id)
    except Exception as e:
        print(json.dumps({"error": f"usuario_id_existe({old_id}) failed: {e}"}))
        return 1
    try:
        exists_new = db.usuario_id_existe(new_id)
    except Exception as e:
        print(json.dumps({"error": f"usuario_id_existe({new_id}) failed: {e}"}))
        return 1
    summary = {}
    if exists_old:
        try:
            summary = db.obtener_resumen_referencias_usuario(old_id) or {}
        except Exception as e:
            summary = {"error": f"obtener_resumen_referencias_usuario failed: {e}"}
    result = {
        "old_id": old_id,
        "new_id": new_id,
        "exists_old": bool(exists_old),
        "exists_new": bool(exists_new),
        "can_change": bool(exists_old) and not bool(exists_new),
        "summary": summary
    }
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())