import json
from datetime import datetime

from database import DatabaseManager


def main():
    db = DatabaseManager()
    resumen = db.eliminar_objetos_sync_antiguos()
    salida = {
        "executed_at": datetime.now().isoformat(timespec="seconds"),
        "summary": resumen,
    }
    print(json.dumps(salida, indent=2))


if __name__ == "__main__":
    main()
"""Remueve objetos antiguos de sincronización ya no utilizados.

Genera un resumen JSON con el estado de eliminación por objeto.
"""