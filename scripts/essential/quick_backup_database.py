#!/usr/bin/env python3
"""
Backup rápido headless de la base de datos local usando pg_dump.
Unificado con la UI mediante utils_modules.backup_utils.perform_quick_backup.
"""
import sys
from pathlib import Path

# Asegurar que el repo raíz esté en sys.path para poder importar utils_modules
_repo_root = Path(__file__).resolve().parent.parent
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from utils_modules.backup_utils import perform_quick_backup


def main():
    rc, out_path = perform_quick_backup()
    if rc == 0:
        print(f"[OK] Backup finalizado: {out_path}")
    else:
        print(f"[ERROR] Backup con código {rc}. Archivo destino: {out_path}")
    return rc


if __name__ == '__main__':
    raise SystemExit(main())