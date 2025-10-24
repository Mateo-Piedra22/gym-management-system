# -*- coding: utf-8 -*-
"""
Bootstrap de automatización completa (headless) para Gym Management System.

Acciones principales:
- Detecta/deriva un device_id estable (Windows MachineGuid; fallback hostname/mac).
- Ejecuta ensure_prerequisites(device_id): instala PostgreSQL si falta, crea DB,
  instala outbox/función/índices/triggers, aplica tareas programadas (schtasks),
  configura red/Firewall/VPN cuando aplica y orquesta replicación lógica
  (unidireccional o bidireccional) según la configuración disponible.
- Opcionalmente, ejecuta una verificación de salud de replicación y la imprime (JSON).

Uso:
  python scripts/bootstrap_full_automation.py [--device-id <id>] [--skip-health]

Exit codes:
  0 = OK (bootstrap ejecutado y reportado)
  1 = Error inesperado
"""
import json
import os
import sys
import uuid
import socket
from pathlib import Path
from typing import Optional

# Asegurar imports del proyecto
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))


def _read_windows_machine_guid() -> Optional[str]:
    """Lee MachineGuid del registro de Windows si es posible."""
    try:
        if os.name != 'nt':
            return None
        import winreg  # type: ignore
        key_path = r"SOFTWARE\Microsoft\Cryptography"
        with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, key_path) as key:
            value, _ = winreg.QueryValueEx(key, "MachineGuid")
            if value and isinstance(value, str):
                return value.strip()
    except Exception:
        return None
    return None


def _derive_device_id() -> str:
    """Genera un device_id estable usando varias fuentes."""
    # 1) MachineGuid en Windows
    mg = _read_windows_machine_guid()
    if mg:
        return f"win-{mg}"
    # 2) Hostname + MAC (uuid.getnode)
    try:
        host = socket.gethostname()
    except Exception:
        host = "unknown-host"
    try:
        mac_int = uuid.getnode()
        mac_hex = f"{mac_int:012x}"
    except Exception:
        mac_hex = "nomac"
    return f"host-{host}-mac-{mac_hex}"


def main(argv: list[str]) -> int:
    import argparse
    parser = argparse.ArgumentParser(description="Bootstrap headless de automatización completa")
    parser.add_argument("--device-id", dest="device_id", default=None, help="Device ID a utilizar (opcional)")
    parser.add_argument("--skip-health", dest="skip_health", action="store_true", help="No ejecutar verificación de salud de replicación")
    args = parser.parse_args(argv)

    device_id = args.device_id or _derive_device_id()

    out = {
        "ok": False,
        "device_id": device_id,
        "ensure_prerequisites": None,
        "replication_health": None,
    }

    try:
        # Ejecutar prerequisites end-to-end
        from utils_modules.prerequisites import ensure_prerequisites  # type: ignore
        prereq_res = ensure_prerequisites(device_id)
        out["ensure_prerequisites"] = prereq_res
    except Exception as e:
        out["error"] = f"ensure_prerequisites failed: {e}"
        print(json.dumps(out, ensure_ascii=False, indent=2, default=str))
        return 1

    # Verificación de salud de replicación (opcional)
    if not args.skip_health:
        try:
            # Importar y ejecutar como función para evitar nuevo proceso
            # El script imprime JSON; aquí devolvemos el dict si es posible.
            # Reutilizamos su lógica de conexión.
            from scripts.verify_replication_health import main as verify_main  # type: ignore
            # Capturar salida JSON imprimiéndola también
            rc = verify_main()
            out["replication_health"] = "printed"
        except SystemExit as se:
            # El verify_main hace sys.exit; capturamos exit code
            out["replication_health"] = {"exit_code": int(getattr(se, 'code', 1) or 0)}
        except Exception as e:
            out["replication_health"] = {"error": str(e)}

    out["ok"] = True
    print(json.dumps(out, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))