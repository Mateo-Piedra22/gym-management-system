import argparse
import json
from datetime import datetime

from database import DatabaseManager


def main():
    parser = argparse.ArgumentParser(
        description="Ejecuta limpieza sencilla de datos no esenciales (WhatsApp, auditoría, notificaciones)."
    )
    parser.add_argument("--whatsapp-days", type=int, default=120, help="Antigüedad en días para whatsapp_messages")
    parser.add_argument("--audit-logs-days", type=int, default=180, help="Antigüedad en días para audit_logs")
    parser.add_argument("--auditoria-days", type=int, default=180, help="Antigüedad en días para auditoria")
    parser.add_argument("--notificaciones-days", type=int, default=90, help="Antigüedad en días para notificaciones_cupos")
    parser.add_argument("--sysdiag-days", type=int, default=180, help="Antigüedad en días para system_diagnostics")
    # Eliminado: purge-sync (anterior)

    args = parser.parse_args()

    db = DatabaseManager()
    resumen = db.limpiar_datos_innecesarios(
        whatsapp_days=args.whatsapp_days,
        audit_logs_days=args.audit_logs_days,
        auditoria_days=args.auditoria_days,
        notificaciones_days=args.notificaciones_days,
        sysdiag_days=args.sysdiag_days,
    )

    salida = {
        "executed_at": datetime.now().isoformat(timespec="seconds"),
        "summary": resumen,
    }
    print(json.dumps(salida, indent=2))


if __name__ == "__main__":
    main()