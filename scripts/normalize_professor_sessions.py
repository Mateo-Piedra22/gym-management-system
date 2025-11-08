import argparse
import json
from datetime import datetime

from database import DatabaseManager


def parse_date(value):
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except Exception:
        raise argparse.ArgumentTypeError("Formato de fecha inválido. Use YYYY-MM-DD.")


def main():
    parser = argparse.ArgumentParser(
        description="Normaliza sesiones antiguas corrigiendo hora_fin o minutos_totales"
    )
    parser.add_argument("--profesor-id", type=int, required=True)
    parser.add_argument("--fecha-inicio", type=parse_date, required=False)
    parser.add_argument("--fecha-fin", type=parse_date, required=False)
    parser.add_argument(
        "--preferencia",
        type=str,
        choices=["minutos", "timestamps"],
        default="minutos",
    )
    parser.add_argument("--tolerancia-minutos", type=int, default=5)

    args = parser.parse_args()

    db = DatabaseManager()
    result = db.normalizar_sesiones_profesor(
        profesor_id=args.profesor_id,
        fecha_inicio=args.fecha_inicio,
        fecha_fin=args.fecha_fin,
        preferencia=args.preferencia,
        tolerancia_minutos=args.tolerancia_minutos,
    )

    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()