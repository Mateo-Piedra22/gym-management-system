import argparse
import json
import sys
from typing import Any, Dict

try:
    import requests  # type: ignore
except Exception:
    requests = None  # type: ignore


def main() -> int:
    parser = argparse.ArgumentParser(description="Ejecuta migración de esquema de sincronización vía API admin")
    parser.add_argument("--base-url", required=True, help="Base URL de la webapp (ej: https://gym-ms-zrk.up.railway.app)")
    parser.add_argument("--dev-password", dest="dev_password", help="DEV_PASSWORD para autorización", default=None)
    parser.add_argument(
        "--owner-password", dest="owner_password", help="Contraseña del dueño (alternativa a dev_password)", default=None
    )
    args = parser.parse_args()

    if requests is None:
        print("ERROR: 'requests' no está instalado. pip install requests", file=sys.stderr)
        return 2

    if not args.dev_password and not args.owner_password:
        print("Debe especificar --dev-password o --owner-password", file=sys.stderr)
        return 2

    url = args.base_url.rstrip("/") + "/api/admin/sync-migrate"
    payload: Dict[str, Any] = {}
    if args.dev_password:
        payload["dev_password"] = args.dev_password
    if args.owner_password:
        payload["owner_password"] = args.owner_password

    try:
        resp = requests.post(url, json=payload, timeout=30)
        print(f"HTTP {resp.status_code}")
        try:
            print(json.dumps(resp.json(), ensure_ascii=False, indent=2))
        except Exception:
            print(resp.text)
        return 0 if resp.ok else 1
    except Exception as e:
        print(f"ERROR llamando a {url}: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())