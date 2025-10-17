# -*- coding: utf-8 -*-
"""
Script de prueba para POST /api/sync/upload_outbox
- Lee token con utils.get_sync_upload_token (y lo persiste desde ENV si aplica)
- Resuelve la base URL con utils.get_webapp_base_url
- Envía un payload mínimo {"changes": []}
- Imprime JSON con status y cuerpo de la respuesta
"""
import json
import sys
import os

# Asegurar que el proyecto raíz esté en sys.path para importar utils.py
try:
    _SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    _PROJECT_ROOT = os.path.dirname(_SCRIPT_DIR)
    if _PROJECT_ROOT not in sys.path:
        sys.path.insert(0, _PROJECT_ROOT)
except Exception:
    pass


def _http_post(url: str, headers: dict, payload: dict, timeout: float = 10.0):
    # Intentar con requests, fallback a urllib
    try:
        import requests  # type: ignore
        resp = requests.post(url, json=payload, headers=headers, timeout=timeout)
        return resp.status_code, resp.text
    except Exception:
        pass
    # Fallback urllib
    try:
        import urllib.request as ur  # type: ignore
        import urllib.error as ue  # type: ignore
        data = json.dumps(payload).encode("utf-8")
        req = ur.Request(url, data=data, headers=headers, method="POST")
        try:
            with ur.urlopen(req, timeout=timeout) as fp:  # type: ignore
                status = fp.status
                text = fp.read().decode("utf-8", errors="replace")
                return status, text
        except ue.HTTPError as he:  # type: ignore
            status = getattr(he, "code", 0) or 0
            text = he.read().decode("utf-8", errors="replace") if getattr(he, "fp", None) else str(he)
            return status, text
    except Exception as e2:
        return 0, json.dumps({"error": "http_client_error", "detail": str(e2)})


def main():
    try:
        from utils import get_sync_upload_token, get_webapp_base_url  # type: ignore
    except Exception as e:
        print(json.dumps({"ok": False, "error": "import_error", "detail": str(e)}))
        sys.exit(1)

    token = (get_sync_upload_token(persist_from_env=True) or "").strip()
    base_url = (get_webapp_base_url() or "").strip().rstrip("/")
    url = base_url + "/api/sync/upload_outbox"

    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
        headers["X-Upload-Token"] = token

    payload = {"changes": []}

    status, text = _http_post(url, headers, payload, timeout=12.0)

    try:
        body = json.loads(text) if text else {}
    except Exception:
        body = {"raw": text}

    result = {
        "url": url,
        "has_token": bool(token),
        "status": status,
        "body": body,
    }
    print(json.dumps(result, ensure_ascii=False))

    if status == 401:
        print("Sugerencia: el token local no coincide con el del servidor o falta token.", file=sys.stderr)
    elif status == 0:
        print("No fue posible contactar el servidor. Verifica conectividad/URL.", file=sys.stderr)


if __name__ == "__main__":
    main()