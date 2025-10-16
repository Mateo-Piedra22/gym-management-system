import os
import sys
from typing import List, Tuple

try:
    import requests
    from requests.auth import HTTPBasicAuth
except Exception as e:
    print(f"[Error] requests no disponible: {e}")
    sys.exit(1)


def try_endpoint(method: str, url: str, auth: HTTPBasicAuth | None) -> Tuple[int, str]:
    try:
        if method.upper() == "GET":
            r = requests.get(url, auth=auth, timeout=15)
        else:
            r = requests.post(url, auth=auth, timeout=15)
        txt = r.text
        return r.status_code, txt[:200]
    except Exception as e:
        return -1, f"ERR: {e}"


def main():
    base = os.getenv("SERVER_BASE_URL", "https://gymms-symmetricds-node-production.up.railway.app").rstrip("/")
    user = os.getenv("SYM_REST_USER", "sym_user")
    pwd = os.getenv("SYM_REST_PASSWORD", "sym_password")
    auth = HTTPBasicAuth(user, pwd)

    candidates: List[Tuple[str, str]] = [
        ("GET", f"{base}/api"),
        ("GET", f"{base}/api/engine"),
        ("GET", f"{base}/api/engines"),
        ("GET", f"{base}/api/engine/corp-000"),
        ("GET", f"{base}/api/engines/corp-000"),
        ("POST", f"{base}/api/engine/corp-000/start"),
        ("POST", f"{base}/api/engines/corp-000/start"),
        ("POST", f"{base}/api/engine/start?engineName=corp-000"),
        ("POST", f"{base}/api/engine?action=start&engineName=corp-000"),
        ("POST", f"{base}/sync/corp-000/start"),
        ("POST", f"{base}/sync/corp-000?action=start"),
        ("GET", f"{base}/sync/corp-000"),
    ]

    print(f"[Info] Base: {base}")
    for method, url in candidates:
        code, text = try_endpoint(method, url, auth)
        print(f"{method} {url} -> {code} {text}")


if __name__ == "__main__":
    main()