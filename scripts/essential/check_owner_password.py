#!/usr/bin/env python3
import json
import os
import sys
from pathlib import Path

try:
    import psycopg2
except Exception as e:
    print(json.dumps({"ok": False, "error": f"psycopg2 import failed: {e}"}))
    sys.exit(1)

try:
    # Add project root (two levels up from scripts/essential)
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
except Exception:
    pass

def main():
    from secure_config import config as secure_config
    from security_utils import SecurityUtils

    result = {
        "ok": True,
        "db_query": None,
        "is_bcrypt": False,
        "env_present": False,
        "env_keys": [],
        "env_verifies": False,
        "notes": []
    }

    # Read env for owner password
    env_pwd = (os.getenv("WEBAPP_OWNER_PASSWORD", "") or os.getenv("OWNER_PASSWORD", "")).strip()
    if env_pwd:
        result["env_present"] = True
        result["env_keys"] = [k for k in ("WEBAPP_OWNER_PASSWORD", "OWNER_PASSWORD") if os.getenv(k)]

    # Connect to DB
    conn = None
    try:
        cfg = secure_config.get_db_config('local')
        conn = psycopg2.connect(**cfg)
    except Exception as e:
        # Fallback: intentar con config/config.json
        try:
            cfg_path = Path(__file__).resolve().parents[2] / 'config' / 'config.json'
            with open(cfg_path, 'r', encoding='utf-8') as f:
                cfg_json = json.load(f) or {}
            node = cfg_json.get('db_connection') or cfg_json
            alt_cfg = {
                'host': node.get('host') or cfg_json.get('host') or 'localhost',
                'port': int(node.get('port') or cfg_json.get('port') or 5432),
                'dbname': node.get('database') or cfg_json.get('database') or 'gimnasio',
                'user': node.get('user') or cfg_json.get('user') or 'postgres',
                'password': node.get('password') or cfg_json.get('password') or os.getenv('PGPASSWORD') or '',
                'sslmode': node.get('sslmode') or cfg_json.get('sslmode') or 'prefer',
                'application_name': node.get('application_name') or cfg_json.get('application_name') or 'gym_management_system',
                'connect_timeout': int(node.get('connect_timeout') or cfg_json.get('connect_timeout') or 10),
            }
            conn = psycopg2.connect(**alt_cfg)
            result["notes"].append("Conectado via config.json fallback")
        except Exception as e2:
            result["ok"] = False
            result["error"] = f"DB connect failed: {e}; fallback failed: {e2}"
            print(json.dumps(result, ensure_ascii=False))
            return

    try:
        cur = conn.cursor()
        cur.execute("SELECT valor FROM configuracion WHERE clave='owner_password'")
        row = cur.fetchone()
        db_val = row[0] if row else None
        result["db_query"] = db_val

        # Detect bcrypt format
        if isinstance(db_val, str) and db_val.startswith("$2") and len(db_val) >= 40:
            result["is_bcrypt"] = True

        # If env provided, verify it against stored value
        if env_pwd and db_val:
            try:
                result["env_verifies"] = SecurityUtils.verify_password(env_pwd, db_val)
            except Exception:
                # If not bcrypt, fallback to plaintext compare
                result["env_verifies"] = (env_pwd == db_val)

        cur.close()
        conn.close()
    except Exception as e:
        result["ok"] = False
        result["error"] = f"DB query failed: {e}"

    # Notes for diagnosis
    if not result["db_query"]:
        result["notes"].append("No existe owner_password en configuracion")
    elif not result["is_bcrypt"]:
        result["notes"].append("owner_password no parece hasheado (bcrypt)")
    else:
        result["notes"].append("owner_password hasheado correctamente")

    if result["env_present"] and not result["env_verifies"]:
        result["notes"].append("El valor en DB no verifica contra ENV actual")

    print(json.dumps(result, ensure_ascii=False))

if __name__ == "__main__":
    main()