import os
import logging
import re
import unicodedata
from typing import Any, Dict, List, Optional

import psycopg2
import psycopg2.extras

try:
    import requests  # type: ignore
except Exception:
    requests = None  # type: ignore

from core.database import DatabaseManager  # type: ignore
from core.security_utils import SecurityUtils  # type: ignore


def _resolve_admin_db_params() -> Dict[str, Any]:
    host = os.getenv("ADMIN_DB_HOST", os.getenv("DB_HOST", "localhost")).strip()
    try:
        port = int(os.getenv("ADMIN_DB_PORT", os.getenv("DB_PORT", 5432)))
    except Exception:
        port = 5432
    user = os.getenv("ADMIN_DB_USER", os.getenv("DB_USER", "postgres")).strip()
    password = os.getenv("ADMIN_DB_PASSWORD", os.getenv("DB_PASSWORD", ""))
    sslmode = os.getenv("ADMIN_DB_SSLMODE", os.getenv("DB_SSLMODE", "prefer")).strip()
    try:
        connect_timeout = int(os.getenv("ADMIN_DB_CONNECT_TIMEOUT", os.getenv("DB_CONNECT_TIMEOUT", 10)))
    except Exception:
        connect_timeout = 10
    application_name = os.getenv("ADMIN_DB_APPLICATION_NAME", os.getenv("DB_APPLICATION_NAME", "gym_management_admin")).strip()
    database = os.getenv("ADMIN_DB_NAME", "gymms_admin").strip()
    try:
        h = host.lower()
        if ("neon.tech" in h) or ("neon" in h):
            if not sslmode or sslmode.lower() in ("disable", "prefer"):
                sslmode = "require"
    except Exception:
        pass
    return {
        "host": host,
        "port": port,
        "database": database,
        "user": user,
        "password": password,
        "sslmode": sslmode,
        "connect_timeout": connect_timeout,
        "application_name": application_name,
    }


class AdminDatabaseManager:
    def __init__(self, connection_params: Dict[str, Any] | None = None):
        params = connection_params or _resolve_admin_db_params()
        created_admin_db = False
        try:
            try:
                if (os.getenv("NEON_API_TOKEN") or "").strip():
                    created_admin_db = bool(self._ensure_admin_database())
            except Exception:
                created_admin_db = False
            self.db = DatabaseManager(connection_params=params)  # type: ignore
            ok = DatabaseManager.test_connection(params=params, timeout_seconds=6)
            if not ok:
                try:
                    created_admin_db = bool(self._ensure_admin_database())
                except Exception:
                    created_admin_db = False
                self.db = DatabaseManager(connection_params=params)  # type: ignore
        except Exception:
            try:
                created_admin_db = bool(self._ensure_admin_database())
            except Exception:
                created_admin_db = False
            self.db = DatabaseManager(connection_params=params)  # type: ignore
        self._ensure_schema()
        try:
            if created_admin_db:
                try:
                    self.log_action("system", "bootstrap_admin_database", None, str(params.get("database") or "gymms_admin"))
                except Exception:
                    pass
            try:
                self.log_action("system", "bootstrap_admin_schema", None, None)
            except Exception:
                pass
        except Exception:
            pass
        self._ensure_owner_user()

    def _ensure_admin_database(self) -> bool:
        try:
            token = (os.getenv("NEON_API_TOKEN") or "").strip()
            if token and requests is not None:
                base = _resolve_admin_db_params()
                admin_db_name = (base.get("database") or "gymms_admin").strip()
                host = str(base.get("host") or "").strip().lower()
                comp_host = host.replace("-pooler.", ".")
                api = "https://console.neon.tech/api/v2"
                headers = {"Accept": "application/json", "Authorization": f"Bearer {token}", "Content-Type": "application/json"}
                project_id = (os.getenv("NEON_PROJECT_ID") or "").strip()
                branch_id = (os.getenv("NEON_BRANCH_ID") or "").strip()
                if project_id and branch_id:
                    lr = requests.get(f"{api}/projects/{project_id}/branches/{branch_id}/databases", headers=headers)
                    if lr.status_code != 200:
                        return False
                    dbs = (lr.json() or {}).get("databases") or []
                    for d in dbs:
                        if str(d.get("name") or "").strip().lower() == admin_db_name.lower():
                            return False
                    owner = os.getenv("ADMIN_DB_USER", os.getenv("DB_USER", "neondb_owner")).strip() or "neondb_owner"
                    cr = requests.post(f"{api}/projects/{project_id}/branches/{branch_id}/databases", headers=headers, json={"database": {"name": admin_db_name, "owner_name": owner}})
                    if 200 <= cr.status_code < 300:
                        return True
                    return False
                pr = requests.get(f"{api}/projects", headers=headers)
                if pr.status_code != 200:
                    return False
                pjs = (pr.json() or {}).get("projects") or []
                project_id = None
                branch_id = None
                for pj in pjs:
                    pid = pj.get("id")
                    if not pid:
                        continue
                    er = requests.get(f"{api}/projects/{pid}/endpoints", headers=headers)
                    if er.status_code != 200:
                        continue
                    eps = (er.json() or {}).get("endpoints") or []
                    for ep in eps:
                        h = str(ep.get("host") or "").strip().lower()
                        hp = h.replace("-pooler.", ".")
                        if h == host or hp == host or h == comp_host or hp == comp_host or host.startswith(h) or h.startswith(host):
                            project_id = pid
                            branch_id = ep.get("branch_id")
                            break
                    if project_id:
                        break
                if not project_id or not branch_id:
                    return False
                lr = requests.get(f"{api}/projects/{project_id}/branches/{branch_id}/databases", headers=headers)
                if lr.status_code != 200:
                    return False
                dbs = (lr.json() or {}).get("databases") or []
                for d in dbs:
                    if str(d.get("name") or "").strip().lower() == admin_db_name.lower():
                        return False
                owner = os.getenv("ADMIN_DB_USER", os.getenv("DB_USER", "neondb_owner")).strip() or "neondb_owner"
                cr = requests.post(f"{api}/projects/{project_id}/branches/{branch_id}/databases", headers=headers, json={"database": {"name": admin_db_name, "owner_name": owner}})
                if 200 <= cr.status_code < 300:
                    return True
                return False
            base = _resolve_admin_db_params()
            host = base.get("host")
            port = int(base.get("port") or 5432)
            user = base.get("user")
            password = base.get("password")
            sslmode = base.get("sslmode")
            try:
                connect_timeout = int(base.get("connect_timeout") or 10)
            except Exception:
                connect_timeout = 10
            appname = (base.get("application_name") or "gym_admin_bootstrap").strip()
            admin_db_name = (base.get("database") or "gymms_admin").strip()
            base_db = os.getenv("ADMIN_DB_BASE_NAME", "neondb").strip() or "neondb"
            conn = psycopg2.connect(host=host, port=port, dbname=base_db, user=user, password=password, sslmode=sslmode, connect_timeout=connect_timeout, application_name=appname)
            try:
                conn.autocommit = True
                cur = conn.cursor()
                cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (admin_db_name,))
                exists = bool(cur.fetchone())
                created = False
                if not exists:
                    try:
                        cur.execute(f"CREATE DATABASE {admin_db_name}")
                        created = True
                    except Exception:
                        created = False
                try:
                    cur.close()
                except Exception:
                    pass
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
            return created
        except Exception as e:
            logging.getLogger(__name__).error(str(e))
            return False

    def _ensure_schema(self) -> None:
        try:
            with self.db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor()
                cur.execute(
                    "CREATE TABLE IF NOT EXISTS gyms (id BIGSERIAL PRIMARY KEY, nombre TEXT NOT NULL, subdominio TEXT NOT NULL UNIQUE, db_name TEXT NOT NULL UNIQUE, b2_bucket_name TEXT, b2_bucket_id TEXT, whatsapp_phone_id TEXT, whatsapp_access_token TEXT, whatsapp_business_account_id TEXT, whatsapp_verify_token TEXT, whatsapp_app_secret TEXT, whatsapp_nonblocking BOOLEAN NOT NULL DEFAULT false, whatsapp_send_timeout_seconds NUMERIC(6,2) NULL, owner_phone TEXT, status TEXT NOT NULL DEFAULT 'active', hard_suspend BOOLEAN NOT NULL DEFAULT false, suspended_until TIMESTAMP WITHOUT TIME ZONE NULL, suspended_reason TEXT NULL, created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW())"
                )
                try:
                    cur.execute("ALTER TABLE gyms ADD COLUMN IF NOT EXISTS b2_key_id TEXT")
                except Exception:
                    pass
                try:
                    cur.execute("ALTER TABLE gyms ADD COLUMN IF NOT EXISTS b2_application_key TEXT")
                except Exception:
                    pass
                try:
                    cur.execute("ALTER TABLE gyms ADD COLUMN IF NOT EXISTS owner_password_hash TEXT")
                except Exception:
                    pass
                try:
                    cur.execute("ALTER TABLE gyms ADD COLUMN IF NOT EXISTS owner_phone TEXT")
                except Exception:
                    pass
                try:
                    cur.execute("ALTER TABLE gyms ADD COLUMN IF NOT EXISTS whatsapp_business_account_id TEXT")
                except Exception:
                    pass
                try:
                    cur.execute("ALTER TABLE gyms ADD COLUMN IF NOT EXISTS whatsapp_verify_token TEXT")
                except Exception:
                    pass
                try:
                    cur.execute("ALTER TABLE gyms ADD COLUMN IF NOT EXISTS whatsapp_app_secret TEXT")
                except Exception:
                    pass
                try:
                    cur.execute("ALTER TABLE gyms ADD COLUMN IF NOT EXISTS whatsapp_nonblocking BOOLEAN NOT NULL DEFAULT false")
                except Exception:
                    pass
                try:
                    cur.execute("ALTER TABLE gyms ADD COLUMN IF NOT EXISTS whatsapp_send_timeout_seconds NUMERIC(6,2) NULL")
                except Exception:
                    pass
                cur.execute(
                    "CREATE TABLE IF NOT EXISTS gym_payments (id BIGSERIAL PRIMARY KEY, gym_id BIGINT NOT NULL REFERENCES gyms(id) ON DELETE CASCADE, plan TEXT, amount NUMERIC(12,2), currency TEXT, paid_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(), valid_until TIMESTAMP WITHOUT TIME ZONE NULL, status TEXT, notes TEXT)"
                )
                cur.execute(
                    "CREATE TABLE IF NOT EXISTS admin_users (id BIGSERIAL PRIMARY KEY, username TEXT UNIQUE NOT NULL, password_hash TEXT NOT NULL, created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW())"
                )
                cur.execute(
                    "CREATE TABLE IF NOT EXISTS admin_audit (id BIGSERIAL PRIMARY KEY, actor_username TEXT, action TEXT NOT NULL, gym_id BIGINT NULL, details TEXT NULL, created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW())"
                )
                cur.execute(
                    "CREATE TABLE IF NOT EXISTS plans (id BIGSERIAL PRIMARY KEY, name TEXT UNIQUE NOT NULL, amount NUMERIC(12,2) NOT NULL, currency TEXT NOT NULL, period_days INTEGER NOT NULL, created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW())"
                )
                try:
                    cur.execute("ALTER TABLE plans ADD COLUMN IF NOT EXISTS active BOOLEAN NOT NULL DEFAULT true")
                except Exception:
                    pass
                cur.execute(
                    "CREATE TABLE IF NOT EXISTS gym_subscriptions (id BIGSERIAL PRIMARY KEY, gym_id BIGINT NOT NULL REFERENCES gyms(id) ON DELETE CASCADE, plan_id BIGINT NOT NULL REFERENCES plans(id) ON DELETE RESTRICT, start_date DATE NOT NULL, next_due_date DATE NOT NULL, status TEXT NOT NULL DEFAULT 'active', created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW())"
                )
                conn.commit()
        except Exception as e:
            logging.getLogger(__name__).error(str(e))

    def _hash_password(self, password: str) -> str:
        import secrets, hashlib, base64
        salt = secrets.token_bytes(16)
        dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 120000)
        return base64.b64encode(salt).decode("ascii") + ":" + base64.b64encode(dk).decode("ascii")

    def _verify_password(self, password: str, stored: str) -> bool:
        import hashlib, base64
        try:
            s, h = stored.split(":", 1)
            try:
                s = s.strip().strip('"').strip("'")
                h = h.strip().strip('"').strip("'")
            except Exception:
                pass
            salt = base64.b64decode(s.encode("ascii"))
            expected = base64.b64decode(h.encode("ascii"))
            dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 120000)
            return dk == expected
        except Exception:
            return False

    def _ensure_owner_user(self) -> None:
        try:
            pwd = os.getenv("ADMIN_INITIAL_PASSWORD", "").strip() or os.getenv("DEV_PASSWORD", "").strip()
            if not pwd:
                return
            with self.db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor()
                cur.execute("SELECT id FROM admin_users WHERE username = %s", ("owner",))
                row = cur.fetchone()
                if not row:
                    ph = self._hash_password(pwd)
                    cur.execute("INSERT INTO admin_users (username, password_hash) VALUES (%s, %s)", ("owner", ph))
                    conn.commit()
                    try:
                        self.log_action("system", "bootstrap_admin_owner_user", None, None)
                    except Exception:
                        pass
        except Exception:
            pass

    def verificar_owner_password(self, password: str) -> bool:
        try:
            with self.db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor()
                cur.execute("SELECT password_hash FROM admin_users WHERE username = %s", ("owner",))
                row = cur.fetchone()
                if not row:
                    return False
                try:
                    stored = str(row[0] or "").strip()
                except Exception:
                    stored = row[0]
                return self._verify_password(password, stored)
        except Exception:
            return False

    def set_admin_owner_password(self, new_password: str) -> bool:
        try:
            if not (new_password or "").strip():
                return False
            ph = self._hash_password(str(new_password).strip())
            with self.db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor()
                cur.execute("UPDATE admin_users SET password_hash = %s WHERE username = %s", (ph, "owner"))
                conn.commit()
            return True
        except Exception:
            return False

    def listar_gimnasios(self) -> List[Dict[str, Any]]:
        try:
            with self.db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur.execute("SELECT id, nombre, subdominio, db_name, owner_phone, status, hard_suspend, suspended_until, b2_bucket_name, b2_bucket_id, created_at FROM gyms ORDER BY id DESC")
                rows = cur.fetchall()
                return [dict(r) for r in rows]
        except Exception:
            return []

    def listar_gimnasios_avanzado(self, page: int, page_size: int, q: Optional[str], status: Optional[str], order_by: Optional[str], order_dir: Optional[str]) -> Dict[str, Any]:
        try:
            p = max(int(page or 1), 1)
            ps = max(int(page_size or 20), 1)
            allowed_cols = {"id", "nombre", "subdominio", "status", "created_at"}
            ob = (order_by or "id").strip().lower()
            if ob not in allowed_cols:
                ob = "id"
            od = (order_dir or "desc").strip().upper()
            if od not in {"ASC", "DESC"}:
                od = "DESC"
            where_terms: List[str] = []
            params: List[Any] = []
            qv = str(q or "").strip().lower()
            if qv:
                where_terms.append("(LOWER(nombre) LIKE %s OR LOWER(subdominio) LIKE %s)")
                like = f"%{qv}%"
                params.extend([like, like])
            sv = str(status or "").strip().lower()
            if sv:
                where_terms.append("LOWER(status) = %s")
                params.append(sv)
            where_sql = (" WHERE " + " AND ".join(where_terms)) if where_terms else ""
            with self.db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor()
                cur.execute(f"SELECT COUNT(*) FROM gyms{where_sql}", params)
                total_row = cur.fetchone()
                total = int(total_row[0]) if total_row else 0
            with self.db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur.execute(
                    f"SELECT id, nombre, subdominio, db_name, owner_phone, status, hard_suspend, suspended_until, b2_bucket_name, b2_bucket_id, created_at FROM gyms{where_sql} ORDER BY {ob} {od} LIMIT %s OFFSET %s",
                    params + [ps, (p - 1) * ps]
                )
                rows = cur.fetchall()
            return {"items": [dict(r) for r in rows], "total": total, "page": p, "page_size": ps}
        except Exception:
            return {"items": [], "total": 0, "page": 1, "page_size": int(page_size or 20)}

    def listar_gimnasios_con_resumen(self, page: int, page_size: int, q: Optional[str], status: Optional[str], order_by: Optional[str], order_dir: Optional[str]) -> Dict[str, Any]:
        try:
            p = max(int(page or 1), 1)
            ps = max(int(page_size or 20), 1)
            allowed_cols = {"id", "nombre", "subdominio", "status", "created_at"}
            ob = (order_by or "id").strip().lower()
            if ob not in allowed_cols:
                ob = "id"
            od = (order_dir or "desc").strip().upper()
            if od not in {"ASC", "DESC"}:
                od = "DESC"
            where_terms: List[str] = []
            params: List[Any] = []
            qv = str(q or "").strip().lower()
            if qv:
                where_terms.append("(LOWER(g.nombre) LIKE %s OR LOWER(g.subdominio) LIKE %s)")
                like = f"%{qv}%"
                params.extend([like, like])
            sv = str(status or "").strip().lower()
            if sv:
                where_terms.append("LOWER(g.status) = %s")
                params.append(sv)
            where_sql = (" WHERE " + " AND ".join(where_terms)) if where_terms else ""
            with self.db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor()
                cur.execute(f"SELECT COUNT(*) FROM gyms g{where_sql}", params)
                total_row = cur.fetchone()
                total = int(total_row[0]) if total_row else 0
            with self.db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur.execute(
                    f"""
                    SELECT g.id, g.nombre, g.subdominio, g.db_name, g.owner_phone, g.status, g.hard_suspend, g.suspended_until,
                           g.b2_bucket_name, g.b2_bucket_id, g.created_at,
                           gs.next_due_date, gs.status AS sub_status,
                           (SELECT amount FROM gym_payments WHERE gym_id = g.id ORDER BY paid_at DESC LIMIT 1) AS last_payment_amount,
                           (SELECT currency FROM gym_payments WHERE gym_id = g.id ORDER BY paid_at DESC LIMIT 1) AS last_payment_currency,
                           (SELECT paid_at FROM gym_payments WHERE gym_id = g.id ORDER BY paid_at DESC LIMIT 1) AS last_payment_at
                    FROM gyms g
                    LEFT JOIN gym_subscriptions gs ON gs.gym_id = g.id
                    {where_sql}
                    ORDER BY g.{ob} {od}
                    LIMIT %s OFFSET %s
                    """,
                    params + [ps, (p - 1) * ps]
                )
                rows = cur.fetchall()
            return {"items": [dict(r) for r in rows], "total": total, "page": p, "page_size": ps}
        except Exception:
            return {"items": [], "total": 0, "page": 1, "page_size": int(page_size or 20)}

    def obtener_gimnasio(self, gym_id: int) -> Optional[Dict[str, Any]]:
        try:
            with self.db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur.execute("SELECT * FROM gyms WHERE id = %s", (int(gym_id),))
                row = cur.fetchone()
                return dict(row) if row else None
        except Exception:
            return None

    def eliminar_gimnasio(self, gym_id: int) -> bool:
        try:
            try:
                self.eliminar_bucket_gym(int(gym_id))
            except Exception:
                pass
            with self.db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor()
                cur.execute("DELETE FROM gyms WHERE id = %s", (int(gym_id),))
                conn.commit()
                return True
        except Exception:
            return False

    def set_estado_gimnasio(self, gym_id: int, status: str, hard_suspend: bool = False, suspended_until: Optional[str] = None, reason: Optional[str] = None) -> bool:
        try:
            with self.db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor()
                cur.execute("UPDATE gyms SET status = %s, hard_suspend = %s, suspended_until = %s, suspended_reason = %s WHERE id = %s", (status, bool(hard_suspend), suspended_until, reason, int(gym_id)))
                conn.commit()
                return True
        except Exception:
            return False

    def is_gym_suspended(self, subdominio: str) -> bool:
        try:
            with self.db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor()
                cur.execute("SELECT status, hard_suspend, suspended_until FROM gyms WHERE subdominio = %s", (subdominio.strip().lower(),))
                row = cur.fetchone()
                if not row:
                    return False
                status, hard_s, until = row[0], row[1], row[2]
                if hard_s:
                    return True
                if str(status or "").lower() == "suspended":
                    if until is None:
                        return True
                    try:
                        from datetime import datetime
                        return datetime.utcnow() <= until
                    except Exception:
                        return True
                try:
                    cur.execute("SELECT valid_until FROM gym_payments gp JOIN gyms g ON gp.gym_id = g.id WHERE g.subdominio = %s ORDER BY gp.paid_at DESC LIMIT 1", (subdominio.strip().lower(),))
                    prow = cur.fetchone()
                    if not prow:
                        return False
                    vu = prow[0]
                    if vu is None:
                        return False
                    from datetime import datetime
                    return datetime.utcnow() > vu
                except Exception:
                    return False
        except Exception:
            return False

    def set_mantenimiento(self, gym_id: int, message: Optional[str]) -> bool:
        try:
            with self.db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor()
                cur.execute("UPDATE gyms SET status = %s, hard_suspend = false, suspended_until = NULL, suspended_reason = %s WHERE id = %s", ("maintenance", message, int(gym_id)))
                conn.commit()
                return True
        except Exception:
            return False

    def clear_mantenimiento(self, gym_id: int) -> bool:
        try:
            with self.db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor()
                cur.execute("UPDATE gyms SET status = %s, suspended_reason = NULL WHERE id = %s", ("active", int(gym_id)))
                conn.commit()
                return True
        except Exception:
            return False

    def get_mantenimiento(self, subdominio: str) -> Optional[str]:
        try:
            with self.db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor()
                cur.execute("SELECT suspended_reason FROM gyms WHERE subdominio = %s AND status = 'maintenance'", (subdominio.strip().lower(),))
                row = cur.fetchone()
                return row[0] if row else None
        except Exception:
            return None

    def registrar_pago(self, gym_id: int, plan: Optional[str], amount: Optional[float], currency: Optional[str], valid_until: Optional[str], status: Optional[str], notes: Optional[str]) -> bool:
        try:
            with self.db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor()
                cur.execute("INSERT INTO gym_payments (gym_id, plan, amount, currency, valid_until, status, notes) VALUES (%s, %s, %s, %s, %s, %s, %s)", (int(gym_id), plan, amount, currency, valid_until, status, notes))
                conn.commit()
                return True
        except Exception:
            return False

    def listar_pagos(self, gym_id: int) -> List[Dict[str, Any]]:
        try:
            with self.db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur.execute("SELECT id, plan, amount, currency, paid_at, valid_until, status, notes FROM gym_payments WHERE gym_id = %s ORDER BY paid_at DESC", (int(gym_id),))
                rows = cur.fetchall()
                return [dict(r) for r in rows]
        except Exception:
            return []

    def crear_gimnasio(self, nombre: str, subdominio: str, whatsapp_phone_id: str | None = None, whatsapp_access_token: str | None = None, owner_phone: str | None = None, whatsapp_business_account_id: str | None = None, whatsapp_verify_token: str | None = None, whatsapp_app_secret: str | None = None, whatsapp_nonblocking: bool | None = None, whatsapp_send_timeout_seconds: float | None = None) -> Dict[str, Any]:
        try:
            self._ensure_schema()
        except Exception:
            pass
        sub = subdominio.strip().lower()
        suffix = os.getenv("TENANT_DB_SUFFIX", "_db")
        db_name = f"{sub}{suffix}"
        bucket_prefix = os.getenv("B2_BUCKET_PREFIX", "motiona-assets")
        bucket_name = f"{bucket_prefix}-{sub}"
        created_db = False
        bucket_info = {"bucket_name": bucket_name, "bucket_id": None, "key_id": None, "application_key": None}
        try:
            created_db = bool(self._crear_db_postgres(db_name))
        except Exception:
            created_db = False
        try:
            bucket_info = self._crear_bucket_b2(bucket_name)
        except Exception:
            bucket_info = {"bucket_name": bucket_name, "bucket_id": None, "key_id": None, "application_key": None}
        try:
            with self.db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor()
                cur.execute("INSERT INTO gyms (nombre, subdominio, db_name, b2_bucket_name, b2_bucket_id, b2_key_id, b2_application_key, whatsapp_phone_id, whatsapp_access_token, whatsapp_business_account_id, whatsapp_verify_token, whatsapp_app_secret, whatsapp_nonblocking, whatsapp_send_timeout_seconds, owner_phone) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id", (nombre.strip(), sub, db_name, bucket_info.get("bucket_name") or bucket_name, bucket_info.get("bucket_id") or None, bucket_info.get("key_id") or None, bucket_info.get("application_key") or None, (whatsapp_phone_id or "").strip() or None, (whatsapp_access_token or "").strip() or None, (whatsapp_business_account_id or "").strip() or None, (whatsapp_verify_token or "").strip() or None, (whatsapp_app_secret or "").strip() or None, bool(whatsapp_nonblocking or False), whatsapp_send_timeout_seconds, (owner_phone or "").strip() or None))
                rid = cur.fetchone()[0]
                conn.commit()
                try:
                    if created_db:
                        base = _resolve_admin_db_params()
                        params = dict(base)
                        params["database"] = db_name
                        dm = DatabaseManager(connection_params=params)  # type: ignore
                        try:
                            dm.inicializar_base_datos()
                        except Exception:
                            pass
                except Exception:
                    pass
                try:
                    if (whatsapp_phone_id or whatsapp_access_token or whatsapp_business_account_id or whatsapp_verify_token or whatsapp_app_secret):
                        self._push_whatsapp_to_gym_db(int(rid))
                except Exception:
                    pass
                return {"id": int(rid), "nombre": nombre.strip(), "subdominio": sub, "db_name": db_name, "db_created": bool(created_db), "b2_bucket_name": bucket_info.get("bucket_name") or bucket_name, "b2_bucket_id": bucket_info.get("bucket_id") or None}
        except Exception as e:
            logging.getLogger(__name__).error(str(e))
            return {"error": str(e)}

    def subdominio_disponible(self, subdominio: str) -> bool:
        try:
            s = str(subdominio or "").strip().lower()
            if not s:
                return False
            with self.db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor()
                cur.execute("SELECT 1 FROM gyms WHERE subdominio = %s", (s,))
                row = cur.fetchone()
                return not bool(row)
        except Exception:
            return False

    def _slugify(self, value: str) -> str:
        v = str(value or "").strip().lower()
        if not v:
            return ""
        nf = unicodedata.normalize("NFKD", v)
        ascii_v = nf.encode("ascii", "ignore").decode("ascii")
        ascii_v = re.sub(r"[^a-z0-9]+", "-", ascii_v)
        ascii_v = re.sub(r"-+", "-", ascii_v)
        ascii_v = ascii_v.strip("-")
        return ascii_v

    def sugerir_subdominio_unico(self, nombre_base: str) -> str:
        base = self._slugify(nombre_base)
        if not base:
            base = "gym"
        cur = base
        if self.subdominio_disponible(cur):
            return cur
        i = 1
        while i < 1000:
            cand = f"{base}-{i}"
            if self.subdominio_disponible(cand):
                return cand
            i += 1
        return f"{base}-{int(os.urandom(2).hex(), 16)}"

    def actualizar_gimnasio(self, gym_id: int, nombre: Optional[str], subdominio: Optional[str]) -> Dict[str, Any]:
        try:
            gid = int(gym_id)
            nm = (nombre or "").strip()
            sd = (subdominio or "").strip().lower()
            sets: List[str] = []
            params: List[Any] = []
            if nm:
                sets.append("nombre = %s")
                params.append(nm)
            if sd:
                with self.db.get_connection_context() as conn:  # type: ignore
                    cur = conn.cursor()
                    cur.execute("SELECT 1 FROM gyms WHERE subdominio = %s AND id <> %s", (sd, gid))
                    if cur.fetchone():
                        return {"ok": False, "error": "subdominio_in_use"}
                sets.append("subdominio = %s")
                params.append(sd)
            if not sets:
                return {"ok": False, "error": "no_fields"}
            with self.db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor()
                sql = f"UPDATE gyms SET {', '.join(sets)} WHERE id = %s"
                params.append(gid)
                cur.execute(sql, params)
                conn.commit()
            return {"ok": True}
        except Exception as e:
            logging.getLogger(__name__).error(str(e))
            return {"ok": False, "error": str(e)}

    def _crear_db_postgres(self, db_name: str) -> bool:
        try:
            name = str(db_name or "").strip()
            if not name:
                return False
            token = (os.getenv("NEON_API_TOKEN") or "").strip()
            if token and requests is not None:
                base = _resolve_admin_db_params()
                host = str(base.get("host") or "").strip().lower()
                comp_host = host.replace("-pooler.", ".")
                api = "https://console.neon.tech/api/v2"
                headers = {"Accept": "application/json", "Authorization": f"Bearer {token}", "Content-Type": "application/json"}
                project_id = (os.getenv("NEON_PROJECT_ID") or "").strip()
                branch_id = (os.getenv("NEON_BRANCH_ID") or "").strip()
                if project_id and branch_id:
                    lr = requests.get(f"{api}/projects/{project_id}/branches/{branch_id}/databases", headers=headers, timeout=10)
                    if lr.status_code != 200:
                        return False
                    dbs = (lr.json() or {}).get("databases") or []
                    for d in dbs:
                        if str(d.get("name") or "").strip().lower() == name.lower():
                            params = _resolve_admin_db_params()
                            params["database"] = name
                            dm = DatabaseManager(connection_params=params)  # type: ignore
                            try:
                                dm.inicializar_base_datos()
                            except Exception:
                                pass
                            return True
                    owner = os.getenv("ADMIN_DB_USER", os.getenv("DB_USER", "neondb_owner")).strip() or "neondb_owner"
                    cr = requests.post(f"{api}/projects/{project_id}/branches/{branch_id}/databases", headers=headers, json={"database": {"name": name, "owner_name": owner}}, timeout=12)
                    if not (200 <= cr.status_code < 300):
                        return False
                    params = _resolve_admin_db_params()
                    params["database"] = name
                    dm = DatabaseManager(connection_params=params)  # type: ignore
                    try:
                        dm.inicializar_base_datos()
                    except Exception:
                        pass
                    return True
                pr = requests.get(f"{api}/projects", headers=headers, timeout=10)
                if pr.status_code != 200:
                    return False
                pjs = (pr.json() or {}).get("projects") or []
                project_id = None
                branch_id = None
                for pj in pjs:
                    pid = pj.get("id")
                    if not pid:
                        continue
                    er = requests.get(f"{api}/projects/{pid}/endpoints", headers=headers, timeout=10)
                    if er.status_code != 200:
                        continue
                    eps = (er.json() or {}).get("endpoints") or []
                    for ep in eps:
                        h = str(ep.get("host") or "").strip().lower()
                        hp = h.replace("-pooler.", ".")
                        if h == host or hp == host or h == comp_host or hp == comp_host or host.startswith(h) or h.startswith(host):
                            project_id = pid
                            branch_id = ep.get("branch_id")
                            break
                    if project_id:
                        break
                if not project_id or not branch_id:
                    return False
                lr = requests.get(f"{api}/projects/{project_id}/branches/{branch_id}/databases", headers=headers, timeout=10)
                if lr.status_code != 200:
                    return False
                dbs = (lr.json() or {}).get("databases") or []
                for d in dbs:
                    if str(d.get("name") or "").strip().lower() == name.lower():
                        params = _resolve_admin_db_params()
                        params["database"] = name
                        dm = DatabaseManager(connection_params=params)  # type: ignore
                        try:
                            dm.inicializar_base_datos()
                        except Exception:
                            pass
                        return True
                owner = os.getenv("ADMIN_DB_USER", os.getenv("DB_USER", "neondb_owner")).strip() or "neondb_owner"
                cr = requests.post(f"{api}/projects/{project_id}/branches/{branch_id}/databases", headers=headers, json={"database": {"name": name, "owner_name": owner}}, timeout=12)
                if not (200 <= cr.status_code < 300):
                    return False
                params = _resolve_admin_db_params()
                params["database"] = name
                dm = DatabaseManager(connection_params=params)  # type: ignore
                try:
                    dm.inicializar_base_datos()
                except Exception:
                    pass
                return True
            base = _resolve_admin_db_params()
            host = base.get("host")
            port = int(base.get("port") or 5432)
            user = base.get("user")
            password = base.get("password")
            sslmode = base.get("sslmode")
            connect_timeout = int(base.get("connect_timeout") or 10)
            application_name = (base.get("application_name") or "gym_admin_provisioner").strip()
            conn = psycopg2.connect(host=host, port=port, dbname=base.get("database"), user=user, password=password, sslmode=sslmode, connect_timeout=connect_timeout, application_name=application_name)
            try:
                conn.autocommit = True
                cur = conn.cursor()
                cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,))
                exists = bool(cur.fetchone())
                if not exists:
                    try:
                        cur.execute(f"CREATE DATABASE {name}")
                    except Exception:
                        pass
                try:
                    cur.close()
                except Exception:
                    pass
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
            params = dict(base)
            params["database"] = name
            try:
                dm = DatabaseManager(connection_params=params)  # type: ignore
                dm.inicializar_base_datos()
                return True
            except Exception:
                return False
        except Exception:
            return False

    def _b2_authorize_master(self) -> Dict[str, Any]:
        try:
            acc = (os.getenv("B2_MASTER_ACCOUNT_ID") or "").strip()
            key = (os.getenv("B2_MASTER_APPLICATION_KEY") or "").strip()
            if not acc or not key:
                return {}
            r = requests.get("https://api.backblazeb2.com/b2api/v2/b2_authorize_account", auth=(acc, key), timeout=10)
            if r.status_code != 200:
                return {}
            return r.json() or {}
        except Exception:
            return {}

    def _crear_bucket_b2(self, bucket_name: str) -> Dict[str, Any]:
        try:
            name = str(bucket_name or "").strip()
            if not name:
                return {"bucket_name": "", "bucket_id": None}
            if requests is None:
                return {"bucket_name": name, "bucket_id": None}
            auth = self._b2_authorize_master()
            api_url = str(auth.get("apiUrl") or "").strip()
            token = str(auth.get("authorizationToken") or "").strip()
            account_id = (os.getenv("B2_MASTER_ACCOUNT_ID") or "").strip()
            if not api_url or not token or not account_id:
                return {"bucket_name": name, "bucket_id": None}
            headers = {"Authorization": token}
            lb = requests.post(f"{api_url}/b2api/v2/b2_list_buckets", headers=headers, json={"accountId": account_id}, timeout=10)
            bucket_id = None
            bucket_name = name
            if lb.status_code == 200:
                data = lb.json()
                for b in data.get("buckets", []) or []:
                    try:
                        if str(b.get("bucketName") or "").strip().lower() == name.lower():
                            bucket_id = b.get("bucketId")
                            bucket_name = b.get("bucketName")
                            break
                    except Exception:
                        continue
            if not bucket_id:
                btype = "allPublic"
                try:
                    priv = (os.getenv("B2_BUCKET_PRIVATE", "0").strip().lower() in ("1", "true", "yes"))
                    btype = "allPrivate" if priv else "allPublic"
                except Exception:
                    btype = "allPublic"
                cb = requests.post(f"{api_url}/b2api/v2/b2_create_bucket", headers=headers, json={"accountId": account_id, "bucketName": name, "bucketType": btype}, timeout=12)
                if cb.status_code == 200:
                    bj = cb.json()
                    bucket_name = bj.get("bucketName")
                    bucket_id = bj.get("bucketId")
            key_id = None
            application_key = None
            if bucket_id:
                key_name = f"gym-{name}"
                caps = ["listFiles", "readFiles", "writeFiles", "deleteFiles", "listBuckets", "readBuckets"]
                ck = requests.post(f"{api_url}/b2api/v2/b2_create_key", headers=headers, json={"accountId": account_id, "capabilities": caps, "keyName": key_name, "bucketId": bucket_id}, timeout=12)
                if ck.status_code == 200:
                    kj = ck.json()
                    key_id = kj.get("applicationKeyId")
                    application_key = kj.get("applicationKey")
            return {"bucket_name": bucket_name, "bucket_id": bucket_id, "key_id": key_id, "application_key": application_key}
        except Exception as e:
            logging.getLogger(__name__).error(str(e))
            return {"bucket_name": bucket_name, "bucket_id": None}

    def provisionar_recursos(self, gym_id: int) -> Dict[str, Any]:
        try:
            with self.db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur.execute("SELECT id, subdominio, db_name, b2_bucket_name FROM gyms WHERE id = %s", (int(gym_id),))
                row = cur.fetchone()
            if not row:
                return {"ok": False, "error": "gym_not_found"}
            sub = str(row.get("subdominio") or "").strip().lower()
            if not sub:
                return {"ok": False, "error": "invalid_subdomain"}
            db_name = str(row.get("db_name") or "").strip() or f"{sub}{os.getenv('TENANT_DB_SUFFIX', '_db')}"
            bucket_name = str(row.get("b2_bucket_name") or "").strip() or f"{os.getenv('B2_BUCKET_PREFIX', 'motiona-assets')}-{sub}"
            created_db = self._crear_db_postgres(db_name)
            bucket_info = self._crear_bucket_b2(bucket_name)
            with self.db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor()
                cur.execute("UPDATE gyms SET db_name = %s, b2_bucket_name = %s, b2_bucket_id = %s, b2_key_id = %s, b2_application_key = %s WHERE id = %s", (db_name, bucket_info.get("bucket_name") or bucket_name, bucket_info.get("bucket_id") or None, bucket_info.get("key_id") or None, bucket_info.get("application_key") or None, int(gym_id)))
                conn.commit()
            return {"ok": True, "db_created": bool(created_db), "bucket": bucket_info}
        except Exception as e:
            logging.getLogger(__name__).error(str(e))
            return {"ok": False, "error": str(e)}

    def set_gym_owner_phone(self, gym_id: int, owner_phone: Optional[str]) -> bool:
        try:
            with self.db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor()
                cur.execute("UPDATE gyms SET owner_phone = %s WHERE id = %s", ((owner_phone or "").strip() or None, int(gym_id)))
                conn.commit()
                return True
        except Exception:
            return False

    def set_gym_branding(self, gym_id: int, gym_name: Optional[str], gym_address: Optional[str], logo_url: Optional[str]) -> bool:
        try:
            with self.db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur.execute("SELECT db_name FROM gyms WHERE id = %s", (int(gym_id),))
                row = cur.fetchone()
            if not row:
                return False
            db_name = str(row.get("db_name") or "").strip()
            if not db_name:
                return False
            base = _resolve_admin_db_params()
            params = dict(base)
            params["database"] = db_name
            dm = DatabaseManager(connection_params=params)  # type: ignore
            data = {}
            if (gym_name or "").strip():
                data["gym_name"] = str(gym_name).strip()
            if (gym_address or "").strip():
                data["gym_address"] = str(gym_address).strip()
            ok1 = True
            if data:
                ok1 = dm.actualizar_configuracion_gimnasio(data)
            ok2 = True
            if (logo_url or "").strip():
                ok2 = dm.actualizar_configuracion_logo(str(logo_url).strip()) if hasattr(dm, 'actualizar_configuracion_logo') else dm.actualizar_configuracion_gimnasio({'logo_url': str(logo_url).strip()})
            return bool(ok1 and ok2)
        except Exception:
            return False

    def set_gym_theme(self, gym_id: int, theme: Dict[str, Optional[str]], font_base: Optional[str], font_heading: Optional[str]) -> bool:
        try:
            with self.db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur.execute("SELECT db_name FROM gyms WHERE id = %s", (int(gym_id),))
                row = cur.fetchone()
            if not row:
                return False
            db_name = str(row.get("db_name") or "").strip()
            if not db_name:
                return False
            base = _resolve_admin_db_params()
            params = dict(base)
            params["database"] = db_name
            dm = DatabaseManager(connection_params=params)  # type: ignore
            ok = True
            mapping = {
                "primary": "theme_primary",
                "secondary": "theme_secondary",
                "accent": "theme_accent",
                "bg": "theme_bg",
                "card": "theme_card",
                "text": "theme_text",
                "muted": "theme_muted",
                "border": "theme_border",
            }
            for k, cfgk in mapping.items():
                v = str((theme.get(k) or "").strip())
                if v and re.match(r"^#([0-9a-fA-F]{6}|[0-9a-fA-F]{3})$", v):
                    try:
                        if not dm.actualizar_configuracion(cfgk, v):
                            ok = False
                    except Exception:
                        ok = False
            fb = str((font_base or "").strip())
            if fb and len(fb) <= 128:
                try:
                    if not dm.actualizar_configuracion("font_base", fb):
                        ok = False
                except Exception:
                    ok = False
            fh = str((font_heading or "").strip())
            if fh and len(fh) <= 128:
                try:
                    if not dm.actualizar_configuracion("font_heading", fh):
                        ok = False
                except Exception:
                    ok = False
            return bool(ok)
        except Exception:
            return False

    def set_gym_owner_password(self, gym_id: int, new_password: Optional[str]) -> bool:
        try:
            pwd = str(new_password or "").strip()
            if not pwd:
                return False
            hashed = SecurityUtils.hash_password(pwd)
            with self.db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor()
                cur.execute("UPDATE gyms SET owner_password_hash = %s WHERE id = %s", (hashed, int(gym_id)))
                conn.commit()
            try:
                with self.db.get_connection_context() as conn:  # type: ignore
                    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                    cur.execute("SELECT db_name FROM gyms WHERE id = %s", (int(gym_id),))
                    row = cur.fetchone()
                if row:
                    db_name = str(row.get("db_name") or "").strip()
                    if db_name:
                        base = _resolve_admin_db_params()
                        params = dict(base)
                        params["database"] = db_name
                        dm = DatabaseManager(connection_params=params)  # type: ignore
                        try:
                            dm.actualizar_configuracion("owner_password", hashed)
                        except Exception:
                            pass
            except Exception:
                pass
            return True
        except Exception:
            return False

    def eliminar_bucket_gym(self, gym_id: int) -> bool:
        try:
            with self.db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor()
                cur.execute("SELECT b2_bucket_id, b2_key_id FROM gyms WHERE id = %s", (int(gym_id),))
                row = cur.fetchone()
                if not row:
                    return False
                bid = row[0]
                kid = row[1]
            if kid:
                try:
                    self._b2_delete_key(kid)
                except Exception:
                    pass
            ok = False
            if bid:
                ok = self._b2_delete_bucket(bid)
            try:
                with self.db.get_connection_context() as conn:  # type: ignore
                    cur = conn.cursor()
                    cur.execute("UPDATE gyms SET b2_bucket_id = NULL, b2_bucket_name = NULL, b2_key_id = NULL, b2_application_key = NULL WHERE id = %s", (int(gym_id),))
                    conn.commit()
            except Exception:
                pass
            return bool(ok)
        except Exception:
            return False

    def regenerar_clave_b2(self, gym_id: int) -> Dict[str, Any]:
        try:
            with self.db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor()
                cur.execute("SELECT b2_bucket_id FROM gyms WHERE id = %s", (int(gym_id),))
                row = cur.fetchone()
                if not row:
                    return {"ok": False}
                bid = str(row[0] or "").strip()
            if not bid:
                return {"ok": False}
            auth = self._b2_authorize_master()
            api_url = str(auth.get("apiUrl") or "").strip()
            token = str(auth.get("authorizationToken") or "").strip()
            account_id = (os.getenv("B2_MASTER_ACCOUNT_ID") or "").strip()
            if not api_url or not token or not account_id:
                return {"ok": False}
            headers = {"Authorization": token}
            key_name = f"gym-regenerated-{gym_id}"
            caps = ["listFiles", "readFiles", "writeFiles", "deleteFiles", "listBuckets", "readBuckets"]
            ck = requests.post(f"{api_url}/b2api/v2/b2_create_key", headers=headers, json={"accountId": account_id, "capabilities": caps, "keyName": key_name, "bucketId": bid})
            if ck.status_code != 200:
                return {"ok": False}
            kj = ck.json()
            key_id = kj.get("applicationKeyId")
            application_key = kj.get("applicationKey")
            with self.db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor()
                cur.execute("UPDATE gyms SET b2_key_id = %s, b2_application_key = %s WHERE id = %s", (key_id, application_key, int(gym_id)))
                conn.commit()
            return {"ok": True, "key_id": key_id, "application_key": application_key}
        except Exception:
            return {"ok": False}

    def set_gym_whatsapp_config(self, gym_id: int, phone_id: Optional[str], access_token: Optional[str], waba_id: Optional[str], verify_token: Optional[str], app_secret: Optional[str], nonblocking: Optional[bool], send_timeout_seconds: Optional[float]) -> bool:
        try:
            with self.db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor()
                cur.execute(
                    "UPDATE gyms SET whatsapp_phone_id = %s, whatsapp_access_token = %s, whatsapp_business_account_id = %s, whatsapp_verify_token = %s, whatsapp_app_secret = %s, whatsapp_nonblocking = %s, whatsapp_send_timeout_seconds = %s WHERE id = %s",
                    (
                        (phone_id or "").strip() or None,
                        (access_token or "").strip() or None,
                        (waba_id or "").strip() or None,
                        (verify_token or "").strip() or None,
                        (app_secret or "").strip() or None,
                        bool(nonblocking or False),
                        send_timeout_seconds,
                        int(gym_id),
                    ),
                )
                conn.commit()
            try:
                self._push_whatsapp_to_gym_db(int(gym_id))
            except Exception:
                pass
            return True
        except Exception:
            return False

    def _push_whatsapp_to_gym_db(self, gym_id: int) -> bool:
        try:
            # Obtener datos del gimnasio y parámetros base de conexión
            with self.db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur.execute("SELECT db_name, whatsapp_phone_id, whatsapp_access_token, whatsapp_business_account_id, whatsapp_verify_token, whatsapp_app_secret FROM gyms WHERE id = %s", (int(gym_id),))
                row = cur.fetchone()
            if not row:
                return False
            db_name = str(row.get("db_name") or "").strip()
            if not db_name:
                return False
            params = _resolve_admin_db_params()
            params = dict(params)
            params["database"] = db_name
            try:
                dm = DatabaseManager(connection_params=params)  # type: ignore
            except Exception:
                return False
            # Actualizar configuración en DB del gimnasio
            ok1 = dm.actualizar_configuracion_whatsapp(
                phone_id=str(row.get("whatsapp_phone_id") or "") or None,
                waba_id=str(row.get("whatsapp_business_account_id") or "") or None,
                access_token=str(row.get("whatsapp_access_token") or "") or None,
            )
            # Opcional: tokens de verificación en configuracion genérica
            vt = str(row.get("whatsapp_verify_token") or "")
            asct = str(row.get("whatsapp_app_secret") or "")
            if vt:
                try:
                    dm.actualizar_configuracion("WHATSAPP_VERIFY_TOKEN", vt)
                except Exception:
                    pass
            if asct:
                try:
                    dm.actualizar_configuracion("WHATSAPP_APP_SECRET", asct)
                except Exception:
                    pass
            return bool(ok1)
        except Exception:
            return False

    def log_action(self, actor_username: Optional[str], action: str, gym_id: Optional[int], details: Optional[str]) -> bool:
        try:
            with self.db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor()
                cur.execute("INSERT INTO admin_audit (actor_username, action, gym_id, details) VALUES (%s, %s, %s, %s)", (actor_username, action, gym_id, details))
                conn.commit()
                return True
        except Exception:
            return False

    def crear_plan(self, name: str, amount: float, currency: str, period_days: int) -> bool:
        try:
            with self.db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor()
                cur.execute("INSERT INTO plans (name, amount, currency, period_days) VALUES (%s, %s, %s, %s)", (name.strip(), float(amount), currency.strip(), int(period_days)))
                conn.commit()
                return True
        except Exception:
            return False

    def listar_planes(self) -> List[Dict[str, Any]]:
        try:
            with self.db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur.execute("SELECT id, name, amount, currency, period_days, active FROM plans ORDER BY id DESC")
                rows = cur.fetchall()
                return [dict(r) for r in rows]
        except Exception:
            return []

    def actualizar_plan(self, plan_id: int, name: Optional[str], amount: Optional[float], currency: Optional[str], period_days: Optional[int]) -> bool:
        try:
            fields: List[str] = []
            params: List[Any] = []
            if (name or "").strip():
                fields.append("name = %s")
                params.append(str(name).strip())
            if amount is not None:
                try:
                    a = float(amount)
                except Exception:
                    a = -1.0
                if a > 0:
                    fields.append("amount = %s")
                    params.append(a)
            if (currency or "").strip():
                c = str(currency).strip()
                fields.append("currency = %s")
                params.append(c)
            if period_days is not None:
                try:
                    pd = int(period_days)
                except Exception:
                    pd = -1
                if pd > 0:
                    fields.append("period_days = %s")
                    params.append(pd)
            if not fields:
                return False
            params.append(int(plan_id))
            with self.db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor()
                cur.execute(f"UPDATE plans SET {', '.join(fields)} WHERE id = %s", params)
                conn.commit()
                return True
        except Exception:
            return False

    def toggle_plan(self, plan_id: int, active: bool) -> bool:
        try:
            with self.db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor()
                cur.execute("UPDATE plans SET active = %s WHERE id = %s", (bool(active), int(plan_id)))
                conn.commit()
                return True
        except Exception:
            return False

    def listar_auditoria(self, actor: Optional[str], action: Optional[str], gym_id: Optional[int], from_date: Optional[str], to_date: Optional[str], page: int, page_size: int) -> Dict[str, Any]:
        try:
            p = max(int(page or 1), 1)
            ps = max(int(page_size or 50), 1)
            terms: List[str] = []
            params: List[Any] = []
            if (actor or "").strip():
                terms.append("LOWER(actor_username) = LOWER(%s)")
                params.append(str(actor).strip())
            if (action or "").strip():
                terms.append("LOWER(action) = LOWER(%s)")
                params.append(str(action).strip())
            if gym_id is not None:
                terms.append("gym_id = %s")
                params.append(int(gym_id))
            if (from_date or "").strip():
                terms.append("created_at >= %s")
                params.append(str(from_date).strip())
            if (to_date or "").strip():
                terms.append("created_at <= %s")
                params.append(str(to_date).strip())
            where_sql = (" WHERE " + " AND ".join(terms)) if terms else ""
            with self.db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor()
                cur.execute(f"SELECT COUNT(*) FROM admin_audit{where_sql}", params)
                tot_row = cur.fetchone()
                total = int(tot_row[0]) if tot_row else 0
            with self.db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur.execute(
                    f"SELECT id, actor_username, action, gym_id, details, created_at FROM admin_audit{where_sql} ORDER BY id DESC LIMIT %s OFFSET %s",
                    params + [ps, (p - 1) * ps]
                )
                rows = cur.fetchall()
            return {"items": [dict(r) for r in rows], "total": total, "page": p, "page_size": ps}
        except Exception:
            return {"items": [], "total": 0, "page": 1, "page_size": int(page_size or 50)}

    def resumen_auditoria(self, last_days: int = 7) -> Dict[str, Any]:
        try:
            d = max(int(last_days or 7), 1)
            with self.db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur.execute(
                    "SELECT action, COUNT(*) AS c FROM admin_audit WHERE created_at >= (CURRENT_DATE - (%s || ' days')::interval) GROUP BY action ORDER BY c DESC",
                    (d,)
                )
                by_action = [dict(r) for r in cur.fetchall()]
                cur.execute(
                    "SELECT COALESCE(actor_username,'') AS actor_username, COUNT(*) AS c FROM admin_audit WHERE created_at >= (CURRENT_DATE - (%s || ' days')::interval) GROUP BY actor_username ORDER BY c DESC",
                    (d,)
                )
                by_actor = [dict(r) for r in cur.fetchall()]
            return {"by_action": by_action, "by_actor": by_actor, "days": d}
        except Exception:
            return {"by_action": [], "by_actor": [], "days": int(last_days or 7)}

    def obtener_metricas_agregadas(self) -> Dict[str, Any]:
        try:
            with self.db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM gyms")
                total_gyms = int((cur.fetchone() or [0])[0])
                cur.execute("SELECT COUNT(*) FROM gyms WHERE status = 'active'")
                active_gyms = int((cur.fetchone() or [0])[0])
                cur.execute("SELECT COUNT(*) FROM gyms WHERE status = 'suspended'")
                suspended_gyms = int((cur.fetchone() or [0])[0])
                cur.execute("SELECT COUNT(*) FROM gyms WHERE status = 'maintenance'")
                maintenance_gyms = int((cur.fetchone() or [0])[0])
                cur.execute("SELECT COUNT(*) FROM gyms WHERE created_at >= (CURRENT_DATE - INTERVAL '7 days')")
                gyms_last_7 = int((cur.fetchone() or [0])[0])
                cur.execute("SELECT COUNT(*) FROM gyms WHERE created_at >= (CURRENT_DATE - INTERVAL '30 days')")
                gyms_last_30 = int((cur.fetchone() or [0])[0])
                cur.execute("SELECT COUNT(*) FROM gyms WHERE whatsapp_phone_id IS NOT NULL AND whatsapp_access_token IS NOT NULL")
                whatsapp_cfg = int((cur.fetchone() or [0])[0])
                cur.execute("SELECT COUNT(*) FROM gyms WHERE b2_bucket_name IS NOT NULL AND b2_bucket_id IS NOT NULL")
                storage_cfg = int((cur.fetchone() or [0])[0])
                cur.execute("SELECT COUNT(*) FROM gym_subscriptions WHERE status = 'overdue'")
                overdue_subs = int((cur.fetchone() or [0])[0])
                cur.execute("SELECT COUNT(*) FROM gym_subscriptions WHERE status = 'active'")
                active_subs = int((cur.fetchone() or [0])[0])
                cur.execute("SELECT COALESCE(SUM(amount),0) FROM gym_payments WHERE paid_at >= (CURRENT_DATE - INTERVAL '30 days')")
                payments_30_sum = float((cur.fetchone() or [0.0])[0] or 0)
                cur.execute("SELECT created_at::date AS d, COUNT(*) AS c FROM gyms WHERE created_at >= (CURRENT_DATE - INTERVAL '30 days') GROUP BY d ORDER BY d ASC")
                series_rows = cur.fetchall()
                series_30 = [{"date": str(r[0]), "count": int(r[1])} for r in series_rows]
            return {
                "gyms": {"total": total_gyms, "active": active_gyms, "suspended": suspended_gyms, "maintenance": maintenance_gyms, "last_7": gyms_last_7, "last_30": gyms_last_30, "series_30": series_30},
                "whatsapp": {"configured": whatsapp_cfg},
                "storage": {"configured": storage_cfg},
                "subscriptions": {"active": active_subs, "overdue": overdue_subs},
                "payments": {"last_30_sum": payments_30_sum},
            }
        except Exception:
            return {"gyms": {"total": 0, "active": 0, "suspended": 0, "maintenance": 0, "last_7": 0, "last_30": 0, "series_30": []}, "whatsapp": {"configured": 0}, "storage": {"configured": 0}, "subscriptions": {"active": 0, "overdue": 0}, "payments": {"last_30_sum": 0.0}}

    def obtener_warnings_admin(self) -> List[str]:
        ws: List[str] = []
        try:
            m = self.obtener_metricas_agregadas()
            if int((m.get("subscriptions") or {}).get("overdue") or 0) > 0:
                ws.append("Hay suscripciones vencidas")
            with self.db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM gyms WHERE owner_phone IS NULL OR TRIM(owner_phone) = ''")
                no_phone = int((cur.fetchone() or [0])[0])
                if no_phone > 0:
                    ws.append("Gimnasios sin teléfono del dueño")
                cur.execute("SELECT COUNT(*) FROM gyms WHERE whatsapp_phone_id IS NULL OR whatsapp_access_token IS NULL")
                no_wa = int((cur.fetchone() or [0])[0])
                if no_wa > 0:
                    ws.append("Gimnasios sin WhatsApp configurado")
                cur.execute("SELECT COUNT(*) FROM gyms WHERE status = 'suspended'")
                sus = int((cur.fetchone() or [0])[0])
                if sus > 0:
                    ws.append("Gimnasios suspendidos")
        except Exception:
            pass
        return ws

    def set_subscription(self, gym_id: int, plan_id: int, start_date: str) -> bool:
        try:
            with self.db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor()
                cur.execute("SELECT period_days FROM plans WHERE id = %s", (int(plan_id),))
                row = cur.fetchone()
                if not row:
                    return False
                pd = int(row[0])
                cur.execute("SELECT id FROM gym_subscriptions WHERE gym_id = %s", (int(gym_id),))
                existing = cur.fetchone()
                cur.execute("SELECT DATE %s + (%s || ' days')::interval", (start_date, pd))
                nd_row = cur.fetchone()
                next_due = nd_row[0]
                if existing:
                    cur.execute("UPDATE gym_subscriptions SET plan_id = %s, start_date = %s, next_due_date = %s, status = 'active' WHERE gym_id = %s", (int(plan_id), start_date, next_due, int(gym_id)))
                else:
                    cur.execute("INSERT INTO gym_subscriptions (gym_id, plan_id, start_date, next_due_date) VALUES (%s, %s, %s, %s)", (int(gym_id), int(plan_id), start_date, next_due))
                conn.commit()
                return True
        except Exception:
            return False

    def obtener_subscription(self, gym_id: int) -> Optional[Dict[str, Any]]:
        try:
            with self.db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur.execute("SELECT gs.id, gs.plan_id, p.name as plan_name, p.amount, p.currency, p.period_days, gs.start_date, gs.next_due_date, gs.status FROM gym_subscriptions gs JOIN plans p ON p.id = gs.plan_id WHERE gs.gym_id = %s", (int(gym_id),))
                row = cur.fetchone()
                return dict(row) if row else None
        except Exception:
            return None

    def listar_proximos_vencimientos(self, days: int) -> List[Dict[str, Any]]:
        try:
            with self.db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur.execute("SELECT g.id as gym_id, g.nombre, g.subdominio, gs.next_due_date FROM gym_subscriptions gs JOIN gyms g ON g.id = gs.gym_id WHERE gs.status = 'active' AND gs.next_due_date <= (CURRENT_DATE + (%s || ' days')::interval) ORDER BY gs.next_due_date ASC", (int(days),))
                rows = cur.fetchall()
                return [dict(r) for r in rows]
        except Exception:
            return []

    def auto_suspend_overdue(self, grace_days: int) -> int:
        try:
            count = 0
            with self.db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor()
                cur.execute(
                    "SELECT g.id FROM gym_subscriptions gs JOIN gyms g ON g.id = gs.gym_id WHERE gs.status = 'active' AND gs.next_due_date < (CURRENT_DATE - (%s || ' days')::interval)",
                    (int(grace_days),)
                )
                ids = [int(r[0]) for r in cur.fetchall()]
                for gid in ids:
                    try:
                        cur.execute("UPDATE gyms SET status = 'suspended', hard_suspend = false, suspended_until = NULL, suspended_reason = 'overdue' WHERE id = %s", (gid,))
                        cur.execute("UPDATE gym_subscriptions SET status = 'overdue' WHERE gym_id = %s", (gid,))
                        count += 1
                    except Exception:
                        continue
                conn.commit()
            return count
        except Exception:
            return 0