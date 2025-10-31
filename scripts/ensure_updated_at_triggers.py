#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Asegura la columna `updated_at` (TIMESTAMPTZ), su índice y un trigger BEFORE UPDATE
en todas las tablas incluidas en `config/sync_tables.json`, tanto en LOCAL como en REMOTO.

Motivación:
- Los scripts de reconciliación usan `updated_at` para resolver conflictos (el más reciente gana).
- Varias tablas no tenían columna/trigger `updated_at`, por lo que las actualizaciones no se reconciliaban por timestamp.

Uso:
  python scripts/ensure_updated_at_triggers.py --apply-local --apply-remote
  python scripts/ensure_updated_at_triggers.py --tables usuarios pagos --schema public
  python scripts/ensure_updated_at_triggers.py --dry-run

Lee credenciales desde `config/config.json` y acepta variables de entorno como fallback.
"""
import os
import json
import logging
from pathlib import Path
from typing import List, Tuple

try:
    import psycopg2
    from psycopg2 import sql
except Exception:
    psycopg2 = None
    sql = None

PROJ_ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = PROJ_ROOT / 'config'
CONFIG_JSON = CONFIG_DIR / 'config.json'
SYNC_TABLES_JSON = CONFIG_DIR / 'sync_tables.json'


def _load_json(path: Path) -> dict:
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}


def _build_conn_params(profile: str, cfg: dict) -> dict:
    dsn_env = os.getenv('DATABASE_URL_REMOTE') if profile == 'remote' else os.getenv('DATABASE_URL_LOCAL')
    if dsn_env:
        return {'dsn': dsn_env}

    node = cfg.get('db_remote') if profile == 'remote' else (cfg.get('db_local') or {})
    host = node.get('host') or cfg.get('host') or 'localhost'
    port = int(node.get('port') or cfg.get('port') or 5432)
    database = node.get('database') or cfg.get('database') or ('railway' if profile == 'remote' else 'gimnasio')
    user = node.get('user') or cfg.get('user') or 'postgres'
    password = node.get('password') or cfg.get('password') or (
        os.getenv('DB_REMOTE_PASSWORD') if profile == 'remote' else os.getenv('DB_LOCAL_PASSWORD')
    ) or os.getenv('DB_PASSWORD') or ''
    sslmode = node.get('sslmode') or cfg.get('sslmode') or ('require' if profile == 'remote' else 'prefer')

    if not password:
        try:
            import keyring
            from config import KEYRING_SERVICE_NAME
            acct = f"{user}@{host}:{port}"
            saved_pwd = keyring.get_password(KEYRING_SERVICE_NAME, acct)
            if saved_pwd:
                password = saved_pwd
        except Exception:
            pass

    return {
        'host': host,
        'port': port,
        'dbname': database,
        'user': user,
        'password': password,
        'sslmode': sslmode,
        'connect_timeout': int(cfg.get('connect_timeout') or 10),
        'application_name': 'ensure_updated_at_triggers',
    }


def _connect(params: dict):
    if 'dsn' in params and params['dsn']:
        return psycopg2.connect(params['dsn'])
    return psycopg2.connect(**params)


def _load_tables() -> List[str]:
    data = _load_json(SYNC_TABLES_JSON) or {}
    pub = data.get('publishes_remote_to_local') or []
    up = data.get('uploads_local_to_remote') or []
    # Unión con preservación de orden básica
    seen = set()
    result: List[str] = []
    for t in list(pub) + list(up):
        if t and t not in seen:
            seen.add(t)
            result.append(t)
    return result


def _table_exists(conn, schema: str, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (f"{schema}.{table}",))
        r = cur.fetchone()
        return bool(r and r[0])


def _has_updated_at(conn, schema: str, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s AND column_name = 'updated_at'
            """,
            (schema, table),
        )
        return cur.fetchone() is not None


def _ensure_function(conn):
    with conn.cursor() as cur:
        cur.execute(
            r"""
            CREATE OR REPLACE FUNCTION public.set_updated_at()
            RETURNS TRIGGER
            LANGUAGE plpgsql
            AS $$
            BEGIN
              NEW.updated_at = NOW();
              RETURN NEW;
            END;
            $$;
            """
        )
    conn.commit()


def _ensure_updated_at(conn, schema: str, table: str, dry_run: bool = False) -> Tuple[bool, str]:
    """Devuelve (aplicado, mensaje)."""
    if not _table_exists(conn, schema, table):
        return False, f"{schema}.{table}: tabla no existe; omitido"

    applied_actions = []
    with conn.cursor() as cur:
        if not _has_updated_at(conn, schema, table):
            if dry_run:
                applied_actions.append("ADD COLUMN updated_at TIMESTAMPTZ DEFAULT NOW()")
            else:
                cur.execute(sql.SQL("ALTER TABLE {}.{} ADD COLUMN updated_at TIMESTAMPTZ DEFAULT NOW()").format(
                    sql.Identifier(schema), sql.Identifier(table)
                ))
                applied_actions.append("added column updated_at")
        # Inicializar valores nulos
        if dry_run:
            applied_actions.append("UPDATE ... SET updated_at = NOW() WHERE updated_at IS NULL")
        else:
            cur.execute(sql.SQL("UPDATE {}.{} SET updated_at = NOW() WHERE updated_at IS NULL").format(
                sql.Identifier(schema), sql.Identifier(table)
            ))
            applied_actions.append("initialized nulls")
        # Índice
        if dry_run:
            applied_actions.append("CREATE INDEX IF NOT EXISTS idx_<table>_updated_at ON <table>(updated_at)")
        else:
            cur.execute(sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {}.{}(updated_at)").format(
                sql.Identifier(f"idx_{table}_updated_at"), sql.Identifier(schema), sql.Identifier(table)
            ))
            applied_actions.append("index ensured")
        # Trigger
        if dry_run:
            applied_actions.append("CREATE TRIGGER trg_<table>_set_updated_at BEFORE UPDATE EXECUTE FUNCTION public.set_updated_at()")
        else:
            cur.execute(sql.SQL("DROP TRIGGER IF EXISTS {} ON {}.{}").format(
                sql.Identifier(f"trg_{table}_set_updated_at"), sql.Identifier(schema), sql.Identifier(table)
            ))
            cur.execute(sql.SQL(
                "CREATE TRIGGER {} BEFORE UPDATE ON {}.{} FOR EACH ROW EXECUTE FUNCTION public.set_updated_at()"
            ).format(
                sql.Identifier(f"trg_{table}_set_updated_at"), sql.Identifier(schema), sql.Identifier(table)
            ))
            applied_actions.append("trigger ensured")

    if not dry_run:
        conn.commit()
    msg = f"{schema}.{table}: " + ", ".join(applied_actions)
    return True, msg


def run(schema: str = 'public', tables: List[str] | None = None, apply_local: bool = True, apply_remote: bool = True, dry_run: bool = False):
    if psycopg2 is None:
        raise RuntimeError("psycopg2 no está disponible; instala requisitos o ejecuta en entorno con PostgreSQL client")

    cfg = _load_json(CONFIG_JSON) or {}
    all_tables = tables or _load_tables()
    if not all_tables:
        print("No se encontraron tablas en config/sync_tables.json y no se pasaron por CLI; sin trabajo.")
        return

    local_conn = None
    remote_conn = None
    try:
        if apply_local:
            print("Conectando a LOCAL...")
            local_conn = _connect(_build_conn_params('local', cfg))
            _ensure_function(local_conn)
        if apply_remote:
            print("Conectando a REMOTO...")
            remote_conn = _connect(_build_conn_params('remote', cfg))
            _ensure_function(remote_conn)

        for t in all_tables:
            if apply_local and local_conn:
                try:
                    ok, msg = _ensure_updated_at(local_conn, schema, t, dry_run=dry_run)
                    print("LOCAL:", msg)
                except Exception as e:
                    print(f"LOCAL: {schema}.{t}: ERROR {e}")
                    try:
                        local_conn.rollback()
                    except Exception:
                        pass
            if apply_remote and remote_conn:
                try:
                    ok, msg = _ensure_updated_at(remote_conn, schema, t, dry_run=dry_run)
                    print("REMOTE:", msg)
                except Exception as e:
                    print(f"REMOTE: {schema}.{t}: ERROR {e}")
                    try:
                        remote_conn.rollback()
                    except Exception:
                        pass
    finally:
        try:
            if local_conn:
                local_conn.close()
        except Exception:
            pass
        try:
            if remote_conn:
                remote_conn.close()
        except Exception:
            pass


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Asegura columna, índice y trigger updated_at en tablas de sincronización')
    parser.add_argument('--schema', default='public', help='Esquema objetivo (por defecto public)')
    parser.add_argument('--tables', nargs='*', help='Lista opcional de tablas (por defecto, las de sync_tables.json)')
    parser.add_argument('--apply-local', action='store_true', help='Aplicar en base LOCAL')
    parser.add_argument('--apply-remote', action='store_true', help='Aplicar en base REMOTA')
    parser.add_argument('--dry-run', action='store_true', help='No cambia nada; sólo imprime acciones')
    args = parser.parse_args()

    # Por defecto, si no se especifica nada, aplicar en ambas
    apply_local = args.apply_local or (not args.apply_local and not args.apply_remote)
    apply_remote = args.apply_remote or (not args.apply_local and not args.apply_remote)

    run(schema=args.schema, tables=args.tables, apply_local=apply_local, apply_remote=apply_remote, dry_run=args.dry_run)


if __name__ == '__main__':
    main()