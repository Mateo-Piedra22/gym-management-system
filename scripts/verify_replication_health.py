# -*- coding: utf-8 -*-
"""
Verificación de salud de replicación lógica (local y remoto).
- Lee config/config.json para DSNs (usando resolvers de replication_setup).
- Muestra estado de suscripciones en local (pg_stat_subscription).
- Muestra detalles de slots y wal_senders en remoto (pg_replication_slots, pg_stat_replication).
- Salidas en JSON para fácil inspección.
"""
import json
import os
import sys
from typing import Any, Dict, List

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

import psycopg2  # type: ignore
import psycopg2.extras  # type: ignore

from utils_modules.replication_setup import resolve_local_credentials, resolve_remote_credentials  # type: ignore

CFG_PATH = os.path.join(BASE_DIR, 'config', 'config.json')


def load_cfg() -> Dict[str, Any]:
    with open(CFG_PATH, 'r', encoding='utf-8') as f:
        return json.load(f)


def connect(params: Dict[str, Any]):
    dsn = params.get('dsn') or ''
    timeout = int(params.get('connect_timeout') or 10)
    if dsn:
        return psycopg2.connect(dsn, connect_timeout=timeout)
    return psycopg2.connect(
        host=params['host'], port=params['port'], dbname=params['database'],
        user=params['user'], password=params.get('password'), sslmode=params['sslmode'],
        application_name=params.get('application_name') or 'gym_management_system',
        connect_timeout=timeout,
    )


def _available_columns(conn) -> List[str]:
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'pg_catalog' AND table_name = 'pg_stat_subscription'
            ORDER BY ordinal_position
            """
        )
        return [r['column_name'] for r in cur.fetchall()]
    except Exception:
        # Fallback: intentar una fila y leer keys
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("SELECT * FROM pg_stat_subscription LIMIT 1")
            row = cur.fetchone()
            return list(row.keys()) if row else []
        except Exception:
            return []


def read_local_subscription(conn):
    cols = set(_available_columns(conn))
    select_parts: List[str] = []
    for c in ("subname", "pid"):
        if c in cols:
            select_parts.append(c)
    # Campos de tiempo/lag si existen
    for c in ("apply_lag", "latest_end_time", "last_msg_send_time", "last_msg_receipt_time", "sync_state"):
        if c in cols:
            select_parts.append(c)
    # Estado y últimos errores si están disponibles
    for c in ("status", "subconninfo", "last_error", "received_lsn"):
        if c in cols:
            select_parts.append(c)
    if not select_parts:
        return []
    sql = "SELECT " + ", ".join(select_parts) + " FROM pg_stat_subscription ORDER BY 1"
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(sql)
    return [dict(r) for r in cur.fetchall()]


def read_remote_replication(conn):
    # pg_stat_replication (wal_senders)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        """
        SELECT pid, usename, application_name, client_addr, state,
               sent_lsn, write_lsn, flush_lsn, replay_lsn,
               sync_state, sync_priority
        FROM pg_stat_replication
        ORDER BY pid
        """
    )
    stat_replication = [dict(r) for r in cur.fetchall()]
    # pg_replication_slots
    cur.execute(
        """
        SELECT slot_name, plugin, slot_type, active, restart_lsn
        FROM pg_replication_slots
        ORDER BY slot_name
        """
    )
    slots = [dict(r) for r in cur.fetchall()]
    # Config claves en remoto
    remote_settings = {}
    try:
        cur.execute("SHOW wal_level")
        remote_settings["wal_level"] = cur.fetchone()[0]
    except Exception:
        remote_settings["wal_level"] = None
    try:
        cur.execute("SHOW max_wal_senders")
        remote_settings["max_wal_senders"] = cur.fetchone()[0]
    except Exception:
        remote_settings["max_wal_senders"] = None
    try:
        cur.execute("SHOW max_replication_slots")
        remote_settings["max_replication_slots"] = cur.fetchone()[0]
    except Exception:
        remote_settings["max_replication_slots"] = None

    # Publicaciones y tablas publicadas (si existen)
    publications = []
    publication_tables = []
    try:
        cur.execute("SELECT pubname FROM pg_publication ORDER BY pubname")
        publications = [r["pubname"] for r in cur.fetchall()]
    except Exception:
        publications = []
    try:
        cur.execute(
            """
            SELECT pubname, schemaname, tablename
            FROM pg_publication_tables
            ORDER BY pubname, tablename
            """
        )
        publication_tables = [dict(r) for r in cur.fetchall()]
    except Exception:
        publication_tables = []

    return {"wal_senders": stat_replication, "slots": slots, "settings": remote_settings, "publications": publications, "publication_tables": publication_tables}


def main() -> int:
    cfg = load_cfg()
    out: Dict[str, Any] = {"ok": False}
    try:
        local_params = resolve_local_credentials(cfg)
        with connect(local_params) as lconn:
            out["local_pg_stat_subscription"] = read_local_subscription(lconn)
    except Exception as e:
        out["local_error"] = str(e)
    try:
        remote_params = resolve_remote_credentials(cfg)
        with connect(remote_params) as rconn:
            out["remote_replication"] = read_remote_replication(rconn)
    except Exception as e:
        out["remote_error"] = str(e)
    out["ok"] = True
    print(json.dumps(out, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())