import os
import json
from pathlib import Path
from typing import List, Tuple, Dict, Any
import sys

import psycopg2
from psycopg2 import sql

# Asegura que el paquete raíz esté en sys.path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from utils_modules.replication_setup import (
    resolve_remote_credentials,
    resolve_local_credentials,
)

CONFIG_PATH = Path(__file__).resolve().parent.parent / 'config' / 'config.json'


def load_config() -> dict:
    cfg: Dict[str, Any] = {}
    try:
        with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
            cfg = json.load(f)
    except Exception:
        cfg = {}
    return cfg or {}


def connect(params: dict):
    dsn = params.get('dsn') or ''
    timeout = int(params.get('connect_timeout') or 10)
    if dsn:
        return psycopg2.connect(dsn, connect_timeout=timeout)
    return psycopg2.connect(
        host=params['host'], port=params['port'], dbname=params['database'],
        user=params['user'], password=params.get('password'), sslmode=params['sslmode'],
        application_name=params.get('application_name') or 'gym_management_system_verify',
        connect_timeout=timeout,
    )


def fetch_one(cur, query: str) -> Any:
    cur.execute(query)
    row = cur.fetchone()
    return row[0] if row else None


def list_publications(cur) -> List[Tuple[str, bool, bool, bool, bool]]:
    cur.execute("SELECT pubname, puballtables, pubinsert, pubupdate, pubdelete, pubtruncate FROM pg_publication")
    return [(r[0], bool(r[1]), bool(r[2]), bool(r[3]), bool(r[4])) for r in cur.fetchall()]  # type: ignore


def count_publication_tables(cur) -> Dict[str, int]:
    res: Dict[str, int] = {}
    try:
        cur.execute(
            """
            SELECT p.pubname, COUNT(*)
            FROM pg_publication p
            JOIN pg_publication_rel pr ON pr.prpubid = p.oid
            GROUP BY p.pubname
            """
        )
        for pub, cnt in cur.fetchall():
            res[str(pub)] = int(cnt)
    except Exception:
        pass
    return res


def list_subscription(cur) -> List[Tuple[str, bool]]:
    try:
        cur.execute("SELECT subname, enabled FROM pg_subscription")
    except Exception:
        try:
            cur.execute("ROLLBACK")
        except Exception:
            pass
        cur.execute("SELECT subname, subenabled FROM pg_subscription")
    return [(str(r[0]), bool(r[1])) for r in cur.fetchall()]


def stat_subscription(cur) -> List[Dict[str, Any]]:
    cols = [
        'subname', 'pid', 'last_msg_send_time', 'last_msg_receipt_time', 'latest_end_time'
    ]
    try:
        cur.execute("SELECT subname, pid, last_msg_send_time, last_msg_receipt_time, latest_end_time FROM pg_stat_subscription")
        rows = cur.fetchall() or []
        out = []
        for r in rows:
            out.append({
                'subname': r[0],
                'pid': r[1],
                'last_msg_send_time': r[2],
                'last_msg_receipt_time': r[3],
                'latest_end_time': r[4],
                'apply_lag_seconds': None if r[4] is None else max(0, int((psycopg2.TimestampFromTicks(psycopg2.time.time()) - r[4]).seconds)) if hasattr(psycopg2, 'TimestampFromTicks') else None,
            })
        return out
    except Exception:
        return []


def table_sync_states(cur) -> List[Tuple[str, str, str]]:
    try:
        cur.execute(
            """
            SELECT s.subname, sr.srrelid::regclass::text AS tabla, sr.srsubstate AS estado
            FROM pg_subscription_rel sr
            JOIN pg_subscription s ON s.oid = sr.srsubid
            ORDER BY s.subname, tabla
            """
        )
        return [(str(r[0]), str(r[1]), str(r[2])) for r in cur.fetchall()]
    except Exception:
        return []


def get_published_tables(cur, pubname: str) -> List[Tuple[str, str]]:
    # Devuelve lista de (schema, table) para una publicación; si puballtables, lista todas tablas del esquema public
    try:
        cur.execute("SELECT puballtables FROM pg_publication WHERE pubname = %s", (pubname,))
        row = cur.fetchone()
        puball = bool(row[0]) if row else False
    except Exception:
        puball = False

    if puball:
        cur.execute(
            """
            SELECT n.nspname AS schema, c.relname AS table
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public' AND c.relkind = 'r'
            ORDER BY n.nspname, c.relname
            """
        )
        return [(str(s), str(t)) for s, t in cur.fetchall()]
    else:
        cur.execute(
            """
            SELECT n.nspname AS schema, c.relname AS table
            FROM pg_publication_rel pr
            JOIN pg_class c ON c.oid = pr.prrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE EXISTS (SELECT 1 FROM pg_publication p WHERE p.oid = pr.prpubid AND p.pubname = %s)
            ORDER BY n.nspname, c.relname
            """,
            (pubname,)
        )
        return [(str(s), str(t)) for s, t in cur.fetchall()]


def count_table(conn, schema: str, table: str) -> int:
    with conn.cursor() as cur:
        cur.execute(sql.SQL("SELECT COUNT(*) FROM {}.{};").format(sql.Identifier(schema), sql.Identifier(table)))
        row = cur.fetchone()
        return int(row[0]) if row else 0


def main():
    cfg = load_config()
    remote = resolve_remote_credentials(cfg)
    local = resolve_local_credentials(cfg)

    # Conexiones
    rconn = connect(remote); rconn.autocommit = True
    lconn = connect(local); lconn.autocommit = True

    try:
        # Remoto: parámetros y publicaciones
        with rconn.cursor() as rcur:
            wal_level = fetch_one(rcur, "SHOW wal_level")
            max_slots = fetch_one(rcur, "SHOW max_replication_slots")
            max_senders = fetch_one(rcur, "SHOW max_wal_senders")
            in_recovery = fetch_one(rcur, "SELECT pg_is_in_recovery()")
            pubs = list_publications(rcur)
            pub_counts = count_publication_tables(rcur)
        print("Remoto:")
        print(f"  wal_level={wal_level} slots={max_slots} senders={max_senders} recovery={in_recovery}")
        if pubs:
            for p in pubs:
                # pubname, puballtables, pubinsert, pubupdate, pubdelete, pubtruncate
                print(f"  publicación {p[0]}: all={p[1]} ins={p[2]} upd={p[3]} del={p[4]} trunc={p[5] if len(p)>5 else False}")
            for pubname, cnt in pub_counts.items():
                print(f"  publicación {pubname}: {cnt} tablas explícitas")
        else:
            print("  sin publicaciones")

        # Local: suscripciones y estado
        with lconn.cursor() as lcur:
            subs = list_subscription(lcur)
            stats = stat_subscription(lcur)
            syncs = table_sync_states(lcur)
        print("Local:")
        if subs:
            for sname, enabled in subs:
                print(f"  suscripción {sname}: enabled={enabled}")
        else:
            print("  sin suscripciones")
        if stats:
            for st in stats:
                lag = st.get('apply_lag_seconds')
                lag_str = f" lag={lag}s" if isinstance(lag, int) else ""
                print(f"  stat {st['subname']}: pid={st['pid']} send={st['last_msg_send_time']} recv={st['last_msg_receipt_time']} end={st['latest_end_time']}{lag_str}")
        if syncs:
            for subname, table, estado in syncs:
                print(f"  sync {subname}: {table} estado={estado}")

        # Comprobación de conteos por tablas publicadas de gym_pub
        with rconn.cursor() as rcur:
            pub_tables = get_published_tables(rcur, 'gym_pub')
        print("Comparación de conteos (remote vs local) para publicación 'gym_pub':")
        mismatches = []
        for schema, table in pub_tables:
            rc = count_table(rconn, schema, table)
            lc = count_table(lconn, schema, table)
            status = "OK" if rc == lc else "DIFERENTE"
            print(f"  {schema}.{table}: remoto={rc} local={lc} -> {status}")
            if rc != lc:
                mismatches.append((f"{schema}.{table}", rc, lc))
        if mismatches:
            print("Diferencias detectadas:")
            for t, rc, lc in mismatches:
                print(f"  {t}: remoto={rc} local={lc}")
        else:
            print("Sin diferencias de conteo en tablas publicadas.")

    finally:
        try:
            rconn.close()
        except Exception:
            pass
        try:
            lconn.close()
        except Exception:
            pass


if __name__ == '__main__':
    main()