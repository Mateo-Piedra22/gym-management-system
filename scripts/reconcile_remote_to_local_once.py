#!/usr/bin/env python3
"""
Reconciliación segura remoto→local para tablas específicas con desfase.

Pasos:
- DESHABILITA la suscripción local (por defecto 'gym_sub') para evitar conflictos.
- Detecta filas presentes en REMOTO y ausentes en LOCAL (por PK simple).
- Inserta esas filas en local.
- VUELVE A HABILITAR la suscripción.
- Muestra un resumen.

Limitaciones:
- Sólo soporta PK de una columna.

Uso:
  python scripts/reconcile_remote_to_local_once.py --tables whatsapp_config

Config:
- Usa primero DSNs de entorno si existen: DATABASE_URL_LOCAL y DATABASE_URL_REMOTE.
- Si no, lee config/config.json perfiles 'db_local' y 'db_remote'.
"""
import os
import json
from pathlib import Path
from typing import List, Tuple

import psycopg2
import psycopg2.extras
from psycopg2 import sql

DEFAULT_TABLES = [
    'whatsapp_config',
]


def load_config() -> dict:
    base_dir = Path(__file__).resolve().parent.parent
    cfg_path = base_dir / 'config' / 'config.json'
    if cfg_path.exists():
        with open(cfg_path, 'r', encoding='utf-8') as f:
            return json.load(f) or {}
    return {}


def build_conn_params(profile: str, cfg: dict) -> dict:
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
    return {
        'host': host,
        'port': port,
        'dbname': database,
        'user': user,
        'password': password,
        'sslmode': sslmode,
        'connect_timeout': 5,
        'application_name': 'reconcile_remote_to_local_once',
    }


def connect(params: dict):
    if 'dsn' in params:
        return psycopg2.connect(params['dsn'])
    return psycopg2.connect(**params)


def disable_subscription(local_conn, subname: str):
    with local_conn.cursor() as cur:
        try:
            cur.execute(sql.SQL("ALTER SUBSCRIPTION {} DISABLE").format(sql.Identifier(subname)))
            local_conn.commit()
            print(f"Suscripción '{subname}' DESHABILITADA")
        except Exception as e:
            local_conn.rollback()
            print(f"Aviso: No se pudo deshabilitar la suscripción '{subname}': {e}")


def enable_subscription(local_conn, subname: str):
    with local_conn.cursor() as cur:
        try:
            cur.execute(sql.SQL("ALTER SUBSCRIPTION {} ENABLE").format(sql.Identifier(subname)))
            local_conn.commit()
            print(f"Suscripción '{subname}' HABILITADA")
        except Exception as e:
            local_conn.rollback()
            print(f"Aviso: No se pudo habilitar la suscripción '{subname}': {e}")


def get_pk_columns(conn, schema: str, table: str) -> List[str]:
    q = """
    SELECT kcu.column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
     AND tc.table_schema = kcu.table_schema
     AND tc.table_name = kcu.table_name
    WHERE tc.table_schema = %s AND tc.table_name = %s AND tc.constraint_type = 'PRIMARY KEY'
    ORDER BY kcu.ordinal_position
    """
    with conn.cursor() as cur:
        cur.execute(q, (schema, table))
        rows = cur.fetchall()
        return [r[0] for r in rows]


def table_columns(conn, schema: str, table: str) -> List[str]:
    q = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = %s AND table_name = %s
    ORDER BY ordinal_position
    """
    with conn.cursor() as cur:
        cur.execute(q, (schema, table))
        return [r[0] for r in cur.fetchall()]


def fetch_missing_pks(remote_conn, local_conn, schema: str, table: str, pk_cols: List[str]) -> List[Tuple]:
    if not pk_cols:
        cols = table_columns(local_conn, schema, table)
        if 'id' in cols:
            pk_cols = ['id']
        else:
            return []
    if len(pk_cols) != 1:
        print(f"Saltando {schema}.{table}: PK compuesta ({pk_cols}).")
        return []
    pk = pk_cols[0]
    with remote_conn.cursor() as cr, local_conn.cursor() as cl:
        cr.execute(sql.SQL("SELECT {} FROM {}.{} ORDER BY {}").format(
            sql.Identifier(pk), sql.Identifier(schema), sql.Identifier(table), sql.Identifier(pk)))
        remote_ids = [r[0] for r in cr.fetchall()]
        cl.execute(sql.SQL("SELECT {} FROM {}.{} ORDER BY {}").format(
            sql.Identifier(pk), sql.Identifier(schema), sql.Identifier(table), sql.Identifier(pk)))
        local_ids = {r[0] for r in cl.fetchall()}
    return [(i,) for i in remote_ids if i not in local_ids]


def fetch_rows_by_pk(remote_conn, schema: str, table: str, pk_col: str, pks: List[Tuple]) -> List[dict]:
    if not pks:
        return []
    placeholders = sql.SQL(',').join(sql.Placeholder() for _ in pks)
    query = sql.SQL("SELECT * FROM {}.{} WHERE {} IN (" + placeholders.as_string(remote_conn) + ")").format(
        sql.Identifier(schema), sql.Identifier(table), sql.Identifier(pk_col)
    )
    with remote_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(query, [pk[0] for pk in pks])
        return list(cur.fetchall())


def insert_rows_local(local_conn, schema: str, table: str, rows: List[dict], dry_run: bool = False) -> int:
    if not rows:
        return 0
    cols = list(rows[0].keys())
    col_idents = [sql.Identifier(c) for c in cols]
    placeholders = [sql.Placeholder() for _ in cols]
    insert_tmpl = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
        sql.Identifier(schema),
        sql.Identifier(table),
        sql.SQL(',').join(col_idents),
        sql.SQL(',').join(placeholders)
    )
    inserted = 0
    with local_conn.cursor() as cur:
        for row in rows:
            vals = [row[c] for c in cols]
            if dry_run:
                print(f"DRY-RUN: INSERT INTO {schema}.{table} ({', '.join(cols)}) VALUES ({vals})")
            else:
                cur.execute(insert_tmpl, vals)
                inserted += 1
        if not dry_run:
            local_conn.commit()
    return inserted


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Reconciliación segura remoto→local')
    parser.add_argument('--subscription', default='gym_sub', help='Nombre de la suscripción local a deshabilitar/habilitar')
    parser.add_argument('--schema', default='public', help='Esquema de las tablas')
    parser.add_argument('--tables', nargs='*', default=DEFAULT_TABLES, help='Lista de tablas a reconciliar')
    parser.add_argument('--dry-run', action='store_true', help='No aplica cambios, sólo muestra acciones')
    args = parser.parse_args()

    cfg = load_config()
    local_params = build_conn_params('local', cfg)
    remote_params = build_conn_params('remote', cfg)

    print('Conectando a LOCAL...')
    local_conn = connect(local_params)
    print('Conectando a REMOTO...')
    remote_conn = connect(remote_params)

    # Deshabilitar suscripción para evitar conflictos en la aplicación
    disable_subscription(local_conn, args.subscription)

    total_inserted = 0
    for table in args.tables:
        print(f"Procesando {args.schema}.{table}...")
        pk_cols = get_pk_columns(local_conn, args.schema, table)
        missing_pks = fetch_missing_pks(remote_conn, local_conn, args.schema, table, pk_cols)
        if not missing_pks:
            print(f"  Sin filas faltantes en local.")
            continue
        pk = pk_cols[0] if pk_cols else 'id'
        rows = fetch_rows_by_pk(remote_conn, args.schema, table, pk, missing_pks)
        print(f"  Filas a insertar en local: {len(rows)}")
        inserted = insert_rows_local(local_conn, args.schema, table, rows, dry_run=args.dry_run)
        total_inserted += inserted
        print(f"  Insertadas en local: {inserted}")

    # Rehabilitar suscripción
    enable_subscription(local_conn, args.subscription)

    local_conn.close()
    remote_conn.close()

    print(f"Listo. Total insertado en local: {total_inserted}. Use scripts/verify_replication_status.py para validar.")


if __name__ == '__main__':
    main()