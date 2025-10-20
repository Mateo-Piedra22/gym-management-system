#!/usr/bin/env python3
"""
Reconciliación segura local→remoto para tablas específicas con desfase.

Pasos:
- DESHABILITA la suscripción local (por defecto 'gym_sub') para evitar conflictos.
- Detecta filas presentes en local y ausentes en remoto (por PK simple).
- Inserta esas filas en remoto.
- VUELVE A HABILITAR la suscripción.
- Muestra un resumen.

Limitaciones:
- Sólo soporta PK de una columna (común en tablas objetivo). Si detecta PK compuesta, salta la tabla.

Uso:
  python scripts/reconcile_local_remote_once.py --dry-run
  python scripts/reconcile_local_remote_once.py --subscription gym_sub --tables usuarios audit_logs whatsapp_config whatsapp_messages

Config:
- Usa primero DSNs de entorno si existen: DATABASE_URL_LOCAL y DATABASE_URL_REMOTE.
- Si no, lee config/config.json perfiles 'db_local' y 'db_remote'.
- Para contraseñas no incluidas en config, puede usar entorno (DB_LOCAL_PASSWORD / DB_REMOTE_PASSWORD) o keyring si está disponible.
"""
import os
import sys
import json
from pathlib import Path
from typing import List, Tuple

import psycopg2
import psycopg2.extras
from psycopg2 import sql

DEFAULT_TABLES = [
    'usuarios',
    'audit_logs',
    'whatsapp_config',
    'whatsapp_messages',
]


def load_config() -> dict:
    base_dir = Path(__file__).resolve().parent.parent
    cfg_path = base_dir / 'config' / 'config.json'
    if cfg_path.exists():
        with open(cfg_path, 'r', encoding='utf-8') as f:
            return json.load(f) or {}
    return {}


def build_conn_params(profile: str, cfg: dict) -> dict:
    # Prefiere DSN si existe
    dsn_env = os.getenv('DATABASE_URL_REMOTE') if profile == 'remote' else os.getenv('DATABASE_URL_LOCAL')
    if dsn_env:
        return {'dsn': dsn_env}

    node = cfg.get('db_remote') if profile == 'remote' else (cfg.get('db_local') or {})
    # Fallbacks
    host = node.get('host') or cfg.get('host') or 'localhost'
    port = int(node.get('port') or cfg.get('port') or 5432)
    database = node.get('database') or cfg.get('database') or ('railway' if profile == 'remote' else 'gimnasio')
    user = node.get('user') or cfg.get('user') or 'postgres'
    password = node.get('password') or cfg.get('password') or (
        os.getenv('DB_REMOTE_PASSWORD') if profile == 'remote' else os.getenv('DB_LOCAL_PASSWORD')
    ) or os.getenv('DB_PASSWORD') or ''
    sslmode = node.get('sslmode') or cfg.get('sslmode') or ('require' if profile == 'remote' else 'prefer')

    # Intentar keyring si no hay password
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
        'connect_timeout': 5,
        'application_name': 'reconcile_local_remote_once',
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


def fetch_missing_pks(local_conn, remote_conn, schema: str, table: str, pk_cols: List[str]) -> List[Tuple]:
    # Determinar columnas de PK; fallback a 'id' si no definidas
    if not pk_cols:
        cols = table_columns(local_conn, schema, table)
        if 'id' in cols:
            pk_cols = ['id']
        else:
            return []
    # Seleccionar PKs completas en ambos lados
    with local_conn.cursor() as cl, remote_conn.cursor() as cr:
        cl.execute(sql.SQL("SELECT {} FROM {}.{}").format(
            sql.SQL(',').join(sql.Identifier(c) for c in pk_cols), sql.Identifier(schema), sql.Identifier(table)))
        local_keys = [tuple(r) for r in cl.fetchall()]
        cr.execute(sql.SQL("SELECT {} FROM {}.{}").format(
            sql.SQL(',').join(sql.Identifier(c) for c in pk_cols), sql.Identifier(schema), sql.Identifier(table)))
        remote_keys = {tuple(r) for r in cr.fetchall()}
    return [k for k in local_keys if k not in remote_keys]


def fetch_rows_by_pk(local_conn, schema: str, table: str, pk_cols: List[str], pks: List[Tuple]) -> List[dict]:
    if not pks:
        return []
    # Construir cláusula WHERE para PK simple o compuesta
    if len(pk_cols) == 1:
        placeholders = sql.SQL(',').join(sql.Placeholder() for _ in pks)
        query = sql.SQL("SELECT * FROM {}.{} WHERE {} IN (" + placeholders.as_string(local_conn) + ")").format(
            sql.Identifier(schema), sql.Identifier(table), sql.Identifier(pk_cols[0])
        )
        params = [pk[0] for pk in pks]
    else:
        # (pk1=%s AND pk2=%s) OR (pk1=%s AND pk2=%s) ...
        conds = []
        params = []
        for key in pks:
            cond = sql.SQL('(') + sql.SQL(' AND ').join(
                sql.SQL("{} = %s").format(sql.Identifier(col)) for col in pk_cols
            ) + sql.SQL(')')
            conds.append(cond)
            params.extend(list(key))
        where_clause = sql.SQL(' OR ').join(conds)
        query = sql.SQL("SELECT * FROM {}.{} WHERE ") + where_clause
        query = query.format(sql.Identifier(schema), sql.Identifier(table))
    with local_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(query, params)
        return list(cur.fetchall())


def insert_rows_remote(remote_conn, schema: str, table: str, rows: List[dict], pk_cols: List[str], dry_run: bool = False) -> int:
    if not rows:
        return 0
    cols = list(rows[0].keys())
    col_idents = [sql.Identifier(c) for c in cols]
    placeholders = [sql.Placeholder() for _ in cols]
    # Construir INSERT con ON CONFLICT si hay PKs definidas
    base_insert = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
        sql.Identifier(schema),
        sql.Identifier(table),
        sql.SQL(',').join(col_idents),
        sql.SQL(',').join(placeholders)
    )
    if pk_cols:
        conflict_target = sql.SQL(',').join(sql.Identifier(c) for c in pk_cols)
        insert_tmpl = base_insert + sql.SQL(" ON CONFLICT ({}) DO NOTHING").format(conflict_target)
    else:
        insert_tmpl = base_insert
    inserted = 0
    with remote_conn.cursor() as cur:
        for row in rows:
            vals = [row[c] for c in cols]
            if dry_run:
                print(f"DRY-RUN: INSERT INTO {schema}.{table} ({', '.join(cols)}) VALUES ({vals})")
            else:
                cur.execute(insert_tmpl, vals)
                inserted += 1
        if not dry_run:
            remote_conn.commit()
    return inserted


def reconcile_updates_remote(local_conn, remote_conn, schema: str, table: str, pk_cols: List[str], dry_run: bool = False) -> int:
    """Actualiza filas ya existentes en remoto cuando local tiene updated_at más reciente.
    Requiere una columna 'updated_at' en ambas bases. Actualiza todas las columnas no-PK.
    """
    # Verificar columnas y presencia de updated_at en ambos lados
    local_cols = table_columns(local_conn, schema, table)
    remote_cols = table_columns(remote_conn, schema, table)
    if ('updated_at' not in local_cols) or ('updated_at' not in remote_cols):
        return 0
    non_pk_cols = [c for c in local_cols if c not in pk_cols]
    # Mapear updated_at por PK
    with local_conn.cursor() as cl, remote_conn.cursor() as cr:
        cl.execute(sql.SQL("SELECT {}, updated_at FROM {}.{}").format(
            sql.SQL(',').join(sql.Identifier(c) for c in pk_cols), sql.Identifier(schema), sql.Identifier(table)))
        local_entries = [(tuple(r[:-1]), r[-1]) for r in cl.fetchall()]
        cr.execute(sql.SQL("SELECT {}, updated_at FROM {}.{}").format(
            sql.SQL(',').join(sql.Identifier(c) for c in pk_cols), sql.Identifier(schema), sql.Identifier(table)))
        remote_entries = {tuple(r[:-1]): r[-1] for r in cr.fetchall()}
    to_update_keys = [k for (k, lu) in local_entries if (k in remote_entries) and (lu is not None) and (remote_entries.get(k) is not None) and (lu > remote_entries[k])]
    if not to_update_keys:
        return 0
    # Obtener filas completas locales para actualizar
    rows = fetch_rows_by_pk(local_conn, schema, table, pk_cols, to_update_keys)
    if not rows:
        return 0
    # Construir UPDATE (todas las columnas no-PK)
    set_clause = sql.SQL(', ').join(
        sql.SQL("{} = %s").format(sql.Identifier(c)) for c in non_pk_cols
    )
    where_clause = sql.SQL(' AND ').join(
        sql.SQL("{} = %s").format(sql.Identifier(c)) for c in pk_cols
    )
    update_tmpl = sql.SQL("UPDATE {}.{} SET {} WHERE {}").format(
        sql.Identifier(schema), sql.Identifier(table), set_clause, where_clause
    )
    updated = 0
    with remote_conn.cursor() as cur:
        for row in rows:
            vals = [row[c] for c in non_pk_cols] + [row[c] for c in pk_cols]
            if dry_run:
                print(f"DRY-RUN: UPDATE {schema}.{table} SET {', '.join(non_pk_cols)} WHERE {pk_cols} KEYS {to_update_keys}")
            else:
                cur.execute(update_tmpl, vals)
                updated += 1
        if not dry_run:
            remote_conn.commit()
    return updated


def main():
    import argparse

    parser = argparse.ArgumentParser(description='Reconciliación segura local→remoto')
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

    # Resolver tablas desde config/sync_tables.json si está disponible
    tables_to_process = list(args.tables)
    try:
        sync_path = Path(__file__).resolve().parent.parent / 'config' / 'sync_tables.json'
        if sync_path.exists():
            with open(sync_path, 'r', encoding='utf-8') as f:
                sync_cfg = json.load(f) or {}
            uploads = sync_cfg.get('uploads_local_to_remote') or []
            if uploads:
                tables_to_process = uploads
    except Exception:
        pass

    total_inserted = 0
    total_updated = 0
    for table in tables_to_process:
        print(f"Procesando {args.schema}.{table}...")
        pk_cols = get_pk_columns(local_conn, args.schema, table)
        # Inserciones: claves presentes en local y faltantes en remoto
        missing_pks = fetch_missing_pks(local_conn, remote_conn, args.schema, table, pk_cols)
        if missing_pks:
            rows = fetch_rows_by_pk(local_conn, args.schema, table, pk_cols, missing_pks)
            print(f"  Filas a insertar en remoto: {len(rows)}")
            inserted = insert_rows_remote(remote_conn, args.schema, table, rows, pk_cols, dry_run=args.dry_run)
            total_inserted += inserted
            print(f"  Insertadas en remoto: {inserted}")
        else:
            print("  Sin filas faltantes en remoto.")
        # Actualizaciones: filas existentes con updated_at más reciente en local
        updated = reconcile_updates_remote(local_conn, remote_conn, args.schema, table, pk_cols, dry_run=args.dry_run)
        if updated:
            print(f"  Filas actualizadas en remoto: {updated}")
            total_updated += updated
        else:
            print("  Sin actualizaciones pendientes en remoto.")

    # Rehabilitar suscripción
    enable_subscription(local_conn, args.subscription)

    local_conn.close()
    remote_conn.close()

    print(f"Listo. Total insertado en remoto: {total_inserted}, actualizadas: {total_updated}. Use scripts/verify_replication_status.py para validar.")


if __name__ == '__main__':
    main()