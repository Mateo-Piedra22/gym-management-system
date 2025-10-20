#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Reconciliación segura remoto→local para restaurar consistencia cuando el local estuvo desconectado.

Acciones por tabla (schema `public` por defecto):
- Inserta en local filas que existen en remoto y faltan en local (por PK).
- Borra en local filas que existen en local y no en remoto (por PK).
- Actualiza en local filas existentes si `updated_at` remoto es más reciente.

Protecciones:
- Deshabilita la suscripción local durante la reconciliación y la vuelve a habilitar al finalizar.
- Modo `--dry-run` imprime operaciones sin aplicarlas.
- Gating opcional: sólo corre si la suscripción parece estancada por más de `--threshold-minutes`.

Uso:
  python scripts/reconcile_remote_to_local_once.py --dry-run
  python scripts/reconcile_remote_to_local_once.py --tables usuarios pagos
  python scripts/reconcile_remote_to_local_once.py --force
"""
import argparse
import json
from pathlib import Path
from typing import List, Tuple, Dict, Any

import psycopg2
import psycopg2.extras
from psycopg2 import sql

# Asegura que el repo root esté en sys.path antes de imports locales
import sys
BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from utils_modules.replication_setup import (
    resolve_local_credentials,
    resolve_remote_credentials,
)

DEFAULT_SCHEMA = 'public'

def load_config() -> dict:
    base_dir = Path(__file__).resolve().parent.parent
    cfg_path = base_dir / 'config' / 'config.json'
    if cfg_path.exists():
        with open(cfg_path, 'r', encoding='utf-8') as f:
            return json.load(f) or {}
    return {}


def load_tables() -> List[str]:
    base_dir = Path(__file__).resolve().parent.parent
    path = base_dir / 'config' / 'sync_tables.json'
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f) or {}
            return list(data.get('publishes_remote_to_local') or [])
    except Exception:
        return []


def connect(params: dict):
    # Usa psycopg2.connect con parámetros resueltos
    dsn = params.get('dsn') or ''
    timeout = int(params.get('connect_timeout') or 10)
    if dsn:
        return psycopg2.connect(dsn, connect_timeout=timeout)
    return psycopg2.connect(
        host=params['host'], port=params['port'], dbname=params.get('database') or params.get('dbname'),
        user=params['user'], password=params.get('password'), sslmode=params.get('sslmode') or 'prefer',
        application_name=params.get('application_name') or 'reconcile_remote_to_local_once',
        connect_timeout=timeout,
    )


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
        rows = cur.fetchall() or []
        cols = [r[0] for r in rows]
        if not cols:
            # Fallback común
            try:
                cur.execute(
                    "SELECT column_name FROM information_schema.columns WHERE table_schema=%s AND table_name=%s",
                    (schema, table),
                )
                all_cols = [r[0] for r in cur.fetchall() or []]
                if 'id' in all_cols:
                    return ['id']
            except Exception:
                pass
        return cols


def table_columns(conn, schema: str, table: str) -> List[str]:
    q = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = %s AND table_name = %s
    ORDER BY ordinal_position
    """
    with conn.cursor() as cur:
        cur.execute(q, (schema, table))
        return [r[0] for r in cur.fetchall() or []]


def fetch_keys(conn, schema: str, table: str, pk_cols: List[str]) -> List[Tuple]:
    if not pk_cols:
        return []
    with conn.cursor() as cur:
        cur.execute(sql.SQL("SELECT {} FROM {}.{}").format(
            sql.SQL(',').join(sql.Identifier(c) for c in pk_cols), sql.Identifier(schema), sql.Identifier(table)))
        return [tuple(r) for r in cur.fetchall() or []]


def fetch_rows_by_pk(conn, schema: str, table: str, pk_cols: List[str], pks: List[Tuple]) -> List[dict]:
    if not pks:
        return []
    if len(pk_cols) == 1:
        placeholders = sql.SQL(',').join(sql.Placeholder() for _ in pks)
        query = sql.SQL("SELECT * FROM {}.{} WHERE {} IN (" + placeholders.as_string(conn) + ")").format(
            sql.Identifier(schema), sql.Identifier(table), sql.Identifier(pk_cols[0])
        )
        params = [pk[0] for pk in pks]
    else:
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
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(query, params)
        return list(cur.fetchall() or [])


def insert_rows_local(local_conn, schema: str, table: str, rows: List[dict], dry_run: bool = False) -> int:
    if not rows:
        return 0
    cols = list(rows[0].keys())
    col_idents = [sql.Identifier(c) for c in cols]
    placeholders = [sql.Placeholder() for _ in cols]
    base_insert = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
        sql.Identifier(schema), sql.Identifier(table), sql.SQL(',').join(col_idents), sql.SQL(',').join(placeholders)
    )
    stmt = base_insert + sql.SQL(" ON CONFLICT DO NOTHING")
    inserted = 0
    with local_conn.cursor() as cur:
        for row in rows:
            vals = [row[c] for c in cols]
            if dry_run:
                print(f"DRY-RUN: INSERT INTO {schema}.{table} ({', '.join(cols)}) VALUES ({vals})")
            else:
                cur.execute(stmt, vals)
                inserted += 1
        if not dry_run:
            local_conn.commit()
    return inserted


def delete_rows_local(local_conn, schema: str, table: str, pk_cols: List[str], pks: List[Tuple], dry_run: bool = False) -> int:
    if not pks:
        return 0
    if len(pk_cols) == 1:
        placeholders = sql.SQL(',').join(sql.Placeholder() for _ in pks)
        where_clause = sql.SQL("{} IN (" + placeholders.as_string(local_conn) + ")").format(sql.Identifier(pk_cols[0]))
        params = [pk[0] for pk in pks]
    else:
        conds = []
        params = []
        for key in pks:
            cond = sql.SQL('(') + sql.SQL(' AND ').join(
                sql.SQL("{} = %s").format(sql.Identifier(col)) for col in pk_cols
            ) + sql.SQL(')')
            conds.append(cond)
            params.extend(list(key))
        where_clause = sql.SQL(' OR ').join(conds)
    stmt = sql.SQL("DELETE FROM {}.{} WHERE ").format(sql.Identifier(schema), sql.Identifier(table)) + where_clause
    with local_conn.cursor() as cur:
        if dry_run:
            print(f"DRY-RUN: DELETE FROM {schema}.{table} WHERE PKs={pks}")
            return 0
        cur.execute(stmt, params)
        deleted = cur.rowcount
        local_conn.commit()
        return deleted


def update_rows_local(local_conn, remote_conn, schema: str, table: str, pk_cols: List[str], dry_run: bool = False) -> int:
    local_cols = table_columns(local_conn, schema, table)
    remote_cols = table_columns(remote_conn, schema, table)
    if ('updated_at' not in local_cols) or ('updated_at' not in remote_cols):
        return 0
    non_pk_cols = [c for c in local_cols if c not in pk_cols]
    updated = 0
    with local_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cl, \
         remote_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cr:
        # Obtener updated_at remoto por PK
        cr.execute(sql.SQL("SELECT {}, updated_at FROM {}.{}").format(
            sql.SQL(',').join(sql.Identifier(c) for c in pk_cols), sql.Identifier(schema), sql.Identifier(table)))
        remote_map: Dict[Tuple, Any] = {}
        for row in cr.fetchall() or []:
            key = tuple(row[c] for c in pk_cols)
            remote_map[key] = row['updated_at']
        if not remote_map:
            return 0
        # Obtener updated_at local por PK
        cl.execute(sql.SQL("SELECT {}, updated_at FROM {}.{}").format(
            sql.SQL(',').join(sql.Identifier(c) for c in pk_cols), sql.Identifier(schema), sql.Identifier(table)))
        local_map: Dict[Tuple, Any] = {}
        for row in cl.fetchall() or []:
            key = tuple(row[c] for c in pk_cols)
            local_map[key] = row['updated_at']
        # Determinar claves a actualizar (remoto más reciente)
        to_update_keys = [k for k, rv in remote_map.items() if (k in local_map) and (rv and local_map.get(k) and rv > local_map[k])]
        if not to_update_keys:
            return 0
        # Cargar filas completas de remoto para esas claves
        rows = fetch_rows_by_pk(remote_conn, schema, table, pk_cols, to_update_keys)
        if not rows:
            return 0
        # Construir UPDATE genérico para no-PK
        set_clause = sql.SQL(', ').join(sql.SQL("{} = %s").format(sql.Identifier(c)) for c in non_pk_cols)
        where_clause = sql.SQL(' AND ').join(sql.SQL("{} = %s").format(sql.Identifier(c)) for c in pk_cols)
        stmt = sql.SQL("UPDATE {}.{} SET ").format(sql.Identifier(schema), sql.Identifier(table)) + set_clause + sql.SQL(" WHERE ") + where_clause
        with local_conn.cursor() as cu:
            for row in rows:
                set_vals = [row[c] for c in non_pk_cols]
                where_vals = [row[c] for c in pk_cols]
                if dry_run:
                    print(f"DRY-RUN: UPDATE {schema}.{table} SET {[c for c in non_pk_cols]} WHERE {[c for c in pk_cols]} KEYS={where_vals}")
                else:
                    cu.execute(stmt, set_vals + where_vals)
                    updated += 1
            if not dry_run:
                local_conn.commit()
    return updated


def is_subscription_stalled(local_conn, threshold_minutes: int) -> bool:
    try:
        with local_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT COALESCE(EXTRACT(EPOCH FROM apply_lag), 0) AS apply_lag_s, latest_end_time, last_msg_receipt_time FROM pg_stat_subscription")
            rows = cur.fetchall() or []
            if not rows:
                return True  # no hay suscripción detectable
            import datetime
            now = datetime.datetime.utcnow()
            for r in rows:
                # Si no hay timestamps o apply_lag muy alto, considerar estancado
                let = r.get('latest_end_time')
                lmr = r.get('last_msg_receipt_time')
                lag_s = r.get('apply_lag_s')
                if lag_s and float(lag_s) > threshold_minutes * 60:
                    return True
                for ts in [let, lmr]:
                    if ts and isinstance(ts, datetime.datetime):
                        delta = now - ts.replace(tzinfo=None)
                        if delta.total_seconds() > threshold_minutes * 60:
                            return True
            return False
    except Exception:
        return True


def main():
    ap = argparse.ArgumentParser(description="Reconciliación remoto→local (segura)")
    ap.add_argument('--schema', default=DEFAULT_SCHEMA)
    ap.add_argument('--subscription', default=None, help="Nombre de suscripción local (default desde config)")
    ap.add_argument('--tables', nargs='*', default=None, help="Lista de tablas a reconciliar")
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--threshold-minutes', type=int, default=120, help="Gating: ejecutar sólo si suscripción estancada > N minutos")
    ap.add_argument('--force', action='store_true', help="Ignorar gating y ejecutar siempre")
    args = ap.parse_args()

    cfg = load_config() or {}
    rep_cfg = cfg.get('replication') or {}
    subname = args.subscription or rep_cfg.get('subscription_name') or 'gym_sub'

    local_params = resolve_local_credentials(cfg)
    remote_params = resolve_remote_credentials(cfg)

    local_conn = connect(local_params)
    remote_conn = connect(remote_params)

    try:
        if not args.force:
            if not is_subscription_stalled(local_conn, args.threshold_minutes):
                print(f"Suscripción no estancada (threshold {args.threshold_minutes} min). Saliendo.")
                return
        tables = args.tables or load_tables()
        if not tables:
            print("No hay tablas configuradas para publishes_remote_to_local")
            return

        disable_subscription(local_conn, subname)

        total_ins = 0
        total_upd = 0
        total_del = 0
        schema = args.schema
        for table in tables:
            try:
                pk_cols = get_pk_columns(local_conn, schema, table)
                if not pk_cols:
                    print(f"[WARN] {schema}.{table}: No se pudo determinar PK; saltando.")
                    continue
                local_keys = set(fetch_keys(local_conn, schema, table, pk_cols))
                remote_keys = set(fetch_keys(remote_conn, schema, table, pk_cols))

                # Inserciones: claves que faltan en local
                to_insert = list(remote_keys - local_keys)
                rows_to_insert = fetch_rows_by_pk(remote_conn, schema, table, pk_cols, to_insert)
                ins = insert_rows_local(local_conn, schema, table, rows_to_insert, dry_run=args.dry_run)
                total_ins += ins

                # Borrados: claves extra en local
                to_delete = list(local_keys - remote_keys)
                delc = delete_rows_local(local_conn, schema, table, pk_cols, to_delete, dry_run=args.dry_run)
                total_del += delc

                # Actualizaciones por updated_at
                upc = update_rows_local(local_conn, remote_conn, schema, table, pk_cols, dry_run=args.dry_run)
                total_upd += upc

                print(f"{schema}.{table}: +{ins} / ~{upc} / -{delc}")
            except Exception as e:
                print(f"[ERROR] {schema}.{table}: {e}")

        enable_subscription(local_conn, subname)
        print(f"Resumen: INSERT={total_ins} UPDATE={total_upd} DELETE={total_del}")
    finally:
        try:
            local_conn.close()
        except Exception:
            pass
        try:
            remote_conn.close()
        except Exception:
            pass


if __name__ == '__main__':
    main()