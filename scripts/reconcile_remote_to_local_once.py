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


def resolve_conn(creds: Dict[str, Any], *, is_remote: bool = False):
    kwargs = dict(host=creds['host'], port=creds['port'], dbname=creds['database'], user=creds['user'], password=creds['password'])
    if is_remote and 'sslmode' in creds:
        kwargs['sslmode'] = creds['sslmode']
    return psycopg2.connect(**kwargs)


def get_pk_columns(conn, schema: str, table: str) -> List[str]:
    q = """
    SELECT a.attname
    FROM pg_index i
    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
    WHERE i.indrelid = %s::regclass AND i.indisprimary
    ORDER BY a.attnum
    """
    with conn.cursor() as cur:
        cur.execute(q, (f'{schema}.{table}',))
        rows = cur.fetchall() or []
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
                continue
            cur.execute("SAVEPOINT sp_row")
            try:
                cur.execute(stmt, vals)
                inserted += 1
            except Exception as e:
                # Tolerancia por fila: seguir con el resto
                pk_preview = ', '.join(f"{k}={row.get(k)}" for k in cols if k in ('id','dni'))
                print(f"Aviso: se omitió INSERT en {schema}.{table} ({pk_preview}): {e}")
                cur.execute("ROLLBACK TO SAVEPOINT sp_row")
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
    deleted = 0
    with local_conn.cursor() as cur:
        if dry_run:
            print(f"DRY-RUN: DELETE FROM {schema}.{table} WHERE PKs={pks}")
            return 0
        for key in pks:
            cur.execute("SAVEPOINT sp_row")
            try:
                if len(pk_cols) == 1:
                    cur.execute(stmt, [key[0]])
                else:
                    cur.execute(stmt, list(key))
                deleted += cur.rowcount
            except Exception as e:
                print(f"Aviso: se omitió DELETE en {schema}.{table} PK={key}: {e}")
                cur.execute("ROLLBACK TO SAVEPOINT sp_row")
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
                    cu.execute("SAVEPOINT sp_row")
                    try:
                        cu.execute(stmt, set_vals + where_vals)
                        updated += 1
                    except Exception as e:
                        pk_preview = ', '.join(f"{c}={row[c]}" for c in pk_cols)
                        print(f"Aviso: se omitió UPDATE en {schema}.{table} ({pk_preview}): {e}")
                        cu.execute("ROLLBACK TO SAVEPOINT sp_row")
            if not dry_run:
                local_conn.commit()
    return updated


def disable_subscription(local_conn, subname: str):
    try:
        with local_conn.cursor() as cur:
            cur.execute(f"ALTER SUBSCRIPTION {sql.Identifier(subname).as_string(local_conn)} DISABLE")
        local_conn.commit()
    except Exception as e:
        print(f"Aviso: No se pudo deshabilitar la suscripción '{subname}': {e}")
        local_conn.rollback()


def enable_subscription(local_conn, subname: str):
    try:
        with local_conn.cursor() as cur:
            cur.execute(f"ALTER SUBSCRIPTION {sql.Identifier(subname).as_string(local_conn)} ENABLE")
        local_conn.commit()
    except Exception as e:
        print(f"Aviso: No se pudo habilitar la suscripción '{subname}': {e}")
        local_conn.rollback()


def last_activity_minutes(local_conn) -> float:
    q = """
    SELECT EXTRACT(EPOCH FROM (now() - COALESCE(last_msg_receipt_time, 'epoch'))) / 60.0
    FROM pg_stat_subscription
    ORDER BY 1 ASC
    LIMIT 1
    """
    with local_conn.cursor() as cur:
        cur.execute(q)
        r = cur.fetchone()
        return float(r[0]) if r and r[0] is not None else 1e9


def main():
    parser = argparse.ArgumentParser(description='Reconciliación remoto→local puntual')
    parser.add_argument('--schema', default=DEFAULT_SCHEMA)
    parser.add_argument('--tables', nargs='*', help='Lista de tablas a procesar')
    parser.add_argument('--dry-run', action='store_true', help='No aplica cambios, sólo imprime')
    parser.add_argument('--threshold-minutes', type=int, default=5, help='Sólo corre si la suscripción lleva inactiva al menos este tiempo')
    parser.add_argument('--force', action='store_true', help='Ignora threshold; fuerza ejecución')
    parser.add_argument('--subscription', default='gym_sub')

    args = parser.parse_args()

    # Cargar credenciales
    with open(BASE_DIR / 'config' / 'config.json', 'r', encoding='utf-8') as f:
        full_cfg = json.load(f)

    local_conf = resolve_local_credentials(full_cfg)
    remote_conf = resolve_remote_credentials(full_cfg)

    local_conn = resolve_conn(local_conf, is_remote=False)
    remote_conn = resolve_conn(remote_conf, is_remote=True)

    try:
        subname = args.subscription
        # Gating por inactividad
        if not args.force and not args.dry_run:
            try:
                minutes = last_activity_minutes(local_conn)
                if minutes < args.threshold_minutes:
                    print(f"Suscripción activa recientemente ({minutes:.1f} min < {args.threshold_minutes}); no se ejecuta.")
                    return
            except Exception:
                # Si no podemos leer estado, continuamos de todos modos
                pass

        # Pausar suscripción para aplicar cambios masivos
        disable_subscription(local_conn, subname)

        tables = args.tables
        if not tables:
            # Si no se especificaron, inferir desde config/sync_tables.json publicadas en remoto
            try:
                with open(BASE_DIR / 'config' / 'sync_tables.json', 'r', encoding='utf-8') as tf:
                    st = json.load(tf)
                tables = st.get('publishes_remote_to_local') or st.get('tables') or []
            except Exception:
                tables = []
        if not tables:
            print('No hay tablas a procesar')
            return

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
                local_conn.rollback()

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