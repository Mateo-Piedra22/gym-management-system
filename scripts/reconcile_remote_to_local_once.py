#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Reconciliación segura remoto→local para restaurar consistencia cuando el local estuvo desconectado.

Acciones por tabla (schema `public` por defecto):
- Inserta en local filas que existen en remoto y faltan en local (por PK).
- Borra en local filas que existen en local y no en remoto (por PK).
- Actualiza en local filas existentes si la versión lógica (`logical_ts`/`last_op_id`) remota es más reciente.

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
import os
from pathlib import Path
from typing import List, Tuple, Dict, Any

import psycopg2
import psycopg2.extras
from psycopg2 import sql

# Asegura que el repo root esté en sys.path antes de imports locales
import sys

def _resolve_base_dir() -> Path:
    try:
        if getattr(sys, 'frozen', False):
            # PyInstaller/Nuitka: usar el directorio del ejecutable
            return Path(sys.executable).resolve().parent
    except Exception:
        pass
    try:
        return Path(__file__).resolve().parent.parent
    except Exception:
        return Path(os.getcwd())

BASE_DIR = _resolve_base_dir()
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

def resource_path(rel_path: str) -> str:
    """Resuelve rutas empaquetadas en ejecutables y en entorno de desarrollo."""
    try:
        base = Path(getattr(sys, '_MEIPASS', BASE_DIR))
    except Exception:
        base = BASE_DIR
    return str((base / rel_path))

from utils_modules.replication_setup import (
    resolve_local_credentials,
    resolve_remote_credentials,
)

DEFAULT_SCHEMA = 'public'


def _is_newer_version(a_ts, a_opid, b_ts, b_opid) -> bool:
    """Devuelve True si la versión A es más reciente que B.

    Reglas:
    - Compara primero por `logical_ts` (mayor gana).
    - Si `logical_ts` empata, usa `last_op_id` como desempate determinístico (lexicográfico).
    - Si faltan campos, trata None como el mínimo.
    """
    try:
        a_ts = int(a_ts) if a_ts is not None else -1
    except Exception:
        a_ts = -1
    try:
        b_ts = int(b_ts) if b_ts is not None else -1
    except Exception:
        b_ts = -1
    if a_ts != b_ts:
        return a_ts > b_ts
    a_id = str(a_opid) if a_opid is not None else ''
    b_id = str(b_opid) if b_opid is not None else ''
    return a_id > b_id


def table_exists(conn, schema: str, table: str) -> bool:
    """Verifica si existe la tabla en el esquema dado."""
    q = """
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
    )
    """
    try:
        with conn.cursor() as cur:
            cur.execute(q, (schema, table))
            r = cur.fetchone()
            return bool(r and r[0])
    except Exception:
        return False


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
    # Evitar insertar usuarios con rol 'dueño' en local
    if table == 'usuarios':
        original_count = len(rows)
        rows = [r for r in rows if (r.get('rol') != 'dueño')]
        omitted = original_count - len(rows)
        if omitted > 0:
            print(f"  Omitidas {omitted} filas de usuarios con rol 'dueño' (inserción local)")
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
    # Evitar borrar usuarios con rol 'dueño' en local
    if table == 'usuarios':
        with local_conn.cursor() as cur:
            owner_pks = set()
            if len(pk_cols) == 1:
                placeholders = sql.SQL(',').join(sql.Placeholder() for _ in pks)
                query = sql.SQL("SELECT {} FROM {}.{} WHERE {} IN (" + placeholders.as_string(local_conn) + ") AND rol = 'dueño'").format(
                    sql.Identifier(pk_cols[0]), sql.Identifier(schema), sql.Identifier(table), sql.Identifier(pk_cols[0])
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
                query = sql.SQL("SELECT {} FROM {}.{} WHERE ({}) AND rol = 'dueño'").format(
                    sql.SQL(',').join(sql.Identifier(c) for c in pk_cols), sql.Identifier(schema), sql.Identifier(table), where_clause
                )
            cur.execute(query, params)
            owner_results = cur.fetchall() or []
            if len(pk_cols) == 1:
                owner_pks = {tuple([r[0]]) for r in owner_results}
            else:
                owner_pks = {tuple(r) for r in owner_results}
        filtered_pks = [pk for pk in pks if pk not in owner_pks]
        if len(filtered_pks) != len(pks):
            print(f"  Omitidas {len(pks) - len(filtered_pks)} filas de usuarios con rol 'dueño' (borrado local)")
        pks = filtered_pks
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
    use_logical = all(c in local_cols for c in ('logical_ts', 'last_op_id')) and all(c in remote_cols for c in ('logical_ts', 'last_op_id'))
    if not use_logical:
        return 0
    non_pk_cols = [c for c in local_cols if c not in pk_cols]
    updated = 0
    with local_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cl, \
         remote_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cr:
        remote_map: Dict[Tuple, Any] = {}
        local_map: Dict[Tuple, Any] = {}
        cr.execute(sql.SQL("SELECT {}, logical_ts, last_op_id FROM {}.{}").format(
            sql.SQL(',').join(sql.Identifier(c) for c in pk_cols), sql.Identifier(schema), sql.Identifier(table)))
        for row in cr.fetchall() or []:
            key = tuple(row[c] for c in pk_cols)
            remote_map[key] = (row.get('logical_ts'), row.get('last_op_id'))
        cl.execute(sql.SQL("SELECT {}, logical_ts, last_op_id FROM {}.{}").format(
            sql.SQL(',').join(sql.Identifier(c) for c in pk_cols), sql.Identifier(schema), sql.Identifier(table)))
        for row in cl.fetchall() or []:
            key = tuple(row[c] for c in pk_cols)
            local_map[key] = (row.get('logical_ts'), row.get('last_op_id'))
        to_update_keys = [k for k, (r_ts, r_id) in remote_map.items()
                          if (k in local_map) and _is_newer_version(r_ts, r_id, *(local_map.get(k) or (None, None)))]
        if not to_update_keys:
            return 0
        rows = fetch_rows_by_pk(remote_conn, schema, table, pk_cols, to_update_keys)
        if not rows:
            return 0
        if table == 'usuarios':
            original_count = len(rows)
            rows = [r for r in rows if (r.get('rol') != 'dueño')]
            omitted = original_count - len(rows)
            if omitted > 0:
                print(f"  Omitidas {omitted} filas de usuarios con rol 'dueño' (update local)")
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

def _load_cfg() -> Dict[str, Any]:
    """Carga config desde recurso empaquetado o desde el árbol del proyecto."""
    # Intento 1: recurso empaquetado
    try:
        p = Path(resource_path('config/config.json'))
        if p.exists():
            with open(p, 'r', encoding='utf-8') as f:
                return json.load(f) or {}
    except Exception:
        pass
    # Intento 2: ruta relativa al proyecto
    try:
        p2 = BASE_DIR / 'config' / 'config.json'
        if p2.exists():
            with open(p2, 'r', encoding='utf-8') as f:
                return json.load(f) or {}
    except Exception:
        pass
    return {}

def _load_sync_tables_default() -> List[str]:
    """Carga lista de tablas por defecto desde sync_tables.json si existe."""
    # Recurso empaquetado primero
    try:
        p = Path(resource_path('config/sync_tables.json'))
        if p.exists():
            with open(p, 'r', encoding='utf-8') as tf:
                st = json.load(tf) or {}
            return st.get('publishes_remote_to_local') or st.get('tables') or []
    except Exception:
        pass
    # Fallback a BASE_DIR
    try:
        p2 = BASE_DIR / 'config' / 'sync_tables.json'
        if p2.exists():
            with open(p2, 'r', encoding='utf-8') as tf:
                st = json.load(tf) or {}
            return st.get('publishes_remote_to_local') or st.get('tables') or []
    except Exception:
        pass
    return []

def run_once(*, schema: str = DEFAULT_SCHEMA, tables: List[str] | None = None, dry_run: bool = False, threshold_minutes: int = 5, force: bool = False, subscription: str = 'gym_sub') -> dict:
    """Ejecuta la reconciliación remoto→local una vez, invocable desde la app.

    Devuelve un diccionario con métricas por tabla:
    {
      'direction': 'remote_to_local',
      'total_inserted': int,
      'total_updated': int,
      'total_deleted': int,
      'tables': [
        {'name': 'tabla', 'inserted': int, 'updated': int, 'deleted': int, 'error': Optional[str]}
      ]
    }
    """
    # Cargar credenciales
    full_cfg = _load_cfg()
    local_conf = resolve_local_credentials(full_cfg)
    remote_conf = resolve_remote_credentials(full_cfg)

    local_conn = resolve_conn(local_conf, is_remote=False)
    remote_conn = resolve_conn(remote_conf, is_remote=True)

    try:
        subname = subscription
        # Gating por inactividad
        if not force and not dry_run:
            try:
                minutes = last_activity_minutes(local_conn)
                if minutes < threshold_minutes:
                    print(f"Suscripción activa recientemente ({minutes:.1f} min < {threshold_minutes}); no se ejecuta.")
                    return
            except Exception:
                # Si no podemos leer estado, continuamos de todos modos
                pass

        # Pausar suscripción para aplicar cambios masivos
        disable_subscription(local_conn, subname)

        use_tables = list(tables or [])
        if not use_tables:
            use_tables = _load_sync_tables_default()
        if not use_tables:
            print('No hay tablas a procesar')
            return {'direction': 'remote_to_local', 'total_inserted': 0, 'total_updated': 0, 'total_deleted': 0, 'tables': []}

        result = {
            'direction': 'remote_to_local',
            'total_inserted': 0,
            'total_updated': 0,
            'total_deleted': 0,
            'tables': []
        }
        for table in use_tables:
            try:
                # Bypass si falta en local o en remoto
                exists_local = table_exists(local_conn, schema, table)
                exists_remote = table_exists(remote_conn, schema, table)
                if not exists_local or not exists_remote:
                    missing_where = 'local' if not exists_local else 'remote'
                    print(f"[INFO] {schema}.{table}: tabla ausente en {missing_where}; bypass.")
                    result['tables'].append({'name': table, 'inserted': 0, 'updated': 0, 'deleted': 0, 'error': None})
                    continue
                pk_cols = get_pk_columns(local_conn, schema, table)
                if not pk_cols:
                    print(f"[WARN] {schema}.{table}: No se pudo determinar PK; saltando.")
                    result['tables'].append({'name': table, 'inserted': 0, 'updated': 0, 'deleted': 0, 'error': 'PK desconocida'})
                    continue
                local_keys = set(fetch_keys(local_conn, schema, table, pk_cols))
                remote_keys = set(fetch_keys(remote_conn, schema, table, pk_cols))

                # Inserciones: claves que faltan en local
                to_insert = list(remote_keys - local_keys)
                rows_to_insert = fetch_rows_by_pk(remote_conn, schema, table, pk_cols, to_insert)
                ins = insert_rows_local(local_conn, schema, table, rows_to_insert, dry_run=dry_run)
                result['total_inserted'] += ins

                # Borrados: claves extra en local
                to_delete = list(local_keys - remote_keys)
                delc = delete_rows_local(local_conn, schema, table, pk_cols, to_delete, dry_run=dry_run)
                result['total_deleted'] += delc

                # Actualizaciones por versión lógica
                upc = update_rows_local(local_conn, remote_conn, schema, table, pk_cols, dry_run=dry_run)
                result['total_updated'] += upc

                print(f"{schema}.{table}: +{ins} / ~{upc} / -{delc}")
                result['tables'].append({'name': table, 'inserted': ins, 'updated': upc, 'deleted': delc, 'error': None})
            except Exception as e:
                print(f"[ERROR] {schema}.{table}: {e}")
                result['tables'].append({'name': table, 'inserted': 0, 'updated': 0, 'deleted': 0, 'error': str(e)})
                local_conn.rollback()

        enable_subscription(local_conn, subname)
        print(f"Resumen R→L: INSERT={result['total_inserted']} UPDATE={result['total_updated']} DELETE={result['total_deleted']}")
    finally:
        try:
            local_conn.close()
        except Exception:
            pass
        try:
            remote_conn.close()
        except Exception:
            pass
    return result

# Alias de compatibilidad para llamadas existentes desde main.py
def reconcile_updates_local(remote_conn, local_conn, schema: str, table: str, pk_cols: List[str], dry_run: bool = False) -> int:
    """Compatibilidad: delega a update_rows_local con el orden esperado.
    """
    return update_rows_local(local_conn, remote_conn, schema, table, pk_cols, dry_run=dry_run)


def main():
    parser = argparse.ArgumentParser(description='Reconciliación remoto→local puntual')
    parser.add_argument('--schema', default=DEFAULT_SCHEMA)
    parser.add_argument('--tables', nargs='*', help='Lista de tablas a procesar')
    parser.add_argument('--dry-run', action='store_true', help='No aplica cambios, sólo imprime')
    parser.add_argument('--threshold-minutes', type=int, default=5, help='Sólo corre si la suscripción lleva inactiva al menos este tiempo')
    parser.add_argument('--force', action='store_true', help='Ignora threshold; fuerza ejecución')
    parser.add_argument('--subscription', default='gym_sub')
    args = parser.parse_args()
    run_once(
        schema=args.schema,
        tables=args.tables,
        dry_run=args.dry_run,
        threshold_minutes=args.threshold_minutes,
        force=args.force,
        subscription=args.subscription,
    )


if __name__ == '__main__':
    main()