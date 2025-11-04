#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Reconciliación segura local→remoto con inserciones, actualizaciones y borrados.

Pasos:
- DESHABILITA la suscripción local (por defecto 'gym_sub') para evitar conflictos.
- Detecta filas presentes en local y ausentes en remoto (por PK) y las inserta.
- Detecta filas presentes en remoto y ausentes en local (por PK) y las BORRA en remoto.
- Actualiza filas existentes en remoto si local tiene versión lógica (`logical_ts`/`last_op_id`) más reciente.
- VUELVE A HABILITAR la suscripción.

Limitaciones:
- Soporta PK de una o múltiples columnas; si no hay PK, intenta 'id'.

Uso:
  python scripts/reconcile_local_remote_once.py --dry-run
  python scripts/reconcile_local_remote_once.py --subscription gym_sub --tables usuarios audit_logs whatsapp_config whatsapp_messages

Config:
- Usa DSNs de entorno si existen: DATABASE_URL_LOCAL y DATABASE_URL_REMOTE.
- Si no, lee config/config.json perfiles 'db_local' y 'db_remote'.
- Para contraseñas no incluidas en config, puede usar entorno (DB_LOCAL_PASSWORD / DB_REMOTE_PASSWORD) o keyring si está disponible.
"""
import os
import json
import sys
from pathlib import Path
from typing import List, Tuple, Dict, Any

import psycopg2
import psycopg2.extras
from psycopg2 import sql
from psycopg2.extras import Json

DEFAULT_TABLES = [
    'usuarios',
    'audit_logs',
    'whatsapp_config',
    'whatsapp_messages',
]

def _is_newer_version(a_ts, a_opid, b_ts, b_opid) -> bool:
    """True si A es más reciente que B comparando logical_ts y last_op_id.

    - Mayor `logical_ts` gana.
    - Si empata, compara `last_op_id` lexicográficamente.
    - None se considera mínimo.
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

def resource_path(rel_path: str) -> str:
    """Resuelve rutas empaquetadas para ejecutables o desde el árbol del proyecto."""
    try:
        base = Path(getattr(sys, '_MEIPASS', Path(__file__).resolve().parent.parent))
    except Exception:
        base = Path(__file__).resolve().parent.parent
    return str((base / rel_path))


def load_config() -> dict:
    # Intento: recurso empaquetado (PyInstaller/Nuitka)
    try:
        rp = Path(resource_path('config/config.json'))
        if rp.exists():
            with open(rp, 'r', encoding='utf-8') as f:
                return json.load(f) or {}
    except Exception:
        pass
    # Fallback: ruta relativa al repo durante desarrollo
    try:
        base_dir = Path(__file__).resolve().parent.parent
        cfg_path = base_dir / 'config' / 'config.json'
        if cfg_path.exists():
            with open(cfg_path, 'r', encoding='utf-8') as f:
                return json.load(f) or {}
    except Exception:
        pass
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


def table_exists(conn, schema: str, table: str) -> bool:
    """Verifica si existe la tabla en el esquema dado.

    Usa information_schema para evitar excepciones cuando la tabla no está presente.
    """
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


def fetch_keys(conn, schema: str, table: str, pk_cols: List[str]) -> List[Tuple]:
    if not pk_cols:
        cols = table_columns(conn, schema, table)
        if 'id' in cols:
            pk_cols = ['id']
        else:
            return []
    with conn.cursor() as cur:
        cur.execute(sql.SQL("SELECT {} FROM {}.{}").format(
            sql.SQL(',').join(sql.Identifier(c) for c in pk_cols), sql.Identifier(schema), sql.Identifier(table)))
        return [tuple(r) for r in cur.fetchall()]


def fetch_rows_by_pk(local_conn, schema: str, table: str, pk_cols: List[str], pks: List[Tuple]) -> List[dict]:
    if not pks:
        return []
    if len(pk_cols) == 1:
        placeholders = sql.SQL(',').join(sql.Placeholder() for _ in pks)
        query = sql.SQL("SELECT * FROM {}.{} WHERE {} IN (" + placeholders.as_string(local_conn) + ")").format(
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
    with local_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(query, params)
        return list(cur.fetchall())


def get_column_types(conn, schema: str, table: str) -> Dict[str, str]:
    """Obtiene tipos de columnas (udt_name/data_type) para castear adecuadamente JSON/JSONB."""
    q = """
    SELECT column_name, udt_name, data_type
    FROM information_schema.columns
    WHERE table_schema = %s AND table_name = %s
    """
    with conn.cursor() as cur:
        cur.execute(q, (schema, table))
        rows = cur.fetchall() or []
    types: Dict[str, str] = {}
    for name, udt, dt in rows:
        t = (udt or dt or '').lower()
        types[name] = t
    return types


def _adapt_value_for_column(val: Any, col_type: str) -> Any:
    """Adapta valores Python: usa Json para json/jsonb; serializa dict/list a texto para text/varchar."""
    t = (col_type or '').lower()
    if t in ('json', 'jsonb'):
        if isinstance(val, (dict, list)):
            return Json(val)
        return val
    if t in ('text', 'varchar', 'character varying'):
        if isinstance(val, (dict, list)):
            import json as _json
            return _json.dumps(val, ensure_ascii=False)
        return val
    return val


def _build_placeholders_for_columns(cols: List[str], col_types: Dict[str, str]) -> List[sql.SQL]:
    phs: List[sql.SQL] = []
    for c in cols:
        t = (col_types.get(c) or '').lower()
        if t in ('json', 'jsonb'):
            cast = 'jsonb' if t == 'jsonb' else 'json'
            phs.append(sql.SQL('%s::') + sql.SQL(cast))
        else:
            phs.append(sql.Placeholder())
    return phs


def insert_rows_remote(remote_conn, schema: str, table: str, rows: List[dict], pk_cols: List[str], dry_run: bool = False) -> int:
    if not rows:
        return 0
    # Evitar insertar usuarios con rol 'dueño' en remoto
    if table == 'usuarios':
        original_count = len(rows)
        rows = [r for r in rows if (r.get('rol') != 'dueño')]
        omitted = original_count - len(rows)
        if omitted > 0:
            print(f"  Omitidas {omitted} filas de usuarios con rol 'dueño' (inserción remoto)")
        # Si el filtrado deja sin filas, no intentar armar columnas
        if not rows:
            return 0
    cols = list(rows[0].keys())
    col_types = get_column_types(remote_conn, schema, table)
    col_idents = [sql.Identifier(c) for c in cols]
    placeholders = _build_placeholders_for_columns(cols, col_types)
    base_insert = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
        sql.Identifier(schema),
        sql.Identifier(table),
        sql.SQL(',').join(col_idents),
        sql.SQL(',').join(placeholders)
    )
    # Siempre tolera conflictos en cualquier restricción única/PK
    insert_tmpl = base_insert + sql.SQL(" ON CONFLICT DO NOTHING")
    inserted = 0
    with remote_conn.cursor() as cur:
        for row in rows:
            vals = [_adapt_value_for_column(row[c], col_types.get(c)) for c in cols]
            if dry_run:
                print(f"DRY-RUN: INSERT INTO {schema}.{table} ({', '.join(cols)}) VALUES ({vals})")
            else:
                cur.execute(insert_tmpl, vals)
                inserted += 1
        if not dry_run:
            remote_conn.commit()
    return inserted


def delete_rows_remote(remote_conn, schema: str, table: str, pk_cols: List[str], pks: List[Tuple], dry_run: bool = False) -> int:
    if not pks:
        return 0
        
    # Special handling for usuarios table to prevent deletion of owner users
    if table == 'usuarios':
        # Filter out owner users from the list of PKs to delete
        with remote_conn.cursor() as cur:
            owner_pks = set()
            if len(pk_cols) == 1:
                placeholders = sql.SQL(',').join(sql.Placeholder() for _ in pks)
                query = sql.SQL("SELECT {} FROM {}.{} WHERE {} IN (" + placeholders.as_string(remote_conn) + ") AND rol = 'dueño'").format(
                    sql.Identifier(pk_cols[0]), sql.Identifier(schema), sql.Identifier(table), sql.Identifier(pk_cols[0])
                )
                params = [pk[0] for pk in pks]
            else:
                # For composite PKs, we need to check each combination
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
            owner_results = cur.fetchall()
            if len(pk_cols) == 1:
                owner_pks = {tuple([r[0]]) for r in owner_results}
            else:
                owner_pks = {tuple(r) for r in owner_results}
            
            # Filter out owner PKs from the list to delete
            filtered_pks = [pk for pk in pks if pk not in owner_pks]
            if len(filtered_pks) != len(pks):
                print(f"  Omitidas {len(pks) - len(filtered_pks)} filas de usuarios con rol 'dueño'")
            pks = filtered_pks
        
        if not pks:
            return 0
    
    if len(pk_cols) == 1:
        placeholders = sql.SQL(',').join(sql.Placeholder() for _ in pks)
        where_clause = sql.SQL("{} IN (" + placeholders.as_string(remote_conn) + ")").format(sql.Identifier(pk_cols[0]))
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
    delete_tmpl = sql.SQL("DELETE FROM {}.{} WHERE ").format(sql.Identifier(schema), sql.Identifier(table)) + where_clause
    deleted = 0
    with remote_conn.cursor() as cur:
        if dry_run:
            print(f"DRY-RUN: DELETE FROM {schema}.{table} WHERE PKs={pks}")
        else:
            cur.execute(delete_tmpl, params)
            deleted = cur.rowcount
            remote_conn.commit()
    return deleted


def reconcile_updates_remote(local_conn, remote_conn, schema: str, table: str, pk_cols: List[str], dry_run: bool = False) -> int:
    local_cols = table_columns(local_conn, schema, table)
    remote_cols = table_columns(remote_conn, schema, table)
    use_logical = all(c in local_cols for c in ('logical_ts', 'last_op_id')) and all(c in remote_cols for c in ('logical_ts', 'last_op_id'))
    if not use_logical:
        return 0
    non_pk_cols = [c for c in local_cols if c not in pk_cols]
    remote_col_types = get_column_types(remote_conn, schema, table)

    with local_conn.cursor() as cl, remote_conn.cursor() as cr:
        cl.execute(sql.SQL("SELECT {}, logical_ts, last_op_id FROM {}.{}").format(
            sql.SQL(',').join(sql.Identifier(c) for c in pk_cols), sql.Identifier(schema), sql.Identifier(table)))
        local_entries = {tuple(r[:-2]): (r[-2], r[-1]) for r in cl.fetchall()}
        cr.execute(sql.SQL("SELECT {}, logical_ts, last_op_id FROM {}.{}").format(
            sql.SQL(',').join(sql.Identifier(c) for c in pk_cols), sql.Identifier(schema), sql.Identifier(table)))
        remote_entries = {tuple(r[:-2]): (r[-2], r[-1]) for r in cr.fetchall()}
        to_update_keys = [k for (k, (lts, lid)) in local_entries.items()
                          if (k in remote_entries) and _is_newer_version(lts, lid, *(remote_entries.get(k) or (None, None)))]
    if not to_update_keys:
        return 0
    rows = fetch_rows_by_pk(local_conn, schema, table, pk_cols, to_update_keys)
    if not rows:
        return 0
    if table == 'usuarios':
        original_count = len(rows)
        rows = [r for r in rows if (r.get('rol') != 'dueño')]
        omitted = original_count - len(rows)
        if omitted > 0:
            print(f"  Omitidas {omitted} filas de usuarios con rol 'dueño' (update remoto)")
    set_parts: List[sql.SQL] = []
    for c in non_pk_cols:
        t = (remote_col_types.get(c) or '').lower()
        if t in ('json', 'jsonb'):
            cast = 'jsonb' if t == 'jsonb' else 'json'
            set_parts.append(sql.SQL("{} = ").format(sql.Identifier(c)) + sql.SQL('%s::') + sql.SQL(cast))
        else:
            set_parts.append(sql.SQL("{} = %s").format(sql.Identifier(c)))
    set_clause = sql.SQL(', ').join(set_parts)
    where_clause = sql.SQL(' AND ').join(
        sql.SQL("{} = %s").format(sql.Identifier(c)) for c in pk_cols
    )
    update_tmpl = sql.SQL("UPDATE {}.{} SET {} WHERE {}").format(
        sql.Identifier(schema), sql.Identifier(table), set_clause, where_clause
    )
    updated = 0
    with remote_conn.cursor() as cur:
        for row in rows:
            vals = [_adapt_value_for_column(row[c], remote_col_types.get(c)) for c in non_pk_cols] + [row[c] for c in pk_cols]
            if dry_run:
                print(f"DRY-RUN: UPDATE {schema}.{table} SET {', '.join(non_pk_cols)} WHERE PKs={to_update_keys}")
            else:
                cur.execute("SAVEPOINT reconcile_row")
                try:
                    cur.execute(update_tmpl, vals)
                    updated += 1
                except Exception as e:
                    pk_preview = ', '.join(f"{c}={row[c]}" for c in pk_cols)
                    print(f"Aviso: se omitió UPDATE en {schema}.{table} ({pk_preview}): {e}")
                    cur.execute("ROLLBACK TO SAVEPOINT reconcile_row")
        if not dry_run:
            remote_conn.commit()
    return updated


def run_once(*, subscription: str = 'gym_sub', schema: str = 'public', tables: List[str] | None = None, dry_run: bool = False) -> dict:
    """Ejecuta reconciliación local→remoto una vez, invocable desde la app.

    Devuelve un diccionario con métricas por tabla:
    {
      'direction': 'local_to_remote',
      'total_inserted': int,
      'total_updated': int,
      'total_deleted': int,
      'tables': [
        {'name': 'tabla', 'inserted': int, 'updated': int, 'deleted': int, 'error': Optional[str]}
      ]
    }
    """
    cfg = load_config()
    local_params = build_conn_params('local', cfg)
    remote_params = build_conn_params('remote', cfg)

    print('Conectando a LOCAL...')
    local_conn = connect(local_params)
    print('Conectando a REMOTO...')
    remote_conn = connect(remote_params)

    disable_subscription(local_conn, subscription)

    # Determinar tablas a procesar
    tables_to_process = list(tables or DEFAULT_TABLES)
    try:
        sync_path = Path(resource_path('config/sync_tables.json'))
        if not sync_path.exists():
            sync_path = Path(__file__).resolve().parent.parent / 'config' / 'sync_tables.json'
        if sync_path.exists():
            with open(sync_path, 'r', encoding='utf-8') as f:
                sync_cfg = json.load(f) or {}
            uploads = sync_cfg.get('uploads_local_to_remote') or []
            if (tables is None or tables == DEFAULT_TABLES) and uploads:
                tables_to_process = uploads
    except Exception:
        pass

    result = {
        'direction': 'local_to_remote',
        'total_inserted': 0,
        'total_updated': 0,
        'total_deleted': 0,
        'tables': []
    }
    try:
        for table in tables_to_process:
            print(f"Procesando {schema}.{table}...")
            # Bypass limpio si falta la tabla en local o remoto
            exists_local = table_exists(local_conn, schema, table)
            exists_remote = table_exists(remote_conn, schema, table)
            if not exists_local or not exists_remote:
                missing = 'local' if not exists_local else 'remote'
                print(f"  [INFO] {schema}.{table}: tabla ausente en {missing}; bypass.")
                result['tables'].append({'name': table, 'inserted': 0, 'updated': 0, 'deleted': 0, 'error': None})
                continue
            pk_cols = get_pk_columns(local_conn, schema, table)
            local_keys = fetch_keys(local_conn, schema, table, pk_cols)
            remote_keys = fetch_keys(remote_conn, schema, table, pk_cols)
            local_key_set = set(local_keys)
            remote_key_set = set(remote_keys)

            table_metrics = {'name': table, 'inserted': 0, 'updated': 0, 'deleted': 0, 'error': None}

            # Inserciones en remoto: claves presentes en local y faltantes en remoto
            missing_remote = [k for k in local_keys if k not in remote_key_set]
            if missing_remote:
                rows = fetch_rows_by_pk(local_conn, schema, table, pk_cols, missing_remote)
                print(f"  Filas a insertar en remoto: {len(rows)}")
                inserted = insert_rows_remote(remote_conn, schema, table, rows, pk_cols, dry_run=dry_run)
                result['total_inserted'] += inserted
                table_metrics['inserted'] = inserted
                print(f"  Insertadas en remoto: {inserted}")
            else:
                print("  Sin filas faltantes en remoto.")

            # Borrados en remoto: claves presentes en remoto y ausentes en local
            extra_remote = [k for k in remote_keys if k not in local_key_set]
            if extra_remote:
                print(f"  Filas a borrar en remoto: {len(extra_remote)}")
                deleted = delete_rows_remote(remote_conn, schema, table, pk_cols, extra_remote, dry_run=dry_run)
                result['total_deleted'] += deleted
                table_metrics['deleted'] = deleted
                print(f"  Borradas en remoto: {deleted}")
            else:
                print("  Sin filas extra en remoto.")

            # Actualizaciones en remoto por versión lógica más reciente en local
            updated = reconcile_updates_remote(local_conn, remote_conn, schema, table, pk_cols, dry_run=dry_run)
            if updated:
                print(f"  Filas actualizadas en remoto: {updated}")
                result['total_updated'] += updated
                table_metrics['updated'] = updated
            else:
                print("  Sin actualizaciones pendientes en remoto.")

            result['tables'].append(table_metrics)
    finally:
        # Asegurar re-habilitar suscripción aunque falle algo
        try:
            enable_subscription(local_conn, subscription)
        except Exception:
            pass
        try:
            local_conn.close()
        except Exception:
            pass
        try:
            remote_conn.close()
        except Exception:
            pass
    # Imprimir resumen para CLI, pero también devolver métricas para UI
    try:
        print(f"Resumen L→R: INSERT={result['total_inserted']} UPDATE={result['total_updated']} DELETE={result['total_deleted']}")
    except Exception:
        pass
    return result

    print(f"Listo. Insertadas: {total_inserted}, Actualizadas: {total_updated}, Borradas: {total_deleted}. Use scripts/verify_replication_health.py para validar.")


def main():
    import argparse

    parser = argparse.ArgumentParser(description='Reconciliación segura local→remoto con inserciones, actualizaciones y borrados')
    parser.add_argument('--subscription', default='gym_sub', help='Nombre de la suscripción local a deshabilitar/habilitar')
    parser.add_argument('--schema', default='public', help='Esquema de las tablas')
    parser.add_argument('--tables', nargs='*', default=DEFAULT_TABLES, help='Lista de tablas a reconciliar')
    parser.add_argument('--dry-run', action='store_true', help='No aplica cambios, sólo muestra acciones')
    args = parser.parse_args()
    run_once(subscription=args.subscription, schema=args.schema, tables=args.tables, dry_run=args.dry_run)


if __name__ == '__main__':
    main()