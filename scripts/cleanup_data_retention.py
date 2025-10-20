#!/usr/bin/env python3
"""
Limpieza y retención de datos para ambas bases (local y remoto) sin romper replicación.
- Aplica DELETEs específicos por tabla.
- Ejecuta VACUUM ANALYZE en tablas afectadas.
- Usa credenciales desde config/config.json y utilidades de replicación.
"""
import json
import os
from pathlib import Path
from typing import List, Tuple

import psycopg2
from psycopg2 import sql

# Reutilizamos resolución de credenciales del proyecto
try:
    from utils_modules.replication_setup import (
        resolve_local_credentials,
        resolve_remote_credentials,
    )
except Exception:
    resolve_local_credentials = resolve_remote_credentials = None

CONFIG_PATH = Path(__file__).resolve().parent.parent / 'config' / 'config.json'


def load_config() -> dict:
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        return json.load(f) or {}


def connect(params: dict):
    return psycopg2.connect(
        host=params.get('host'),
        port=int(params.get('port') or 5432),
        dbname=params.get('database'),
        user=params.get('user'),
        password=params.get('password'),
        sslmode=params.get('sslmode') or 'prefer',
        application_name='cleanup_data_retention',
        connect_timeout=int(params.get('connect_timeout') or 10),
    )


def safe_exec(conn, statement: str) -> int:
    """Ejecuta un statement y retorna filas afectadas si aplica."""
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(statement)
            try:
                return int(cur.rowcount or 0)
            except Exception:
                return 0
    except Exception as e:
        print(f"ERROR: {e}\n  SQL: {statement}")
        return 0


def vacuum_analyze(conn, table: str):
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql.SQL("VACUUM (ANALYZE) {}.{};").format(sql.Identifier('public'), sql.Identifier(table)))
            print(f"VACUUM ANALYZE aplicado a {table}")
    except Exception as e:
        print(f"ERROR en VACUUM {table}: {e}")


# --- Políticas de retención ---
RETENTION_SQL = [
    # checkin_pending: usados >7d y expirados sin usar >2d
    ("checkin_pending", "DELETE FROM checkin_pending WHERE used = TRUE AND created_at < NOW() - INTERVAL '7 days'"),
    ("checkin_pending", "DELETE FROM checkin_pending WHERE used = FALSE AND expires_at < NOW() - INTERVAL '2 days'"),

    # audit_logs: >90d
    ("audit_logs", "DELETE FROM audit_logs WHERE timestamp < NOW() - INTERVAL '90 days'"),

    # auditoria (legacy): >90d
    ("auditoria", "DELETE FROM auditoria WHERE timestamp < NOW() - INTERVAL '90 days'"),

    # whatsapp_messages: >90d por sent_at
    ("whatsapp_messages", "DELETE FROM whatsapp_messages WHERE sent_at < NOW() - INTERVAL '90 days'"),

    # whatsapp_config: eliminar inactivas y configs muy antiguas, mantener última activa
    (
        "whatsapp_config",
        "DELETE FROM whatsapp_config WHERE id NOT IN (SELECT id FROM whatsapp_config WHERE active = TRUE ORDER BY created_at DESC LIMIT 1) AND (active = FALSE OR created_at < NOW() - INTERVAL '180 days')",
    ),

    # acciones_masivas_pendientes: estados finalizados >30d
    (
        "acciones_masivas_pendientes",
        "DELETE FROM acciones_masivas_pendientes WHERE estado IN ('completado','fallido') AND COALESCE(fecha_ejecucion, fecha_creacion) < NOW() - INTERVAL '30 days'",
    ),

    # system_diagnostics: resueltos >30d
    (
        "system_diagnostics",
        "DELETE FROM system_diagnostics WHERE resolved = TRUE AND COALESCE(resolved_at, timestamp) < NOW() - INTERVAL '30 days'",
    ),

    # maintenance_tasks: completadas/fallidas >90d
    (
        "maintenance_tasks",
        "DELETE FROM maintenance_tasks WHERE status IN ('completed','failed') AND COALESCE(executed_at, scheduled_at) < NOW() - INTERVAL '90 days'",
    ),

    # schedule_conflicts: no activos >60d
    (
        "schedule_conflicts",
        "DELETE FROM schedule_conflicts WHERE status <> 'Activo' AND COALESCE(resolved_at, created_at) < NOW() - INTERVAL '60 days'",
    ),

    # sync_outbox (si existe): >30d
    (
        "sync_outbox",
        "DELETE FROM public.sync_outbox WHERE created_at < NOW() - INTERVAL '30 days'",
    ),
]


AFFECTED_FOR_VACUUM = [
    'checkin_pending',
    'audit_logs',
    'auditoria',
    'whatsapp_messages',
    'whatsapp_config',
    'acciones_masivas_pendientes',
    'system_diagnostics',
    'maintenance_tasks',
    'schedule_conflicts',
    'sync_outbox',
]


def cleanup_one_db(params: dict, label: str):
    print(f"\n== Limpieza en {label} ==")
    conn = connect(params)
    try:
        total_deleted = 0
        for table, stmt in RETENTION_SQL:
            deleted = safe_exec(conn, stmt)
            if deleted:
                print(f"  {table}: {deleted} filas eliminadas")
                total_deleted += deleted
        for table in AFFECTED_FOR_VACUUM:
            vacuum_analyze(conn, table)
        print(f"Total filas eliminadas en {label}: {total_deleted}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


def main():
    cfg = load_config()

    # Resolver credenciales local/remoto
    if resolve_local_credentials and resolve_remote_credentials:
        local = resolve_local_credentials(cfg)
        remote = resolve_remote_credentials(cfg)
    else:
        # Fallback usando claves del JSON
        local = cfg.get('db_local') or cfg
        remote = cfg.get('db_remote') or cfg

    cleanup_one_db(local, 'LOCAL')
    cleanup_one_db(remote, 'REMOTO')
    print("\nLimpieza de retención finalizada en ambas bases.")


if __name__ == '__main__':
    main()