import os
import json
from pathlib import Path
from typing import List

import psycopg2
from psycopg2 import sql


def _load_cfg() -> dict:
    base_dir = Path(__file__).resolve().parent.parent
    cfg_path = base_dir / 'config' / 'config.json'
    with open(cfg_path, 'r', encoding='utf-8') as f:
        return json.load(f) or {}


def _load_sync_tables() -> dict:
    base_dir = Path(__file__).resolve().parent.parent
    cfg_path = base_dir / 'config' / 'sync_tables.json'
    if not cfg_path.exists():
        return {"uploads_local_to_remote": [], "publishes_remote_to_local": []}
    with open(cfg_path, 'r', encoding='utf-8') as f:
        return json.load(f) or {}


def _connect_local(cfg: dict):
    local = cfg.get('db_local') or cfg
    host = local.get('host') or cfg.get('host') or 'localhost'
    port = int(local.get('port') or cfg.get('port') or 5432)
    db = local.get('database') or cfg.get('database') or 'gimnasio'
    user = local.get('user') or cfg.get('user') or 'postgres'
    password = local.get('password') or cfg.get('password') or os.getenv('DB_LOCAL_PASSWORD')
    sslmode = local.get('sslmode') or cfg.get('sslmode') or 'prefer'
    timeout = int(local.get('connect_timeout') or cfg.get('connect_timeout') or 10)
    return psycopg2.connect(
        host=host, port=port, dbname=db, user=user, password=password, sslmode=sslmode,
        application_name='install_outbox_triggers', connect_timeout=timeout
    )


def _ensure_outbox_objects(conn):
    conn.autocommit = True
    with conn.cursor() as cur:
        # Tabla outbox
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS public.sync_outbox (
                id BIGSERIAL PRIMARY KEY,
                schema_name TEXT NOT NULL,
                table_name TEXT NOT NULL,
                op TEXT NOT NULL CHECK (op IN ('INSERT','UPDATE','DELETE')),
                pk JSONB NOT NULL,
                data JSONB,
                dedup_key TEXT,
                txid BIGINT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """
        )
        # Índices auxiliares
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sync_outbox_table_op ON public.sync_outbox(table_name, op)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sync_outbox_created ON public.sync_outbox(created_at)")
        # Función genérica de captura
        cur.execute(
            """
            CREATE OR REPLACE FUNCTION public.sync_outbox_capture() RETURNS trigger AS $$
            DECLARE
                pk_cols TEXT[];
                pk_vals JSONB := '{}'::jsonb;
                col TEXT;
                the_op TEXT := TG_OP::text;
                payload JSONB;
                dedup TEXT;
            BEGIN
                SELECT array_agg(a.attname)
                INTO pk_cols
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = TG_RELID AND i.indisprimary;

                IF pk_cols IS NULL OR array_length(pk_cols,1) IS NULL OR array_length(pk_cols,1) = 0 THEN
                    RETURN CASE WHEN the_op = 'DELETE' THEN OLD ELSE NEW END;
                END IF;

                FOREACH col IN ARRAY pk_cols LOOP
                    IF the_op = 'DELETE' THEN
                        pk_vals := pk_vals || jsonb_build_object(col, to_jsonb(OLD)->col);
                    ELSE
                        pk_vals := pk_vals || jsonb_build_object(col, to_jsonb(NEW)->col);
                    END IF;
                END LOOP;

                IF the_op = 'DELETE' THEN
                    payload := NULL;
                ELSE
                    payload := to_jsonb(NEW);
                END IF;

                dedup := (TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME || ':' || the_op || ':' || pk_vals::text);

                INSERT INTO public.sync_outbox(schema_name, table_name, op, pk, data, dedup_key, txid)
                VALUES (TG_TABLE_SCHEMA, TG_TABLE_NAME, the_op, pk_vals, payload, dedup, txid_current());

                RETURN CASE WHEN the_op = 'DELETE' THEN OLD ELSE NEW END;
            END;
            $$ LANGUAGE plpgsql SECURITY DEFINER;
            """
        )


def _table_exists(conn, schema: str, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s) IS NOT NULL", (f"{schema}.{table}",))
        row = cur.fetchone()
        return bool(row and row[0])


def _get_existing_trigger_names(conn, schema: str, table: str) -> List[str]:
    if not _table_exists(conn, schema, table):
        return []
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT tgname
            FROM pg_trigger
            WHERE NOT tgisinternal
              AND tgrelid = %s::regclass
            """,
            (f"{schema}.{table}",)
        )
        rows = cur.fetchall() or []
        return [r[0] for r in rows]


def _ensure_triggers_for_table(conn, schema: str, table: str):
    if not _table_exists(conn, schema, table):
        return
    existing = set(_get_existing_trigger_names(conn, schema, table))
    # Un trigger conjunto por AIUD para simplificar
    trig_name = f"sync_outbox_aiud_{table}"
    if trig_name in existing:
        return
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                CREATE TRIGGER {trig}
                AFTER INSERT OR UPDATE OR DELETE ON {sch}.{tbl}
                FOR EACH ROW EXECUTE FUNCTION public.sync_outbox_capture();
                """
            ).format(
                trig=sql.Identifier(trig_name),
                sch=sql.Identifier(schema),
                tbl=sql.Identifier(table),
            )
        )


def run():
    cfg = _load_cfg()
    sync_tables = _load_sync_tables()
    uploads = list(sync_tables.get('uploads_local_to_remote') or [])
    if not uploads:
        print("No hay tablas configuradas para outbox (uploads_local_to_remote está vacío)")
    conn = None
    try:
        conn = _connect_local(cfg)
        _ensure_outbox_objects(conn)
        for tbl in uploads:
            schema = 'public'
            table = str(tbl).strip()
            if not table:
                continue
            _ensure_triggers_for_table(conn, schema, table)
        print(f"Instalación de outbox OK. Triggers creados para {len(uploads)} tablas.")
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


if __name__ == '__main__':
    run()