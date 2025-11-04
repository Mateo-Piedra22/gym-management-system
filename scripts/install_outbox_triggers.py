# -*- coding: utf-8 -*-
"""
Instala la tabla public.sync_outbox, la función de captura y los triggers
para las tablas configuradas con uploads_local_to_remote en config/sync_tables.json.

- Tabla: public.sync_outbox(id, schema_name, table_name, op, pk, data, dedup_key, txid, created_at)
- Función: public.sync_outbox_capture()
- Triggers: AFTER INSERT/UPDATE/DELETE por tabla

Requisitos:
- psycopg2
- Configuración de conexión local en config/config.json (db_local) o variables de entorno PGLOCAL_*
"""
import os
import json
import logging
from pathlib import Path
from typing import Optional

try:
    import psycopg2
    from psycopg2 import sql
except Exception:
    psycopg2 = None
    sql = None

PROJ_ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = PROJ_ROOT / 'config'
CONFIG_JSON = CONFIG_DIR / 'config.json'
SYNC_TABLES_JSON = CONFIG_DIR / 'sync_tables.json'

# -------------------- Conexión local (inline, sin dependencias externas) --------------------

def _parse_dsn(dsn: str, defaults: dict):
    host = defaults.get('host')
    port = int(defaults.get('port') or 5432)
    db = defaults.get('database')
    user = defaults.get('user')
    password = defaults.get('password')
    sslmode = defaults.get('sslmode') or 'prefer'
    appname = defaults.get('application_name') or 'gym_management_system'
    timeout = int(defaults.get('connect_timeout') or 10)

    if not dsn:
        return host, port, db, user, password, sslmode, appname, timeout
    try:
        from urllib.parse import urlparse, parse_qs
        u = urlparse(dsn)
        host = u.hostname or host
        port = int(u.port or port)
        db = (u.path or '').lstrip('/') or db
        user = u.username or user
        password = u.password or password
        q = parse_qs(u.query or '')
        sslmode = (q.get('sslmode') or [sslmode])[0]
        appname = (q.get('application_name') or [appname])[0]
        timeout = int((q.get('connect_timeout') or [timeout])[0])
    except Exception:
        pass
    return host, port, db, user, password, sslmode, appname, timeout


def _resolve_local_credentials(cfg: dict) -> dict:
    local = cfg.get('db_local') or cfg
    host = local.get('host') or cfg.get('host') or 'localhost'
    port = int(local.get('port') or cfg.get('port') or 5432)
    db = local.get('database') or cfg.get('database') or 'gimnasio'
    user = local.get('user') or cfg.get('user') or 'postgres'
    password_cfg = local.get('password') or cfg.get('password')
    sslmode = local.get('sslmode') or cfg.get('sslmode') or 'prefer'
    appname = local.get('application_name') or cfg.get('application_name') or 'gym_management_system'
    timeout = int(local.get('connect_timeout') or cfg.get('connect_timeout') or 10)

    dsn = os.environ.get('PGLOCAL_DSN') or ''
    host, port, db, user, pw_from_dsn, sslmode, appname, timeout = _parse_dsn(
        dsn,
        {
            'host': host,
            'port': port,
            'database': db,
            'user': user,
            'password': password_cfg,
            'sslmode': sslmode,
            'application_name': appname,
            'connect_timeout': timeout,
        },
    )

    password = os.environ.get('PGLOCAL_PASSWORD') or pw_from_dsn or password_cfg
    return {
        'host': host,
        'port': port,
        'database': db,
        'user': user,
        'password': password,
        'sslmode': sslmode,
        'application_name': appname,
        'connect_timeout': timeout,
        'dsn': dsn,
    }


def _connect(params: dict, dbname: Optional[str] = None):
    if psycopg2 is None:
        raise RuntimeError("psycopg2 no disponible")
    dbname = dbname or params.get('database')
    dsn = params.get('dsn') or ''
    timeout = int(params.get('connect_timeout') or 10)
    if dsn:
        return psycopg2.connect(dsn, connect_timeout=timeout)
    return psycopg2.connect(
        host=params['host'], port=params['port'], dbname=dbname,
        user=params['user'], password=params.get('password'), sslmode=params['sslmode'],
        application_name=params.get('application_name') or 'gym_management_system',
        connect_timeout=timeout,
    )

# -------------------------------------------------------------------------------------------

DDL_OUTBOX = r"""
CREATE TABLE IF NOT EXISTS public.sync_outbox (
  id BIGSERIAL PRIMARY KEY,
  schema_name TEXT NOT NULL,
  table_name TEXT NOT NULL,
  op TEXT NOT NULL,
  pk JSONB NOT NULL,
  data JSONB,
  dedup_key TEXT NOT NULL,
  txid BIGINT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS sync_outbox_dedup_key_idx ON public.sync_outbox(dedup_key);
CREATE INDEX IF NOT EXISTS sync_outbox_created_at_idx ON public.sync_outbox(created_at);
"""

FN_CAPTURE = r"""
CREATE OR REPLACE FUNCTION public.sync_outbox_capture()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
  v_schema text := TG_TABLE_SCHEMA;
  v_table text := TG_TABLE_NAME;
  v_op text := TG_OP; -- 'INSERT'/'UPDATE'/'DELETE'
  v_tx bigint := txid_current();
  v_data jsonb;
  v_pk jsonb := '{}'::jsonb;
  v_last_id text;
  pk_cols text[];
  rec_new jsonb;
  rec_old jsonb;
  key text;
  dedup text;
BEGIN
  -- Resolver columnas de PK
  SELECT array_agg(a.attname::text ORDER BY a.attnum)
  INTO pk_cols
  FROM pg_index i
  JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
  WHERE i.indrelid = format('%I.%I', v_schema, v_table)::regclass AND i.indisprimary;

  IF v_op = 'INSERT' THEN
    rec_new := to_jsonb(NEW);
    v_data := rec_new;
    v_last_id := COALESCE(rec_new ->> 'last_op_id', NULL);
    IF pk_cols IS NOT NULL THEN
      v_pk := '{}'::jsonb;
      FOREACH key IN ARRAY pk_cols LOOP
        v_pk := v_pk || jsonb_build_object(key, rec_new -> key);
      END LOOP;
    END IF;
  ELSIF v_op = 'UPDATE' THEN
    rec_new := to_jsonb(NEW);
    rec_old := to_jsonb(OLD);
    v_data := '{}'::jsonb;
    FOR key IN SELECT column_name FROM information_schema.columns WHERE table_schema = v_schema AND table_name = v_table LOOP
      IF (rec_new -> key) IS DISTINCT FROM (rec_old -> key) THEN
        IF pk_cols IS NULL OR NOT key = ANY (pk_cols) THEN
          v_data := v_data || jsonb_build_object(key, rec_new -> key);
        END IF;
      END IF;
    END LOOP;
    v_last_id := COALESCE(rec_new ->> 'last_op_id', rec_old ->> 'last_op_id');
    IF pk_cols IS NOT NULL THEN
      v_pk := '{}'::jsonb;
      FOREACH key IN ARRAY pk_cols LOOP
        v_pk := v_pk || jsonb_build_object(key, rec_new -> key);
      END LOOP;
    END IF;
    IF v_data = '{}'::jsonb THEN
      RETURN NULL; -- No hay cambios relevantes
    END IF;
  ELSIF v_op = 'DELETE' THEN
    rec_old := to_jsonb(OLD);
    IF pk_cols IS NOT NULL THEN
      v_pk := '{}'::jsonb;
      FOREACH key IN ARRAY pk_cols LOOP
        v_pk := v_pk || jsonb_build_object(key, rec_old -> key);
      END LOOP;
    END IF;
    v_data := NULL;
    v_last_id := COALESCE(rec_old ->> 'last_op_id', NULL);
  ELSE
    RETURN NULL;
  END IF;

  -- Clave de deduplicación estable preferente por last_op_id + PK; fallback a txid
  dedup := md5( (v_schema || '.' || v_table || ':' || v_op || ':' || COALESCE(v_pk::text, '{}') || ':' || COALESCE(v_last_id, v_tx::text)) );

  INSERT INTO public.sync_outbox(schema_name, table_name, op, pk, data, dedup_key, txid)
  VALUES (v_schema, v_table, v_op, COALESCE(v_pk, '{}'::jsonb), v_data, dedup, v_tx)
  ON CONFLICT (dedup_key) DO NOTHING;

  RETURN NULL;
END;
$$;
"""

TRIGGER_TEMPLATES = {
    'INSERT': """
        DROP TRIGGER IF EXISTS sync_outbox_ins ON {ident_schema}.{ident_table};
        CREATE TRIGGER sync_outbox_ins AFTER INSERT ON {ident_schema}.{ident_table}
        FOR EACH ROW EXECUTE FUNCTION public.sync_outbox_capture();
    """,
    'UPDATE': """
        DROP TRIGGER IF EXISTS sync_outbox_upd ON {ident_schema}.{ident_table};
        CREATE TRIGGER sync_outbox_upd AFTER UPDATE ON {ident_schema}.{ident_table}
        FOR EACH ROW EXECUTE FUNCTION public.sync_outbox_capture();
    """,
    'DELETE': """
        DROP TRIGGER IF EXISTS sync_outbox_del ON {ident_schema}.{ident_table};
        CREATE TRIGGER sync_outbox_del AFTER DELETE ON {ident_schema}.{ident_table}
        FOR EACH ROW EXECUTE FUNCTION public.sync_outbox_capture();
    """,
}


def _load_config_json(path: Path) -> dict:
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}


def _load_sync_tables(path: Path) -> dict:
    data = _load_config_json(path)
    if not isinstance(data, dict):
        return {}
    return data


def _connect_local():
    if psycopg2 is None:
        raise RuntimeError("psycopg2 no disponible")
    cfg = _load_config_json(CONFIG_JSON)
    params = _resolve_local_credentials(cfg if isinstance(cfg, dict) else {})
    return _connect(params)


def _ensure_required_table(cur, schema: str, table: str) -> None:
    """Crea tablas críticas si faltan, con esquema mínimo usado por la app."""
    if table == 'professor_availability':
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {schema}.professor_availability (
            id SERIAL PRIMARY KEY,
            profesor_id INTEGER NOT NULL REFERENCES {schema}.profesores(id) ON DELETE CASCADE,
            date DATE NOT NULL,
            start_time TIME NOT NULL,
            end_time TIME NOT NULL,
            status VARCHAR(50) NOT NULL,
            notes TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(profesor_id, date)
        );
        """
        cur.execute(ddl)
    elif table == 'theme_events':
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {schema}.theme_events (
            id SERIAL PRIMARY KEY,
            evento TEXT NOT NULL,
            theme_id INTEGER NOT NULL,
            fecha_inicio DATE NOT NULL,
            fecha_fin DATE NOT NULL,
            descripcion TEXT,
            activo BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """
        cur.execute(ddl)


def _install_outbox_triggers(conn, uploads: list, schema: str = 'public') -> int:
    count = 0
    # Ejecutar DDL en autocommit para minimizar bloqueos y evitar abortos de transacción
    prev_autocommit = getattr(conn, 'autocommit', False)
    try:
        conn.autocommit = True
        cur = conn.cursor()
        # Timeouts defensivos para evitar deadlocks/largos bloqueos
        try:
            cur.execute("SET lock_timeout = '1500ms'")
            cur.execute("SET statement_timeout = '5000ms'")
        except Exception:
            pass
        # Crear tabla e índices de outbox y función de captura
        cur.execute(DDL_OUTBOX)
        cur.execute(FN_CAPTURE)
        # Instalar triggers por tabla
        for t in uploads:
            if not isinstance(t, str) or not t.strip():
                continue
            tname = t.strip()
            # Crear tablas críticas si faltan (evita 'tabla no encontrada')
            try:
                cur.execute("SELECT to_regclass(%s)", (f"{schema}.{tname}",))
                reg = cur.fetchone()
            except Exception:
                reg = None
            if not reg or reg[0] is None:
                try:
                    _ensure_required_table(cur, schema, tname)
                    # Revalidar existencia
                    cur.execute("SELECT to_regclass(%s)", (f"{schema}.{tname}",))
                    reg2 = cur.fetchone()
                    if not reg2 or reg2[0] is None:
                        logging.warning(f"Tabla no encontrada: {schema}.{tname}, se omite")
                        continue
                except Exception as ce:
                    logging.warning(f"No se pudo crear tabla requerida {schema}.{tname}: {ce}")
                    continue
            for op, tmpl in TRIGGER_TEMPLATES.items():
                ddl = tmpl.format(ident_schema=schema, ident_table=tname)
                try:
                    cur.execute(ddl)
                except Exception as te:
                    # Evitar abortos de transacción persistentes
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    logging.warning(f"No se pudo instalar trigger {op} en {schema}.{tname}: {te}")
            count += 1
    finally:
        try:
            conn.autocommit = prev_autocommit
        except Exception:
            pass
    return count


def run():
    # Ejecuta main() y devuelve dict con resultado
    try:
        code = main()
    except Exception as e:
        return {"ok": False, "error": str(e)}
    return {"ok": (code == 0), "exit_code": code}

def main() -> int:
    logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
    if psycopg2 is None:
        print(json.dumps({"ok": False, "error": "psycopg2_missing"}, ensure_ascii=False))
        return 2
    try:
        tables_cfg = _load_sync_tables(SYNC_TABLES_JSON)
        uploads = list(tables_cfg.get('uploads_local_to_remote') or [])
        if not uploads:
            print(json.dumps({"ok": False, "error": "no_upload_tables"}, ensure_ascii=False))
            return 3
        conn = _connect_local()
        n = _install_outbox_triggers(conn, uploads, schema='public')
        print(json.dumps({"ok": True, "installed_tables": n, "schema": "public"}, ensure_ascii=False))
        return 0
    except Exception as e:
        try:
            logging.exception(f"install_outbox_triggers falló: {e}")
        except Exception:
            pass
        print(json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())