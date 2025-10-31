import uuid
from typing import Tuple


def _ensure_node_state_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS node_state (
              key   TEXT PRIMARY KEY,
              value TEXT NOT NULL
            )
            """
        )
        conn.commit()


def get_or_create_node_id(conn) -> str:
    """Obtiene un node_id único desde node_state, creándolo si no existe."""
    _ensure_node_state_table(conn)
    with conn.cursor() as cur:
        cur.execute("SELECT value FROM node_state WHERE key = 'node_id'")
        r = cur.fetchone()
        if r and r[0]:
            return str(r[0])
        # Generar UUID preferentemente en cliente
        nid = str(uuid.uuid4())
        cur.execute(
            """
            INSERT INTO node_state(key, value)
            VALUES ('node_id', %s)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
            """,
            (nid,)
        )
        conn.commit()
        return nid


def _ensure_webapp_sequence(conn) -> None:
    """Asegura la existencia de la secuencia global usada por el webapp."""
    with conn.cursor() as cur:
        cur.execute("CREATE SEQUENCE IF NOT EXISTS webapp_logical_ts_seq START 1")
        conn.commit()


def get_next_logical_ts(conn) -> int:
    """Devuelve el siguiente logical_ts.
    Prioriza la secuencia webapp_logical_ts_seq; de lo contrario usa node_state.logical_ts_counter.
    Inicializa el contador si falta usando el máximo observado en tablas con logical_ts.
    """
    _ensure_node_state_table(conn)
    _ensure_webapp_sequence(conn)
    with conn.cursor() as cur:
        # 1) Intentar secuencia del webapp
        try:
            cur.execute("SELECT nextval('webapp_logical_ts_seq')")
            r = cur.fetchone()
            if r and r[0]:
                conn.commit()
                return int(r[0])
        except Exception:
            # Continuar con node_state
            conn.rollback()

        # 2) Intentar contador en node_state
        try:
            cur.execute(
                """
                UPDATE node_state
                SET value = (value::BIGINT + 1)::TEXT
                WHERE key = 'logical_ts_counter'
                RETURNING value::BIGINT
                """
            )
            r = cur.fetchone()
            if r and r[0]:
                conn.commit()
                return int(r[0])
        except Exception:
            conn.rollback()

        # 3) Inicialización basada en máximo observado de todas las tablas con logical_ts
        max_ts = 0
        try:
            cur.execute(
                """
                SELECT table_name
                FROM information_schema.columns
                WHERE table_schema = 'public' AND column_name = 'logical_ts'
                """
            )
            tables = [row[0] for row in (cur.fetchall() or [])]
            for tbl in tables:
                if tbl == 'node_state':
                    continue
                try:
                    cur.execute(f"SELECT COALESCE(MAX(logical_ts), 0) FROM public.{tbl}")
                    res = cur.fetchone()
                    val = int((res or [0])[0] or 0)
                    if val > max_ts:
                        max_ts = val
                except Exception:
                    conn.rollback()
        except Exception:
            conn.rollback()

        init_val = max_ts + 1
        cur.execute(
            """
            INSERT INTO node_state(key, value)
            VALUES ('logical_ts_counter', %s)
            ON CONFLICT (key) DO NOTHING
            """,
            (str(init_val),)
        )
        # Leer el valor (insertado o existente)
        cur.execute("SELECT value::BIGINT FROM node_state WHERE key = 'logical_ts_counter'")
        r2 = cur.fetchone()
        conn.commit()
        return int((r2 or [init_val])[0])


def assign_logical_fields(conn) -> Tuple[int, str]:
    """Obtiene (logical_ts, last_op_id) para una operación de escritura local."""
    ts = get_next_logical_ts(conn)
    op_id = str(uuid.uuid4())
    return ts, op_id