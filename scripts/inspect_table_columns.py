import os
import psycopg2


def get_conn_local():
    pwd = os.environ.get("PGPASSWORD") or "Matute03"
    return psycopg2.connect(host="localhost", port=5432, dbname="gimnasio", user="postgres", password=pwd)


def print_columns(table_name: str):
    conn = get_conn_local()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema='public' AND table_name=%s
                ORDER BY ordinal_position
                """,
                (table_name,)
            )
            rows = cur.fetchall()
            if not rows:
                print(f"No columns found for table '{table_name}'")
                return
            for name, dtype, nullable in rows:
                print(f"{name} | {dtype} | {'NULL' if nullable=='YES' else 'NOT NULL'}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    table = os.environ.get("TABLE_NAME") or "etiquetas"
    print_columns(table)