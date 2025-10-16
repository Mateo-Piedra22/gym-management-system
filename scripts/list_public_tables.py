import os
import psycopg2


def list_public_tables():
    pwd = os.environ.get("PGPASSWORD") or "Matute03"
    conn = psycopg2.connect(host="localhost", port=5432, dbname="gimnasio", user="postgres", password=pwd)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema='public' AND table_type='BASE TABLE' AND table_name NOT LIKE 'sym_%'
                ORDER BY 1
                """
            )
            rows = cur.fetchall()
            for (name,) in rows:
                print(name)
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    list_public_tables()