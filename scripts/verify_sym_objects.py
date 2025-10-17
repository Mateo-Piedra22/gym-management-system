import os
import psycopg2


def verify_local():
    pwd = os.environ.get('PGPASSWORD') or 'Matute03'
    conn = psycopg2.connect(host='localhost', port=5432, dbname='gimnasio', user='postgres', password=pwd)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH objs AS (
                  SELECT 'table' AS kind, schemaname AS schema_name, tablename AS obj_name
                  FROM pg_tables WHERE tablename ILIKE 'sym_%'
                  UNION ALL
                  SELECT 'sequence', sequence_schema, sequence_name FROM information_schema.sequences WHERE sequence_name ILIKE 'sym_%'
                  UNION ALL
                  SELECT 'function', n.nspname, p.proname || '(' || pg_get_function_identity_arguments(p.oid) || ')'
                  FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE p.proname ILIKE 'sym_%'
                  UNION ALL
                  SELECT 'trigger', n.nspname, t.tgname
                  FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid JOIN pg_namespace n ON n.oid = c.relnamespace
                  WHERE NOT t.tgisinternal AND t.tgname ILIKE 'sym_%'
                )
                SELECT * FROM objs ORDER BY kind, schema_name, obj_name;
                """
            )
            rows = cur.fetchall()
            print('Objetos remanentes (local):')
            for row in rows:
                print(row)
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == '__main__':
    verify_local()