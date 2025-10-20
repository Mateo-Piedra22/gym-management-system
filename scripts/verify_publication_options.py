import json
import psycopg2
from psycopg2.extras import RealDictCursor

CONFIG_PATH = 'config/config.json'


def load_config():
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        return json.load(f)


def connect_db(params):
    conn = psycopg2.connect(
        host=params.get('host', 'localhost'),
        port=params.get('port', 5432),
        dbname=params.get('database'),
        user=params.get('user'),
        password=params.get('password'),
        sslmode=params.get('sslmode', 'prefer'),
        connect_timeout=params.get('connect_timeout', 10),
        application_name=params.get('application_name', 'verify_publication_options'),
    )
    return conn


def has_pubviaroot_column(cur):
    cur.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'pg_catalog'
          AND table_name = 'pg_publication'
          AND column_name = 'pubviaroot'
        LIMIT 1
        """
    )
    return cur.fetchone() is not None


def get_publication_options(conn):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        via_root_supported = has_pubviaroot_column(cur)
        if via_root_supported:
            cur.execute(
                """
                SELECT pubname, pubinsert, pubupdate, pubdelete, pubtruncate, pubviaroot
                FROM pg_catalog.pg_publication
                ORDER BY pubname
                """
            )
        else:
            cur.execute(
                """
                SELECT pubname, pubinsert, pubupdate, pubdelete, pubtruncate
                FROM pg_catalog.pg_publication
                ORDER BY pubname
                """
            )
        rows = cur.fetchall()
        return {
            'via_root_supported': via_root_supported,
            'publications': [dict(r) for r in rows],
        }


def main():
    cfg = load_config()
    results = {}
    for label, key in [('local', 'db_local'), ('remote', 'db_remote')]:
        params = cfg.get(key)
        if not params:
            results[label] = {'error': f'No hay parámetros para {label}'}
            continue
        try:
            with connect_db(params) as conn:
                results[label] = get_publication_options(conn)
        except Exception as e:
            results[label] = {'error': str(e)}
    print(json.dumps(results, indent=2, ensure_ascii=False))


if __name__ == '__main__':
    main()