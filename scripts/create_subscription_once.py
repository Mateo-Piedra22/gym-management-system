# -*- coding: utf-8 -*-
import json
import psycopg2
import sys

def main():
    cfg = json.load(open(r'config\\config.json', 'r', encoding='utf-8'))
    lp = cfg.get('db_local') or cfg
    rp = cfg.get('db_remote')
    conninfo = f"host={rp['host']} port={rp['port']} dbname={rp['database']} user={rp['user']} password={rp.get('password','')} sslmode={rp.get('sslmode','require')}"
    print('conninfo:', conninfo)

    conn = psycopg2.connect(
        host=lp['host'], port=lp['port'], dbname=lp['database'],
        user=lp['user'], password=lp.get('password'),
        sslmode=lp.get('sslmode', 'prefer'), connect_timeout=lp.get('connect_timeout', 10)
    )
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute(
            "CREATE SUBSCRIPTION gym_sub CONNECTION %s PUBLICATION gym_pub WITH (create_slot = true, slot_name = gym_sub, enabled = true, copy_data = false)",
            (conninfo,)
        )
        print('CREATE SUBSCRIPTION gym_sub done')
    except Exception as e:
        print('CREATE SUBSCRIPTION failed:', e)
        sys.exit(1)
    finally:
        conn.close()

if __name__ == '__main__':
    main()