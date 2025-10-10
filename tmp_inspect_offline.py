import sqlite3, json, os
DB='offline_queue.sqlite'
print('CWD:', os.getcwd())
if not os.path.exists(DB):
    print('No existe', DB)
    raise SystemExit(0)
conn=sqlite3.connect(DB)
conn.row_factory=sqlite3.Row
cur=conn.cursor()
cur.execute("SELECT status, COUNT(1) as c FROM offline_ops GROUP BY status")
print('Resumen estados:')
for r in cur.fetchall():
    print(f" - {r['status']}: {r['c']}")
cur.execute("SELECT id, category, func_name, attempts, last_error, args_json, kwargs_json, created_at FROM offline_ops WHERE status='pending' ORDER BY id")
rows=cur.fetchall()
print('\nPendientes:', len(rows))
for r in rows[:50]:
    args=json.loads(r['args_json'] or '[]')
    kwargs=json.loads(r['kwargs_json'] or '{}')
    print(f"id={r['id']} cat={r['category']} fn={r['func_name']} att={r['attempts']} err={r['last_error']!r}")
    print('  args=',args)
    print('  kwargs=',kwargs)
cur.execute("SELECT id, status, attempts, last_error FROM offline_ops WHERE last_error IS NOT NULL ORDER BY id DESC LIMIT 20")
print('\nUltimos errores:')
for r in cur.fetchall():
    print(f"id={r['id']} status={r['status']} attempts={r['attempts']} err={r['last_error']}")
conn.close()
