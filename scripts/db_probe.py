import json, os, sys, traceback
from pathlib import Path
# Asegura que el proyecto esté en sys.path para importar config
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import psycopg2
try:
    import keyring
except Exception:
    keyring = None
from config import KEYRING_SERVICE_NAME


def load_cfg():
    try:
        base_dir = Path(__file__).resolve().parent.parent
    except Exception:
        base_dir = Path.cwd()
    cfg_path = base_dir / 'config' / 'config.json'
    if cfg_path.exists():
        with open(cfg_path, 'r', encoding='utf-8') as f:
            try:
                return json.load(f)
            except Exception:
                return {}
    return {}


cfg = load_cfg()
local = cfg.get('db_local', {}) or {}
remote = cfg.get('db_remote', {}) or {}


def resolve_password(user, host, port, fallback_cfg_pwd):
    pwd = ''
    # Primero intenta entradas compuestas en keyring para coexistencia local/remoto
    try:
        if keyring and user:
            for account in (
                f"{user}@{host}:{port}",
                f"{user}@{host}",
                user,
            ):
                try:
                    pwd = keyring.get_password(KEYRING_SERVICE_NAME, account) or ''
                except Exception:
                    pwd = ''
                if pwd:
                    break
    except Exception:
        pwd = ''
    if not pwd:
        env = os.environ
        # Variable específica para remoto (si coincide host/port/user)
        if (
            user == (remote.get('user') or '') and
            str(host) == str(remote.get('host')) and
            str(port) == str(remote.get('port'))
        ):
            pwd = env.get('DB_PASSWORD_REMOTE', '')
        if not pwd:
            pwd = env.get('DB_PASSWORD', '') or (fallback_cfg_pwd or '')
    return pwd


def to_connect_params(profile):
    host = profile.get('host', 'localhost')
    try:
        port = int(profile.get('port', 5432) or 5432)
    except Exception:
        port = 5432
    dbname = profile.get('database') or profile.get('dbname') or 'gimnasio'
    user = profile.get('user', 'postgres')
    sslmode = profile.get('sslmode', 'prefer')
    try:
        connect_timeout = int(profile.get('connect_timeout', 5) or 5)
    except Exception:
        connect_timeout = 5
    application_name = profile.get('application_name', 'gym_ms_diag')
    options = profile.get('options', '')
    password = profile.get('password', '')
    if not password:
        password = resolve_password(user, host, port, cfg.get('password', ''))
    params = dict(host=host, port=port, dbname=dbname, user=user, password=password,
                  sslmode=sslmode, connect_timeout=connect_timeout, application_name=application_name)
    if options:
        params['options'] = options
    return params


def probe(params):
    out = {'ok': False, 'error': None, 'server_version': None, 'write_ok': False}
    try:
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                cur.execute('select version()')
                out['server_version'] = cur.fetchone()[0]
                # Prueba de escritura no destructiva con tabla temporal
                cur.execute('create temporary table if not exists conn_probe (id serial primary key, note text, ts timestamptz default now())')
                cur.execute("insert into conn_probe(note) values (%s) returning id", ('diag',))
                _id = cur.fetchone()[0]
                cur.execute('select count(*) from conn_probe')
                cnt = cur.fetchone()[0]
                out['write_ok'] = cnt >= 1
            conn.commit()
        finally:
            try:
                conn.close()
            except Exception:
                pass
        out['ok'] = True
    except Exception as e:
        out['error'] = str(e)
        out['trace'] = traceback.format_exc()
    return out


results = {}
profiles = {'local': local, 'remote': remote}
for name, prof in profiles.items():
    if not prof:
        results[name] = {'ok': False, 'error': 'perfil no configurado'}
        continue
    params = to_connect_params(prof)
    results[name] = probe(params)

print(json.dumps(results, ensure_ascii=False, indent=2))
