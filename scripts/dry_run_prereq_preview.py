import os, sys, json, types

# Ensure project root is importable
PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, PROJECT_ROOT)

# Import prerequisites module
import utils_modules.prerequisites as p  # type: ignore

# Stub replication module to avoid side effects
rep = types.ModuleType('utils_modules.replication_setup')

def ensure_bidirectional_replication(cfg):
    return {'ok': True, 'dry_run': True, 'mode': 'bidirectional'}

def ensure_logical_replication(cfg):
    return {'ok': True, 'dry_run': True, 'mode': 'logical'}

rep.ensure_bidirectional_replication = ensure_bidirectional_replication
rep.ensure_logical_replication = ensure_logical_replication
sys.modules['utils_modules.replication_setup'] = rep

# Patch functions to avoid side effects during dry-run
p.install_postgresql_17 = lambda: (False, 'dry-run: skipped installation')
p.create_database_from_config = lambda *a, **kw: (True, 'dry-run: pretend DB created')
p.ensure_scheduled_tasks = lambda device_id: {'ok': True, 'dry_run': True, 'tasks': {}}
p.ensure_postgres_network_access = lambda cfg: {'ok': True, 'dry_run': True}

# Non-destructive bootstrap preview to avoid renames and markers

def dry_bootstrap():
    try:
        cfg_dir = p.CONFIG_DIR
        path = os.path.join(cfg_dir, 'remote_bootstrap.json')
        if not os.path.exists(path):
            return {'applied': False, 'message': 'no_bootstrap_file'}
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f) or {}
        remote = data.get('remote') or data
        dsn = str(remote.get('dsn') or data.get('remote_dsn') or os.getenv('PGREMOTE_DSN', ''))
        host, port, db, user, password, sslmode, timeout, appname = p._parse_dsn_bootstrap(dsn, {
            'host': remote.get('host'),
            'port': remote.get('port'),
            'database': remote.get('database'),
            'user': remote.get('user'),
            'password': remote.get('password'),
            'sslmode': remote.get('sslmode'),
            'application_name': remote.get('application_name'),
            'connect_timeout': remote.get('connect_timeout'),
        })
        webapp = data.get('webapp') or {}
        pub_url = (webapp.get('public_base_url') or data.get('public_webapp_url') or '').strip()
        return {
            'applied': False, 'message': 'dry_run_preview',
            'remote': {'host': host, 'port': port, 'database': db, 'user': user},
            'webapp_base_url': pub_url,
            'sync_upload_token_present': bool(str(data.get('sync_upload_token') or '').strip()),
            'owner_password_present': bool(str(data.get('owner_password') or '').strip()),
        }
    except Exception as e:
        return {'applied': False, 'message': 'dry_run_error', 'error': str(e)}

p.apply_remote_bootstrap_if_present = dry_bootstrap

# Stub database to avoid any real DB writes
_db = types.ModuleType('database')
class DummyDBM:
    def obtener_configuracion(self, clave):
        if clave == 'owner_password':
            # Pretend it's already set to avoid writes in dry-run
            return 'already'
        return None
    def actualizar_configuracion(self, clave, valor):
        return True
_db.DatabaseManager = DummyDBM
sys.modules['database'] = _db

# Use a fixed device_id for dry-run
device_id = 'dry-run'
res = p.ensure_prerequisites(device_id)

# Print JSON result
print(json.dumps(res, ensure_ascii=False, indent=2))