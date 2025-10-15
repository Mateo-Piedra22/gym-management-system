import json, os, sys, traceback, uuid
from pathlib import Path
from datetime import date
import psycopg2

# Asegura que el proyecto esté en sys.path para importar config si es necesario
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
try:
    import keyring
except Exception:
    keyring = None
try:
    from config import KEYRING_SERVICE_NAME
except Exception:
    KEYRING_SERVICE_NAME = 'GymMS_DB'


def load_cfg():
    base_dir = Path(__file__).resolve().parent.parent
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
    application_name = profile.get('application_name', 'gym_ms_xfer_diag')
    options = profile.get('options', '')
    password = profile.get('password', '')
    if not password:
        password = resolve_password(user, host, port, cfg.get('password', ''))
    params = dict(host=host, port=port, dbname=dbname, user=user, password=password,
                  sslmode=sslmode, connect_timeout=connect_timeout, application_name=application_name)
    if options:
        params['options'] = options
    return params


def ensure_user(conn, uid, nombre, telefono="000-000", pin="1234"):
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM usuarios WHERE id = %s", (uid,))
        row = cur.fetchone()
        if row is None:
            cur.execute(
                "INSERT INTO usuarios (id, nombre, telefono, pin, rol, activo) VALUES (%s, %s, %s, %s, 'socio', TRUE)",
                (uid, nombre, telefono, pin),
            )

def upsert_pago(conn, uid, monto, mes, anio):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pagos (usuario_id, monto, mes, año, fecha_pago)
            VALUES (%s, %s, %s, %s, NOW())
            ON CONFLICT (usuario_id, mes, año)
            DO UPDATE SET monto = EXCLUDED.monto, fecha_pago = NOW()
            RETURNING id
            """,
            (uid, monto, mes, anio),
        )
        return cur.fetchone()[0]

def upsert_asistencia(conn, uid, f: date):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO asistencias (usuario_id, fecha, hora_registro)
            VALUES (%s, %s, NOW())
            ON CONFLICT (usuario_id, fecha)
            DO UPDATE SET hora_registro = NOW()
            RETURNING id
            """,
            (uid, f),
        )
        return cur.fetchone()[0]

def upsert_etiqueta(conn, nombre, color="#3498db", descripcion=None):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO etiquetas (nombre, color, descripcion)
            VALUES (%s, %s, %s)
            ON CONFLICT (nombre) DO UPDATE SET descripcion = COALESCE(EXCLUDED.descripcion, etiquetas.descripcion)
            RETURNING id
            """,
            (nombre, color, descripcion),
        )
        return cur.fetchone()[0]

def upsert_usuario_etiqueta(conn, uid, etiqueta_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO usuario_etiquetas (usuario_id, etiqueta_id)
            VALUES (%s, %s)
            ON CONFLICT (usuario_id, etiqueta_id) DO NOTHING
            """,
            (uid, etiqueta_id),
        )

def insert_estado(conn, uid, estado, descripcion=None):
    with conn.cursor() as cur:
        # Arreglar secuencia desincronizada si existiera
        try:
            cur.execute("SELECT pg_get_serial_sequence('usuario_estados', 'id')")
            seq = cur.fetchone()[0]
            if seq:
                cur.execute("SELECT COALESCE(MAX(id), 0) FROM usuario_estados")
                max_id = cur.fetchone()[0]
                cur.execute("SELECT setval(%s, %s)", (seq, max_id))
        except Exception:
            pass
        cur.execute(
            "INSERT INTO usuario_estados (usuario_id, estado, descripcion, activo) VALUES (%s, %s, %s, TRUE) RETURNING id",
            (uid, estado, descripcion),
        )
        return cur.fetchone()[0]

def ensure_clase_y_horario(conn, nombre_clase, dia_semana="Lunes", hora_inicio="08:00", hora_fin="09:00"):
    with conn.cursor() as cur:
        # Clase
        cur.execute("INSERT INTO clases (nombre, descripcion, activa) VALUES (%s, %s, TRUE) ON CONFLICT (nombre) DO NOTHING",
                    (nombre_clase, f"Clase de prueba {nombre_clase}"))
        cur.execute("SELECT id FROM clases WHERE nombre = %s", (nombre_clase,))
        clase_id = cur.fetchone()[0]
        # Horario (no hay constraint único, insertamos uno)
        cur.execute(
            """
            INSERT INTO clases_horarios (clase_id, dia_semana, hora_inicio, hora_fin, activo)
            VALUES (%s, %s, %s, %s, TRUE)
            RETURNING id
            """,
            (clase_id, dia_semana, hora_inicio, hora_fin),
        )
        horario_id = cur.fetchone()[0]
        return clase_id, horario_id

def upsert_clase_usuario(conn, clase_horario_id, uid):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO clase_usuarios (clase_horario_id, usuario_id)
            VALUES (%s, %s)
            ON CONFLICT (clase_horario_id, usuario_id) DO NOTHING
            RETURNING id
            """,
            (clase_horario_id, uid),
        )
        row = cur.fetchone()
        return row[0] if row else None

def run_dataset(conn, uid, token):
    """Crea/actualiza datos reales y persistentes para el usuario uid usando un token único.
    Devuelve IDs relevantes creados/afectados.
    """
    results = {}
    ensure_user(conn, uid, f"Usuario Diag {uid} {token}")
    # Pago del mes actual
    today = date.today()
    pago_id = upsert_pago(conn, uid, 87.00, today.month, today.year)
    results['pago_id'] = pago_id
    # Asistencia hoy
    asis_id = upsert_asistencia(conn, uid, today)
    results['asistencia_id'] = asis_id
    # Etiqueta y asignación
    tag_name = f"diag_tag_{uid}_{token}"
    tag_id = upsert_etiqueta(conn, tag_name, descripcion=f"Etiqueta diag {token}")
    upsert_usuario_etiqueta(conn, uid, tag_id)
    results['etiqueta_id'] = tag_id
    # Estado de usuario
    estado_id = insert_estado(conn, uid, f"diag_estado_{token}", "Estado de diagnóstico")
    results['estado_id'] = estado_id
    # Clase y horario + inscripción del usuario
    clase_nombre = f"Clase Diag {uid} {token}"
    clase_id, horario_id = ensure_clase_y_horario(conn, clase_nombre)
    cu_id = upsert_clase_usuario(conn, horario_id, uid)
    results['clase_id'] = clase_id
    results['clase_horario_id'] = horario_id
    results['clase_usuario_id'] = cu_id
    return results

def copy_verify_counts(conn, uid, token):
    """Devuelve conteos de filas por tablas clave relacionados al token/uid."""
    out = {}
    with conn.cursor() as cur:
        # pagos del mes actual
        cur.execute("SELECT COUNT(*) FROM pagos WHERE usuario_id = %s AND mes = EXTRACT(MONTH FROM CURRENT_DATE)::int AND año = EXTRACT(YEAR FROM CURRENT_DATE)::int", (uid,))
        out['pagos'] = cur.fetchone()[0]
        # asistencias en una ventana de +-1 día (para evitar desfases por zona horaria)
        cur.execute("SELECT COUNT(*) FROM asistencias WHERE usuario_id = %s AND fecha BETWEEN CURRENT_DATE - INTERVAL '1 day' AND CURRENT_DATE + INTERVAL '1 day'", (uid,))
        out['asistencias'] = cur.fetchone()[0]
        # etiqueta con token
        cur.execute("SELECT COUNT(*) FROM etiquetas WHERE nombre = %s", (f"diag_tag_{uid}_{token}",))
        out['etiquetas'] = cur.fetchone()[0]
        # usuario_etiquetas asignaciones
        cur.execute("""
            SELECT COUNT(*) FROM usuario_etiquetas ue 
            JOIN etiquetas e ON e.id = ue.etiqueta_id
            WHERE ue.usuario_id = %s AND e.nombre = %s
        """, (uid, f"diag_tag_{uid}_{token}"))
        out['usuario_etiquetas'] = cur.fetchone()[0]
        # estados
        cur.execute("SELECT COUNT(*) FROM usuario_estados WHERE usuario_id = %s AND estado = %s", (uid, f"diag_estado_{token}"))
        out['usuario_estados'] = cur.fetchone()[0]
        # clase/hora
        cur.execute("SELECT COUNT(*) FROM clases WHERE nombre = %s", (f"Clase Diag {uid} {token}",))
        out['clases'] = cur.fetchone()[0]
        cur.execute("""
            SELECT COUNT(*) FROM clase_usuarios cu
            JOIN clases_horarios ch ON ch.id = cu.clase_horario_id
            JOIN clases c ON c.id = ch.clase_id
            WHERE cu.usuario_id = %s AND c.nombre = %s
        """, (uid, f"Clase Diag {uid} {token}"))
        out['clase_usuarios'] = cur.fetchone()[0]
    return out

def bidir_persist_test(local_params, remote_params, uid=87):
    out = {'ok': False}
    lc = rc = None
    token_local = uuid.uuid4().hex[:8]
    token_remote = uuid.uuid4().hex[:8]
    try:
        lc = psycopg2.connect(**local_params)
        rc = psycopg2.connect(**remote_params)

        # 1) Crear dataset en local y replicarlo en remoto (creando las mismas entidades)
        with lc:
            l_created = run_dataset(lc, uid, token_local)
        with rc:
            r_up = run_dataset(rc, uid, token_local)

        # 2) Crear dataset diferente en remoto y replicarlo en local
        with rc:
            r_created = run_dataset(rc, uid, token_remote)
        with lc:
            l_up = run_dataset(lc, uid, token_remote)

        # 3) Verificación de conteos en ambos extremos para ambos tokens
        with lc:
            lc_counts_local = copy_verify_counts(lc, uid, token_local)
            lc_counts_remote = copy_verify_counts(lc, uid, token_remote)
        with rc:
            rc_counts_local = copy_verify_counts(rc, uid, token_local)
            rc_counts_remote = copy_verify_counts(rc, uid, token_remote)

        out.update({
            'ok': True,
            'uid': uid,
            'tokens': {'local_token': token_local, 'remote_token': token_remote},
            'created': {
                'local_created': l_created,
                'remote_recreated_for_local': r_up,
                'remote_created': r_created,
                'local_recreated_for_remote': l_up
            },
            'counts': {
                'local_db_for_local_token': lc_counts_local,
                'local_db_for_remote_token': lc_counts_remote,
                'remote_db_for_local_token': rc_counts_local,
                'remote_db_for_remote_token': rc_counts_remote,
            }
        })
    except Exception as e:
        out['error'] = f"{e}"
        out['trace'] = traceback.format_exc()
    finally:
        for c in (lc, rc):
            try:
                if c:
                    c.close()
            except Exception:
                pass
    return out


def main():
    results = {}
    if not local or not remote:
        print(json.dumps({'ok': False, 'error': 'Perfiles local o remoto no configurados'}, ensure_ascii=False))
        return
    lp = to_connect_params(local)
    rp = to_connect_params(remote)

    res = bidir_persist_test(lp, rp, uid=87)
    print(json.dumps(res, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()