import json, os, sys, traceback, uuid
from pathlib import Path
from datetime import date, time
import psycopg2

# Asegura que el proyecto esté en sys.path para importar config
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
    application_name = profile.get('application_name', 'gym_ms_full_diag')
    options = profile.get('options', '')
    password = profile.get('password', '')
    if not password:
        password = resolve_password(user, host, port, cfg.get('password', ''))
    params = dict(host=host, port=port, dbname=dbname, user=user, password=password,
                  sslmode=sslmode, connect_timeout=connect_timeout, application_name=application_name)
    if options:
        params['options'] = options
    return params


# ---- Utilidades de inserción/aseguramiento ----

def fix_sequence(conn, table, id_col='id'):
    with conn.cursor() as cur:
        try:
            cur.execute("SELECT pg_get_serial_sequence(%s, %s)", (table, id_col))
            seq_row = cur.fetchone()
            if not seq_row:
                return
            seq = seq_row[0]
            if seq:
                cur.execute(f"SELECT COALESCE(MAX({id_col}), 0) FROM {table}")
                max_id = cur.fetchone()[0]
                cur.execute("SELECT setval(%s, %s)", (seq, max_id))
        except Exception:
            pass


def ensure_user(conn, uid, nombre, telefono="000-000", pin="1234"):
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM usuarios WHERE id = %s", (uid,))
        row = cur.fetchone()
        if row is None:
            cur.execute(
                "INSERT INTO usuarios (id, nombre, telefono, pin, rol, activo) VALUES (%s, %s, %s, %s, 'socio', TRUE)",
                (uid, nombre, telefono, pin),
            )


def ensure_tipo_clase(conn, nombre):
    with conn.cursor() as cur:
        cur.execute("INSERT INTO tipos_clases (nombre) VALUES (%s) ON CONFLICT (nombre) DO NOTHING", (nombre,))
        cur.execute("SELECT id FROM tipos_clases WHERE nombre = %s", (nombre,))
        row = cur.fetchone()
        return row[0]


def ensure_clase_y_horario(conn, nombre_clase, tipo_id=None, dia_semana="Lunes", hora_inicio="08:00", hora_fin="09:00"):
    with conn.cursor() as cur:
        cur.execute("INSERT INTO clases (nombre, descripcion, activa) VALUES (%s, %s, TRUE) ON CONFLICT (nombre) DO NOTHING",
                    (nombre_clase, f"Clase {nombre_clase}"))
        cur.execute("UPDATE clases SET tipo_clase_id = COALESCE(tipo_clase_id, %s) WHERE nombre = %s", (tipo_id, nombre_clase))
        cur.execute("SELECT id FROM clases WHERE nombre = %s", (nombre_clase,))
        clase_id = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO clases_horarios (clase_id, dia_semana, hora_inicio, hora_fin, activo)
            VALUES (%s, %s, %s, %s, TRUE)
            RETURNING id
            """,
            (clase_id, dia_semana, hora_inicio, hora_fin),
        )
        horario_id = cur.fetchone()[0]
        fix_sequence(conn, 'clases_horarios')
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
        fix_sequence(conn, 'clase_usuarios')
        return row[0] if row else None


def ensure_metodo_pago(conn, nombre, color="#3498db"):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO metodos_pago (nombre, color) VALUES (%s, %s) ON CONFLICT (nombre) DO NOTHING",
            (nombre, color),
        )
        cur.execute("SELECT id FROM metodos_pago WHERE nombre = %s", (nombre,))
        return cur.fetchone()[0]


def upsert_pago(conn, uid, monto, mes, anio, metodo_pago_id=None):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pagos (usuario_id, monto, mes, año, fecha_pago, metodo_pago_id)
            VALUES (%s, %s, %s, %s, NOW(), %s)
            ON CONFLICT (usuario_id, mes, año)
            DO UPDATE SET monto = EXCLUDED.monto, fecha_pago = NOW(), metodo_pago_id = COALESCE(EXCLUDED.metodo_pago_id, pagos.metodo_pago_id)
            RETURNING id
            """,
            (uid, monto, mes, anio, metodo_pago_id),
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
        try:
            fix_sequence(conn, 'usuario_estados')
        except Exception:
            pass
        cur.execute(
            "INSERT INTO usuario_estados (usuario_id, estado, descripcion, activo) VALUES (%s, %s, %s, TRUE) RETURNING id",
            (uid, estado, descripcion),
        )
        return cur.fetchone()[0]


def ensure_ejercicio(conn, nombre):
    with conn.cursor() as cur:
        cur.execute("INSERT INTO ejercicios (nombre) VALUES (%s) ON CONFLICT (nombre) DO NOTHING", (nombre,))
        cur.execute("SELECT id FROM ejercicios WHERE nombre = %s", (nombre,))
        return cur.fetchone()[0]


def ensure_rutina(conn, uid, nombre):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO rutinas (usuario_id, nombre_rutina, activa)
            VALUES (%s, %s, TRUE)
            ON CONFLICT DO NOTHING
            RETURNING id
            """,
            (uid, nombre),
        )
        row = cur.fetchone()
        if row:
            rid = row[0]
        else:
            cur.execute("SELECT id FROM rutinas WHERE usuario_id = %s AND nombre_rutina = %s", (uid, nombre))
            rid = cur.fetchone()[0]
        return rid


def add_rutina_ejercicio(conn, rutina_id, ejercicio_id, dia=1, series=3, rep="10"):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO rutina_ejercicios (rutina_id, ejercicio_id, dia_semana, series, repeticiones, orden)
            VALUES (%s, %s, %s, %s, %s, 1)
            ON CONFLICT DO NOTHING
            """,
            (rutina_id, ejercicio_id, dia, series, rep),
        )


def add_lista_espera(conn, clase_horario_id, uid):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO clase_lista_espera (clase_horario_id, usuario_id, posicion, activo)
            VALUES (%s, %s, 1, TRUE)
            ON CONFLICT (clase_horario_id, usuario_id) DO NOTHING
            RETURNING id
            """,
            (clase_horario_id, uid),
        )
        row = cur.fetchone()
        fix_sequence(conn, 'clase_lista_espera')
        return row[0] if row else None


def add_notificacion_cupo(conn, uid, clase_horario_id, tipo, mensaje):
    with conn.cursor() as cur:
        # Insert if not exists by (usuario, clase_horario, tipo, mensaje)
        cur.execute(
            """
            SELECT id FROM notificaciones_cupos
            WHERE usuario_id = %s AND clase_horario_id = %s AND tipo_notificacion = %s AND mensaje = %s
            """,
            (uid, clase_horario_id, tipo, mensaje),
        )
        row = cur.fetchone()
        if row:
            return row[0]
        cur.execute(
            """
            INSERT INTO notificaciones_cupos (usuario_id, clase_horario_id, tipo_notificacion, mensaje, activa)
            VALUES (%s, %s, %s, %s, TRUE) RETURNING id
            """,
            (uid, clase_horario_id, tipo, mensaje),
        )
        fix_sequence(conn, 'notificaciones_cupos')
        return cur.fetchone()[0]


def upsert_config(conn, clave, valor):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO configuracion (clave, valor) VALUES (%s, %s) ON CONFLICT (clave) DO UPDATE SET valor = EXCLUDED.valor RETURNING id",
            (clave, valor),
        )
        return cur.fetchone()[0]


def insert_nota(conn, uid, titulo, contenido):
    with conn.cursor() as cur:
        # idempotencia por (uid, titulo)
        cur.execute("SELECT id FROM usuario_notas WHERE usuario_id = %s AND titulo = %s", (uid, titulo))
        row = cur.fetchone()
        if row:
            return row[0]
        cur.execute(
            "INSERT INTO usuario_notas (usuario_id, categoria, titulo, contenido, importancia, activa) VALUES (%s, 'general', %s, %s, 'normal', TRUE) RETURNING id",
            (uid, titulo, contenido),
        )
        return cur.fetchone()[0]


def ensure_profesor(conn, uid, tipo="Musculación"):
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM profesores WHERE usuario_id = %s", (uid,))
        row = cur.fetchone()
        if row:
            return row[0]
        cur.execute(
            "INSERT INTO profesores (usuario_id, tipo, estado, experiencia_años) VALUES (%s, %s, 'activo', 1) RETURNING id",
            (uid, tipo),
        )
        fix_sequence(conn, 'profesores')
        return cur.fetchone()[0]


def add_horario_profesor(conn, profesor_id, dia_semana="Lunes", hi="10:00", hf="12:00"):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO horarios_profesores (profesor_id, dia_semana, hora_inicio, hora_fin, disponible) VALUES (%s, %s, %s, %s, TRUE) RETURNING id",
            (profesor_id, dia_semana, hi, hf),
        )
        fix_sequence(conn, 'horarios_profesores')
        return cur.fetchone()[0]


def add_profesor_disponibilidad(conn, profesor_id, f: date, tipo="Disponible", hi="10:00", hf="12:00"):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO profesor_disponibilidad (profesor_id, fecha, tipo_disponibilidad, hora_inicio, hora_fin)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (profesor_id, fecha) DO UPDATE SET tipo_disponibilidad = EXCLUDED.tipo_disponibilidad
            RETURNING id
            """,
            (profesor_id, f, tipo, hi, hf),
        )
        fix_sequence(conn, 'profesor_disponibilidad')
        return cur.fetchone()[0]


def add_asignacion_profesor(conn, clase_horario_id, profesor_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO profesor_clase_asignaciones (clase_horario_id, profesor_id, activa)
            VALUES (%s, %s, TRUE)
            ON CONFLICT (clase_horario_id, profesor_id) DO NOTHING
            RETURNING id
            """,
            (clase_horario_id, profesor_id),
        )
        row = cur.fetchone()
        fix_sequence(conn, 'profesor_clase_asignaciones')
        return row[0] if row else None


def add_suplencia(conn, asignacion_id, f: date, motivo, profesor_suplente_id=None):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO profesor_suplencias (asignacion_id, profesor_suplente_id, fecha_clase, motivo, estado)
            VALUES (%s, %s, %s, %s, 'Pendiente') RETURNING id
            """,
            (asignacion_id, profesor_suplente_id, f, motivo),
        )
        fix_sequence(conn, 'profesor_suplencias')
        return cur.fetchone()[0]


def add_suplencia_general(conn, profesor_original_id, f: date, hi="10:00", hf="12:00", motivo="Cobertura"):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO profesor_suplencias_generales (profesor_original_id, fecha, hora_inicio, hora_fin, motivo, estado)
            VALUES (%s, %s, %s, %s, %s, 'Pendiente') RETURNING id
            """,
            (profesor_original_id, f, hi, hf, motivo),
        )
        fix_sequence(conn, 'profesor_suplencias_generales')
        return cur.fetchone()[0]


def ensure_tipo_cuota(conn, nombre, precio=100.0):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO tipos_cuota (nombre, precio, activo) VALUES (%s, %s, TRUE) ON CONFLICT (nombre) DO NOTHING",
            (nombre, precio),
        )
        cur.execute("SELECT id FROM tipos_cuota WHERE nombre = %s", (nombre,))
        return cur.fetchone()[0]


def ensure_grupo_ejercicios(conn, nombre):
    with conn.cursor() as cur:
        cur.execute("INSERT INTO ejercicio_grupos (nombre) VALUES (%s) ON CONFLICT (nombre) DO NOTHING", (nombre,))
        cur.execute("SELECT id FROM ejercicio_grupos WHERE nombre = %s", (nombre,))
        return cur.fetchone()[0]


def add_grupo_item(conn, grupo_id, ejercicio_id):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO ejercicio_grupo_items (grupo_id, ejercicio_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (grupo_id, ejercicio_id),
        )


def run_full_dataset(conn, uid, token):
    def step(name, fn):
        try:
            return fn()
        except Exception as e:
            raise RuntimeError(f"[{name}] {e}")

    results = {}
    step('ensure_user', lambda: ensure_user(conn, uid, f"Usuario Full {uid} {token}"))

    # Catálogos / Tipos
    tipo_id = step('ensure_tipo_clase', lambda: ensure_tipo_clase(conn, f"tipo_{token}"))
    cuota_id = step('ensure_tipo_cuota', lambda: ensure_tipo_cuota(conn, f"cuota_{token}"))
    results['tipo_clase_id'] = tipo_id
    results['tipo_cuota_id'] = cuota_id

    # Clase y horario + inscripción + espera + notificación
    clase_id, horario_id = step('ensure_clase_y_horario', lambda: ensure_clase_y_horario(conn, f"Clase Full {uid} {token}", tipo_id))
    cu_id = step('upsert_clase_usuario', lambda: upsert_clase_usuario(conn, horario_id, uid))
    le_id = step('add_lista_espera', lambda: add_lista_espera(conn, horario_id, uid))
    notif_id = step('add_notificacion_cupo', lambda: add_notificacion_cupo(conn, uid, horario_id, 'recordatorio', f"notif_{token}"))
    results.update({'clase_id': clase_id, 'clase_horario_id': horario_id, 'clase_usuario_id': cu_id, 'lista_espera_id': le_id, 'notificacion_id': notif_id})

    # Métodos de pago + pago
    mp_id = step('ensure_metodo_pago', lambda: ensure_metodo_pago(conn, f"metodo_{token}"))
    today = date.today()
    pago_id = step('upsert_pago', lambda: upsert_pago(conn, uid, 187.00, today.month, today.year, mp_id))
    results['metodo_pago_id'] = mp_id
    results['pago_id'] = pago_id

    # Asistencia
    asis_id = step('upsert_asistencia', lambda: upsert_asistencia(conn, uid, today))
    results['asistencia_id'] = asis_id

    # Etiquetas y relación
    tag_id = step('upsert_etiqueta', lambda: upsert_etiqueta(conn, f"full_tag_{uid}_{token}", descripcion=f"Etiqueta {token}"))
    step('upsert_usuario_etiqueta', lambda: upsert_usuario_etiqueta(conn, uid, tag_id))
    results['etiqueta_id'] = tag_id

    # Estados
    est_id = step('insert_estado', lambda: insert_estado(conn, uid, f"full_estado_{token}", "Estado full diag"))
    results['estado_id'] = est_id

    # Rutinas y ejercicios
    e1 = step('ensure_ejercicio_1', lambda: ensure_ejercicio(conn, f"press_banca_{token}"))
    e2 = step('ensure_ejercicio_2', lambda: ensure_ejercicio(conn, f"sentadilla_{token}"))
    r1 = step('ensure_rutina', lambda: ensure_rutina(conn, uid, f"rutina_{token}"))
    step('add_rutina_ejercicio_1', lambda: add_rutina_ejercicio(conn, r1, e1, dia=1))
    step('add_rutina_ejercicio_2', lambda: add_rutina_ejercicio(conn, r1, e2, dia=3))
    results.update({'ejercicio_1': e1, 'ejercicio_2': e2, 'rutina_id': r1})

    # Grupos de ejercicios
    gid = step('ensure_grupo_ejercicios', lambda: ensure_grupo_ejercicios(conn, f"grupo_{token}"))
    step('add_grupo_item_1', lambda: add_grupo_item(conn, gid, e1))
    step('add_grupo_item_2', lambda: add_grupo_item(conn, gid, e2))
    results['grupo_ejercicios_id'] = gid

    # Configuración
    cfg_id = step('upsert_config', lambda: upsert_config(conn, f"diag_config_{token}", json.dumps({"ok": True, "token": token})))
    results['config_id'] = cfg_id

    # Notas de usuario
    nota_id = step('insert_nota', lambda: insert_nota(conn, uid, f"nota_{token}", "Contenido de prueba"))
    results['usuario_nota_id'] = nota_id

    # Profesores y relaciones
    prof_id = step('ensure_profesor', lambda: ensure_profesor(conn, uid))
    hp_id = step('add_horario_profesor', lambda: add_horario_profesor(conn, prof_id))
    pd_id = step('add_profesor_disponibilidad', lambda: add_profesor_disponibilidad(conn, prof_id, today))
    asig_id = step('add_asignacion_profesor', lambda: add_asignacion_profesor(conn, horario_id, prof_id))
    supl_id = step('add_suplencia', lambda: add_suplencia(conn, asig_id, today, f"supl_{token}"))
    supg_id = step('add_suplencia_general', lambda: add_suplencia_general(conn, prof_id, today))
    results.update({'profesor_id': prof_id, 'horario_prof_id': hp_id, 'prof_disp_id': pd_id, 'asignacion_profesor_id': asig_id, 'suplencia_id': supl_id, 'suplencia_general_id': supg_id})

    return results


def verify_counts(conn, uid, token):
    out = {}
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM clases WHERE nombre = %s", (f"Clase Full {uid} {token}",))
        out['clases'] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM clases_horarios ch JOIN clases c ON c.id = ch.clase_id WHERE c.nombre = %s", (f"Clase Full {uid} {token}",))
        out['clases_horarios'] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM clase_usuarios cu JOIN clases_horarios ch ON ch.id = cu.clase_horario_id JOIN clases c ON c.id = ch.clase_id WHERE cu.usuario_id = %s AND c.nombre = %s", (uid, f"Clase Full {uid} {token}"))
        out['clase_usuarios'] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM clase_lista_espera cle JOIN clases_horarios ch ON ch.id = cle.clase_horario_id JOIN clases c ON c.id = ch.clase_id WHERE cle.usuario_id = %s AND c.nombre = %s", (uid, f"Clase Full {uid} {token}"))
        out['clase_lista_espera'] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM notificaciones_cupos WHERE mensaje = %s", (f"notif_{token}",))
        out['notificaciones_cupos'] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM pagos WHERE usuario_id = %s AND mes = EXTRACT(MONTH FROM CURRENT_DATE)::int AND año = EXTRACT(YEAR FROM CURRENT_DATE)::int", (uid,))
        out['pagos'] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM asistencias WHERE usuario_id = %s AND fecha BETWEEN CURRENT_DATE - INTERVAL '1 day' AND CURRENT_DATE + INTERVAL '1 day'", (uid,))
        out['asistencias'] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM etiquetas WHERE nombre = %s", (f"full_tag_{uid}_{token}",))
        out['etiquetas'] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM usuario_etiquetas ue JOIN etiquetas e ON e.id = ue.etiqueta_id WHERE ue.usuario_id = %s AND e.nombre = %s", (uid, f"full_tag_{uid}_{token}"))
        out['usuario_etiquetas'] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM usuario_estados WHERE usuario_id = %s AND estado = %s", (uid, f"full_estado_{token}"))
        out['usuario_estados'] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM ejercicios WHERE nombre IN (%s, %s)", (f"press_banca_{token}", f"sentadilla_{token}"))
        out['ejercicios'] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM rutinas WHERE usuario_id = %s AND nombre_rutina = %s", (uid, f"rutina_{token}"))
        out['rutinas'] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM rutina_ejercicios re JOIN rutinas r ON r.id = re.rutina_id WHERE r.nombre_rutina = %s", (f"rutina_{token}",))
        out['rutina_ejercicios'] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM ejercicio_grupos WHERE nombre = %s", (f"grupo_{token}",))
        out['ejercicio_grupos'] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM ejercicio_grupo_items gi JOIN ejercicio_grupos g ON g.id = gi.grupo_id WHERE g.nombre = %s", (f"grupo_{token}",))
        out['ejercicio_grupo_items'] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM configuracion WHERE clave = %s", (f"diag_config_{token}",))
        out['configuracion'] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM usuario_notas WHERE usuario_id = %s AND titulo = %s", (uid, f"nota_{token}"))
        out['usuario_notas'] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM profesores WHERE usuario_id = %s", (uid,))
        out['profesores'] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM horarios_profesores WHERE profesor_id = (SELECT id FROM profesores WHERE usuario_id = %s)", (uid,))
        out['horarios_profesores'] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM profesor_disponibilidad WHERE profesor_id = (SELECT id FROM profesores WHERE usuario_id = %s)", (uid,))
        out['profesor_disponibilidad'] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM profesor_clase_asignaciones WHERE profesor_id = (SELECT id FROM profesores WHERE usuario_id = %s)", (uid,))
        out['profesor_clase_asignaciones'] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM profesor_suplencias WHERE asignacion_id IN (SELECT id FROM profesor_clase_asignaciones WHERE profesor_id = (SELECT id FROM profesores WHERE usuario_id = %s))", (uid,))
        out['profesor_suplencias'] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM profesor_suplencias_generales WHERE profesor_original_id = (SELECT id FROM profesores WHERE usuario_id = %s)", (uid,))
        out['profesor_suplencias_generales'] = cur.fetchone()[0]
    return out


def bidir_full_test(local_params, remote_params, uid=87):
    out = {'ok': False}
    lc = rc = None
    t_local = uuid.uuid4().hex[:8]
    t_remote = uuid.uuid4().hex[:8]
    try:
        lc = psycopg2.connect(**local_params)
        rc = psycopg2.connect(**remote_params)

        with lc:
            local_created = run_full_dataset(lc, uid, t_local)
        with rc:
            remote_recreated_for_local = run_full_dataset(rc, uid, t_local)

        with rc:
            remote_created = run_full_dataset(rc, uid, t_remote)
        with lc:
            local_recreated_for_remote = run_full_dataset(lc, uid, t_remote)

        with lc:
            lc_counts_local = verify_counts(lc, uid, t_local)
            lc_counts_remote = verify_counts(lc, uid, t_remote)
        with rc:
            rc_counts_local = verify_counts(rc, uid, t_local)
            rc_counts_remote = verify_counts(rc, uid, t_remote)

        out.update({
            'ok': True,
            'uid': uid,
            'tokens': {'local_token': t_local, 'remote_token': t_remote},
            'created': {
                'local_created': local_created,
                'remote_recreated_for_local': remote_recreated_for_local,
                'remote_created': remote_created,
                'local_recreated_for_remote': local_recreated_for_remote
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
    if not local or not remote:
        print(json.dumps({'ok': False, 'error': 'Perfiles local o remoto no configurados'}, ensure_ascii=False))
        return
    lp = to_connect_params(local)
    rp = to_connect_params(remote)
    res = bidir_full_test(lp, rp, uid=87)
    print(json.dumps(res, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()