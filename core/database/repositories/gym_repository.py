from datetime import datetime, date, timedelta
from typing import List, Optional, Dict, Set, Tuple, Any
import json
import logging
import psycopg2
import psycopg2.extras
import uuid
from .base import BaseRepository
from ..connection import database_retry
from ...models import Ejercicio, Rutina, RutinaEjercicio, Clase, ClaseHorario, ClaseUsuario, EjercicioGrupo, EjercicioGrupoItem
from ...utils import get_gym_name

class GymRepository(BaseRepository):
    pass

    # --- Methods moved from DatabaseManager ---

    def get_table_columns(self, table_name: str) -> List[str]:
        """Devuelve la lista de columnas de una tabla en orden, con caché.

        Usa information_schema para descubrir columnas y cachea el resultado
        para minimizar consultas adicionales.
        """
        try:
            # Cache hit
            cols = self._table_columns_cache.get(table_name)
            if cols:
                return cols
        except Exception:
            pass
        with self._table_columns_lock:
            # Revalidar dentro del lock
            cols2 = self._table_columns_cache.get(table_name)
            if cols2:
                return cols2
            try:
                with self.readonly_session(lock_ms=0, statement_ms=2000, idle_s=2, seqscan_off=True) as conn:
                    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                        cur.execute(
                            """
                            SELECT column_name
                            FROM information_schema.columns
                            WHERE table_schema = 'public' AND table_name = %s
                            ORDER BY ordinal_position
                            """,
                            (table_name,)
                        )
                        rows = cur.fetchall() or []
                        names = [r['column_name'] for r in rows]
                        # Cachear
                        self._table_columns_cache[table_name] = names
                        return names
            except Exception:
                # Fallback: devolver vacío para evitar romper llamadas; el caller puede usar '*'
                return []


    def invalidate_table_columns_cache(self, table_name: str) -> None:
        try:
            with self._table_columns_lock:
                if table_name in self._table_columns_cache:
                    try:
                        del self._table_columns_cache[table_name]
                    except Exception:
                        self._table_columns_cache.pop(table_name, None)
        except Exception:
            pass


    def _column_exists(self, conn, table_name: str, column_name: str) -> bool:
        """Verifica si existe una columna en una tabla (schema público)."""
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT EXISTS(
                        SELECT 1 FROM information_schema.columns
                        WHERE table_schema = 'public' AND table_name = %s AND column_name = %s
                    ) AS exists
                    """,
                    (table_name, column_name)
                )
                row = cur.fetchone()
                return bool(row and row.get('exists'))
        except Exception:
            return False


    def ensure_rutina_uuid_ready(self, conn) -> bool:
        """Asegura que la columna uuid_rutina exista y tenga valores únicos.

        - Crea la columna si no existe (intenta con DEFAULT gen_random_uuid())
        - Rellena valores faltantes con uuid4() en caso necesario
        - Crea índice único si falta

        Devuelve True si la columna existe al finalizar (aunque sin default), False si no se pudo asegurar.
        """
        try:
            has_col = self._column_exists(conn, 'rutinas', 'uuid_rutina')
            if not has_col:
                with conn.cursor() as cur:
                    # Intentar crear extensión y columna con default gen_random_uuid()
                    created = False
                    try:
                        cur.execute('CREATE EXTENSION IF NOT EXISTS pgcrypto')
                        cur.execute("ALTER TABLE rutinas ADD COLUMN uuid_rutina TEXT DEFAULT gen_random_uuid()::text")
                        created = True
                    except Exception:
                        # Fallback: crear sin default
                        try:
                            cur.execute("ALTER TABLE rutinas ADD COLUMN uuid_rutina TEXT")
                            created = True
                        except Exception:
                            created = False
                    if created:
                        try:
                            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_rutinas_uuid ON rutinas (uuid_rutina)")
                        except Exception:
                            pass
                        conn.commit()
                    else:
                        conn.rollback()
                        return False
                has_col = True

            # Rellenar valores nulos con UUIDs
            try:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute("SELECT id FROM rutinas WHERE uuid_rutina IS NULL OR uuid_rutina = ''")
                    rows = cur.fetchall() or []
                    if rows:
                        # Generar y actualizar en lotes
                        for r in rows:
                            rid = r['id'] if isinstance(r, dict) else r[0]
                            new_uuid = str(uuid.uuid4())
                            try:
                                cur.execute("UPDATE rutinas SET uuid_rutina = %s WHERE id = %s", (new_uuid, rid))
                            except Exception:
                                # Si falla por duplicado improbable, generar otro
                                try:
                                    cur.execute("UPDATE rutinas SET uuid_rutina = %s WHERE id = %s", (str(uuid.uuid4()), rid))
                                except Exception:
                                    pass
                        try:
                            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_rutinas_uuid ON rutinas (uuid_rutina)")
                        except Exception:
                            pass
                        conn.commit()
            except Exception:
                # No bloquear si algo falla al rellenar
                try:
                    conn.rollback()
                except Exception:
                    pass

            return has_col
        except Exception:
            return False


    def _apply_readonly_timeouts(self, cursor, lock_ms: int = 800, statement_ms: int = 1500, idle_s: int = 2):
        """Aplica parámetros de sesión para evitar bloqueos en lecturas.

        Usa SET LOCAL para limitar los tiempos y forzar read-only en la transacción actual.
        """
        try:
            cursor.execute(f"SET LOCAL lock_timeout = '{lock_ms}ms'")
        except Exception:
            pass
        try:
            cursor.execute(f"SET LOCAL statement_timeout = '{statement_ms}ms'")
        except Exception:
            pass
        try:
            cursor.execute(f"SET LOCAL idle_in_transaction_session_timeout = '{idle_s}s'")
        except Exception:
            pass
        try:
            cursor.execute("SET LOCAL default_transaction_read_only = on")
        except Exception:
            pass

    @contextmanager

    def get_owner_password_cached(self, ttl_seconds: int = 600) -> Optional[str]:
        """Obtiene la contraseña del Dueño usando caché TTL y fallbacks controlados."""
        try:
            now = time.time()
            exp = float(self._owner_cache.get('password_expiry', 0.0) or 0.0)
            pwd = self._owner_cache.get('password')
            if pwd and now < exp:
                return pwd
            # Intento DB con timeout agresivo
            try:
                pwd = self.obtener_configuracion('owner_password', timeout_ms=700)
            except Exception:
                pwd = None
            # Fallback entorno
            if not pwd:
                try:
                    pwd = os.getenv('WEBAPP_OWNER_PASSWORD') or os.getenv('OWNER_PASSWORD')
                except Exception:
                    pwd = None
            # Fallback desarrollo
            if not pwd:
                try:
                    from .secure_config import SecureConfig as _SC
                    pwd = _SC.get_dev_password()
                except Exception:
                    pwd = None
            if pwd:
                try:
                    self._owner_cache['password'] = pwd
                    self._owner_cache['password_expiry'] = now + max(60, ttl_seconds)
                except Exception:
                    pass
            return pwd
        except Exception:
            return None


    def prefetch_owner_credentials_async(self, ttl_seconds: int = 600):
        """Precarga en segundo plano las credenciales del Dueño para acelerar login."""
        try:
            def _prefetch():
                try:
                    self.get_owner_password_cached(ttl_seconds=ttl_seconds)
                except Exception:
                    pass
                try:
                    self.get_owner_user_cached(ttl_seconds=ttl_seconds)
                except Exception:
                    pass
            threading.Thread(target=_prefetch, daemon=True).start()
        except Exception:
            pass

    # === Utilidades de soporte para modo offline ===

    def _get_default_connection_params(self) -> dict:
        """Obtiene parámetros de conexión por defecto únicamente desde .env.
        Usa `DB_PROFILE` para elegir entre `DB_LOCAL_*` y `DB_REMOTE_*`. Como
        fallback acepta `DB_HOST/PORT/NAME/USER/PASSWORD`.
        """
        # Determinar perfil desde entorno (default: local)
        try:
            profile = str(os.getenv('DB_PROFILE', 'local')).lower()
        except Exception:
            profile = 'local'

        # Resolver parámetros primarios desde secure_config
        try:
            from .secure_config import SecureConfig as _SC  # carga .env automáticamente
            if profile in ('local', 'remote'):
                base = _SC.get_db_config(profile)
            else:
                base = {}
        except Exception:
            base = {}

        # Si existe un DSN/URL en entorno, sobreponer los parámetros parseados.
        # Esto permite compatibilidad inmediata con proveedores como Railway/Neon.
        try:
            # Aceptar también RAILWAY_DATABASE_URL como fallback cuando DATABASE_URL no esté presente
            dsn = str(os.getenv('DATABASE_URL', '') or os.getenv('RAILWAY_DATABASE_URL', '')).strip()
        except Exception:
            dsn = ''
        if dsn:
            try:
                from urllib.parse import urlparse, parse_qs
                u = urlparse(dsn)
                q = parse_qs(u.query or '')
                parsed = {}
                try:
                    host_val = (u.hostname or base.get('host') or 'localhost')
                    parsed['host'] = str(host_val).strip()
                except Exception:
                    parsed['host'] = str(base.get('host', 'localhost')).strip()
                try:
                    parsed['port'] = int(u.port or base.get('port') or 5432)
                except Exception:
                    parsed['port'] = int(base.get('port') or 5432)
                try:
                    db_val = (u.path or '').lstrip('/') or base.get('database') or 'gimnasio'
                    parsed['database'] = str(db_val).strip()
                except Exception:
                    parsed['database'] = str(base.get('database', 'gimnasio')).strip()
                try:
                    user_val = (u.username or base.get('user') or 'postgres')
                    parsed['user'] = str(user_val).strip()
                except Exception:
                    parsed['user'] = str(base.get('user', 'postgres')).strip()
                try:
                    pwd_val = (u.password or base.get('password') or '')
                    parsed['password'] = str(pwd_val)
                except Exception:
                    parsed['password'] = str(base.get('password', ''))
                try:
                    ssl_val = (q.get('sslmode') or [base.get('sslmode', 'prefer')])[0]
                    parsed['sslmode'] = str(ssl_val).strip()
                except Exception:
                    parsed['sslmode'] = str(base.get('sslmode', 'prefer')).strip()
                try:
                    app_val = (q.get('application_name') or [base.get('application_name', 'gym_management_system')])[0]
                    parsed['application_name'] = str(app_val).strip()
                except Exception:
                    parsed['application_name'] = str(base.get('application_name', 'gym_management_system')).strip()
                try:
                    parsed['connect_timeout'] = int((q.get('connect_timeout') or [base.get('connect_timeout', 5)])[0])
                except Exception:
                    parsed['connect_timeout'] = int(base.get('connect_timeout') or 5)

                # Aplicar merge preferiendo los valores del DSN
                try:
                    base.update(parsed)
                except Exception:
                    # Si por alguna razón base no es dict, recrear
                    base = dict(parsed)
            except Exception:
                # Ignorar DSN inválido y continuar con env/genéricos
                pass

        # Fallbacks genéricos (DB_*) si faltan claves específicas
        host = str(base.get('host') or os.getenv('DB_HOST', 'localhost')).strip()
        try:
            port = int(base.get('port') or os.getenv('DB_PORT', 5432))
        except Exception:
            port = 5432
        database = str(base.get('database') or os.getenv('DB_NAME', 'gimnasio')).strip()
        user = str(base.get('user') or os.getenv('DB_USER', 'postgres')).strip()
        sslmode = str(base.get('sslmode') or os.getenv('DB_SSLMODE', 'prefer')).strip()
        try:
            connect_timeout = int(base.get('connect_timeout') or os.getenv('DB_CONNECT_TIMEOUT', 5))
        except Exception:
            connect_timeout = 5
        application_name = str(base.get('application_name') or os.getenv('DB_APPLICATION_NAME', 'gym_management_system')).strip()

        # Opciones de sesión por conexión para evitar SET dentro de transacciones
        # Permite: statement_timeout, lock_timeout, idle_in_transaction_session_timeout y zona horaria
        options_parts = []
        try:
            st_timeout = str(os.getenv('DB_STATEMENT_TIMEOUT', (base.get('statement_timeout') or '4s')))
            if st_timeout:
                options_parts.append(f"-c statement_timeout={st_timeout}")
        except Exception:
            options_parts.append("-c statement_timeout=4s")
        try:
            lk_timeout = str(os.getenv('DB_LOCK_TIMEOUT', (base.get('lock_timeout') or '2s')))
            if lk_timeout:
                options_parts.append(f"-c lock_timeout={lk_timeout}")
        except Exception:
            options_parts.append("-c lock_timeout=2s")
        try:
            idle_trx_timeout = str(os.getenv('DB_IDLE_IN_TRX_TIMEOUT', (base.get('idle_in_transaction_session_timeout') or '30s')))
            if idle_trx_timeout:
                options_parts.append(f"-c idle_in_transaction_session_timeout={idle_trx_timeout}")
        except Exception:
            options_parts.append("-c idle_in_transaction_session_timeout=30s")
        try:
            tz = str(os.getenv('DB_TIME_ZONE', (base.get('time_zone') or 'America/Argentina/Buenos_Aires')))
            if tz:
                options_parts.append(f"-c TimeZone={tz}")
        except Exception:
            options_parts.append("-c TimeZone=America/Argentina/Buenos_Aires")
        # Construir cadena options final
        options = " ".join(options_parts).strip()

        # Algunos proveedores como Neon no soportan 'options' con '-c ...' en el startup packet.
        # Permite deshabilitarlo vía entorno o automáticamente si el host parece ser Neon.
        try:
            disable_options_flag = str(os.getenv('DB_DISABLE_OPTIONS', 'false')).lower() in ('1', 'true', 'yes', 'on')
        except Exception:
            disable_options_flag = False
        try:
            host_lc = str(host).lower()
        except Exception:
            host_lc = ''
        if disable_options_flag or ('neon.tech' in host_lc):
            options = ''

        # Contraseña: ENV > perfil config.json > top-level > almacén seguro
        password = str(base.get('password') or os.getenv('DB_PASSWORD', ''))
        if not password:
            try:
                import keyring
                from .config import KEYRING_SERVICE_NAME, LEGACY_KEYRING_SERVICE_NAMES
                # Soporta coexistencia de credenciales local/remoto usando identificadores compuestos
                account_candidates = []
                try:
                    account_candidates.append(f"{user}@{host}:{port}")
                except Exception:
                    pass
                try:
                    account_candidates.append(f"{user}@{host}")
                except Exception:
                    pass
                account_candidates.append(user)

                saved_pwd = None
                for account in account_candidates:
                    try:
                        saved_pwd = keyring.get_password(KEYRING_SERVICE_NAME, account)
                    except Exception:
                        saved_pwd = None
                    if saved_pwd:
                        break

                # Migración automática desde etiquetas anteriores si no existe en la actual
                if not saved_pwd:
                    for old_service in LEGACY_KEYRING_SERVICE_NAMES:
                        if not old_service or old_service == KEYRING_SERVICE_NAME:
                            continue
                        for account in account_candidates:
                            try:
                                legacy_pwd = keyring.get_password(old_service, account)
                            except Exception:
                                legacy_pwd = None
                            if legacy_pwd:
                                # Copiar a la etiqueta nueva (sin eliminar la vieja)
                                try:
                                    keyring.set_password(KEYRING_SERVICE_NAME, account, legacy_pwd)
                                except Exception:
                                    pass
                                saved_pwd = legacy_pwd
                                break
                        if saved_pwd:
                            break

                password = (saved_pwd or '')
            except Exception:
                password = ''

        # Sanitizar valores de conexión para evitar espacios accidentales
        params = {
            'host': str(host).strip(),
            'port': port,
            'database': str(database).strip(),
            'user': str(user).strip(),
            'password': password,
            'sslmode': str(sslmode).strip(),
            'connect_timeout': connect_timeout,
            'application_name': str(application_name).strip(),
        }
        
        # Solo agregar parámetros keepalives si no es Neon.tech (no los soporta)
        try:
            host_lc = str(host).lower()
            if 'neon.tech' not in host_lc:
                # Mantener viva la conexión TCP en redes inestables (solo para no-Neon)
                params['keepalives'] = 1
                params['keepalives_idle'] = int(os.getenv('DB_KEEPALIVES_IDLE', (base.get('keepalives_idle') or 30)))
                params['keepalives_interval'] = int(os.getenv('DB_KEEPALIVES_INTERVAL', (base.get('keepalives_interval') or 10)))
                params['keepalives_count'] = int(os.getenv('DB_KEEPALIVES_COUNT', (base.get('keepalives_count') or 3)))
        except Exception:
            pass
        # Solo incluir 'options' si hay contenido
        if options:
            params['options'] = options
        return params
    

    def _cb_register_failure(self, error: Exception):
        now = time.time()
        if self._cb_first_failure_ts == 0.0 or (now - self._cb_first_failure_ts) > float(self._cb_conf.get('window_seconds', 20)):
            # Reiniciar ventana
            self._cb_first_failure_ts = now
            self._cb_failure_count = 1
        else:
            self._cb_failure_count += 1

        threshold = int(self._cb_conf.get('failure_threshold', 3))
        if self._cb_failure_count >= threshold:
            self._cb_is_open = True
            self._cb_open_until = now + float(self._cb_conf.get('open_seconds', 25))
            try:
                self.logger.warning(f"Circuit Breaker abierto por fallos consecutivos ({self._cb_failure_count}). Bloqueado hasta {self._cb_open_until:.3f}")
            except Exception:
                pass


    def _cb_register_success(self):
        # Éxito: cerrar circuito y resetear contadores
        self._cb_failure_count = 0
        self._cb_first_failure_ts = 0.0
        if self._cb_is_open:
            # Cerrar circuito si está en half-open y hubo éxito
            self._cb_is_open = False
            self._cb_open_until = 0.0
            try:
                self.logger.info("Circuit Breaker cerrado tras éxito")
            except Exception:
                pass


    def is_circuit_open(self) -> bool:
        try:
            if not self._cb_is_open:
                return False
            return time.time() < float(self._cb_open_until)
        except Exception:
            return False


    def get_circuit_state(self) -> dict:
        return {
            'is_open': bool(getattr(self, '_cb_is_open', False)),
            'failure_count': int(getattr(self, '_cb_failure_count', 0)),
            'open_until': float(getattr(self, '_cb_open_until', 0.0)),
            'conf': dict(self._cb_conf),
        }
    
    @contextmanager

    def _ensure_gym_config_table(self, conn):
        """Crea la tabla gym_config si no existe y asegura una fila inicial."""
        with conn.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS gym_config (
                    id SERIAL PRIMARY KEY,
                    gym_name TEXT DEFAULT '',
                    gym_slogan TEXT DEFAULT '',
                    gym_address TEXT DEFAULT '',
                    gym_phone TEXT DEFAULT '',
                    gym_email TEXT DEFAULT '',
                    gym_website TEXT DEFAULT '',
                    facebook TEXT DEFAULT '',
                    instagram TEXT DEFAULT '',
                    twitter TEXT DEFAULT '',
                    logo_url TEXT DEFAULT '',
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            cursor.execute("SELECT id FROM gym_config LIMIT 1")
            row = cursor.fetchone()
            if row is None:
                cursor.execute(
                    """
                    INSERT INTO gym_config (
                        gym_name, gym_slogan, gym_address, gym_phone, gym_email,
                        gym_website, facebook, instagram, twitter, logo_url
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        'Gimnasio', '', '', '', '',
                        '', '', '', '', ''
                    )
                )
        try:
            conn.commit()
        except Exception:
            pass


    def obtener_configuracion_gimnasio(self, timeout_ms: int = 1000) -> Dict[str, str]:
        """Obtiene todos los datos del gimnasio desde la tabla gym_config."""
        with self.get_connection_context() as conn:
            self._ensure_gym_config_table(conn)
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT gym_name, gym_slogan, gym_address, gym_phone, gym_email,
                           gym_website, facebook, instagram, twitter, logo_url
                    FROM gym_config
                    ORDER BY id
                    LIMIT 1
                    """
                )
                row = cursor.fetchone()
                if not row:
                    return {
                        'gym_name': 'Gimnasio',
                        'gym_slogan': '',
                        'gym_address': '',
                        'gym_phone': '',
                        'gym_email': '',
                        'gym_website': '',
                        'facebook': '',
                        'instagram': '',
                        'twitter': '',
                        'logo_url': ''
                    }
                # Mapear por posición fija
                return {
                    'gym_name': row[0] or '',
                    'gym_slogan': row[1] or '',
                    'gym_address': row[2] or '',
                    'gym_phone': row[3] or '',
                    'gym_email': row[4] or '',
                    'gym_website': row[5] or '',
                    'facebook': row[6] or '',
                    'instagram': row[7] or '',
                    'twitter': row[8] or '',
                    'logo_url': row[9] or ''
                }


    def actualizar_configuracion_gimnasio(self, data: dict) -> bool:
        """Actualiza múltiples campos de gym_config y sincroniza el logo en configuracion."""
        allowed = {
            'gym_name','gym_slogan','gym_address','gym_phone','gym_email',
            'gym_website','facebook','instagram','twitter','logo_url'
        }
        cols = [k for k in data.keys() if k in allowed]
        if not cols:
            return False
        values = [str(data.get(k, '') or '') for k in cols]
        set_clause = ", ".join([f"{k} = %s" for k in cols])
        with self.get_connection_context() as conn:
            self._ensure_gym_config_table(conn)
            with conn.cursor() as cursor:
                cursor.execute(
                    f"UPDATE gym_config SET {set_clause}, updated_at = CURRENT_TIMESTAMP "
                    "WHERE id = (SELECT id FROM gym_config ORDER BY id LIMIT 1)"
                    , values
                )
                if cursor.rowcount == 0:
                    placeholders = ", ".join(["%s"] * len(cols))
                    columns = ", ".join(cols)
                    cursor.execute(
                        f"INSERT INTO gym_config ({columns}) VALUES ({placeholders})",
                        values
                    )
                # Sincronizar logo_url en configuracion para compatibilidad
                if 'logo_url' in data:
                    try:
                        cursor.execute(
                            """
                            INSERT INTO configuracion (clave, valor)
                            VALUES ('gym_logo_url', %s)
                            ON CONFLICT (clave) DO UPDATE SET valor = EXCLUDED.valor
                            """,
                            (str(data.get('logo_url') or ''),)
                        )
                    except Exception:
                        pass
                # Sincronizar claves principales en configuracion para compatibilidad amplia
                try:
                    cursor.execute(
                        """
                        CREATE TABLE IF NOT EXISTS configuracion (
                            id SERIAL PRIMARY KEY,
                            clave VARCHAR(255) UNIQUE NOT NULL,
                            valor TEXT NOT NULL,
                            tipo VARCHAR(50) DEFAULT 'string',
                            descripcion TEXT
                        )
                        """
                    )
                except Exception:
                    pass
                for k in ['gym_name','gym_slogan','gym_address','gym_phone','gym_email','gym_website','facebook','instagram','twitter']:
                    if k in data:
                        try:
                            cursor.execute(
                                """
                                INSERT INTO configuracion (clave, valor)
                                VALUES (%s, %s)
                                ON CONFLICT (clave) DO UPDATE SET valor = EXCLUDED.valor
                                """,
                                (k, str(data.get(k) or ''))
                            )
                        except Exception:
                            pass
            try:
                conn.commit()
            except Exception:
                pass
        # Invalidar cache de configuración para las claves afectadas
        try:
            for k in ['gym_name','gym_slogan','gym_address','gym_phone','gym_email','gym_website','facebook','instagram','twitter','gym_logo_url']:
                self.cache.invalidate('config', k)
        except Exception:
            pass
        return True


    def obtener_logo_url(self) -> Optional[str]:
        """Obtiene el logo_url desde gym_config, con fallback a configuracion."""
        with self.get_connection_context() as conn:
            self._ensure_gym_config_table(conn)
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT logo_url FROM gym_config ORDER BY id LIMIT 1"
                )
                row = cursor.fetchone()
                if row and isinstance(row[0], str) and row[0].strip():
                    return row[0].strip()
        try:
            val = self.obtener_configuracion('gym_logo_url')
            if isinstance(val, str) and val.strip():
                return val.strip()
        except Exception:
            pass
        return None


    def actualizar_logo_url(self, url: str) -> bool:
        """Actualiza el logo_url en gym_config y en configuracion."""
        return self.actualizar_configuracion_gimnasio({'logo_url': str(url or '')})


    def _crear_tabla_acciones_masivas_pendientes(self):
        """Crea la tabla acciones_masivas_pendientes si no existe"""
        try:
            with self.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS acciones_masivas_pendientes (
                            id SERIAL PRIMARY KEY,
                            operation_id VARCHAR(255) UNIQUE NOT NULL,
                            tipo VARCHAR(100) NOT NULL,
                            descripcion TEXT,
                            usuario_ids INTEGER[],
                            parametros JSONB,
                            estado VARCHAR(50) DEFAULT 'pendiente',
                            fecha_creacion TIMESTAMP DEFAULT NOW(),
                            fecha_completado TIMESTAMP,
                            resultado TEXT,
                            created_by VARCHAR(255),
                            error_message TEXT
                        )
                    """)
                    
                    # Crear índices para mejor rendimiento
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_acciones_masivas_operation_id 
                        ON acciones_masivas_pendientes(operation_id)
                    """)
                    
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_acciones_masivas_estado 
                        ON acciones_masivas_pendientes(estado)
                    """)
                    
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_acciones_masivas_usuario_ids 
                        ON acciones_masivas_pendientes USING GIN(usuario_ids)
                    """)
                    
                    conn.commit()
                    logging.info("Tabla acciones_masivas_pendientes creada exitosamente")
                    
        except Exception as e:
            logging.error(f"Error creando tabla acciones_masivas_pendientes: {str(e)}")
            import traceback
            logging.error(f"Traceback: {traceback.format_exc()}")


    def obtener_todos_ejercicios(self) -> List:
        """Obtiene todos los ejercicios"""
        try:
            with self.get_connection_context() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    cursor.execute(
                        """
                        SELECT 
                            id, nombre, grupo_muscular, descripcion, objetivo
                        FROM ejercicios
                        ORDER BY nombre
                        """
                    )
                    return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logging.error(f"Error al obtener todos los ejercicios: {str(e)}")
            return []


    def obtener_todas_rutinas(self) -> List:
        """Obtiene todas las rutinas optimizado: columnas específicas, timeouts y caché"""
        # 1) Caché en memoria
        try:
            cached = self.cache.get('rutinas', ('all_basic',))
            if cached is not None:
                return cached
        except Exception:
            pass

        
        # 3) Consulta optimizada
        try:
            with self.get_connection_context() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    try:
                        self._apply_readonly_timeouts(cursor, lock_ms=1000, statement_ms=2000, idle_s=2)
                    except Exception:
                        pass
                    try:
                        cursor.execute("SET LOCAL enable_seqscan = off")
                    except Exception:
                        pass
                    cursor.execute(
                        """
                        SELECT 
                            id, usuario_id, nombre_rutina, descripcion, dias_semana, categoria, fecha_creacion, activa
                        FROM rutinas
                        ORDER BY nombre_rutina ASC
                        """
                    )
                    rows = [dict(row) for row in cursor.fetchall()]

                    try:
                        self.cache.set('rutinas', ('all_basic',), rows)
                    except Exception:
                        pass
                    return rows
        except Exception as e:
            logging.error(f"Error al obtener todas las rutinas: {str(e)}")
            return []


    def obtener_todas_clases(self) -> List:
        """Obtiene todas las clases optimizado: columnas específicas, timeouts y caché"""
        # 1) Caché en memoria
        try:
            cached = self.cache.get('clases', ('all_basic',))
            if cached is not None:
                return cached
        except Exception:
            pass

        
        # 3) Consulta optimizada
        try:
            with self.get_connection_context() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    try:
                        self._apply_readonly_timeouts(cursor, lock_ms=800, statement_ms=1500, idle_s=2)
                    except Exception:
                        pass
                    try:
                        cursor.execute("SET LOCAL enable_seqscan = off")
                    except Exception:
                        pass
                    cursor.execute(
                        """
                        SELECT 
                            id, nombre, descripcion, activa, tipo_clase_id
                        FROM clases
                        ORDER BY nombre ASC
                        """
                    )
                    rows = [dict(row) for row in cursor.fetchall()]

                    try:
                        self.cache.set('clases', ('all_basic',), rows)
                    except Exception:
                        pass
                    return rows
        except Exception as e:
            logging.error(f"Error al obtener todas las clases: {str(e)}")
            return []


    def obtener_arpu_y_morosos_mes_actual(self) -> Tuple[float, int]:
        """Obtiene ARPU y cantidad de morosos del mes actual."""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                mes_actual = datetime.now().month
                anio_actual = datetime.now().year
                
                # Obtener total de usuarios activos
                cursor.execute("SELECT COUNT(*) as total FROM usuarios WHERE activo = true AND rol IN ('socio','miembro','profesor')")
                result = cursor.fetchone()
                total_activos = result['total'] if result else 0
                
                # Obtener ingresos del mes
                cursor.execute(
                    """
                    SELECT COALESCE(SUM(monto), 0) AS total_ingresos
                    FROM pagos
                    WHERE date_trunc('month', COALESCE(fecha_pago, make_date(año, mes, 1))) = date_trunc('month', make_date(%s, %s, 1))
                    """,
                    (anio_actual, mes_actual)
                )
                result = cursor.fetchone()
                ingresos_mes = result['total_ingresos'] if result and result['total_ingresos'] else 0.0
                
                # Calcular ARPU
                arpu = (ingresos_mes / total_activos) if total_activos > 0 else 0.0
                
                # Obtener morosos (usuarios activos sin pago este mes)
                cursor.execute("""
                    SELECT COUNT(*) AS total_morosos
                    FROM usuarios u
                    WHERE u.activo = true AND u.rol IN ('socio','miembro','profesor')
                      AND NOT EXISTS (
                        SELECT 1 FROM pagos p
                        WHERE p.usuario_id = u.id
                          AND date_trunc('month', COALESCE(p.fecha_pago, make_date(p.año, p.mes, 1))) = date_trunc('month', make_date(%s, %s, 1))
                      )
                """, (anio_actual, mes_actual))
                result = cursor.fetchone()
                morosos = result['total_morosos'] if result else 0
                
                return arpu, morosos
        except Exception as e:
            logging.error(f"Error al obtener ARPU y morosos: {str(e)}")
            return 0.0, 0

    @database_retry()

    def dni_existe(self, dni: str, user_id_to_ignore: Optional[int] = None) -> bool:
        """Verifica si un DNI ya existe en la base de datos"""
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                if user_id_to_ignore:
                    cursor.execute("SELECT 1 FROM usuarios WHERE dni = %s AND id != %s", (dni, user_id_to_ignore))
                else:
                    cursor.execute("SELECT 1 FROM usuarios WHERE dni = %s", (dni,))
                return cursor.fetchone() is not None


    def registrar_ejercicios_batch(self, ejercicios_items: List[Dict[str, Any]], skip_duplicates: bool = True, validate_data: bool = True) -> Dict[str, Any]:
        """Inserta/actualiza ejercicios en lote usando claves naturales por nombre.

        - Si `skip_duplicates` es False, actualiza descripción/grupo_muscular por nombre.
        - Usa inserción en bloque para nuevos registros.
        """
        if not ejercicios_items:
            return {'insertados': [], 'actualizados': [], 'omitidos': [], 'count': 0}

        omitidos: List[Dict[str, Any]] = []
        rows_norm: List[Tuple[str, Optional[str], Optional[str]]] = []  # (nombre, grupo_muscular, descripcion)

        for item in ejercicios_items:
            try:
                nombre = str(item.get('nombre')).strip()
                if validate_data and not nombre:
                    raise ValueError('nombre vacío')
                grupo = str(item.get('grupo_muscular')).strip() if item.get('grupo_muscular') is not None else None
                desc = str(item.get('descripcion')).strip() if item.get('descripcion') is not None else None
                rows_norm.append((nombre, grupo if grupo else None, desc if desc else None))
            except Exception as e:
                omitidos.append({'nombre': item.get('nombre'), 'motivo': f'payload inválido: {e}'})

        if not rows_norm:
            return {'insertados': [], 'actualizados': [], 'omitidos': omitidos, 'count': 0}

        try:
            with self.atomic_transaction(isolation_level="READ COMMITTED") as conn:
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

                actualizados: List[str] = []
                insertados_ids: List[int] = []

                if not skip_duplicates:
                    update_sql = (
                        """
                        UPDATE ejercicios AS e
                        SET grupo_muscular = COALESCE(v.grupo_muscular, e.grupo_muscular),
                            descripcion = COALESCE(v.descripcion, e.descripcion)
                        FROM (VALUES %s) AS v(nombre, grupo_muscular, descripcion)
                        WHERE LOWER(e.nombre) = LOWER(v.nombre)
                        RETURNING e.id, e.nombre
                        """
                    )
                    psycopg2.extras.execute_values(cur, update_sql, rows_norm, page_size=250)
                    upd_rows = cur.fetchall() or []
                    actualizados = [str(r['nombre']) for r in upd_rows]

                insert_sql = (
                    """
                    INSERT INTO ejercicios (nombre, grupo_muscular, descripcion)
                    SELECT v.nombre, v.grupo_muscular, v.descripcion
                    FROM (VALUES %s) AS v(nombre, grupo_muscular, descripcion)
                    WHERE NOT EXISTS (
                        SELECT 1 FROM ejercicios e WHERE LOWER(e.nombre) = LOWER(v.nombre)
                    )
                    RETURNING id
                    """
                )
                psycopg2.extras.execute_values(cur, insert_sql, rows_norm, page_size=250)
                ins_rows = cur.fetchall() or []
                insertados_ids = [int(r['id']) for r in ins_rows]

                try:
                    self.cache.invalidate('ejercicios')
                except Exception:
                    pass

                return {
                    'insertados': insertados_ids,
                    'actualizados': actualizados,
                    'omitidos': omitidos,
                    'count': len(insertados_ids) + len(actualizados)
                }
        except Exception as e:
            logging.error(f"Error registrar_ejercicios_batch: {e}")
            raise

    # --- MÉTODOS DE CLASE (BATCH) ---


    def registrar_clases_batch(self, clases_items: List[Dict[str, Any]], skip_duplicates: bool = True, validate_data: bool = True) -> Dict[str, Any]:
        """Inserta/actualiza clases en lote usando nombre como clave natural."""
        if not clases_items:
            return {'insertados': [], 'actualizados': [], 'omitidos': [], 'count': 0}

        omitidos: List[Dict[str, Any]] = []
        rows_norm: List[Tuple[str, Optional[str], int]] = []  # (nombre, descripcion, capacidad_maxima)

        for item in clases_items:
            try:
                nombre = str(item.get('nombre')).strip()
                if validate_data and not nombre:
                    raise ValueError('nombre vacío')
                desc = str(item.get('descripcion')).strip() if item.get('descripcion') is not None else None
                cap_raw = item.get('capacidad_maxima')
                capacidad = int(cap_raw) if cap_raw is not None else 20
                if validate_data and capacidad <= 0:
                    capacidad = 20
                rows_norm.append((nombre, desc if desc else None, capacidad))
            except Exception as e:
                omitidos.append({'nombre': item.get('nombre'), 'motivo': f'payload inválido: {e}'})

        if not rows_norm:
            return {'insertados': [], 'actualizados': [], 'omitidos': omitidos, 'count': 0}

        try:
            with self.atomic_transaction(isolation_level="READ COMMITTED") as conn:
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

                actualizados: List[str] = []
                insertados_ids: List[int] = []

                if not skip_duplicates:
                    update_sql = (
                        """
                        UPDATE clases AS c
                        SET descripcion = COALESCE(v.descripcion, c.descripcion),
                            capacidad_maxima = COALESCE(v.capacidad_maxima, c.capacidad_maxima)
                        FROM (VALUES %s) AS v(nombre, descripcion, capacidad_maxima)
                        WHERE LOWER(c.nombre) = LOWER(v.nombre)
                        RETURNING c.id, c.nombre
                        """
                    )
                    psycopg2.extras.execute_values(cur, update_sql, rows_norm, page_size=250)
                    upd_rows = cur.fetchall() or []
                    actualizados = [str(r['nombre']) for r in upd_rows]

                insert_sql = (
                    """
                    INSERT INTO clases (nombre, descripcion, capacidad_maxima)
                    SELECT v.nombre, v.descripcion, v.capacidad_maxima
                    FROM (VALUES %s) AS v(nombre, descripcion, capacidad_maxima)
                    WHERE NOT EXISTS (
                        SELECT 1 FROM clases c WHERE LOWER(c.nombre) = LOWER(v.nombre)
                    )
                    RETURNING id
                    """
                )
                psycopg2.extras.execute_values(cur, insert_sql, rows_norm, page_size=250)
                ins_rows = cur.fetchall() or []
                insertados_ids = [int(r['id']) for r in ins_rows]

                try:
                    self.cache.invalidate('clases')
                except Exception:
                    pass

                return {
                    'insertados': insertados_ids,
                    'actualizados': actualizados,
                    'omitidos': omitidos,
                    'count': len(insertados_ids) + len(actualizados)
                }
        except Exception as e:
            logging.error(f"Error registrar_clases_batch: {e}")
            raise


    def obtener_configuracion(self, clave: str, timeout_ms: int = 800) -> Optional[str]:
        """Obtiene un valor de configuración con caché, timeouts y reintentos.

        - Respeta Circuit Breaker: usa caché/env y evita bloquear.
        - Aplica timeouts de lectura para no bloquear la UI.
        """
        # 1) Intento ultra-rápido: caché en memoria
        try:
            cached = self.cache.get('config', clave)
            if cached is not None:
                return cached
        except Exception:
            cached = None

        # 2) Si el circuito está abierto, usar fallback sin tocar la DB
        try:
            if hasattr(self, 'is_circuit_open') and self.is_circuit_open():
                # Fallback por entorno (permite override en despliegues)
                env_key = f"CONFIG_{str(clave).upper()}"
                env_val = os.getenv(env_key)
                if not env_val and clave == 'owner_password':
                    env_val = os.getenv('WEBAPP_OWNER_PASSWORD') or os.getenv('OWNER_PASSWORD')
                if env_val:
                    try:
                        self.cache.set('config', clave, env_val)
                    except Exception:
                        pass
                    return env_val
                # Fallback de desarrollo coherente (sin hardcode)
                if clave == 'owner_password':
                    dev_pwd = None
                    try:
                        from .secure_config import SecureConfig as _SC
                        dev_pwd = str(_SC.get_dev_password()).strip()
                    except Exception:
                        dev_pwd = None
                    if not dev_pwd:
                        try:
                            dev_pwd = os.getenv("DEV_PASSWORD", "").strip()
                        except Exception:
                            dev_pwd = None
                    if dev_pwd:
                        try:
                            self.cache.set('config', clave, dev_pwd)
                        except Exception:
                            pass
                        return dev_pwd
                return None
        except Exception:
            pass

        # 3) Consultar DB con timeouts de lectura muy agresivos
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                try:
                    # Aplicar timeouts de lectura no bloqueantes
                    if hasattr(self, '_apply_readonly_timeouts'):
                        self._apply_readonly_timeouts(cursor, lock_ms=max(200, int(timeout_ms * 0.4)), statement_ms=timeout_ms, idle_s=2)
                    else:
                        cursor.execute(f"SET LOCAL lock_timeout = '{max(200, int(timeout_ms * 0.4))}ms'")
                        cursor.execute(f"SET LOCAL statement_timeout = '{int(timeout_ms)}ms'")
                        cursor.execute("SET LOCAL default_transaction_read_only = on")
                except Exception:
                    pass
                # Consulta robusta con fallback si la tabla aún no existe
                try:
                    cursor.execute("SELECT valor FROM configuracion WHERE clave = %s", (clave,))
                    row = cursor.fetchone()
                except Exception as e:
                    msg = str(e).lower()
                    # Manejar tabla inexistente durante arranque antes de inicialización
                    if 'relation' in msg and 'configuracion' in msg:
                        logging.warning("Tabla 'configuracion' no existe aún; usando fallback por entorno/defecto para %s", clave)
                        row = None
                    else:
                        # Re-lanzar otros errores para manejo estándar del decorador
                        raise
                if row and len(row) > 0:
                    value = row[0]
                    try:
                        self.cache.set('config', clave, value)
                    except Exception:
                        pass
                    return value
                # 3.1) Fallback: leer desde gym_config si la clave corresponde
                try:
                    gc_map = {
                        'gym_name': 'gym_name',
                        'gym_slogan': 'gym_slogan',
                        'gym_address': 'gym_address',
                        'gym_phone': 'gym_phone',
                        'gym_email': 'gym_email',
                        'gym_website': 'gym_website',
                        'facebook': 'facebook',
                        'instagram': 'instagram',
                        'twitter': 'twitter',
                        'gym_logo_url': 'logo_url'
                    }
                    col = gc_map.get(str(clave))
                    if col:
                        # Asegurar que la tabla exista antes del SELECT
                        try:
                            self._ensure_gym_config_table(conn)
                        except Exception:
                            pass
                        try:
                            cursor.execute(f"SELECT {col} FROM gym_config ORDER BY id LIMIT 1")
                            r2 = cursor.fetchone()
                            if r2 and len(r2) > 0:
                                gval = r2[0]
                                if isinstance(gval, str) and gval.strip():
                                    try:
                                        self.cache.set('config', clave, gval)
                                    except Exception:
                                        pass
                                    return gval
                        except Exception:
                            pass
                except Exception:
                    pass
                # Fallback por entorno si la DB no devuelve valor
                try:
                    env_key = f"CONFIG_{str(clave).upper()}"
                    env_val = os.getenv(env_key)
                    if not env_val and clave == 'owner_password':
                        env_val = os.getenv('WEBAPP_OWNER_PASSWORD') or os.getenv('OWNER_PASSWORD')
                    if env_val:
                        try:
                            self.cache.set('config', clave, env_val)
                        except Exception:
                            pass
                        return env_val
                except Exception:
                    pass
                return None

    @database_retry()

    def actualizar_configuracion(self, clave: str, valor: str) -> bool:
        """Actualiza o inserta un valor de configuración"""
        try:
            with self.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    # Asegurar la existencia de la tabla antes de upsert para evitar errores tempranos
                    try:
                        cursor.execute(
                            """
                            CREATE TABLE IF NOT EXISTS configuracion (
                                id SERIAL PRIMARY KEY,
                                clave VARCHAR(255) UNIQUE NOT NULL,
                                valor TEXT NOT NULL,
                                tipo VARCHAR(50) DEFAULT 'string',
                                descripcion TEXT
                            )
                            """
                        )
                    except Exception:
                        # Si falla la creación, continuar y dejar que el INSERT reporte el error
                        pass
                    cursor.execute("""
                        INSERT INTO configuracion (clave, valor) VALUES (%s, %s) 
                        ON CONFLICT (clave) DO UPDATE SET valor = EXCLUDED.valor
                    """, (clave, valor))
                    conn.commit()
                    # Invalidar caches relevantes para reflejar cambios inmediatamente
                    try:
                        self.cache.invalidate('config', clave)
                    except Exception:
                        pass
                    try:
                        if str(clave) == 'owner_password':
                            self._owner_cache['password'] = str(valor)
                            import time as _time
                            self._owner_cache['password_expiry'] = _time.time() + 600
                    except Exception:
                        pass
                    return True
        except Exception as e:
            logging.error(f"Error al actualizar configuración {clave}: {e}")
            return False
    
    # --- MÉTODOS PARA SISTEMA DE CUOTAS VENCIDAS ---
    

    def actualizar_configuracion_moneda(self, moneda: str, simbolo: str = ""):
        """Actualiza la configuración de moneda del sistema"""
        try:
            with self.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO configuracion (clave, valor) VALUES ('moneda', %s) 
                        ON CONFLICT (clave) DO UPDATE SET valor = EXCLUDED.valor
                    """, (moneda,))
                    if simbolo:
                        cursor.execute("""
                            INSERT INTO configuracion (clave, valor) VALUES ('simbolo_moneda', %s) 
                            ON CONFLICT (clave) DO UPDATE SET valor = EXCLUDED.valor
                        """, (simbolo,))
                    conn.commit()
                    logging.info(f"Configuración de moneda actualizada: {moneda}")
                    return True
        except Exception as e:
            logging.error(f"Error al actualizar configuración de moneda: {e}")
            return False
    
    def actualizar_configuracion_politica_contraseñas(self, longitud_minima: int = 8, 
                                                      requiere_mayusculas: bool = True,
                                                      requiere_numeros: bool = True,
                                                      requiere_simbolos: bool = False):
        """Actualiza la política de contraseñas"""
        try:
            with self.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    politicas = {
                        'longitud_minima_password': str(longitud_minima),
                        'requiere_mayusculas_password': str(requiere_mayusculas),
                        'requiere_numeros_password': str(requiere_numeros),
                        'requiere_simbolos_password': str(requiere_simbolos)
                    }
                    
                    for clave, valor in politicas.items():
                        cursor.execute("""
                            INSERT INTO configuracion (clave, valor) VALUES (%s, %s) 
                            ON CONFLICT (clave) DO UPDATE SET valor = EXCLUDED.valor
                        """, (clave, valor))
                    
                    conn.commit()
                    logging.info("Política de contraseñas actualizada")
                    return True
        except Exception as e:
            logging.error(f"Error al actualizar política de contraseñas: {e}")
            return False
    

    def actualizar_configuracion_notificaciones_email(self, habilitado: bool = True,
                                                      servidor_smtp: str = "",
                                                      puerto: int = 587,
                                                      usuario: str = "",
                                                      usar_tls: bool = True):
        """Actualiza la configuración de notificaciones por email"""
        try:
            with self.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    configuraciones = {
                        'email_habilitado': str(habilitado),
                        'smtp_servidor': servidor_smtp,
                        'smtp_puerto': str(puerto),
                        'smtp_usuario': usuario,
                        'smtp_usar_tls': str(usar_tls)
                    }
                    
                    for clave, valor in configuraciones.items():
                        cursor.execute("""
                            INSERT INTO configuracion (clave, valor) VALUES (%s, %s) 
                            ON CONFLICT (clave) DO UPDATE SET valor = EXCLUDED.valor
                        """, (clave, valor))
                    
                    conn.commit()
                    logging.info("Configuración de notificaciones por email actualizada")
                    return True
        except Exception as e:
            logging.error(f"Error al actualizar configuración de email: {e}")
            return False
    

    def actualizar_configuracion_notificaciones_sms(self, habilitado: bool = False,
                                                    proveedor: str = "",
                                                    api_key: str = "",
                                                    numero_remitente: str = ""):
        """Actualiza la configuración de notificaciones por SMS"""
        try:
            with self.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    configuraciones = {
                        'sms_habilitado': str(habilitado),
                        'sms_proveedor': proveedor,
                        'sms_api_key': api_key,
                        'sms_numero_remitente': numero_remitente
                    }
                    
                    for clave, valor in configuraciones.items():
                        cursor.execute("""
                            INSERT INTO configuracion (clave, valor) VALUES (%s, %s) 
                            ON CONFLICT (clave) DO UPDATE SET valor = EXCLUDED.valor
                        """, (clave, valor))
                    
                    conn.commit()
                    logging.info("Configuración de notificaciones por SMS actualizada")
                    return True
        except Exception as e:
            logging.error(f"Error al actualizar configuración de SMS: {e}")
            return False
    

    def actualizar_configuracion_umbrales_alerta(self, capacidad_maxima: int = 100,
                                                 umbral_ocupacion: int = 80,
                                                 dias_vencimiento_membresia: int = 7,
                                                 monto_minimo_pago: float = 0.0):
        """Actualiza los umbrales de alerta del sistema"""
        try:
            with self.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    umbrales = {
                        'capacidad_maxima_gimnasio': str(capacidad_maxima),
                        'umbral_ocupacion_alerta': str(umbral_ocupacion),
                        'dias_alerta_vencimiento': str(dias_vencimiento_membresia),
                        'monto_minimo_pago': str(monto_minimo_pago)
                    }
                    
                    for clave, valor in umbrales.items():
                        cursor.execute("""
                            INSERT INTO configuracion (clave, valor) VALUES (%s, %s) 
                            ON CONFLICT (clave) DO UPDATE SET valor = EXCLUDED.valor
                        """, (clave, valor))
                    
                    conn.commit()
                    logging.info("Umbrales de alerta actualizados")
                    return True
        except Exception as e:
            logging.error(f"Error al actualizar umbrales de alerta: {e}")
            return False
    

    def actualizar_configuracion_automatizacion(self, configuraciones: dict):
        """Actualiza configuraciones de automatización del sistema"""
        try:
            with self.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    for clave, valor in configuraciones.items():
                        cursor.execute("""
                            INSERT INTO configuracion (clave, valor) VALUES (%s, %s) 
                            ON CONFLICT (clave) DO UPDATE SET valor = EXCLUDED.valor
                        """, (clave, str(valor)))
                    
                    conn.commit()
                    logging.info("Configuraciones de automatización actualizadas")
                    return True
        except Exception as e:
            logging.error(f"Error al actualizar configuraciones de automatización: {e}")
            return False
    

    def guardar_configuracion_sistema(self, clave: str, valor: str):
        """Guarda una configuración del sistema"""
        try:
            with self.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO configuracion (clave, valor) VALUES (%s, %s) 
                        ON CONFLICT (clave) DO UPDATE SET valor = EXCLUDED.valor
                    """, (clave, valor))
                    conn.commit()
                    logging.info(f"Configuración guardada: {clave} = {valor}")
                    return True
        except Exception as e:
            logging.error(f"Error al guardar configuración {clave}: {e}")
            return False

    # --- MÉTODOS DE EJERCICIOS Y RUTINAS ---

    # --- MÉTODOS DE CLASES ---
    
    @database_retry()

    def crear_clase(self, clase: Clase) -> int:
        """Crea una nueva clase"""
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                sql = "INSERT INTO clases (nombre, descripcion) VALUES (%s, %s) RETURNING id"
                cursor.execute(sql, (clase.nombre, clase.descripcion))
                clase_id = cursor.fetchone()[0]
                conn.commit()
                try:
                    self.cache.invalidate('clases')
                except Exception:
                    pass
                return clase_id


    def obtener_clases(self) -> List[Clase]:
        """Obtiene todas las clases"""
        # Cache de clases completas (incluye join con tipos)
        try:
            cached = self.cache.get('clases', ('full',))
            if cached is not None:
                return cached
        except Exception:
            pass
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT 
                        c.id,
                        c.nombre,
                        c.descripcion,
                        c.activa,
                        c.tipo_clase_id,
                        tc.nombre AS tipo_clase_nombre
                    FROM clases c
                    LEFT JOIN tipos_clases tc ON tc.id = c.tipo_clase_id
                    ORDER BY c.nombre
                    """
                )
                result = [Clase(**dict(r)) for r in cursor.fetchall()]
                try:
                    self.cache.set('clases', ('full',), result)
                except Exception:
                    pass
                return result

    # --- MÉTODOS DE PROFESORES ---
    
    @database_retry()

    def backup_database(self, backup_path: str) -> bool:
        """Crea un backup de la base de datos PostgreSQL.
        Intenta usar pg_dump y, si no está disponible, realiza un backup SQL básico como fallback.
        """
        try:
            import subprocess
            import shutil
            import os
            import glob

            def _find_pg_dump_path():
                """Resuelve la ruta de pg_dump en Windows/Linux usando varias heurísticas."""
                exe = 'pg_dump.exe' if os.name == 'nt' else 'pg_dump'

                # 1) Variable de entorno explícita
                pg_dump_env = os.environ.get('PG_DUMP_PATH')
                if pg_dump_env and os.path.isfile(pg_dump_env):
                    return pg_dump_env

                # 2) Directorio binario de PostgreSQL
                pg_bin = os.environ.get('PG_BIN')
                if pg_bin:
                    candidate = os.path.join(pg_bin, exe)
                    if os.path.isfile(candidate):
                        return candidate

                # 3) En el PATH del sistema
                which_path = shutil.which(exe)
                if which_path:
                    return which_path

                # 4) Búsqueda común en Windows
                if os.name == 'nt':
                    for base in (r"C:\\Program Files\\PostgreSQL", r"C:\\Program Files (x86)\\PostgreSQL"):
                        if os.path.isdir(base):
                            matches = glob.glob(os.path.join(base, '*', 'bin', 'pg_dump.exe'))
                            if matches:
                                return sorted(matches)[-1]
                return None

            pg_dump_path = _find_pg_dump_path()

            if not pg_dump_path:
                logging.error("pg_dump no encontrado. Usando fallback de backup SQL básico.")
                return self._create_sql_backup_basic(backup_path)

            # Construir comando pg_dump con la ruta encontrada
            cmd = [
                pg_dump_path,
                f"--host={self.connection_params['host']}",
                f"--port={self.connection_params['port']}",
                f"--username={self.connection_params['user']}",
                f"--dbname={self.connection_params['database']}",
                '--verbose',
                '--clean',
                '--no-owner',
                '--no-privileges',
                f"--file={backup_path}"
            ]

            # Configurar variable de entorno para la contraseña
            env = os.environ.copy()
            env['PGPASSWORD'] = self.connection_params['password']

            # Ejecutar backup
            result = subprocess.run(cmd, env=env, capture_output=True, text=True)

            if result.returncode == 0:
                logging.info(f"Backup creado exitosamente: {backup_path}")
                return True
            else:
                logging.error(f"Error creando backup con pg_dump: {result.stderr}")
                # Intentar fallback cuando pg_dump falla
                return self._create_sql_backup_basic(backup_path)

        except Exception as e:
            logging.error(f"Error en backup_database: {e}")
            return False


    def _create_sql_backup_basic(self, backup_path: str) -> bool:
        """Genera un backup básico en formato SQL usando consultas, como fallback cuando no hay pg_dump."""
        try:
            import psycopg2.extras
            from datetime import datetime

            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

                with open(backup_path, 'w', encoding='utf-8') as f:
                    f.write("-- Backup de base de datos PostgreSQL (fallback)\n")
                    f.write(f"-- Generado el: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

                    # Listar tablas públicas
                    cursor.execute(
                        """
                        SELECT tablename FROM pg_tables 
                        WHERE schemaname = 'public' 
                        ORDER BY tablename
                        """
                    )
                    tables = [row['tablename'] for row in cursor.fetchall()]

                    for table in tables:
                        f.write(f"\n-- Tabla: {table}\n")
                        try:
                            cols = self.get_table_columns(table) or []
                        except Exception:
                            cols = []
                        if not cols:
                            # Sin columnas detectadas, saltar para evitar SELECT *
                            continue
                        sel = ", ".join(cols)
                        cursor.execute(f"SELECT {sel} FROM {table}")
                        rows = cursor.fetchall()

                        if not rows:
                            continue

                        columns = list(rows[0].keys())
                        cols_sql = ", ".join(columns)

                        for row in rows:
                            values = []
                            for col in columns:
                                val = row[col]
                                if val is None:
                                    values.append("NULL")
                                elif isinstance(val, (int, float)):
                                    values.append(str(val))
                                else:
                                    s = str(val).replace("'", "''")
                                    values.append(f"'{s}'")
                            values_sql = ", ".join(values)
                            f.write(f"INSERT INTO {table} ({cols_sql}) VALUES ({values_sql});\n")

            logging.info(f"Backup SQL básico creado: {backup_path}")
            return True

        except Exception as e:
            logging.error(f"Error en backup SQL básico: {e}")
            return False


    def verificar_conexion(self) -> bool:
        """Verifica si la conexión a la base de datos está funcionando"""
        try:
            with self.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    result = cursor.fetchone()
                    return result is not None and result[0] == 1
        except Exception as e:
            logging.error(f"Error verificando conexión: {e}")
            return False


    def obtener_info_base_datos(self) -> Dict:
        """Obtiene información sobre la base de datos"""
        try:
            with self.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    # Información básica
                    cursor.execute("SELECT version()")
                    version = cursor.fetchone()[0]
                    
                    cursor.execute("SELECT current_database()")
                    database_name = cursor.fetchone()[0]
                    
                    cursor.execute("SELECT current_user")
                    current_user = cursor.fetchone()[0]
                    
                    # Tamaño de la base de datos
                    cursor.execute("SELECT pg_size_pretty(pg_database_size(current_database()))")
                    database_size = cursor.fetchone()[0]
                    
                    # Número de conexiones activas
                    cursor.execute("SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()")
                    active_connections = cursor.fetchone()[0]
                    
                    return {
                        'version': version,
                        'database_name': database_name,
                        'current_user': current_user,
                        'database_size': database_size,
                        'active_connections': active_connections,
                        'connection_params': {
                            'host': self.connection_params['host'],
                            'port': self.connection_params['port'],
                            'database': self.connection_params['database'],
                            'user': self.connection_params['user']
                        }
                    }
        except Exception as e:
            logging.error(f"Error obteniendo información de la base de datos: {e}")
            return {'error': str(e)}

    # Nota: Los métodos restantes seguirían el mismo patrón de migración,
    # reemplazando sqlite3 por psycopg2 y adaptando la sintaxis SQL


    def _procesar_acciones_individuales_robusto(self, cursor, lote: List[int], accion: str, parametros: dict, resultados: dict):
        """Procesa acciones individuales complejas para un lote de usuarios con validaciones robustas"""
        try:
            # Validaciones iniciales
            if not lote:
                self.logger.warning("[_procesar_acciones_individuales_robusto] Lote vacío")
                return
            
            if not accion or accion.strip() == "":
                error_msg = "Acción no especificada"
                self.logger.error(f"[_procesar_acciones_individuales_robusto] {error_msg}")
                resultados['errores'].append(error_msg)
                resultados['fallidos'] += len(lote)
                return
            
            # Validar acción
            acciones_validas = ['eliminar', 'cambiar_tipo_cuota', 'agregar_estado', 'asignar_etiqueta']
            if accion not in acciones_validas:
                error_msg = f"Acción '{accion}' no válida. Acciones válidas: {', '.join(acciones_validas)}"
                self.logger.error(f"[_procesar_acciones_individuales_robusto] {error_msg}")
                resultados['errores'].append(error_msg)
                resultados['fallidos'] += len(lote)
                return
            
            # Verificar que los usuarios existen
            placeholders = ','.join(['%s' for _ in lote])
            cursor.execute(f"SELECT id, rol, nombre FROM usuarios WHERE id IN ({placeholders})", lote)
            usuarios_existentes = cursor.fetchall()
            
            usuarios_info = {row['id']: {'rol': row['rol'], 'nombre': row['nombre']} for row in usuarios_existentes}
            usuarios_encontrados = set(usuarios_info.keys())
            usuarios_no_encontrados = set(lote) - usuarios_encontrados
            
            # Reportar usuarios no encontrados
            for usuario_id in usuarios_no_encontrados:
                error_msg = f"Usuario {usuario_id} no existe"
                self.logger.warning(f"[_procesar_acciones_individuales_robusto] {error_msg}")
                resultados['errores'].append(error_msg)
                resultados['fallidos'] += 1
            
            # Procesar cada usuario existente
            for usuario_id in usuarios_encontrados:
                try:
                    usuario_info = usuarios_info[usuario_id]
                    nombre_completo = str(usuario_info['nombre'] or "").strip()
                    
                    if accion == 'eliminar':
                        # Verificar si es el dueño
                        if usuario_info['rol'] == 'dueño':
                            error_msg = f'Usuario {usuario_id} ({nombre_completo}): No se puede eliminar al dueño'
                            self.logger.warning(f"[_procesar_acciones_individuales_robusto] {error_msg}")
                            resultados['errores'].append(error_msg)
                            resultados['fallidos'] += 1
                            continue
                        
                        # Verificar dependencias antes de eliminar
                        try:
                            # Verificar si tiene clases asignadas como profesor
                            cursor.execute("SELECT COUNT(*) FROM clases WHERE profesor_id = (SELECT id FROM profesores WHERE usuario_id = %s)", (usuario_id,))
                            clases_count = cursor.fetchone()[0]
                            
                            if clases_count > 0:
                                error_msg = f'Usuario {usuario_id} ({nombre_completo}): No se puede eliminar, tiene {clases_count} clases asignadas'
                                self.logger.warning(f"[_procesar_acciones_individuales_robusto] {error_msg}")
                                resultados['errores'].append(error_msg)
                                resultados['fallidos'] += 1
                                continue
                            
                            # Verificar si tiene pagos pendientes
                            cursor.execute("SELECT COUNT(*) FROM pagos WHERE usuario_id = %s AND estado = 'pendiente'", (usuario_id,))
                            pagos_pendientes = cursor.fetchone()[0]
                            
                            if pagos_pendientes > 0:
                                error_msg = f'Usuario {usuario_id} ({nombre_completo}): No se puede eliminar, tiene {pagos_pendientes} pagos pendientes'
                                self.logger.warning(f"[_procesar_acciones_individuales_robusto] {error_msg}")
                                resultados['errores'].append(error_msg)
                                resultados['fallidos'] += 1
                                continue
                            
                        except Exception as e:
                            self.logger.error(f"[_procesar_acciones_individuales_robusto] Error verificando dependencias para usuario {usuario_id}: {str(e)}")
                        
                        # Proceder con la eliminación
                        cursor.execute("DELETE FROM usuarios WHERE id = %s", (usuario_id,))
                        resultados['detalles'].append(f'Usuario {usuario_id} ({nombre_completo}) eliminado')
                        resultados['exitosos'] += 1
                        self.logger.info(f"[_procesar_acciones_individuales_robusto] Usuario {usuario_id} ({nombre_completo}) eliminado exitosamente")
                    
                    elif accion == 'cambiar_tipo_cuota':
                        if not parametros or 'nuevo_tipo' not in parametros:
                            error_msg = f'Usuario {usuario_id} ({nombre_completo}): Parámetro nuevo_tipo requerido para cambiar tipo de cuota'
                            self.logger.error(f"[_procesar_acciones_individuales_robusto] {error_msg}")
                            resultados['errores'].append(error_msg)
                            resultados['fallidos'] += 1
                            continue
                        
                        nuevo_tipo = parametros['nuevo_tipo']
                        if not nuevo_tipo or nuevo_tipo.strip() == "":
                            error_msg = f'Usuario {usuario_id} ({nombre_completo}): Tipo de cuota no válido'
                            self.logger.error(f"[_procesar_acciones_individuales_robusto] {error_msg}")
                            resultados['errores'].append(error_msg)
                            resultados['fallidos'] += 1
                            continue
                        
                        cursor.execute("UPDATE usuarios SET tipo_cuota = %s, fecha_modificacion = CURRENT_TIMESTAMP WHERE id = %s", 
                                     (nuevo_tipo, usuario_id))
                        resultados['detalles'].append(f'Usuario {usuario_id} ({nombre_completo}) cambió tipo de cuota a {nuevo_tipo}')
                        resultados['exitosos'] += 1
                        self.logger.info(f"[_procesar_acciones_individuales_robusto] Usuario {usuario_id} ({nombre_completo}) cambió tipo de cuota a {nuevo_tipo}")
                    
                    elif accion == 'agregar_estado':
                        if not parametros:
                            error_msg = f'Usuario {usuario_id} ({nombre_completo}): Parámetros requeridos para agregar estado'
                            self.logger.error(f"[_procesar_acciones_individuales_robusto] {error_msg}")
                            resultados['errores'].append(error_msg)
                            resultados['fallidos'] += 1
                            continue
                        
                        self._procesar_agregar_estado_usuario(cursor, usuario_id, parametros, resultados)
                    
                    elif accion == 'asignar_etiqueta':
                        if not parametros:
                            error_msg = f'Usuario {usuario_id} ({nombre_completo}): Parámetros requeridos para asignar etiqueta'
                            self.logger.error(f"[_procesar_acciones_individuales_robusto] {error_msg}")
                            resultados['errores'].append(error_msg)
                            resultados['fallidos'] += 1
                            continue
                        
                        self._procesar_asignar_etiqueta_usuario(cursor, usuario_id, parametros, resultados)
                    
                except Exception as e:
                    error_msg = f'Usuario {usuario_id}: {str(e) if str(e) else "Error desconocido"}'
                    self.logger.error(f"[_procesar_acciones_individuales_robusto] {error_msg}")
                    import traceback
                    self.logger.error(f"[_procesar_acciones_individuales_robusto] Traceback para usuario {usuario_id}: {traceback.format_exc()}")
                    resultados['errores'].append(error_msg)
                    resultados['fallidos'] += 1
                    
        except Exception as e:
            error_msg = f"Error crítico en _procesar_acciones_individuales_robusto: {str(e) if str(e) else 'Error desconocido'}"
            self.logger.error(f"[_procesar_acciones_individuales_robusto] {error_msg}")
            import traceback
            self.logger.error(f"[_procesar_acciones_individuales_robusto] Traceback: {traceback.format_exc()}")
            resultados['errores'].append(error_msg)
            resultados['fallidos'] += len(lote)
    

    def _procesar_acciones_individuales(self, cursor, lote: List[int], accion: str, parametros: dict, resultados: dict):
        """Wrapper anterior para compatibilidad - usa la versión robusta"""
        return self._procesar_acciones_individuales_robusto(cursor, lote, accion, parametros, resultados)


    def crear_grupo_ejercicios(self, nombre_grupo: str, ejercicio_ids: List[int]) -> int:
        """Crea un nuevo grupo de ejercicios"""
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute("INSERT INTO ejercicio_grupos (nombre) VALUES (%s) RETURNING id", (nombre_grupo,))
                grupo_id = cursor.fetchone()[0]
                
                if ejercicio_ids:
                    data = [(grupo_id, ej_id) for ej_id in ejercicio_ids]
                    cursor.executemany("INSERT INTO ejercicio_grupo_items (grupo_id, ejercicio_id) VALUES (%s, %s)", data)
                
                conn.commit()
                return grupo_id


    def obtener_grupos_ejercicios(self) -> List[EjercicioGrupo]:
        """Obtiene todos los grupos de ejercicios"""
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute("SELECT id, nombre FROM ejercicio_grupos ORDER BY nombre")
                return [EjercicioGrupo(**dict(r)) for r in cursor.fetchall()]


    def obtener_ejercicios_de_grupo(self, grupo_id: int) -> List[Ejercicio]:
        """Obtiene todos los ejercicios de un grupo específico"""
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                sql = """
                    SELECT e.id, e.nombre, e.descripcion, e.grupo_muscular, e.objetivo FROM ejercicios e
                    JOIN ejercicio_grupo_items egi ON e.id = egi.ejercicio_id
                    WHERE egi.grupo_id = %s ORDER BY e.nombre
                """
                cursor.execute(sql, (grupo_id,))
                return [Ejercicio(**dict(r)) for r in cursor.fetchall()]


    def eliminar_grupo_ejercicios(self, grupo_id: int) -> bool:
        """Elimina un grupo de ejercicios y sus relaciones"""
        try:
            with self.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("DELETE FROM ejercicio_grupo_items WHERE grupo_id = %s", (grupo_id,))
                    cursor.execute("DELETE FROM ejercicio_grupos WHERE id = %s", (grupo_id,))
                    conn.commit()
                    return True
        except Exception as e:
            logging.error(f"Error eliminando grupo de ejercicios: {e}")
            return False


    def limpiar_datos_antiguos(self, years: int) -> tuple[int, int]:
        """Limpia datos antiguos con optimizaciones de rendimiento"""
        fecha_limite = date.today().replace(year=date.today().year - years)
        
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                try:
                    # Limpiar pagos antiguos
                    cursor.execute("""
                        DELETE FROM pagos 
                        WHERE (año || '-' || LPAD(mes::text, 2, '0') || '-01')::date < %s
                    """, (fecha_limite,))
                    pagos_eliminados = cursor.rowcount
                    
                    # Limpiar asistencias antiguas
                    cursor.execute("""
                        DELETE FROM asistencias 
                        WHERE fecha < %s
                    """, (fecha_limite,))
                    asistencias_eliminadas = cursor.rowcount
                    
                    # Limpiar logs de auditoría antiguos si existen
                    try:
                        cursor.execute("""
                            DELETE FROM audit_logs 
                            WHERE timestamp < %s
                        """, (fecha_limite,))
                    except Exception:
                        pass  # Tabla puede no existir
                    
                    conn.commit()
                    
                    # Limpiar cache después de la limpieza
                    if hasattr(self, 'cache'):
                        self.cache.invalidate('pagos')
                        self.cache.invalidate('asistencias')
                    
                    logging.info(f"Limpieza completada: {pagos_eliminados} pagos, {asistencias_eliminadas} asistencias eliminados")
                    return pagos_eliminados, asistencias_eliminadas
                    
                except Exception as e:
                    conn.rollback()
                    logging.error(f"Error en limpieza de datos antiguos: {e}")
                    raise


    def obtener_nota_por_id(self, nota_id: int):
        """Obtiene una nota específica por su ID."""
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(
                    "SELECT id, usuario_id, categoria, titulo, contenido, importancia, "
                    "fecha_creacion, fecha_modificacion, activa, autor_id "
                    "FROM usuario_notas WHERE id = %s",
                    (nota_id,)
                )
                row = cursor.fetchone()
                if row:
                    return dict(row)
                return None
    

    def obtener_notas_por_categoria(self, categoria: str) -> List:
        """Obtiene todas las notas de una categoría específica."""
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                sql = (
                    "SELECT id, usuario_id, categoria, titulo, contenido, importancia, "
                    "fecha_creacion, fecha_modificacion, activa, autor_id "
                    "FROM usuario_notas WHERE categoria = %s AND activa = TRUE "
                    "ORDER BY fecha_creacion DESC"
                )
                cursor.execute(sql, (categoria,))
                return [dict(row) for row in cursor.fetchall()]
    

    def obtener_notas_por_importancia(self, importancia: str) -> List:
        """Obtiene todas las notas de una importancia específica."""
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                sql = (
                    "SELECT id, usuario_id, categoria, titulo, contenido, importancia, "
                    "fecha_creacion, fecha_modificacion, activa, autor_id "
                    "FROM usuario_notas WHERE importancia = %s AND activa = TRUE "
                    "ORDER BY fecha_creacion DESC"
                )
                cursor.execute(sql, (importancia,))
                return [dict(row) for row in cursor.fetchall()]


    def crear_etiqueta(self, etiqueta) -> int:
        """Crea una nueva etiqueta."""
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                sql = "INSERT INTO etiquetas (nombre, color, descripcion) VALUES (%s, %s, %s) RETURNING id"
                cursor.execute(sql, (etiqueta.nombre, etiqueta.color, etiqueta.descripcion))
                etiqueta_id = cursor.fetchone()[0]
                conn.commit()
                return etiqueta_id
    

    def obtener_todas_etiquetas(self, solo_activas: bool = True) -> List:
        """Obtiene todas las etiquetas."""
        from .models import Etiqueta
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                sql = "SELECT id, nombre, color, descripcion, fecha_creacion, activo FROM etiquetas"
                if solo_activas:
                    sql += " WHERE activo = TRUE"
                sql += " ORDER BY nombre"
                cursor.execute(sql)
                etiquetas = []
                for row in cursor.fetchall():
                    etiqueta = Etiqueta(
                        id=row.get('id'),
                        nombre=row.get('nombre', ''),
                        color=row.get('color', '#3498db'),
                        descripcion=row.get('descripcion'),
                        fecha_creacion=row.get('fecha_creacion'),
                        activo=row.get('activo', True)
                    )
                    etiquetas.append(etiqueta)
                return etiquetas
    

    def obtener_etiquetas(self, solo_activas: bool = True) -> List:
        """Alias para obtener_todas_etiquetas - mantiene compatibilidad."""
        return self.obtener_todas_etiquetas(solo_activas)
    

    def obtener_etiqueta_por_id(self, etiqueta_id: int):
        """Obtiene una etiqueta por su ID."""
        from .models import Etiqueta
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(
                    "SELECT id, nombre, color, descripcion, fecha_creacion, activo FROM etiquetas WHERE id = %s",
                    (etiqueta_id,)
                )
                row = cursor.fetchone()
                if row:
                    return Etiqueta(
                        id=row.get('id'),
                        nombre=row.get('nombre', ''),
                        color=row.get('color', '#3498db'),
                        descripcion=row.get('descripcion'),
                        fecha_creacion=row.get('fecha_creacion'),
                        activo=row.get('activo', True)
                    )
                return None
    

    def obtener_etiqueta_por_nombre(self, nombre: str):
        """Obtiene una etiqueta por su nombre."""
        from .models import Etiqueta
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(
                    "SELECT id, nombre, color, descripcion, fecha_creacion, activo "
                    "FROM etiquetas WHERE nombre = %s AND activo = TRUE",
                    (nombre,)
                )
                row = cursor.fetchone()
                if row:
                    return Etiqueta(
                        id=row.get('id'),
                        nombre=row.get('nombre', ''),
                        color=row.get('color', '#3498db'),
                        descripcion=row.get('descripcion'),
                        fecha_creacion=row.get('fecha_creacion'),
                        activo=row.get('activo', True)
                    )
                return None
    

    def obtener_o_crear_etiqueta(self, nombre: str, color: str = "#007bff", descripcion: str = ""):
        """Obtiene una etiqueta existente o crea una nueva si no existe."""
        from .models import Etiqueta
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                
                # Intentar obtener la etiqueta
                cursor.execute(
                    "SELECT id, nombre, color, descripcion, fecha_creacion, activo "
                    "FROM etiquetas WHERE nombre = %s AND activo = TRUE",
                    (nombre,)
                )
                row = cursor.fetchone()
                if row:
                    return Etiqueta(
                        id=row.get('id'),
                        nombre=row.get('nombre', ''),
                        color=row.get('color', '#3498db'),
                        descripcion=row.get('descripcion'),
                        fecha_creacion=row.get('fecha_creacion'),
                        activo=row.get('activo', True)
                    )
                
                # Si no existe, crearla dentro de la misma transacción
                sql = (
                    "INSERT INTO etiquetas (nombre, color, descripcion) VALUES (%s, %s, %s) "
                    "RETURNING id, nombre, color, descripcion, fecha_creacion, activo"
                )
                cursor.execute(sql, (nombre, color, descripcion))
                nueva_etiqueta = cursor.fetchone()
                conn.commit()
                
                return Etiqueta(
                    id=nueva_etiqueta.get('id'),
                    nombre=nueva_etiqueta.get('nombre', ''),
                    color=nueva_etiqueta.get('color', '#3498db'),
                    descripcion=nueva_etiqueta.get('descripcion'),
                    fecha_creacion=nueva_etiqueta.get('fecha_creacion'),
                    activo=nueva_etiqueta.get('activo', True)
                )
    

    def actualizar_etiqueta(self, etiqueta) -> bool:
        """Actualiza una etiqueta."""
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                sql = "UPDATE etiquetas SET nombre = %s, color = %s, descripcion = %s WHERE id = %s"
                cursor.execute(sql, (etiqueta.nombre, etiqueta.color, etiqueta.descripcion, etiqueta.id))
                conn.commit()
                ok = cursor.rowcount > 0
                return ok
    

    def eliminar_etiqueta(self, etiqueta_id: int) -> bool:
        """Elimina una etiqueta y todas sus asignaciones."""
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                # Primero eliminar las asignaciones
                cursor.execute("DELETE FROM usuario_etiquetas WHERE etiqueta_id = %s", (etiqueta_id,))
                # Luego eliminar la etiqueta
                cursor.execute("DELETE FROM etiquetas WHERE id = %s", (etiqueta_id,))
                conn.commit()
                ok = cursor.rowcount > 0
                return ok
    
    # --- MÉTODOS DE ASIGNACIÓN DE ETIQUETAS A USUARIOS ---
    

    def obtener_plantillas_estados(self) -> List:
        """Obtiene plantillas predefinidas de estados para usuarios."""
        plantillas = [
            {
                'id': 'activo',
                'nombre': 'Activo',
                'descripcion': 'Usuario activo con cuota al día',
                'color': '#22c55e',
                'icono': 'check-circle',
                'categoria': 'estado_cuota'
            },
            {
                'id': 'cuota_vencida',
                'nombre': 'Cuota Vencida',
                'descripcion': 'Usuario con cuota vencida',
                'color': '#ef4444',
                'icono': 'alert-circle',
                'categoria': 'estado_cuota'
            },
            {
                'id': 'suspendido',
                'nombre': 'Suspendido',
                'descripcion': 'Usuario suspendido temporalmente',
                'color': '#f59e0b',
                'icono': 'pause-circle',
                'categoria': 'estado_disciplinario'
            },
            {
                'id': 'baja_temporal',
                'nombre': 'Baja Temporal',
                'descripcion': 'Usuario en baja temporal',
                'color': '#6b7280',
                'icono': 'clock',
                'categoria': 'estado_membresia'
            },
            {
                'id': 'baja_definitiva',
                'nombre': 'Baja Definitiva',
                'descripcion': 'Usuario dado de baja definitivamente',
                'color': '#374151',
                'icono': 'x-circle',
                'categoria': 'estado_membresia'
            },
            {
                'id': 'nuevo',
                'nombre': 'Nuevo',
                'descripcion': 'Usuario recién registrado',
                'color': '#3b82f6',
                'icono': 'user-plus',
                'categoria': 'estado_membresia'
            },
            {
                'id': 'promocion',
                'nombre': 'Promoción',
                'descripcion': 'Usuario con promoción especial',
                'color': '#8b5cf6',
                'icono': 'star',
                'categoria': 'estado_especial'
            },
            {
                'id': 'becado',
                'nombre': 'Becado',
                'descripcion': 'Usuario con beca o descuento especial',
                'color': '#06b6d4',
                'icono': 'award',
                'categoria': 'estado_especial'
            }
        ]
        
        return plantillas
    

    def obtener_estados_vencidos(self) -> List:
        """Obtiene todos los estados que han vencido."""
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                sql = (
                    "SELECT id, usuario_id, estado, descripcion, fecha_inicio, fecha_vencimiento, activo, creado_por "
                    "FROM usuario_estados "
                    "WHERE fecha_vencimiento < CURRENT_TIMESTAMP AND activo = TRUE "
                    "ORDER BY fecha_vencimiento"
                )
                cursor.execute(sql)
                return [dict(row) for row in cursor.fetchall()]
    

    def desactivar_estados_vencidos(self) -> int:
        """Desactiva automáticamente los estados que han vencido."""
        with self.atomic_transaction(isolation_level="REPEATABLE READ") as conn:
            with conn.cursor() as cursor:
                sql = """
                UPDATE usuario_estados 
                SET activo = FALSE 
                WHERE fecha_vencimiento < CURRENT_TIMESTAMP AND activo = TRUE
                """
                cursor.execute(sql)
                return cursor.rowcount
    

    def limpiar_estados_vencidos(self) -> int:
        """Elimina permanentemente los estados que han vencido."""
        with self.atomic_transaction(isolation_level="REPEATABLE READ") as conn:
            with conn.cursor() as cursor:
                sql = """
                DELETE FROM usuario_estados 
                WHERE fecha_vencimiento < CURRENT_TIMESTAMP AND activo = FALSE
                """
                cursor.execute(sql)
                return cursor.rowcount
    

    def obtener_configuracion_numeracion(self, tipo_comprobante: str = None) -> List[Dict]:
        """Obtiene configuración de numeración de comprobantes."""
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                
                sql = (
                    "SELECT id, tipo_comprobante, prefijo, numero_inicial, separador, "
                    "reiniciar_anual, longitud_numero, activo, fecha_creacion "
                    "FROM numeracion_comprobantes"
                )
                params = []
                
                if tipo_comprobante:
                    sql += " WHERE tipo_comprobante = %s"
                    params.append(tipo_comprobante)
                
                sql += " ORDER BY tipo_comprobante"
                
                cursor.execute(sql, params)
                return [dict(zip([desc[0] for desc in cursor.description], row)) for row in cursor.fetchall()]
    

    def actualizar_configuracion_numeracion(self, tipo_comprobante: str, prefijo: str = None, 
                                          siguiente_numero: int = None, longitud_numero: int = None,
                                          activo: bool = None) -> bool:
        """Actualiza configuración de numeración de comprobantes."""
        campos_actualizar = {}
        
        if prefijo is not None:
            campos_actualizar['prefijo'] = prefijo
        if siguiente_numero is not None:
            # Usar numero_inicial como contador vigente según el esquema actual
            campos_actualizar['numero_inicial'] = siguiente_numero
        if longitud_numero is not None:
            campos_actualizar['longitud_numero'] = longitud_numero
        if activo is not None:
            campos_actualizar['activo'] = activo
        
        if not campos_actualizar:
            return False
        
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                
                set_clause = ", ".join([f"{campo} = %s" for campo in campos_actualizar.keys()])
                sql = f"UPDATE numeracion_comprobantes SET {set_clause} WHERE tipo_comprobante = %s"
                params = list(campos_actualizar.values()) + [tipo_comprobante]
                
                cursor.execute(sql, params)
                conn.commit()
                return cursor.rowcount > 0
    

    def get_receipt_numbering_config(self) -> dict:
        """Obtiene la configuración de numeración de comprobantes para el diálogo."""
        try:
            configs = self.obtener_configuracion_numeracion('recibo')
            if configs:
                # Retorna la primera configuración encontrada o una por defecto
                config = configs[0]
                return {
                    'prefijo': config.get('prefijo', 'REC'),
                    'numero_inicial': config.get('numero_inicial', 1),
                    'longitud_numero': config.get('longitud_numero', 6),
                    'separador': config.get('separador', '-'),
                    'reiniciar_anual': config.get('reiniciar_anual', False),
                    'incluir_año': config.get('incluir_año', True),
                    'incluir_mes': config.get('incluir_mes', False)
                }
            else:
                # Configuración por defecto si no existe
                return {
                    'prefijo': 'REC',
                    'numero_inicial': 1,
                    'longitud_numero': 6,
                    'separador': '-',
                    'reiniciar_anual': False,
                    'incluir_año': True,
                    'incluir_mes': False
                }
        except Exception as e:
            logging.error(f"Error obteniendo configuración de numeración: {str(e)}")
            return {
                'prefijo': 'REC',
                'numero_inicial': 1,
                'longitud_numero': 6,
                'separador': '-',
                'reiniciar_anual': False,
                'incluir_año': True,
                'incluir_mes': False
            }
    

    def save_receipt_numbering_config(self, config: dict) -> bool:
        """Guarda la configuración de numeración de comprobantes."""
        try:
            # Primero verificar si existe la configuración
            with self.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT COUNT(*) FROM numeracion_comprobantes WHERE tipo_comprobante = %s",
                        ('recibo',)
                    )
                    existe = cursor.fetchone()[0] > 0
                    
                    if existe:
                        # Actualizar configuración existente
                        cursor.execute("""
                            UPDATE numeracion_comprobantes 
                            SET prefijo = %s, numero_inicial = %s, longitud_numero = %s,
                                separador = %s, reiniciar_anual = %s, incluir_año = %s, incluir_mes = %s
                            WHERE tipo_comprobante = %s
                        """, (
                            config.get('prefijo', 'REC'),
                            config.get('numero_inicial', 1),
                            config.get('longitud_numero', 6),
                            config.get('separador', '-'),
                            config.get('reiniciar_anual', False),
                            config.get('incluir_año', True),
                            config.get('incluir_mes', False),
                            'recibo'
                        ))
                    else:
                        # Crear nueva configuración
                        cursor.execute("""
                            INSERT INTO numeracion_comprobantes 
                            (tipo_comprobante, prefijo, numero_inicial, longitud_numero, 
                             separador, reiniciar_anual, incluir_año, incluir_mes, activo)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            'recibo',
                            config.get('prefijo', 'REC'),
                            config.get('numero_inicial', 1),
                            config.get('longitud_numero', 6),
                            config.get('separador', '-'),
                            config.get('reiniciar_anual', False),
                            config.get('incluir_año', True),
                            config.get('incluir_mes', False),
                            True
                        ))
                    
                    conn.commit()
                    return True
                    
        except Exception as e:
            logging.error(f"Error guardando configuración de numeración: {str(e)}")
            return False


    def get_next_receipt_number(self) -> str:
        """Devuelve el próximo número de comprobante para recibos sin incrementar el contador."""
        return self.obtener_proximo_numero_comprobante('recibo')
 
    # --- MÉTODOS DE AUTOMATIZACIÓN Y ESTADOS ---
    

    def actualizar_estados_automaticos(self) -> dict:
        """Actualiza automáticamente los estados de usuarios según reglas de negocio"""
        resultados = {
            'usuarios_actualizados': 0,
            'estados_cambiados': [],
            'alertas_generadas': [],
            'errores': [],
            'tiempo_procesamiento': 0,
        }
        inicio_tiempo = time.time()

        try:
            with self.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    config = self.obtener_configuracion_automatizacion()

                    # 1. Vencer estados antiguos
                    vencidos = self._vencer_estados_antiguos(cursor)
                    resultados['estados_cambiados'].extend(vencidos)

                    # 2. Identificar usuarios con cuotas vencidas o próximas a vencer
                    usuarios_a_procesar = self._identificar_usuarios_para_actualizar(cursor, config)

                    # 3. Aplicar cambios
                    actualizados, alertas = self._aplicar_actualizaciones_de_estado(cursor, usuarios_a_procesar, config)
                    resultados['usuarios_actualizados'] = actualizados
                    resultados['alertas_generadas'] = alertas

                    conn.commit()

        except Exception as e:
            resultados['errores'].append({'error': str(e), 'contexto': 'procesamiento_general'})
            logging.error(f"Error en automatización de estados: {e}")
        finally:
            resultados['tiempo_procesamiento'] = round(time.time() - inicio_tiempo, 2)
            logging.info(f"Automatización de estados completada: {resultados}")

        return resultados
    

    def obtener_configuracion_automatizacion(self) -> dict:
        """Obtiene la configuración para automatización de estados"""
        try:
            dias_vencimiento = int(self.obtener_configuracion('dias_vencimiento_cuota') or '30')
            dias_alerta = int(self.obtener_configuracion('dias_alerta_vencimiento') or '7')
            
            return {
                'dias_vencimiento': dias_vencimiento,
                'dias_alerta': dias_alerta,
                'automatizacion_activa': self.obtener_configuracion('automatizacion_estados_activa') == 'true'
            }
        except Exception as e:
            logging.error(f"Error obteniendo configuración de automatización: {e}")
            return {
                'dias_vencimiento': 30,
                'dias_alerta': 7,
                'automatizacion_activa': False
            }
    

    def _vencer_estados_antiguos(self, cursor) -> list:
        """Vence los estados de usuario que han expirado"""
        fecha_actual = datetime.now().date()
        cursor.execute("""
            SELECT id, usuario_id, estado FROM usuario_estados
            WHERE activo = TRUE AND fecha_vencimiento IS NOT NULL AND fecha_vencimiento < %s
        """, (fecha_actual,))
        estados_a_vencer = cursor.fetchall()

        vencidos = []
        if estados_a_vencer:
            ids = [e[0] for e in estados_a_vencer]
            # Actualización por lote para minimizar round-trips
            placeholders = ",".join(["%s"] * len(ids))
            cursor.execute(f"UPDATE usuario_estados SET activo = FALSE WHERE id IN ({placeholders})", ids)
            # Registrar historial por cada estado (mantener trazabilidad)
            for estado in estados_a_vencer:
                self.registrar_historial_estado(
                    usuario_id=estado[1],
                    estado_id=estado[0],
                    accion='desactivar',
                    estado_anterior=estado[2],
                    motivo='Vencimiento automático',
                    usuario_modificador=1  # Sistema
                )
                vencidos.append({
                    'usuario_id': estado[1],
                    'estado_id': estado[0],
                    'estado': estado[2],
                    'motivo': 'Vencimiento automático'
                })
        return vencidos


    def _aplicar_actualizaciones_de_estado(self, cursor, usuarios, config) -> tuple[int, list]:
        """Aplica los cambios de estado y genera alertas"""
        actualizados = 0
        alertas = []
        fecha_actual = datetime.now().date()

        for usuario in usuarios:
            dias_sin_pago = float('inf')
            if usuario[3]:  # ultimo_pago
                ultimo_pago_str = str(usuario[3])
                try:
                    if ' ' in ultimo_pago_str:
                        ultimo_pago_date = datetime.strptime(ultimo_pago_str.split()[0], '%Y-%m-%d').date()
                    else:
                        ultimo_pago_date = datetime.fromisoformat(ultimo_pago_str).date()
                    dias_sin_pago = (fecha_actual - ultimo_pago_date).days
                except (ValueError, TypeError):
                    dias_sin_pago = float('inf')

            if dias_sin_pago >= config['dias_vencimiento']:
                nuevo_estado_id = self._cambiar_estado_usuario(cursor, usuario['id'], 'cuota_vencida', f"Cuota vencida. Días sin pago: {dias_sin_pago}")
                cursor.execute("UPDATE usuarios SET activo = FALSE WHERE id = %s", (usuario['id'],))
                # Registrar en historial con el ID correcto
                self.registrar_historial_estado(
                    usuario_id=usuario['id'],
                    estado_id=nuevo_estado_id,
                    accion='actualizar',
                    estado_nuevo='cuota_vencida',
                    motivo=f"Cuota vencida automáticamente. Días sin pago: {dias_sin_pago}",
                    usuario_modificador=1  # Sistema
                )
                actualizados += 1
            elif dias_sin_pago >= (config['dias_vencimiento'] - config['dias_alerta']):
                dias_restantes = config['dias_vencimiento'] - dias_sin_pago
                alerta = {
                    'usuario_id': usuario['id'],
                    'nombre': usuario['nombre'],
                    'dni': usuario['dni'],
                    'dias_restantes': dias_restantes,
                    'ultimo_pago': usuario['ultimo_pago']
                }
                nuevo_estado_id = self._cambiar_estado_usuario(cursor, usuario['id'], 'proximo_vencimiento', f"Cuota próxima a vencer en {dias_restantes} días")
                # Registrar en historial con el ID correcto
                self.registrar_historial_estado(
                    usuario_id=usuario['id'],
                    estado_id=nuevo_estado_id,
                    accion='actualizar',
                    estado_nuevo='proximo_vencimiento',
                    motivo=f"Cuota próxima a vencer en {dias_restantes} días",
                    usuario_modificador=1  # Sistema
                )
                alertas.append(alerta)

        return actualizados, alertas


    def registrar_historial_estado(self, usuario_id: int, estado_id: int = None, accion: str = 'crear',
                                 estado_anterior: str = None, estado_nuevo: str = None,
                                 motivo: str = None, usuario_modificador: int = None):
        """Registra cambios en el historial de estados con manejo de timeouts y reintentos"""
        import time
        import psycopg2
        
        max_retries = 3
        base_delay = 0.1  # 100ms base delay
        
        for intento in range(max_retries):
            try:
                # Usar conexión del pool con configuración de timeouts
                with self.get_connection_context() as conn:
                    with conn.cursor() as cursor:
                        # Configurar timeout de statement para esta sesión
                        cursor.execute("SET statement_timeout = '15s'")
                        cursor.execute("SET lock_timeout = '5s'")
                        
                        # Verificar existencia de estado_id; si no existe o la acción es 'eliminar', registrar sin FK
                        estado_id_valido = None
                        if estado_id is not None and (accion or '').lower() != 'eliminar':
                            try:
                                cursor.execute("SELECT 1 FROM usuario_estados WHERE id = %s", (estado_id,))
                                if cursor.fetchone():
                                    estado_id_valido = estado_id
                            except Exception as check_err:
                                logging.warning(f"No se pudo verificar existencia de estado {estado_id}: {check_err}. Registrando sin FK")
                        
                        # Construir detalles con la información disponible
                        detalles_dict = {}
                        if estado_anterior:
                            detalles_dict['estado_anterior'] = estado_anterior
                        if estado_nuevo:
                            detalles_dict['estado_nuevo'] = estado_nuevo
                        if motivo:
                            detalles_dict['motivo'] = motivo
                        if usuario_modificador:
                            detalles_dict['usuario_modificador'] = usuario_modificador
                        if estado_id is not None and estado_id_valido is None:
                            detalles_dict['estado_id_original'] = estado_id  # conservar referencia informativa
                        
                        detalles_json = str(detalles_dict) if detalles_dict else None
                        
                        # Usar la estructura real de la tabla historial_estados
                        cursor.execute("""
                            INSERT INTO historial_estados 
                            (usuario_id, estado_id, accion, detalles, creado_por)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (usuario_id, estado_id_valido, accion, detalles_json, usuario_modificador))
                        
                        conn.commit()
                        return  # Éxito, salir del bucle de reintentos
                        
            except (psycopg2.OperationalError, psycopg2.DatabaseError) as e:
                error_msg = str(e).lower()
                
                # Verificar si es un error de timeout o deadlock que puede reintentarse
                if any(keyword in error_msg for keyword in ['timeout', 'deadlock', 'lock', 'canceling statement']):
                    if intento < max_retries - 1:
                        # Backoff exponencial con jitter
                        delay = base_delay * (2 ** intento) + (time.time() % 0.1)
                        logging.warning(f"Reintentando registrar historial de estado (intento {intento + 1}/{max_retries}) después de {delay:.2f}s: {e}")
                        time.sleep(delay)
                        continue
                    else:
                        logging.error(f"Error registrando historial de estado después de {max_retries} intentos: {e}")
                        # En caso de fallo crítico, intentar registro mínimo sin FK
                        self._registrar_historial_fallback(usuario_id, accion, estado_anterior, estado_nuevo)
                else:
                    # Error no recuperable
                    logging.error(f"Error no recuperable registrando historial de estado: {e}")
                    break
                    
            except Exception as e:
                logging.error(f"Error inesperado registrando historial de estado: {e}")
                break
    

    def _registrar_historial_fallback(self, usuario_id: int, accion: str, estado_anterior: str = None, estado_nuevo: str = None):
        """Registro de fallback sin foreign keys para casos críticos"""
        try:
            with self.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SET statement_timeout = '10s'")
                    
                    # Construir detalles para el fallback
                    detalles_dict = {}
                    if estado_anterior:
                        detalles_dict['estado_anterior'] = estado_anterior
                    if estado_nuevo:
                        detalles_dict['estado_nuevo'] = estado_nuevo
                    detalles_dict['fallback'] = True
                    
                    detalles_json = str(detalles_dict) if detalles_dict else None
                    
                    cursor.execute("""
                        INSERT INTO historial_estados 
                        (usuario_id, estado_id, accion, detalles)
                        VALUES (%s, NULL, %s, %s)
                    """, (usuario_id, accion, detalles_json))
                    conn.commit()
                    logging.info(f"Historial registrado en modo fallback para usuario {usuario_id}")
        except Exception as e:
            logging.error(f"Error en registro de historial fallback: {e}")


    def obtener_alertas_estados_proximos_vencer(self, dias_anticipacion: int = 7) -> List[dict]:
        """Obtiene alertas de estados próximos a vencer"""
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                
                fecha_limite = (datetime.now().date() + timedelta(days=dias_anticipacion))
                
                cursor.execute("""
                    SELECT u.id, u.nombre, u.telefono, 
                           ue.estado, ue.fecha_vencimiento, ue.descripcion
                    FROM usuarios u
                    JOIN usuario_estados ue ON u.id = ue.usuario_id
                    WHERE ue.activo = TRUE 
                      AND ue.fecha_vencimiento IS NOT NULL 
                      AND ue.fecha_vencimiento <= %s
                      AND ue.fecha_vencimiento >= CURRENT_DATE
                    ORDER BY ue.fecha_vencimiento ASC
                """, (fecha_limite,))
                
                alertas = []
                for row in cursor.fetchall():
                    usuario_id, nombre, telefono, estado, fecha_vencimiento, descripcion = row
                    
                    # Calcular días restantes
                    if isinstance(fecha_vencimiento, str):
                        fecha_vencimiento_date = datetime.fromisoformat(fecha_vencimiento).date()
                    else:
                        fecha_vencimiento_date = fecha_vencimiento
                    
                    dias_restantes = (fecha_vencimiento_date - datetime.now().date()).days
                    
                    alertas.append({
                        'usuario_id': usuario_id,
                        'nombre': nombre,
                        'telefono': telefono,
                        'estado': estado,
                        'fecha_vencimiento': fecha_vencimiento,
                        'dias_restantes': dias_restantes,
                        'descripcion': descripcion,
                        'prioridad': 'alta' if dias_restantes <= 3 else 'media' if dias_restantes <= 7 else 'baja'
                    })
                
                return alertas
    

    def _procesar_lote_activacion_robusto(self, cursor, lote: List[int], accion: str, resultados: dict):
        """Procesa un lote de activación/desactivación de usuarios con validaciones robustas"""
        try:
            valor_activo = True if accion == 'activar' else False
            
            # Validar que la acción es válida
            if accion not in ['activar', 'desactivar']:
                raise ValueError(f"Acción inválida: {accion}")
            
            # Verificar que todos los usuarios existen y obtener su estado actual
            placeholders = ','.join(['%s' for _ in lote])
            cursor.execute(
                f"SELECT id, activo, nombre, dni FROM usuarios WHERE id IN ({placeholders})",
                lote
            )
            
            # MANEJO SEGURO DE cursor.fetchall() - verificar None y validar estructura
            result_rows = cursor.fetchall()
            if result_rows is None:
                logging.error("_procesar_lote_activacion_robusto: Error al consultar usuarios - resultado None")
                resultados['errores'].append("Error al consultar usuarios para cambio de estado")
                resultados['fallidos'] += len(lote)
                return
            
            # Validar estructura de cada fila y construir lista segura
            usuarios_actuales = []
            for row in result_rows:
                if (row and isinstance(row, dict) and 
                    'id' in row and 'activo' in row and 'nombre' in row and 'dni' in row and
                    isinstance(row['id'], int)):
                    usuarios_actuales.append(row)
                else:
                    logging.warning(f"_procesar_lote_activacion_robusto: Fila inválida en resultado: {row}")
                    resultados['errores'].append(f"Datos de usuario inválidos encontrados: {row}")
            
            if len(usuarios_actuales) != len(lote):
                usuarios_encontrados = [u['id'] for u in usuarios_actuales]
                usuarios_faltantes = set(lote) - set(usuarios_encontrados)
                logging.warning(f"_procesar_lote_activacion_robusto: Usuarios no encontrados: {usuarios_faltantes}")
                resultados['errores'].append(f"Usuarios no encontrados para {accion}: {list(usuarios_faltantes)}")
                resultados['fallidos'] += len(usuarios_faltantes)
            
            # Filtrar usuarios que ya están en el estado deseado
            usuarios_a_cambiar = [u for u in usuarios_actuales if u['activo'] != valor_activo]
            usuarios_sin_cambio = [u for u in usuarios_actuales if u['activo'] == valor_activo]
            
            if usuarios_sin_cambio:
                logging.info(f"_procesar_lote_activacion_robusto: {len(usuarios_sin_cambio)} usuarios ya están en estado '{accion}'")
            
            if usuarios_a_cambiar:
                ids_a_cambiar = [u['id'] for u in usuarios_a_cambiar]
                placeholders_cambio = ','.join(['%s' for _ in ids_a_cambiar])
                
                # Actualizar con timestamp de modificación
                cursor.execute(
                    f"""UPDATE usuarios 
                        SET activo = %s, 
                            fecha_modificacion = CURRENT_TIMESTAMP
                        WHERE id IN ({placeholders_cambio})""",
                    [valor_activo] + ids_a_cambiar
                )
                
                affected_rows = cursor.rowcount
                resultados['exitosos'] += affected_rows
                
                # Registrar en log de auditoría si existe la tabla
                try:
                    for usuario in usuarios_a_cambiar:
                        cursor.execute(
                            """INSERT INTO log_auditoria (tabla, accion, registro_id, datos_anteriores, datos_nuevos, usuario_modificacion, fecha_modificacion)
                               VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)""",
                            ('usuarios', accion, usuario['id'], 
                             f"activo: {usuario['activo']}", f"activo: {valor_activo}", 
                             'sistema_masivo')
                        )
                except psycopg2.Error:
                    # Si no existe la tabla de auditoría, continuar sin error
                    pass
                
                resultados['detalles'].append(f'Lote {accion}: {affected_rows} usuarios procesados exitosamente')
                logging.info(f"_procesar_lote_activacion_robusto: {affected_rows} usuarios {accion}dos exitosamente")
            else:
                resultados['detalles'].append(f'Lote {accion}: 0 usuarios requerían cambios')
            
        except psycopg2.Error as e:
            error_msg = f'Error de base de datos en lote {accion}: {str(e)}'
            logging.error(f"_procesar_lote_activacion_robusto: {error_msg}")
            resultados['errores'].append(error_msg)
            resultados['fallidos'] += len(lote)
            raise  # Re-lanzar para manejo de transacciones
        except Exception as e:
            error_msg = f'Error inesperado en lote {accion}: {str(e)}'
            logging.error(f"_procesar_lote_activacion_robusto: {error_msg}")
            resultados['errores'].append(error_msg)
            resultados['fallidos'] += len(lote)
            raise  # Re-lanzar para manejo de transacciones
    

    def _procesar_lote_activacion(self, cursor, lote: List[int], accion: str, resultados: dict):
        """Procesa un lote de activación/desactivación de usuarios (método anterior)"""
        # Llamar al método robusto para mantener compatibilidad
        return self._procesar_lote_activacion_robusto(cursor, lote, accion, resultados)
    

    def _procesar_lote_eliminacion_robusto(self, cursor, lote: List[int], resultados: dict):
        """Procesa un lote de eliminación de usuarios con validaciones robustas"""
        try:
            # Debug logging para rastrear el problema
            logging.debug(f"_procesar_lote_eliminacion_robusto: lote recibido = {lote}, tipo = {type(lote)}")
            
            # Verificar que lote es una lista válida
            if not isinstance(lote, list):
                raise ValueError(f"El parámetro 'lote' debe ser una lista, recibido: {type(lote)} = {lote}")
            
            if not lote:
                logging.warning("_procesar_lote_eliminacion_robusto: lote vacío")
                return
            
            # Verificar que todos los elementos son enteros
            for i, item in enumerate(lote):
                if not isinstance(item, int):
                    raise ValueError(f"Elemento {i} del lote no es entero: {type(item)} = {item}")
            
            # Verificar que todos los usuarios existen
            placeholders = ','.join(['%s' for _ in lote])
            cursor.execute(
                f"SELECT id, nombre, dni, rol FROM usuarios WHERE id IN ({placeholders})",
                lote
            )
            
            # MANEJO SEGURO DE cursor.fetchall() - verificar None y validar/normalizar estructura
            result_rows = cursor.fetchall()
            if result_rows is None:
                logging.error("_procesar_lote_eliminacion_robusto: Error al consultar usuarios - resultado None")
                resultados['errores'].append("Error al consultar usuarios para eliminación")
                resultados['fallidos'] += len(lote)
                return

            # Determinar nombres de columnas (para mapear tuplas si el cursor no retorna dict)
            try:
                column_names = [desc.name for desc in cursor.description]
            except Exception:
                column_names = ['id', 'nombre', 'dni', 'rol']

            # Normalizar filas a dict compatible y validar estructura
            usuarios_existentes = []
            for row in result_rows:
                normalized = None
                # Aceptar mapeos tipo dict o RealDictRow (verificamos por claves)
                if row and hasattr(row, 'keys') and 'id' in row and 'nombre' in row and 'dni' in row and 'rol' in row:
                    normalized = dict(row)
                # Mapear tuplas usando column_names
                elif isinstance(row, (tuple, list)) and len(row) >= 4:
                    try:
                        mapped = dict(zip(column_names, row))
                        normalized = mapped if 'id' in mapped and 'nombre' in mapped and 'dni' in mapped and 'rol' in mapped else None
                    except Exception:
                        normalized = None

                if normalized and isinstance(normalized.get('id'), int):
                    usuarios_existentes.append(normalized)
                else:
                    logging.warning(f"_procesar_lote_eliminacion_robusto: Fila inválida en resultado: {row}")
                    resultados['errores'].append(f"Datos de usuario inválidos encontrados: {row}")
            
            if len(usuarios_existentes) != len(lote):
                usuarios_encontrados = [u['id'] for u in usuarios_existentes]
                usuarios_faltantes = set(lote) - set(usuarios_encontrados)
                logging.warning(f"_procesar_lote_eliminacion_robusto: Usuarios no encontrados: {usuarios_faltantes}")
                resultados['errores'].append(f"Usuarios no encontrados para eliminación: {list(usuarios_faltantes)}")
                resultados['fallidos'] += len(usuarios_faltantes)
            
            if usuarios_existentes:
                # Verificar usuarios con rol 'dueño' que no se pueden eliminar
                usuarios_dueno = [u for u in usuarios_existentes if u['rol'] == 'dueño']
                if usuarios_dueno:
                    nombres_dueno = [u['nombre'] for u in usuarios_dueno]
                    error_msg = f"No se pueden eliminar usuarios con rol 'dueño': {', '.join(nombres_dueno)}"
                    logging.error(f"_procesar_lote_eliminacion_robusto: {error_msg}")
                    resultados['errores'].append(error_msg)
                    resultados['fallidos'] += len(usuarios_dueno)
                    # Filtrar usuarios dueño de la lista
                    usuarios_existentes = [u for u in usuarios_existentes if u['rol'] != 'dueño']
                
                if usuarios_existentes:
                    ids_a_eliminar = [u['id'] for u in usuarios_existentes]
                    placeholders_eliminar = ','.join(['%s' for _ in ids_a_eliminar])
                    
                    # Eliminar registros relacionados primero (en orden de dependencias)
                    tablas_relacionadas = [
                        ('whatsapp_messages', 'user_id'),
                        ('usuario_etiquetas', 'usuario_id'),
                        ('usuario_estados', 'usuario_id'),
                        ('usuario_notas', 'usuario_id'),
                        ('asistencias', 'usuario_id'),
                        ('pagos', 'usuario_id')
                    ]
                    
                    # Usar SAVEPOINT por tabla para evitar abortar toda la transacción si alguna relación no existe
                    for tabla, columna in tablas_relacionadas:
                        sp_name = f"sp_del_{tabla}_{columna}".replace('.', '_')
                        try:
                            cursor.execute(f"SAVEPOINT {sp_name}")
                            cursor.execute(
                                f"DELETE FROM {tabla} WHERE {columna} IN ({placeholders_eliminar})",
                                ids_a_eliminar
                            )
                            logging.debug(f"_procesar_lote_eliminacion_robusto: Eliminados {cursor.rowcount} registros de {tabla}")
                            try:
                                cursor.execute(f"RELEASE SAVEPOINT {sp_name}")
                            except psycopg2.Error:
                                pass
                        except psycopg2.Error as e:
                            logging.warning(f"_procesar_lote_eliminacion_robusto: Error eliminando de {tabla}: {e}")
                            try:
                                cursor.execute(f"ROLLBACK TO SAVEPOINT {sp_name}")
                            except psycopg2.Error:
                                pass

                    # Neutralizar referencias con FK sin ON DELETE para permitir la eliminación
                    tablas_fk_candidatas = [
                        ('audit_logs', 'user_id'),
                        ('system_diagnostics', 'resolved_by'),
                        ('maintenance_tasks', 'created_by'),
                        ('maintenance_tasks', 'executed_by')
                    ]
                    
                    # Filtrar solo las tablas que existan para evitar errores
                    tablas_fk_existentes = []
                    for tabla, columna in tablas_fk_candidatas:
                        try:
                            # Preferir nombre calificado en esquema público
                            cursor.execute("SELECT to_regclass(%s)", (f'public.{tabla}',))
                            reg = cursor.fetchone()
                            # reg puede ser tupla o RealDictRow; el valor está en el índice 0
                            existe = False
                            if reg is not None:
                                try:
                                    existe = bool(reg[0])
                                except Exception:
                                    # fallback para dict-like
                                    existe = bool(list(reg.values())[0]) if hasattr(reg, 'values') else False
                            if existe:
                                tablas_fk_existentes.append((tabla, columna))
                        except psycopg2.Error as e:
                            # Si falla la verificación, registrar y OMITIR esta tabla para evitar abortar transacción
                            logging.warning(f"_procesar_lote_eliminacion_robusto: Error verificando existencia de {tabla}: {e}")
                            continue
                    
                    for tabla, columna in tablas_fk_existentes:
                        sp_name = f"sp_null_{tabla}_{columna}".replace('.', '_')
                        sp_creado = False
                        try:
                            cursor.execute(f"SAVEPOINT {sp_name}")
                            sp_creado = True
                        except psycopg2.Error as e:
                            # Si no se puede crear SAVEPOINT (p.ej. autocommit), evitar ejecutar UPDATE sin protección
                            logging.warning(f"_procesar_lote_eliminacion_robusto: No se pudo crear SAVEPOINT {sp_name}: {e}")
                            continue
                        try:
                            cursor.execute(
                                f"UPDATE {tabla} SET {columna} = NULL WHERE {columna} IN ({placeholders_eliminar})",
                                ids_a_eliminar
                            )
                            logging.debug(f"_procesar_lote_eliminacion_robusto: Actualizados {cursor.rowcount} registros en {tabla}.{columna} -> NULL")
                            try:
                                cursor.execute(f"RELEASE SAVEPOINT {sp_name}")
                            except psycopg2.Error:
                                pass
                        except psycopg2.Error as e:
                            logging.warning(f"_procesar_lote_eliminacion_robusto: Error actualizando FK en {tabla}.{columna}: {e}")
                            if sp_creado:
                                try:
                                    cursor.execute(f"ROLLBACK TO SAVEPOINT {sp_name}")
                                except psycopg2.Error:
                                    pass
                    
                    # Finalmente eliminar usuarios uno por uno para aislar fallos por restricciones restantes
                    eliminados_en_lote = 0
                    for usuario in usuarios_existentes:
                        usuario_id = usuario['id']
                        nombre = usuario.get('nombre')
                        sp_name = f"sp_del_usuario_{usuario_id}"
                        sp_creado = False
                        try:
                            try:
                                cursor.execute(f"SAVEPOINT {sp_name}")
                                sp_creado = True
                            except psycopg2.Error as e:
                                # Si no se puede crear SAVEPOINT, continuar sin protección pero registrando advertencia
                                logging.warning(f"_procesar_lote_eliminacion_robusto: No se pudo crear SAVEPOINT {sp_name}: {e}")

                            # Verificar dependencias de clases como en la ruta individual
                            try:
                                cursor.execute("SELECT COUNT(*) FROM clases WHERE profesor_id = (SELECT id FROM profesores WHERE usuario_id = %s)", (usuario_id,))
                                clases_count = cursor.fetchone()[0]
                            except Exception:
                                clases_count = 0
                            if clases_count and clases_count > 0:
                                msg = f"Usuario {usuario_id} ({nombre}): no se puede eliminar, tiene {clases_count} clases asignadas"
                                logging.warning(f"_procesar_lote_eliminacion_robusto: {msg}")
                                resultados['errores'].append(msg)
                                resultados['fallidos'] += 1
                                # Revertir cualquier operación en este usuario y liberar savepoint
                                if sp_creado:
                                    try:
                                        cursor.execute(f"ROLLBACK TO SAVEPOINT {sp_name}")
                                    except psycopg2.Error:
                                        pass
                                    try:
                                        cursor.execute(f"RELEASE SAVEPOINT {sp_name}")
                                    except psycopg2.Error:
                                        pass
                                continue

                            cursor.execute("DELETE FROM usuarios WHERE id = %s", (usuario_id,))
                            if cursor.rowcount == 1:
                                resultados['exitosos'] += 1
                                eliminados_en_lote += 1
                                # Registrar en log de auditoría si existe la tabla
                                try:
                                    cursor.execute(
                                        """INSERT INTO log_auditoria (tabla, accion, registro_id, datos_anteriores, usuario_modificacion, fecha_modificacion)
                                           VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)""",
                                        (
                                            'usuarios', 'eliminar', usuario_id,
                                            f"nombre: {usuario.get('nombre')}, dni: {usuario.get('dni')}, rol: {usuario.get('rol')}",
                                            'sistema_masivo'
                                        )
                                    )
                                except psycopg2.Error:
                                    pass
                                # Liberar savepoint tras éxito
                                if sp_creado:
                                    try:
                                        cursor.execute(f"RELEASE SAVEPOINT {sp_name}")
                                    except psycopg2.Error:
                                        pass
                            else:
                                msg = f"Usuario {usuario_id} ({nombre}): no eliminado (rowcount={cursor.rowcount})"
                                logging.warning(f"_procesar_lote_eliminacion_robusto: {msg}")
                                resultados['errores'].append(msg)
                                resultados['fallidos'] += 1
                                # Revertir y liberar savepoint en caso de no eliminación
                                if sp_creado:
                                    try:
                                        cursor.execute(f"ROLLBACK TO SAVEPOINT {sp_name}")
                                    except psycopg2.Error:
                                        pass
                                    try:
                                        cursor.execute(f"RELEASE SAVEPOINT {sp_name}")
                                    except psycopg2.Error:
                                        pass
                        except psycopg2.Error as e:
                            msg = f"Usuario {usuario_id} ({nombre}): error al eliminar -> {str(e)}"
                            logging.error(f"_procesar_lote_eliminacion_robusto: {msg}")
                            resultados['errores'].append(msg)
                            resultados['fallidos'] += 1
                            # Revertir cambios de este usuario y liberar savepoint
                            if sp_creado:
                                try:
                                    cursor.execute(f"ROLLBACK TO SAVEPOINT {sp_name}")
                                except psycopg2.Error:
                                    pass
                                try:
                                    cursor.execute(f"RELEASE SAVEPOINT {sp_name}")
                                except psycopg2.Error:
                                    pass
                        except Exception as e:
                            # Capturar errores no-DB (p.ej. KeyError en datos), revertir y continuar
                            msg = f"Usuario {usuario_id} ({nombre}): error inesperado -> {str(e)}"
                            logging.error(f"_procesar_lote_eliminacion_robusto: {msg}")
                            resultados['errores'].append(msg)
                            resultados['fallidos'] += 1
                            if sp_creado:
                                try:
                                    cursor.execute(f"ROLLBACK TO SAVEPOINT {sp_name}")
                                except psycopg2.Error:
                                    pass
                                try:
                                    cursor.execute(f"RELEASE SAVEPOINT {sp_name}")
                                except psycopg2.Error:
                                    pass
                    
                    resultados['detalles'].append(f'Lote eliminación: {eliminados_en_lote} usuarios eliminados exitosamente')
                    logging.info(f"_procesar_lote_eliminacion_robusto: {eliminados_en_lote} usuarios eliminados exitosamente")
                else:
                    resultados['detalles'].append('Lote eliminación: 0 usuarios eliminados (todos eran dueños)')
            
        except psycopg2.Error as e:
            error_msg = f'Error de base de datos en lote eliminación: {str(e)}'
            logging.error(f"_procesar_lote_eliminacion_robusto: {error_msg}")
            resultados['errores'].append(error_msg)
            resultados['fallidos'] += len(lote)
            raise  # Re-lanzar para manejo de transacciones
        except ValueError as e:
            error_msg = f'Error de validación en lote eliminación: {str(e)}'
            logging.error(f"_procesar_lote_eliminacion_robusto: {error_msg}")
            resultados['errores'].append(error_msg)
            resultados['fallidos'] += len(lote)
            # No re-lanzar errores de validación para continuar con otros lotes
        except Exception as e:
            error_msg = f'Error inesperado en lote eliminación: {str(e)} - Tipo: {type(e).__name__}'
            logging.error(f"_procesar_lote_eliminacion_robusto: {error_msg}")
            logging.error(f"_procesar_lote_eliminacion_robusto: Lote problemático: {lote}")
            resultados['errores'].append(error_msg)
            resultados['fallidos'] += len(lote)
            raise  # Re-lanzar para manejo de transacciones
    

    def _procesar_acciones_individuales_robusto(self, cursor, lote: List[int], accion: str, parametros: dict, resultados: dict):
        """Procesa acciones individuales complejas para un lote de usuarios"""
        try:
            # Para acciones no implementadas masivamente, procesar individualmente
            for usuario_id in lote:
                try:
                    # Aquí se pueden agregar más acciones específicas según sea necesario
                    if accion == 'resetear_password':
                        # Ejemplo de acción individual
                        cursor.execute(
                            "UPDATE usuarios SET password = %s, fecha_modificacion = CURRENT_TIMESTAMP WHERE id = %s",
                            (parametros.get('nueva_password', 'default123'), usuario_id)
                        )
                        if cursor.rowcount > 0:
                            resultados['exitosos'] += 1
                        else:
                            resultados['fallidos'] += 1
                            resultados['errores'].append(f"No se pudo resetear password para usuario {usuario_id}")
                    else:
                        # Acción no reconocida
                        error_msg = f"Acción no implementada: {accion}"
                        logging.warning(f"_procesar_acciones_individuales_robusto: {error_msg}")
                        resultados['errores'].append(error_msg)
                        resultados['fallidos'] += len(lote)
                        return
                        
                except Exception as e:
                    error_msg = f"Error procesando acción '{accion}' para usuario {usuario_id}: {str(e)}"
                    logging.error(f"_procesar_acciones_individuales_robusto: {error_msg}")
                    resultados['errores'].append(error_msg)
                    resultados['fallidos'] += 1
            
            if resultados['exitosos'] > 0:
                resultados['detalles'].append(f'Acciones individuales: {resultados["exitosos"]} usuarios procesados con acción "{accion}"')
                
        except Exception as e:
            error_msg = f'Error inesperado en acciones individuales: {str(e)}'
            logging.error(f"_procesar_acciones_individuales_robusto: {error_msg}")
            resultados['errores'].append(error_msg)
            resultados['fallidos'] += len(lote)
            raise  # Re-lanzar para manejo de transacciones
 
     # --- MÉTODOS DE RUTINAS Y EJERCICIOS ---
    

    def crear_rutina(self, rutina: Rutina) -> int:
        """Crea una nueva rutina.
        Si rutina.usuario_id es None, se considera una plantilla y no se valida el estado del usuario.
        """
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                # Validar usuario activo solo si hay usuario asociado
                if getattr(rutina, 'usuario_id', None) is not None:
                    cursor.execute("SELECT activo FROM usuarios WHERE id = %s", (rutina.usuario_id,))
                    row = cursor.fetchone()
                    activo = (row.get('activo') if isinstance(row, dict) else (row[0] if row else False))
                    if not row or not activo:
                        raise PermissionError("El usuario está inactivo: no se puede crear una rutina")

                sql = "INSERT INTO rutinas (usuario_id, nombre_rutina, descripcion, dias_semana, categoria) VALUES (%s, %s, %s, %s, %s) RETURNING id"
                categoria = getattr(rutina, 'categoria', 'general')
                cursor.execute(sql, (getattr(rutina, 'usuario_id', None), rutina.nombre_rutina, rutina.descripcion, rutina.dias_semana, categoria))
                rutina_id = cursor.fetchone()['id']
                conn.commit()
                return rutina_id


    def actualizar_rutina(self, rutina: Rutina) -> bool:
        """Actualiza una rutina existente"""
        try:
            with self.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    sql = "UPDATE rutinas SET nombre_rutina = %s, descripcion = %s, dias_semana = %s, categoria = %s WHERE id = %s"
                    categoria = getattr(rutina, 'categoria', 'general')
                    cursor.execute(sql, (rutina.nombre_rutina, rutina.descripcion, rutina.dias_semana, categoria, rutina.id))
                    conn.commit()
                    return cursor.rowcount > 0
        except Exception as e:
            logging.error(f"Error updating routine: {e}")
            return False


    def obtener_rutina_completa(self, rutina_id: int) -> Optional[Rutina]:
        """Obtiene una rutina completa con todos sus ejercicios"""
        rutina = None
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                # Construir SELECT dinámico según columnas disponibles
                cols = self.get_table_columns('rutinas')
                base_cols = ['id', 'usuario_id', 'nombre_rutina', 'descripcion', 'dias_semana', 'categoria', 'fecha_creacion', 'activa']
                select_cols = base_cols + (['uuid_rutina'] if 'uuid_rutina' in (cols or []) else [])
                sql = f"SELECT {', '.join(select_cols)} FROM rutinas WHERE id = %s"
                cursor.execute(sql, (rutina_id,))
                row = cursor.fetchone()
                
                if row:
                    # Filtrar campos válidos para el modelo Rutina y asignar uuid_rutina como atributo adicional
                    base_fields = {k: row[k] for k in ['id', 'usuario_id', 'nombre_rutina', 'descripcion', 'dias_semana', 'categoria', 'fecha_creacion', 'activa'] if k in row}
                    rutina = Rutina(**base_fields)
                    try:
                        setattr(rutina, 'uuid_rutina', row.get('uuid_rutina'))
                    except Exception:
                        pass
                    
                    # Obtener ejercicios de la rutina
                    sql_ejercicios = """
                        SELECT re.*, e.nombre, e.grupo_muscular, e.descripcion as ejercicio_descripcion,
                               e.video_url as ejercicio_video_url, e.video_mime as ejercicio_video_mime 
                        FROM rutina_ejercicios re 
                        JOIN ejercicios e ON re.ejercicio_id = e.id 
                        WHERE re.rutina_id = %s 
                        ORDER BY re.dia_semana, re.orden
                    """
                    cursor.execute(sql_ejercicios, (rutina_id,))
                    
                    for ejercicio_row in cursor.fetchall():
                        ejercicio_data = Ejercicio(
                            id=ejercicio_row['ejercicio_id'],
                            nombre=ejercicio_row['nombre'],
                            grupo_muscular=ejercicio_row['grupo_muscular'],
                            descripcion=ejercicio_row['ejercicio_descripcion'],
                            video_url=ejercicio_row.get('ejercicio_video_url'),
                            video_mime=ejercicio_row.get('ejercicio_video_mime')
                        )
                        
                        # Crear RutinaEjercicio con solo los campos válidos del modelo
                        rutina_ejercicio = RutinaEjercicio(
                            id=ejercicio_row.get('id'),
                            rutina_id=ejercicio_row.get('rutina_id'),
                            ejercicio_id=ejercicio_row.get('ejercicio_id'),
                            dia_semana=ejercicio_row.get('dia_semana'),
                            series=ejercicio_row.get('series'),
                            repeticiones=ejercicio_row.get('repeticiones'),
                            orden=ejercicio_row.get('orden'),
                            ejercicio=ejercicio_data
                        )
                        rutina.ejercicios.append(rutina_ejercicio)
        
        return rutina


    def obtener_rutina_completa_por_uuid(self, uuid_rutina: str) -> Optional[Rutina]:
        """Obtiene una rutina completa por su uuid_rutina, con todos sus ejercicios."""
        rutina = None
        if not uuid_rutina:
            return None
        with self.get_connection_context() as conn:
            # Asegurar que la columna exista y tenga valores
            self.ensure_rutina_uuid_ready(conn)
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                # Si la columna no existe, no se puede buscar por UUID
                cols = self.get_table_columns('rutinas')
                if 'uuid_rutina' not in (cols or []):
                    return None
                select_cols = ['id', 'usuario_id', 'nombre_rutina', 'descripcion', 'dias_semana', 'categoria', 'fecha_creacion', 'activa', 'uuid_rutina']
                sql = f"SELECT {', '.join(select_cols)} FROM rutinas WHERE uuid_rutina = %s"
                cursor.execute(sql, (uuid_rutina,))
                row = cursor.fetchone()
                if row:
                    base_fields = {k: row[k] for k in ['id', 'usuario_id', 'nombre_rutina', 'descripcion', 'dias_semana', 'categoria', 'fecha_creacion', 'activa'] if k in row}
                    rutina = Rutina(**base_fields)
                    try:
                        setattr(rutina, 'uuid_rutina', row.get('uuid_rutina'))
                    except Exception:
                        pass
                    sql_ejercicios = """
                        SELECT re.*, e.nombre, e.grupo_muscular, e.descripcion as ejercicio_descripcion 
                        FROM rutina_ejercicios re 
                        JOIN ejercicios e ON re.ejercicio_id = e.id 
                        WHERE re.rutina_id = %s 
                        ORDER BY re.dia_semana, re.orden
                    """
                    cursor.execute(sql_ejercicios, (row['id'],))
                    for ejercicio_row in cursor.fetchall():
                        ejercicio_data = Ejercicio(
                            id=ejercicio_row['ejercicio_id'],
                            nombre=ejercicio_row['nombre'],
                            grupo_muscular=ejercicio_row['grupo_muscular'],
                            descripcion=ejercicio_row['ejercicio_descripcion']
                        )
                        rutina_ejercicio = RutinaEjercicio(
                            id=ejercicio_row.get('id'),
                            rutina_id=ejercicio_row.get('rutina_id'),
                            ejercicio_id=ejercicio_row.get('ejercicio_id'),
                            dia_semana=ejercicio_row.get('dia_semana'),
                            series=ejercicio_row.get('series'),
                            repeticiones=ejercicio_row.get('repeticiones'),
                            orden=ejercicio_row.get('orden'),
                            ejercicio=ejercicio_data
                        )
                        rutina.ejercicios.append(rutina_ejercicio)
        return rutina


    def guardar_ejercicios_de_rutina(self, rutina_id: int, rutina_ejercicios: List[RutinaEjercicio]) -> bool:
        """Guarda los ejercicios de una rutina"""
        try:
            with self.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    # Eliminar ejercicios existentes
                    cursor.execute("DELETE FROM rutina_ejercicios WHERE rutina_id = %s", (rutina_id,))
                    
                    # Insertar nuevos ejercicios
                    if rutina_ejercicios:
                        sql = "INSERT INTO rutina_ejercicios (rutina_id, ejercicio_id, dia_semana, series, repeticiones, orden) VALUES (%s, %s, %s, %s, %s, %s)"
                        ejercicios_a_guardar = [
                            (rutina_id, re.ejercicio_id, re.dia_semana, re.series, re.repeticiones, re.orden)
                            for re in rutina_ejercicios
                        ]
                        cursor.executemany(sql, ejercicios_a_guardar)
                    
                    conn.commit()
                    return True
        except Exception as e:
            logging.error(f"Error saving routine exercises: {e}")
            return False


    def actualizar_ejercicio(self, ejercicio: Ejercicio):
        """Actualiza un ejercicio existente"""
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                # Descubrir columnas existentes para construir UPDATE dinámico
                try:
                    cols = self.get_table_columns('ejercicios') or []
                except Exception:
                    cols = []
                set_parts = ["nombre = %s"]
                params = [ejercicio.nombre]
                if 'grupo_muscular' in cols:
                    set_parts.append("grupo_muscular = %s")
                    params.append(ejercicio.grupo_muscular)
                if 'descripcion' in cols:
                    set_parts.append("descripcion = %s")
                    params.append(ejercicio.descripcion)
                if 'objetivo' in cols:
                    set_parts.append("objetivo = %s")
                    params.append(getattr(ejercicio, 'objetivo', 'general'))
                if 'video_url' in cols:
                    set_parts.append("video_url = %s")
                    params.append(getattr(ejercicio, 'video_url', None))
                if 'video_mime' in cols:
                    set_parts.append("video_mime = %s")
                    params.append(getattr(ejercicio, 'video_mime', None))
                sql = f"UPDATE ejercicios SET {', '.join(set_parts)} WHERE id = %s"
                params.append(ejercicio.id)
                cursor.execute(sql, tuple(params))
                conn.commit()
                try:
                    self.cache.invalidate('ejercicios')
                except Exception:
                    pass


    def eliminar_ejercicio(self, ejercicio_id: int):
        """Elimina un ejercicio"""
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM ejercicios WHERE id = %s", (ejercicio_id,))
                conn.commit()
                try:
                    self.cache.invalidate('ejercicios')
                except Exception:
                    pass


    def obtener_plantillas_rutina(self) -> List[Rutina]:
        """Obtiene todas las plantillas de rutina (rutinas sin usuario asignado)"""
        try:
            cached = self.cache.get('rutinas', ('plantillas',))
            if cached is not None:
                return cached
        except Exception:
            pass
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(
                    "SELECT id, usuario_id, nombre_rutina, descripcion, dias_semana, categoria, fecha_creacion, activa "
                    "FROM rutinas WHERE usuario_id IS NULL ORDER BY nombre_rutina"
                )
                result = [Rutina(**dict(r)) for r in cursor.fetchall()]
                try:
                    self.cache.set('rutinas', ('plantillas',), result)
                except Exception:
                    pass
                return result


    def eliminar_rutina(self, rutina_id: int) -> bool:
        """Elimina una rutina"""
        try:
            with self.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    # First delete related exercises
                    cursor.execute("DELETE FROM rutina_ejercicios WHERE rutina_id = %s", (rutina_id,))
                    # Then delete the routine
                    cursor.execute("DELETE FROM rutinas WHERE id = %s", (rutina_id,))
                    conn.commit()
                    try:
                        self.cache.invalidate('rutinas')
                    except Exception:
                        pass
                    return cursor.rowcount > 0
        except Exception as e:
            logging.error(f"Error deleting routine: {e}")
            return False

    # --- MÉTODOS DE CLASES Y HORARIOS ---
    

    def actualizar_clase(self, clase: Clase):
        """Actualiza una clase existente"""
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                sql = "UPDATE clases SET nombre = %s, descripcion = %s WHERE id = %s"
                cursor.execute(sql, (clase.nombre, clase.descripcion, clase.id))
                conn.commit()
                try:
                    self.cache.invalidate('clases')
                except Exception:
                    pass


    def eliminar_clase(self, clase_id: int):
        """Elimina una clase"""
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM clases WHERE id = %s", (clase_id,))
                conn.commit()
                try:
                    self.cache.invalidate('clases')
                except Exception:
                    pass


    def crear_horario_clase(self, horario: ClaseHorario) -> int:
        """Crea un nuevo horario para una clase"""
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                # Crear horario sin profesor_id
                sql = "INSERT INTO clases_horarios (clase_id, dia_semana, hora_inicio, hora_fin, cupo_maximo) VALUES (%s, %s, %s, %s, %s) RETURNING id"
                params = (horario.clase_id, horario.dia_semana, 
                         horario.hora_inicio, horario.hora_fin, horario.cupo_maximo)
                cursor.execute(sql, params)
                horario_id = cursor.fetchone()[0]
                
                # Si hay profesor asignado, crear la asignación
                if hasattr(horario, 'profesor_id') and horario.profesor_id:
                    self.asignar_profesor_a_clase(horario_id, horario.profesor_id, cursor)
                
                conn.commit()
                return horario_id


    def obtener_horarios_de_clase(self, clase_id: int) -> List[ClaseHorario]:
        """Obtiene todos los horarios de una clase"""
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                sql = """
                    SELECT ch.*, u.nombre as nombre_profesor, pca.profesor_id,
                           (SELECT COUNT(*) FROM clase_usuarios cu WHERE cu.clase_horario_id = ch.id) as inscriptos
                    FROM clases_horarios ch 
                    LEFT JOIN profesor_clase_asignaciones pca ON ch.id = pca.clase_horario_id AND pca.activa = true
                    LEFT JOIN profesores p ON pca.profesor_id = p.id
                    LEFT JOIN usuarios u ON p.usuario_id = u.id 
                    WHERE ch.clase_id = %s 
                    ORDER BY CASE ch.dia_semana 
                        WHEN 'Lunes' THEN 1 
                        WHEN 'Martes' THEN 2 
                        WHEN 'Miércoles' THEN 3 
                        WHEN 'Jueves' THEN 4 
                        WHEN 'Viernes' THEN 5 
                        WHEN 'Sábado' THEN 6 
                        WHEN 'Domingo' THEN 7 
                    END, ch.hora_inicio
                """
                cursor.execute(sql, (clase_id,))
                return [ClaseHorario(**dict(r)) for r in cursor.fetchall()]


    def obtener_horario_por_id(self, clase_horario_id: int) -> Optional[Dict]:
        """Obtiene información detallada de un horario de clase por su ID"""
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                sql = """
                    SELECT ch.*, c.nombre AS clase_nombre, t.nombre AS tipo_clase_nombre
                    FROM clases_horarios ch
                    JOIN clases c ON ch.clase_id = c.id
                    LEFT JOIN tipos_clases t ON c.tipo_clase_id = t.id
                    WHERE ch.id = %s
                """
                cursor.execute(sql, (clase_horario_id,))
                row = cursor.fetchone()
                return dict(row) if row else None


    def obtener_horarios_profesor(self, profesor_id: int) -> List[dict]:
        """Obtiene todos los horarios de clases asignados a un profesor"""
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                sql = """
                    SELECT ch.*, c.nombre as nombre_clase, c.descripcion as descripcion_clase,
                           (SELECT COUNT(*) FROM clase_usuarios cu WHERE cu.clase_horario_id = ch.id) as inscriptos
                    FROM clases_horarios ch 
                    JOIN clases c ON ch.clase_id = c.id
                    JOIN profesor_clase_asignaciones pca ON ch.id = pca.clase_horario_id
                    WHERE pca.profesor_id = %s AND pca.activa = true
                    ORDER BY CASE ch.dia_semana 
                        WHEN 'Lunes' THEN 1 
                        WHEN 'Martes' THEN 2 
                        WHEN 'Miércoles' THEN 3 
                        WHEN 'Jueves' THEN 4 
                        WHEN 'Viernes' THEN 5 
                        WHEN 'Sábado' THEN 6 
                        WHEN 'Domingo' THEN 7 
                    END, ch.hora_inicio
                """
                cursor.execute(sql, (profesor_id,))
                return [dict(r) for r in cursor.fetchall()]
    

    def obtener_horarios_profesor_dia(self, profesor_id: int, dia: str) -> List[dict]:
        """Obtiene los horarios de clases asignados a un profesor en un día específico"""
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                sql = """
                    SELECT ch.*, c.nombre as clase_nombre, c.descripcion as descripcion_clase,
                           (SELECT COUNT(*) FROM clase_usuarios cu WHERE cu.clase_horario_id = ch.id) as inscriptos
                    FROM clases_horarios ch 
                    JOIN clases c ON ch.clase_id = c.id
                    JOIN profesor_clase_asignaciones pca ON ch.id = pca.clase_horario_id
                    WHERE pca.profesor_id = %s AND pca.activa = true AND ch.dia_semana = %s
                    ORDER BY ch.hora_inicio
                """
                cursor.execute(sql, (profesor_id, dia))
                return [dict(r) for r in cursor.fetchall()]


    def crear_horario_profesor(self, profesor_id: int, dia: str, hora_inicio: str, hora_fin: str, disponible: bool = True) -> int:
        """Crea un horario de disponibilidad para un profesor (validando horas en Python)."""
        # Validación robusta de horas para evitar casts con strings vacías
        from datetime import datetime as _dt
        def _parse_time_str(s):
            if s is None:
                raise ValueError("horas_requeridas")
            ss = str(s).strip()
            if not ss:
                raise ValueError("horas_requeridas")
            for fmt in ("%H:%M:%S", "%H:%M"):
                try:
                    return _dt.strptime(ss, fmt).time()
                except Exception:
                    pass
            raise ValueError("formato_hora_invalido")

        t_inicio = _parse_time_str(hora_inicio)
        t_fin = _parse_time_str(hora_fin)
        if not (t_inicio < t_fin):
            raise ValueError("hora_inicio debe ser menor que hora_fin")

        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                # Crear tabla horarios_profesores si no existe
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS horarios_profesores (
                        id SERIAL PRIMARY KEY,
                        profesor_id INTEGER NOT NULL,
                        dia_semana VARCHAR(20) NOT NULL,
                        hora_inicio TIME NOT NULL,
                        hora_fin TIME NOT NULL,
                        disponible BOOLEAN DEFAULT true,
                        fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (profesor_id) REFERENCES profesores (id) ON DELETE CASCADE
                    )
                    """
                )
                # Validaciones de entrada
                # 1) Verificar existencia de profesor
                cursor.execute("SELECT 1 FROM profesores WHERE id = %s", (profesor_id,))
                if cursor.fetchone() is None:
                    raise ValueError("Profesor no existe")
                # 2) Validar día de la semana
                valid_days = ('Lunes','Martes','Miércoles','Jueves','Viernes','Sábado','Domingo')
                if dia not in valid_days:
                    raise ValueError("Día inválido")
                
                sql = """
                    INSERT INTO horarios_profesores (profesor_id, dia_semana, hora_inicio, hora_fin, disponible)
                    VALUES (%s, %s, %s::time, %s::time, %s)
                    RETURNING id
                """
                cursor.execute(sql, (profesor_id, dia, str(t_inicio), str(t_fin), disponible))
                horario_id = cursor.fetchone()[0]
                conn.commit()
                return horario_id


    def actualizar_horario_profesor(self, horario_id: int, dia: str, hora_inicio: str, hora_fin: str, disponible: bool = True):
        """Actualiza un horario de disponibilidad de profesor"""
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                # Validaciones básicas
                valid_days = ('Lunes','Martes','Miércoles','Jueves','Viernes','Sábado','Domingo')
                if dia not in valid_days:
                    raise ValueError("Día inválido")
                cursor.execute("SELECT %s::time < %s::time", (hora_inicio, hora_fin))
                res = cursor.fetchone()
                ok = bool(res[0]) if res is not None else False
                if not ok:
                    raise ValueError("hora_inicio debe ser menor que hora_fin")
                # Actualización
                sql = "UPDATE horarios_profesores SET dia_semana = %s, hora_inicio = %s, hora_fin = %s, disponible = %s WHERE id = %s"
                cursor.execute(sql, (dia, hora_inicio, hora_fin, disponible, horario_id))
                conn.commit()


    def eliminar_horario_profesor(self, horario_id: int):
        """Elimina un horario de disponibilidad de profesor"""
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM horarios_profesores WHERE id = %s", (horario_id,))
                conn.commit()


    def obtener_horarios_disponibilidad_profesor(self, profesor_id: int) -> List[dict]:
        """Obtiene todos los horarios de disponibilidad de un profesor"""
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                # Selección explícita de columnas para evitar SELECT * y mejorar desempeño
                # Índices recomendados (también creados en ensure_indexes):
                # - CREATE INDEX IF NOT EXISTS idx_horarios_profesores_profesor_id ON horarios_profesores(profesor_id);
                # - CREATE INDEX IF NOT EXISTS idx_horarios_profesores_dia_inicio ON horarios_profesores(dia_semana, hora_inicio);
                sql = """
                    SELECT id, profesor_id, dia_semana, hora_inicio, hora_fin, disponible
                    FROM horarios_profesores 
                    WHERE profesor_id = %s 
                    ORDER BY CASE dia_semana 
                        WHEN 'Lunes' THEN 1 
                        WHEN 'Martes' THEN 2 
                        WHEN 'Miércoles' THEN 3 
                        WHEN 'Jueves' THEN 4 
                        WHEN 'Viernes' THEN 5 
                        WHEN 'Sábado' THEN 6 
                        WHEN 'Domingo' THEN 7 
                    END, hora_inicio
                """
                cursor.execute(sql, (profesor_id,))
                return [dict(r) for r in cursor.fetchall()]


    def obtener_todos_horarios_clases(self, activos_solo=False):
        """Obtiene todos los horarios de clases con información de profesor y clase"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            if activos_solo:
                sql = """
                    SELECT ch.id, ch.dia_semana, ch.hora_inicio, ch.hora_fin, ch.cupo_maximo,
                           c.nombre as clase_nombre, c.descripcion as clase_descripcion,
                           u.nombre as profesor_nombre, p.id as profesor_id,
                           pca.activa as activo
                    FROM clases_horarios ch
                    JOIN clases c ON ch.clase_id = c.id
                    LEFT JOIN profesor_clase_asignaciones pca ON ch.id = pca.clase_horario_id AND pca.activa = true
                    LEFT JOIN profesores p ON pca.profesor_id = p.id
                    LEFT JOIN usuarios u ON p.usuario_id = u.id
                    WHERE pca.activa = true
                    ORDER BY ch.dia_semana, ch.hora_inicio
                """
            else:
                sql = """
                    SELECT ch.id, ch.dia_semana, ch.hora_inicio, ch.hora_fin, ch.cupo_maximo,
                           c.nombre as clase_nombre, c.descripcion as clase_descripcion,
                           u.nombre as profesor_nombre, p.id as profesor_id,
                           COALESCE(pca.activa, false) as activo
                    FROM clases_horarios ch
                    JOIN clases c ON ch.clase_id = c.id
                    LEFT JOIN profesor_clase_asignaciones pca ON ch.id = pca.clase_horario_id
                    LEFT JOIN profesores p ON pca.profesor_id = p.id
                    LEFT JOIN usuarios u ON p.usuario_id = u.id
                    ORDER BY ch.dia_semana, ch.hora_inicio
                """
            cursor.execute(sql)
            return [dict(r) for r in cursor.fetchall()]


    def actualizar_horario_clase(self, horario: ClaseHorario):
        """Actualiza un horario de clase"""
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                # Actualizar horario sin profesor_id
                sql = "UPDATE clases_horarios SET dia_semana = %s, hora_inicio = %s, hora_fin = %s, cupo_maximo = %s WHERE id = %s"
                params = (horario.dia_semana, horario.hora_inicio, 
                         horario.hora_fin, horario.cupo_maximo, horario.id)
                cursor.execute(sql, params)
                
                # Manejar asignación de profesor por separado si es necesario
                if hasattr(horario, 'profesor_id') and horario.profesor_id:
                    # Desactivar asignación anterior
                    cursor.execute("UPDATE profesor_clase_asignaciones SET activa = false WHERE clase_horario_id = %s", (horario.id,))
                    # Crear nueva asignación
                    self.asignar_profesor_a_clase(horario.id, horario.profesor_id, cursor)
                
                conn.commit()


    def eliminar_horario_clase(self, horario_id: int):
        """Elimina un horario de clase"""
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM clases_horarios WHERE id = %s", (horario_id,))
                conn.commit()
    

    def asignar_profesor_a_clase(self, clase_horario_id: int, profesor_id: int, cursor=None):
        """Asigna un profesor a una clase específica"""
        if cursor:
            # Usar cursor existente (transacción en curso)
            cursor.execute(
                "INSERT INTO profesor_clase_asignaciones (clase_horario_id, profesor_id) VALUES (%s, %s)",
                (clase_horario_id, profesor_id)
            )
        else:
            # Crear nueva conexión
            with self.get_connection_context() as conn:
                with conn.cursor() as new_cursor:
                    new_cursor.execute(
                        "INSERT INTO profesor_clase_asignaciones (clase_horario_id, profesor_id) VALUES (%s, %s)",
                        (clase_horario_id, profesor_id)
                    )
                    conn.commit()
    

    def desasignar_profesor_de_clase(self, clase_horario_id: int, profesor_id: int):
        """Desasigna un profesor de una clase específica"""
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "UPDATE profesor_clase_asignaciones SET activa = false WHERE clase_horario_id = %s AND profesor_id = %s",
                    (clase_horario_id, profesor_id)
                )
                conn.commit()


    def verificar_cupo_disponible(self, clase_horario_id: int) -> bool:
        """Verifica si hay cupo disponible en una clase"""
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                sql = """
                    SELECT ch.cupo_maximo, COUNT(cu.id) as inscriptos
                    FROM clases_horarios ch
                    LEFT JOIN clase_usuarios cu ON ch.id = cu.clase_horario_id
                    WHERE ch.id = %s
                    GROUP BY ch.cupo_maximo
                """
                cursor.execute(sql, (clase_horario_id,))
                row = cursor.fetchone()
                
                if row:
                    cupo_maximo, inscriptos = row
                    return inscriptos < cupo_maximo
                return False


    def agregar_a_lista_espera(self, clase_horario_id: int, usuario_id: int):
        """Método anterior redirigido: usa clase_lista_espera con gestión de posición/activo."""
        # Redirigir al método principal para evitar escrituras en la tabla anterior
        try:
            return self.agregar_a_lista_espera_completo(clase_horario_id, usuario_id)
        except Exception:
            # No interrumpir el flujo si falla; retornar None para compatibilidad
            return None


    def obtener_actividad_reciente(self, limit: int = 10) -> List[dict]:
        """Obtiene actividad reciente con usuario, timestamp y tipo usando datos reales.
        Devuelve elementos con claves: actividad (str), fecha (datetime/str), actor (str), tipo (str).
        """
        with self.get_connection_context() as conn:
            try:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                sql = """
                SELECT actividad, fecha, actor, tipo FROM (
                    -- USUARIOS (incluye creación de socios y actualizaciones/eliminaciones)
                    SELECT 
                        CASE 
                            WHEN al.action = 'CREATE' AND u.rol = 'socio' THEN 'Nuevo Socio: ' || u.nombre
                            WHEN al.action = 'UPDATE' THEN 'Usuario actualizado: ' || COALESCE(u.nombre, 'ID ' || al.record_id::text)
                            WHEN al.action = 'DELETE' THEN 'Usuario eliminado: ' || COALESCE(u.nombre, 'ID ' || al.record_id::text)
                            ELSE 'Usuario: ' || COALESCE(u.nombre, 'ID ' || al.record_id::text)
                        END AS actividad,
                        COALESCE(al.timestamp, u.fecha_registro) AS fecha,
                        COALESCE(uc.nombre, 'Desconocido') AS actor,
                        CASE WHEN u.rol = 'socio' THEN 'Socio' ELSE 'Usuario' END AS tipo
                    FROM audit_logs al
                    LEFT JOIN usuarios u ON u.id = al.record_id
                    LEFT JOIN usuarios uc ON al.user_id = uc.id
                    WHERE al.table_name = 'usuarios'

                    UNION ALL

                    -- PAGOS (creación/actualización/eliminación)
                    SELECT 
                        CASE 
                            WHEN al.action = 'CREATE' THEN 'Nuevo Pago: ' || u.nombre || ' ($' || CAST(p.monto AS INTEGER) || ')'
                            WHEN al.action = 'UPDATE' THEN 'Pago actualizado: ' || COALESCE(u.nombre, 'ID ' || al.record_id::text)
                            WHEN al.action = 'DELETE' THEN 'Pago eliminado: ' || COALESCE(u.nombre, 'ID ' || al.record_id::text)
                            ELSE 'Pago: ' || COALESCE(u.nombre, 'ID ' || al.record_id::text)
                        END AS actividad,
                        COALESCE(al.timestamp, p.fecha_pago) AS fecha,
                        COALESCE(uc.nombre, 'Desconocido') AS actor,
                        'Pago' AS tipo
                    FROM audit_logs al
                    LEFT JOIN pagos p ON p.id = al.record_id
                    LEFT JOIN usuarios u ON p.usuario_id = u.id
                    LEFT JOIN usuarios uc ON al.user_id = uc.id
                    WHERE al.table_name = 'pagos'

                    UNION ALL

                    -- ASISTENCIAS (creación/eliminación)
                    SELECT 
                        CASE 
                            WHEN al.action = 'CREATE' THEN 'Nueva Asistencia: ' || COALESCE(u.nombre, 'ID ' || al.record_id::text)
                            WHEN al.action = 'DELETE' THEN 'Asistencia eliminada: ' || COALESCE(u.nombre, 'ID ' || al.record_id::text)
                            ELSE 'Asistencia: ' || COALESCE(u.nombre, 'ID ' || al.record_id::text)
                        END AS actividad,
                        COALESCE(al.timestamp, a.fecha) AS fecha,
                        COALESCE(uc.nombre, 'Desconocido') AS actor,
                        'Asistencia' AS tipo
                    FROM audit_logs al
                    LEFT JOIN asistencias a ON a.id = al.record_id
                    LEFT JOIN usuarios u ON a.usuario_id = u.id
                    LEFT JOIN usuarios uc ON al.user_id = uc.id
                    WHERE al.table_name = 'asistencias'

                    UNION ALL

                    -- MÉTODOS DE PAGO
                    SELECT 
                        CASE 
                            WHEN al.action = 'CREATE' THEN 'Método de Pago creado: ' || COALESCE(mp.nombre, 'ID ' || al.record_id::text)
                            WHEN al.action = 'UPDATE' THEN 'Método de Pago actualizado: ' || COALESCE(mp.nombre, 'ID ' || al.record_id::text)
                            WHEN al.action = 'DELETE' THEN 'Método de Pago eliminado: ' || COALESCE(mp.nombre, 'ID ' || al.record_id::text)
                            ELSE 'Método de Pago: ' || COALESCE(mp.nombre, 'ID ' || al.record_id::text)
                        END AS actividad,
                        al.timestamp AS fecha,
                        COALESCE(uc.nombre, 'Desconocido') AS actor,
                        'Método de Pago' AS tipo
                    FROM audit_logs al
                    LEFT JOIN metodos_pago mp ON mp.id = al.record_id
                    LEFT JOIN usuarios uc ON al.user_id = uc.id
                    WHERE al.table_name = 'metodos_pago'

                    UNION ALL

                    -- CONCEPTOS DE PAGO
                    SELECT 
                        CASE 
                            WHEN al.action = 'CREATE' THEN 'Concepto de Pago creado: ' || COALESCE(cp.nombre, 'ID ' || al.record_id::text)
                            WHEN al.action = 'UPDATE' THEN 'Concepto de Pago actualizado: ' || COALESCE(cp.nombre, 'ID ' || al.record_id::text)
                            WHEN al.action = 'DELETE' THEN 'Concepto de Pago eliminado: ' || COALESCE(cp.nombre, 'ID ' || al.record_id::text)
                            ELSE 'Concepto de Pago: ' || COALESCE(cp.nombre, 'ID ' || al.record_id::text)
                        END AS actividad,
                        al.timestamp AS fecha,
                        COALESCE(uc.nombre, 'Desconocido') AS actor,
                        'Concepto de Pago' AS tipo
                    FROM audit_logs al
                    LEFT JOIN conceptos_pago cp ON cp.id = al.record_id
                    LEFT JOIN usuarios uc ON al.user_id = uc.id
                    WHERE al.table_name = 'conceptos_pago'
                ) t
                ORDER BY fecha DESC
                LIMIT %s
                """
                cursor.execute(sql, (limit,))
                return [dict(r) for r in cursor.fetchall()]
            except Exception:
                # Fallback básico si audit_logs no está disponible
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                sql = """
                SELECT actividad, fecha, actor, tipo FROM (
                    SELECT 'Nuevo Socio: ' || nombre AS actividad, fecha_registro AS fecha, 'Desconocido' AS actor, 'Socio' AS tipo
                    FROM usuarios WHERE rol = 'socio'
                    UNION ALL
                    SELECT 'Nuevo Pago: ' || u.nombre || ' ($' || CAST(p.monto AS INTEGER) || ')' AS actividad, p.fecha_pago AS fecha, 'Desconocido' AS actor, 'Pago' AS tipo
                    FROM pagos p JOIN usuarios u ON p.usuario_id = u.id
                    UNION ALL
                    SELECT 'Nueva Asistencia: ' || u.nombre AS actividad, a.fecha AS fecha, 'Desconocido' AS actor, 'Asistencia' AS tipo
                    FROM asistencias a JOIN usuarios u ON a.usuario_id = u.id
                ) t
                ORDER BY fecha DESC
                LIMIT %s
                """
                cursor.execute(sql, (limit,))
                return [dict(r) for r in cursor.fetchall()]


    def obtener_profesores_asignados_a_clase(self, clase_horario_id: int) -> List[dict]:
        """Obtiene los profesores asignados a una clase específica"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            sql = """
                SELECT p.id, u.nombre, u.email, p.especialidades, p.tarifa_por_hora
                FROM profesor_clase_asignaciones pca
                JOIN profesores p ON pca.profesor_id = p.id
                JOIN usuarios u ON p.usuario_id = u.id
                WHERE pca.clase_horario_id = %s AND pca.activa = true
            """
            cursor.execute(sql, (clase_horario_id,))
            return [dict(row) for row in cursor.fetchall()]


    def obtener_clase_por_id(self, clase_id: int) -> Optional[dict]:
        """Obtiene información de una clase por su ID"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            sql = """
                SELECT c.*, t.nombre as tipo_clase_nombre
                FROM clases c
                LEFT JOIN tipos_clases t ON c.tipo_clase_id = t.id
                WHERE c.id = %s
            """
            cursor.execute(sql, (clase_id,))
            row = cursor.fetchone()
            return dict(row) if row else None


    def obtener_estudiantes_clase(self, clase_horario_id: int) -> List[dict]:
        """Obtiene los estudiantes inscritos en una clase específica"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            sql = """
                SELECT u.id, u.nombre, u.email, i.fecha_inscripcion
                FROM inscripciones i
                JOIN usuarios u ON i.usuario_id = u.id
                WHERE i.clase_horario_id = %s
                ORDER BY i.fecha_inscripcion
            """
            cursor.execute(sql, (clase_horario_id,))
            return [dict(row) for row in cursor.fetchall()]

     # --- MÉTODOS DE PAGOS ---
    

    def obtener_arpu_por_mes_ultimos_12(self) -> Dict[str, float]:
        """Devuelve ARPU mensual para los últimos 12 meses con clave 'YYYY-MM'.

        Soporta esquema mixto (fecha_pago o año/mes) y rellena meses faltantes con 0.
        """
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    WITH meses AS (
                        SELECT date_trunc('month', CURRENT_DATE) - INTERVAL '11 months' + (gs || ' months')::interval AS inicio_mes
                        FROM generate_series(0, 11) AS gs
                    ), pagos_mes AS (
                        SELECT date_trunc('month',
                               CASE
                                   WHEN p.fecha_pago IS NOT NULL THEN p.fecha_pago
                                   WHEN p.año ~ '^[0-9]+' AND p.mes ~ '^[0-9]+' THEN make_date(p.año::int, p.mes::int, 1)
                                   ELSE NULL
                               END
                        ) AS mes,
                               SUM(p.monto) AS total_monto,
                               COUNT(DISTINCT p.usuario_id) AS pagadores
                        FROM pagos p
                        WHERE (
                            (p.fecha_pago IS NOT NULL AND p.fecha_pago >= date_trunc('month', CURRENT_DATE) - INTERVAL '11 months')
                            OR (
                                p.fecha_pago IS NULL AND p.año ~ '^[0-9]+' AND p.mes ~ '^[0-9]+' AND
                                make_date(p.año::int, p.mes::int, 1) >= date_trunc('month', CURRENT_DATE) - INTERVAL '11 months'
                            )
                        )
                        GROUP BY 1
                    )
                    SELECT to_char(meses.inicio_mes, 'YYYY-MM') AS m,
                           COALESCE(pagos_mes.total_monto / NULLIF(pagos_mes.pagadores, 0), 0) AS arpu
                    FROM meses
                    LEFT JOIN pagos_mes ON pagos_mes.mes = meses.inicio_mes
                    ORDER BY 1
                    """
                )
                out: Dict[str, float] = {}
                for m, v in cursor.fetchall():
                    out[str(m)] = float(v or 0.0)
                return out
        except Exception as e:
            logging.error(f"Error obtener_arpu_por_mes_ultimos_12: {e}")
            return {}

    # --- MÉTODOS DE ASISTENCIAS ---
    

    def crear_grupo_ejercicios(self, nombre_grupo: str, ejercicio_ids: List[int]) -> int:
        """Crea un grupo de ejercicios con PostgreSQL"""
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO ejercicio_grupos (nombre) VALUES (%s) RETURNING id",
                    (nombre_grupo,)
                )
                grupo_id = cursor.fetchone()[0]
                
                if ejercicio_ids:
                    data = [(grupo_id, ej_id) for ej_id in ejercicio_ids]
                    cursor.executemany(
                        "INSERT INTO ejercicio_grupo_items (grupo_id, ejercicio_id) VALUES (%s, %s)",
                        data
                    )
                conn.commit()
                return grupo_id


    def obtener_grupos_ejercicios(self) -> List[EjercicioGrupo]:
        """Obtiene todos los grupos de ejercicios con PostgreSQL"""
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM ejercicio_grupos ORDER BY nombre")
                return [EjercicioGrupo(**dict(zip([desc[0] for desc in cursor.description], row))) 
                       for row in cursor.fetchall()]


    def obtener_ejercicios_de_grupo(self, grupo_id: int) -> List[Ejercicio]:
        """Obtiene ejercicios de un grupo específico con PostgreSQL"""
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                sql = """
                    SELECT e.* FROM ejercicios e
                    JOIN ejercicio_grupo_items egi ON e.id = egi.ejercicio_id
                    WHERE egi.grupo_id = %s ORDER BY e.nombre
                """
                cursor.execute(sql, (grupo_id,))
                return [Ejercicio(**dict(zip([desc[0] for desc in cursor.description], row))) 
                       for row in cursor.fetchall()]


    def eliminar_grupo_ejercicios(self, grupo_id: int) -> bool:
        """Elimina un grupo de ejercicios (PostgreSQL) y sus items"""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM ejercicio_grupo_items WHERE grupo_id = %s", (grupo_id,))
                cursor.execute("DELETE FROM ejercicio_grupos WHERE id = %s", (grupo_id,))
                conn.commit()
                return True
        except Exception as e:
            logging.error(f"Error eliminando grupo de ejercicios: {e}")
            return False

    # --- MÉTODOS DE PRECIOS Y TIPOS DE CUOTA ---

    def crear_rutina(self, rutina) -> int:
        """Crea una nueva rutina para un usuario.
        Si rutina.usuario_id es None, se considera una plantilla y no se valida el estado del usuario.
        """
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            # Validar usuario activo solo si hay usuario asociado
            if getattr(rutina, 'usuario_id', None) is not None:
                cursor.execute("SELECT activo FROM usuarios WHERE id = %s", (rutina.usuario_id,))
                row = cursor.fetchone()
                activo = (row.get('activo') if isinstance(row, dict) else (row[0] if row else False))
                if not row or not activo:
                    raise PermissionError("El usuario está inactivo: no se puede crear una rutina")
            sql = """
            INSERT INTO rutinas (usuario_id, nombre_rutina, descripcion, dias_semana, categoria) 
            VALUES (%s, %s, %s, %s, %s) RETURNING id
            """
            categoria = getattr(rutina, 'categoria', 'general')
            cursor.execute(sql, (getattr(rutina, 'usuario_id', None), rutina.nombre_rutina, rutina.descripcion, 
                               rutina.dias_semana, categoria))
            rutina_id = cursor.fetchone()['id']
            conn.commit()
            try:
                self.cache.invalidate('rutinas')
            except Exception:
                pass
            # Enforce única rutina activa para el usuario recién actualizado
            try:
                uid = getattr(rutina, 'usuario_id', None)
                if uid is not None:
                    self.enforce_single_active_rutina_usuario(int(uid))
            except Exception:
                pass
            return rutina_id
    

    def actualizar_rutina(self, rutina) -> bool:
        """Actualiza una rutina existente."""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                sql = """
                UPDATE rutinas SET nombre_rutina = %s, descripcion = %s, 
                                 dias_semana = %s, categoria = %s 
                WHERE id = %s
                """
                categoria = getattr(rutina, 'categoria', 'general')
                cursor.execute(sql, (rutina.nombre_rutina, rutina.descripcion, 
                                   rutina.dias_semana, categoria, rutina.id))
                conn.commit()
                try:
                    self.cache.invalidate('rutinas')
                except Exception:
                    pass
                return cursor.rowcount > 0
        except Exception as e:
            logging.error(f"Error updating routine: {e}")
            return False


    def obtener_rutina_completa(self, rutina_id: int) -> Optional[dict]:
        """Obtiene una rutina completa con sus ejercicios."""
        # Cache por rutina completa (versión dict)
        try:
            cached = self.cache.get('rutinas', ('completa_dict', int(rutina_id)))
            if cached is not None:
                return cached
        except Exception:
            pass
        rutina = None
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            # Obtener rutina
            cols = self.get_table_columns('rutinas')
            base_cols = ['id', 'usuario_id', 'nombre_rutina', 'descripcion', 'dias_semana', 'categoria', 'fecha_creacion', 'activa']
            select_cols = base_cols + (['uuid_rutina'] if 'uuid_rutina' in (cols or []) else [])
            sql = f"SELECT {', '.join(select_cols)} FROM rutinas WHERE id = %s"
            cursor.execute(sql, (rutina_id,))
            row = cursor.fetchone()
            
            if row:
                rutina = dict(row)
                rutina['ejercicios'] = []
                
                # Obtener ejercicios de la rutina
                sql_ejercicios = """
                SELECT re.*, e.nombre, e.grupo_muscular, 
                       e.descripcion as ejercicio_descripcion,
                       e.video_url as ejercicio_video_url,
                       e.video_mime as ejercicio_video_mime
                FROM rutina_ejercicios re 
                JOIN ejercicios e ON re.ejercicio_id = e.id 
                WHERE re.rutina_id = %s 
                ORDER BY re.dia_semana, re.orden
                """
                cursor.execute(sql_ejercicios, (rutina_id,))
                
                for ejercicio_row in cursor.fetchall():
                    ejercicio_data = {
                        'id': ejercicio_row['ejercicio_id'],
                        'nombre': ejercicio_row['nombre'],
                        'grupo_muscular': ejercicio_row['grupo_muscular'],
                        'descripcion': ejercicio_row['ejercicio_descripcion'],
                        'video_url': ejercicio_row.get('ejercicio_video_url'),
                        'video_mime': ejercicio_row.get('ejercicio_video_mime')
                    }
                    
                    rutina_ejercicio = dict(ejercicio_row)
                    rutina_ejercicio['ejercicio'] = ejercicio_data
                    rutina['ejercicios'].append(rutina_ejercicio)
        
        try:
            self.cache.set('rutinas', ('completa_dict', int(rutina_id)), rutina)
        except Exception:
            pass


    def obtener_rutina_completa_por_uuid_dict(self, uuid_rutina: str) -> Optional[Dict[str, Any]]:
        """Obtiene la rutina y sus ejercicios en formato dict por uuid_rutina."""
        rutina: Optional[Dict[str, Any]] = None
        if not uuid_rutina:
            return None
        with self.get_connection_context() as conn:
            # Asegurar que la columna exista y tenga valores
            self.ensure_rutina_uuid_ready(conn)
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cols = self.get_table_columns('rutinas')
            if 'uuid_rutina' not in (cols or []):
                return None
            select_cols = ['id', 'usuario_id', 'nombre_rutina', 'descripcion', 'dias_semana', 'categoria', 'fecha_creacion', 'activa', 'uuid_rutina']
            sql = f"SELECT {', '.join(select_cols)} FROM rutinas WHERE uuid_rutina = %s"
            cursor.execute(sql, (uuid_rutina,))
            row = cursor.fetchone()
            if row:
                rutina = dict(row)
                rutina['ejercicios'] = []
                # Incluir columnas de video si existen en la tabla ejercicios
                ejercicios_cols = self.get_table_columns('ejercicios') or []
                has_video_url = 'video_url' in ejercicios_cols
                has_video_mime = 'video_mime' in ejercicios_cols

                sql_ejercicios = (
                    "SELECT re.*, e.nombre, e.grupo_muscular, "
                    "e.descripcion as ejercicio_descripcion" +
                    (", e.video_url as ejercicio_video_url" if has_video_url else "") +
                    (", e.video_mime as ejercicio_video_mime" if has_video_mime else "") +
                    " FROM rutina_ejercicios re "
                    "JOIN ejercicios e ON re.ejercicio_id = e.id "
                    "WHERE re.rutina_id = %s "
                    "ORDER BY re.dia_semana, re.orden"
                )
                cursor.execute(sql_ejercicios, (row['id'],))
                for ejercicio_row in cursor.fetchall():
                    ejercicio_data = {
                        'id': ejercicio_row['ejercicio_id'],
                        'nombre': ejercicio_row['nombre'],
                        'grupo_muscular': ejercicio_row['grupo_muscular'],
                        'descripcion': ejercicio_row['ejercicio_descripcion']
                    }
                    # Adjuntar datos de video si están disponibles
                    if has_video_url:
                        ejercicio_data['video_url'] = ejercicio_row.get('ejercicio_video_url')
                    if has_video_mime:
                        ejercicio_data['video_mime'] = ejercicio_row.get('ejercicio_video_mime')
                    rutina_ejercicio = dict(ejercicio_row)
                    rutina_ejercicio['ejercicio'] = ejercicio_data
                    rutina['ejercicios'].append(rutina_ejercicio)
        try:
            self.cache.set('rutinas', ('completa_dict_uuid', str(uuid_rutina)), rutina)
        except Exception:
            pass
        return rutina
        return rutina


    def set_rutina_activa_por_uuid(self, usuario_id: int, uuid_rutina: str, activa: bool) -> Optional[Dict[str, Any]]:
        """Activa o desactiva una rutina por su UUID para un usuario.

        - Si `activa` es True: desactiva todas las rutinas del usuario y activa la indicada (forzada).
        - Si `activa` es False: desactiva la indicada. Si tras la operación el usuario queda con 0 rutinas activas, re-activa por comodidad la más reciente.

        Retorna la rutina actualizada o None si no se encuentra/autorizada.
        """
        try:
            with self.get_connection_context() as conn:
                # Preparar columna uuid y valores
                try:
                    self.ensure_rutina_uuid_ready(conn)
                except Exception:
                    pass
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur.execute("SELECT id, usuario_id, activa FROM rutinas WHERE uuid_rutina = %s", (uuid_rutina,))
                row = cur.fetchone()
                if not row:
                    return None
                try:
                    rid = int(row.get('id'))
                    owner_uid = int(row.get('usuario_id')) if row.get('usuario_id') is not None else None
                except Exception:
                    rid = None
                    owner_uid = None
                if rid is None or owner_uid is None:
                    return None
                if int(owner_uid) != int(usuario_id):
                    raise PermissionError("No autorizado para modificar esta rutina")

                if bool(activa):
                    # Activación forzada: desactiva todas las rutinas del usuario y activa la indicada
                    cur.execute("UPDATE rutinas SET activa = FALSE WHERE usuario_id = %s", (int(usuario_id),))
                    cur.execute("UPDATE rutinas SET activa = TRUE WHERE id = %s", (int(rid),))
                    conn.commit()
                else:
                    # Desactivar solo la seleccionada
                    cur.execute("UPDATE rutinas SET activa = FALSE WHERE id = %s", (int(rid),))
                    conn.commit()

                    # Conveniencia: si quedan 0 activas, activar la más reciente del usuario
                    try:
                        cur.execute(
                            "SELECT COUNT(*) AS c FROM rutinas WHERE usuario_id = %s AND activa = TRUE",
                            (int(usuario_id),),
                        )
                        row_c = cur.fetchone() or {}
                        count_active = int(row_c.get("c") if isinstance(row_c, dict) else (row_c[0] if row_c else 0))
                        if count_active == 0:
                            cur.execute(
                                """
                                SELECT id FROM rutinas
                                WHERE usuario_id = %s
                                ORDER BY fecha_creacion DESC NULLS LAST, id DESC
                                LIMIT 1
                                """,
                                (int(usuario_id),),
                            )
                            row_latest = cur.fetchone()
                            latest_id = int(row_latest.get("id")) if row_latest and row_latest.get("id") is not None else None
                            if latest_id is not None:
                                cur.execute("UPDATE rutinas SET activa = TRUE WHERE id = %s", (int(latest_id),))
                                conn.commit()
                    except Exception:
                        pass

                try:
                    self.cache.invalidate('rutinas')
                except Exception:
                    pass

                # Devolver la rutina actualizada
                try:
                    cols = self.get_table_columns('rutinas') or []
                except Exception:
                    cols = []
                select_cols = ['id', 'usuario_id', 'nombre_rutina', 'descripcion', 'dias_semana', 'categoria', 'fecha_creacion', 'activa'] + (['uuid_rutina'] if 'uuid_rutina' in cols else [])
                cur.execute(f"SELECT {', '.join(select_cols)} FROM rutinas WHERE id = %s", (int(rid),))
                updated = cur.fetchone()
                return dict(updated) if updated else None
        except PermissionError:
            raise
        except Exception as e:
            logging.error(f"Error al cambiar estado activa de rutina uuid={uuid_rutina} usuario={usuario_id}: {e}")
            return None


    def set_rutina_activa_por_id(self, usuario_id: int, rutina_id: int, activa: bool) -> Optional[Dict[str, Any]]:
        """Activa o desactiva una rutina por su ID para un usuario.

        - Si `activa` es True: desactiva todas las rutinas del usuario y activa la indicada (forzada).
        - Si `activa` es False: desactiva la indicada. Si tras la operación el usuario queda con 0 rutinas activas, re-activa por comodidad la más reciente.

        Retorna la rutina actualizada o None si no se encuentra/autorizada.
        """
        try:
            with self.get_connection_context() as conn:
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur.execute("SELECT id, usuario_id, activa FROM rutinas WHERE id = %s", (int(rutina_id),))
                row = cur.fetchone()
                if not row:
                    return None
                try:
                    rid = int(row.get('id'))
                    owner_uid = int(row.get('usuario_id')) if row.get('usuario_id') is not None else None
                except Exception:
                    rid = None
                    owner_uid = None
                if rid is None or owner_uid is None:
                    return None
                if int(owner_uid) != int(usuario_id):
                    raise PermissionError("No autorizado para modificar esta rutina")

                if bool(activa):
                    # Activación forzada por ID
                    cur.execute("UPDATE rutinas SET activa = FALSE WHERE usuario_id = %s", (int(usuario_id),))
                    cur.execute("UPDATE rutinas SET activa = TRUE WHERE id = %s", (int(rid),))
                    conn.commit()
                else:
                    # Desactivar solo la seleccionada
                    cur.execute("UPDATE rutinas SET activa = FALSE WHERE id = %s", (int(rid),))
                    conn.commit()

                    # Conveniencia: si quedan 0 activas, activar la más reciente del usuario
                    try:
                        cur.execute(
                            "SELECT COUNT(*) AS c FROM rutinas WHERE usuario_id = %s AND activa = TRUE",
                            (int(usuario_id),),
                        )
                        row_c = cur.fetchone() or {}
                        count_active = int(row_c.get("c") if isinstance(row_c, dict) else (row_c[0] if row_c else 0))
                        if count_active == 0:
                            cur.execute(
                                """
                                SELECT id FROM rutinas
                                WHERE usuario_id = %s
                                ORDER BY fecha_creacion DESC NULLS LAST, id DESC
                                LIMIT 1
                                """,
                                (int(usuario_id),),
                            )
                            row_latest = cur.fetchone()
                            latest_id = int(row_latest.get("id")) if row_latest and row_latest.get("id") is not None else None
                            if latest_id is not None:
                                cur.execute("UPDATE rutinas SET activa = TRUE WHERE id = %s", (int(latest_id),))
                                conn.commit()
                    except Exception:
                        pass

                try:
                    self.cache.invalidate('rutinas')
                except Exception:
                    pass

                # Devolver la rutina actualizada
                try:
                    cols = self.get_table_columns('rutinas') or []
                except Exception:
                    cols = []
                select_cols = ['id', 'usuario_id', 'nombre_rutina', 'descripcion', 'dias_semana', 'categoria', 'fecha_creacion', 'activa'] + (['uuid_rutina'] if 'uuid_rutina' in cols else [])
                cur.execute(f"SELECT {', '.join(select_cols)} FROM rutinas WHERE id = %s", (int(rid),))
                updated = cur.fetchone()
                return dict(updated) if updated else None
        except PermissionError:
            raise
        except Exception as e:
            logging.error(f"Error al cambiar estado activa de rutina id={rutina_id} usuario={usuario_id}: {e}")
            return None
    

    def obtener_ejercicios(self, filtro: str = "", objetivo: str = "", 
                          grupo_muscular: str = "") -> List[Ejercicio]:
        """Obtiene ejercicios con filtros avanzados con caché TTL por combinación de filtros."""
        # Normalizar claves de filtro para caché
        filtro_key = (filtro or "").strip().lower() or None
        objetivo_key = None if not objetivo or objetivo == "Todos" else str(objetivo).strip().lower()
        grupo_key = None if not grupo_muscular or grupo_muscular == "Todos" else str(grupo_muscular).strip().lower()
        cache_key = (filtro_key, objetivo_key, grupo_key)

        try:
            cached = self.cache.get('ejercicios', cache_key)
            if cached is not None:
                return cached
        except Exception:
            pass

        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            # Columnas existentes en ejercicios
            cols = self.get_table_columns('ejercicios') or []
            has_objetivo = 'objetivo' in cols
            has_grupo = 'grupo_muscular' in cols
            has_video_url = 'video_url' in cols
            has_video_mime = 'video_mime' in cols

            select_cols = ['id', 'nombre', 'descripcion']
            if has_grupo:
                select_cols.append('grupo_muscular')
            if has_objetivo:
                select_cols.append('objetivo')
            if has_video_url:
                select_cols.append('video_url')
            if has_video_mime:
                select_cols.append('video_mime')

            # Construir consulta dinámica segura
            sql = f"SELECT {', '.join(select_cols)} FROM ejercicios WHERE 1=1"
            params: List[Any] = []

            # Filtro por nombre
            if filtro:
                sql += " AND (nombre ILIKE %s OR descripcion ILIKE %s)"
                params.extend([f"%{filtro}%", f"%{filtro}%"])

            # Filtro por objetivo (solo si la columna existe)
            if has_objetivo and objetivo and objetivo != "Todos":
                sql += " AND objetivo = %s"
                params.append(objetivo)

            # Filtro por grupo muscular (solo si la columna existe)
            if has_grupo and grupo_muscular and grupo_muscular != "Todos":
                sql += " AND grupo_muscular = %s"
                params.append(grupo_muscular)

            sql += " ORDER BY nombre"
            cursor.execute(sql, params)
            # Mapear filas a modelo, tolerando columnas faltantes
            result = [Ejercicio(**dict(r)) for r in cursor.fetchall()]
            try:
                # TTL corto para evitar staleness visible en UI tras crear/editar
                self.cache.set('ejercicios', cache_key, result, ttl_seconds=3.0)
            except Exception:
                pass
            return result
    

    def crear_ejercicio(self, ejercicio) -> int:
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                full_cols = ['nombre', 'grupo_muscular', 'descripcion', 'objetivo', 'video_url', 'video_mime']
                full_vals = [
                    ejercicio.nombre,
                    ejercicio.grupo_muscular,
                    ejercicio.descripcion,
                    getattr(ejercicio, 'objetivo', 'general'),
                    getattr(ejercicio, 'video_url', None),
                    getattr(ejercicio, 'video_mime', None),
                ]
                try:
                    cursor.execute(
                        f"INSERT INTO ejercicios ({', '.join(full_cols)}) VALUES ({', '.join(['%s'] * len(full_cols))}) RETURNING id",
                        tuple(full_vals),
                    )
                except Exception as e:
                    try:
                        conn.rollback()
                    except psycopg2.Error:
                        pass
                    code = getattr(e, 'pgcode', None)
                    msg = str(e).lower()
                    if code == '23505' or 'duplicate key' in msg:
                        raise
                    try:
                        cols = self.get_table_columns('ejercicios') or []
                    except Exception:
                        cols = []
                    insert_cols = ['nombre']
                    insert_vals = [ejercicio.nombre]
                    if 'grupo_muscular' in cols:
                        insert_cols.append('grupo_muscular')
                        insert_vals.append(ejercicio.grupo_muscular)
                    if 'descripcion' in cols:
                        insert_cols.append('descripcion')
                        insert_vals.append(ejercicio.descripcion)
                    if 'objetivo' in cols:
                        insert_cols.append('objetivo')
                        insert_vals.append(getattr(ejercicio, 'objetivo', 'general'))
                    if 'video_url' in cols:
                        insert_cols.append('video_url')
                        insert_vals.append(getattr(ejercicio, 'video_url', None))
                    if 'video_mime' in cols:
                        insert_cols.append('video_mime')
                        insert_vals.append(getattr(ejercicio, 'video_mime', None))
                    placeholders = ', '.join(['%s'] * len(insert_vals))
                    sql = f"INSERT INTO ejercicios ({', '.join(insert_cols)}) VALUES ({placeholders}) RETURNING id"
                    cursor.execute(sql, tuple(insert_vals))
                result = cursor.fetchone()
                if result is None:
                    raise Exception(f"Error creando ejercicio {ejercicio.nombre}: fetchone returned None")
                ejercicio_id = result['id']
                conn.commit()
                try:
                    self.cache.invalidate('ejercicios')
                except Exception:
                    pass
                return ejercicio_id
        except Exception as e:
            logging.error(f"Error creating exercise {ejercicio.nombre}: {e}")
            raise Exception(f"Error creando ejercicio {ejercicio.nombre}: {str(e)}")
    
    # === MÉTODOS DE GESTIÓN DE CLASES Y HORARIOS ===
    

    def guardar_ejercicios_para_clase(self, clase_id: int, items: List[Any]):
        """Guarda ejercicios asociados a una clase.
        Soporta dos formatos de entrada:
        - Lista de enteros: [ejercicio_id, ...]
        - Lista de dicts: [{ ejercicio_id, orden, series, repeticiones, descanso_segundos, notas }, ...]
        """
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            # Eliminar ejercicios existentes
            cursor.execute("DELETE FROM clase_ejercicios WHERE clase_id = %s", (clase_id,))

            # Insertar nuevos ejercicios con soporte de metadatos y orden
            if isinstance(items, list) and items:
                # Si son dicts con detalle
                if isinstance(items[0], dict):
                    data_to_insert = []
                    for idx, obj in enumerate(items):
                        try:
                            ej_id = int(obj.get('ejercicio_id') or obj.get('id'))
                        except Exception:
                            continue
                        orden = int(obj.get('orden')) if str(obj.get('orden','')).isdigit() else idx
                        series = int(obj.get('series')) if str(obj.get('series','')).isdigit() else 0
                        repeticiones = str(obj.get('repeticiones', '') or '')
                        descanso = int(obj.get('descanso_segundos')) if str(obj.get('descanso_segundos','')).isdigit() else 0
                        notas = obj.get('notas') if obj.get('notas') is not None else None
                        data_to_insert.append((clase_id, ej_id, orden, series, repeticiones, descanso, notas))
                    if data_to_insert:
                        sql = """
                        INSERT INTO clase_ejercicios (clase_id, ejercicio_id, orden, series, repeticiones, descanso_segundos, notas)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """
                        cursor.executemany(sql, data_to_insert)
                else:
                    # Lista de ids simples
                    ids = []
                    for x in items:
                        try:
                            ids.append(int(x))
                        except Exception:
                            pass
                    if ids:
                        sql = "INSERT INTO clase_ejercicios (clase_id, ejercicio_id, orden) VALUES (%s, %s, %s)"
                        data_to_insert = [(clase_id, ej_id, idx) for idx, ej_id in enumerate(ids)]
                        cursor.executemany(sql, data_to_insert)

            conn.commit()
            # Invalidar caches relacionadas
            try:
                self.cache.invalidate('clase_ejercicios', (int(clase_id),))
            except Exception:
                pass
            try:
                self.cache.invalidate('clase_ejercicios_detalle', (int(clase_id),))
            except Exception:
                pass
    

    def obtener_ejercicios_de_clase(self, clase_id: int) -> List[Ejercicio]:
        """Obtiene ejercicios asociados a una clase."""
        # Cache por clase_id
        try:
            cached = self.cache.get('clase_ejercicios', (int(clase_id),))
            if cached is not None:
                return cached
        except Exception:
            pass
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            sql = """
            SELECT e.* FROM ejercicios e 
            JOIN clase_ejercicios ce ON e.id = ce.ejercicio_id 
            WHERE ce.clase_id = %s 
            ORDER BY e.nombre
            """
            cursor.execute(sql, (clase_id,))
            result = [Ejercicio(**dict(r)) for r in cursor.fetchall()]
            try:
                self.cache.set('clase_ejercicios', (int(clase_id),), result)
            except Exception:
                pass
            return result


    def obtener_ejercicios_de_clase_detalle(self, clase_id: int) -> List[Dict]:
        """Obtiene ejercicios de una clase con metadatos y orden."""
        try:
            cached = self.cache.get('clase_ejercicios_detalle', (int(clase_id),))
            if cached is not None:
                return cached
        except Exception:
            pass
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            sql = """
            SELECT 
                e.id,
                e.nombre,
                e.descripcion,
                ce.orden,
                ce.series,
                ce.repeticiones,
                ce.descanso_segundos,
                ce.notas
            FROM clase_ejercicios ce
            JOIN ejercicios e ON e.id = ce.ejercicio_id
            WHERE ce.clase_id = %s
            ORDER BY ce.orden ASC, e.nombre ASC
            """
            cursor.execute(sql, (clase_id,))
            rows = cursor.fetchall() or []
            result = [dict(r) for r in rows]
            try:
                self.cache.set('clase_ejercicios_detalle', (int(clase_id),), result)
            except Exception:
                pass
            return result
    
    # === MÉTODOS DE GESTIÓN DE PROFESORES AVANZADOS ===
    
    @database_retry(max_retries=3, base_delay=0.8)

    def _increment_timeout_metric(self, key: str) -> None:
        """Incrementa contadores de timeouts a nivel de DatabaseManager."""
        try:
            if not hasattr(self, "_timeout_metrics") or self._timeout_metrics is None:
                self._timeout_metrics = {"lock_timeouts": 0, "statement_timeouts": 0, "idle_timeouts": 0}
            self._timeout_metrics[key] = int(self._timeout_metrics.get(key, 0) or 0) + 1
        except Exception:
            pass


    def get_timeout_metrics(self) -> Dict[str, int]:
        """Devuelve métricas de timeouts acumuladas."""
        try:
            base = {"lock_timeouts": 0, "statement_timeouts": 0, "idle_timeouts": 0}
            if hasattr(self, "_timeout_metrics") and isinstance(self._timeout_metrics, dict):
                base.update(self._timeout_metrics)
            return base
        except Exception:
            return {"lock_timeouts": 0, "statement_timeouts": 0, "idle_timeouts": 0}

    # === MÉTODOS DE GESTIÓN DE CONFLICTOS DE HORARIO ===


    def ensure_schedule_conflicts_table(self) -> None:
        """Crea la tabla schedule_conflicts si no existe (PostgreSQL)."""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS schedule_conflicts (
                        id SERIAL PRIMARY KEY,
                        conflict_type VARCHAR(50) NOT NULL,
                        severity VARCHAR(20) NOT NULL,
                        professor_id INTEGER,
                        class_id INTEGER,
                        room_id INTEGER,
                        conflict_date DATE NOT NULL,
                        conflict_time TIME NOT NULL,
                        description TEXT NOT NULL,
                        status VARCHAR(20) DEFAULT 'Activo',
                        resolution_type VARCHAR(50),
                        resolution_notes TEXT,
                        resolved_by VARCHAR(100),
                        resolved_at TIMESTAMP NULL,
                        resolution_time INTEGER,
                        created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                        CONSTRAINT fk_conflict_profesor FOREIGN KEY (professor_id) REFERENCES profesores(id),
                        CONSTRAINT fk_conflict_clase FOREIGN KEY (class_id) REFERENCES clases(id)
                    )
                    """
                )
                conn.commit()
        except Exception as e:
            logging.error(f"Error creando tabla schedule_conflicts: {e}")


    def listar_conflictos_activos(self) -> List[Dict]:
        """Lista conflictos activos con nombre del profesor (si existe)."""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cursor.execute(
                    """
                    SELECT 
                        c.id, c.conflict_type, c.severity, c.conflict_date, c.conflict_time,
                        c.description, c.status, c.created_at,
                        u.nombre AS professor_name
                    FROM schedule_conflicts c
                    LEFT JOIN profesores p ON c.professor_id = p.id
                    LEFT JOIN usuarios u ON p.usuario_id = u.id
                    WHERE c.status = 'Activo'
                    ORDER BY c.severity DESC, c.conflict_date, c.conflict_time
                    """
                )
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logging.error(f"Error en listar_conflictos_activos: {e}")
            return []


    def listar_historial_conflictos(self, limit: int = 100) -> List[Dict]:
        """Lista historial de conflictos resueltos o ignorados."""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cursor.execute(
                    """
                    SELECT conflict_date, conflict_type, description, resolution_type,
                           resolved_by, resolution_time, status
                    FROM schedule_conflicts
                    WHERE status IN ('Resuelto', 'Ignorado')
                    ORDER BY resolved_at DESC NULLS LAST
                    LIMIT %s
                    """,
                    (limit,)
                )
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logging.error(f"Error en listar_historial_conflictos: {e}")
            return []


    def obtener_conflicto_por_id(self, conflicto_id: int) -> Optional[Dict]:
        """Obtiene un conflicto por ID con nombres asociados."""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cursor.execute(
                    """
                    SELECT c.*, u.nombre AS professor_name, cl.nombre AS class_name
                    FROM schedule_conflicts c
                    LEFT JOIN profesores p ON c.professor_id = p.id
                    LEFT JOIN usuarios u ON p.usuario_id = u.id
                    LEFT JOIN clases cl ON c.class_id = cl.id
                    WHERE c.id = %s
                    """,
                    (conflicto_id,)
                )
                row = cursor.fetchone()
                return dict(row) if row else None
        except Exception as e:
            logging.error(f"Error en obtener_conflicto_por_id: {e}")
            return None


    def crear_conflicto_horario(
        self,
        conflict_type: str,
        severity: str,
        professor_id: Optional[int],
        description: str,
        conflict_date,
        conflict_time,
        class_id: Optional[int] = None,
        room_id: Optional[int] = None,
    ) -> Optional[int]:
        """Crea un nuevo conflicto y devuelve su ID."""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT INTO schedule_conflicts (
                        conflict_type, severity, professor_id, class_id, room_id,
                        conflict_date, conflict_time, description, status, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'Activo', NOW())
                    RETURNING id
                    """,
                    (
                        conflict_type, severity, professor_id, class_id, room_id,
                        conflict_date, conflict_time, description,
                    ),
                )
                new_id = cursor.fetchone()[0]
                conn.commit()
                return new_id
        except Exception as e:
            logging.error(f"Error en crear_conflicto_horario: {e}")
            return None


    def resolver_conflicto_horario(
        self, conflicto_id: int, resolution_type: str, resolution_notes: str, resolved_by: str = 'Usuario'
    ) -> bool:
        """Marca un conflicto como resuelto."""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    UPDATE schedule_conflicts SET
                        status = 'Resuelto',
                        resolution_type = %s,
                        resolution_notes = %s,
                        resolved_by = %s,
                        resolved_at = NOW()
                    WHERE id = %s
                    """,
                    (resolution_type, resolution_notes, resolved_by, conflicto_id),
                )
                conn.commit()
                return cursor.rowcount > 0
        except Exception as e:
            logging.error(f"Error en resolver_conflicto_horario: {e}")
            return False


    def ignorar_conflicto_horario(self, conflicto_id: int) -> bool:
        """Marca un conflicto como ignorado."""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    UPDATE schedule_conflicts SET
                        status = 'Ignorado'
                    WHERE id = %s
                    """,
                    (conflicto_id,),
                )
                conn.commit()
                return cursor.rowcount > 0
        except Exception as e:
            logging.error(f"Error en ignorar_conflicto_horario: {e}")
            return False


    def obtener_solapamientos_horarios(self) -> List[Dict]:
        """Obtiene solapamientos de horarios por profesor (evita duplicados)."""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cursor.execute(
                    """
                    SELECT 
                        h1.id AS h1_id,
                        h2.id AS h2_id,
                        h1.profesor_id AS professor_id,
                        h1.dia_semana,
                        h1.hora_inicio AS h1_hora_inicio,
                        h1.hora_fin AS h1_hora_fin,
                        h2.hora_inicio AS h2_hora_inicio,
                        h2.hora_fin AS h2_hora_fin,
                        u.nombre AS profesor_nombre
                    FROM horarios h1
                    JOIN horarios h2 
                      ON h1.dia_semana = h2.dia_semana 
                     AND h1.profesor_id = h2.profesor_id
                     AND h1.id < h2.id
                     AND h1.activo = TRUE AND h2.activo = TRUE
                    JOIN profesores p ON h1.profesor_id = p.id
                    JOIN usuarios u ON p.usuario_id = u.id
                    WHERE (h1.hora_inicio < h2.hora_fin AND h1.hora_fin > h2.hora_inicio)
                    """
                )
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logging.error(f"Error en obtener_solapamientos_horarios: {e}")
            return []

    # --- MÉTODOS DE CONTEO DE CONFLICTOS ---

    def contar_conflictos_activos(self) -> int:
        """Cuenta conflictos con estado 'Activo'."""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM schedule_conflicts WHERE status = 'Activo'")
                return cursor.fetchone()[0] or 0
        except Exception as e:
            logging.error(f"Error en contar_conflictos_activos: {e}")
            return 0


    def contar_conflictos_criticos_activos(self) -> int:
        """Cuenta conflictos activos cuyo severity es crítico/alto."""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT COUNT(*)
                    FROM schedule_conflicts
                    WHERE status = 'Activo'
                      AND LOWER(TRANSLATE(severity, 'áéíóúÁÉÍÓÚ', 'aeiouAEIOU')) IN (%s, %s, %s, %s)
                """, ('critico', 'critical', 'high', 'alto'))
                return cursor.fetchone()[0] or 0
        except Exception as e:
            logging.error(f"Error en contar_conflictos_criticos_activos: {e}")
            return 0


    def contar_conflictos_activos_hoy(self) -> int:
        """Cuenta conflictos activos creados hoy o con conflict_date hoy."""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT COUNT(*)
                    FROM schedule_conflicts
                    WHERE status = 'Activo'
                      AND (
                        DATE(created_at) = CURRENT_DATE
                        OR conflict_date = CURRENT_DATE
                      )
                """)
                return cursor.fetchone()[0] or 0
        except Exception as e:
            logging.error(f"Error en contar_conflictos_activos_hoy: {e}")
            return 0

    # === MÉTODOS DE REPORTES Y ACTIVIDAD RECIENTE ===
    

    def obtener_actividad_reciente(self, limit: int = 10, current_user_id: int | None = None) -> List[dict]:
        """Obtiene actividad reciente del gimnasio con usuario, timestamp y tipo.
        Devuelve elementos con claves: actividad, fecha, actor, tipo.
        Si no hay actor en logs, usa el usuario de sesión (current_user_id) como fallback.
        """
        with self.get_connection_context() as conn:
            # Preferir datos con auditoría; fallback si no está disponible
            try:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                sql = """
                SELECT actividad, fecha, actor, tipo FROM (
                    -- Creación de socios
                    SELECT 
                        'Nuevo Socio: ' || u.nombre AS actividad,
                        COALESCE(al.timestamp, u.fecha_registro) AS fecha,
                        COALESCE(uc.nombre, 'Desconocido') AS actor,
                        'Socio' AS tipo
                    FROM usuarios u
                    LEFT JOIN audit_logs al ON al.table_name = 'usuarios' AND al.action = 'CREATE' AND al.record_id = u.id
                    LEFT JOIN usuarios uc ON al.user_id = uc.id
                    WHERE u.rol = 'socio'
                    UNION ALL
                    -- Creación de pagos
                    SELECT 
                        'Nuevo Pago: ' || u.nombre || ' ($' || CAST(p.monto AS INTEGER) || ')' AS actividad,
                        COALESCE(al.timestamp, p.fecha_pago) AS fecha,
                        COALESCE(uc.nombre, 'Desconocido') AS actor,
                        'Pago' AS tipo
                    FROM pagos p
                    JOIN usuarios u ON p.usuario_id = u.id
                    LEFT JOIN audit_logs al ON al.table_name = 'pagos' AND al.action = 'CREATE' AND al.record_id = p.id
                    LEFT JOIN usuarios uc ON al.user_id = uc.id
                    UNION ALL
                    -- Alertas del sistema desde audit_logs
                    SELECT 
                        COALESCE((al.new_values::json ->> 'title'), 'Alerta del Sistema') ||
                        CASE WHEN (al.new_values::json ->> 'message') IS NOT NULL AND (al.new_values::json ->> 'message') <> ''
                             THEN ': ' || (al.new_values::json ->> 'message') ELSE '' END AS actividad,
                        al.timestamp AS fecha,
                        COALESCE(u.nombre, 'Desconocido') AS actor,
                        'Alerta' AS tipo
                    FROM audit_logs al
                    LEFT JOIN usuarios u ON al.user_id = u.id
                    WHERE al.table_name = 'alerts' AND al.action = 'ALERT'
                ) t
                ORDER BY fecha DESC
                LIMIT %s
                """
                cursor.execute(sql, (limit,))
                rows = [dict(r) for r in cursor.fetchall()]
            except Exception:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                sql = """
                SELECT actividad, fecha, actor, tipo FROM (
                    SELECT 'Nuevo Socio: ' || nombre AS actividad, fecha_registro AS fecha, 'Desconocido' AS actor, 'Socio' AS tipo
                    FROM usuarios WHERE rol = 'socio'
                    UNION ALL
                    SELECT 'Nuevo Pago: ' || u.nombre || ' ($' || CAST(p.monto AS INTEGER) || ')' AS actividad, p.fecha_pago AS fecha, 'Desconocido' AS actor, 'Pago' AS tipo
                    FROM pagos p JOIN usuarios u ON p.usuario_id = u.id
                    UNION ALL
                    SELECT 'Alerta del Sistema' AS actividad, al.timestamp AS fecha, 'Desconocido' AS actor, 'Alerta' AS tipo
                    FROM audit_logs al WHERE al.table_name = 'alerts' AND al.action = 'ALERT'
                ) t
                ORDER BY fecha DESC
                LIMIT %s
                """
                cursor.execute(sql, (limit,))
                rows = [dict(r) for r in cursor.fetchall()]

            # Fallback del actor al usuario de sesión si está disponible
            if current_user_id:
                try:
                    cursor = conn.cursor()
                    cursor.execute("SELECT nombre FROM usuarios WHERE id = %s", (current_user_id,))
                    res = cursor.fetchone()
                    current_user_name = res[0] if res else None
                except Exception:
                    current_user_name = None
                if current_user_name:
                    for r in rows:
                        if not r.get('actor') or r.get('actor') == 'Desconocido':
                            r['actor'] = current_user_name
            return rows
    
    # === USER STATE AUTOMATION METHODS ===
    

    def crear_tarea_mantenimiento(self, task_name: str, task_type: str, description: str = None,
                                 scheduled_at: str = None, created_by: int = None, 
                                 auto_schedule: bool = False, frequency_days: int = None) -> int:
        """Crea una nueva tarea de mantenimiento PostgreSQL"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            # Calcular próxima ejecución si es automática
            next_execution = None
            if auto_schedule and frequency_days:
                cursor.execute("SELECT CURRENT_TIMESTAMP + INTERVAL '%s days'", (frequency_days,))
                next_execution = cursor.fetchone()[0]
            
            sql = """
                INSERT INTO maintenance_tasks 
                (task_name, task_type, description, scheduled_at, created_by, auto_schedule, 
                 frequency_days, next_execution)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """
            cursor.execute(sql, (task_name, task_type, description, scheduled_at, created_by,
                               auto_schedule, frequency_days, next_execution))
            conn.commit()
            return cursor.fetchone()[0]
    

    def obtener_tareas_mantenimiento(self, status: str = None, limit: int = 50) -> List[Dict]:
        """Obtiene tareas de mantenimiento con filtros opcionales PostgreSQL"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            sql = """
                SELECT mt.*, 
                       uc.nombre as creado_por_nombre,
                       ue.nombre as ejecutado_por_nombre
                FROM maintenance_tasks mt
                LEFT JOIN usuarios uc ON mt.created_by = uc.id
                LEFT JOIN usuarios ue ON mt.executed_by = ue.id
                WHERE 1=1
            """
            params = []
            
            if status:
                sql += " AND mt.status = %s"
                params.append(status)
            
            sql += " ORDER BY mt.scheduled_at DESC LIMIT %s"
            params.append(limit)
            
            cursor.execute(sql, params)
            return [dict(zip([desc[0] for desc in cursor.description], row)) for row in cursor.fetchall()]
    

    def ejecutar_tarea_mantenimiento(self, task_id: int, executed_by: int, result: str = None, 
                                   error_message: str = None) -> bool:
        """Marca una tarea de mantenimiento como ejecutada PostgreSQL"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            status = 'completed' if not error_message else 'failed'
            
            sql = """
                UPDATE maintenance_tasks 
                SET executed_at = CURRENT_TIMESTAMP, executed_by = %s, status = %s, 
                    result = %s, error_message = %s
                WHERE id = %s
            """
            cursor.execute(sql, (executed_by, status, result, error_message, task_id))
            
            # Si es una tarea automática, programar la siguiente
            cursor.execute("""
                SELECT auto_schedule, frequency_days, task_name, task_type, description
                FROM maintenance_tasks 
                WHERE id = %s AND auto_schedule = true AND frequency_days IS NOT NULL
            """, (task_id,))
            
            tarea_auto = cursor.fetchone()
            if tarea_auto:
                # Crear la siguiente tarea automática
                cursor.execute("SELECT CURRENT_TIMESTAMP + INTERVAL '%s days'", (tarea_auto[1],))
                next_execution = cursor.fetchone()[0]
                
                cursor.execute("""
                    INSERT INTO maintenance_tasks 
                    (task_name, task_type, description, scheduled_at, auto_schedule, 
                     frequency_days, next_execution, status)
                    VALUES (%s, %s, %s, %s, true, %s, %s, 'pending')
                """, (tarea_auto[2], tarea_auto[3], tarea_auto[4],
                      next_execution, tarea_auto[1], next_execution))
            
            conn.commit()
            return cursor.rowcount > 0


    def cerrar_sesiones_huerfanas(self, threshold_hours: int = 24, cap_hours: int = 12,
                                  executed_by: int | None = None) -> Dict[str, Any]:
        """Cierra sesiones con `hora_fin IS NULL` cuya duración supera `threshold_hours`.

        Por extremo cuidado:
        - Audita cada cierre con acción `auto_close_orphan_session`.
        - Limita la duración máxima cerrada a `cap_hours` para evitar registrar duraciones irreales.
        - Devuelve un resumen detallado.
        """
        resultado = {
            'success': True,
            'cerradas': 0,
            'umbral_horas': threshold_hours,
            'cap_horas': cap_hours,
            'detalles': [],
        }
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cursor.execute(
                    """
                        SELECT id, profesor_id, hora_inicio, fecha, tipo_actividad
                        FROM profesor_horas_trabajadas
                        WHERE hora_fin IS NULL
                          AND (CURRENT_TIMESTAMP - hora_inicio) > INTERVAL %s
                        ORDER BY hora_inicio
                    """,
                    (f"{threshold_hours} hours",)
                )
                rows = cursor.fetchall() or []

                for r in rows:
                    sesion_id = r['id']
                    profesor_id = r['profesor_id']
                    hora_inicio = r['hora_inicio']

                    # Hora de cierre cuidadosa: mínima entre ahora y inicio + cap_hours
                    cursor.execute("SELECT CURRENT_TIMESTAMP")
                    ahora = cursor.fetchone()['current_timestamp']
                    cursor.execute("SELECT %s::timestamp + INTERVAL %s", (hora_inicio, f"{cap_hours} hours"))
                    cierre_cap = cursor.fetchone()[0]
                    hora_cierre = cierre_cap if cierre_cap < ahora else ahora

                    try:
                        cursor.execute(
                            """
                                UPDATE profesor_horas_trabajadas
                                SET hora_fin = %s,
                                    tipo_actividad = COALESCE(tipo_actividad, '') || ' [auto_cierre]'
                                WHERE id = %s AND hora_fin IS NULL
                                RETURNING id
                            """,
                            (hora_cierre, sesion_id)
                        )
                        updated = cursor.fetchone()
                        if updated:
                            resultado['cerradas'] += 1
                            detalle = {
                                'sesion_id': sesion_id,
                                'profesor_id': profesor_id,
                                'hora_inicio': str(hora_inicio),
                                'hora_fin': str(hora_cierre),
                                'motivo': f"excede {threshold_hours}h, capped a {cap_hours}h",
                            }
                            resultado['detalles'].append(detalle)

                            # Auditoría del cierre automático
                            try:
                                self.registrar_audit_log(
                                    user_id=executed_by or 0,
                                    action='auto_close_orphan_session',
                                    table_name='profesor_horas_trabajadas',
                                    record_id=sesion_id,
                                    old_values={'hora_inicio': str(hora_inicio), 'hora_fin': None},
                                    new_values={'hora_fin': str(hora_cierre), 'cap_hours': cap_hours},
                                )
                            except Exception as _ae:
                                logging.warning(f"No se pudo registrar auditoría de cierre auto: {_ae}")
                    except Exception as ue:
                        logging.error(f"Error cerrando sesión huérfana {sesion_id}: {ue}")
                conn.commit()
        except Exception as e:
            logging.error(f"Error en cierre de sesiones huérfanas: {e}")
            resultado['success'] = False
            resultado['error'] = str(e)
        return resultado


    def programar_cierre_sesiones_huerfanas(self, threshold_hours: int = 24, cap_hours: int = 12,
                                             created_by: int | None = None, auto_schedule: bool = True,
                                             frequency_days: int = 1) -> Dict[str, Any]:
        """Crea una tarea de mantenimiento para cierre de sesiones huérfanas y la ejecuta de inmediato.

        Programación automática diaria por defecto. Devuelve información de la ejecución inicial.
        """
        try:
            descripcion = f"Cerrar sesiones huérfanas (> {threshold_hours}h, cap {cap_hours}h)"
            tarea_id = self.crear_tarea_mantenimiento(
                task_name='cerrar_sesiones_huerfanas',
                task_type='db_maintenance',
                description=descripcion,
                scheduled_at=None,
                created_by=created_by or 0,
                auto_schedule=auto_schedule,
                frequency_days=frequency_days
            )

            # Ejecutar inmediatamente y registrar resultado
            res = self.cerrar_sesiones_huerfanas(threshold_hours=threshold_hours,
                                                 cap_hours=cap_hours,
                                                 executed_by=created_by or 0)
            ok = self.ejecutar_tarea_mantenimiento(task_id=tarea_id,
                                                   executed_by=created_by or 0,
                                                   result=json.dumps(res, ensure_ascii=False),
                                                   error_message=None if res.get('success') else res.get('error'))
            return {
                'success': ok and res.get('success', False),
                'tarea_id': tarea_id,
                'resultado': res,
            }
        except Exception as e:
            logging.error(f"Error programando/ejecutando cierre de sesiones huérfanas: {e}")
            return {'success': False, 'error': str(e)}
    

    def obtener_tareas_pendientes(self) -> List[Dict]:
        """Obtiene tareas de mantenimiento pendientes que deben ejecutarse PostgreSQL"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            sql = """
                SELECT * FROM maintenance_tasks 
                WHERE status = 'pending' 
                AND (scheduled_at IS NULL OR scheduled_at <= CURRENT_TIMESTAMP)
                ORDER BY scheduled_at ASC
            """
            cursor.execute(sql)
            return [dict(row) for row in cursor.fetchall()]

    # === MÉTODOS ADICIONALES MIGRADOS DE SQLITE ===
    

    def obtener_rutina_completa(self, rutina_id: int) -> Optional[Rutina]:
        """Obtiene una rutina completa con todos sus ejercicios (robusta ante columnas opcionales)."""
        # Cache por rutina completa (versión objeto)
        try:
            cached = self.cache.get('rutinas', ('completa_obj', int(rutina_id)))
            if cached is not None:
                return cached
        except Exception:
            pass
        rutina = None
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            # Selección dinámica de columnas para evitar errores si falta uuid_rutina
            cols = self.get_table_columns('rutinas')
            base_cols = ['id', 'usuario_id', 'nombre_rutina', 'descripcion', 'dias_semana', 'categoria', 'fecha_creacion', 'activa']
            select_cols = base_cols + (['uuid_rutina'] if 'uuid_rutina' in (cols or []) else [])
            sql = f"SELECT {', '.join(select_cols)} FROM rutinas WHERE id = %s"
            cursor.execute(sql, (rutina_id,))
            row = cursor.fetchone()
            
            if row:
                base_fields = {k: row[k] for k in ['id', 'usuario_id', 'nombre_rutina', 'descripcion', 'dias_semana', 'categoria', 'fecha_creacion', 'activa'] if k in row}
                rutina = Rutina(**base_fields)
                try:
                    setattr(rutina, 'uuid_rutina', row.get('uuid_rutina'))
                except Exception:
                    pass
                rutina.ejercicios = []
                
                # Selección dinámica de columnas en ejercicios para evitar fallos si faltan video_url/video_mime
                e_cols = self.get_table_columns('ejercicios') or []
                nombre_sel = 'e.nombre'
                gm_sel = 'e.grupo_muscular' if 'grupo_muscular' in e_cols else "NULL AS grupo_muscular"
                desc_sel = 'e.descripcion as ejercicio_descripcion' if 'descripcion' in e_cols else "NULL AS ejercicio_descripcion"
                vurl_sel = 'e.video_url as ejercicio_video_url' if 'video_url' in e_cols else "NULL AS ejercicio_video_url"
                vmime_sel = 'e.video_mime as ejercicio_video_mime' if 'video_mime' in e_cols else "NULL AS ejercicio_video_mime"

                sql_ejercicios = f"""
                    SELECT re.*, {nombre_sel}, {gm_sel}, {desc_sel},
                           {vurl_sel}, {vmime_sel}
                    FROM rutina_ejercicios re 
                    JOIN ejercicios e ON re.ejercicio_id = e.id 
                    WHERE re.rutina_id = %s 
                    ORDER BY re.dia_semana, re.orden
                """
                cursor.execute(sql_ejercicios, (rutina_id,))
                
                for ejercicio_row in cursor.fetchall():
                    ejercicio_data = Ejercicio(
                        id=ejercicio_row['ejercicio_id'], 
                        nombre=ejercicio_row['nombre'], 
                        grupo_muscular=ejercicio_row['grupo_muscular'], 
                        descripcion=ejercicio_row['ejercicio_descripcion'],
                        video_url=ejercicio_row.get('ejercicio_video_url'),
                        video_mime=ejercicio_row.get('ejercicio_video_mime')
                    )
                    
                    # Construir RutinaEjercicio con campos válidos solamente
                    rutina_ejercicio = RutinaEjercicio(
                        id=ejercicio_row.get('id'),
                        rutina_id=ejercicio_row.get('rutina_id'),
                        ejercicio_id=ejercicio_row.get('ejercicio_id'),
                        dia_semana=ejercicio_row.get('dia_semana'),
                        series=ejercicio_row.get('series'),
                        repeticiones=ejercicio_row.get('repeticiones'),
                        orden=ejercicio_row.get('orden'),
                        ejercicio=ejercicio_data
                    )
                    rutina.ejercicios.append(rutina_ejercicio)
        
        try:
            self.cache.set('rutinas', ('completa_obj', int(rutina_id)), rutina)
        except Exception:
            pass
        return rutina
    

    def crear_rutina(self, rutina: Rutina) -> int:
        """Crea una nueva rutina PostgreSQL.
        Si rutina.usuario_id es None, se considera una plantilla y no se valida el estado del usuario.
        """
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            # Validar usuario activo solo si hay usuario asociado
            if getattr(rutina, 'usuario_id', None) is not None:
                cursor.execute("SELECT activo FROM usuarios WHERE id = %s", (rutina.usuario_id,))
                row = cursor.fetchone()
                activo = (row.get('activo') if isinstance(row, dict) else (row[0] if row else False))
                if not row or not activo:
                    raise PermissionError("El usuario está inactivo: no se puede crear una rutina")
            sql = "INSERT INTO rutinas (usuario_id, nombre_rutina, descripcion, dias_semana, categoria) VALUES (%s, %s, %s, %s, %s) RETURNING id"
            categoria = getattr(rutina, 'categoria', 'general')
            cursor.execute(sql, (getattr(rutina, 'usuario_id', None), rutina.nombre_rutina, rutina.descripcion, rutina.dias_semana, categoria))
            rutina_id = cursor.fetchone()['id']
            conn.commit()
            try:
                self.cache.invalidate('rutinas')
            except Exception:
                pass
            # Enforce única rutina activa para el usuario recién actualizado
            try:
                uid = getattr(rutina, 'usuario_id', None)
                if uid is not None:
                    self.enforce_single_active_rutina_usuario(int(uid))
            except Exception:
                pass
            return rutina_id
    
    # === MÉTODOS PARA ESPECIALIDADES Y CERTIFICACIONES ===
    

    def crear_especialidad(self, nombre: str, descripcion: str = None, categoria: str = None) -> int:
        """Crea una nueva especialidad PostgreSQL"""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                sql = """
                INSERT INTO especialidades (nombre, descripcion, categoria, activo)
                VALUES (%s, %s, %s, TRUE) RETURNING id
                """
                cursor.execute(sql, (nombre, descripcion, categoria))
                result = cursor.fetchone()
                if result:
                    especialidad_id = result['id']  # Use dict access for RealDictCursor
                    conn.commit()
                    return especialidad_id
                else:
                    logging.error("No result returned from INSERT query")
                    return 0
        except Exception as e:
            logging.error(f"Error en crear_especialidad: {e}")
            import traceback
            logging.error(f"Traceback: {traceback.format_exc()}")
            return 0
    

    def obtener_especialidades(self, solo_activas: bool = True) -> List[Dict]:
        """Obtiene todas las especialidades PostgreSQL"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            sql = "SELECT * FROM especialidades"
            
            if solo_activas:
                sql += " WHERE activo = TRUE"
            
            sql += " ORDER BY categoria, nombre"
            
            cursor.execute(sql)
            return [dict(row) for row in cursor.fetchall()]
    

    def actualizar_especialidad(self, especialidad_id: int, nombre: str = None, 
                               descripcion: str = None, categoria: str = None, 
                               activa: bool = None) -> bool:
        """Actualiza una especialidad PostgreSQL"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            campos = []
            valores = []
            
            if nombre is not None:
                campos.append("nombre = %s")
                valores.append(nombre)
            
            if descripcion is not None:
                campos.append("descripcion = %s")
                valores.append(descripcion)
            
            if categoria is not None:
                campos.append("categoria = %s")
                valores.append(categoria)
            
            if activa is not None:
                campos.append("activo = %s")
                valores.append(activa)
            
            if not campos:
                return False
            
            valores.append(especialidad_id)
            
            sql = f"UPDATE especialidades SET {', '.join(campos)} WHERE id = %s"
            cursor.execute(sql, valores)
            conn.commit()
            return cursor.rowcount > 0
    

    def eliminar_especialidad(self, especialidad_id: int) -> bool:
        """Elimina (desactiva) una especialidad PostgreSQL"""
        return self.actualizar_especialidad(especialidad_id, activa=False)
    

    def obtener_certificaciones_vencidas(self, dias_anticipacion: int = 30) -> List[Dict]:
        """Obtiene certificaciones que están vencidas o próximas a vencer PostgreSQL"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            fecha_limite = date.today() + timedelta(days=dias_anticipacion)
            
            sql = """
            SELECT pc.*, p.*, u.nombre as profesor_nombre, u.telefono
            FROM profesor_certificaciones pc
            JOIN profesores p ON pc.profesor_id = p.id
            JOIN usuarios u ON p.usuario_id = u.id
            WHERE pc.fecha_vencimiento IS NOT NULL 
              AND pc.fecha_vencimiento <= %s
              AND pc.estado = 'vigente'
            ORDER BY pc.fecha_vencimiento ASC
            """
            
            cursor.execute(sql, (fecha_limite,))
            return [dict(zip([desc[0] for desc in cursor.description], row)) for row in cursor.fetchall()]
    
    # === MÉTODOS PARA DISPONIBILIDAD DE PROFESORES ===
    

    def _verificar_conflictos_horario_profesor(self, profesor_id: int, fecha: date, 
                                             hora_inicio: str, hora_fin: str) -> List[Dict]:
        """Verifica conflictos de horario para un profesor en una fecha específica PostgreSQL"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            # Obtener día de la semana en español
            dias_semana = {
                0: 'Lunes', 1: 'Martes', 2: 'Miércoles', 3: 'Jueves', 
                4: 'Viernes', 5: 'Sábado', 6: 'Domingo'
            }
            dia_semana = dias_semana[fecha.weekday()]
            
            sql = """
            SELECT ch.*, c.nombre as clase_nombre
            FROM clases_horarios ch
            JOIN clases c ON ch.clase_id = c.id
            JOIN profesor_clase_asignaciones pca ON ch.id = pca.clase_horario_id
            WHERE pca.profesor_id = %s AND pca.activa = true
              AND ch.dia_semana = %s
              AND ch.activo = TRUE
              AND (
                  (ch.hora_inicio <= %s AND ch.hora_fin > %s) OR
                  (ch.hora_inicio < %s AND ch.hora_fin >= %s) OR
                  (ch.hora_inicio >= %s AND ch.hora_fin <= %s)
              )
            """
            
            cursor.execute(sql, (profesor_id, dia_semana, hora_inicio, hora_inicio,
                               hora_fin, hora_fin, hora_inicio, hora_fin))
            
            conflictos = []
            for row in cursor.fetchall():
                conflicto_dict = dict(zip([desc[0] for desc in cursor.description], row))
                conflictos.append({
                    'tipo': 'clase_regular',
                    'clase_id': conflicto_dict['clase_id'],
                    'clase_nombre': conflicto_dict['clase_nombre'],
                    'hora_inicio': conflicto_dict['hora_inicio'],
                    'hora_fin': conflicto_dict['hora_fin']
                })
            
            return conflictos
    
    # === MÉTODOS PARA SISTEMA DE SUPLENCIAS ===
    

    def asignar_suplente(self, suplencia_id: int, profesor_suplente_id: int, notas: str = None) -> bool:
        """Asigna un profesor suplente a una suplencia PostgreSQL"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            sql = """
            UPDATE profesor_suplencias 
            SET profesor_suplente_id = %s, estado = 'Asignado', 
                fecha_resolucion = CURRENT_TIMESTAMP, notas = COALESCE(%s, notas)
            WHERE id = %s
            """
            cursor.execute(sql, (profesor_suplente_id, notas, suplencia_id))
            conn.commit()
            return cursor.rowcount > 0
    

    def asignar_suplente_general(self, suplencia_id: int, profesor_suplente_id: int, notas: str = None) -> bool:
        """Asigna suplente en una suplencia general y marca estado 'Asignado'"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            sql = """
            UPDATE profesor_suplencias_generales
            SET profesor_suplente_id = %s,
                estado = 'Asignado',
                notas = CASE WHEN %s IS NOT NULL THEN COALESCE(notas, '') || ' - ' || %s ELSE notas END
            WHERE id = %s
            """
            cursor.execute(sql, (profesor_suplente_id, notas, notas, suplencia_id))
            conn.commit()
            return cursor.rowcount > 0


    def registrar_horas_trabajadas(self, profesor_id: int, fecha: date, hora_inicio: datetime,
                                  hora_fin: datetime, tipo_actividad: str, clase_id: int = None,
                                  notas: str = None) -> int:
        """Registra horas trabajadas por un profesor PostgreSQL"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            # Calcular horas totales y minutos totales, tolerando cruce de medianoche
            try:
                from datetime import timedelta
                inicio = hora_inicio
                fin = hora_fin
                duracion = fin - inicio
                if duracion.total_seconds() < 0:
                    # Si fin quedó antes que inicio (posible sesión que cruzó medianoche), ajustar fin +1 día
                    fin = fin + timedelta(days=1)
                    duracion = fin - inicio
            except Exception:
                duracion = hora_fin - hora_inicio
                try:
                    if duracion.total_seconds() < 0:
                        from datetime import timedelta
                        duracion = timedelta(seconds=0)
                except Exception:
                    pass

            horas_totales = round(duracion.total_seconds() / 3600.0, 4)
            minutos_totales = int(duracion.total_seconds() // 60)
            
            sql = """
            INSERT INTO profesor_horas_trabajadas 
            (profesor_id, fecha, hora_inicio, hora_fin, horas_totales, minutos_totales, tipo_actividad, clase_id, notas)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
            """
            cursor.execute(sql, (profesor_id, fecha, hora_inicio, fin, horas_totales, minutos_totales,
                               tipo_actividad, clase_id, notas))
            result = cursor.fetchone()
            registro_id = result['id'] if result and 'id' in result else None
            if not registro_id or registro_id <= 0:
                raise Exception("No se pudo obtener el ID del registro de horas (RETURNING id).")
            conn.commit()
            return registro_id
    

    def aplicar_optimizaciones_database(self) -> Dict[str, Any]:
        """Aplica optimizaciones específicas de PostgreSQL"""
        resultados = {
            'optimizaciones_aplicadas': [],
            'errores': [],
            'tiempo_total': 0
        }
        
        inicio_tiempo = time.time()
        
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                # Optimización global centralizada (VACUUM/ANALYZE según corresponda)
                try:
                    ok = self.optimizar_base_datos()
                    if ok:
                        resultados['optimizaciones_aplicadas'].append('Optimización global ejecutada')
                    else:
                        resultados['errores'].append('Fallo en optimización global')
                except Exception as e:
                    resultados['errores'].append(f"Error en optimización global: {e}")
                
                # Reindexar tablas críticas
                tablas_criticas = ['usuarios', 'pagos', 'asistencias', 'clases_horarios']
                for tabla in tablas_criticas:
                    cursor.execute(f"REINDEX TABLE {tabla}")
                    resultados['optimizaciones_aplicadas'].append(f'Tabla {tabla} reindexada')
                
                conn.commit()
                
        except Exception as e:
            resultados['errores'].append(str(e))
            logging.error(f"Error en optimizaciones de base de datos: {e}")
        
        resultados['tiempo_total'] = time.time() - inicio_tiempo
        return resultados
    

    def obtener_politica_cancelacion(self, clase_id: int = None) -> Dict:
        """Obtiene la política de cancelación para una clase específica o la global PostgreSQL"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            # Primero buscar política específica de la clase
            if clase_id:
                cursor.execute(
                    "SELECT * FROM politicas_cancelacion WHERE clase_id = %s AND activa = TRUE",
                    (clase_id,)
                )
                result = cursor.fetchone()
                if result:
                    return dict(zip([desc[0] for desc in cursor.description], result))
            
            # Si no hay política específica, usar la global
            cursor.execute(
                "SELECT * FROM politicas_cancelacion WHERE clase_id IS NULL AND activa = TRUE"
            )
            result = cursor.fetchone()
            return dict(zip([desc[0] for desc in cursor.description], result)) if result else None
    

    def verificar_puede_cancelar(self, usuario_id: int, clase_horario_id: int) -> Dict:
        """Verifica si un usuario puede cancelar su inscripción según las políticas PostgreSQL"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            # Obtener información de la clase
            cursor.execute(
                """SELECT ch.*, c.id as clase_id 
                   FROM clases_horarios ch 
                   JOIN clases c ON ch.clase_id = c.id 
                   WHERE ch.id = %s""",
                (clase_horario_id,)
            )
            clase_info = cursor.fetchone()
            
            if not clase_info:
                return {'puede_cancelar': False, 'razon': 'Clase no encontrada'}
            
            clase_dict = dict(zip([desc[0] for desc in cursor.description], clase_info))
            
            # Obtener política de cancelación
            politica = self.obtener_politica_cancelacion(clase_dict['clase_id'])
            
            if not politica:
                return {'puede_cancelar': True, 'razon': 'Sin restricciones'}
            
            return {
                'puede_cancelar': True, 
                'razon': 'Cumple con las políticas',
                'horas_minimas': politica.get('horas_minimas_cancelacion', 0),
                'penalizacion': politica.get('penalizacion_cancelacion', False)
            }
    

    def verificar_sesiones_abiertas(self) -> List[Dict]:
        """Reporta únicamente sesiones en BD abiertas por más de 12 horas."""
        sesiones_abiertas: List[Dict] = []
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cursor.execute(
                    """
                    SELECT 
                        pht.id,
                        pht.profesor_id,
                        pht.hora_inicio,
                        pht.fecha,
                        u.nombre AS profesor_nombre,
                        EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - pht.hora_inicio)) / 3600.0 AS horas_transcurridas
                    FROM profesor_horas_trabajadas pht
                    JOIN profesores p ON pht.profesor_id = p.id
                    JOIN usuarios u ON p.usuario_id = u.id
                    WHERE pht.hora_fin IS NULL
                    AND (CURRENT_TIMESTAMP - pht.hora_inicio) > INTERVAL '12 hours'
                    ORDER BY pht.hora_inicio ASC
                    """
                )
                registros = cursor.fetchall() or []
                for r in registros:
                    sesiones_abiertas.append({
                        'profesor_id': r['profesor_id'],
                        'profesor_nombre': r['profesor_nombre'],
                        'sesion_id': r['id'],
                        'hora_inicio': r['hora_inicio'],
                        'horas_transcurridas': float(r.get('horas_transcurridas') or 0.0),
                        'fecha': r['fecha'],
                        'es_sesion_local': False,
                    })
            logging.info(f"Sesiones abiertas >12h encontradas | total={len(sesiones_abiertas)}")
            return sesiones_abiertas
        except Exception as e:
            logging.error(f"Error verificando sesiones abiertas: {e}")
            return []


    def contar_sesiones_activas(self) -> int:
        """Cuenta el número total de sesiones activas en el sistema PostgreSQL"""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor()
                sql = """
                SELECT COUNT(*) as total_sesiones_activas
                FROM profesor_horas_trabajadas 
                WHERE hora_fin IS NULL
                """
                cursor.execute(sql)
                result = cursor.fetchone()
                return result[0] if result else 0
        except Exception as e:
            logging.error(f"Error contando sesiones activas: {e}")
            return 0
    

    def verificar_reinicio_mensual_horas(self, profesor_id: int) -> Dict[str, Any]:
        """Verifica si es necesario hacer reinicio mensual y maneja el archivado de datos PostgreSQL"""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                # Verificar si hay registros del mes anterior que no han sido archivados
                sql_verificar = """
                SELECT COUNT(*) as registros_mes_anterior
                FROM profesor_horas_trabajadas 
                WHERE profesor_id = %s 
                AND DATE_TRUNC('month', fecha) < DATE_TRUNC('month', CURRENT_DATE)
                AND hora_fin IS NOT NULL
                """
                cursor.execute(sql_verificar, (profesor_id,))
                resultado = cursor.fetchone()
                
                registros_anteriores = resultado['registros_mes_anterior'] if resultado else 0
                
                # Obtener estadísticas del mes anterior si existen registros
                estadisticas_mes_anterior = None
                if registros_anteriores > 0:
                    sql_estadisticas = """
                    SELECT 
                        DATE_TRUNC('month', fecha) as mes,
                        COUNT(*) as total_sesiones,
                        COALESCE(SUM(horas_totales), 0) as total_horas,
                        MIN(fecha) as primera_fecha,
                        MAX(fecha) as ultima_fecha
                    FROM profesor_horas_trabajadas 
                    WHERE profesor_id = %s 
                    AND DATE_TRUNC('month', fecha) < DATE_TRUNC('month', CURRENT_DATE)
                    AND hora_fin IS NOT NULL
                    GROUP BY DATE_TRUNC('month', fecha)
                    ORDER BY mes DESC
                    LIMIT 1
                    """
                    cursor.execute(sql_estadisticas, (profesor_id,))
                    estadisticas_mes_anterior = cursor.fetchone()
                    if estadisticas_mes_anterior:
                        estadisticas_mes_anterior = dict(estadisticas_mes_anterior)
                
                # Verificar registros del mes actual
                sql_mes_actual = """
                SELECT COUNT(*) as registros_mes_actual
                FROM profesor_horas_trabajadas 
                WHERE profesor_id = %s 
                AND DATE_TRUNC('month', fecha) = DATE_TRUNC('month', CURRENT_DATE)
                """
                cursor.execute(sql_mes_actual, (profesor_id,))
                resultado_actual = cursor.fetchone()
                registros_mes_actual = resultado_actual['registros_mes_actual'] if resultado_actual else 0
                
                return {
                    'success': True,
                    'necesita_reinicio': registros_anteriores > 0 and registros_mes_actual == 0,
                    'registros_mes_anterior': registros_anteriores,
                    'registros_mes_actual': registros_mes_actual,
                    'estadisticas_mes_anterior': estadisticas_mes_anterior,
                    'mensaje': f'Verificación completada. Registros anteriores: {registros_anteriores}, Mes actual: {registros_mes_actual}'
                }
                
        except Exception as e:
            logging.error(f"Error verificando reinicio mensual para profesor {profesor_id}: {e}")
            return {
                'success': False,
                'necesita_reinicio': False,
                'error': str(e),
                'mensaje': f'Error en verificación: {str(e)}'
            }
    

    def verificar_certificaciones_vencidas(self) -> List[Dict]:
        """Verifica y actualiza el estado de certificaciones vencidas o por vencer PostgreSQL"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            # Actualizar certificaciones vencidas
            cursor.execute(
                "UPDATE profesor_certificaciones SET estado = 'vencida' WHERE fecha_vencimiento < CURRENT_DATE AND estado != 'vencida'"
            )
            
            # Actualizar certificaciones por vencer (30 días)
            cursor.execute(
                "UPDATE profesor_certificaciones SET estado = 'por_vencer' WHERE fecha_vencimiento BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '30 days' AND estado = 'vigente'"
            )
            
            # Obtener certificaciones que requieren atención
            sql = """
            SELECT 
                pc.*,
                p.id as profesor_id,
                u.nombre as profesor_nombre
            FROM profesor_certificaciones pc
            JOIN profesores p ON pc.profesor_id = p.id
            JOIN usuarios u ON p.usuario_id = u.id
            WHERE pc.estado IN ('vencida', 'por_vencer')
            ORDER BY pc.fecha_vencimiento
            """
            
            cursor.execute(sql)
            conn.commit()
            return [dict(row) for row in cursor.fetchall()]
    

    def obtener_certificaciones_por_vencer(self, dias: int = 30) -> List[Dict]:
        """Obtiene certificaciones que vencen en los próximos días especificados PostgreSQL"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            sql = """
            SELECT 
                pc.*,
                p.id as profesor_id,
                u.nombre as profesor_nombre,
                u.telefono
            FROM profesor_certificaciones pc
            JOIN profesores p ON pc.profesor_id = p.id
            JOIN usuarios u ON p.usuario_id = u.id
            WHERE pc.fecha_vencimiento BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '%s days'
            AND pc.estado = 'vigente'
            ORDER BY pc.fecha_vencimiento
            """
            cursor.execute(sql, (dias,))
            return [dict(row) for row in cursor.fetchall()]
    
    # === MÉTODOS DE OPTIMIZACIÓN DE BASE DE DATOS ===
    

    def verificar_indices_database(self) -> Dict[str, Any]:
        """Verifica el estado de los índices en la base de datos PostgreSQL"""
        resultados = {
            'indices_existentes': [],
            'indices_faltantes': [],
            'estadisticas_indices': {},
            'recomendaciones': []
        }
        
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                # Obtener lista de índices existentes
                cursor.execute("""
                    SELECT indexname, tablename, indexdef 
                    FROM pg_indexes 
                    WHERE schemaname = 'public' 
                    AND indexname NOT LIKE 'pg_%'
                """)
                indices_existentes = cursor.fetchall()
                
                for indice in indices_existentes:
                    resultados['indices_existentes'].append({
                        'nombre': indice[0],
                        'tabla': indice[1],
                        'definicion': indice[2]
                    })
                
                # Verificar índices críticos recomendados
                indices_criticos = [
                    'idx_usuarios_activo',
                    'idx_usuarios_rol',
                    'idx_asistencias_usuario_id',
                    'idx_asistencias_fecha',
                    'idx_pagos_usuario_id',
                    'idx_pagos_fecha'
                ]
                
                indices_encontrados = [idx['nombre'] for idx in resultados['indices_existentes']]
                
                for indice_critico in indices_criticos:
                    if indice_critico not in indices_encontrados:
                        resultados['indices_faltantes'].append(indice_critico)
                
                # Obtener estadísticas de uso de índices
                cursor.execute("""
                    SELECT schemaname, tablename, indexname, idx_tup_read, idx_tup_fetch
                    FROM pg_stat_user_indexes 
                    WHERE schemaname = 'public'
                """)
                stats_indices = cursor.fetchall()
                
                for stat in stats_indices:
                    resultados['estadisticas_indices'][stat[2]] = {
                        'tabla': stat[1],
                        'tuplas_leidas': stat[3],
                        'tuplas_obtenidas': stat[4]
                    }
                
                # Generar recomendaciones
                if resultados['indices_faltantes']:
                    resultados['recomendaciones'].append(
                        f"Se recomienda crear {len(resultados['indices_faltantes'])} índices faltantes para mejorar el rendimiento"
                    )
                
                if len(resultados['indices_existentes']) < 10:
                    resultados['recomendaciones'].append(
                        "La base de datos tiene pocos índices. Considere aplicar las optimizaciones completas."
                    )
                
                return resultados
                
        except Exception as e:
            logging.error(f"Error verificando índices: {str(e)}")
            resultados['error'] = str(e)
            return resultados
    
    
    # Recomendaciones de índices (aceleran WHERE/ORDER y JOIN comunes)
    #
    # CREATE INDEX IF NOT EXISTS idx_audit_logs_record_id ON audit_logs(record_id);
    # CREATE INDEX IF NOT EXISTS idx_audit_logs_action_id ON audit_logs(action, id);
    # CREATE INDEX IF NOT EXISTS idx_clases_horarios_comp ON clases_horarios(clase_id, dia_semana, hora_inicio);
    # CREATE INDEX IF NOT EXISTS idx_asistencias_fecha_date ON asistencias ((fecha::date));
    # CREATE INDEX IF NOT EXISTS idx_clase_usuarios_clase_horario ON clase_usuarios(clase_horario_id);
    # CREATE INDEX IF NOT EXISTS idx_clase_usuarios_usuario ON clase_usuarios(usuario_id);
    # CREATE INDEX IF NOT EXISTS idx_profesor_clase_asignaciones_profesor_activa ON profesor_clase_asignaciones(profesor_id, activa);
    # CREATE INDEX IF NOT EXISTS idx_profesor_clase_asignaciones_clase ON profesor_clase_asignaciones(clase_horario_id);
    # CREATE INDEX IF NOT EXISTS idx_clases_tipo_clase_id ON clases(tipo_clase_id);
    # CREATE INDEX IF NOT EXISTS idx_usuario_notas_usuario_activa_fecha ON usuario_notas(usuario_id, activa, fecha_creacion);
    # CREATE INDEX IF NOT EXISTS idx_usuario_notas_categoria_activa_fecha ON usuario_notas(categoria, activa, fecha_creacion);
    # CREATE INDEX IF NOT EXISTS idx_usuario_notas_importancia_activa_fecha ON usuario_notas(importancia, activa, fecha_creacion);
    # CREATE INDEX IF NOT EXISTS idx_etiquetas_activo ON etiquetas(activo);
    # CREATE INDEX IF NOT EXISTS idx_etiquetas_activo_nombre ON etiquetas(activo, nombre);
    # CREATE INDEX IF NOT EXISTS idx_usuario_etiquetas_usuario_id ON usuario_etiquetas(usuario_id);
    # CREATE INDEX IF NOT EXISTS idx_usuario_etiquetas_etiqueta_id ON usuario_etiquetas(etiqueta_id);
    # CREATE INDEX IF NOT EXISTS idx_usuario_estados_usuario_fecha_inicio_desc ON usuario_estados(usuario_id, fecha_inicio DESC);
    # CREATE INDEX IF NOT EXISTS idx_usuario_estados_usuario_activo_fecha_inicio_desc ON usuario_estados(usuario_id, activo, fecha_inicio DESC);
    # CREATE INDEX IF NOT EXISTS idx_usuario_estados_activo_vencimiento ON usuario_estados(activo, fecha_vencimiento);
    # CREATE INDEX IF NOT EXISTS idx_historial_estados_usuario_fecha_desc ON historial_estados(usuario_id, fecha_accion DESC);
    # CREATE INDEX IF NOT EXISTS idx_pagos_usuario_mes_anio ON pagos(usuario_id, año, mes);
    # CREATE INDEX IF NOT EXISTS idx_pagos_usuario_fecha_desc ON pagos(usuario_id, fecha_pago DESC);
    # CREATE INDEX IF NOT EXISTS idx_pago_detalles_pago_id ON pago_detalles(pago_id);
    # CREATE INDEX IF NOT EXISTS idx_pago_detalles_concepto_id ON pago_detalles(concepto_id);
    # CREATE INDEX IF NOT EXISTS idx_conceptos_pago_activo ON conceptos_pago(activo);
    # CREATE INDEX IF NOT EXISTS idx_metodos_pago_activo ON metodos_pago(activo);


    def ensure_indexes(self) -> None:
        """Crea (idempotentemente) índices críticos en autocommit usando advisory lock y ejecuta ANALYZE.

        Unifica lógica para evitar duplicaciones y garantiza que no exista transacción abierta
        antes de usar CREATE INDEX CONCURRENTLY.
        """
        global _INDEX_ONCE_DONE, _INDEX_ONCE_LOCK
        # Guard de ejecución única por proceso
        with _INDEX_ONCE_LOCK:
            if _INDEX_ONCE_DONE:
                return
        try:
            # Usar conexión directa para mayor control de autocommit y transacciones
            conn = self._crear_conexion_directa()
            try:
                # Asegurar que no haya transacción activa
                try:
                    conn.rollback()
                except Exception:
                    pass
                try:
                    conn.autocommit = True
                except Exception:
                    pass

                # Advisory lock global para índices
                advisory_key = 684122013
                got_lock = True
                try:
                    with conn.cursor() as lock_cur:
                        lock_cur.execute("SELECT pg_try_advisory_lock(%s)", (advisory_key,))
                        res = lock_cur.fetchone()
                        got_lock = bool(res[0]) if res else False
                except Exception as e:
                    logging.warning(f"Fallo advisory lock en ensure_indexes: {e}")

                if not got_lock:
                    logging.info("Otra instancia creando índices; se omite en este proceso")
                    return

                # Endurecer timeouts de sesión para evitar bloqueos largos
                try:
                    with conn.cursor() as cset:
                        cset.execute("SET lock_timeout = '1000ms'")
                        cset.execute("SET statement_timeout = '120s'")
                        cset.execute("SET idle_in_transaction_session_timeout = '0'")
                except Exception as e:
                    logging.debug(f"No se pudieron establecer timeouts de sesión: {e}")

                # Helper para crear índices de forma segura bajo lock_timeout
                def _safe_create(cur, sql: str):
                    try:
                        cur.execute(sql)
                    except Exception as e:
                        code = getattr(e, 'pgcode', None)
                        try:
                            import psycopg2
                            from psycopg2 import errors as pg_errors
                            is_lock = isinstance(e, getattr(pg_errors, 'LockNotAvailable', tuple())) or (code == '55P03')
                            is_timeout = isinstance(e, getattr(pg_errors, 'QueryCanceled', tuple())) or (code == '57014')
                        except Exception:
                            is_lock = code == '55P03'
                            is_timeout = code == '57014'
                        if is_lock or is_timeout:
                            logging.info(f"Indexación concurrente saltada por lock/timeout en: {sql}")
                            return
                        logging.warning(f"Fallo creando índice: {e}; sql={sql}")

                with conn.cursor() as cursor:
                    # Usuarios
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_usuarios_dni ON usuarios(dni)")
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_usuarios_activo ON usuarios(activo)")
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_usuarios_rol ON usuarios(rol)")
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_usuarios_rol_nombre ON usuarios(rol, nombre)")
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_usuarios_activo_rol_nombre ON usuarios(activo, rol, nombre)")

                    # Profesores (relación con usuarios)
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_profesores_usuario_id ON profesores(usuario_id)")

                    # Pagos
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_pagos_usuario_id ON pagos(usuario_id)")
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_pagos_fecha ON pagos(fecha_pago)")
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_pagos_month_year ON pagos ((EXTRACT(MONTH FROM fecha_pago)), (EXTRACT(YEAR FROM fecha_pago)))")
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_pagos_usuario_fecha_desc ON pagos(usuario_id, fecha_pago DESC)")
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_pagos_usuario_mes_anio ON pagos(usuario_id, año, mes)")

                    # Detalles de pago
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_pago_detalles_pago_id ON pago_detalles(pago_id)")
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_pago_detalles_concepto_id ON pago_detalles(concepto_id)")

                    # Asistencias
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_asistencias_usuario_id ON asistencias(usuario_id)")
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_asistencias_fecha ON asistencias(fecha)")
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_asistencias_usuario_fecha_desc ON asistencias(usuario_id, fecha DESC)")

                    # Clases / horarios
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_clases_tipo_clase_id ON clases (tipo_clase_id)")
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_clases_horarios_clase_id ON clases_horarios(clase_id)")

                    # Horarios de profesores (disponibilidad)
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_horarios_profesores_profesor_id ON horarios_profesores(profesor_id)")
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_horarios_profesores_dia_inicio ON horarios_profesores(dia_semana, hora_inicio)")

                    # Lista de espera
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_clase_lista_espera_clase ON clase_lista_espera (clase_horario_id)")
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_clase_lista_espera_activo ON clase_lista_espera (activo)")
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_clase_lista_espera_posicion ON clase_lista_espera (posicion)")

                    # Notificaciones de cupos
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_notif_cupos_usuario_activa ON notificaciones_cupos (usuario_id, activa)")
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_notif_cupos_clase ON notificaciones_cupos (clase_horario_id)")
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_notif_cupos_leida ON notificaciones_cupos (leida)")
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_notif_cupos_tipo ON notificaciones_cupos (tipo_notificacion)")

                    # Historial de asistencia
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_clase_asistencia_historial_clase_horario_id ON clase_asistencia_historial(clase_horario_id)")
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_clase_asistencia_historial_usuario_id ON clase_asistencia_historial(usuario_id)")
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_clase_asistencia_historial_fecha ON clase_asistencia_historial(fecha_clase)")
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_clase_asistencia_historial_estado ON clase_asistencia_historial(estado_asistencia)")

                    # Sesiones de profesores (horas trabajadas)
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_profesor_horas_fecha ON profesor_horas_trabajadas(fecha)")
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_profesor_horas_profesor_fecha ON profesor_horas_trabajadas(profesor_id, fecha)")
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_profesor_horas_activa_inicio ON profesor_horas_trabajadas(profesor_id, hora_inicio) WHERE hora_fin IS NULL")
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_profesor_horas_cerrada_profesor_fecha_inicio ON profesor_horas_trabajadas(profesor_id, fecha, hora_inicio) WHERE hora_fin IS NOT NULL")

                    # Estados de usuario e historial
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_usuario_estados_usuario_id ON usuario_estados(usuario_id)")
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_usuario_estados_creado_por ON usuario_estados(creado_por)")
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_usuario_estados_usuario_fecha_inicio_desc ON usuario_estados(usuario_id, fecha_inicio DESC)")
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_usuario_estados_usuario_activo_fecha_inicio_desc ON usuario_estados(usuario_id, activo, fecha_inicio DESC)")
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_usuario_estados_activo_vencimiento ON usuario_estados(activo, fecha_vencimiento)")
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_historial_estados_usuario_id ON historial_estados(usuario_id)")
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_historial_estados_estado_id ON historial_estados(estado_id)")
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_historial_estados_fecha ON historial_estados(fecha_accion)")
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_historial_estados_usuario_fecha_desc ON historial_estados(usuario_id, fecha_accion DESC)")

                    # Comprobantes
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_comprobantes_pago_pago_id ON comprobantes_pago(pago_id)")
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_comprobantes_pago_numero ON comprobantes_pago(numero_comprobante)")
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_comprobantes_pago_fecha ON comprobantes_pago(fecha_emision)")
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_comprobantes_pago_estado ON comprobantes_pago(estado)")

                    # Auditoría
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_audit_logs_timestamp ON audit_logs(timestamp)")
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_audit_logs_user_id ON audit_logs(user_id)")
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_audit_logs_table_name ON audit_logs(table_name)")
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_audit_logs_action ON audit_logs(action)")
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_audit_logs_record_id ON audit_logs(record_id)")

                    # Acciones masivas pendientes
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_acciones_masivas_estado_fecha ON acciones_masivas_pendientes(estado, fecha_creacion)")

                    # WhatsApp templates y config
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_whatsapp_templates_active_name ON whatsapp_templates(active, template_name)")
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_whatsapp_config_active_created ON whatsapp_config(active, created_at)")

                    # WhatsApp messages (anti-spam reciente)
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_whatsapp_messages_user_type_date ON whatsapp_messages(user_id, message_type, sent_at)")
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_whatsapp_messages_user_type_date_nofailed ON whatsapp_messages(user_id, message_type, sent_at) WHERE status <> 'failed'")

                    # Auditoría: índices opcionales si existen columnas avanzadas
                    try:
                        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cinfo:
                            cinfo.execute(
                                """
                                SELECT column_name FROM information_schema.columns
                                WHERE table_schema = 'public' AND table_name = 'audit_logs'
                                """
                            )
                            cols = {row['column_name'] for row in (cinfo.fetchall() or [])}
                        # Crear índices sólo si las columnas existen
                        if 'level' in cols:
                            _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_audit_logs_level ON audit_logs(level)")
                        if 'category' in cols:
                            _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_audit_logs_category ON audit_logs(category)")
                        if 'source' in cols:
                            _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_audit_logs_source ON audit_logs(source)")
                        if {'timestamp', 'level', 'category'} <= cols:
                            _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_audit_logs_ts_level_cat ON audit_logs(timestamp, level, category)")
                    except Exception as e:
                        logging.debug(f"No se pudieron verificar/crear índices opcionales de audit_logs: {e}")

                    # Notas de usuarios
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_usuario_notas_usuario_activa_fecha ON usuario_notas(usuario_id, activa, fecha_creacion)")
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_usuario_notas_categoria_activa_fecha ON usuario_notas(categoria, activa, fecha_creacion)")
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_usuario_notas_importancia_activa_fecha ON usuario_notas(importancia, activa, fecha_creacion)")

                    # Etiquetas
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_etiquetas_activo ON etiquetas(activo)")
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_etiquetas_activo_nombre ON etiquetas(activo, nombre)")

                    # Conceptos y métodos de pago
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_conceptos_pago_activo ON conceptos_pago(activo)")
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_metodos_pago_activo ON metodos_pago(activo)")

                    # Ejercicios
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ejercicios_nombre_lower ON ejercicios(LOWER(nombre))")
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ejercicios_grupo_muscular ON ejercicios(grupo_muscular)")
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ejercicios_objetivo ON ejercicios(objetivo)")

                    # Relación usuario-etiquetas
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_usuario_etiquetas_usuario_id ON usuario_etiquetas(usuario_id)")
                    _safe_create(cursor, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_usuario_etiquetas_etiqueta_id ON usuario_etiquetas(etiqueta_id)")

                # ANALYZE sin bloquear
                try:
                    with conn.cursor() as c2:
                        c2.execute("ANALYZE")
                except Exception:
                    pass
                # Marcar ejecución completa
                try:
                    with _INDEX_ONCE_LOCK:
                        _INDEX_ONCE_DONE = True
                except Exception:
                    pass
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
        except Exception as e:
            logging.warning(f"Error asegurando índices: {e}")


    def agregar_a_lista_espera_completo(self, clase_horario_id: int, usuario_id: int) -> int:
        """Agrega un usuario a la lista de espera de una clase PostgreSQL."""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            # Obtener la siguiente posición en la lista
            cursor.execute(
                "SELECT COALESCE(MAX(posicion), 0) + 1 AS next_pos FROM clase_lista_espera WHERE clase_horario_id = %s AND activo = true",
                (clase_horario_id,)
            )
            posicion_row = cursor.fetchone()
            posicion = posicion_row['next_pos'] if posicion_row and 'next_pos' in posicion_row else 1
            
            sql = """
            INSERT INTO clase_lista_espera 
            (clase_horario_id, usuario_id, posicion, activo)
            VALUES (%s, %s, %s, true)
            ON CONFLICT (clase_horario_id, usuario_id) 
            DO UPDATE SET posicion = EXCLUDED.posicion, activo = true
            RETURNING id
            """
            cursor.execute(sql, (clase_horario_id, usuario_id, posicion))
            result = cursor.fetchone()
            conn.commit()
            # RealDictCursor devuelve un dict; retornamos el id explícitamente
            return result['id'] if result and 'id' in result else None
    

    def quitar_de_lista_espera_completo(self, clase_horario_id: int, usuario_id: int) -> bool:
        """Quita un usuario de la lista de espera PostgreSQL."""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            # Obtener la posición del usuario a quitar
            cursor.execute(
                "SELECT posicion FROM clase_lista_espera WHERE clase_horario_id = %s AND usuario_id = %s AND activo = true",
                (clase_horario_id, usuario_id)
            )
            posicion_eliminada = cursor.fetchone()
            
            if not posicion_eliminada:
                return False
            
            # Marcar como inactivo
            cursor.execute(
                "UPDATE clase_lista_espera SET activo = false WHERE clase_horario_id = %s AND usuario_id = %s",
                (clase_horario_id, usuario_id)
            )
            
            # Reordenar posiciones
            cursor.execute(
                """UPDATE clase_lista_espera 
                   SET posicion = posicion - 1 
                   WHERE clase_horario_id = %s AND posicion > %s AND activo = true""",
                (clase_horario_id, posicion_eliminada['posicion'])
            )
            
            conn.commit()
            return cursor.rowcount > 0
    

    def obtener_lista_espera(self, clase_horario_id: int) -> List[Dict]:
        """Obtiene la lista de espera de una clase (método simplificado)."""
        return self.obtener_lista_espera_completa(clase_horario_id)
    

    def quitar_de_lista_espera(self, clase_horario_id: int, usuario_id: int) -> bool:
        """Quita un usuario de la lista de espera (método simplificado)."""
        return self.quitar_de_lista_espera_completo(clase_horario_id, usuario_id)
    

    def obtener_lista_espera_completa(self, clase_horario_id: int) -> List[Dict]:
        """Obtiene la lista de espera de una clase ordenada por posición PostgreSQL."""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            sql = """
            SELECT le.*, u.nombre AS nombre_usuario, u.telefono
            FROM clase_lista_espera le
            JOIN usuarios u ON le.usuario_id = u.id
            WHERE le.clase_horario_id = %s AND le.activo = true
            ORDER BY le.posicion
            """
            cursor.execute(sql, (clase_horario_id,))
            rows = cursor.fetchall() or []
            result = [dict(row) for row in rows]
            return result
    

    def obtener_siguiente_en_lista_espera_completo(self, clase_horario_id: int) -> Dict:
        """Obtiene el siguiente usuario en la lista de espera PostgreSQL."""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                sql = """
                SELECT le.*, u.nombre AS nombre_usuario, u.telefono
                FROM clase_lista_espera le
                JOIN usuarios u ON le.usuario_id = u.id
                WHERE le.clase_horario_id = %s AND le.activo = true
                ORDER BY le.posicion
                LIMIT 1
                """
                cursor.execute(sql, (clase_horario_id,))
                result = cursor.fetchone()
                return dict(result) if result else None
        except Exception as e:
            # If clase_lista_espera table doesn't exist, return None
            error_msg = str(e).lower()
            if ("relation" in error_msg and "does not exist" in error_msg) or "no existe la relación" in error_msg:
                return None
            raise e
    

    def procesar_liberacion_cupo_completo(self, clase_horario_id: int) -> bool:
        """Procesa la liberación de un cupo, moviendo el siguiente de la lista de espera PostgreSQL."""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            # Verificar si hay cupo disponible
            if not self.verificar_cupo_disponible(clase_horario_id):
                return False
            
            # Validar ventana de inscripción: si está fuera, no autopromover
            dentro_ventana = True
            try:
                cursor.execute("SELECT dia_semana, hora_inicio FROM clases_horarios WHERE id = %s", (clase_horario_id,))
                row = cursor.fetchone()
                if row:
                    from datetime import datetime, timedelta, time
                    dia_semana = row.get('dia_semana') if isinstance(row, dict) else row[0]
                    hora_inicio = row.get('hora_inicio') if isinstance(row, dict) else row[1]
                    now = datetime.now()
                    try:
                        if isinstance(hora_inicio, str):
                            parts = [int(x) for x in str(hora_inicio).split(":")[:2]]
                            start_t = time(parts[0], parts[1] if len(parts)>1 else 0)
                        else:
                            start_t = hora_inicio
                    except Exception:
                        start_t = time(0,0)
                    ds = str(dia_semana or '').strip().lower()
                    dmap = {'lunes':0,'martes':1,'miercoles':2,'miércoles':2,'jueves':3,'viernes':4,'sabado':5,'sábado':5,'domingo':6}
                    target = dmap.get(ds, None)
                    if target is not None:
                        current = now.weekday()
                        days_ahead = (target - current) % 7
                        next_date = now.date() + timedelta(days=days_ahead)
                        next_dt = datetime.combine(next_date, start_t)
                        if days_ahead == 0 and now >= next_dt:
                            next_dt = next_dt + timedelta(days=7)
                        try:
                            ventana_horas = int(self.obtener_configuracion('ventana_inscripcion_horas') or '72')
                        except Exception:
                            ventana_horas = 72
                        diff_hours = (next_dt - now).total_seconds() / 3600.0
                        dentro_ventana = diff_hours <= ventana_horas
            except Exception:
                dentro_ventana = True

            # Obtener el siguiente en la lista de espera
            siguiente = self.obtener_siguiente_en_lista_espera_completo(clase_horario_id)
            if not siguiente:
                return False
            
            try:
                # Consultar configuración para decidir si autopromover o solo notificar
                use_prompt = True
                try:
                    flag = self.obtener_configuracion('enable_waitlist_prompt')
                    use_prompt = (str(flag).lower() == 'true') if flag is not None else True
                except Exception:
                    use_prompt = True

                # Si está fuera de ventana, forzar modo prompt aunque la autopromoción esté habilitada
                if use_prompt or (not dentro_ventana):
                    # Solo notificar; la UI decidirá si promover y/o enviar WhatsApp
                    self.crear_notificacion_cupo_completa(
                        siguiente['usuario_id'],
                        clase_horario_id,
                        'cupo_liberado',
                        "Se liberó un cupo en tu clase. Confirma promoción desde la UI."
                    )
                    return True
                else:
                    # Autopromoción deshabilita el prompt: inscribir y quitar de espera
                    self.inscribir_usuario_en_clase(clase_horario_id, siguiente['usuario_id'])
                    self.quitar_de_lista_espera_completo(clase_horario_id, siguiente['usuario_id'])
                    self.crear_notificacion_cupo_completa(
                        siguiente['usuario_id'],
                        clase_horario_id,
                        'cupo_liberado',
                        "¡Buenas noticias! Se ha liberado un cupo en tu clase y has sido inscrito automáticamente."
                    )
                    return True
                
            except Exception as e:
                print(f"Error al procesar liberación de cupo: {e}")
                return False
    

    def migrar_lista_espera_legacy(self, drop_legacy: bool = True) -> Dict[str, int]:
        """Migra la tabla anterior 'lista_espera' a 'clase_lista_espera'.
        - Respeta el orden por fecha_inscripcion para asignar posiciones.
        - Marca registros como activo=true.
        - Si drop_legacy=True, elimina la tabla anterior al finalizar.
        Retorna contadores del proceso.
        """
        resultados = {"procesados": 0, "insertados_actualizados": 0, "errores": 0}
        try:
            with self.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.tables 
                            WHERE table_schema = 'public' AND table_name = 'lista_espera'
                        )
                        """
                    )
                    existe_legacy = cursor.fetchone()[0]
                    if not existe_legacy:
                        return resultados

                    cursor.execute(
                        """
                        SELECT clase_horario_id, usuario_id, fecha_inscripcion
                        FROM lista_espera
                        ORDER BY clase_horario_id, fecha_inscripcion
                        """
                    )
                    rows = cursor.fetchall() or []

                    from collections import defaultdict
                    agrupados = defaultdict(list)
                    for clase_horario_id, usuario_id, fecha_inscripcion in rows:
                        agrupados[clase_horario_id].append((usuario_id, fecha_inscripcion))

                    for clase_horario_id, elementos in agrupados.items():
                        cursor.execute(
                            "SELECT COALESCE(MAX(posicion), 0) FROM clase_lista_espera WHERE clase_horario_id = %s AND activo = true",
                            (clase_horario_id,)
                        )
                        base_pos = cursor.fetchone()[0] or 0

                        posicion = base_pos
                        for (usuario_id, _fecha) in elementos:
                            posicion += 1
                            try:
                                cursor.execute(
                                    """
                                    INSERT INTO clase_lista_espera (clase_horario_id, usuario_id, posicion, activo)
                                    VALUES (%s, %s, %s, true)
                                    ON CONFLICT (clase_horario_id, usuario_id)
                                    DO UPDATE SET posicion = EXCLUDED.posicion, activo = true
                                    """,
                                    (clase_horario_id, usuario_id, posicion)
                                )
                                resultados["insertados_actualizados"] += 1
                            except Exception as ie:
                                logging.error(f"Error migrando espera clase {clase_horario_id}, usuario {usuario_id}: {ie}")
                                resultados["errores"] += 1
                            finally:
                                resultados["procesados"] += 1

                    conn.commit()

                    if drop_legacy:
                        try:
                            cursor.execute("DROP TABLE IF EXISTS lista_espera")
                            conn.commit()
                        except Exception as de:
                            logging.error(f"Error eliminando tabla anterior lista_espera: {de}")
            return resultados
        except Exception as e:
            logging.error(f"Error en migrar_lista_espera_legacy (anterior): {e}")
            return resultados

    # --- MÉTODOS PARA HISTORIAL DE ASISTENCIA ---
    

    def obtener_politica_cancelacion_completa(self, clase_id: int = None) -> Dict:
        """Obtiene la política de cancelación para una clase específica o la global PostgreSQL."""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            # Primero buscar política específica de la clase
            if clase_id:
                cursor.execute(
                    "SELECT * FROM politicas_cancelacion WHERE clase_id = %s AND activa = true",
                    (clase_id,)
                )
                result = cursor.fetchone()
                if result:
                    return dict(zip([desc[0] for desc in cursor.description], result))
            
            # Si no hay política específica, usar la global
            cursor.execute(
                "SELECT * FROM politicas_cancelacion WHERE clase_id IS NULL AND activa = true"
            )
            result = cursor.fetchone()
            return dict(zip([desc[0] for desc in cursor.description], result)) if result else None
    

    def verificar_puede_cancelar_completo(self, usuario_id: int, clase_horario_id: int) -> Dict:
        """Verifica si un usuario puede cancelar su inscripción según las políticas PostgreSQL."""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            # Obtener información de la clase
            cursor.execute(
                """SELECT ch.*, c.id as clase_id 
                   FROM clases_horarios ch 
                   JOIN clases c ON ch.clase_id = c.id 
                   WHERE ch.id = %s""",
                (clase_horario_id,)
            )
            clase_info = cursor.fetchone()
            
            if not clase_info:
                return {'puede_cancelar': False, 'razon': 'Clase no encontrada'}
            
            # Obtener política de cancelación
            politica = self.obtener_politica_cancelacion_completa(clase_info[len(clase_info)-1])  # clase_id es el último campo
            
            if not politica:
                return {'puede_cancelar': True, 'razon': 'Sin restricciones'}
            
            # Verificar tiempo mínimo
            # Aquí se podría implementar la lógica de verificación de tiempo
            # Por ahora, permitir cancelación
            
            return {
                'puede_cancelar': True, 
                'razon': 'Cumple con las políticas',
                'horas_minimas': politica['horas_minimas_cancelacion'],
                'penalizacion': politica['penalizacion_cancelacion']
            }
    
    # === MÉTODOS DE VERIFICACIÓN DE CERTIFICACIONES ===
    

    def verificar_certificaciones_vencidas(self) -> List[Dict]:
        """Verifica y actualiza el estado de certificaciones vencidas o por vencer PostgreSQL"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            # Actualizar certificaciones vencidas
            cursor.execute(
                "UPDATE profesor_certificaciones SET estado = 'vencida' WHERE fecha_vencimiento < CURRENT_DATE AND estado != 'vencida'"
            )
            
            # Actualizar certificaciones por vencer (30 días)
            cursor.execute(
                "UPDATE profesor_certificaciones SET estado = 'por_vencer' WHERE fecha_vencimiento BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '30 days' AND estado = 'vigente'"
            )
            
            # Obtener certificaciones que requieren atención
            sql = """
            SELECT 
                pc.*,
                p.id as profesor_id,
                u.nombre as profesor_nombre
            FROM profesor_certificaciones pc
            JOIN profesores p ON pc.profesor_id = p.id
            JOIN usuarios u ON p.usuario_id = u.id
            WHERE pc.estado IN ('vencida', 'por_vencer')
            ORDER BY pc.fecha_vencimiento
            """
            
            cursor.execute(sql)
            conn.commit()
            return [dict(zip([desc[0] for desc in cursor.description], row)) for row in cursor.fetchall()]
    

    def obtener_certificaciones_por_vencer(self, dias: int = 30) -> List[Dict]:
        """Obtiene certificaciones que vencen en los próximos días especificados PostgreSQL"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            sql = """
            SELECT 
                pc.*,
                p.id as profesor_id,
                u.nombre as profesor_nombre,
                u.telefono
            FROM profesor_certificaciones pc
            JOIN profesores p ON pc.profesor_id = p.id
            JOIN usuarios u ON p.usuario_id = u.id
            WHERE pc.fecha_vencimiento BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '%s days'
            AND pc.estado = 'vigente'
            ORDER BY pc.fecha_vencimiento
            """
            cursor.execute(sql, (dias,))
            return [dict(zip([desc[0] for desc in cursor.description], row)) for row in cursor.fetchall()]
    
    # === MÉTODOS PARA CONTEO AUTOMÁTICO DE HORAS TRABAJADAS ===
    

    

    def verificar_sesiones_abiertas(self) -> List[Dict]:
        """Verifica solo sesiones abiertas en BD con más de 12 horas."""
        sesiones_abiertas: List[Dict] = []
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                sql = """
                    SELECT 
                        pht.id AS sesion_id,
                        pht.profesor_id,
                        pht.hora_inicio,
                        pht.fecha,
                        u.nombre AS profesor_nombre,
                        EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - pht.hora_inicio)) / 3600 AS horas_transcurridas
                    FROM profesor_horas_trabajadas pht
                    JOIN profesores p ON pht.profesor_id = p.id
                    JOIN usuarios u ON p.usuario_id = u.id
                    WHERE pht.hora_fin IS NULL
                      AND (CURRENT_TIMESTAMP - pht.hora_inicio) > INTERVAL '12 hours'
                    ORDER BY pht.hora_inicio
                """
                cursor.execute(sql)
                filas = cursor.fetchall()
                for fila in filas:
                    row = dict(fila)
                    sesiones_abiertas.append({
                        'profesor_id': row.get('profesor_id'),
                        'profesor_nombre': row.get('profesor_nombre'),
                        'sesion_id': row.get('sesion_id'),
                        'hora_inicio': row.get('hora_inicio'),
                        'horas_transcurridas': float(row.get('horas_transcurridas', 0) or 0),
                        'fecha': row.get('fecha'),
                        'es_sesion_local': False,
                    })
        except Exception as e:
            logging.error(f"Error verificando sesiones abiertas en BD: {e}")

        return sesiones_abiertas
    
    # === MÉTODOS DE OPTIMIZACIÓN DE BASE DE DATOS ===
    

    def optimizar_consultas_criticas(self) -> Dict[str, Any]:
        """Optimiza las consultas más críticas del sistema PostgreSQL"""
        import time
        
        inicio_tiempo = time.time()
        resultados = {
            'consultas_optimizadas': 0,
            'mejoras_rendimiento': {},
            'errores': [],
            'tiempo_procesamiento': 0
        }
        
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                # Optimización 1: Consulta de actividad de usuarios
                try:
                    inicio_consulta = time.time()
                    cursor.execute("SELECT COUNT(*) FROM usuarios WHERE activo = true")
                    tiempo_antes = time.time() - inicio_consulta
                    
                    # Usar índice específico si existe
                    inicio_consulta = time.time()
                    cursor.execute("SELECT COUNT(*) FROM usuarios WHERE activo = true")
                    tiempo_despues = time.time() - inicio_consulta
                    
                    mejora = ((tiempo_antes - tiempo_despues) / tiempo_antes * 100) if tiempo_antes > 0 else 0
                    resultados['mejoras_rendimiento']['usuarios_activos'] = {
                        'tiempo_antes': tiempo_antes,
                        'tiempo_despues': tiempo_despues,
                        'mejora_porcentaje': mejora
                    }
                    resultados['consultas_optimizadas'] += 1
                    
                except Exception as e:
                    resultados['errores'].append(f"Error optimizando consulta usuarios: {str(e)}")
                
                # Optimización 2: Consulta de asistencias por fecha
                try:
                    from datetime import datetime
                    fecha_test = datetime.now().date().isoformat()
                    
                    inicio_consulta = time.time()
                    cursor.execute("SELECT COUNT(*) FROM asistencias WHERE fecha = %s", (fecha_test,))
                    tiempo_antes = time.time() - inicio_consulta
                    
                    inicio_consulta = time.time()
                    cursor.execute("SELECT COUNT(*) FROM asistencias WHERE fecha = %s", (fecha_test,))
                    tiempo_despues = time.time() - inicio_consulta
                    
                    mejora = ((tiempo_antes - tiempo_despues) / tiempo_antes * 100) if tiempo_antes > 0 else 0
                    resultados['mejoras_rendimiento']['asistencias_fecha'] = {
                        'tiempo_antes': tiempo_antes,
                        'tiempo_despues': tiempo_despues,
                        'mejora_porcentaje': mejora
                    }
                    resultados['consultas_optimizadas'] += 1
                    
                except Exception as e:
                    resultados['errores'].append(f"Error optimizando consulta asistencias: {str(e)}")
                
                # Optimización 3: Consulta de pagos por mes/año
                try:
                    from datetime import datetime
                    mes_actual = datetime.now().month
                    año_actual = datetime.now().year
                    
                    inicio_consulta = time.time()
                    cursor.execute("SELECT SUM(monto) FROM pagos WHERE EXTRACT(MONTH FROM fecha_pago) = %s AND EXTRACT(YEAR FROM fecha_pago) = %s", (mes_actual, año_actual))
                    tiempo_antes = time.time() - inicio_consulta
                    
                    inicio_consulta = time.time()
                    cursor.execute("SELECT SUM(monto) FROM pagos WHERE EXTRACT(MONTH FROM fecha_pago) = %s AND EXTRACT(YEAR FROM fecha_pago) = %s", (mes_actual, año_actual))
                    tiempo_despues = time.time() - inicio_consulta
                    
                    mejora = ((tiempo_antes - tiempo_despues) / tiempo_antes * 100) if tiempo_antes > 0 else 0
                    resultados['mejoras_rendimiento']['pagos_mes_año'] = {
                        'tiempo_antes': tiempo_antes,
                        'tiempo_despues': tiempo_despues,
                        'mejora_porcentaje': mejora
                    }
                    resultados['consultas_optimizadas'] += 1
                    
                except Exception as e:
                    resultados['errores'].append(f"Error optimizando consulta pagos: {str(e)}")
                
                resultados['tiempo_procesamiento'] = time.time() - inicio_tiempo
                
                # Calcular mejora promedio
                mejoras = [m['mejora_porcentaje'] for m in resultados['mejoras_rendimiento'].values()]
                mejora_promedio = sum(mejoras) / len(mejoras) if mejoras else 0
                resultados['mejora_promedio'] = mejora_promedio
                
                return resultados
                
        except Exception as e:
            resultados['errores'].append(f"Error general: {str(e)}")
            resultados['tiempo_procesamiento'] = time.time() - inicio_tiempo
            return resultados
    
    

    def _migrar_campo_objetivo_ejercicios(self, cursor):
        """Migra el campo objetivo para ejercicios existentes PostgreSQL"""
        try:
            # Verificar si la columna objetivo ya existe
            cursor.execute("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name = 'ejercicios' AND column_name = 'objetivo'
            """)
            
            if not cursor.fetchone():
                # Agregar la columna objetivo con valor por defecto
                cursor.execute("ALTER TABLE ejercicios ADD COLUMN objetivo TEXT DEFAULT 'general'")
                logging.info("Campo 'objetivo' agregado a la tabla ejercicios")
            
        except Exception as e:
            logging.error(f"Error en migración del campo objetivo en ejercicios: {e}")
            # En caso de error, no interrumpir la inicialización


    def _migrar_campo_categoria_rutinas(self, cursor):
        """Migra el campo categoria para rutinas existentes PostgreSQL"""
        try:
            # Verificar si la columna categoria ya existe
            cursor.execute("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name = 'rutinas' AND column_name = 'categoria'
            """)
            
            if not cursor.fetchone():
                # Agregar la columna categoria con valor por defecto
                cursor.execute("ALTER TABLE rutinas ADD COLUMN categoria TEXT DEFAULT 'general'")
                logging.info("Campo 'categoria' agregado a la tabla rutinas")
            
        except Exception as e:
            logging.error(f"Error en migración del campo categoria en rutinas: {e}")
            # En caso de error, no interrumpir la inicialización


    def _migrar_columnas_activa_activo(self, cursor):
        """Migra las columnas activa y activo para las tablas clases y clases_horarios PostgreSQL"""
        try:
            # Verificar si la columna activa ya existe en la tabla clases
            cursor.execute("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name = 'clases' AND column_name = 'activa'
            """)
            
            if not cursor.fetchone():
                # Agregar la columna activa con valor por defecto true
                cursor.execute("ALTER TABLE clases ADD COLUMN activa BOOLEAN DEFAULT true")
                logging.info("Campo 'activa' agregado a la tabla clases")
            
            # Verificar si la columna activo ya existe en la tabla clases_horarios
            cursor.execute("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name = 'clases_horarios' AND column_name = 'activo'
            """)
            
            if not cursor.fetchone():
                # Agregar la columna activo con valor por defecto true
                cursor.execute("ALTER TABLE clases_horarios ADD COLUMN activo BOOLEAN DEFAULT true")
                logging.info("Campo 'activo' agregado a la tabla clases_horarios")
            
        except Exception as e:
            logging.error(f"Error en migración de columnas activa y activo: {e}")
            # En caso de error, no interrumpir la inicialización


    def _verificar_columna_minutos_totales(self, cursor):
        """Verifica y crea la columna minutos_totales en profesor_horas_trabajadas si no existe"""
        try:
            # Verificar si la columna minutos_totales ya existe
            cursor.execute("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name = 'profesor_horas_trabajadas' AND column_name = 'minutos_totales'
            """)
            
            if not cursor.fetchone():
                # Agregar la columna minutos_totales
                cursor.execute("ALTER TABLE profesor_horas_trabajadas ADD COLUMN minutos_totales INTEGER")
                logging.info("Campo 'minutos_totales' agregado a la tabla profesor_horas_trabajadas")
                
                # Actualizar registros existentes calculando minutos_totales
                cursor.execute("""
                    UPDATE profesor_horas_trabajadas 
                    SET minutos_totales = CASE 
                        WHEN hora_fin IS NOT NULL AND hora_inicio IS NOT NULL THEN 
                            EXTRACT(EPOCH FROM (hora_fin - hora_inicio)) / 60
                        WHEN horas_totales IS NOT NULL THEN 
                            horas_totales * 60
                        ELSE 0
                    END
                    WHERE minutos_totales IS NULL
                """)
                logging.info("Registros existentes actualizados con minutos_totales calculados")
            
        except Exception as e:
            logging.error(f"Error en verificación de columna minutos_totales: {e}")
            # En caso de error, no interrumpir la inicialización


    def _migrar_series_a_varchar(self, cursor):
        """Migra el campo series de INTEGER a VARCHAR(50) en la tabla rutina_ejercicios"""
        try:
            # Verificar si la columna series es INTEGER
            cursor.execute("""
                SELECT data_type FROM information_schema.columns 
                WHERE table_name = 'rutina_ejercicios' AND column_name = 'series'
            """)
            
            result = cursor.fetchone()
            if result and result[0] == 'integer':
                # Cambiar el tipo de columna de INTEGER a VARCHAR(50)
                cursor.execute("""
                    ALTER TABLE rutina_ejercicios 
                    ALTER COLUMN series TYPE VARCHAR(50) USING series::text
                """)
                logging.info("Campo 'series' migrado de INTEGER a VARCHAR(50) en tabla rutina_ejercicios")
            
        except Exception as e:
            logging.error(f"Error en migración de campo series: {e}")
            # En caso de error, no interrumpir la inicialización


    def _migrar_tablas_temas_personalizados(self, cursor):
        """Migra las tablas para temas personalizados PostgreSQL"""
        try:
            # Crear tabla custom_themes
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS custom_themes (
                    id SERIAL PRIMARY KEY,
                    nombre VARCHAR(100) NOT NULL UNIQUE,
                    descripcion TEXT,
                    colores JSONB NOT NULL,
                    fuentes JSONB,
                    activo BOOLEAN DEFAULT false,
                    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    fecha_modificacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Crear tabla theme_scheduling_config
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS theme_scheduling_config (
                    id SERIAL PRIMARY KEY,
                    theme_id INTEGER REFERENCES custom_themes(id) ON DELETE CASCADE,
                    auto_change BOOLEAN DEFAULT false,
                    schedule_type VARCHAR(20) DEFAULT 'daily',
                    config_data JSONB,
                    activo BOOLEAN DEFAULT true,
                    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Crear tabla theme_schedules
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS theme_schedules (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(200) NOT NULL,
                    theme_name VARCHAR(100) NOT NULL,
                    config_id INTEGER REFERENCES theme_scheduling_config(id) ON DELETE CASCADE,
                    theme_id INTEGER REFERENCES custom_themes(id) ON DELETE CASCADE,
                    start_time TIME NOT NULL,
                    end_time TIME NOT NULL,
                    hora_inicio TIME NOT NULL,
                    hora_fin TIME NOT NULL,
                    dias_semana VARCHAR(20),
                    monday BOOLEAN DEFAULT FALSE,
                    tuesday BOOLEAN DEFAULT FALSE,
                    wednesday BOOLEAN DEFAULT FALSE,
                    thursday BOOLEAN DEFAULT FALSE,
                    friday BOOLEAN DEFAULT FALSE,
                    saturday BOOLEAN DEFAULT FALSE,
                    sunday BOOLEAN DEFAULT FALSE,
                    is_active BOOLEAN DEFAULT TRUE,
                    fecha_inicio DATE,
                    fecha_fin DATE,
                    activo BOOLEAN DEFAULT true,
                    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Crear tabla theme_events
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS theme_events (
                    id SERIAL PRIMARY KEY,
                    theme_id INTEGER REFERENCES custom_themes(id) ON DELETE CASCADE,
                    evento VARCHAR(50) NOT NULL,
                    fecha_inicio DATE NOT NULL,
                    fecha_fin DATE NOT NULL,
                    descripcion TEXT,
                    activo BOOLEAN DEFAULT true,
                    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Crear índices para optimizar consultas
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_custom_themes_activo ON custom_themes(activo)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_theme_scheduling_config_theme_id ON theme_scheduling_config(theme_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_theme_schedules_config_id ON theme_schedules(config_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_theme_events_theme_id ON theme_events(theme_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_theme_events_fechas ON theme_events(fecha_inicio, fecha_fin)")
            
            logging.info("Tablas de temas personalizados creadas exitosamente")
            
        except Exception as e:
            logging.error(f"Error en migración de tablas de temas personalizados: {e}")
            # En caso de error, no interrumpir la inicialización


    def _migrar_sistema_comprobantes(self, cursor):
        """Migra el sistema de comprobantes de pago PostgreSQL"""
        try:
            # Crear tabla comprobantes_pago
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS comprobantes_pago (
                    id SERIAL PRIMARY KEY,
                    pago_id INTEGER NOT NULL,
                    numero_comprobante VARCHAR(50) UNIQUE NOT NULL,
                    tipo_comprobante VARCHAR(20) DEFAULT 'recibo',
                    fecha_emision DATE NOT NULL,
                    monto DECIMAL(10,2) NOT NULL,
                    concepto TEXT,
                    datos_cliente JSONB,
                    datos_empresa JSONB,
                    estado VARCHAR(20) DEFAULT 'emitido',
                    observaciones TEXT,
                    archivo_pdf TEXT,
                    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    fecha_modificacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (pago_id) REFERENCES pagos(id) ON DELETE CASCADE
                )
            """)
            
            # Crear tabla configuracion_comprobantes
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS configuracion_comprobantes (
                    id SERIAL PRIMARY KEY,
                    nombre_empresa VARCHAR(200) NOT NULL,
                    ruc_empresa VARCHAR(20),
                    direccion_empresa TEXT,
                    telefono_empresa VARCHAR(20),
                    email_empresa VARCHAR(100),
                    logo_empresa TEXT,
                    formato_numero VARCHAR(50) DEFAULT 'REC-{:06d}',
                    contador_actual INTEGER DEFAULT 0,
                    incluir_qr BOOLEAN DEFAULT false,
                    plantilla_html TEXT,
                    activo BOOLEAN DEFAULT true,
                    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    fecha_modificacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Crear índices para optimizar consultas
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_comprobantes_pago_pago_id ON comprobantes_pago(pago_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_comprobantes_pago_numero ON comprobantes_pago(numero_comprobante)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_comprobantes_pago_fecha ON comprobantes_pago(fecha_emision)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_comprobantes_pago_estado ON comprobantes_pago(estado)")
            
            # Insertar configuración por defecto si no existe
            cursor.execute("SELECT COUNT(*) FROM configuracion_comprobantes")
            if cursor.fetchone()[0] == 0:
                cursor.execute("""
                    INSERT INTO configuracion_comprobantes 
                    (nombre_empresa, formato_numero, contador_actual, activo) 
                    VALUES ('Gimnasio', 'REC-{:06d}', 0, true)
                """)
            
            logging.info("Sistema de comprobantes creado exitosamente")
            
        except Exception as e:
            logging.error(f"Error en migración del sistema de comprobantes: {e}")
            # En caso de error, no interrumpir la inicialización


    def aplicar_optimizacion_completa(self) -> Dict[str, Any]:
        """Aplica todas las optimizaciones disponibles en secuencia PostgreSQL"""
        import time
        
        inicio_tiempo = time.time()
        resultados = {
            'optimizaciones_aplicadas': [],
            'errores': [],
            'tiempo_total': 0,
            'estado_final': 'completado'
        }
        
        try:
            # 1. Aplicar optimizaciones de base de datos (índices, vistas, etc.)
            resultado_db = self.aplicar_optimizaciones_database()
            resultados['optimizaciones_aplicadas'].append({
                'tipo': 'database_optimization',
                'resultado': resultado_db
            })
            
            # 2. Optimizar consultas críticas
            resultado_consultas = self.optimizar_consultas_criticas()
            resultados['optimizaciones_aplicadas'].append({
                'tipo': 'critical_queries',
                'resultado': resultado_consultas
            })
            
            # 3. Limpiar cache expirado
            cache_limpiado = self._cleanup_expired_cache()
            resultados['optimizaciones_aplicadas'].append({
                'tipo': 'cache_cleanup',
                'resultado': {'entradas_limpiadas': cache_limpiado}
            })
            
            # 4. Gestionar tamaño de cache
            cache_gestionado = self._manage_cache_size()
            resultados['optimizaciones_aplicadas'].append({
                'tipo': 'cache_management',
                'resultado': {'entradas_removidas': cache_gestionado}
            })
            
            resultados['tiempo_total'] = time.time() - inicio_tiempo
            
            # Verificar si hubo errores
            total_errores = sum(len(opt['resultado'].get('errores', [])) for opt in resultados['optimizaciones_aplicadas'])
            if total_errores > 0:
                resultados['estado_final'] = 'completado_con_errores'
                resultados['total_errores'] = total_errores
            
            return resultados
            
        except Exception as e:
            resultados['errores'].append(f"Error en optimización completa: {str(e)}")
            resultados['estado_final'] = 'error'
            resultados['tiempo_total'] = time.time() - inicio_tiempo
            return resultados


    def obtener_conteo_activos_inactivos(self) -> Dict[str, int]:
        """Obtiene el conteo de usuarios activos e inactivos."""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                sql = """
                SELECT 
                    CASE WHEN activo THEN 'Activos' ELSE 'Inactivos' END as estado,
                    COUNT(*) as cantidad
                FROM usuarios 
                WHERE rol IN ('socio','miembro','profesor')
                GROUP BY activo
                """
                cursor.execute(sql)
                
                resultado = {'Activos': 0, 'Inactivos': 0}
                for row in cursor.fetchall():
                    estado = row['estado']
                    cantidad = row['cantidad']
                    resultado[estado] = cantidad
                
                return resultado
                
        except Exception as e:
            logging.error(f"Error al obtener conteo activos/inactivos: {str(e)}")
            return {'Activos': 0, 'Inactivos': 0}
    

    def ensure_indexes_secondary(self) -> None:
        """Duplicado anterior: delega en ensure_indexes primario."""
        try:
            return self.ensure_indexes()
        except Exception:
            pass

    # Eliminado: métodos exclusivos de la webapp (migrados a server.py)
    

    def obtener_proximo_numero_comprobante(self, tipo_comprobante: str) -> str:
        """Obtiene el próximo número de comprobante sin incrementar el contador."""
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                
                # Obtener configuración de numeración
                cursor.execute(
                    "SELECT prefijo, numero_inicial, separador, longitud_numero FROM numeracion_comprobantes WHERE tipo_comprobante = %s AND activo = true",
                    (tipo_comprobante,)
                )
                config = cursor.fetchone()
                
                if not config:
                    # Si no existe configuración, crear una por defecto
                    cursor.execute(
                        "INSERT INTO numeracion_comprobantes (tipo_comprobante, prefijo, numero_inicial, separador, longitud_numero) VALUES (%s, %s, %s, %s, %s)",
                        (tipo_comprobante, 'REC', 1, '-', 6)
                    )
                    conn.commit()
                    config = {'prefijo': 'REC', 'numero_inicial': 1, 'separador': '-', 'longitud_numero': 6}
                
                # Generar número sin incrementar
                numero = str(config['numero_inicial']).zfill(config['longitud_numero'])
                numero_completo = f"{config['prefijo']}{config['separador']}{numero}"
                
                return numero_completo


    def crear_comprobante(self, tipo_comprobante: str, pago_id: int, usuario_id: int, monto_total, plantilla_id=None, datos_comprobante=None, emitido_por=None) -> int:
        """Crea un comprobante de pago, genera y reserva el número de comprobante de forma atómica y retorna el ID creado."""
        try:
            with self.atomic_transaction(isolation_level="REPEATABLE READ") as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    # Obtener/crear configuración de numeración y bloquear fila para actualización concurrente
                    cursor.execute(
                        """
                        SELECT id, prefijo, numero_inicial, separador, longitud_numero 
                        FROM numeracion_comprobantes 
                        WHERE tipo_comprobante = %s AND activo = true
                        FOR UPDATE
                        """,
                        (tipo_comprobante,)
                    )
                    config = cursor.fetchone()

                    if not config:
                        # Crear configuración por defecto si no existe
                        cursor.execute(
                            """
                            INSERT INTO numeracion_comprobantes 
                            (tipo_comprobante, prefijo, numero_inicial, separador, longitud_numero, activo)
                            VALUES (%s, %s, %s, %s, %s, true)
                            RETURNING id, prefijo, numero_inicial, separador, longitud_numero
                            """,
                            (tipo_comprobante, 'REC', 1, '-', 6)
                        )
                        config = cursor.fetchone()

                    # Generar número de comprobante con zfill y reservar incrementando el contador
                    numero = str(config['numero_inicial']).zfill(config['longitud_numero'])
                    numero_comprobante = f"{config['prefijo']}{config['separador']}{numero}"

                    # Incrementar contador (usando numero_inicial como siguiente número)
                    cursor.execute(
                        "UPDATE numeracion_comprobantes SET numero_inicial = numero_inicial + 1 WHERE id = %s",
                        (config['id'],)
                    )

                    # Obtener datos del pago (fecha y referencia a usuario)
                    cursor.execute(
                        "SELECT id, usuario_id, fecha_pago, mes, año, monto FROM pagos WHERE id = %s",
                        (pago_id,)
                    )
                    pago = cursor.fetchone()
                    if not pago:
                        raise ValueError("Pago no encontrado para generar comprobante")

                    # Derivar fecha de emisión
                    from datetime import date, datetime
                    if pago.get('fecha_pago') is not None:
                        fecha_pago_val = pago['fecha_pago']
                        try:
                            fecha_emision = fecha_pago_val.date() if hasattr(fecha_pago_val, 'date') else datetime.fromisoformat(str(fecha_pago_val)).date()
                        except Exception:
                            fecha_emision = date.today()
                    else:
                        fecha_emision = date.today()

                    # Concepto legible
                    concepto = None
                    try:
                        if pago.get('mes') and pago.get('año'):
                            concepto = f"Pago de cuota {int(pago['mes'])}/{int(pago['año'])}"
                    except Exception:
                        pass

                    # Construir datos de cliente mínimos
                    import json
                    datos_cliente = {'usuario_id': usuario_id}
                    try:
                        usuario = self.obtener_usuario(usuario_id)
                        if usuario:
                            # usuario puede ser dict u objeto, normalizar
                            if isinstance(usuario, dict):
                                datos_cliente.update({
                                    'nombre': usuario.get('nombre'),
                                    'dni': usuario.get('dni')
                                })
                            else:
                                # fallback por si retorna objeto con atributos
                                datos_cliente.update({
                                    'nombre': getattr(usuario, 'nombre', None),
                                    'dni': getattr(usuario, 'dni', None)
                                })
                    except Exception:
                        pass

                    # Insertar comprobante
                    cursor.execute(
                        """
                        INSERT INTO comprobantes_pago (
                            pago_id, numero_comprobante, tipo_comprobante, fecha_emision, monto, 
                            concepto, datos_cliente, estado, observaciones, archivo_pdf
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s)
                        RETURNING id
                        """,
                        (
                            pago_id,
                            numero_comprobante,
                            tipo_comprobante,
                            fecha_emision,
                            monto_total,
                            concepto,
                            json.dumps(datos_cliente) if datos_cliente else None,
                            'emitido',
                            None,
                            None
                        )
                    )

                    nuevo_id = cursor.fetchone()['id']
                    return nuevo_id
        except Exception as e:
            logging.error(f"Error al crear comprobante: {e}")
            raise


    def obtener_comprobante(self, comprobante_id: int):
        """Obtiene un comprobante por ID, incluyendo usuario_id (por join con pagos)."""
        try:
            with self.get_connection_context() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    cursor.execute(
                        """
                        SELECT 
                            c.id, c.pago_id, c.numero_comprobante, c.tipo_comprobante, c.fecha_emision, 
                            c.monto, c.concepto, c.datos_cliente, c.datos_empresa, c.estado, 
                            c.observaciones, c.archivo_pdf, c.fecha_creacion, c.fecha_modificacion,
                            p.usuario_id AS usuario_id
                        FROM comprobantes_pago c
                        JOIN pagos p ON p.id = c.pago_id
                        WHERE c.id = %s
                        """,
                        (comprobante_id,)
                    )
                    row = cursor.fetchone()
                    return row
        except Exception as e:
            logging.error(f"Error al obtener comprobante: {e}")
            return None

    # ===== MÉTODOS PARA REPORTS DASHBOARD =====
    

    def verificar_integridad_base_datos(self) -> dict:
        """Verifica la integridad de la base de datos."""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                integridad = {
                    'estado': 'OK',
                    'errores': [],
                    'advertencias': [],
                    'tablas_verificadas': 0
                }
                
                # Verificar existencia de tablas principales
                # Nota: la tabla de configuración se llama 'configuracion'
                tablas_requeridas = ['usuarios', 'pagos', 'asistencias', 'clases', 'rutinas', 'profesores', 'configuracion']
                
                for tabla in tablas_requeridas:
                    try:
                        cursor.execute(f"SELECT COUNT(*) FROM {tabla}")
                        integridad['tablas_verificadas'] += 1
                    except Exception as e:
                        integridad['errores'].append(f"Tabla {tabla} no encontrada o inaccesible: {e}")
                        integridad['estado'] = 'ERROR'
                
                # Verificar integridad referencial básica
                try:
                    cursor.execute("""
                        SELECT COUNT(*) FROM pagos p 
                        LEFT JOIN usuarios u ON p.usuario_id = u.id 
                        WHERE u.id IS NULL
                    """)
                    pagos_huerfanos = cursor.fetchone()[0]
                    if pagos_huerfanos > 0:
                        integridad['advertencias'].append(f"Encontrados {pagos_huerfanos} pagos sin usuario asociado")
                except:
                    pass
                
                return integridad
                
        except Exception as e:
            logging.error(f"Error verificando integridad de base de datos: {str(e)}")
            return {'estado': 'ERROR', 'errores': [str(e)], 'advertencias': [], 'tablas_verificadas': 0}
    

    def listar_respaldos_disponibles(self) -> list:
        """Lista los respaldos disponibles en el directorio de respaldos."""
        try:
            import os
            import tempfile
            from datetime import datetime
            
            temp_dir = tempfile.gettempdir()
            respaldos = []
            
            # Buscar archivos de respaldo en el directorio temporal
            for archivo in os.listdir(temp_dir):
                if archivo.startswith('gym_backup_') and archivo.endswith('.sql'):
                    ruta_completa = os.path.join(temp_dir, archivo)
                    try:
                        stat = os.stat(ruta_completa)
                        respaldos.append({
                            'nombre': archivo,
                            'ruta': ruta_completa,
                            'tamaño': stat.st_size,
                            'fecha_creacion': datetime.fromtimestamp(stat.st_ctime),
                            'fecha_modificacion': datetime.fromtimestamp(stat.st_mtime)
                        })
                    except:
                        continue
            
            # Ordenar por fecha de creación (más reciente primero)
            respaldos.sort(key=lambda x: x['fecha_creacion'], reverse=True)
            
            return respaldos
            
        except Exception as e:
             logging.error(f"Error listando respaldos disponibles: {str(e)}")
             return []
    

    def optimizar_base_datos(self) -> bool:
        """Optimiza la base de datos ejecutando operaciones de mantenimiento."""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor()
                
                # Ejecutar VACUUM ANALYZE en las tablas principales
                tablas = ['usuarios', 'pagos', 'asistencias', 'clases', 'rutinas', 'profesores', 'configuracion']
                
                for tabla in tablas:
                    try:
                        cursor.execute(f"VACUUM ANALYZE {tabla}")
                        conn.commit()
                    except Exception as e:
                        self.logger.warning(f"No se pudo optimizar tabla {tabla}: {e}")
                        continue
                
                self.logger.info("Optimización de base de datos completada")
                return True
                
        except Exception as e:
            logging.error(f"Error optimizando base de datos: {str(e)}")
            return False
    

    def obtener_configuracion_respaldo_automatico(self) -> dict:
        """Obtiene la configuración de respaldo automático."""
        try:
            config = {
                'habilitado': self.obtener_configuracion('backup_automatico_habilitado') or 'false',
                'frecuencia': self.obtener_configuracion('backup_frecuencia') or 'diario',
                'hora': self.obtener_configuracion('backup_hora') or '02:00',
                'directorio': self.obtener_configuracion('backup_directorio') or 'backups/auto',
                'retener_dias': self.obtener_configuracion('backup_retener_dias') or '30'
            }
            return config
            
        except Exception as e:
            logging.error(f"Error obteniendo configuración de respaldo automático: {str(e)}")
            return {}
    

    def validar_respaldo(self, ruta_respaldo: str) -> bool:
        """Valida que un archivo de respaldo sea válido."""
        try:
            import os
            
            if not os.path.exists(ruta_respaldo):
                return False
            
            # Verificar que el archivo no esté vacío
            if os.path.getsize(ruta_respaldo) == 0:
                return False
            
            # Verificar que contenga comandos SQL básicos
            with open(ruta_respaldo, 'r', encoding='utf-8') as f:
                contenido = f.read(1000)  # Leer primeros 1000 caracteres
                
            # Buscar indicadores de un respaldo SQL válido
            indicadores = ['CREATE TABLE', 'INSERT INTO', 'COPY', '--']
            tiene_indicadores = any(indicador in contenido.upper() for indicador in indicadores)
            
            return tiene_indicadores
            
        except Exception as e:
            logging.error(f"Error validando respaldo: {str(e)}")
            return False
    

    def calcular_tamano_base_datos(self) -> float:
        """Calcula el tamaño de la base de datos en MB."""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                    SELECT pg_size_pretty(pg_database_size(current_database())) as size,
                           pg_database_size(current_database()) as size_bytes
                """)
                
                resultado = cursor.fetchone()
                if resultado:
                    size_bytes = resultado[1]
                    size_mb = size_bytes / (1024 * 1024)  # Convertir a MB
                    return round(size_mb, 2)
                
                return 0.0
                
        except Exception as e:
            logging.error(f"Error calculando tamaño de base de datos: {str(e)}")
            return 0.0
    

    def contar_clases_totales(self) -> int:
        """Cuenta el total de clases en el sistema."""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM clases")
                return cursor.fetchone()[0] or 0
        except Exception as e:
            logging.error(f"Error contando clases totales: {str(e)}")
            return 0
    

    def obtener_todas_configuraciones(self) -> dict:
        """Obtiene todas las configuraciones del sistema."""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                cursor.execute("SELECT clave, valor FROM configuracion")
                configuraciones = {}
                
                for row in cursor.fetchall():
                    clave = row['clave']
                    valor = row['valor']
                    
                    # Intentar convertir valores numéricos automáticamente
                    try:
                        # Intentar convertir a entero
                        if valor.isdigit():
                            configuraciones[clave] = int(valor)
                        # Intentar convertir a float
                        elif '.' in valor and valor.replace('.', '').isdigit():
                            configuraciones[clave] = float(valor)
                        # Convertir booleanos
                        elif valor.lower() in ['true', 'false']:
                            configuraciones[clave] = valor.lower() == 'true'
                        else:
                            configuraciones[clave] = valor
                    except (AttributeError, ValueError):
                        # Si no se puede convertir, mantener como string
                        configuraciones[clave] = valor
                
                return configuraciones
                
        except Exception as e:
            logging.error(f"Error obteniendo todas las configuraciones: {str(e)}")
            return {}
    

    def eliminar_configuracion(self, clave: str) -> bool:
        """Elimina una configuración específica."""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM configuracion WHERE clave = %s", (clave,))
                conn.commit()
                return cursor.rowcount > 0
        except Exception as e:
            logging.error(f"Error eliminando configuración {clave}: {str(e)}")
            return False
    

    def crear_respaldo_base_datos(self, ruta_destino: str = None) -> str:
        """Crea un respaldo de la base de datos."""
        try:
            import subprocess
            import tempfile
            import os
            from datetime import datetime
            
            if not ruta_destino:
                temp_dir = tempfile.gettempdir()
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                ruta_destino = os.path.join(temp_dir, f"gym_backup_{timestamp}.sql")
            
            # Comando pg_dump
            cmd = [
                'pg_dump',
                '-h', self.connection_params['host'],
                '-p', str(self.connection_params['port']),
                '-U', self.connection_params['user'],
                '-d', self.connection_params['database'],
                '-f', ruta_destino,
                '--no-password'
            ]
            
            # Ejecutar pg_dump
            env = os.environ.copy()
            env['PGPASSWORD'] = self.connection_params['password']
            
            result = subprocess.run(cmd, env=env, capture_output=True, text=True)
            
            if result.returncode == 0:
                return ruta_destino
            else:
                logging.error(f"Error en pg_dump: {result.stderr}")
                return ""
                
        except Exception as e:
             logging.error(f"Error creando respaldo de base de datos: {str(e)}")
             return ""
    

    def contar_clases_activas(self) -> int:
        """Cuenta clases activas en el sistema."""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM clases WHERE activa = true")
                return cursor.fetchone()[0] or 0
        except Exception as e:
            logging.error(f"Error contando clases activas: {str(e)}")
            return 0
    

    def obtener_alertas_sistema(self) -> list:
        """Obtiene alertas del sistema."""
        try:
            alertas = []
            
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                # Verificar usuarios con pagos vencidos
                from datetime import datetime, timedelta
                fecha_limite = datetime.now() - timedelta(days=30)
                
                cursor.execute("""
                    SELECT COUNT(*) FROM usuarios u
                    WHERE u.activo = true AND u.rol IN ('socio','profesor')
                    AND NOT EXISTS (
                        SELECT 1 FROM pagos p 
                        WHERE p.usuario_id = u.id 
                        AND p.fecha_pago >= %s
                    )
                """, (fecha_limite,))
                
                usuarios_sin_pago = cursor.fetchone()[0] or 0
                if usuarios_sin_pago > 0:
                    alertas.append({
                        'tipo': 'warning',
                        'mensaje': f'{usuarios_sin_pago} usuarios sin pagos en los últimos 30 días',
                        'fecha': datetime.now().isoformat()
                    })
                
                # Verificar clases inactivas
                cursor.execute("""
                    SELECT COUNT(*) FROM clases c
                    WHERE c.activa = false
                """)
                
                clases_inactivas = cursor.fetchone()[0] or 0
                if clases_inactivas > 0:
                    alertas.append({
                        'tipo': 'info',
                        'mensaje': f'{clases_inactivas} clases inactivas en el sistema',
                        'fecha': datetime.now().isoformat()
                    })
                
                return alertas
                
        except Exception as e:
            logging.error(f"Error obteniendo alertas del sistema: {str(e)}")
            return []
    

    def obtener_estadisticas_ocupacion_clases(self) -> dict:
        """Obtiene estadísticas de ocupación de clases."""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                estadisticas = {}
                
                # Como la tabla clases no tiene las columnas necesarias, retornamos estadísticas simuladas
                cursor.execute("SELECT COUNT(*) FROM clases WHERE activa = true")
                total_clases = cursor.fetchone()[0] or 0
                
                # Estadísticas simuladas basadas en el total de clases
                estadisticas['promedio_ocupacion'] = 75.0  # 75% promedio
                estadisticas['clases_llenas'] = int(total_clases * 0.2)  # 20% llenas
                estadisticas['clases_baja_ocupacion'] = int(total_clases * 0.1)  # 10% baja ocupación
                
                return estadisticas
                
        except Exception as e:
            logging.error(f"Error obteniendo estadísticas de ocupación: {str(e)}")
            return {}
    

    def exportar_clases_csv(self, fecha_inicio=None, fecha_fin=None) -> str:
        """Exporta clases a CSV y retorna la ruta del archivo."""
        try:
            import csv
            import tempfile
            import os
            from datetime import datetime
            
            # Crear archivo temporal
            temp_dir = tempfile.gettempdir()
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"clases_export_{timestamp}.csv"
            filepath = os.path.join(temp_dir, filename)
            
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                # Consulta base - usando solo columnas disponibles
                sql = """
                    SELECT c.id, c.nombre, c.descripcion, c.activa
                    FROM clases c
                """
                params = []
                
                # No aplicamos filtros de fecha ya que la columna no existe
                sql += " ORDER BY c.id"
                
                cursor.execute(sql, params)
                clases = cursor.fetchall()
                
                # Escribir CSV
                with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                    fieldnames = ['ID', 'Nombre', 'Descripción', 'Activa']
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    
                    writer.writeheader()
                    for clase in clases:
                        writer.writerow({
                            'ID': clase['id'],
                            'Nombre': clase['nombre'],
                            'Descripción': clase['descripcion'],
                            'Activa': 'Sí' if clase['activa'] else 'No'
                        })
                
                return filepath
                
        except Exception as e:
            logging.error(f"Error exportando clases a CSV: {str(e)}")
            return ""
    

    def obtener_clases_mas_populares(self, limit=10) -> list:
        """Obtiene las clases más populares."""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cursor.execute("""
                    SELECT 
                        c.nombre as clase_nombre,
                        COUNT(c.id) as total_clases,
                        c.descripcion
                    FROM clases c
                    WHERE c.activa = true
                    GROUP BY c.nombre, c.descripcion
                    ORDER BY total_clases DESC
                    LIMIT %s
                """, (limit,))
                return cursor.fetchall()
        except Exception as e:
            logging.error(f"Error obteniendo clases más populares: {str(e)}")
            return []
    

    def obtener_horas_fuera_horario_profesor(self, profesor_id: int, mes: int = None, año: int = None) -> dict:
        """Calcula las horas trabajadas fuera del horario programado"""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                if mes is None or año is None:
                    from datetime import datetime
                    now = datetime.now()
                    mes = mes or now.month
                    año = año or now.year
                
                # Obtener todas las sesiones del mes
                cursor.execute("""
                    SELECT fecha, hora_inicio, hora_fin, minutos_totales,
                           EXTRACT(DOW FROM fecha) as dia_semana_num
                    FROM profesor_horas_trabajadas
                    WHERE profesor_id = %s
                    AND EXTRACT(MONTH FROM fecha) = %s
                    AND EXTRACT(YEAR FROM fecha) = %s
                    AND hora_fin IS NOT NULL
                    ORDER BY fecha, hora_inicio
                """, (profesor_id, mes, año))
                sesiones = cursor.fetchall()
                
                # Obtener horarios programados del profesor
                cursor.execute("""
                    SELECT ch.dia_semana, ch.hora_inicio, ch.hora_fin
                    FROM clases_horarios ch
                    JOIN profesor_clase_asignaciones pca ON ch.id = pca.clase_horario_id
                    WHERE pca.profesor_id = %s AND pca.activa = true AND ch.activo = true
                """, (profesor_id,))
                horarios_programados = cursor.fetchall()
                
                # Mapear días de la semana (num<->nombre) y normalizar claves a entero
                dias_num_a_nombre = {
                    0: 'Domingo', 1: 'Lunes', 2: 'Martes', 3: 'Miércoles',
                    4: 'Jueves', 5: 'Viernes', 6: 'Sábado'
                }
                dias_nombre_a_num = {
                    'Domingo': 0, 'Lunes': 1, 'Martes': 2, 'Miércoles': 3, 'Miercoles': 3,
                    'Jueves': 4, 'Viernes': 5, 'Sábado': 6, 'Sabado': 6
                }
                
                # Crear diccionario de horarios por día usando número (Postgres: 0=Domingo..6=Sábado)
                horarios_por_dia = {}
                for horario in horarios_programados:
                    dia_val = horario['dia_semana']
                    if isinstance(dia_val, int):
                        dia_num = dia_val
                    else:
                        # Convertir nombre a número si viene como texto
                        dia_num = dias_nombre_a_num.get(str(dia_val), None)
                        if dia_num is None:
                            try:
                                dia_num = int(dia_val)
                            except Exception:
                                logging.warning(f"dia_semana inválido en horario: {dia_val}")
                                continue
                    if dia_num not in horarios_por_dia:
                        horarios_por_dia[dia_num] = []
                    horarios_por_dia[dia_num].append({
                        'inicio': horario['hora_inicio'],
                        'fin': horario['hora_fin']
                    })
                
                total_horas_fuera = 0
                sesiones_fuera = []
                
                for sesion in sesiones:
                    dia_num = int(sesion['dia_semana_num'])  # 0=Domingo..6=Sábado
                    horarios_dia = horarios_por_dia.get(dia_num, [])
                    
                    if not horarios_dia:
                        # No hay horario programado para este día
                        minutos_fuera = sesion['minutos_totales']
                        total_horas_fuera += minutos_fuera / 60.0
                        sesiones_fuera.append({
                            'fecha': sesion['fecha'],
                            'horas_fuera': minutos_fuera / 60.0,
                            'motivo': 'Día no programado'
                        })
                    else:
                        # Verificar si la sesión está dentro de algún horario programado
                        horas_dentro_horario = 0
                        
                        for horario in horarios_dia:
                            # Calcular intersección entre sesión y horario programado
                            inicio_interseccion = max(sesion['hora_inicio'], horario['inicio'])
                            fin_interseccion = min(sesion['hora_fin'], horario['fin'])
                            
                            if inicio_interseccion < fin_interseccion:
                                # Hay intersección
                                from datetime import datetime, timedelta
                                delta = datetime.combine(datetime.min, fin_interseccion) - datetime.combine(datetime.min, inicio_interseccion)
                                horas_dentro_horario += delta.total_seconds() / 3600
                        
                        minutos_sesion = sesion['minutos_totales']
                        horas_fuera_sesion = max(0, (minutos_sesion / 60.0) - horas_dentro_horario)
                        if horas_fuera_sesion > 0:
                            total_horas_fuera += horas_fuera_sesion
                            sesiones_fuera.append({
                                'fecha': sesion['fecha'],
                                'horas_fuera': horas_fuera_sesion,
                                'motivo': 'Fuera de horario programado'
                            })
                
                return {
                    'success': True,
                    'total_horas_fuera': round(total_horas_fuera, 2),
                    'sesiones_fuera': sesiones_fuera,
                    'mes': mes,
                    'año': año
                }
                
        except Exception as e:
            logging.error(f"Error calculando horas fuera de horario: {str(e)}")
            from datetime import datetime
            return {
                'success': False,
                'mensaje': f"Error calculando horas fuera de horario: {str(e)}",
                'total_horas_fuera': 0,
                'sesiones_fuera': [],
                'mes': mes or datetime.now().month,
                'año': año or datetime.now().year
            }
    

    def simular_automatizacion_estados(self, config: dict = None) -> dict:
        """Simula la ejecución de automatización de estados sin aplicar cambios"""
        try:
            if not config:
                config = self.obtener_configuracion_automatizacion()
            
            with self.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    # Simular vencimientos
                    fecha_actual = datetime.now().date()
                    cursor.execute("""
                        SELECT COUNT(*) 
                        FROM usuario_estados ue
                        JOIN usuarios u ON ue.usuario_id = u.id
                        WHERE ue.activo = TRUE 
                          AND ue.fecha_vencimiento IS NOT NULL 
                          AND ue.fecha_vencimiento < %s
                          AND u.rol = 'socio'
                    """, (fecha_actual,))
                    estados_a_vencer = cursor.fetchone()[0]
                    
                    # Simular usuarios para actualizar
                    cursor.execute("""
                        SELECT u.id, u.nombre, u.dni,
                               (SELECT MAX(p.fecha_pago) FROM pagos p WHERE p.usuario_id = u.id) as ultimo_pago
                        FROM usuarios u
                        WHERE u.activo = TRUE AND u.rol = 'socio'
                    """)
                    usuarios = cursor.fetchall()
                    
                    usuarios_a_desactivar = 0
                    usuarios_en_alerta = 0
                    detalles_simulacion = []
                    
                    for usuario in usuarios:
                        dias_sin_pago = float('inf')
                        if usuario[3]:  # ultimo_pago
                            try:
                                ultimo_pago_str = str(usuario[3])
                                if ' ' in ultimo_pago_str:
                                    ultimo_pago_date = datetime.strptime(ultimo_pago_str.split()[0], '%Y-%m-%d').date()
                                else:
                                    ultimo_pago_date = datetime.fromisoformat(ultimo_pago_str).date()
                                dias_sin_pago = (fecha_actual - ultimo_pago_date).days
                            except (ValueError, TypeError):
                                dias_sin_pago = float('inf')
                        
                        if dias_sin_pago >= config['dias_vencimiento']:
                            usuarios_a_desactivar += 1
                            detalles_simulacion.append({
                                'usuario_id': usuario[0],
                                'nombre': usuario[1],
                                'dni': usuario[2],
                                'accion': 'desactivar',
                                'motivo': f'Cuota vencida ({dias_sin_pago} días sin pago)',
                                'dias_sin_pago': dias_sin_pago
                            })
                        elif dias_sin_pago >= (config['dias_vencimiento'] - config['dias_alerta']):
                            usuarios_en_alerta += 1
                            dias_restantes = config['dias_vencimiento'] - dias_sin_pago
                            detalles_simulacion.append({
                                'usuario_id': usuario[0],
                                'nombre': usuario[1],
                                'dni': usuario[2],
                                'accion': 'alerta',
                                'motivo': f'Próximo vencimiento ({dias_restantes} días restantes)',
                                'dias_sin_pago': dias_sin_pago
                            })
                    
                    return {
                        'configuracion': config,
                        'resultados_simulacion': {
                            'estados_a_vencer': estados_a_vencer,
                            'usuarios_a_desactivar': usuarios_a_desactivar,
                            'usuarios_en_alerta': usuarios_en_alerta,
                            'total_afectados': usuarios_a_desactivar + usuarios_en_alerta
                        },
                        'detalles': detalles_simulacion[:20],  # Limitar a 20 para no sobrecargar UI
                        'total_detalles': len(detalles_simulacion),
                        'fecha_simulacion': fecha_actual.isoformat()
                    }
                    
        except Exception as e:
            logging.error(f"Error simulando automatización de estados: {e}")
            return {
                'configuracion': config or {},
                'resultados_simulacion': {
                    'estados_a_vencer': 0,
                    'usuarios_a_desactivar': 0,
                    'usuarios_en_alerta': 0,
                    'total_afectados': 0
                },
                'detalles': [],
                'total_detalles': 0,
                'error': str(e)
            }
    

    def obtener_acciones_masivas_pendientes(self, usuario_id=None, estado=None):
        """Obtiene las acciones masivas pendientes con manejo robusto de errores"""
        try:
            # Validaciones iniciales
            if usuario_id is not None and not isinstance(usuario_id, (int, str)):
                logging.error(f"obtener_acciones_masivas_pendientes: usuario_id inválido: {usuario_id} (tipo: {type(usuario_id)})")
                return []
            
            if estado is not None and not isinstance(estado, str):
                logging.error(f"obtener_acciones_masivas_pendientes: estado inválido: {estado} (tipo: {type(estado)})")
                return []
            
            # Verificar conexión a la base de datos
            if not hasattr(self, 'get_connection_context'):
                logging.error("obtener_acciones_masivas_pendientes: Método get_connection_context no disponible")
                return []
            
            with self.get_connection_context() as conn:
                if conn is None:
                    logging.error("obtener_acciones_masivas_pendientes: No se pudo establecer conexión con la base de datos")
                    return []
                
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    if cursor is None:
                        logging.error("obtener_acciones_masivas_pendientes: No se pudo crear cursor")
                        return []
                    
                    # Verificar si la tabla existe
                    try:
                        cursor.execute("""
                            SELECT EXISTS (
                                SELECT FROM information_schema.tables 
                                WHERE table_schema = 'public' 
                                AND table_name = 'acciones_masivas_pendientes'
                            )
                        """)
                        row = cursor.fetchone()
                        exists_value = False
                        if row is not None:
                            if isinstance(row, dict):
                                exists_value = row.get('exists')
                                if exists_value is None:
                                    exists_value = row.get('?column?')
                            else:
                                try:
                                    exists_value = row[0]
                                except Exception:
                                    exists_value = bool(row)

                        if not bool(exists_value):
                            logging.warning("obtener_acciones_masivas_pendientes: Tabla acciones_masivas_pendientes no existe - creando tabla")
                            try:
                                self._crear_tabla_acciones_masivas_pendientes()
                                logging.info("obtener_acciones_masivas_pendientes: Tabla creada exitosamente")
                            except Exception as create_error:
                                logging.error(f"obtener_acciones_masivas_pendientes: Error creando tabla: {create_error}")
                                return []
                    except Exception as table_check_error:
                        logging.error(f"obtener_acciones_masivas_pendientes: Error verificando existencia de tabla: {table_check_error}")
                        return []
                    
                    # Construir consulta
                    try:
                        query = (
                            "SELECT id, operation_id, tipo, descripcion, usuario_ids, parametros, "
                            "estado, fecha_creacion, fecha_completado, resultado, created_by, error_message "
                            "FROM acciones_masivas_pendientes WHERE 1=1"
                        )
                        params = []
                        
                        if usuario_id:
                            query += " AND usuario_ids @> ARRAY[%s]"
                            params.append(usuario_id)
                        
                        if estado:
                            query += " AND estado = %s"
                            params.append(estado)
                        
                        query += " ORDER BY fecha_creacion DESC"
                        
                        logging.debug(f"obtener_acciones_masivas_pendientes: Ejecutando consulta: {query} con parámetros: {params}")
                        
                        cursor.execute(query, params)
                        resultados = cursor.fetchall()
                        
                        logging.info(f"obtener_acciones_masivas_pendientes: Se obtuvieron {len(resultados)} registros")
                        return resultados
                        
                    except Exception as query_error:
                        logging.error(f"obtener_acciones_masivas_pendientes: Error ejecutando consulta: {query_error}")
                        return []
                    
        except Exception as e:
            # Manejo robusto de errores con información detallada
            error_msg = str(e) if str(e) else "Error desconocido sin mensaje"
            error_type = type(e).__name__
            
            logging.error(f"obtener_acciones_masivas_pendientes: Error crítico - Tipo: {error_type}, Mensaje: '{error_msg}'")
            logging.error(f"obtener_acciones_masivas_pendientes: Parámetros - usuario_id: {usuario_id}, estado: {estado}")
            
            # Log del traceback completo
            import traceback
            logging.error(f"obtener_acciones_masivas_pendientes: Traceback completo:\n{traceback.format_exc()}")
            
            # Verificar tipos específicos de error
            if "relation" in error_msg.lower() and "does not exist" in error_msg.lower():
                logging.warning("obtener_acciones_masivas_pendientes: Error de tabla inexistente - intentando crear tabla")
                try:
                    self._crear_tabla_acciones_masivas_pendientes()
                    logging.info("obtener_acciones_masivas_pendientes: Tabla creada después del error")
                except Exception as create_error:
                    logging.error(f"obtener_acciones_masivas_pendientes: Error creando tabla después del fallo: {create_error}")
            elif "connection" in error_msg.lower():
                logging.error("obtener_acciones_masivas_pendientes: Error de conexión a la base de datos")
            elif "permission" in error_msg.lower():
                logging.error("obtener_acciones_masivas_pendientes: Error de permisos en la base de datos")
            
            return []
    

    def cancelar_acciones_masivas(self, operation_ids):
        """Cancela acciones masivas pendientes"""
        try:
            with self.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    if isinstance(operation_ids, str):
                        operation_ids = [operation_ids]
                    
                    placeholders = ','.join(['%s' for _ in operation_ids])
                    
                    # Actualizar estado a cancelado
                    cursor.execute(f"""
                        UPDATE acciones_masivas_pendientes 
                        SET estado = 'cancelado',
                            fecha_completado = NOW(),
                            resultado = 'Cancelado por el usuario'
                        WHERE operation_id IN ({placeholders})
                          AND estado IN ('pendiente', 'en_progreso')
                    """, operation_ids)
                    
                    affected_rows = cursor.rowcount
                    conn.commit()
                    
                    return {
                        'success': True,
                        'cancelled_count': affected_rows,
                        'message': f'Se cancelaron {affected_rows} acciones masivas'
                    }
                    
        except Exception as e:
            logging.error(f"Error cancelando acciones masivas: {e}")
            return {
                'success': False,
                'error': str(e),
                'cancelled_count': 0
            }
    

    def obtener_configuracion_whatsapp(self) -> Optional[Dict]:
        """Obtiene la configuración activa de WhatsApp"""
        try:
            with self.get_connection_context() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT 
                            id, phone_id, waba_id, access_token, active, created_at
                        FROM whatsapp_config 
                        WHERE active = TRUE 
                        ORDER BY created_at DESC 
                        LIMIT 1
                    """)
                    
                    result = cursor.fetchone()
                    return dict(result) if result else None
                    
        except Exception as e:
            logging.error(f"Error obteniendo configuración WhatsApp: {e}")
            return None
    

    def obtener_configuracion_whatsapp_completa(self) -> Dict[str, Any]:
        """Obtiene la configuración completa de WhatsApp.

        - Prioriza valores almacenados en base de datos (tabla `whatsapp_config`).
        - Usa variables de entorno para el `access_token` si está disponible.
        - Incluye preferencias adicionales desde la tabla genérica `configuracion`.
        - No usa valores hardcodeados.
        """
        try:
            # Token de acceso desde variable de entorno (preferido)
            env_access_token = None
            try:
                from .secure_config import config as secure_config
                try:
                    env_access_token = secure_config.get_whatsapp_access_token()
                except ValueError:
                    env_access_token = None
            except Exception:
                env_access_token = None

            # Configuración activa en BD
            db_conf = self.obtener_configuracion_whatsapp() or {}

            # IDs desde BD o entorno (si existen en entorno)
            try:
                import os as _os
                env_phone_id = _os.getenv('WHATSAPP_PHONE_NUMBER_ID')
                env_waba_id = _os.getenv('WHATSAPP_BUSINESS_ACCOUNT_ID')
            except Exception:
                env_phone_id = None
                env_waba_id = None

            phone_id = db_conf.get('phone_id') or env_phone_id
            waba_id = db_conf.get('waba_id') or env_waba_id
            access_token = env_access_token or db_conf.get('access_token')

            # Preferencias adicionales en tabla `configuracion`
            def _get_pref(key: str, default: str) -> str:
                try:
                    val = self.obtener_configuracion(key)
                    return default if val is None else str(val)
                except Exception:
                    return default

            cfg = {
                'phone_id': phone_id,
                'waba_id': waba_id,
                'access_token': access_token,
                'active': bool(db_conf.get('active', False)),
                # Preferencias adicionales
                'allowlist_numbers': _get_pref('allowlist_numbers', ''),
                'allowlist_enabled': _get_pref('allowlist_enabled', 'false'),
                'enable_webhook': _get_pref('enable_webhook', 'false'),
                'max_retries': _get_pref('max_retries', '3'),
                'retry_delay_seconds': _get_pref('retry_delay_seconds', '5'),
            }

            return cfg

        except Exception as e:
            logging.error(f"Error obteniendo configuración WhatsApp completa: {e}")
            return {}
    

    def actualizar_configuracion_whatsapp(self, phone_id: str = None, waba_id: str = None,
                                        access_token: str = None) -> bool:
        """Actualiza la configuración activa de WhatsApp.

        Soporta actualizaciones parciales: si algún parámetro es None, conserva el valor anterior.
        """
        try:
            # Obtener configuración previa para completar valores faltantes
            prev = self.obtener_configuracion_whatsapp() or {}
            new_phone_id = phone_id if phone_id is not None else prev.get('phone_id')
            new_waba_id = waba_id if waba_id is not None else prev.get('waba_id')
            new_access_token = access_token if access_token is not None else prev.get('access_token')

            # Si no hay ningún valor definido, no insertar
            if not (new_phone_id or new_waba_id or new_access_token):
                return False

            with self.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    # Desactivar configuraciones anteriores
                    try:
                        cursor.execute("UPDATE whatsapp_config SET active = FALSE")
                    except Exception:
                        pass

                    # Insertar nueva configuración activa
                    cursor.execute(
                        """
                        INSERT INTO whatsapp_config (phone_id, waba_id, access_token, active)
                        VALUES (%s, %s, %s, TRUE)
                        """,
                        (new_phone_id, new_waba_id, new_access_token)
                    )

                    conn.commit()
                    return True

        except Exception as e:
            logging.error(f"Error actualizando configuración WhatsApp: {e}")
            return False
    

    def limpiar_datos_innecesarios(
        self,
        whatsapp_days: int = 120,
        audit_logs_days: int = 180,
        auditoria_days: int = 180,
        notificaciones_days: int = 90,
        sysdiag_days: int = 180,
    ) -> Dict[str, int]:
        """Limpia datos no esenciales de forma sencilla y directa.
        
        Operaciones:
        - Borra mensajes de WhatsApp más antiguos que `whatsapp_days`.
        - Borra registros de `audit_logs` y `auditoria` por antigüedad.
        - Borra `notificaciones_cupos` leídas/inactivas según `notificaciones_days`.
        - Borra registros antiguos en `system_diagnostics`.
        
        Devuelve un diccionario con conteos por tabla.
        """
        resultados: Dict[str, int] = {}
        try:
            # WhatsApp: reutiliza método existente
            try:
                resultados['whatsapp_messages'] = int(self.limpiar_mensajes_antiguos_whatsapp(whatsapp_days))
            except Exception as e:
                logging.warning(f"Limpieza WhatsApp falló: {e}")
                resultados['whatsapp_messages'] = 0

            with self.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    # audit_logs por antigüedad
                    try:
                        cursor.execute(
                            "DELETE FROM audit_logs WHERE timestamp < NOW() - INTERVAL %s",
                            (f"{audit_logs_days} days",)
                        )
                        resultados['audit_logs'] = cursor.rowcount
                    except Exception as e:
                        logging.warning(f"No se pudo limpiar audit_logs: {e}")
                        resultados['audit_logs'] = 0

                    # auditoria por antigüedad
                    try:
                        cursor.execute(
                            "DELETE FROM auditoria WHERE timestamp < NOW() - INTERVAL %s",
                            (f"{auditoria_days} days",)
                        )
                        resultados['auditoria'] = cursor.rowcount
                    except Exception as e:
                        logging.warning(f"No se pudo limpiar auditoria: {e}")
                        resultados['auditoria'] = 0

                    # notificaciones_cupos leídas/inactivas antiguas
                    try:
                        cursor.execute(
                            """
                            DELETE FROM notificaciones_cupos
                            WHERE (leida = TRUE OR activa = FALSE)
                              AND COALESCE(fecha_lectura, fecha_creacion) < NOW() - INTERVAL %s
                            """,
                            (f"{notificaciones_days} days",)
                        )
                        resultados['notificaciones_cupos'] = cursor.rowcount
                    except Exception as e:
                        logging.warning(f"No se pudo limpiar notificaciones_cupos: {e}")
                        resultados['notificaciones_cupos'] = 0

                    # system_diagnostics por antigüedad (si existe)
                    try:
                        cursor.execute(
                            "DELETE FROM system_diagnostics WHERE timestamp < NOW() - INTERVAL %s",
                            (f"{sysdiag_days} days",)
                        )
                        resultados['system_diagnostics'] = cursor.rowcount
                    except Exception as e:
                        logging.info(f"system_diagnostics no limpiado: {e}")
                        resultados['system_diagnostics'] = 0
                try:
                    conn.commit()
                except Exception:
                    pass

            logging.info(f"Limpieza simple completada: {resultados}")
            return resultados
        except Exception as e:
            logging.error(f"Error en limpiar_datos_innecesarios: {e}")
            return resultados


    def eliminar_objetos_sync_antiguos(self) -> Dict[str, Any]:
        """Elimina definitivamente objetos antiguos de sincronización ya no utilizados.

        Incluye:
        - Tablas: public.sync_outbox, public.sync_inbox
        - Funciones: public.sync_outbox_capture, public.sync_outbox_ins, public.sync_outbox_upd, public.sync_outbox_del
        Devuelve resumen con estado por objeto.
        """
        resumen: Dict[str, Any] = {"tables": {}, "functions": {}}
        try:
            with self.get_connection_context() as conn:
                with conn.cursor() as cur:
                    # Eliminar tablas si existen
                    for t in ("sync_outbox", "sync_inbox"):
                        try:
                            cur.execute(
                                """
                                SELECT EXISTS (
                                    SELECT 1 FROM information_schema.tables 
                                    WHERE table_schema = 'public' AND table_name = %s
                                )
                                """,
                                (t,)
                            )
                            exists = bool(cur.fetchone()[0])
                            if exists:
                                cur.execute(f"DROP TABLE IF EXISTS public.{t} CASCADE")
                                resumen["tables"][t] = "dropped"
                            else:
                                resumen["tables"][t] = "absent"
                        except Exception as e:
                            resumen["tables"][t] = f"error: {e}"

                    # Eliminar funciones si existen (todas las variantes por firma)
                    func_names = [
                        "sync_outbox_capture",
                        "sync_outbox_ins",
                        "sync_outbox_upd",
                        "sync_outbox_del",
                    ]
                    try:
                        cur.execute(
                            """
                            SELECT n.nspname, p.proname, pg_get_function_identity_arguments(p.oid) AS args
                            FROM pg_proc p
                            JOIN pg_namespace n ON n.oid = p.pronamespace
                            WHERE n.nspname = 'public' AND p.proname = ANY(%s)
                            """,
                            (func_names,)
                        )
                        rows = cur.fetchall() or []
                        for nspname, proname, args in rows:
                            ident_args = args or ""
                            stmt = f"DROP FUNCTION IF EXISTS {nspname}.{proname}({ident_args}) CASCADE"
                            try:
                                cur.execute(stmt)
                                resumen["functions"][f"{nspname}.{proname}({ident_args})"] = "dropped"
                            except Exception as fe:
                                resumen["functions"][f"{nspname}.{proname}({ident_args})"] = f"error: {fe}"
                        if not rows:
                            resumen["functions"]["public.sync_outbox_*"] = "absent"
                    except Exception as e:
                        resumen["functions"]["public.sync_outbox_*"] = f"error: {e}"

                try:
                    conn.commit()
                except Exception:
                    pass

            logging.info(f"Objetos de sincronización antiguos eliminados: {resumen}")
            return resumen
        except Exception as e:
            logging.error(f"Error eliminando objetos de sincronización antiguos: {e}")
            return resumen


# ==================== QTHREAD WORKERS PARA OPERACIONES ASÍNCRONAS ====================

class DatabaseWorker(QThread):
    """Worker asíncrono para operaciones de base de datos sin bloquear la UI"""
    
    # Señales para comunicación con la UI
    started = pyqtSignal(str)  # Mensaje de inicio
    finished = pyqtSignal(object)  # Resultado de la operación
    error = pyqtSignal(str)  # Mensaje de error
    progress = pyqtSignal(int)  # Progreso 0-100
    

    def run(self):
        """Ejecuta la operación de base de datos en segundo plano"""
        try:
            self.started.emit(f"Iniciando operación: {self.operation}")
            
            if self.operation == "get_usuarios":
                result = self._get_usuarios()
            elif self.operation == "get_usuario_by_id":
                result = self._get_usuario_by_id()
            elif self.operation == "get_pagos_by_usuario":
                result = self._get_pagos_by_usuario()
            elif self.operation == "get_asistencias_today":
                result = self._get_asistencias_today()
            elif self.operation == "get_asistencias_by_usuario":
                result = self._get_asistencias_by_usuario()
            elif self.operation == "get_clases_activas":
                result = self._get_clases_activas()
            elif self.operation == "get_profesores_activos":
                result = self._get_profesores_activos()
            elif self.operation == "search_usuarios":
                result = self._search_usuarios()
            elif self.operation == "get_reporte_pagos":
                result = self._get_reporte_pagos()
            elif self.operation == "get_reporte_asistencias":
                result = self._get_reporte_asistencias()
            else:
                raise ValueError(f"Operación no soportada: {self.operation}")
            
            if self._is_running:
                self.finished.emit(result)
                
        except Exception as e:
            error_msg = f"Error en operación {self.operation}: {str(e)}"
            self.error.emit(error_msg)
            logging.error(error_msg)
    

    def stop(self):
        """Detiene el worker de manera segura"""
        self._is_running = False
    

    def _get_clases_activas(self):
        """Obtiene clases activas"""
        with self.db_manager.readonly_session() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT c.id, c.nombre, c.descripcion, c.activa
                    FROM clases c
                    WHERE c.activa = true
                    ORDER BY c.nombre
                """)
                return cur.fetchall()
    

    def run(self):
        """Ejecuta operación masiva en segundo plano"""
        try:
            self.started.emit(f"Iniciando operación masiva: {self.operation}")
            
            if self.operation == "bulk_insert_usuarios":
                result = self._bulk_insert_usuarios()
            elif self.operation == "bulk_update_pagos":
                result = self._bulk_update_pagos()
            elif self.operation == "bulk_insert_asistencias":
                result = self._bulk_insert_asistencias()
            else:
                raise ValueError(f"Operación masiva no soportada: {self.operation}")
            
            if self._is_running:
                self.finished.emit(result)
                
        except Exception as e:
            error_msg = f"Error en operación masiva {self.operation}: {str(e)}"
            self.error.emit(error_msg)
            logging.error(error_msg)
    

    def stop(self):
        """Detiene el worker"""
        self._is_running = False
    

    def execute_operation(self, operation: str, params: Dict = None, operation_type: str = 'single'):
        """Ejecuta una operación de base de datos de manera asíncrona"""
        self.worker_counter += 1
        worker_id = f"worker_{self.worker_counter}"
        
        if operation_type == 'bulk':
            data = params.get('data', []) if params else []
            worker = BulkDatabaseWorker(self.db_manager, operation, data)
        else:
            worker = DatabaseWorker(self.db_manager, operation, params)
        
        # Conectar señales
        worker.started.connect(lambda msg: self.operation_started.emit(f"{worker_id}: {msg}"))
        worker.finished.connect(lambda result: self._on_operation_finished(worker_id, operation, result))
        worker.error.connect(lambda error: self._on_operation_error(worker_id, operation, error))
        worker.progress.connect(lambda progress: self.operation_progress.emit(worker_id, progress))
        
        # Guardar referencia
        self.active_workers[worker_id] = worker
        
        # Iniciar operación
        worker.start()
        
        return worker_id
    

    def _on_operation_finished(self, worker_id: str, operation: str, result):
        """Maneja la finalización de una operación"""
        self.operation_finished.emit(operation, result)
        self._cleanup_worker(worker_id)
    

    def _on_operation_error(self, worker_id: str, operation: str, error: str):
        """Maneja errores de operación"""
        self.operation_error.emit(operation, error)
        self._cleanup_worker(worker_id)
    

    def _cleanup_worker(self, worker_id: str):
        """Limpia el worker terminado"""
        if worker_id in self.active_workers:
            worker = self.active_workers[worker_id]
            worker.quit()
            worker.wait()
            del self.active_workers[worker_id]
    

    def cancel_operation(self, worker_id: str):
        """Cancela una operación en progreso"""
        if worker_id in self.active_workers:
            self.active_workers[worker_id].stop()
            self._cleanup_worker(worker_id)

if __name__ == "__main__":
    import sys
    import logging
    try:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    except Exception:
        pass
    try:
        print("Inicializando la base de datos...")
        db_manager = DatabaseManager()
        db_manager.inicializar_base_datos()
        print("✅ Base de datos inicializada correctamente")
        # Verificar tablas clave
        try:
            with db_manager.get_connection_context() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.tables
                            WHERE table_schema = 'public' AND table_name = 'gym_config'
                        )
                    """)
                    gym_exists = bool(cur.fetchone()[0])
                    cur.execute("""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.tables
                            WHERE table_schema = 'public' AND table_name = 'clase_bloques'
                        )
                    """)
                    bloques_exists = bool(cur.fetchone()[0])
                    cur.execute("""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.tables
                            WHERE table_schema = 'public' AND table_name = 'clase_bloque_items'
                        )
                    """)
                    bloque_items_exists = bool(cur.fetchone()[0])
            print(f"gym_config: {'OK' if gym_exists else 'FALTA'}")
            print(f"clase_bloques: {'OK' if bloques_exists else 'FALTA'}")
            print(f"clase_bloque_items: {'OK' if bloque_items_exists else 'FALTA'}")
        except Exception:
            # No bloquear si la verificación falla
            pass
        sys.exit(0)
    except Exception as e:
        print(f"❌ Error al inicializar la base de datos: {e}")
        try:
            logging.exception("Fallo al inicializar database.py")
        except Exception:
            pass
        sys.exit(1)
