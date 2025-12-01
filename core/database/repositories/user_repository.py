from datetime import datetime, date, timedelta
from typing import List, Optional, Dict, Set, Tuple, Any
import json
import logging
import psycopg2
import psycopg2.extras
from .base import BaseRepository
from ..connection import database_retry
from ...models import Usuario, Pago, Asistencia, Ejercicio, Rutina, RutinaEjercicio, Clase, ClaseHorario, ClaseUsuario, EjercicioGrupo, EjercicioGrupoItem, TipoCuota, UsuarioNota, Etiqueta, UsuarioEtiqueta, UsuarioEstado
from ...utils import get_gym_name

class UserRepository(BaseRepository):
    pass

    # --- Methods moved from DatabaseManager ---

    def get_owner_user_cached(self, ttl_seconds: int = 600, timeout_ms: int = 1200) -> Optional[dict]:
        """Obtiene un usuario 'dueño' con caché TTL; cae a un stub mínimo si RLS impide SELECT."""
        try:
            now = time.time()
            exp = float(self._owner_cache.get('user_expiry', 0.0) or 0.0)
            usr = self._owner_cache.get('user')
            if usr and now < exp:
                return usr
            # Intento DB (puede estar protegido por RLS)
            try:
                if hasattr(self, 'obtener_usuario_por_rol'):
                    usr = self.obtener_usuario_por_rol('dueño', timeout_ms=timeout_ms)
            except Exception:
                usr = None
            # Stub mínimo para evitar bloqueos
            if not usr:
                usr = {'id': 1, 'rol': 'dueño', 'nombre': 'Dueño'}
            try:
                self._owner_cache['user'] = usr
                self._owner_cache['user_expiry'] = now + max(60, ttl_seconds)
            except Exception:
                pass
            return usr
        except Exception:
            return None


    def crear_usuario(self, usuario: Usuario) -> int:
        """Crea un nuevo usuario en la base de datos"""
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                sql = """INSERT INTO usuarios (nombre, dni, telefono, pin, rol, activo, tipo_cuota, notas) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id"""
                params = (usuario.nombre, usuario.dni, usuario.telefono, usuario.pin,
                         usuario.rol, usuario.activo, usuario.tipo_cuota, usuario.notas)
                cursor.execute(sql, params)
                usuario_id = cursor.fetchone()[0]
                
                # Inicializar fecha de próximo vencimiento para socios (1 mes desde hoy)
                if usuario.rol == 'socio':
                    from datetime import date
                    fecha_registro = date.today()
                    self.actualizar_fecha_proximo_vencimiento(usuario_id, fecha_registro)
                
                conn.commit()
                
                # Registrar en auditoría
                if self.audit_logger:
                    new_values = {
                        'id': usuario_id,
                        'nombre': usuario.nombre,
                        'dni': usuario.dni,
                        'telefono': usuario.telefono,
                        'rol': usuario.rol,
                        'activo': usuario.activo,
                        'tipo_cuota': usuario.tipo_cuota,
                        'notas': usuario.notas
                    }
                    self.audit_logger.log_operation('CREATE', 'usuarios', usuario_id, None, new_values)
                
                # Crear perfil de profesor automáticamente si el rol es 'profesor'
                if usuario.rol == 'profesor':
                    self.crear_profesor(usuario_id)
                
                # Limpiar cache de usuarios
                self.cache.invalidate('usuarios')
                
                # Enviar mensaje de bienvenida automático por WhatsApp
                self._enviar_mensaje_bienvenida_automatico(usuario_id, usuario.nombre, usuario.telefono)
                
                
                
                return usuario_id

    @database_retry()

    def registrar_usuarios_batch(self, items: List[Dict[str, Any]], *, skip_duplicates: bool = True, validate_data: bool = True) -> Dict[str, Any]:
        """Registra usuarios en lote de forma chunky y optimizada.

        - Normaliza datos (nombre, rol, activo, etc.).
        - Si hay 'dni', deduplica por dni. Si no hay 'dni', inserta tal cual.
        - Inserta en batch con RETURNING para obtener IDs.
        - Actualiza fecha_proximo_vencimiento para nuevos socios en un único UPDATE.
        - Crea perfiles de profesor para nuevos profesores con un único INSERT batch.
        - Invalida cache y encola sincronización upstream en un único paso.
        """
        result: Dict[str, Any] = {
            'insertados': [],
            'actualizados': [],
            'omitidos': [],
        }
        if not items:
            return result

        # Pre-procesamiento: normalizar y validar
        prepped: List[Dict[str, Any]] = []
        for raw in items:
            try:
                nombre = str((raw.get('nombre') or '')).strip()
                dni = str(raw.get('dni')).strip() if raw.get('dni') is not None else None
                telefono = str(raw.get('telefono')).strip() if raw.get('telefono') is not None else None
                pin = str(raw.get('pin')).strip() if raw.get('pin') is not None else None
                rol = str((raw.get('rol') or 'socio')).strip().lower()
                activo = bool(raw.get('activo', True))
                tipo_cuota = raw.get('tipo_cuota')
                notas = raw.get('notas')
                if validate_data and not nombre:
                    result['omitidos'].append({'motivo': 'nombre vacío', 'item': raw})
                    continue
                prepped.append({
                    'nombre': nombre,
                    'dni': dni,
                    'telefono': telefono,
                    'pin': pin,
                    'rol': rol,
                    'activo': activo,
                    'tipo_cuota': tipo_cuota,
                    'notas': notas,
                })
            except Exception as e:
                result['omitidos'].append({'motivo': f'error normalizando: {e}', 'item': raw})

        if not prepped:
            return result

        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                # Detectar dnis ya existentes (si se proveen)
                dnis: List[str] = [p['dni'] for p in prepped if p.get('dni')]
                existing_by_dni: Set[str] = set()
                if dnis:
                    try:
                        cursor.execute("SELECT dni FROM usuarios WHERE dni = ANY(%s)", (dnis,))
                        rows = cursor.fetchall() or []
                        existing_by_dni = {str(r.get('dni')).strip() for r in rows if r.get('dni') is not None}
                    except Exception:
                        existing_by_dni = set()

                nuevos: List[Dict[str, Any]] = []
                existentes: List[Dict[str, Any]] = []
                for p in prepped:
                    if p.get('dni') and p['dni'] in existing_by_dni:
                        existentes.append(p)
                    else:
                        nuevos.append(p)

                # Inserción en lote
                inserted_rows: List[Dict[str, Any]] = []
                if nuevos:
                    tpl_values = [
                        (
                            n['nombre'], n.get('dni'), n.get('telefono'), n.get('pin'),
                            n['rol'], n['activo'], n.get('tipo_cuota'), n.get('notas')
                        )
                        for n in nuevos
                    ]
                    sql_insert = (
                        "INSERT INTO usuarios (nombre, dni, telefono, pin, rol, activo, tipo_cuota, notas) "
                        "VALUES %s RETURNING id, dni, rol"
                    )
                    psycopg2.extras.execute_values(cursor, sql_insert, tpl_values, page_size=max(100, min(1000, len(tpl_values))))
                    for r in cursor.fetchall() or []:
                        rid = r.get('id')
                        inserted_rows.append({'id': rid, 'dni': r.get('dni'), 'rol': (r.get('rol') or '').strip().lower()})
                        result['insertados'].append(rid)

                # Actualización en lote por DNI si se solicita
                if existentes and not skip_duplicates:
                    upd_values = [
                        (
                            e.get('dni'), e['nombre'], e.get('telefono'), e.get('pin'), e['rol'], e['activo'], e.get('tipo_cuota'), e.get('notas')
                        )
                        for e in existentes if e.get('dni')
                    ]
                    sql_update = (
                        "UPDATE usuarios AS u SET "
                         "  nombre = v.nombre, "
                         "  telefono = v.telefono, "
                         "  pin = v.pin, "
                         "  rol = v.rol, "
                         "  activo = v.activo, "
                         "  tipo_cuota = v.tipo_cuota, "
                         "  notas = v.notas "
                         "FROM (VALUES %s) AS v(dni, nombre, telefono, pin, rol, activo, tipo_cuota, notas) "
                         "WHERE u.dni = v.dni"
                    )
                    psycopg2.extras.execute_values(cursor, sql_update, upd_values, page_size=max(100, min(1000, len(upd_values))))
                    # Obtener IDs actualizados
                    try:
                        cursor.execute("SELECT id FROM usuarios WHERE dni = ANY(%s)", ([e[0] for e in upd_values],))
                        for r in cursor.fetchall() or []:
                            result['actualizados'].append(r.get('id'))
                    except Exception:
                        pass
                else:
                    # Marcar existentes como omitidos si se decide saltar duplicados
                    for e in existentes:
                        result['omitidos'].append({'motivo': 'dni duplicado', 'item': e})

                # Actualización de fecha_proximo_vencimiento para nuevos socios en un único UPDATE
                socios_ids = [r['id'] for r in inserted_rows if (r.get('rol') or '') == 'socio']
                if socios_ids:
                    try:
                        cursor.execute(
                            "UPDATE usuarios SET fecha_proximo_vencimiento = CURRENT_DATE + INTERVAL '1 month' WHERE id = ANY(%s)",
                            (socios_ids,)
                        )
                    except Exception as e:
                        logging.debug(f"Aviso al actualizar vencimientos batch: {e}")

                # Crear perfiles profesor para nuevos profesores en un único INSERT batch (valores por defecto)
                profesores_ids = [r['id'] for r in inserted_rows if (r.get('rol') or '') == 'profesor']
                if profesores_ids:
                    try:
                        tpl_prof = [
                            (pid, '', '', 0, 0.0, date.today(), '', '')
                            for pid in profesores_ids
                        ]
                        sql_prof = (
                            "INSERT INTO profesores (usuario_id, especialidades, certificaciones, experiencia_años, "
                            "tarifa_por_hora, fecha_contratacion, biografia, telefono_emergencia) VALUES %s"
                        )
                        psycopg2.extras.execute_values(cursor, sql_prof, tpl_prof, page_size=max(100, min(1000, len(tpl_prof))))
                    except Exception as e:
                        logging.debug(f"Aviso al crear perfiles profesor batch: {e}")

                conn.commit()

                # Auditoría y sincronización upstream
                try:
                    if self.audit_logger:
                        self.audit_logger.log_operation('BULK_CREATE', 'usuarios', None, None, {
                            'insertados': len(result['insertados']),
                            'actualizados': len(result['actualizados']),
                            'omitidos': len(result['omitidos']),
                        })
                except Exception:
                    pass

                try:
                    payloads = []
                    for r in nuevos:
                        payloads.append({
                            'dni': r.get('dni'),
                            'nombre': r.get('nombre'),
                            'telefono': r.get('telefono'),
                            'tipo_cuota': r.get('tipo_cuota'),
                            'active': bool(r.get('activo')), 
                            'rol': r.get('rol'),
                        })
                    
                except Exception:
                    pass

                # Invalidar cache
                self.cache.invalidate('usuarios')

        return result


    def obtener_usuario(self, usuario_id: int) -> Optional[Usuario]:
        """Obtiene un usuario por su ID"""
        # Intento rápido: caché por ID
        try:
            cache_key = ('id', int(usuario_id))
            cached = self.cache.get('usuarios', cache_key)
            if cached is not None:
                return cached
        except Exception:
            pass

        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                # Selección de columnas dinámica para soportar esquemas sin 'apellido'/'email'
                _cols = self.get_table_columns('usuarios')
                _desired = ['id','nombre','dni','telefono','pin','rol','notas','fecha_registro','activo','tipo_cuota','fecha_proximo_vencimiento','cuotas_vencidas','ultimo_pago','apellido','email']
                _sel = ", ".join([c for c in _desired if c in (_cols or [])]) or "id, nombre, rol"
                cursor.execute(
                    f"SELECT {_sel} FROM usuarios WHERE id = %s",
                    (usuario_id,)
                )
                row = cursor.fetchone()
                if row:
                    data = dict(row)
                    if 'apellido' in data:
                        apellido = (data.get('apellido') or '').strip()
                        nombre = (data.get('nombre') or '').strip()
                        data['nombre'] = (f"{nombre} {apellido}".strip() if nombre or apellido else nombre or apellido)
                        data.pop('apellido', None)
                    else:
                        data['nombre'] = (data.get('nombre') or '').strip()
                    data.pop('email', None)
                    allowed_fields = set(Usuario.__dataclass_fields__.keys())
                    filtered = {k: v for k, v in data.items() if k in allowed_fields}
                    user_obj = Usuario(**filtered)
                    # Guardar en caché
                    try:
                        self.cache.set('usuarios', ('id', int(usuario_id)), user_obj)
                    except Exception:
                        pass
                    return user_obj
                return None
    

    def obtener_usuario_por_id(self, usuario_id: int) -> Optional[Usuario]:
        """Obtiene un usuario por su ID (alias para obtener_usuario)"""
        return self.obtener_usuario(usuario_id)


    def obtener_todos_usuarios(self) -> List[Usuario]:
        """Obtiene todos los usuarios optimizado: columnas específicas, timeouts y caché"""
        # 1) Intento rápido: caché en memoria
        try:
            cached = self.cache.get('usuarios', ('all',))
            if cached is not None:
                return cached
        except Exception:
            pass

        
        # 3) Consulta endurecida con columnas específicas y timeouts (readonly_session)
        try:
            with self.readonly_session(lock_ms=300, statement_ms=2500, idle_s=1, seqscan_off=True) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    try:
                        cursor.execute(
                            """
                            SELECT 
                                id, nombre, dni, telefono, pin, rol, notas, 
                                fecha_registro, activo, tipo_cuota, 
                                fecha_proximo_vencimiento, cuotas_vencidas, ultimo_pago
                            FROM usuarios
                            ORDER BY CASE 
                                WHEN rol = 'dueño' THEN 0 
                                WHEN rol = 'profesor' THEN 1 
                                ELSE 2 
                            END, nombre ASC
                            """
                        )
                    except Exception as e:
                        # Manejo robusto ante lock timeout: primero intentamos devolver caché,
                        # luego un retry sin espera de lock; si todo falla, devolvemos lista vacía.
                        msg = str(e).lower()
                        if 'lock timeout' in msg or 'canceling statement due to lock timeout' in msg:
                            # 1) Fallback inmediato a cachés
                            try:
                                cached_mem = self.cache.get('usuarios', ('all',))
                                if cached_mem:
                                    return cached_mem
                            except Exception:
                                pass
                            # 2) Pequeña espera y retry con lock_timeout=0 y sin optimizaciones de index
                            try:
                                import time
                                time.sleep(0.5)
                            except Exception:
                                pass
                            try:
                                with self.readonly_session(lock_ms=0, statement_ms=6000, idle_s=2, seqscan_off=False) as conn2:
                                    with conn2.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor2:
                                        cursor2.execute(
                                            """
                                            SELECT 
                                                id, nombre, dni, telefono, pin, rol, notas, 
                                                fecha_registro, activo, tipo_cuota, 
                                                fecha_proximo_vencimiento, cuotas_vencidas, ultimo_pago
                                            FROM usuarios
                                            ORDER BY CASE 
                                                WHEN rol = 'dueño' THEN 0 
                                                WHEN rol = 'profesor' THEN 1 
                                                ELSE 2 
                                            END, nombre ASC
                                            """
                                        )
                                        # Capturar filas aquí para evitar 'cursor already closed'
                                        fallback_rows = cursor2.fetchall()
                                        # Procesar directamente resultados del retry
                                        usuarios = []
                                        for r in fallback_rows:
                                            data = dict(r)
                                            apellido = data.pop('apellido', None)
                                            nombre = (data.get('nombre') or '').strip()
                                            if apellido:
                                                try:
                                                    ap = str(apellido).strip()
                                                    if ap and ap not in nombre:
                                                        nombre = f"{nombre} {ap}".strip() if nombre else ap
                                                except Exception:
                                                    pass
                                            data['nombre'] = nombre
                                            data.pop('email', None)
                                            allowed = {
                                                'id', 'nombre', 'dni', 'telefono', 'pin', 'rol', 'notas',
                                                'fecha_registro', 'activo', 'tipo_cuota',
                                                'fecha_proximo_vencimiento', 'cuotas_vencidas', 'ultimo_pago'
                                            }
                                            filtered = {k: data.get(k) for k in allowed if k in data}
                                            usuarios.append(Usuario(**filtered))
                                        # Actualizar cachés y devolver
                                        try:
                                            self.cache.set('usuarios', ('all',), usuarios)
                                        except Exception:
                                            pass
                                        return usuarios
                            except Exception:
                                # 3) Último recurso: devolver lista vacía para evitar crash en UI
                                return []
                        else:
                            # Errores no relacionados con timeout: propagar para manejo superior
                            raise
                    usuarios: List[Usuario] = []
                    for r in cursor.fetchall():
                        data = dict(r)
                        # Unificar nombre completo si existiera 'apellido' en esquemas alternativos:
                        apellido = data.pop('apellido', None)
                        nombre = (data.get('nombre') or '').strip()
                        if apellido:
                            try:
                                ap = str(apellido).strip()
                                if ap and ap not in nombre:
                                    nombre = f"{nombre} {ap}".strip() if nombre else ap
                            except Exception:
                                pass
                        data['nombre'] = nombre
                        # Campos no utilizados
                        data.pop('email', None)
                        allowed = {
                            'id', 'nombre', 'dni', 'telefono', 'pin', 'rol', 'notas',
                            'fecha_registro', 'activo', 'tipo_cuota',
                            'fecha_proximo_vencimiento', 'cuotas_vencidas', 'ultimo_pago'
                        }
                        filtered = {k: data.get(k) for k in allowed if k in data}
                        usuarios.append(Usuario(**filtered))

                    # Actualizar cachés (memoria + persistente) para acelerar futuras lecturas
                    try:
                        self.cache.set('usuarios', ('all',), usuarios)
                    except Exception:
                        pass
                    return usuarios
        except Exception as e:
            # Log estructurado para detectar origen del bloqueo
            try:
                logging.error(f"Error de Carga de Usuarios: {str(e)}")
            except Exception:
                pass
            # 4) Fallback: intentar devolver lo que haya en caché persistente o memoria
            try:
                cached = self.cache.get('usuarios', ('all',))
                if cached is not None:
                    return cached
            except Exception:
                pass
            return []


    def obtener_usuario_por_rol(self, rol: str, timeout_ms: Optional[int] = None) -> Optional[Usuario]:
        """Obtiene el primer usuario activo por rol, con endurecimiento.

        - Lectura modo read-only con timeouts
        - Reintentos básicos ante pérdidas de conexión
        - Caché para roles críticos (dueño)
        - Gating durante inicialización para evitar bloqueos
        """
        role_norm = (rol or '').strip().lower()
        # Si se pide el dueño, evitar golpear la DB por RLS y durante el arranque
        if role_norm == 'dueño':
            try:
                owner_cached = self.get_owner_user_cached(ttl_seconds=600, timeout_ms=(timeout_ms or 1200))
                if isinstance(owner_cached, dict):
                    allowed_fields = set(Usuario.__dataclass_fields__.keys())
                    filtered = {k: v for k, v in owner_cached.items() if k in allowed_fields}
                    try:
                        return Usuario(**filtered)
                    except Exception:
                        pass
                if isinstance(owner_cached, Usuario):
                    return owner_cached
            except Exception:
                pass
            # Si todavía se está inicializando la base, salir temprano
            try:
                if getattr(self, '_initializing', False):
                    return None
            except Exception:
                pass
        # Cachear dueño explícitamente para evitar hits repetidos en arranque
        cache_key = ("first_by_rol", rol)
        if rol.strip().lower() == 'dueño':
            try:
                cached = self.cache.get('usuarios', cache_key)
                if cached is not None:
                    return cached
            except Exception:
                pass

        max_retries = 3
        stmt_ms = 2000
        try:
            if isinstance(timeout_ms, int) and timeout_ms > 0:
                stmt_ms = max(300, min(timeout_ms, 5000))
        except Exception:
            stmt_ms = 2000
        for attempt in range(max_retries):
            try:
                with self.readonly_session(lock_ms=800, statement_ms=stmt_ms, idle_s=2, seqscan_off=True) as conn:
                    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                        # Seleccionar columnas explícitas para minimizar transferencia
                        _cols = self.get_table_columns('usuarios')
                        _desired = ['id','nombre','dni','telefono','pin','rol','notas','fecha_registro','activo','tipo_cuota','fecha_proximo_vencimiento','cuotas_vencidas','ultimo_pago','apellido','email']
                        _sel = ", ".join([c for c in _desired if c in (_cols or [])]) or "id, nombre, rol"
                        cursor.execute(
                            f"SELECT {_sel} FROM usuarios WHERE rol = %s AND activo = TRUE ORDER BY id LIMIT 1",
                            (rol,)
                        )
                        row = cursor.fetchone()
                        if row:
                            data = dict(row)
                            if 'apellido' in data:
                                nombre = (data.get('nombre') or '').strip()
                                apellido = (data.get('apellido') or '').strip()
                                data['nombre'] = (f"{nombre} {apellido}".strip() if nombre or apellido else nombre or apellido)
                                data.pop('apellido', None)
                            data.pop('email', None)
                            allowed_fields = set(Usuario.__dataclass_fields__.keys())
                            filtered = {k: v for k, v in data.items() if k in allowed_fields}
                            usuario = Usuario(**filtered)
                            # Cachear dueño si corresponde
                            if rol.strip().lower() == 'dueño':
                                try:
                                    self.cache.set('usuarios', cache_key, usuario)
                                except Exception:
                                    pass
                            return usuario
                        # Fallback: si no hay resultado, intentar devolver caché del dueño
                        if rol.strip().lower() == 'dueño':
                            try:
                                cached = self.cache.get('usuarios', cache_key)
                                if cached is not None:
                                    return cached
                            except Exception:
                                pass
                        return None
            except psycopg2.OperationalError as e:
                if "server closed the connection unexpectedly" in str(e) and attempt < max_retries - 1:
                    self.logger.warning(f"Conexión perdida, reintentando... (intento {attempt + 1}/{max_retries})")
                    self._invalidate_connection()
                    time.sleep(0.5 * (attempt + 1))
                    continue
                else:
                    # Fallback ante timeouts/locks: intentar devolver caché si es dueño
                    logging.error(f"Error de conexión PostgreSQL al obtener usuario por rol {rol}: {str(e)}")
                    if rol.strip().lower() == 'dueño':
                        try:
                            cached = self.cache.get('usuarios', cache_key)
                            if cached is not None:
                                return cached
                        except Exception:
                            pass
                    return None
            except Exception as e:
                msg = str(e).lower()
                # Manejo específico de lock timeout: retry rápido sin esperar lock
                if 'lock timeout' in msg or 'canceling statement due to lock timeout' in msg:
                    try:
                        # Reintento ligero sin esperar lock
                        retry_stmt_ms = 3000
                        try:
                            if isinstance(timeout_ms, int) and timeout_ms > 0:
                                retry_stmt_ms = max(500, min(timeout_ms, 5000))
                        except Exception:
                            pass
                        with self.readonly_session(lock_ms=0, statement_ms=retry_stmt_ms, idle_s=2, seqscan_off=False) as conn2:
                            with conn2.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as c2:
                                _cols2 = self.get_table_columns('usuarios')
                                _desired2 = ['id','nombre','dni','telefono','pin','rol','notas','fecha_registro','activo','tipo_cuota','fecha_proximo_vencimiento','cuotas_vencidas','ultimo_pago','apellido','email']
                                _sel2 = ", ".join([c for c in _desired2 if c in (_cols2 or [])]) or "id, nombre, rol"
                                c2.execute(
                                    f"SELECT {_sel2} FROM usuarios WHERE rol = %s AND activo = TRUE ORDER BY id LIMIT 1",
                                    (rol,)
                                )
                                r2 = c2.fetchone()
                                if r2:
                                    data = dict(r2)
                                    if 'apellido' in data:
                                        nombre = (data.get('nombre') or '').strip()
                                        apellido = (data.get('apellido') or '').strip()
                                        data['nombre'] = (f"{nombre} {apellido}".strip() if nombre or apellido else nombre or apellido)
                                        data.pop('apellido', None)
                                    data.pop('email', None)
                                    allowed_fields = set(Usuario.__dataclass_fields__.keys())
                                    filtered = {k: v for k, v in data.items() if k in allowed_fields}
                                    usuario = Usuario(**filtered)
                                    if rol.strip().lower() == 'dueño':
                                        try:
                                            self.cache.set('usuarios', cache_key, usuario)
                                        except Exception:
                                            pass
                                    return usuario
                    except Exception:
                        pass
                    # Fallback: caché si es dueño
                    if rol.strip().lower() == 'dueño':
                        try:
                            cached = self.cache.get('usuarios', cache_key)
                            if cached is not None:
                                return cached
                        except Exception:
                            pass
                    return None
                # Otros errores: log y fallback a caché si es dueño
                logging.error(f"Error al obtener usuario por rol {rol}: {str(e)}")
                if rol.strip().lower() == 'dueño':
                    try:
                        cached = self.cache.get('usuarios', cache_key)
                        if cached is not None:
                            return cached
                    except Exception:
                        pass
                return None
        # Si todos los intentos fallan, último fallback a caché para dueño
        if rol.strip().lower() == 'dueño':
            try:
                cached = self.cache.get('usuarios', cache_key)
                if cached is not None:
                    return cached
            except Exception:
                pass
        return None


    def desactivar_usuarios_por_falta_de_pago(self) -> List[Dict]:
        """Desactiva usuarios que no han pagado en los últimos 90 días"""
        fecha_limite = date.today() - timedelta(days=90)
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                # Buscar usuarios sin pagos recientes
                find_sql = """
                    SELECT id, nombre FROM usuarios 
                    WHERE activo = true AND rol = 'socio' 
                    AND id NOT IN (
                        SELECT DISTINCT p.usuario_id FROM pagos p 
                        WHERE (p.año || '-' || LPAD(p.mes::text, 2, '0') || '-01')::date > %s
                    )
                """
                cursor.execute(find_sql, (fecha_limite,))
                columns = [desc[0] for desc in cursor.description]
                users_to_deactivate = [dict(zip(columns, row)) for row in cursor.fetchall()]
                
                if not users_to_deactivate:
                    return []
                
                # Desactivar usuarios
                user_ids_to_deactivate = [user['id'] for user in users_to_deactivate]
                placeholders = ','.join(['%s'] * len(user_ids_to_deactivate))
                update_sql = f"UPDATE usuarios SET activo = false WHERE id IN ({placeholders})"
                cursor.execute(update_sql, user_ids_to_deactivate)
                conn.commit()
                
                return users_to_deactivate


    def obtener_usuarios_por_rol(self, rol: str) -> List[Usuario]:
        """Obtiene todos los usuarios por rol específico."""
        # Cache para listas por rol (socio/profesor/miembro)
        cache_key = ("list_by_rol", rol)
        try:
            cached = self.cache.get('usuarios', cache_key)
            if cached is not None:
                return cached
        except Exception:
            pass
        try:
            with self.readonly_session(lock_ms=6000, statement_ms=5000, idle_s=2, seqscan_off=True) as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                _cols = self.get_table_columns('usuarios')
                _desired = ['id','nombre','dni','telefono','pin','rol','notas','fecha_registro','activo','tipo_cuota','fecha_proximo_vencimiento','cuotas_vencidas','ultimo_pago','apellido','email']
                _sel = ", ".join([c for c in _desired if c in (_cols or [])]) or "id, nombre, rol"
                cursor.execute(
                    f"SELECT {_sel} FROM usuarios WHERE rol = %s ORDER BY nombre",
                    (rol,)
                )
                rows = cursor.fetchall()
                usuarios: List[Usuario] = []
                allowed_fields = set(Usuario.__dataclass_fields__.keys())
                for row in rows:
                    data = dict(row)
                    if 'apellido' in data:
                        nombre = (data.get('nombre') or '').strip()
                        apellido = (data.get('apellido') or '').strip()
                        data['nombre'] = (f"{nombre} {apellido}".strip() if nombre or apellido else nombre or apellido)
                        data.pop('apellido', None)
                    data.pop('email', None)
                    filtered = {k: v for k, v in data.items() if k in allowed_fields}
                    usuarios.append(Usuario(**filtered))
                try:
                    self.cache.set('usuarios', cache_key, usuarios)
                except Exception:
                    pass
                return usuarios
        except Exception as e:
            logging.error(f"Error al obtener usuarios por rol {rol}: {str(e)}")
            return []


    def actualizar_usuario(self, usuario: Usuario):
        """Actualiza un usuario existente"""
        usuario_anterior = self.obtener_usuario(usuario.id)
        rol_anterior = usuario_anterior.rol if usuario_anterior else None
        
        old_values = None
        if self.audit_logger and usuario_anterior:
            old_values = {
                'id': usuario_anterior.id,
                'nombre': usuario_anterior.nombre,
                'dni': usuario_anterior.dni,
                'telefono': usuario_anterior.telefono,
                'rol': usuario_anterior.rol,
                'activo': usuario_anterior.activo,
                'tipo_cuota': usuario_anterior.tipo_cuota,
                'notas': usuario_anterior.notas
            }
        
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                sql = """UPDATE usuarios SET nombre = %s, dni = %s, telefono = %s, pin = %s, 
                        rol = %s, activo = %s, tipo_cuota = %s, notas = %s WHERE id = %s"""
                params = (usuario.nombre, usuario.dni, usuario.telefono, usuario.pin,
                         usuario.rol, usuario.activo, usuario.tipo_cuota, usuario.notas, usuario.id)
                cursor.execute(sql, params)
                conn.commit()
                
                # Registrar en auditoría
                if self.audit_logger:
                    new_values = {
                        'id': usuario.id,
                        'nombre': usuario.nombre,
                        'dni': usuario.dni,
                        'telefono': usuario.telefono,
                        'rol': usuario.rol,
                        'activo': usuario.activo,
                        'tipo_cuota': usuario.tipo_cuota,
                        'notas': usuario.notas
                    }
                    self.audit_logger.log_operation('UPDATE', 'usuarios', usuario.id, old_values, new_values)
                
                # Gestión automática de perfiles de profesor
                if rol_anterior != usuario.rol:
                    if usuario.rol == 'profesor' and rol_anterior != 'profesor':
                        self.crear_profesor(usuario.id)
                    elif rol_anterior == 'profesor' and usuario.rol != 'profesor':
                        perfil_profesor = self.obtener_profesor_por_usuario_id(usuario.id)
                        if perfil_profesor:
                            self.eliminar_profesor(perfil_profesor['id'])
                
                # Limpiar cache de usuarios
                try:
                    # Invalidaciones específicas: ficha y resumen, más lista completa
                    self.cache.invalidate('usuarios', ('id', int(usuario.id)))
                    self.cache.invalidate('usuarios', ('refs', int(usuario.id)))
                    self.cache.invalidate('usuarios', ('all',))
                except Exception:
                    pass
                # Invalidación global (compatibilidad)
                self.limpiar_cache_usuarios()

                

    @database_retry()

    def eliminar_usuario(self, usuario_id: int):
        """Elimina un usuario y todos sus datos relacionados"""
        user_to_delete = self.obtener_usuario(usuario_id)
        if user_to_delete and user_to_delete.rol == 'dueño': 
            raise PermissionError(f"El usuario con ID {usuario_id} tiene el rol 'dueño' y no puede ser eliminado.")
        
        old_values = None
        if self.audit_logger and user_to_delete:
            old_values = {
                'id': user_to_delete.id,
                'nombre': user_to_delete.nombre,
                'dni': user_to_delete.dni,
                'telefono': user_to_delete.telefono,
                'rol': user_to_delete.rol,
                'activo': user_to_delete.activo,
                'tipo_cuota': user_to_delete.tipo_cuota,
                'notas': user_to_delete.notas
            }
        
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                # Eliminar primero los mensajes de WhatsApp asociados
                cursor.execute("DELETE FROM whatsapp_messages WHERE user_id = %s", (usuario_id,))
                
                # Ahora eliminar el usuario
                cursor.execute("DELETE FROM usuarios WHERE id = %s", (usuario_id,))
                conn.commit()
                
                # Registrar en auditoría
                if self.audit_logger and old_values:
                    self.audit_logger.log_operation('DELETE', 'usuarios', usuario_id, old_values, None)
                
                # Limpiar cache de usuarios
                try:
                    self.cache.invalidate('usuarios', ('id', int(usuario_id)))
                    self.cache.invalidate('usuarios', ('refs', int(usuario_id)))
                    self.cache.invalidate('usuarios', ('all',))
                except Exception:
                    pass
                self.limpiar_cache_usuarios()

                


    def usuario_id_existe(self, usuario_id: int) -> bool:
        """Verifica si un ID de usuario ya existe en la base de datos"""
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1 FROM usuarios WHERE id = %s", (usuario_id,))
                return cursor.fetchone() is not None


    def obtener_resumen_referencias_usuario(self, usuario_id: int) -> dict:
        """Obtiene un resumen de conteos de referencias al usuario en tablas relacionadas mediante una sola consulta."""
        # Cache por usuario para evitar múltiples subconsultas repetidas
        try:
            cache_key = ('refs', int(usuario_id))
            cached = self.cache.get('usuarios', cache_key)
            if isinstance(cached, dict):
                return cached
        except Exception:
            pass

        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                try:
                    cursor.execute(
                        """
                        SELECT
                          (SELECT COUNT(*) FROM pagos WHERE usuario_id = %s)                AS pagos,
                          (SELECT COUNT(*) FROM asistencias WHERE usuario_id = %s)          AS asistencias,
                          (SELECT COUNT(*) FROM rutinas WHERE usuario_id = %s)              AS rutinas,
                          (SELECT COUNT(*) FROM clase_usuarios WHERE usuario_id = %s)       AS clase_usuarios,
                          (SELECT COUNT(*) FROM clase_lista_espera WHERE usuario_id = %s)   AS clase_lista_espera,
                          (SELECT COUNT(*) FROM usuario_notas WHERE usuario_id = %s)        AS usuario_notas,
                          (SELECT COUNT(*) FROM usuario_etiquetas WHERE usuario_id = %s)    AS usuario_etiquetas,
                          (SELECT COUNT(*) FROM usuario_estados WHERE usuario_id = %s)      AS usuario_estados,
                          (SELECT COUNT(*) FROM profesores WHERE usuario_id = %s)           AS profesores,
                          (SELECT COUNT(*) FROM notificaciones_cupos WHERE usuario_id = %s) AS notificaciones_cupos,
                          (SELECT COUNT(*) FROM audit_logs WHERE user_id = %s)              AS audit_logs_user,
                          (SELECT COUNT(*) FROM checkin_pending WHERE usuario_id = %s)      AS checkin_pending,
                          (SELECT COUNT(*) FROM whatsapp_messages WHERE user_id = %s)       AS whatsapp_messages
                        """,
                        (
                            usuario_id, usuario_id, usuario_id, usuario_id, usuario_id,
                            usuario_id, usuario_id, usuario_id, usuario_id, usuario_id,
                            usuario_id, usuario_id, usuario_id
                        )
                    )
                    row = cursor.fetchone() or {}
                    # Normalizar a dict simple con enteros
                    result = {k: int(row.get(k) or 0) for k in [
                        'pagos','asistencias','rutinas','clase_usuarios','clase_lista_espera',
                        'usuario_notas','usuario_etiquetas','usuario_estados','profesores',
                        'notificaciones_cupos','audit_logs_user','checkin_pending','whatsapp_messages'
                    ]}
                    try:
                        self.cache.set('usuarios', ('refs', int(usuario_id)), result)
                    except Exception:
                        pass
                    return result
                except Exception:
                    # Fallback seguro si alguna tabla no existe, devolvemos ceros
                    result = {
                        'pagos': 0, 'asistencias': 0, 'rutinas': 0, 'clase_usuarios': 0,
                        'clase_lista_espera': 0, 'usuario_notas': 0, 'usuario_etiquetas': 0,
                        'usuario_estados': 0, 'profesores': 0, 'notificaciones_cupos': 0,
                        'audit_logs_user': 0, 'checkin_pending': 0, 'whatsapp_messages': 0
                    }
                    try:
                        self.cache.set('usuarios', ('refs', int(usuario_id)), result)
                    except Exception:
                        pass
                    return result

    # --- MÉTODOS DE PAGO ---
    

    def obtener_usuarios_con_cuotas_por_vencer(self, dias_anticipacion: int = 3) -> List[Dict[str, Any]]:
        """Obtiene usuarios cuyas cuotas vencen en los próximos días especificados usando duracion_dias, priorizando fecha_proximo_vencimiento si está definida"""
        try:
            with self.atomic_transaction(isolation_level="REPEATABLE READ") as conn:
                with conn.cursor() as cursor:
                    fecha_actual = date.today()
                    fecha_limite = fecha_actual + timedelta(days=dias_anticipacion)
                    
                    # Consulta que calcula el vencimiento real priorizando fecha_proximo_vencimiento y usando duracion_dias del tipo de cuota
                    cursor.execute("""
                        SELECT u.id, u.nombre, u.telefono, 
                               tc.precio as monto_cuota,
                               tc.duracion_dias,
                               COALESCE(ultimo_pago.fecha_pago, u.fecha_registro) as fecha_base,
                               COALESCE(u.fecha_proximo_vencimiento::date, (COALESCE(ultimo_pago.fecha_pago, u.fecha_registro) + INTERVAL '1 day' * COALESCE(tc.duracion_dias, 30))::date) as fecha_vencimiento_calculada
                        FROM usuarios u
                        LEFT JOIN tipos_cuota tc ON u.tipo_cuota = tc.nombre
                        LEFT JOIN (
                            SELECT usuario_id, MAX(fecha_pago) as fecha_pago
                            FROM pagos
                            GROUP BY usuario_id
                        ) ultimo_pago ON u.id = ultimo_pago.usuario_id
                        WHERE u.activo = TRUE 
                          AND u.rol = 'socio'
                          AND COALESCE(u.fecha_proximo_vencimiento::date, (COALESCE(ultimo_pago.fecha_pago, u.fecha_registro) + INTERVAL '1 day' * COALESCE(tc.duracion_dias, 30))::date) BETWEEN %s AND %s
                    """, (fecha_actual, fecha_limite))
                    
                    usuarios = []
                    for row in cursor.fetchall():
                        usuarios.append({
                            'id': row[0],
                            'nombre': row[1],
                            'telefono': row[2],
                            'monto': row[3] if row[3] else 0,
                            'duracion_dias': row[4],
                            'fecha_base': row[5],
                            'fecha_vencimiento': row[6].strftime('%d/%m/%Y') if row[6] else None,
                            'dias_para_vencer': (row[6] - fecha_actual).days if row[6] else None
                        })
                    
                    logging.info(f"Encontrados {len(usuarios)} usuarios con cuotas por vencer en {dias_anticipacion} días usando duracion_dias")
                    return usuarios
                    
        except Exception as e:
            logging.error(f"Error al obtener usuarios con cuotas por vencer: {e}")
            return []
    

    def obtener_usuarios_morosos(self) -> List[Dict[str, Any]]:
        """Obtiene usuarios con cuotas vencidas. Considera fecha_proximo_vencimiento si existe; si no, calcula vencimiento con duracion_dias y el último pago/registro."""
        try:
            with self.atomic_transaction(isolation_level="REPEATABLE READ") as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT u.id, u.nombre, u.telefono,
                               COALESCE(u.fecha_proximo_vencimiento::date,
                                        (COALESCE(ultimo_pago.fecha_pago, u.fecha_registro) + INTERVAL '1 day' * COALESCE(tc.duracion_dias, 30))::date) AS fecha_vencimiento,
                               u.cuotas_vencidas, tc.precio as monto_cuota
                        FROM usuarios u
                        LEFT JOIN tipos_cuota tc ON u.tipo_cuota = tc.nombre
                        LEFT JOIN (
                            SELECT usuario_id, MAX(fecha_pago) as fecha_pago
                            FROM pagos
                            GROUP BY usuario_id
                        ) ultimo_pago ON u.id = ultimo_pago.usuario_id
                        WHERE u.activo = TRUE 
                          AND u.rol = 'socio'
                          AND COALESCE(u.fecha_proximo_vencimiento::date,
                                       (COALESCE(ultimo_pago.fecha_pago, u.fecha_registro) + INTERVAL '1 day' * COALESCE(tc.duracion_dias, 30))::date) < %s
                    """, (date.today(),))
                    
                    usuarios = []
                    for row in cursor.fetchall():
                        usuarios.append({
                            'id': row[0],
                            'nombre': row[1],
                            'telefono': row[2],
                            'fecha_vencimiento': row[3].strftime('%d/%m/%Y') if row[3] else None,
                            'cuotas_vencidas': row[4] if row[4] else 0,
                            'monto': row[5] if row[5] else 0
                        })
                    
                    return usuarios
                    
        except Exception as e:
            logging.error(f"Error al obtener usuarios morosos: {e}")
            return []
    

    def desactivar_usuario_por_cuotas_vencidas(self, usuario_id: int, motivo: str = "3 cuotas vencidas consecutivas") -> bool:
        """Desactiva un usuario por acumular 3 cuotas vencidas"""
        try:
            with self.atomic_transaction(isolation_level="REPEATABLE READ") as conn:
                with conn.cursor() as cursor:
                    # Obtener datos del usuario antes de desactivar
                    cursor.execute("""
                        SELECT nombre, telefono, rol FROM usuarios WHERE id = %s
                    """, (usuario_id,))
                    usuario_data = cursor.fetchone()
                    
                    if not usuario_data:
                        logging.error(f"Usuario {usuario_id} no encontrado para desactivación")
                        return False
                    
                    nombre, telefono, rol = usuario_data

                    # No desactivar por falta de pago si es Profesor o Dueño
                    if str(rol).lower() in ("profesor", "dueño"):
                        logging.info(f"Evitar desactivación por cuotas vencidas para usuario {usuario_id} ({nombre}) con rol '{rol}'")
                        return False
                    
                    # Desactivar usuario
                    cursor.execute("""
                        UPDATE usuarios 
                        SET activo = FALSE
                        WHERE id = %s
                    """, (usuario_id,))
            
            # Limpiar cache tras commit
            self.cache.invalidate('usuarios', usuario_id)
            
            # Crear estado usando el método crear_estado_usuario que maneja el historial correctamente
            try:
                from .models import UsuarioEstado
                estado = UsuarioEstado(
                    usuario_id=usuario_id,
                    estado='desactivado_por_morosidad',
                    descripcion=motivo,
                    activo=True
                )
                self.crear_estado_usuario(estado, motivo, "127.0.0.1")
                logging.info(f"Estado 'desactivado_por_morosidad' creado para usuario {usuario_id}")
            except Exception as estado_error:
                logging.error(f"Error al crear estado para usuario desactivado: {estado_error}")
                # No fallar la desactivación si falla la creación del estado
            
            # La notificación de desactivación se envía desde PaymentManager para evitar duplicidades
            
            logging.info(f"Usuario {usuario_id} desactivado por cuotas vencidas: {motivo}")
            return True
                    
        except Exception as e:
            logging.error(f"Error al desactivar usuario por cuotas vencidas: {e}")
            return False
    

    def obtener_profesor_por_usuario_id(self, usuario_id: int) -> Optional['Usuario']:
        """Obtiene el perfil de profesor por ID de usuario"""
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                sql = """
                SELECT u.*, p.tipo as tipo_profesor, p.id as profesor_id
                FROM usuarios u
                LEFT JOIN profesores p ON u.id = p.usuario_id
                WHERE u.id = %s AND u.rol = 'profesor'
                """
                print(f"🔍 DEBUG obtener_profesor_por_usuario_id: Buscando usuario_id={usuario_id}")
                cursor.execute(sql, (usuario_id,))
                row = cursor.fetchone()
                print(f"🔍 DEBUG obtener_profesor_por_usuario_id: Resultado SQL: {row}")
                if row:
                    from .models import Usuario
                    # Agregar el tipo de profesor y el ID del profesor al objeto Usuario
                    usuario_data = dict(row)
                    # Unificar nombre completo si existe 'apellido' y limpiar campos no soportados por Usuario
                    if 'apellido' in usuario_data:
                        nombre = (usuario_data.get('nombre') or '').strip()
                        apellido = (usuario_data.get('apellido') or '').strip()
                        usuario_data['nombre'] = (f"{nombre} {apellido}".strip() if nombre or apellido else nombre or apellido)
                        usuario_data.pop('apellido', None)
                    # El modelo Usuario no incluye email
                    usuario_data.pop('email', None)
                    # Filtrar solo campos permitidos por el dataclass Usuario
                    allowed_fields = set(Usuario.__dataclass_fields__.keys())
                    filtered = {k: v for k, v in usuario_data.items() if k in allowed_fields}
                    usuario = Usuario(**filtered)
                    # Agregar el tipo de profesor y el ID del profesor como atributos adicionales
                    usuario.tipo_profesor = usuario_data.get('tipo_profesor')
                    usuario.profesor_id = usuario_data.get('profesor_id')
                    print(f"🔍 DEBUG obtener_profesor_por_usuario_id: Usuario creado con profesor_id={usuario.profesor_id}")
                    return usuario
                print(f"🔍 DEBUG obtener_profesor_por_usuario_id: No se encontró usuario o no es profesor")
                return None
    

    def limpiar_cache_usuarios(self):
        """Limpia el cache de usuarios"""
        if hasattr(self, 'cache') and self.cache:
            self.cache.invalidate('usuarios')

    @database_retry

    def _procesar_lote_cambio_rol_robusto(self, cursor, lote: List[int], nuevo_rol: str, resultados: dict):
        """Procesa un lote de cambio de rol de usuarios con validaciones robustas"""
        try:
            # Validaciones iniciales
            if not lote:
                self.logger.warning("[_procesar_lote_cambio_rol_robusto] Lote vacío")
                return
            
            if not nuevo_rol or nuevo_rol.strip() == "":
                error_msg = "Rol nuevo no válido"
                self.logger.error(f"[_procesar_lote_cambio_rol_robusto] {error_msg}")
                resultados['errores'].append(error_msg)
                resultados['fallidos'] += len(lote)
                return
            
            # Validar que el rol sea válido
            roles_validos = ['miembro', 'profesor', 'administrador', 'dueño']
            if nuevo_rol not in roles_validos:
                error_msg = f"Rol '{nuevo_rol}' no es válido. Roles válidos: {', '.join(roles_validos)}"
                self.logger.error(f"[_procesar_lote_cambio_rol_robusto] {error_msg}")
                resultados['errores'].append(error_msg)
                resultados['fallidos'] += len(lote)
                return
            
            placeholders = ','.join(['%s' for _ in lote])
            
            # Verificar que los usuarios existen y obtener roles anteriores
            cursor.execute(f"SELECT id, rol, nombre FROM usuarios WHERE id IN ({placeholders})", lote)
            usuarios_existentes = cursor.fetchall()
            
            if not usuarios_existentes:
                error_msg = f"Ninguno de los usuarios del lote existe: {lote}"
                self.logger.error(f"[_procesar_lote_cambio_rol_robusto] {error_msg}")
                resultados['errores'].append(error_msg)
                resultados['fallidos'] += len(lote)
                return
            
            roles_anteriores = {row['id']: {'rol': row['rol'], 'nombre': row['nombre']} for row in usuarios_existentes}
            usuarios_encontrados = set(roles_anteriores.keys())
            usuarios_no_encontrados = set(lote) - usuarios_encontrados
            
            # Reportar usuarios no encontrados
            for usuario_id in usuarios_no_encontrados:
                error_msg = f"Usuario {usuario_id} no existe"
                self.logger.warning(f"[_procesar_lote_cambio_rol_robusto] {error_msg}")
                resultados['errores'].append(error_msg)
                resultados['fallidos'] += 1
            
            # Procesar usuarios existentes
            usuarios_a_procesar = list(usuarios_encontrados)
            if not usuarios_a_procesar:
                return
            
            # Validar restricciones especiales
            for usuario_id in usuarios_a_procesar:
                rol_anterior = roles_anteriores[usuario_id]['rol']
                
                # No permitir cambiar el rol del dueño
                if rol_anterior == 'dueño' and nuevo_rol != 'dueño':
                    error_msg = f"No se puede cambiar el rol del dueño (Usuario {usuario_id})"
                    self.logger.warning(f"[_procesar_lote_cambio_rol_robusto] {error_msg}")
                    resultados['errores'].append(error_msg)
                    resultados['fallidos'] += 1
                    usuarios_a_procesar.remove(usuario_id)
                    continue
                
                # No permitir múltiples dueños
                if nuevo_rol == 'dueño' and rol_anterior != 'dueño':
                    cursor.execute("SELECT COUNT(*) FROM usuarios WHERE rol = 'dueño'")
                    count_duenos = cursor.fetchone()[0]
                    if count_duenos > 0:
                        error_msg = f"Ya existe un dueño. No se puede asignar rol de dueño al Usuario {usuario_id}"
                        self.logger.warning(f"[_procesar_lote_cambio_rol_robusto] {error_msg}")
                        resultados['errores'].append(error_msg)
                        resultados['fallidos'] += 1
                        usuarios_a_procesar.remove(usuario_id)
                        continue
            
            if not usuarios_a_procesar:
                self.logger.info("[_procesar_lote_cambio_rol_robusto] No hay usuarios válidos para procesar")
                return
            
            # Actualización en lote para usuarios válidos
            placeholders_validos = ','.join(['%s' for _ in usuarios_a_procesar])
            cursor.execute(f"UPDATE usuarios SET rol = %s, fecha_modificacion = CURRENT_TIMESTAMP WHERE id IN ({placeholders_validos})", [nuevo_rol] + usuarios_a_procesar)
            
            # Gestionar perfiles de profesor automáticamente
            for usuario_id in usuarios_a_procesar:
                try:
                    if usuario_id in roles_anteriores:
                        rol_anterior = roles_anteriores[usuario_id]['rol']
                        nombre_completo = str(roles_anteriores[usuario_id]['nombre'] or "").strip()
                        
                        # Si cambió de no-profesor a profesor, crear perfil
                        if nuevo_rol == 'profesor' and rol_anterior != 'profesor':
                            try:
                                self.crear_profesor(usuario_id)
                                self.logger.info(f"[_procesar_lote_cambio_rol_robusto] Perfil de profesor creado para usuario {usuario_id} ({nombre_completo})")
                            except Exception as e:
                                self.logger.error(f"[_procesar_lote_cambio_rol_robusto] Error creando perfil de profesor para usuario {usuario_id}: {str(e)}")
                                # No fallar la operación completa por esto
                        
                        # Si cambió de profesor a no-profesor, eliminar perfil
                        elif rol_anterior == 'profesor' and nuevo_rol != 'profesor':
                            try:
                                perfil_profesor = self.obtener_profesor_por_usuario_id(usuario_id)
                                if perfil_profesor:
                                    self.eliminar_profesor(perfil_profesor['id'])
                                    self.logger.info(f"[_procesar_lote_cambio_rol_robusto] Perfil de profesor eliminado para usuario {usuario_id} ({nombre_completo})")
                            except Exception as e:
                                self.logger.error(f"[_procesar_lote_cambio_rol_robusto] Error eliminando perfil de profesor para usuario {usuario_id}: {str(e)}")
                                # No fallar la operación completa por esto
                        
                        resultados['detalles'].append(f'Usuario {usuario_id} ({nombre_completo}) cambió rol de {rol_anterior} a {nuevo_rol}')
                        resultados['exitosos'] += 1
                        
                        # Log de auditoría
                        self.logger.info(f"[_procesar_lote_cambio_rol_robusto] Usuario {usuario_id} ({nombre_completo}): {rol_anterior} -> {nuevo_rol}")
                        
                except Exception as e:
                    error_msg = f"Error procesando usuario {usuario_id}: {str(e)}"
                    self.logger.error(f"[_procesar_lote_cambio_rol_robusto] {error_msg}")
                    resultados['errores'].append(error_msg)
                    resultados['fallidos'] += 1
                    
        except Exception as e:
            error_msg = f"Error crítico en _procesar_lote_cambio_rol_robusto: {str(e)}"
            self.logger.error(f"[_procesar_lote_cambio_rol_robusto] {error_msg}")
            import traceback
            self.logger.error(f"[_procesar_lote_cambio_rol_robusto] Traceback: {traceback.format_exc()}")
            resultados['errores'].append(error_msg)
            resultados['fallidos'] += len(lote)
    

    def _procesar_lote_cambio_rol(self, cursor, lote: List[int], nuevo_rol: str, resultados: dict):
        """Wrapper anterior para compatibilidad - usa la versión robusta"""
        return self._procesar_lote_cambio_rol_robusto(cursor, lote, nuevo_rol, resultados)


    def _procesar_agregar_estado_usuario(self, cursor, usuario_id: int, parametros: dict, resultados: dict):
        """Procesa la adición de un estado a un usuario específico"""
        try:
            plantilla_id = parametros.get('plantilla_id')
            descripcion = parametros.get('descripcion')
            reemplazar_existentes = parametros.get('reemplazar_existentes', False)
            generar_alertas = parametros.get('generar_alertas', True)
            
            # Obtener información de la plantilla
            plantillas = self.obtener_plantillas_estados()
            plantilla = next((p for p in plantillas if p['id'] == plantilla_id), None)
            
            if not plantilla:
                resultados['errores'].append(f'Usuario {usuario_id}: Plantilla de estado no encontrada')
                resultados['fallidos'] += 1
                return
            
            # Verificar si ya tiene un estado del mismo tipo
            if reemplazar_existentes:
                cursor.execute(
                    "DELETE FROM usuario_estados WHERE usuario_id = %s AND estado = %s",
                    (usuario_id, plantilla['id'])
                )
            else:
                cursor.execute(
                    "SELECT id FROM usuario_estados WHERE usuario_id = %s AND estado = %s AND activo = true",
                    (usuario_id, plantilla['id'])
                )
                if cursor.fetchone():
                    resultados['errores'].append(f'Usuario {usuario_id}: Ya tiene el estado {plantilla["nombre"]}')
                    resultados['fallidos'] += 1
                    return
            
            # Validar existencia de usuario antes de insertar estado
            try:
                if not self.usuario_id_existe(usuario_id):
                    resultados['errores'].append(f'Usuario {usuario_id}: No existe en la tabla usuarios')
                    resultados['fallidos'] += 1
                    return
            except Exception:
                # Si falla la verificación, continuar con precaución
                pass

            # Insertar nuevo estado
            from datetime import datetime, timedelta
            fecha_vencimiento = datetime.now() + timedelta(days=30)  # Estado válido por 30 días
            
            cursor.execute("""
                INSERT INTO usuario_estados (usuario_id, estado, descripcion, fecha_vencimiento, activo, creado_por)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                usuario_id, 
                plantilla['id'], 
                descripcion or plantilla['descripcion'],
                fecha_vencimiento,
                True,
                1  # Sistema
            ))
            
            resultados['detalles'].append(f'Usuario {usuario_id}: Estado {plantilla["nombre"]} agregado')
            resultados['exitosos'] += 1
            
        except Exception as e:
            resultados['errores'].append(f'Usuario {usuario_id}: Error agregando estado - {str(e)}')
            resultados['fallidos'] += 1
    

    def _procesar_asignar_etiqueta_usuario(self, cursor, usuario_id: int, parametros: dict, resultados: dict):
        """Procesa la asignación de una etiqueta a un usuario específico"""
        try:
            etiqueta_nombre = parametros.get('etiqueta_nombre')
            omitir_existentes = parametros.get('omitir_existentes', True)
            crear_si_no_existe = parametros.get('crear_si_no_existe', True)
            
            if not etiqueta_nombre:
                resultados['errores'].append(f'Usuario {usuario_id}: Nombre de etiqueta no especificado')
                resultados['fallidos'] += 1
                return
            
            # Verificar si la etiqueta existe
            cursor.execute("SELECT id FROM etiquetas WHERE nombre = %s", (etiqueta_nombre,))
            etiqueta_row = cursor.fetchone()
            
            if not etiqueta_row:
                if crear_si_no_existe:
                    # Crear nueva etiqueta
                    cursor.execute(
                        "INSERT INTO etiquetas (nombre, color, descripcion) VALUES (%s, %s, %s) RETURNING id",
                        (etiqueta_nombre, '#3b82f6', f'Etiqueta creada automáticamente: {etiqueta_nombre}')
                    )
                    etiqueta_id = cursor.fetchone()[0]
                else:
                    resultados['errores'].append(f'Usuario {usuario_id}: Etiqueta "{etiqueta_nombre}" no existe')
                    resultados['fallidos'] += 1
                    return
            else:
                etiqueta_id = etiqueta_row[0]
            
            # Verificar si ya tiene la etiqueta
            cursor.execute(
                "SELECT id FROM usuario_etiquetas WHERE usuario_id = %s AND etiqueta_id = %s",
                (usuario_id, etiqueta_id)
            )
            
            if cursor.fetchone():
                if omitir_existentes:
                    if 'omitidos' not in resultados:
                        resultados['omitidos'] = 0
                    resultados['omitidos'] += 1
                    return
                else:
                    resultados['errores'].append(f'Usuario {usuario_id}: Ya tiene la etiqueta "{etiqueta_nombre}"')
                    resultados['fallidos'] += 1
                    return
            
            # Asignar etiqueta
            cursor.execute(
                "INSERT INTO usuario_etiquetas (usuario_id, etiqueta_id) VALUES (%s, %s)",
                (usuario_id, etiqueta_id)
            )
            
            resultados['detalles'].append(f'Usuario {usuario_id}: Etiqueta "{etiqueta_nombre}" asignada')
            resultados['exitosos'] += 1
            
        except Exception as e:
            resultados['errores'].append(f'Usuario {usuario_id}: Error asignando etiqueta - {str(e)}')
            resultados['fallidos'] += 1


    def limpiar_cache_usuarios(self):
        """Limpia el cache de usuarios de forma thread-safe"""
        if hasattr(self, 'cache') and self.cache:
            self.cache.invalidate('usuarios')


    def buscar_usuarios(self, termino_busqueda: str, limite: int = 50) -> List[Usuario]:
        """Búsqueda optimizada de usuarios con índices mejorados"""
        if not termino_busqueda.strip():
            return []
        
        # Usar cache para búsquedas frecuentes
        cache_key = f"busqueda_{termino_busqueda.lower()}_{limite}"
        cached_result = self.cache.get('usuarios', cache_key)
        if cached_result:
            return cached_result
        
        with self.readonly_session(lock_ms=800, statement_ms=2500, idle_s=2, seqscan_off=True) as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            # Búsqueda optimizada con ILIKE para PostgreSQL (case-insensitive)
            termino = f"%{termino_busqueda}%"
            _cols = self.get_table_columns('usuarios')
            _desired = ['id','nombre','dni','telefono','pin','rol','notas','fecha_registro','activo','tipo_cuota','fecha_proximo_vencimiento','cuotas_vencidas','ultimo_pago','apellido','email']
            _sel = ", ".join([c for c in _desired if c in (_cols or [])]) or "id, nombre, rol"
            cursor.execute(
                f"""
                SELECT {_sel} FROM usuarios 
                WHERE (nombre ILIKE %s OR dni ILIKE %s OR telefono ILIKE %s)
                  AND activo = true
                ORDER BY 
                    CASE WHEN rol = 'dueño' THEN 1 
                         WHEN rol = 'profesor' THEN 2 
                         ELSE 3 END,
                    nombre ASC
                LIMIT %s
                """,
                (termino, termino, termino, limite)
            )
            
            usuarios = []
            for row in cursor.fetchall():
                data = dict(row)
                apellido = data.pop('apellido', None)
                nombre = (data.get('nombre') or '').strip()
                if apellido:
                    ap = str(apellido).strip()
                    if ap and ap not in nombre:
                        nombre = f"{nombre} {ap}".strip() if nombre else ap
                data['nombre'] = nombre
                data.pop('email', None)
                allowed = {'id','nombre','dni','telefono','pin','rol','notas','fecha_registro','activo','tipo_cuota','fecha_proximo_vencimiento','cuotas_vencidas','ultimo_pago'}
                filtered = {k: data.get(k) for k in allowed if k in data}
                usuarios.append(Usuario(**filtered))
            
            # Guardar en cache por 5 minutos
            self.cache.set('usuarios', cache_key, usuarios)
            
            return usuarios


    def obtener_estadisticas_usuarios(self) -> dict:
        """Obtiene estadísticas generales de usuarios con optimizaciones de rendimiento"""
        cache_key = "estadisticas_usuarios"
        cached_result = self.cache.get('reportes', cache_key)
        if cached_result:
            return cached_result
        
        with self.readonly_session(lock_ms=800, statement_ms=2500, idle_s=2, seqscan_off=True) as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            # Consulta optimizada con una sola pasada por la tabla
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(CASE WHEN activo = true THEN 1 END) as activos,
                    COUNT(CASE WHEN activo = false THEN 1 END) as inactivos,
                    COUNT(CASE WHEN rol = 'socio' THEN 1 END) as socios,
                    COUNT(CASE WHEN rol = 'profesor' THEN 1 END) as profesores,
                    COUNT(CASE WHEN rol = 'dueño' THEN 1 END) as dueños,
                    COUNT(CASE WHEN tipo_cuota = 'estandar' THEN 1 END) as cuota_estandar,
                    COUNT(CASE WHEN tipo_cuota = 'estudiante' THEN 1 END) as cuota_estudiante
                FROM usuarios
            """)
            
            row = cursor.fetchone()
            estadisticas = {
                'total_usuarios': row[0],
                'usuarios_activos': row[1],
                'usuarios_inactivos': row[2],
                'socios': row[3],
                'profesores': row[4],
                'dueños': row[5],
                'cuota_estandar': row[6],
                'cuota_estudiante': row[7],
                'porcentaje_activos': round((row[1] / row[0] * 100) if row[0] > 0 else 0, 2)
            }
            
            # Cache por 10 minutos
            self.cache.set('reportes', cache_key, estadisticas)
            
            return estadisticas

    # --- MÉTODOS DE UTILIDAD Y VERIFICACIÓN ---
    

    def usuario_tiene_pagos(self, usuario_id: int) -> bool:
        """Verifica si un usuario tiene pagos registrados."""
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1 FROM pagos WHERE usuario_id = %s LIMIT 1", (usuario_id,))
                return cursor.fetchone() is not None
    

    def usuario_tiene_asistencias(self, usuario_id: int) -> bool:
        """Verifica si un usuario tiene asistencias registradas."""
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1 FROM asistencias WHERE usuario_id = %s LIMIT 1", (usuario_id,))
                return cursor.fetchone() is not None
    

    def usuario_tiene_rutinas(self, usuario_id: int) -> bool:
        """Verifica si un usuario tiene rutinas registradas."""
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1 FROM rutinas WHERE usuario_id = %s LIMIT 1", (usuario_id,))
                return cursor.fetchone() is not None
    

    def usuario_tiene_clases(self, usuario_id: int) -> bool:
        """Verifica si un usuario está inscrito en alguna clase."""
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1 FROM clase_usuarios WHERE usuario_id = %s LIMIT 1", (usuario_id,))
                return cursor.fetchone() is not None

    @database_retry()

    def cambiar_usuario_id(self, old_id: int, new_id: int) -> None:
        """Cambia el ID de un usuario de forma segura, actualizando todas las referencias.
        Estrategia: insertar una fila nueva con el nuevo ID, migrar referencias y eliminar la fila anterior.
        Se usa un DNI temporal único al crear la nueva fila para evitar colisión de unicidad.
        """
        if old_id == new_id:
            return
        if new_id is None or int(new_id) <= 0:
            raise ValueError("El nuevo ID debe ser un entero positivo.")

        with self.readonly_session(lock_ms=800, statement_ms=2500, idle_s=2, seqscan_off=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                # Validaciones previas
                _cols = self.get_table_columns('usuarios')
                _desired = ['id','nombre','dni','telefono','pin','rol','activo','tipo_cuota','notas','fecha_registro','fecha_proximo_vencimiento','cuotas_vencidas','ultimo_pago','apellido','email']
                _sel = ", ".join([c for c in _desired if c in (_cols or [])]) or "id, nombre, rol"
                cursor.execute(f"SELECT {_sel} FROM usuarios WHERE id = %s", (old_id,))
                old_row = cursor.fetchone()
                if not old_row:
                    raise ValueError("Usuario original no existe.")
                if (old_row.get('rol') == 'dueño'):
                    raise PermissionError("El usuario con rol 'dueño' no puede cambiar su ID.")

                cursor.execute("SELECT 1 FROM usuarios WHERE id = %s", (new_id,))
                if cursor.fetchone():
                    raise ValueError("El nuevo ID ya está en uso por otro usuario.")

                # Preparar DNI temporal único para evitar colisión de unicidad al insertar la nueva fila
                orig_dni = old_row.get('dni')
                try:
                    cursor.execute("SELECT COALESCE(MAX(CAST(dni AS INTEGER)), 0) FROM usuarios")
                    max_dni = cursor.fetchone()[0] or 0
                    tmp_dni = int(max_dni) + 1
                except Exception:
                    # Fallback si el CAST falla o la columna no es numérica
                    tmp_dni = f"{str(orig_dni)}__tmp__{old_id}_{new_id}"

                cursor.execute(
                    """
                    INSERT INTO usuarios (
                        id, nombre, dni, telefono, pin, rol, activo, tipo_cuota, notas,
                        fecha_registro, fecha_proximo_vencimiento, cuotas_vencidas, ultimo_pago
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        new_id,
                        old_row.get('nombre'), tmp_dni, old_row.get('telefono'), old_row.get('pin'),
                        old_row.get('rol'), old_row.get('activo'), old_row.get('tipo_cuota'), old_row.get('notas'),
                        old_row.get('fecha_registro'), old_row.get('fecha_proximo_vencimiento'),
                        old_row.get('cuotas_vencidas'), old_row.get('ultimo_pago')
                    )
                )

                # Actualizar referencias en tablas relacionadas
                updates = [
                    ("UPDATE pagos SET usuario_id = %s WHERE usuario_id = %s", (new_id, old_id)),
                    ("UPDATE asistencias SET usuario_id = %s WHERE usuario_id = %s", (new_id, old_id)),
                    ("UPDATE rutinas SET usuario_id = %s WHERE usuario_id = %s", (new_id, old_id)),
                    ("UPDATE clase_usuarios SET usuario_id = %s WHERE usuario_id = %s", (new_id, old_id)),
                    ("UPDATE clase_lista_espera SET usuario_id = %s WHERE usuario_id = %s", (new_id, old_id)),
                    ("UPDATE usuario_notas SET usuario_id = %s WHERE usuario_id = %s", (new_id, old_id)),
                    ("UPDATE usuario_etiquetas SET usuario_id = %s WHERE usuario_id = %s", (new_id, old_id)),
                    ("UPDATE usuario_estados SET usuario_id = %s WHERE usuario_id = %s", (new_id, old_id)),
                    ("UPDATE profesores SET usuario_id = %s WHERE usuario_id = %s", (new_id, old_id)),
                    ("UPDATE notificaciones_cupos SET usuario_id = %s WHERE usuario_id = %s", (new_id, old_id)),
                    ("UPDATE audit_logs SET user_id = %s WHERE user_id = %s", (new_id, old_id)),
                    ("UPDATE checkin_pending SET usuario_id = %s WHERE usuario_id = %s", (new_id, old_id)),
                    ("UPDATE whatsapp_messages SET user_id = %s WHERE user_id = %s", (new_id, old_id)),
                    # Referencias como autor/asignador/creador
                    ("UPDATE usuario_notas SET autor_id = %s WHERE autor_id = %s", (new_id, old_id)),
                    ("UPDATE usuario_etiquetas SET asignado_por = %s WHERE asignado_por = %s", (new_id, old_id)),
                    ("UPDATE usuario_estados SET creado_por = %s WHERE creado_por = %s", (new_id, old_id)),
                ]
                for sql, params in updates:
                    try:
                        cursor.execute(sql, params)
                    except Exception as e:
                        logging.debug(f"Aviso al actualizar referencias para cambio de ID: {e}")

                # Actualizar arrays de IDs en acciones masivas pendientes
                try:
                    cursor.execute(
                        "UPDATE acciones_masivas_pendientes SET usuario_ids = array_replace(usuario_ids, %s, %s) WHERE %s = ANY(usuario_ids)",
                        (old_id, new_id, old_id)
                    )
                except Exception as e:
                    logging.debug(f"Aviso al actualizar arrays de acciones masivas: {e}")

                # Eliminar la fila anterior
                cursor.execute("DELETE FROM usuarios WHERE id = %s", (old_id,))

                # Restaurar el DNI original en la nueva fila (ahora sin colisión)
                try:
                    cursor.execute("UPDATE usuarios SET dni = %s WHERE id = %s", (orig_dni, new_id))
                except Exception as e:
                    logging.warning(f"No se pudo restaurar DNI original tras cambio de ID: {e}")

                conn.commit()

                # Auditoría
                try:
                    if self.audit_logger:
                        self.audit_logger.log_operation('UPDATE', 'usuarios', new_id, {'id': old_id}, {'id': new_id})
                except Exception:
                    pass

                # Limpiar caches y sincronización
                try:
                    self.limpiar_cache_usuarios()
                except Exception:
                    pass
                

    @database_retry()

    def renumerar_usuario_ids(self, start_id: int = 1) -> dict:
        """Renumera IDs de usuarios de forma segura y secuencial.
        - Mantiene sin cambios los usuarios con rol "dueño".
        - Usa `cambiar_usuario_id` para actualizar referencias en cascada.
        - Comienza desde `start_id` (>=1) y rellena huecos, evitando IDs reservados.
        """
        cambios = []
        skipped_owner_ids = set()
        with self.readonly_session(lock_ms=800, statement_ms=2500, idle_s=2, seqscan_off=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute("SELECT id, rol FROM usuarios ORDER BY id ASC")
                rows = cursor.fetchall() or []
                owner_ids = {int(r['id']) for r in rows if str(r.get('rol') or '').strip().lower() == 'dueño'}
                skipped_owner_ids = owner_ids.copy()
                try:
                    next_id = int(start_id)
                except Exception:
                    next_id = 1
                if next_id <= 0:
                    next_id = 1
                for r in rows:
                    old_id = int(r['id'])
                    rol = str(r.get('rol') or '').strip().lower()
                    # Saltar IDs reservados (dueños)
                    while next_id in owner_ids:
                        next_id += 1
                    if rol == 'dueño':
                        # Mantener el ID del dueño y avanzar target si coincide
                        if next_id == old_id:
                            next_id += 1
                        continue
                    # Si ya está en posición, avanzar
                    if old_id == next_id:
                        next_id += 1
                        continue
                    # Verificar disponibilidad del target (defensivo)
                    cursor.execute("SELECT 1 FROM usuarios WHERE id = %s", (next_id,))
                    exists = cursor.fetchone() is not None
                    if exists:
                        # Buscar siguiente libre evitando IDs de dueños
                        probe = next_id
                        while True:
                            probe += 1
                            if probe in owner_ids:
                                continue
                            cursor.execute("SELECT 1 FROM usuarios WHERE id = %s", (probe,))
                            if cursor.fetchone() is None:
                                next_id = probe
                                break
                    # Cambiar ID usando rutina segura (maneja referencias y auditoría)
                    try:
                        self.cambiar_usuario_id(old_id, next_id)
                        cambios.append({"from": old_id, "to": next_id})
                        next_id += 1
                    except PermissionError as e:
                        cambios.append({"from": old_id, "to": None, "error": str(e)})
                    except Exception as e:
                        cambios.append({"from": old_id, "to": None, "error": str(e)})
        # Limpiar caches
        try:
            self.limpiar_cache_usuarios()
        except Exception:
            pass
        return {
            "ok": True,
            "cambios": cambios,
            "dueños_reservados": sorted(list(skipped_owner_ids)),
            "nota": "Los IDs de dueños no se modificaron y pueden dejar huecos."
        }

    # --- MÉTODOS PARA GRUPOS DE EJERCICIOS ---
    

    def crear_nota_usuario(self, nota) -> int:
        """Crea una nueva nota para un usuario."""
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                # Asegurar valores por defecto coherentes con el esquema para evitar NULL explícitos
                categoria = getattr(nota, "categoria", None) or 'general'
                titulo = getattr(nota, "titulo", None) or ''
                contenido = getattr(nota, "contenido", None) or ''
                importancia = getattr(nota, "importancia", None) or 'normal'
                autor_id = getattr(nota, "autor_id", None)
                sql = """
                INSERT INTO usuario_notas (usuario_id, categoria, titulo, contenido, importancia, autor_id)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id
                """
                cursor.execute(sql, (nota.usuario_id, categoria, titulo, contenido, importancia, autor_id))
                nota_id = cursor.fetchone()[0]
                conn.commit()
                return nota_id
    

    def obtener_notas_usuario(self, usuario_id: int, solo_activas: bool = True) -> List:
        """Obtiene todas las notas de un usuario."""
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                sql = (
                    "SELECT id, usuario_id, categoria, titulo, contenido, importancia, "
                    "fecha_creacion, fecha_modificacion, activa, autor_id "
                    "FROM usuario_notas WHERE usuario_id = %s"
                )
                params = [usuario_id]
                
                if solo_activas:
                    sql += " AND activa = TRUE"
                
                sql += " ORDER BY fecha_creacion DESC"
                cursor.execute(sql, params)
                return [dict(row) for row in cursor.fetchall()]


    def actualizar_nota_usuario(self, nota) -> bool:
        """Actualiza una nota de usuario."""
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                sql = """
                UPDATE usuario_notas 
                SET categoria = %s, titulo = %s, contenido = %s, importancia = %s, 
                    fecha_modificacion = CURRENT_TIMESTAMP
                WHERE id = %s
                """
                cursor.execute(sql, (nota.categoria, nota.titulo, nota.contenido, 
                                   nota.importancia, nota.id))
                conn.commit()
                ok = cursor.rowcount > 0
                return ok
    

    def eliminar_nota_usuario(self, nota_id: int, eliminar_permanente: bool = False) -> bool:
        """Elimina una nota de usuario (lógica o física)."""
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                
                if eliminar_permanente:
                    cursor.execute("DELETE FROM usuario_notas WHERE id = %s", (nota_id,))
                else:
                    cursor.execute("UPDATE usuario_notas SET activa = FALSE WHERE id = %s", (nota_id,))
                
                conn.commit()
                ok = cursor.rowcount > 0
                return ok
    

    def obtener_notas_y_etiquetas_usuario(self, usuario_id: int, solo_activas_notas: bool = True) -> Dict[str, List]:
        """Obtiene todas las notas y etiquetas de un usuario en un solo viaje.

        - Devuelve un diccionario con claves 'notas' y 'etiquetas'.
        - Las listas contienen diccionarios con las columnas necesarias.
        - Aplica filtro por 'activa' en notas cuando 'solo_activas_notas' es True.
        """
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(
                    """
                    WITH notas AS (
                        SELECT json_agg(
                            json_build_object(
                                'id', n.id,
                                'usuario_id', n.usuario_id,
                                'categoria', n.categoria,
                                'titulo', n.titulo,
                                'contenido', n.contenido,
                                'importancia', n.importancia,
                                'fecha_creacion', n.fecha_creacion,
                                'fecha_modificacion', n.fecha_modificacion,
                                'activa', n.activa,
                                'autor_id', n.autor_id
                            )
                            ORDER BY n.fecha_creacion DESC
                        ) AS items
                        FROM (
                            SELECT id, usuario_id, categoria, titulo, contenido, importancia, fecha_creacion, fecha_modificacion, activa, autor_id
                            FROM usuario_notas
                            WHERE usuario_id = %s AND (NOT %s OR activa = TRUE)
                            ORDER BY fecha_creacion DESC
                        ) n
                    ),
                    etiquetas AS (
                        SELECT json_agg(
                            json_build_object(
                                'id', e.id,
                                'nombre', e.nombre,
                                'color', e.color,
                                'descripcion', e.descripcion,
                                'fecha_creacion', e.fecha_creacion,
                                'activo', e.activo
                            )
                            ORDER BY e.nombre ASC
                        ) AS items
                        FROM etiquetas e
                        JOIN usuario_etiquetas ue ON e.id = ue.etiqueta_id
                        WHERE ue.usuario_id = %s AND e.activo = TRUE
                    )
                    SELECT COALESCE(notas.items, '[]'::json) AS notas,
                           COALESCE(etiquetas.items, '[]'::json) AS etiquetas
                    FROM notas, etiquetas
                    """,
                    (usuario_id, solo_activas_notas, usuario_id)
                )
                row = cursor.fetchone() or {}
                notas_raw = row.get('notas')
                etiquetas_raw = row.get('etiquetas')
                try:
                    notas = notas_raw if isinstance(notas_raw, list) else (json.loads(notas_raw) if notas_raw is not None else [])
                except Exception:
                    notas = []
                try:
                    etiquetas = etiquetas_raw if isinstance(etiquetas_raw, list) else (json.loads(etiquetas_raw) if etiquetas_raw is not None else [])
                except Exception:
                    etiquetas = []
                return {'notas': notas, 'etiquetas': etiquetas}

    # --- MÉTODOS DE ETIQUETAS ---
    

    def asignar_etiqueta_usuario(self, usuario_id: int, etiqueta_id: int, asignado_por: int = None) -> bool:
        """Asigna una etiqueta a un usuario."""
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                sql = "INSERT INTO usuario_etiquetas (usuario_id, etiqueta_id, asignado_por) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING"
                cursor.execute(sql, (usuario_id, etiqueta_id, asignado_por))
                conn.commit()
                ok = cursor.rowcount > 0
                return ok


    def asignar_etiquetas_usuario_bulk(self, usuario_id: int, etiqueta_ids: List[int], asignado_por: int = None) -> Dict[str, int]:
        """Asigna un conjunto de etiquetas a un usuario en una sola operación.

        - Elimina asignaciones existentes que no estén en `etiqueta_ids`.
        - Inserta asignaciones nuevas usando inserción por lotes.
        - Retorna conteos de inserciones y eliminaciones.
        """
        try:
            with self.get_connection_context() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    etiquetas_nuevas = set(int(e) for e in (etiqueta_ids or []))

                    # Obtener asignaciones actuales
                    cursor.execute(
                        "SELECT etiqueta_id FROM usuario_etiquetas WHERE usuario_id = %s",
                        (usuario_id,)
                    )
                    actuales = {int(r['etiqueta_id']) for r in (cursor.fetchall() or [])}

                    a_eliminar = list(actuales - etiquetas_nuevas)
                    a_insertar = list(etiquetas_nuevas - actuales)

                    eliminados = 0
                    insertados = 0

                    # Eliminar en lote lo que ya no corresponde
                    if a_eliminar:
                        cursor.execute(
                            "DELETE FROM usuario_etiquetas WHERE usuario_id = %s AND etiqueta_id = ANY(%s)",
                            (usuario_id, a_eliminar)
                        )
                        eliminados = cursor.rowcount or 0

                    # Insertar en lote nuevas asignaciones
                    if a_insertar:
                        valores = [(usuario_id, eid, asignado_por) for eid in a_insertar]
                        insert_sql = (
                            "INSERT INTO usuario_etiquetas (usuario_id, etiqueta_id, asignado_por) VALUES %s "
                            "ON CONFLICT DO NOTHING"
                        )
                        psycopg2.extras.execute_values(
                            cursor,
                            insert_sql,
                            valores,
                            template="(%s, %s, %s)",
                            page_size=max(50, min(1000, len(valores)))
                        )
                        # execute_values rowcount puede ser None; asumimos todos insertados por set diff
                        insertados = len(a_insertar)

                    conn.commit()
                    return {"inserted": insertados, "deleted": eliminados}
        except Exception as e:
            logging.error(f"Error en asignar_etiquetas_usuario_bulk: {e}")
            return {"inserted": 0, "deleted": 0}
    

    def desasignar_etiqueta_usuario(self, usuario_id: int, etiqueta_id: int) -> bool:
        """Desasigna una etiqueta de un usuario."""
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM usuario_etiquetas WHERE usuario_id = %s AND etiqueta_id = %s", 
                             (usuario_id, etiqueta_id))
                conn.commit()
                ok = cursor.rowcount > 0
                return ok
    

    def obtener_etiquetas_usuario(self, usuario_id: int) -> List:
        """Obtiene todas las etiquetas asignadas a un usuario."""
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                sql = """
                SELECT e.id, e.nombre, e.color, e.descripcion, e.fecha_creacion, e.activo
                FROM etiquetas e
                JOIN usuario_etiquetas ue ON e.id = ue.etiqueta_id
                WHERE ue.usuario_id = %s AND e.activo = TRUE
                ORDER BY e.nombre
                """
                cursor.execute(sql, (usuario_id,))
                etiquetas = []
                for row in cursor.fetchall():
                    etiquetas.append(Etiqueta(
                        id=row.get('id'),
                        nombre=row.get('nombre'),
                        descripcion=row.get('descripcion'),
                        color=row.get('color'),
                        activo=row.get('activo', True),
                        fecha_creacion=row.get('fecha_creacion'),
                        fecha_modificacion=row.get('fecha_modificacion')
                    ))
                return etiquetas
    

    def obtener_usuarios_por_etiqueta(self, etiqueta_id: int) -> List:
        """Obtiene todos los usuarios que tienen una etiqueta específica."""
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                sql = """
                SELECT u.id, u.nombre, u.dni, u.telefono, u.pin, u.rol, u.notas,
                       u.fecha_registro, u.activo, u.tipo_cuota,
                       u.fecha_proximo_vencimiento, u.cuotas_vencidas, u.ultimo_pago
                FROM usuarios u
                JOIN usuario_etiquetas ue ON u.id = ue.usuario_id
                WHERE ue.etiqueta_id = %s AND u.activo = TRUE
                ORDER BY u.nombre
                """
                cursor.execute(sql, (etiqueta_id,))
                return [dict(row) for row in cursor.fetchall()]
    
    # --- MÉTODOS DE ESTADOS TEMPORALES DE USUARIOS ---
    

    def crear_estado_usuario(self, estado, motivo: str = None, ip_origen: str = None) -> int:
        """Crea un nuevo estado temporal para un usuario y registra la creación en el historial."""
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                # Validar que el usuario exista para evitar violación de clave foránea
                try:
                    if not self.usuario_id_existe(estado.usuario_id):
                        raise ValueError(f"Usuario {estado.usuario_id} no existe; no se puede crear estado")
                except Exception:
                    # Si falla la verificación, intentar insert y dejar que la FK lo bloquee
                    pass
                sql = """
                INSERT INTO usuario_estados (usuario_id, estado, descripcion, fecha_vencimiento, creado_por)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
                """
                cursor.execute(sql, (estado.usuario_id, estado.estado, estado.descripcion, 
                                   estado.fecha_vencimiento, estado.creado_por))
                estado_id = cursor.fetchone()[0]
                
                # Registramos la creación en el historial
                self.registrar_historial_estado(
                    usuario_id=estado.usuario_id,
                    estado_id=estado_id,
                    accion='crear',
                    estado_nuevo=estado.estado,
                    usuario_modificador=estado.creado_por,
                    motivo=motivo
                )
                
                conn.commit()
                return estado_id
    

    def obtener_estados_usuario(self, usuario_id: int, solo_activos: bool = True) -> List:
        """Obtiene todos los estados de un usuario."""
        from .models import UsuarioEstado
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                sql = (
                    "SELECT id, usuario_id, estado, descripcion, fecha_inicio, fecha_vencimiento, activo, creado_por "
                    "FROM usuario_estados WHERE usuario_id = %s"
                )
                params = [usuario_id]

                if solo_activos:
                    sql += " AND activo = TRUE"

                sql += " ORDER BY fecha_inicio DESC"
                cursor.execute(sql, params)
                estados = []
                for row in cursor.fetchall():
                    estado = UsuarioEstado(
                        id=row.get('id'),
                        usuario_id=row.get('usuario_id', 0),
                        estado=row.get('estado', ''),
                        descripcion=row.get('descripcion'),
                        fecha_inicio=row.get('fecha_inicio'),
                        fecha_vencimiento=row.get('fecha_vencimiento'),
                        activo=row.get('activo', True),
                        creado_por=row.get('creado_por')
                    )
                    estados.append(estado)
                return estados
    

    def actualizar_estado_usuario(self, estado, usuario_modificador: int = None, motivo: str = None, ip_origen: str = None) -> bool:
        """Actualiza un estado de usuario y registra el cambio en el historial."""
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                
                # Primero obtenemos el estado actual para registrar los cambios
                cursor.execute("SELECT usuario_id, estado, descripcion, fecha_vencimiento, activo FROM usuario_estados WHERE id = %s", (estado.id,))
                estado_actual = cursor.fetchone()
                
                if not estado_actual:
                    return False
                    
                usuario_id, estado_anterior, descripcion_anterior, fecha_vencimiento_anterior, activo_anterior = estado_actual
                
                # Actualizamos el estado
                sql = """
                UPDATE usuario_estados 
                SET estado = %s, descripcion = %s, fecha_vencimiento = %s, activo = %s
                WHERE id = %s
                """
                cursor.execute(sql, (estado.estado, estado.descripcion, estado.fecha_vencimiento, 
                                   estado.activo, estado.id))
                
                # Registramos el cambio en el historial
                self.registrar_historial_estado(
                    usuario_id=usuario_id, 
                    estado_id=estado.id, 
                    accion='modificar',
                    estado_anterior=estado_anterior,
                    estado_nuevo=estado.estado,
                    usuario_modificador=usuario_modificador or estado.creado_por,
                    motivo=motivo
                )
                
                conn.commit()
                return cursor.rowcount > 0
    

    def eliminar_estado_usuario(self, estado_id: int, usuario_modificador: int = None, motivo: str = None, ip_origen: str = None) -> bool:
        """Elimina un estado de usuario y registra la eliminación en el historial."""
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                
                # Primero obtenemos los datos del estado antes de eliminarlo
                cursor.execute("SELECT usuario_id, estado, descripcion, fecha_vencimiento, activo FROM usuario_estados WHERE id = %s", (estado_id,))
                estado_actual = cursor.fetchone()
                
                if not estado_actual:
                    return False
                    
                usuario_id, estado, descripcion, fecha_vencimiento, activo = estado_actual
                
                # Eliminamos el estado
                cursor.execute("DELETE FROM usuario_estados WHERE id = %s", (estado_id,))
                
                if cursor.rowcount > 0:
                    # Registramos la eliminación en el historial SIN FK para evitar violación de referencia (el estado ya fue borrado)
                    self.registrar_historial_estado(
                        usuario_id=usuario_id,
                        estado_id=None,
                        accion='eliminar',
                        estado_anterior=estado,
                        usuario_modificador=usuario_modificador,
                        motivo=motivo
                    )
                    
                conn.commit()
                return cursor.rowcount > 0
    

    def obtener_historial_estados_usuario(self, usuario_id: int, limite: int = 50) -> List:
        """Obtiene el historial completo de cambios de estado de un usuario."""
        from .models import HistorialEstado
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                sql = """
                SELECT 
                    h.id, h.usuario_id, h.estado_id, h.accion, h.estado_anterior, h.estado_nuevo,
                    h.fecha_accion, h.creado_por, h.motivo, h.detalles, h.ip_origen,
                    u_mod.nombre AS modificador_nombre,
                    ue.estado AS estado_actual_nombre
                FROM historial_estados h
                LEFT JOIN usuarios u_mod ON h.creado_por = u_mod.id
                LEFT JOIN usuario_estados ue ON h.estado_id = ue.id
                WHERE h.usuario_id = %s
                ORDER BY h.fecha_accion DESC
                LIMIT %s
                """
                cursor.execute(sql, (usuario_id, limite))
                historial = []
                for row in cursor.fetchall():
                    historial_item = HistorialEstado(
                        id=row.get('id'),
                        usuario_id=row.get('usuario_id'),
                        estado_id=row.get('estado_id'),
                        accion=row.get('accion', ''),
                        estado_anterior=row.get('estado_anterior'),
                        estado_nuevo=row.get('estado_nuevo'),
                        fecha_accion=row.get('fecha_accion'),
                        usuario_modificador=row.get('creado_por'),
                        motivo=row.get('motivo'),
                        detalles=row.get('detalles'),
                        ip_origen=row.get('ip_origen'),
                        modificador_nombre=row.get('modificador_nombre'),
                        estado_actual_nombre=row.get('estado_actual_nombre')
                    )
                    historial.append(historial_item)
                return historial
    

    def generar_reporte_usuarios_periodo(self, fecha_inicio: str, fecha_fin: str, incluir_inactivos: bool = False) -> dict:
        """Genera reporte automático de usuarios por período con una consulta optimizada."""
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                
                filtro_activo = "" if incluir_inactivos else "AND u.activo = TRUE"
                
                query = f"""
                    SELECT
                        (SELECT COUNT(*) FROM usuarios u WHERE u.fecha_registro BETWEEN %s AND %s {filtro_activo}) as total_registros,
                        (SELECT COUNT(*) FROM usuarios u WHERE u.rol = 'miembro' AND u.fecha_registro BETWEEN %s AND %s {filtro_activo}) as miembros_registrados,
                        (SELECT COUNT(*) FROM usuarios u WHERE u.rol = 'profesor' AND u.fecha_registro BETWEEN %s AND %s {filtro_activo}) as profesores_registrados,
                        (SELECT COUNT(*) FROM usuarios u WHERE u.rol = 'dueño' AND u.fecha_registro BETWEEN %s AND %s {filtro_activo}) as dueños_registrados,
                        (SELECT COUNT(*) FROM pagos p JOIN usuarios u ON p.usuario_id = u.id WHERE p.fecha_pago BETWEEN %s AND %s {filtro_activo}) as total_pagos,
                        (SELECT SUM(p.monto) FROM pagos p JOIN usuarios u ON p.usuario_id = u.id WHERE p.fecha_pago BETWEEN %s AND %s {filtro_activo}) as monto_total_pagos,
                        (SELECT AVG(p.monto) FROM pagos p JOIN usuarios u ON p.usuario_id = u.id WHERE p.fecha_pago BETWEEN %s AND %s {filtro_activo}) as monto_promedio_pagos
                """
                
                params = [fecha_inicio, fecha_fin] * 7
                cursor.execute(query, params)
                stats = dict(zip([desc[0] for desc in cursor.description], cursor.fetchone()))
                
                # Estados más comunes
                cursor.execute(f"""
                    SELECT ue.estado, COUNT(*) as cantidad
                    FROM usuario_estados ue
                    JOIN usuarios u ON ue.usuario_id = u.id
                    WHERE ue.fecha_inicio BETWEEN %s AND %s {filtro_activo}
                    GROUP BY ue.estado
                    ORDER BY cantidad DESC
                    LIMIT 10
                """, (fecha_inicio, fecha_fin))
                estados_comunes = [dict(zip([desc[0] for desc in cursor.description], row)) for row in cursor.fetchall()]
                
                # Usuarios con más asistencias
                cursor.execute(f"""
                    SELECT u.id, u.nombre, COUNT(a.id) as asistencias
                    FROM usuarios u
                    LEFT JOIN asistencias a ON u.id = a.usuario_id AND a.fecha BETWEEN %s AND %s
                    WHERE 1=1 {filtro_activo}
                    GROUP BY u.id, u.nombre
                    HAVING COUNT(a.id) > 0
                    ORDER BY asistencias DESC
                    LIMIT 20
                """, (fecha_inicio, fecha_fin))
                top_asistencias = [dict(zip([desc[0] for desc in cursor.description], row)) for row in cursor.fetchall()]
                
                reporte = {
                    'periodo': {
                        'fecha_inicio': fecha_inicio,
                        'fecha_fin': fecha_fin,
                        'incluir_inactivos': incluir_inactivos
                    },
                    'registros': {
                        'total': stats['total_registros'],
                        'miembros': stats['miembros_registrados'],
                        'profesores': stats['profesores_registrados'],
                        'dueños': stats['dueños_registrados']
                    },
                    'estados_comunes': estados_comunes,
                    'top_asistencias': top_asistencias,
                    'resumen_pagos': {
                        'total_pagos': stats['total_pagos'],
                        'monto_total': stats['monto_total_pagos'],
                        'monto_promedio': stats['monto_promedio_pagos']
                    },
                    'fecha_generacion': datetime.now().isoformat()
                }
                
                return reporte
    

    def verificar_pin_usuario(self, usuario_id: int, pin: str) -> bool:
        """Verifica el PIN de un usuario."""
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1 FROM usuarios WHERE id = %s AND pin = %s", (usuario_id, pin))
                return cursor.fetchone() is not None
    

    def _identificar_usuarios_para_actualizar(self, cursor, config) -> list:
        """Identifica usuarios que requieren una actualización de estado"""
        cursor.execute("""
            SELECT u.id, u.nombre, u.dni, 
                   (SELECT MAX(p.fecha_pago) FROM pagos p WHERE p.usuario_id = u.id) as ultimo_pago
            FROM usuarios u
            WHERE u.activo = TRUE AND u.rol = 'socio'
        """)
        return cursor.fetchall()


    def _cambiar_estado_usuario(self, cursor, usuario_id, nuevo_estado, descripcion):
        """Crea un nuevo estado para un usuario y retorna el ID del nuevo estado"""
        # Verificar existencia de usuario para evitar error de clave foránea
        try:
            if not self.usuario_id_existe(usuario_id):
                raise ValueError(f"Usuario {usuario_id} no existe; no se puede cambiar estado")
        except Exception:
            pass
        cursor.execute("""
            INSERT INTO usuario_estados (usuario_id, estado, descripcion, creado_por)
            VALUES (%s, %s, %s, %s)
            RETURNING id
        """, (usuario_id, nuevo_estado, descripcion, 1))  # Sistema
        return cursor.fetchone()[0]


    def ejecutar_accion_masiva_usuarios(self, usuario_ids: List[int], accion: str, parametros: dict = None) -> dict:
        """Ejecuta acciones masivas sobre usuarios seleccionados"""
        if not usuario_ids:
            return {
                'exitosos': 0,
                'fallidos': 0,
                'errores': ['No se proporcionaron IDs de usuarios'],
                'detalles': [],
                'tiempo_procesamiento': 0,
                'lotes_procesados': 0
            }
        
        # Usar el sistema de cola para serializar la operación
        import uuid
        operation_id = f"mass_user_operation_{uuid.uuid4().hex[:8]}"
        future = self.mass_operation_queue.submit_operation(
            operation_id,
            self._ejecutar_accion_masiva_usuarios_interno,
            usuario_ids, accion, parametros
        )
        
        # Esperar el resultado
        return future.result(timeout=300)  # Timeout de 5 minutos
    

    def _ejecutar_accion_masiva_usuarios_interno(self, usuario_ids: List[int], accion: str, parametros: dict = None) -> dict:
        """Implementación interna de acciones masivas con transacciones atómicas robustas"""
        # Debug logging para rastrear el problema
        logging.debug(f"_ejecutar_accion_masiva_usuarios_interno: usuario_ids={usuario_ids}, tipo={type(usuario_ids)}, accion={accion}")
        
        inicio_tiempo = time.time()
        resultados = {
            'exitosos': 0,
            'fallidos': 0,
            'errores': [],
            'detalles': [],
            'tiempo_procesamiento': 0,
            'lotes_procesados': 0,
            'reintentos_totales': 0,
            'transacciones_exitosas': 0,
            'transacciones_fallidas': 0
        }
        
        # Validaciones iniciales robustas
        if not usuario_ids or not isinstance(usuario_ids, list):
            resultados['errores'].append("Lista de usuario_ids inválida o vacía")
            return resultados
        
        if not accion or not isinstance(accion, str):
            resultados['errores'].append("Acción inválida o no especificada")
            return resultados
        
        # Verificar conexión a la base de datos
        if not hasattr(self, 'get_connection_context'):
            resultados['errores'].append("Método de conexión a base de datos no disponible")
            return resultados
        
        logging.info(f"_ejecutar_accion_masiva_usuarios_interno: Iniciando procesamiento de {len(usuario_ids)} usuarios con acción '{accion}'")
        
        # Procesar en lotes para mejor rendimiento y transacciones más pequeñas
        batch_size = 25  # Reducido para transacciones más rápidas
        total_lotes = (len(usuario_ids) + batch_size - 1) // batch_size
        
        # Configuración de reintentos robusta
        max_retries = 5  # Aumentado para mayor robustez
        base_delay = 0.1  # Delay base en segundos
        max_delay = 2.0   # Delay máximo
        
        for lote_num in range(total_lotes):
            inicio_lote = lote_num * batch_size
            fin_lote = min(inicio_lote + batch_size, len(usuario_ids))
            lote = usuario_ids[inicio_lote:fin_lote]
            
            logging.debug(f"_ejecutar_accion_masiva_usuarios_interno: Procesando lote {lote_num + 1}/{total_lotes} con {len(lote)} usuarios")
            
            # Reintentos para cada lote con transacciones atómicas
            lote_exitoso = False
            for intento in range(max_retries):
                try:
                    # Usar transacción atómica robusta con rollback automático
                    with self._connection_pool.transaction() as conn:
                        # Configurar nivel de aislamiento para evitar deadlocks
                        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
                        
                        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                            # Establecer timeout para la transacción
                            cursor.execute("SET statement_timeout = '30s'")
                            
                            # Verificar que todos los usuarios del lote existen antes de procesar
                            placeholders = ','.join(['%s' for _ in lote])
                            cursor.execute(f"SELECT id FROM usuarios WHERE id IN ({placeholders})", lote)
                            
                            # MANEJO SEGURO DE cursor.fetchall() - verificar None
                            result_rows = cursor.fetchall()
                            if result_rows is None:
                                logging.error(f"_ejecutar_accion_masiva_usuarios_interno: Error al consultar usuarios - resultado None para lote {lote}")
                                resultados['errores'].append(f"Error al consultar usuarios en lote {lote_num + 1}")
                                resultados['fallidos'] += len(lote)
                                continue
                            
                            # Extraer IDs de forma segura
                            usuarios_existentes = []
                            for row in result_rows:
                                if row and 'id' in row and isinstance(row['id'], int):
                                    usuarios_existentes.append(row['id'])
                                else:
                                    logging.warning(f"_ejecutar_accion_masiva_usuarios_interno: Fila inválida en resultado: {row}")
                            
                            usuarios_no_encontrados = set(lote) - set(usuarios_existentes)
                            if usuarios_no_encontrados:
                                logging.warning(f"_ejecutar_accion_masiva_usuarios_interno: Usuarios no encontrados en lote {lote_num + 1}: {usuarios_no_encontrados}")
                                resultados['errores'].append(f"Usuarios no encontrados en lote {lote_num + 1}: {list(usuarios_no_encontrados)}")
                                resultados['fallidos'] += len(usuarios_no_encontrados)
                            
                            # Procesar solo usuarios existentes
                            if usuarios_existentes:
                                # Procesar según el tipo de acción con validaciones adicionales
                                if accion in ['activar', 'desactivar']:
                                    self._procesar_lote_activacion_robusto(cursor, usuarios_existentes, accion, resultados)
                                elif accion == 'cambiar_rol' and parametros and 'nuevo_rol' in parametros:
                                    self._procesar_lote_cambio_rol_robusto(cursor, usuarios_existentes, parametros['nuevo_rol'], resultados)
                                elif accion == 'eliminar':
                                    # Validar que usuarios_existentes es una lista de enteros
                                    if not isinstance(usuarios_existentes, list) or not all(isinstance(uid, int) for uid in usuarios_existentes):
                                        logging.error(f"_ejecutar_accion_masiva_usuarios_interno: usuarios_existentes inválido: {usuarios_existentes}, tipo: {type(usuarios_existentes)}")
                                        resultados['errores'].append(f"Lista de usuarios inválida para eliminación: {usuarios_existentes}")
                                        resultados['fallidos'] += len(lote)
                                    else:
                                        self._procesar_lote_eliminacion_robusto(cursor, usuarios_existentes, resultados)
                                else:
                                    # Acciones individuales complejas
                                    self._procesar_acciones_individuales_robusto(cursor, usuarios_existentes, accion, parametros, resultados)
                            
                            # Confirmar transacción
                            conn.commit()
                            resultados['lotes_procesados'] += 1
                            resultados['transacciones_exitosas'] += 1
                            lote_exitoso = True
                            
                            logging.debug(f"_ejecutar_accion_masiva_usuarios_interno: Lote {lote_num + 1} procesado exitosamente en intento {intento + 1}")
                            break  # Éxito, salir del bucle de reintentos
                            
                except psycopg2.OperationalError as e:
                    error_msg = str(e).lower()
                    resultados['reintentos_totales'] += 1
                    
                    if ("deadlock" in error_msg or "lock" in error_msg or "timeout" in error_msg) and intento < max_retries - 1:
                        # Calcular delay con backoff exponencial y jitter
                        delay = min(base_delay * (2 ** intento) + random.uniform(0, 0.1), max_delay)
                        logging.warning(f"_ejecutar_accion_masiva_usuarios_interno: Error de concurrencia en lote {lote_num + 1}, intento {intento + 1}. Reintentando en {delay:.2f}s: {e}")
                        time.sleep(delay)
                        continue
                    else:
                        error_detail = f'Error operacional en lote {lote_num + 1} después de {intento + 1} intentos: {str(e)}'
                        logging.error(f"_ejecutar_accion_masiva_usuarios_interno: {error_detail}")
                        resultados['errores'].append(error_detail)
                        resultados['fallidos'] += len(lote)
                        resultados['transacciones_fallidas'] += 1
                        break
                        
                except psycopg2.IntegrityError as e:
                    error_detail = f'Error de integridad en lote {lote_num + 1}: {str(e)}'
                    logging.error(f"_ejecutar_accion_masiva_usuarios_interno: {error_detail}")
                    resultados['errores'].append(error_detail)
                    resultados['fallidos'] += len(lote)
                    resultados['transacciones_fallidas'] += 1
                    break  # No reintentar errores de integridad
                    
                except Exception as e:
                    error_detail = f'Error inesperado en lote {lote_num + 1}, intento {intento + 1}: {str(e)}'
                    logging.error(f"_ejecutar_accion_masiva_usuarios_interno: {error_detail}")
                    
                    if intento < max_retries - 1:
                        delay = min(base_delay * (2 ** intento), max_delay)
                        logging.info(f"_ejecutar_accion_masiva_usuarios_interno: Reintentando lote {lote_num + 1} en {delay:.2f}s")
                        time.sleep(delay)
                        resultados['reintentos_totales'] += 1
                        continue
                    else:
                        resultados['errores'].append(error_detail)
                        resultados['fallidos'] += len(lote)
                        resultados['transacciones_fallidas'] += 1
                        break
            
            if not lote_exitoso:
                logging.error(f"_ejecutar_accion_masiva_usuarios_interno: Lote {lote_num + 1} falló después de {max_retries} intentos")
        
        # Calcular tiempo de procesamiento y estadísticas finales
        resultados['tiempo_procesamiento'] = round(time.time() - inicio_tiempo, 3)
        
        # Limpiar caché de usuarios después de operaciones masivas
        try:
            if hasattr(self, 'cache') and self.cache:
                self.cache.invalidate('usuarios')
                logging.debug("_ejecutar_accion_masiva_usuarios_interno: Caché de usuarios invalidado")
        except Exception as cache_error:
            logging.warning(f"_ejecutar_accion_masiva_usuarios_interno: Error invalidando caché: {cache_error}")
        
        # Log de resumen
        logging.info(f"_ejecutar_accion_masiva_usuarios_interno: Completado - Exitosos: {resultados['exitosos']}, Fallidos: {resultados['fallidos']}, Tiempo: {resultados['tiempo_procesamiento']}s, Reintentos: {resultados['reintentos_totales']}")
        
        return resultados
    

    def _procesar_lote_cambio_rol_robusto(self, cursor, lote: List[int], nuevo_rol: str, resultados: dict):
        """Procesa un lote de cambio de rol de usuarios con validaciones robustas"""
        try:
            # Validar que el nuevo rol es válido
            roles_validos = ['socio', 'profesor', 'administrador', 'dueño']
            if nuevo_rol not in roles_validos:
                error_msg = f"Rol inválido: {nuevo_rol}. Roles válidos: {', '.join(roles_validos)}"
                logging.error(f"_procesar_lote_cambio_rol_robusto: {error_msg}")
                resultados['errores'].append(error_msg)
                resultados['fallidos'] += len(lote)
                return
            
            # Verificar que todos los usuarios existen y obtener su rol actual
            placeholders = ','.join(['%s' for _ in lote])
            cursor.execute(
                f"SELECT id, nombre, dni, rol FROM usuarios WHERE id IN ({placeholders})",
                lote
            )
            
            # MANEJO SEGURO DE cursor.fetchall() - verificar None y validar estructura
            result_rows = cursor.fetchall()
            if result_rows is None:
                logging.error("_procesar_lote_cambio_rol_robusto: Error al consultar usuarios - resultado None")
                resultados['errores'].append("Error al consultar usuarios para cambio de rol")
                resultados['fallidos'] += len(lote)
                return
            
            # Validar estructura de cada fila y construir lista segura
            usuarios_existentes = []
            for row in result_rows:
                if (row and isinstance(row, dict) and 
                    'id' in row and 'nombre' in row and 'dni' in row and 'rol' in row and
                    isinstance(row['id'], int)):
                    usuarios_existentes.append(row)
                else:
                    logging.warning(f"_procesar_lote_cambio_rol_robusto: Fila inválida en resultado: {row}")
                    resultados['errores'].append(f"Datos de usuario inválidos encontrados: {row}")
            
            if len(usuarios_existentes) != len(lote):
                usuarios_encontrados = [u['id'] for u in usuarios_existentes]
                usuarios_faltantes = set(lote) - set(usuarios_encontrados)
                logging.warning(f"_procesar_lote_cambio_rol_robusto: Usuarios no encontrados: {usuarios_faltantes}")
                resultados['errores'].append(f"Usuarios no encontrados para cambio de rol: {list(usuarios_faltantes)}")
                resultados['fallidos'] += len(usuarios_faltantes)
            
            # Filtrar usuarios que ya tienen el rol deseado
            usuarios_a_cambiar = [u for u in usuarios_existentes if u['rol'] != nuevo_rol]
            usuarios_sin_cambio = [u for u in usuarios_existentes if u['rol'] == nuevo_rol]
            
            if usuarios_sin_cambio:
                logging.info(f"_procesar_lote_cambio_rol_robusto: {len(usuarios_sin_cambio)} usuarios ya tienen el rol '{nuevo_rol}'")
            
            # Verificar restricciones especiales para rol 'dueño'
            if nuevo_rol == 'dueño':
                # Solo debe haber un dueño en el sistema
                cursor.execute("SELECT COUNT(*) FROM usuarios WHERE rol = 'dueño'")
                result = cursor.fetchone()
                
                # MANEJO SEGURO DE cursor.fetchone() - verificar None
                if result is None:
                    logging.error("_procesar_lote_cambio_rol_robusto: Error al consultar usuarios dueño - resultado None")
                    resultados['errores'].append("Error al verificar usuarios dueño existentes")
                    resultados['fallidos'] += len(usuarios_a_cambiar)
                    return
                
                # Con RealDictCursor, COUNT(*) se accede por el nombre de la columna
                duenos_existentes = result.get('count', 0) if result else 0
                logging.debug(f"_procesar_lote_cambio_rol_robusto: Dueños existentes: {duenos_existentes}")
                
                if duenos_existentes > 0 and len(usuarios_a_cambiar) > 0:
                    error_msg = "Solo puede haber un usuario con rol 'dueño' en el sistema"
                    logging.error(f"_procesar_lote_cambio_rol_robusto: {error_msg}")
                    resultados['errores'].append(error_msg)
                    resultados['fallidos'] += len(usuarios_a_cambiar)
                    return
            
            if usuarios_a_cambiar:
                ids_a_cambiar = [u['id'] for u in usuarios_a_cambiar]
                placeholders_cambio = ','.join(['%s' for _ in ids_a_cambiar])
                
                # Actualizar con timestamp de modificación
                cursor.execute(
                    f"""UPDATE usuarios 
                        SET rol = %s, 
                            fecha_modificacion = CURRENT_TIMESTAMP
                        WHERE id IN ({placeholders_cambio})""",
                    [nuevo_rol] + ids_a_cambiar
                )
                
                affected_rows = cursor.rowcount
                resultados['exitosos'] += affected_rows
                
                # Registrar en log de auditoría si existe la tabla
                try:
                    for usuario in usuarios_a_cambiar:
                        cursor.execute(
                            """INSERT INTO log_auditoria (tabla, accion, registro_id, datos_anteriores, datos_nuevos, usuario_modificacion, fecha_modificacion)
                               VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)""",
                            ('usuarios', 'cambiar_rol', usuario['id'], 
                             f"rol: {usuario['rol']}", f"rol: {nuevo_rol}", 
                             'sistema_masivo')
                        )
                except psycopg2.Error:
                    # Si no existe la tabla de auditoría, continuar sin error
                    pass
                
                resultados['detalles'].append(f'Lote cambio rol: {affected_rows} usuarios cambiados a rol "{nuevo_rol}" exitosamente')
                logging.info(f"_procesar_lote_cambio_rol_robusto: {affected_rows} usuarios cambiados a rol '{nuevo_rol}' exitosamente")
            else:
                resultados['detalles'].append(f'Lote cambio rol: 0 usuarios requerían cambios a rol "{nuevo_rol}"')
            
        except psycopg2.Error as e:
            error_msg = f'Error de base de datos en lote cambio rol: {str(e)}'
            logging.error(f"_procesar_lote_cambio_rol_robusto: {error_msg}")
            resultados['errores'].append(error_msg)
            resultados['fallidos'] += len(lote)
            raise  # Re-lanzar para manejo de transacciones
        except Exception as e:
            error_msg = f'Error inesperado en lote cambio rol: {str(e)}'
            logging.error(f"_procesar_lote_cambio_rol_robusto: {error_msg}")
            resultados['errores'].append(error_msg)
            resultados['fallidos'] += len(lote)
            raise  # Re-lanzar para manejo de transacciones
    

    def obtener_rutinas_por_usuario(self, usuario_id: int) -> List[Rutina]:
        """Obtiene todas las rutinas de un usuario"""
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(
                    "SELECT id, usuario_id, nombre_rutina, descripcion, dias_semana, categoria, fecha_creacion, activa "
                    "FROM rutinas WHERE usuario_id = %s ORDER BY fecha_creacion DESC",
                    (usuario_id,)
                )
                return [Rutina(**dict(r)) for r in cursor.fetchall()]
    

    def obtener_rutinas_usuario(self, usuario_id: int) -> List[Rutina]:
        """Alias for obtener_rutinas_por_usuario - for compatibility"""
        return self.obtener_rutinas_por_usuario(usuario_id)


    def inscribir_usuario_en_clase(self, clase_horario_id: int, usuario_id: int) -> bool:
        """Inscribe un usuario en una clase. Retorna True si se inscribió, False si se agregó a lista de espera"""
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                # Validación de ventana de inscripción relativa a la próxima sesión
                try:
                    cursor.execute("SELECT dia_semana, hora_inicio FROM clases_horarios WHERE id = %s", (clase_horario_id,))
                    row = cursor.fetchone()
                    if row:
                        dia_semana, hora_inicio = row
                        from datetime import datetime, timedelta, time
                        now = datetime.now()
                        # Normalizar hora_inicio
                        try:
                            if isinstance(hora_inicio, str):
                                parts = [int(x) for x in str(hora_inicio).split(":")[:2]]
                                start_t = time(parts[0], parts[1] if len(parts) > 1 else 0)
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
                            next_date = (now.date() + timedelta(days=days_ahead))
                            next_dt = datetime.combine(next_date, start_t)
                            if days_ahead == 0 and now >= next_dt:
                                next_dt = next_dt + timedelta(days=7)
                            try:
                                ventana_horas = int(self.obtener_configuracion('ventana_inscripcion_horas') or '72')
                            except Exception:
                                ventana_horas = 72
                            diff_hours = (next_dt - now).total_seconds() / 3600.0
                            if diff_hours > ventana_horas:
                                # Fuera de ventana: agregar a lista de espera
                                try:
                                    self.agregar_a_lista_espera_completo(clase_horario_id, usuario_id)
                                except Exception:
                                    pass
                                return False
                except Exception:
                    # Si falla la validación, continuar con la lógica estándar
                    pass
                # Evitar duplicados: si ya está inscripto, no generar error
                try:
                    cursor.execute(
                        "SELECT 1 FROM clase_usuarios WHERE clase_horario_id = %s AND usuario_id = %s",
                        (clase_horario_id, usuario_id)
                    )
                    if cursor.fetchone():
                        return True
                except Exception:
                    # Continuar; si la tabla no existe o hay un error, dejar que la lógica principal maneje
                    pass
                # Verificar si hay cupo disponible
                if self.verificar_cupo_disponible(clase_horario_id):
                    # Hay cupo, inscribir directamente
                    sql = "INSERT INTO clase_usuarios (clase_horario_id, usuario_id) VALUES (%s, %s)"
                    cursor.execute(sql, (clase_horario_id, usuario_id))
                    conn.commit()
                    return True
                else:
                    # No hay cupo, agregar a lista de espera
                    # Usar versión completa que mantiene posición y estado en clase_lista_espera
                    self.agregar_a_lista_espera_completo(clase_horario_id, usuario_id)
                    return False


    def obtener_nuevos_usuarios_por_mes(self) -> List[dict]:
        """Obtiene el conteo de nuevos usuarios por mes para los últimos 12 meses"""
        cache_key = "nuevos_usuarios_por_mes"
        cached_result = self.cache.get('reportes', cache_key)
        if cached_result:
            return cached_result
        
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            # Consulta adaptada para PostgreSQL usando EXTRACT y DATE_TRUNC
            cursor.execute("""
                SELECT 
                    EXTRACT(YEAR FROM fecha_registro) as año,
                    EXTRACT(MONTH FROM fecha_registro) as mes,
                    COUNT(*) as nuevos_usuarios
                FROM usuarios 
                WHERE fecha_registro >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '11 months')
                  AND rol = 'socio'
                GROUP BY EXTRACT(YEAR FROM fecha_registro), EXTRACT(MONTH FROM fecha_registro)
                ORDER BY año, mes
            """)
            
            # Formatear como diccionario {mes_año: cantidad} para compatibilidad con widgets
            resultados = {}
            for row in cursor.fetchall():
                # PostgreSQL EXTRACT returns Decimal, need to convert to int
                año = int(row['año']) if row['año'] is not None else 0
                mes = int(row['mes']) if row['mes'] is not None else 1
                count = row['nuevos_usuarios']
                # Crear clave en formato "Mes Año" (ej: "Ene 2024")
                meses = ['Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun',
                        'Jul', 'Ago', 'Sep', 'Oct', 'Nov', 'Dic']
                mes_nombre = meses[mes - 1]
                clave = f"{mes_nombre} {año}"
                resultados[clave] = count
            
            # Cache por 1 hora
            self.cache.set('reportes', cache_key, resultados)
            
            return resultados


    def obtener_nuevos_usuarios_por_mes_ultimos_12(self) -> Dict[str, int]:
        """Devuelve nuevos usuarios por mes (últimos 12 meses) con clave 'YYYY-MM'."""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT to_char(date_trunc('month', fecha_registro), 'YYYY-MM') AS m, COUNT(*)
                    FROM usuarios
                    WHERE fecha_registro >= date_trunc('month', CURRENT_DATE) - INTERVAL '11 months'
                    GROUP BY 1
                    ORDER BY 1
                    """
                )
                out: Dict[str, int] = {}
                for m, c in cursor.fetchall():
                    out[str(m)] = int(c or 0)
                return out
        except Exception as e:
            logging.error(f"Error obtener_nuevos_usuarios_por_mes_ultimos_12: {e}")
            return {}


    def obtener_nuevos_usuarios_por_mes_rango(self, fecha_inicio: date, fecha_fin: date) -> Dict[str, int]:
        """Devuelve nuevos usuarios por mes para un rango específico con clave 'YYYY-MM'."""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT to_char(date_trunc('month', fecha_registro), 'YYYY-MM') AS m, COUNT(*)
                    FROM usuarios
                    WHERE fecha_registro BETWEEN %s AND %s
                    GROUP BY 1
                    ORDER BY 1
                    """,
                    (fecha_inicio, fecha_fin)
                )
                out: Dict[str, int] = {}
                for m, c in cursor.fetchall():
                    out[str(m)] = int(c or 0)
                return out
        except Exception as e:
            logging.error(f"Error obtener_nuevos_usuarios_por_mes_rango: {e}")
            return {}


    def obtener_asistencias_usuario(self, usuario_id: int, fecha_inicio: date = None, fecha_fin: date = None) -> List[dict]:
        """Obtiene las asistencias de un usuario en un rango de fechas"""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                if fecha_inicio and fecha_fin:
                    cursor.execute("""
                        SELECT id, usuario_id, fecha, hora_registro FROM asistencias 
                        WHERE usuario_id = %s AND fecha BETWEEN %s AND %s
                        ORDER BY fecha DESC
                    """, (usuario_id, fecha_inicio, fecha_fin))
                else:
                    cursor.execute("""
                        SELECT id, usuario_id, fecha, hora_registro FROM asistencias 
                        WHERE usuario_id = %s
                        ORDER BY fecha DESC
                    """, (usuario_id,))
                
                rows = cursor.fetchall()
                return [
                    {
                        'id': row['id'],
                        'usuario_id': row['usuario_id'],
                        'fecha': row['fecha'],
                        'hora_registro': row['hora_registro']
                    }
                    for row in rows if row
                ]
        except Exception as e:
            logging.error(f"Error obteniendo asistencias del usuario {usuario_id}: {e}")
            return []


    def contar_usuarios_por_tipo_cuota(self, tipo_cuota: str) -> int:
        """Cuenta cuántos usuarios están usando un tipo de cuota específico"""
        try:
            with self.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    # Convertir tipo_cuota a string para evitar error de tipos
                    cursor.execute(
                        "SELECT COUNT(*) FROM usuarios WHERE tipo_cuota::text = %s AND activo = TRUE",
                        (str(tipo_cuota),)
                    )
                    result = cursor.fetchone()
                    return result[0] if result else 0
        except Exception as e:
            logging.error(f"Error al contar usuarios por tipo de cuota: {str(e)}")
            return 0
    
    # === MÉTODOS DE GESTIÓN DE RUTINAS ===
    

    def enforce_single_active_rutina_usuario(self, usuario_id: int) -> int:
        """Fuerza que solo la rutina más reciente del usuario quede activa.

        Desactiva todas las rutinas del usuario y activa únicamente la más reciente
        según `fecha_creacion` (con `NULLS LAST`) y desempate por `id`.

        Returns: id de la rutina marcada activa (o 0 si no hay rutinas).
        """
        try:
            with self.get_connection_context() as conn:
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                # Encontrar la rutina más reciente
                cur.execute(
                    """
                    SELECT id FROM rutinas
                    WHERE usuario_id = %s
                    ORDER BY fecha_creacion DESC NULLS LAST, id DESC
                    LIMIT 1
                    """,
                    (usuario_id,),
                )
                row = cur.fetchone()
                latest_id = int(row.get('id')) if row and row.get('id') is not None else None
                # Desactivar todas
                cur.execute("UPDATE rutinas SET activa = FALSE WHERE usuario_id = %s", (usuario_id,))
                # Activar la más reciente
                if latest_id is not None:
                    cur.execute("UPDATE rutinas SET activa = TRUE WHERE id = %s", (latest_id,))
                    conn.commit()
                    try:
                        self.cache.invalidate('rutinas')
                    except Exception:
                        pass
                    return latest_id or 0
                conn.commit()
                try:
                    self.cache.invalidate('rutinas')
                except Exception:
                    pass
                return 0
        except Exception as e:
            logging.error(f"Error enforcing single active rutina for usuario {usuario_id}: {e}")
            return 0
    

    def obtener_rutinas_por_usuario(self, usuario_id: int) -> List:
        """Obtiene todas las rutinas de un usuario."""
        # Cache por usuario
        try:
            cached = self.cache.get('rutinas', ('usuario', int(usuario_id)))
            if cached is not None:
                return cached
        except Exception:
            pass
        with self.get_connection_context() as conn:
            # Asegurar columna y valores de uuid_rutina listos si existe
            try:
                self.ensure_rutina_uuid_ready(conn)
            except Exception:
                pass
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            try:
                cols = self.get_table_columns('rutinas') or []
            except Exception:
                cols = []
            base_cols = ['id', 'usuario_id', 'nombre_rutina', 'descripcion', 'dias_semana', 'categoria', 'fecha_creacion', 'activa']
            select_cols = base_cols + (['uuid_rutina'] if 'uuid_rutina' in cols else [])
            sql = f"SELECT {', '.join(select_cols)} FROM rutinas WHERE usuario_id = %s ORDER BY fecha_creacion DESC"
            cursor.execute(sql, (usuario_id,))
            result = [dict(r) for r in cursor.fetchall()]
            try:
                self.cache.set('rutinas', ('usuario', int(usuario_id)), result)
            except Exception:
                pass
            return result
    

    def obtener_usuarios_en_clase(self, clase_horario_id: int) -> List[dict]:
        """Obtiene usuarios inscritos en una clase."""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            sql = """
            SELECT cu.*, u.nombre as nombre_usuario 
            FROM clase_usuarios cu 
            JOIN usuarios u ON cu.usuario_id = u.id 
            WHERE cu.clase_horario_id = %s 
            ORDER BY u.nombre
            """
            cursor.execute(sql, (clase_horario_id,))
            return [dict(r) for r in cursor.fetchall()]
    

    def quitar_usuario_de_clase(self, clase_horario_id: int, usuario_id: int):
        """Quita un usuario de una clase y procesa la lista de espera."""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            sql = "DELETE FROM clase_usuarios WHERE clase_horario_id = %s AND usuario_id = %s"
            cursor.execute(sql, (clase_horario_id, usuario_id))
            conn.commit()
            
            # Procesar liberación de cupo (mover siguiente de lista de espera)
            self.procesar_liberacion_cupo_completo(clase_horario_id)
    

    def crear_backup_selectivo_usuarios(self, criterios: dict = None) -> dict:
        """Crea backup selectivo de datos de usuarios según criterios"""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                # Construir query según criterios
                where_clauses = []
                params = []
                
                if criterios:
                    if criterios.get('activos_solo'):
                        where_clauses.append("u.activo = true")
                    if criterios.get('fecha_registro_desde'):
                        where_clauses.append("u.fecha_registro >= %s")
                        params.append(criterios['fecha_registro_desde'])
                    if criterios.get('rol'):
                        where_clauses.append("u.rol = %s")
                        params.append(criterios['rol'])
                
                where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
                
                # Obtener usuarios
                query = f"""
                    SELECT u.*, tc.nombre as tipo_cuota_nombre, tc.precio as tipo_cuota_precio
                    FROM usuarios u
                    LEFT JOIN tipos_cuota tc ON u.tipo_cuota::integer = tc.id
                    WHERE {where_sql}
                """
                
                cursor.execute(query, params)
                usuarios = [dict(zip([desc[0] for desc in cursor.description], row)) for row in cursor.fetchall()]
                
                # Crear archivo de backup
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_filename = f"backup_usuarios_{timestamp}.json"
                backup_path = f"backups/{backup_filename}"
                
                import json
                import os
                
                os.makedirs("backups", exist_ok=True)
                
                backup_data = {
                    'metadata': {
                        'fecha_backup': datetime.now().isoformat(),
                        'criterios': criterios or {},
                        'total_usuarios': len(usuarios)
                    },
                    'usuarios': usuarios
                }
                
                with open(backup_path, 'w', encoding='utf-8') as f:
                    json.dump(backup_data, f, ensure_ascii=False, indent=2, default=str)
                
                return {
                    'archivo': backup_path,
                    'total_usuarios': len(usuarios),
                    'tamaño_mb': round(os.path.getsize(backup_path) / 1024 / 1024, 2)
                }
                
        except Exception as e:
            logging.error(f"Error creando backup selectivo: {e}")
            return {'error': str(e)}
    
    # === ADVANCED REPORTING METHODS ===
    

    def obtener_usuarios_por_fecha_registro(self, fecha_limite):
        """Obtiene usuarios registrados después de una fecha límite"""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cursor.execute("""
                    SELECT u.* FROM usuarios u
                    WHERE u.fecha_registro >= %s
                    ORDER BY u.fecha_registro DESC
                """, (fecha_limite,))
                
                usuarios = []
                for row in cursor.fetchall():
                    data = dict(row)
                    apellido = data.pop('apellido', None)
                    nombre = (data.get('nombre') or '').strip()
                    if apellido:
                        ap = str(apellido).strip()
                        if ap and ap not in nombre:
                            nombre = f"{nombre} {ap}".strip() if nombre else ap
                    data['nombre'] = nombre
                    data.pop('email', None)
                    allowed = {'id','nombre','dni','telefono','pin','rol','notas','fecha_registro','activo','tipo_cuota','fecha_proximo_vencimiento','cuotas_vencidas','ultimo_pago'}
                    filtered = {k: data.get(k) for k in allowed if k in data}
                    usuarios.append(Usuario(**filtered))
                
                return usuarios
            
        except Exception as e:
            logging.error(f"Error obteniendo usuarios por fecha de registro: {e}")
            return []
    

    def obtener_usuarios_sin_pagos_recientes(self, fecha_limite):
        """Obtiene usuarios que no han realizado pagos después de una fecha límite"""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                cursor.execute("""
                    SELECT u.*
                    FROM usuarios u
                    WHERE u.activo = true 
                      AND u.rol != 'dueño'
                      AND NOT EXISTS (
                          SELECT 1 
                          FROM pagos p 
                          WHERE p.usuario_id = u.id 
                            AND p.fecha_pago >= %s::date
                      )
                    ORDER BY (
                        SELECT MAX(fecha_pago) 
                        FROM pagos p2 
                        WHERE p2.usuario_id = u.id
                    ) ASC NULLS FIRST
                """, (fecha_limite.date(),))
                
                usuarios = []
                for row in cursor.fetchall():
                    data = dict(row)
                    apellido = data.pop('apellido', None)
                    nombre = (data.get('nombre') or '').strip()
                    if apellido:
                        ap = str(apellido).strip()
                        if ap and ap not in nombre:
                            nombre = f"{nombre} {ap}".strip() if nombre else ap
                    data['nombre'] = nombre
                    data.pop('email', None)
                    allowed = {'id','nombre','dni','telefono','pin','rol','notas','fecha_registro','activo','tipo_cuota','fecha_proximo_vencimiento','cuotas_vencidas','ultimo_pago'}
                    filtered = {k: data.get(k) for k in allowed if k in data}
                    usuarios.append(Usuario(**filtered))
                
                return usuarios
            
        except Exception as e:
            logging.error(f"Error obteniendo usuarios sin pagos recientes: {e}")
            return []
    

    def obtener_usuarios_reporte_completo(self):
        """Obtiene todos los usuarios con información completa para reportes"""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                cursor.execute("""
                    SELECT u.*
                    FROM usuarios u
                    WHERE u.activo = true
                    ORDER BY u.nombre
                """)
                
                usuarios = []
                for row in cursor.fetchall():
                    data = dict(row)
                    apellido = data.pop('apellido', None)
                    nombre = (data.get('nombre') or '').strip()
                    if apellido:
                        ap = str(apellido).strip()
                        if ap and ap not in nombre:
                            nombre = f"{nombre} {ap}".strip() if nombre else ap
                    data['nombre'] = nombre
                    data.pop('email', None)
                    allowed = {'id','nombre','dni','telefono','pin','rol','notas','fecha_registro','activo','tipo_cuota','fecha_proximo_vencimiento','cuotas_vencidas','ultimo_pago'}
                    filtered = {k: data.get(k) for k in allowed if k in data}
                    usuarios.append(Usuario(**filtered))
                
                return usuarios
            
        except Exception as e:
            logging.error(f"Error obteniendo usuarios para reporte completo: {e}")
            return []
    

    def obtener_asistencias_usuario(self, usuario_id, limit=None):
        """Obtiene las asistencias de un usuario específico con optimización PostgreSQL"""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                # Optimización: Establecer límite por defecto para evitar consultas masivas
                if limit is None:
                    limit = 100  # Límite por defecto razonable
                
                cursor.execute("""
                    SELECT id, fecha, hora_registro
                    FROM asistencias 
                    WHERE usuario_id = %s
                    ORDER BY fecha DESC, hora_registro DESC
                    LIMIT %s
                """, (usuario_id, limit))
                
                # Optimización: Usar list comprehension para mejor rendimiento con manejo seguro
                rows = cursor.fetchall()
                return [
                    {
                        'id': row['id'],
                        'fecha': row['fecha'],
                        'hora_registro': row['hora_registro']
                    }
                    for row in rows if row
                ]
            
        except Exception as e:
            logging.error(f"Error obteniendo asistencias del usuario {usuario_id}: {e}")
            return []
    
    # === AUDIT SYSTEM METHODS ===
    

    def obtener_historial_asistencia_usuario_completo(self, usuario_id: int, fecha_desde: str = None, 
                                            fecha_hasta: str = None) -> List[Dict]:
        """Obtiene el historial de asistencia de un usuario PostgreSQL."""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            sql = """
            SELECT 
                ah.*,
                c.nombre as clase_nombre,
                ch.dia_semana,
                ch.hora_inicio,
                ch.hora_fin
            FROM clase_asistencia_historial ah
            JOIN clases_horarios ch ON ah.clase_horario_id = ch.id
            JOIN clases c ON ch.clase_id = c.id
            WHERE ah.usuario_id = %s
            """
            
            params = [usuario_id]
            
            if fecha_desde:
                sql += " AND ah.fecha_clase >= %s"
                params.append(fecha_desde)
            
            if fecha_hasta:
                sql += " AND ah.fecha_clase <= %s"
                params.append(fecha_hasta)
            
            sql += " ORDER BY ah.fecha_clase DESC"
            
            cursor.execute(sql, params)
            return [dict(zip([desc[0] for desc in cursor.description], row)) for row in cursor.fetchall()]
    

    def obtener_estadisticas_asistencia_usuario_completas(self, usuario_id: int, meses: int = 3) -> Dict:
        """Obtiene estadísticas de asistencia de un usuario PostgreSQL."""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            sql = """
            SELECT 
                COUNT(*) as total_clases,
                SUM(CASE WHEN estado_asistencia = 'presente' THEN 1 ELSE 0 END) as presentes,
                SUM(CASE WHEN estado_asistencia = 'ausente' THEN 1 ELSE 0 END) as ausentes,
                SUM(CASE WHEN estado_asistencia = 'tardanza' THEN 1 ELSE 0 END) as tardanzas,
                SUM(CASE WHEN estado_asistencia = 'justificado' THEN 1 ELSE 0 END) as justificados
            FROM clase_asistencia_historial
            WHERE usuario_id = %s AND fecha_clase >= CURRENT_DATE - INTERVAL '%s months'
            """
            
            cursor.execute(sql, (usuario_id, meses))
            result = cursor.fetchone()
            
            if result and result[0] > 0:
                return {
                    'total_clases': result[0],
                    'presentes': result[1],
                    'ausentes': result[2],
                    'tardanzas': result[3],
                    'justificados': result[4],
                    'porcentaje_asistencia': round((result[1] / result[0]) * 100, 2)
                }
            else:
                return {
                    'total_clases': 0,
                    'presentes': 0,
                    'ausentes': 0,
                    'tardanzas': 0,
                    'justificados': 0,
                    'porcentaje_asistencia': 0
                }
    
    # --- MÉTODOS PARA NOTIFICACIONES ---
    

    def obtener_notificaciones_usuario_completas(self, usuario_id: int, solo_no_leidas: bool = False) -> List[Dict]:
        """Obtiene las notificaciones de un usuario PostgreSQL."""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            sql = """
            SELECT 
                n.*,
                c.nombre as clase_nombre,
                ch.dia_semana,
                ch.hora_inicio
            FROM notificaciones_cupos n
            JOIN clases_horarios ch ON n.clase_horario_id = ch.id
            JOIN clases c ON ch.clase_id = c.id
            WHERE n.usuario_id = %s AND n.activa = true
            """
            
            params = [usuario_id]
            
            if solo_no_leidas:
                sql += " AND n.leida = false"
            
            sql += " ORDER BY n.fecha_creacion DESC"
            
            cursor.execute(sql, params)
            return [dict(zip([desc[0] for desc in cursor.description], row)) for row in cursor.fetchall()]
    

    def obtener_distribucion_usuarios_por_edad(self) -> dict:
        """Obtiene distribución de usuarios por grupos de edad (simulada)."""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor()
                
                # Obtener total de usuarios activos
                cursor.execute("SELECT COUNT(*) FROM usuarios WHERE activo = true")
                total_usuarios = cursor.fetchone()[0] or 0
                
                # Simular distribución por edad basada en el total
                if total_usuarios == 0:
                    return {}
                
                distribucion = {
                    '18-25': int(total_usuarios * 0.25),
                    '26-35': int(total_usuarios * 0.35),
                    '36-45': int(total_usuarios * 0.25),
                    '46-55': int(total_usuarios * 0.10),
                    'Mayor de 55': int(total_usuarios * 0.05)
                }
                
                # Ajustar para que la suma sea exacta
                diferencia = total_usuarios - sum(distribucion.values())
                if diferencia > 0:
                    distribucion['26-35'] += diferencia
                
                return distribucion
                
        except Exception as e:
            logging.error(f"Error obteniendo distribución de usuarios por edad: {str(e)}")
            return {}
    

    def contar_usuarios_totales(self) -> int:
        """Cuenta el total de usuarios en el sistema."""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM usuarios")
                return cursor.fetchone()[0] or 0
        except Exception as e:
            logging.error(f"Error contando usuarios totales: {str(e)}")
            return 0
    

    def contar_usuarios_activos(self) -> int:
        """Cuenta usuarios activos en el sistema."""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM usuarios WHERE activo = true")
                return cursor.fetchone()[0] or 0
        except Exception as e:
            logging.error(f"Error contando usuarios activos: {str(e)}")
            return 0
    

    def exportar_usuarios_csv(self, filtros=None) -> str:
        """Exporta usuarios a CSV y retorna la ruta del archivo."""
        try:
            import csv
            import tempfile
            import os
            from datetime import datetime
            
            # Crear archivo temporal
            temp_dir = tempfile.gettempdir()
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"usuarios_export_{timestamp}.csv"
            filepath = os.path.join(temp_dir, filename)
            
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                # Consulta base
                sql = "SELECT id, nombre, dni, telefono, rol, activo, fecha_registro FROM usuarios"
                params = []
                
                # Aplicar filtros si existen
                if filtros:
                    conditions = []
                    if 'activo' in filtros:
                        conditions.append("activo = %s")
                        params.append(filtros['activo'])
                    if 'rol' in filtros:
                        conditions.append("rol = %s")
                        params.append(filtros['rol'])
                    
                    if conditions:
                        sql += " WHERE " + " AND ".join(conditions)
                
                sql += " ORDER BY nombre"
                
                cursor.execute(sql, params)
                usuarios = cursor.fetchall()
                
                # Escribir CSV
                with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                    fieldnames = ['ID', 'Nombre', 'DNI', 'Teléfono', 'Rol', 'Activo', 'Fecha Registro']
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    
                    writer.writeheader()
                    for usuario in usuarios:
                        writer.writerow({
                            'ID': usuario['id'],
                            'Nombre': usuario['nombre'],
                            'DNI': usuario['dni'],
                            'Teléfono': usuario['telefono'],
                            'Rol': usuario['rol'],
                            'Activo': 'Sí' if usuario['activo'] else 'No',
                            'Fecha Registro': usuario['fecha_registro']
                        })
                
                return filepath
                
        except Exception as e:
            logging.error(f"Error exportando usuarios a CSV: {str(e)}")
            return ""
    

    def contar_usuarios_nuevos_periodo(self, fecha_inicio, fecha_fin) -> int:
        """Cuenta usuarios nuevos en un período específico."""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT COUNT(*) FROM usuarios 
                    WHERE fecha_registro BETWEEN %s AND %s
                """, (fecha_inicio, fecha_fin))
                return cursor.fetchone()[0] or 0
        except Exception as e:
            logging.error(f"Error contando usuarios nuevos en período: {str(e)}")
            return 0
    

    def obtener_distribucion_usuarios_por_genero(self) -> dict:
        """Obtiene la distribución de usuarios por género."""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                # Como la tabla usuarios no tiene columna genero, retornamos datos simulados
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_usuarios
                    FROM usuarios 
                    WHERE activo = true
                """)
                total = cursor.fetchone()['total_usuarios'] or 0
                
                # Distribución simulada basada en el total de usuarios
                if total > 0:
                    return {
                        'Masculino': int(total * 0.6),
                        'Femenino': int(total * 0.35),
                        'No especificado': total - int(total * 0.6) - int(total * 0.35)
                    }
                else:
                    return {'No especificado': 0}
        except Exception as e:
            logging.error(f"Error obteniendo distribución por género: {str(e)}")
            return {}
    

    def crear_backup_selectivo_usuarios_mejorado(self, file_path: str, criterios: dict) -> dict:
        """Crea un backup selectivo mejorado de usuarios con validaciones"""
        import json
        from datetime import datetime
        
        try:
            resultado = {
                'usuarios_procesados': 0,
                'total_registros': 0,
                'validaciones': [],
                'errores': []
            }
            
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                usuario_ids = criterios.get('usuario_ids', [])
                incluir_relacionados = criterios.get('incluir_datos_relacionados', True)
                incluir_validaciones = criterios.get('incluir_validaciones', True)
                
                if not usuario_ids:
                    raise ValueError("No se especificaron IDs de usuarios")
                
                backup_data = {
                    'metadata': {
                        'fecha_backup': datetime.now().isoformat(),
                        'version': '1.0',
                        'criterios': criterios,
                        'total_usuarios': len(usuario_ids)
                    },
                    'usuarios': [],
                    'datos_relacionados': {}
                }
                
                # Obtener datos de usuarios con columnas explícitas
                placeholders = ','.join(['%s' for _ in usuario_ids])
                def _get_columns(table: str) -> List[str]:
                    cursor.execute(
                        """
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema = 'public' AND table_name = %s
                        ORDER BY ordinal_position
                        """,
                        (table,)
                    )
                    return [r['column_name'] for r in (cursor.fetchall() or [])]

                usuario_cols = _get_columns('usuarios')
                usuarios_select = ", ".join(usuario_cols) if usuario_cols else "id, nombre, rol, activo"
                cursor.execute(f"SELECT {usuarios_select} FROM usuarios WHERE id IN ({placeholders})", usuario_ids)
                usuarios = cursor.fetchall()
                
                for usuario in usuarios:
                    usuario_dict = dict(usuario)
                    
                    # Convertir fechas a string para JSON
                    for key, value in usuario_dict.items():
                        if hasattr(value, 'isoformat'):
                            usuario_dict[key] = value.isoformat()
                    
                    backup_data['usuarios'].append(usuario_dict)
                    resultado['usuarios_procesados'] += 1
                    
                    # Validaciones
                    if incluir_validaciones:
                        if usuario['rol'] == 'dueño':
                            resultado['validaciones'].append(f"Usuario {usuario['nombre']} es dueño")
                        if not usuario['activo']:
                            resultado['validaciones'].append(f"Usuario {usuario['nombre']} está inactivo")
                
                # Datos relacionados (selecciones explícitas por tabla)
                if incluir_relacionados:
                    # Pagos
                    pagos_cols = _get_columns('pagos')
                    pagos_sel = ", ".join(pagos_cols) if pagos_cols else "id, usuario_id, monto, mes, año, fecha_pago, metodo_pago_id"
                    cursor.execute(f"SELECT {pagos_sel} FROM pagos WHERE usuario_id IN ({placeholders})", usuario_ids)
                    pagos = cursor.fetchall()
                    backup_data['datos_relacionados']['pagos'] = [dict(p) for p in pagos]
                    
                    # Asistencias
                    asist_cols = _get_columns('asistencias')
                    asist_sel = ", ".join(asist_cols) if asist_cols else "id, usuario_id, fecha, hora_registro, hora_entrada"
                    cursor.execute(f"SELECT {asist_sel} FROM asistencias WHERE usuario_id IN ({placeholders})", usuario_ids)
                    asistencias = cursor.fetchall()
                    backup_data['datos_relacionados']['asistencias'] = [dict(a) for a in asistencias]
                    
                    # Estados
                    estados_cols = _get_columns('usuario_estados')
                    estados_sel = ", ".join(estados_cols) if estados_cols else "id, usuario_id, estado, descripcion, fecha_inicio, fecha_vencimiento, activo, creado_por"
                    cursor.execute(f"SELECT {estados_sel} FROM usuario_estados WHERE usuario_id IN ({placeholders})", usuario_ids)
                    estados = cursor.fetchall()
                    backup_data['datos_relacionados']['estados'] = [dict(e) for e in estados]
                    
                    # Notas
                    notas_cols = _get_columns('usuario_notas')
                    notas_sel = ", ".join(notas_cols) if notas_cols else "id, usuario_id, categoria, titulo, contenido, importancia, fecha_creacion, fecha_modificacion, activa, autor_id"
                    cursor.execute(f"SELECT {notas_sel} FROM usuario_notas WHERE usuario_id IN ({placeholders})", usuario_ids)
                    notas = cursor.fetchall()
                    backup_data['datos_relacionados']['notas'] = [dict(n) for n in notas]
                    
                    # Etiquetas (explicitar columnas de ue y nombre de etiqueta)
                    ue_cols = _get_columns('usuario_etiquetas')
                    ue_cols_aliased = ", ".join([f"ue.{c}" for c in ue_cols]) if ue_cols else "ue.id, ue.usuario_id, ue.etiqueta_id, ue.asignado_por, ue.fecha_asignacion"
                    cursor.execute(f"""
                        SELECT {ue_cols_aliased}, e.nombre AS etiqueta_nombre
                        FROM usuario_etiquetas ue
                        JOIN etiquetas e ON ue.etiqueta_id = e.id
                        WHERE ue.usuario_id IN ({placeholders})
                    """, usuario_ids)
                    etiquetas = cursor.fetchall()
                    backup_data['datos_relacionados']['etiquetas'] = [dict(et) for et in etiquetas]
                
                # Calcular total de registros
                resultado['total_registros'] = len(backup_data['usuarios'])
                if incluir_relacionados:
                    resultado['total_registros'] += sum([
                        len(backup_data['datos_relacionados'].get('pagos', [])),
                        len(backup_data['datos_relacionados'].get('asistencias', [])),
                        len(backup_data['datos_relacionados'].get('estados', [])),
                        len(backup_data['datos_relacionados'].get('notas', [])),
                        len(backup_data['datos_relacionados'].get('etiquetas', []))
                    ])
                
                # Guardar archivo
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(backup_data, f, indent=2, ensure_ascii=False, default=str)
                
                return resultado
                
        except Exception as e:
            logging.error(f"Error creando backup selectivo: {e}")
            resultado['errores'].append(f"Error general: {str(e)}")
            return resultado
    

    def obtener_usuarios_morosos_por_mes(self, mes: int = None, anio: int = None) -> List[Dict]:
        """Obtiene usuarios con cuotas vencidas para el mes especificado"""
        try:
            # Si no se especifica mes/año, usar el actual
            if mes is None or anio is None:
                from datetime import datetime
                now = datetime.now()
                mes = mes or now.month
                anio = anio or now.year
            
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                # Buscar usuarios activos que NO tienen pago para el mes/año especificado
                cursor.execute("""
                    SELECT u.id, u.nombre, u.telefono, u.tipo_cuota,
                           COALESCE(tc.precio, 25000) as monto
                    FROM usuarios u
                    LEFT JOIN tipos_cuota tc ON u.tipo_cuota = tc.nombre
                    LEFT JOIN pagos p ON u.id = p.usuario_id AND p.mes = %s AND p.año = %s
                    WHERE u.activo = true 
                    AND u.telefono IS NOT NULL 
                    AND u.telefono != ''
                    AND p.id IS NULL  -- No tiene pago para este mes/año
                    ORDER BY u.nombre
                """, (mes, anio))
                
                usuarios_morosos = []
                for row in cursor.fetchall():
                    user_dict = dict(row)
                    # Calcular fecha de vencimiento (último día del mes)
                    from calendar import monthrange
                    ultimo_dia = monthrange(anio, mes)[1]
                    user_dict['fecha_vencimiento'] = f"{ultimo_dia:02d}/{mes:02d}/{anio}"
                    user_dict['monto'] = user_dict.get('monto', 25000)  # Asegurar que siempre hay monto
                    user_dict['vencido'] = True
                    user_dict['usuario_nombre'] = user_dict['nombre']
                    usuarios_morosos.append(user_dict)
                
                return usuarios_morosos
                
        except Exception as e:
            logging.error(f"Error al obtener usuarios morosos: {e}")
            return []
    

    def _obtener_user_id_por_telefono_whatsapp(self, telefono: str) -> Optional[int]:
        """Obtiene el user_id basado en número de WhatsApp con normalización avanzada"""
        try:
            raw = str(telefono or "").strip()
            import re
            wa_digits = re.sub(r"\D", "", raw)
            cands = set()
            if wa_digits:
                cands.add(wa_digits)
                # Manejo frecuente: Argentina (+54) móviles con "9" intercalado y/o cero troncal
                if wa_digits.startswith("54"):
                    after = wa_digits[2:]
                    cands.add(after)  # sin país
                    if after.startswith("9"):
                        after_no9 = after[1:]
                        cands.add(after_no9)  # sin país y sin 9
                        cands.add("0" + after_no9)  # con cero troncal
                    else:
                        cands.add("0" + after)  # con cero troncal
                # Considerar también 549 -> 54 (sin 9)
                if wa_digits.startswith("549"):
                    cands.add(wa_digits.replace("549", "54", 1))
            if not cands:
                return None
            with self.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    # Comparar contra versión digit-only del teléfono en DB
                    placeholders = ",".join(["%s"] * len(cands))
                    sql = f"""
                        SELECT id
                        FROM usuarios
                        WHERE regexp_replace(COALESCE(telefono,''), '\\D', '', 'g') IN ({placeholders})
                        LIMIT 1
                    """
                    cursor.execute(sql, tuple(cands))
                    row = cursor.fetchone()
                    if row:
                        try:
                            return int(row[0])
                        except Exception:
                            return None
                    # Fallback exacto
                    cursor.execute("SELECT id FROM usuarios WHERE telefono = %s LIMIT 1", (raw,))
                    r2 = cursor.fetchone()
                    return int(r2[0]) if r2 else None
        except Exception as e:
            logging.error(f"Error obteniendo user_id por teléfono: {e}")
            return None
    

    def limpiar_mensajes_fallidos_usuario(self, telefono: str, fecha_limite: datetime) -> bool:
        """Limpia mensajes fallidos de un usuario desde fecha límite"""
        try:
            with self.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """DELETE FROM whatsapp_messages 
                           WHERE phone_number = %s AND status = 'failed' AND sent_at >= %s""",
                        (telefono, fecha_limite)
                    )
                    return cursor.rowcount > 0
        except Exception as e:
            logging.error(f"Error al limpiar mensajes fallidos del usuario {telefono}: {e}")
            return False
    

    def _get_usuarios(self):
        """Obtiene lista de usuarios con paginación"""
        self.progress.emit(10)
        limit = self.params.get('limit', 100)
        offset = self.params.get('offset', 0)
        
        with self.db_manager.readonly_session() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT id, nombre, dni, telefono, rol, activo, tipo_cuota, fecha_registro
                    FROM usuarios 
                    ORDER BY nombre 
                    LIMIT %s OFFSET %s
                """, (limit, offset))
                self.progress.emit(80)
                return cur.fetchall()
    

    def _get_usuario_by_id(self):
        """Obtiene un usuario por ID"""
        usuario_id = self.params.get('usuario_id')
        if not usuario_id:
            raise ValueError("usuario_id es requerido")
        
        with self.db_manager.readonly_session() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT id, nombre, dni, telefono, rol, activo, tipo_cuota, 
                           fecha_proximo_vencimiento, cuotas_vencidas, ultimo_pago
                    FROM usuarios 
                    WHERE id = %s
                """, (usuario_id,))
                return cur.fetchone()
    

    def _get_pagos_by_usuario(self):
        """Obtiene pagos de un usuario"""
        usuario_id = self.params.get('usuario_id')
        limit = self.params.get('limit', 50)
        
        with self.db_manager.readonly_session() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT p.id, p.monto, p.fecha_pago, p.mes, p.año, mp.nombre as metodo_pago
                    FROM pagos p
                    LEFT JOIN metodos_pago mp ON mp.id = p.metodo_pago_id
                    WHERE p.usuario_id = %s
                    ORDER BY p.fecha_pago DESC
                    LIMIT %s
                """, (usuario_id, limit))
                return cur.fetchall()
    

    def _get_asistencias_by_usuario(self):
        """Obtiene asistencias de un usuario en un rango de fechas"""
        usuario_id = self.params.get('usuario_id')
        fecha_inicio = self.params.get('fecha_inicio', date.today() - timedelta(days=30))
        fecha_fin = self.params.get('fecha_fin', date.today())
        
        with self.db_manager.readonly_session() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT fecha, COUNT(*) as cantidad
                    FROM asistencias
                    WHERE usuario_id = %s AND fecha BETWEEN %s AND %s
                    GROUP BY fecha
                    ORDER BY fecha DESC
                """, (usuario_id, fecha_inicio, fecha_fin))
                return cur.fetchall()
    

    def _search_usuarios(self):
        """Búsqueda de usuarios"""
        query = self.params.get('query', '')
        limit = self.params.get('limit', 50)
        
        with self.db_manager.readonly_session() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT id, nombre, dni, telefono, rol, activo
                    FROM usuarios
                    WHERE LOWER(nombre) LIKE LOWER(%s) 
                       OR dni LIKE %s 
                       OR telefono LIKE %s
                    ORDER BY activo DESC, nombre ASC
                    LIMIT %s
                """, (f'%{query}%', f'%{query}%', f'%{query}%', limit))
                return cur.fetchall()
    

    def _bulk_insert_usuarios(self):
        """Inserta múltiples usuarios"""
        total = len(self.data)
        inserted = 0
        
        with self.db_manager.connection() as conn:
            with conn.cursor() as cur:
                for i, usuario in enumerate(self.data):
                    if not self._is_running:
                        break
                    
                    try:
                        cur.execute("""
                            INSERT INTO usuarios (nombre, dni, telefono, rol, activo, tipo_cuota)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """, (
                            usuario.get('nombre'),
                            usuario.get('dni'),
                            usuario.get('telefono'),
                            usuario.get('rol', 'socio'),
                            usuario.get('activo', True),
                            usuario.get('tipo_cuota', 'estandar')
                        ))
                        inserted += 1
                        
                        # Actualizar progreso cada 10 registros
                        if i % 10 == 0:
                            progress = int((i / total) * 100)
                            self.progress.emit(progress)
                            
                    except Exception as e:
                        logging.warning(f"Error insertando usuario {usuario.get('nombre')}: {e}")
                        continue
                
                conn.commit()
        
        self.progress.emit(100)
        return {'inserted': inserted, 'total': total}
    
