import sqlite3
import json
import threading
import time
import logging
import socket
import os
from http.server import BaseHTTPRequestHandler, HTTPServer
from dataclasses import is_dataclass, asdict, fields
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple, Callable
try:
    from models import (
        Pago, Asistencia, Ejercicio, Rutina, RutinaEjercicio, Clase, ClaseHorario,
        ClaseUsuario, EjercicioGrupo, EjercicioGrupoItem, TipoCuota, UsuarioNota,
        Etiqueta, UsuarioEtiqueta, MetodoPago, ConceptoPago, PagoDetalle, Especialidad,
        ProfesorEspecialidad
    )
    from models import Usuario  # type: ignore
except Exception:
    Usuario = None  # type: ignore


class OfflineSyncManager:
    """Cola persistente para operaciones offline (DB y WhatsApp).

    Guarda operaciones en SQLite y las procesa cuando la conectividad vuelve.
    Cambios mínimos: no requiere modificar firmas públicas de gestores.
    """

    def __init__(self, db_path: str = "offline_queue.sqlite"):
        self.db_path = db_path
        self._ensure_schema()
        self._stop_event = threading.Event()
        self._worker_thread: Optional[threading.Thread] = None
        self._processing_interval_sec = 30
        # Timeout corto para conexiones SQLite a fin de evitar bloqueos en UI
        self._sqlite_conn_timeout_sec: float = 0.5
        # Control simple de cuotas por categoría para evitar starvation
        self._db_quota_ratio: float = 0.6  # ~60% DB, ~40% WhatsApp por ciclo
        # Habilitar ajuste dinámico del intervalo de procesamiento
        self._adaptive_worker_enabled: bool = True
        self._db_manager = None
        self._whatsapp_manager = None
        self._metrics_thread: Optional[threading.Thread] = None
        self._metrics_interval_sec = 60
        self._metrics_http_thread: Optional[threading.Thread] = None
        self._metrics_http_server: Optional[HTTPServer] = None
        self._metrics_http_port: int = 8765
        # Servidor HTTP de métricas deshabilitado por defecto en desktop
        self._metrics_http_enabled: bool = (
            os.getenv("ENABLE_OFFLINE_METRICS_HTTP", "0").strip().lower() in ("1", "true", "yes")
        )
        # TTL para operaciones de WhatsApp (horas) y consolidación de duplicados
        self._whatsapp_ttl_hours: int = 72
        self._whatsapp_dedup_enabled: bool = True
        # Contador de uso de lecturas para promoción automática a críticos
        self._read_usage_counter: Dict[str, int] = {}
        # Mapa de tipos de dataclass disponibles por nombre
        self._dataclass_type_map: Dict[str, Any] = {}
        self._build_dataclass_type_map()
        # Registro opcional de reconstrucción por método (override de metadatos)
        self._reconstruction_registry: Dict[str, Dict[str, Any]] = {}
        # Lista de métodos críticos a cachear siempre (lecturas clave para continuidad)
        self.CRITICAL_READ_METHODS = {
            # Usuarios
            'obtener_usuario', 'obtener_usuario_por_id', 'obtener_todos_usuarios',
            'buscar_usuarios_por_nombre', 'obtener_estados_usuario',
            # Clases y horarios
            'obtener_todas_clases', 'obtener_clase_por_id', 'obtener_profesores_asignados_a_clase',
            'obtener_estudiantes_clase', 'obtener_lista_espera', 'obtener_lista_espera_completa',
            'obtener_horarios_de_clase', 'obtener_horarios_profesor', 'obtener_horarios_profesor_dia',
            # Rutinas y ejercicios
            'obtener_todas_rutinas', 'obtener_rutina_completa', 'obtener_rutinas_por_usuario',
            'obtener_todos_ejercicios', 'obtener_ejercicios_de_clase',
            # Pagos
            'obtener_pago', 'obtener_pagos_mes', 'obtener_todos_pagos',
            # Reportes y dashboards
            'obtener_asistencias_por_dia_semana', 'obtener_actividad_reciente', 'obtener_dashboard_resumen',
            'obtener_estadisticas_automatizacion', 'obtener_certificaciones_vencidas', 'obtener_certificaciones_por_vencer',
            # WhatsApp/config
            'obtener_configuracion_whatsapp', 'get_whatsapp_config',
            # Profesores
            'obtener_todos_profesores', 'obtener_profesores',
        }
        # Métricas de caché para monitoreo liviano
        self._cache_metrics = {
            'hits': 0,
            'misses': 0,
            'stores': 0,
        }
        # Registrar reconstrucciones por defecto para métodos comunes
        self._configure_default_reconstruction()

    def attach_managers(self, db_manager, whatsapp_manager=None):
        """Registra managers para ejecutar operaciones cuando haya conexión."""
        self._db_manager = db_manager
        self._whatsapp_manager = whatsapp_manager
        # Cargar preferencias de cola desde configuración del sistema si está disponible
        try:
            self._load_queue_preferences(db_manager)
        except Exception as e:
            logging.debug(f"No se pudieron cargar preferencias de cola al adjuntar managers: {e}")

    def start_background_worker(self):
        if self._worker_thread and self._worker_thread.is_alive():
            return
        self._worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        self._worker_thread.start()
        # Arrancar logger periódico de métricas
        self.start_metrics_logger(self._metrics_interval_sec)
        # Arrancar servidor HTTP interno de métricas (opcional)
        try:
            if self._metrics_http_enabled:
                self.start_metrics_http_server(self._metrics_http_port)
            else:
                logging.info("Servidor HTTP de métricas deshabilitado en desktop")
        except Exception:
            pass

    def stop_background_worker(self):
        self._stop_event.set()
        if self._worker_thread:
            self._worker_thread.join(timeout=2)
        self.stop_metrics_logger()
        self.stop_metrics_http_server()

    def _ensure_schema(self):
        conn = sqlite3.connect(self.db_path)
        try:
            # Optimizar SQLite para uso concurrente y minimizar bloqueos
            try:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=NORMAL")
                conn.execute("PRAGMA temp_store=MEMORY")
                conn.execute("PRAGMA busy_timeout=250")
            except Exception:
                pass
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS offline_ops (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    category TEXT NOT NULL, -- 'db' | 'whatsapp'
                    func_name TEXT NOT NULL,
                    args_json TEXT,
                    kwargs_json TEXT,
                    created_at TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending', -- 'pending'|'processing'|'done'|'failed'
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS offline_cache (
                    cache_key TEXT PRIMARY KEY,
                    value_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            # Evolución del esquema: columnas para TTL, deduplicación y tiempos
            try:
                cur = conn.cursor()
                cur.execute("PRAGMA table_info(offline_ops)")
                cols = {row[1] for row in cur.fetchall()}
                if 'expires_at' not in cols:
                    cur.execute("ALTER TABLE offline_ops ADD COLUMN expires_at TEXT")
                if 'dedup_key' not in cols:
                    cur.execute("ALTER TABLE offline_ops ADD COLUMN dedup_key TEXT")
                if 'first_processing_at' not in cols:
                    cur.execute("ALTER TABLE offline_ops ADD COLUMN first_processing_at TEXT")
                if 'completed_at' not in cols:
                    cur.execute("ALTER TABLE offline_ops ADD COLUMN completed_at TEXT")
                # Programación de reintentos por operación (backoff)
                if 'next_attempt_at' not in cols:
                    cur.execute("ALTER TABLE offline_ops ADD COLUMN next_attempt_at TEXT")
                # Índices útiles
                try:
                    cur.execute("CREATE INDEX IF NOT EXISTS idx_offline_ops_status ON offline_ops(status)")
                except Exception:
                    pass
                try:
                    cur.execute("CREATE INDEX IF NOT EXISTS idx_offline_ops_expires ON offline_ops(expires_at)")
                except Exception:
                    pass
                try:
                    cur.execute("CREATE INDEX IF NOT EXISTS idx_offline_ops_next_attempt ON offline_ops(next_attempt_at)")
                except Exception:
                    pass
                # Índice único parcial para deduplicación de pendientes (SQLite soporta WHERE)
                try:
                    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_offline_ops_dedup_pending ON offline_ops(dedup_key) WHERE status='pending' AND dedup_key IS NOT NULL")
                except Exception:
                    pass
            except Exception:
                # Si no podemos evolucionar, continuar con el esquema básico
                pass
            conn.commit()
        finally:
            conn.close()

    def _build_dataclass_type_map(self):
        """Construye un mapa de nombre -> clase para dataclasses conocidas."""
        type_map: Dict[str, Any] = {}
        for cls in [
            Pago, Asistencia, Ejercicio, Rutina, RutinaEjercicio, Clase, ClaseHorario,
            ClaseUsuario, EjercicioGrupo, EjercicioGrupoItem, TipoCuota, UsuarioNota,
            Etiqueta, UsuarioEtiqueta, MetodoPago, ConceptoPago, PagoDetalle, Especialidad,
            ProfesorEspecialidad, Usuario
        ]:
            try:
                if cls is not None and hasattr(cls, '__name__'):
                    type_map[cls.__name__] = cls
            except Exception:
                continue
        self._dataclass_type_map = type_map

    def enqueue_db_operation(self, func_name: str, args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> int:
        return self._enqueue(
            category="db",
            func_name=func_name,
            args=args,
            kwargs=kwargs,
        )

    def enqueue_whatsapp_operation(self, func_name: str, payload_kwargs: Dict[str, Any]) -> int:
        return self._enqueue(
            category="whatsapp",
            func_name=func_name,
            args=(),
            kwargs=payload_kwargs,
        )

    def _enqueue(self, category: str, func_name: str, args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> int:
        created_at = datetime.now(timezone.utc)
        args_json = json.dumps(list(args)) if args else "[]"
        kwargs = kwargs or {}
        kwargs_json = json.dumps(kwargs)
        dedup_key: Optional[str] = None
        expires_at: Optional[str] = None

        # Deduplicación y TTL para WhatsApp
        if category == "whatsapp":
            try:
                user_ident = (
                    kwargs.get('usuario_id') or kwargs.get('user_id') or
                    (kwargs.get('usuario', {}) or {}).get('id') or kwargs.get('to') or kwargs.get('to_phone')
                )
                template_ident = kwargs.get('template') or kwargs.get('tipo') or kwargs.get('type') or func_name
                dedup_key = json.dumps({'cat': 'whatsapp', 'func': func_name, 'user': user_ident, 'template': template_ident}, sort_keys=True)
            except Exception:
                dedup_key = None
            try:
                expires_at_dt = created_at + timedelta(hours=int(self._whatsapp_ttl_hours))
                expires_at = expires_at_dt.isoformat()
            except Exception:
                expires_at = None

        conn = sqlite3.connect(self.db_path, timeout=self._sqlite_conn_timeout_sec)
        try:
            cur = conn.cursor()
            # Consolidación de duplicados: si ya existe pendiente con la misma dedup_key, reutilizar
            if dedup_key and self._whatsapp_dedup_enabled:
                try:
                    cur.execute(
                        "SELECT id FROM offline_ops WHERE status='pending' AND dedup_key = ? ORDER BY id ASC LIMIT 1",
                        (dedup_key,)
                    )
                    row = cur.fetchone()
                    if row and row[0]:
                        return int(row[0])
                except Exception:
                    pass
            cur.execute(
                """
                INSERT INTO offline_ops (category, func_name, args_json, kwargs_json, created_at, dedup_key, expires_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (category, func_name, args_json, kwargs_json, created_at.isoformat(), dedup_key, expires_at),
            )
            conn.commit()
            return cur.lastrowid
        finally:
            conn.close()

    def _load_queue_preferences(self, db_manager=None):
        """Carga preferencias de TTL y deduplicación desde configuración sistema."""
        try:
            getter = None
            if db_manager and hasattr(db_manager, 'obtener_configuracion'):
                getter = db_manager.obtener_configuracion
            elif self._db_manager and hasattr(self._db_manager, 'obtener_configuracion'):
                getter = self._db_manager.obtener_configuracion
            if getter:
                ttl_str = getter('whatsapp_queue_ttl_hours') or '72'
                dedup_str = getter('whatsapp_queue_dedup_enabled') or 'true'
                try:
                    self._whatsapp_ttl_hours = max(1, min(168, int(ttl_str)))
                except Exception:
                    self._whatsapp_ttl_hours = 72
                try:
                    self._whatsapp_dedup_enabled = str(dedup_str).lower() == 'true'
                except Exception:
                    self._whatsapp_dedup_enabled = True
        except Exception as e:
            logging.debug(f"Fallo cargando preferencias de cola: {e}")

    def reload_preferences(self, db_manager=None):
        """Recarga preferencias de TTL/deduplicación en caliente."""
        try:
            self._load_queue_preferences(db_manager)
        except Exception as e:
            logging.debug(f"No se pudieron recargar preferencias: {e}")

    def process_pending(self, max_ops: int = 50) -> int:
        """Procesa operaciones pendientes. Retorna cantidad procesada con éxito."""
        conn = sqlite3.connect(self.db_path, timeout=self._sqlite_conn_timeout_sec)
        processed_ok = 0
        try:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            # Recuperar operaciones marcadas como 'processing' en caso de caída previa
            try:
                cur.execute("UPDATE offline_ops SET status = 'pending' WHERE status = 'processing'")
                conn.commit()
            except Exception:
                pass

            # Eliminar pendientes expiradas por TTL
            try:
                now_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
                cur.execute("DELETE FROM offline_ops WHERE status='pending' AND expires_at IS NOT NULL AND expires_at < ?", (now_iso,))
                conn.commit()
            except Exception:
                pass

            cur.execute(
                "SELECT * FROM offline_ops WHERE status = 'pending' AND (next_attempt_at IS NULL OR next_attempt_at <= ?) ORDER BY id ASC LIMIT ?",
                (datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"), max_ops),
            )
            rows = cur.fetchall()
            # Evaluar conectividad para determinar elegibilidad de WhatsApp
            internet_ok = False
            try:
                internet_ok = self._check_internet()
            except Exception:
                internet_ok = False

            # Cuotas por categoría para evitar starvation
            db_quota = max(1, int(max_ops * self._db_quota_ratio))
            wa_quota = max(1, max_ops - db_quota)
            processed_db = 0
            processed_wa = 0

            for row in rows:
                op_id = row["id"]
                category = row["category"]
                func_name = row["func_name"]
                args = tuple(json.loads(row["args_json"])) if row["args_json"] else ()
                kwargs = json.loads(row["kwargs_json"]) if row["kwargs_json"] else {}

                try:
                    # Determinar elegibilidad antes de marcar 'processing'
                    eligible = False
                    if category == "db":
                        eligible = self._db_manager is not None
                        # Respetar cuota
                        if processed_db >= db_quota:
                            eligible = False
                    elif category == "whatsapp":
                        eligible = (
                            self._whatsapp_manager is not None and
                            getattr(self._whatsapp_manager, 'wa_client', None) is not None and
                            internet_ok
                        )
                        # Respetar cuota
                        if processed_wa >= wa_quota:
                            eligible = False
                    else:
                        eligible = False

                    if not eligible:
                        # Saltar operación no elegible por ahora (no incrementar attempts)
                        continue

                    # Marcar en procesamiento solo si es elegible
                    cur.execute(
                        "UPDATE offline_ops SET status = 'processing', attempts = attempts + 1, first_processing_at = COALESCE(first_processing_at, ?) WHERE id = ?",
                        (datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"), op_id),
                    )
                    conn.commit()

                    if category == "db" and self._db_manager is not None:
                        # Evitar re-encolado en el decorador
                        setattr(self._db_manager, "_executing_offline_op", True)
                        try:
                            target = getattr(self._db_manager, func_name, None)
                            if callable(target):
                                target(*args, **kwargs)
                            else:
                                raise RuntimeError(f"DB target {func_name} no disponible")
                        finally:
                            setattr(self._db_manager, "_executing_offline_op", False)

                    elif category == "whatsapp" and self._whatsapp_manager is not None:
                        target = getattr(self._whatsapp_manager, func_name, None)
                        if callable(target):
                            target(**kwargs)
                        else:
                            raise RuntimeError(f"WhatsApp target {func_name} no disponible")
                    else:
                        raise RuntimeError("Manager no adjuntado para categoría")

                    # Marcar como hecho
                    cur.execute(
                        "UPDATE offline_ops SET status = 'done', last_error = NULL, completed_at = ?, next_attempt_at = NULL WHERE id = ?",
                        (datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"), op_id),
                    )
                    conn.commit()
                    processed_ok += 1
                    if category == "db":
                        processed_db += 1
                    elif category == "whatsapp":
                        processed_wa += 1

                except Exception as exec_err:
                    logging.warning(f"Fallo procesando op {op_id} {category}:{func_name}: {exec_err}")
                    # Programar próximo intento con backoff exponencial y jitter
                    try:
                        cur.execute("SELECT attempts FROM offline_ops WHERE id = ?", (op_id,))
                        row_attempts = cur.fetchone()
                        attempts_val = int(row_attempts[0]) if row_attempts and row_attempts[0] is not None else 1
                    except Exception:
                        attempts_val = 1
                    try:
                        base = 15.0
                        delay = base * (2 ** max(0, attempts_val - 1))
                        delay = min(delay, 15 * 60)
                        jitter = delay * 0.1
                        delay = delay + (jitter * 0.5)
                        next_at = datetime.now(timezone.utc) + timedelta(seconds=delay)
                        next_at_iso = next_at.isoformat().replace("+00:00", "Z")
                    except Exception:
                        next_at_iso = None
                    cur.execute(
                        "UPDATE offline_ops SET status = 'pending', last_error = ?, next_attempt_at = ? WHERE id = ?",
                        (str(exec_err), next_at_iso, op_id),
                    )
                    conn.commit()

        finally:
            conn.close()
        return processed_ok

    def _worker_loop(self):
        while not self._stop_event.is_set():
            try:
                # Determinar intervalo dinámico según cantidad de accionables
                actionable = 0
                try:
                    actionable = self._count_pending_ops(only_actionable=True)
                except Exception:
                    actionable = 0
                # Procesar con tamaño de lote razonable
                processed = self.process_pending(max_ops=20)
                if processed:
                    logging.info(f"OfflineSyncManager: procesadas {processed} operaciones pendientes")
            except Exception as e:
                logging.debug(f"OfflineSyncManager worker error: {e}")
            # Ajuste del sueño según carga y configuración
            try:
                if self._adaptive_worker_enabled:
                    if actionable >= 20:
                        sleep_sec = 2
                    elif actionable > 0:
                        sleep_sec = 5
                    else:
                        sleep_sec = self._processing_interval_sec
                else:
                    sleep_sec = self._processing_interval_sec
            except Exception:
                sleep_sec = self._processing_interval_sec
            time.sleep(sleep_sec)

    # ===== MÉTRICAS Y OBSERVABILIDAD =====
    def start_metrics_logger(self, interval_sec: int = 60):
        self._metrics_interval_sec = max(5, int(interval_sec))
        if self._metrics_thread and self._metrics_thread.is_alive():
            return
        self._metrics_thread = threading.Thread(target=self._metrics_loop, daemon=True)
        self._metrics_thread.start()

    def stop_metrics_logger(self):
        if self._metrics_thread:
            # Señal compartida con _stop_event; el loop chequea esta bandera
            self._metrics_thread.join(timeout=2)
            self._metrics_thread = None

    def _metrics_loop(self):
        while not self._stop_event.is_set():
            try:
                snapshot = self.get_connectivity_snapshot()
                # Log legible
                obs = snapshot.get('offline_observability', {}) or {}
                logging.info(
                    f"OfflineMetrics: internet={snapshot.get('internet_ok')} db={snapshot.get('db_ok')} whatsapp={snapshot.get('whatsapp_ok')} "
                    f"pending_ops={snapshot.get('pending_ops')} attempts={obs.get('attempts_by_category')} drain_avg={obs.get('avg_drain_sec_by_category')} cache={snapshot.get('cache_metrics')}"
                )
                # Snapshot JSON en archivo
                try:
                    os.makedirs('logs', exist_ok=True)
                    with open(os.path.join('logs', 'cache_metrics.json'), 'w', encoding='utf-8') as f:
                        json.dump({
                            'timestamp': datetime.utcnow().isoformat(),
                            **snapshot
                        }, f, ensure_ascii=False, indent=2)
                except Exception:
                    pass
                # Promoción automática de métodos críticos basada en uso
                self._auto_refresh_critical_methods()
            except Exception:
                pass
            time.sleep(self._metrics_interval_sec)

    def _compute_offline_observability(self) -> Dict[str, Any]:
        """Calcula métricas de intentos por categoría y tiempo promedio de drenaje."""
        metrics = {
            'attempts_by_category': {},
            'avg_drain_sec_by_category': {},
        }
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            # Intentos por categoría (en operaciones completadas)
            try:
                cur.execute("SELECT category, SUM(attempts) as total_attempts FROM offline_ops WHERE status='done' GROUP BY category")
                for r in cur.fetchall() or []:
                    metrics['attempts_by_category'][r['category']] = int(r['total_attempts'] or 0)
            except Exception:
                pass
            # Tiempo promedio de drenaje (completed_at - created_at) en segundos por categoría
            try:
                cur.execute("SELECT category, created_at, completed_at FROM offline_ops WHERE status='done' AND completed_at IS NOT NULL")
                durations: Dict[str, List[float]] = {}
                for r in cur.fetchall() or []:
                    try:
                        ca = datetime.fromisoformat(r['created_at'])
                        co = datetime.fromisoformat(r['completed_at'])
                        dur = max(0.0, (co - ca).total_seconds())
                        durations.setdefault(r['category'], []).append(dur)
                    except Exception:
                        continue
                for cat, arr in durations.items():
                    if arr:
                        metrics['avg_drain_sec_by_category'][cat] = round(sum(arr) / len(arr), 2)
            except Exception:
                pass
        except Exception:
            pass
        finally:
            try:
                conn.close()
            except Exception:
                pass
        return metrics

    # ===== SERVIDOR HTTP DE MÉTRICAS =====
    def start_metrics_http_server(self, port: int = 8765):
        # No iniciar el servidor si está deshabilitado
        if not getattr(self, "_metrics_http_enabled", False):
            logging.debug("Metrics HTTP server disabled; skipping start")
            return
        self._metrics_http_port = int(port)
        if self._metrics_http_thread and self._metrics_http_thread.is_alive():
            return

        manager = self

        class MetricsHandler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args):
                try:
                    logging.debug("MetricsHTTP: " + format % args)
                except Exception:
                    pass

            def _send_json(self, obj: Any, status: int = 200):
                payload = json.dumps(obj, ensure_ascii=False).encode('utf-8')
                self.send_response(status)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Content-Length', str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def do_GET(self):
                try:
                    if self.path in ('/metrics', '/metrics/'):
                        snap = manager.get_connectivity_snapshot()
                        snap['source'] = 'live'
                        return self._send_json(snap, 200)
                    if self.path in ('/metrics/file', '/metrics/file/'):
                        try:
                            file_path = os.path.join('logs', 'cache_metrics.json')
                            if os.path.exists(file_path):
                                with open(file_path, 'r', encoding='utf-8') as f:
                                    data = json.load(f)
                                data['source'] = 'file'
                                return self._send_json(data, 200)
                            return self._send_json({'error': 'metrics file not found'}, 404)
                        except Exception as e:
                            return self._send_json({'error': str(e)}, 500)
                    # 404 para rutas no reconocidas
                    return self._send_json({'error': 'not found'}, 404)
                except Exception as e:
                    return self._send_json({'error': str(e)}, 500)

        # Crear servidor
        try:
            server = HTTPServer(('127.0.0.1', self._metrics_http_port), MetricsHandler)
            self._metrics_http_server = server
            t = threading.Thread(target=server.serve_forever, daemon=True)
            self._metrics_http_thread = t
            t.start()
            logging.info(f"Metrics HTTP server listening on http://127.0.0.1:{self._metrics_http_port}/metrics")
        except Exception as e:
            logging.warning(f"No se pudo iniciar el servidor HTTP de métricas: {e}")

    def stop_metrics_http_server(self):
        try:
            if self._metrics_http_server:
                self._metrics_http_server.shutdown()
                self._metrics_http_server.server_close()
            if self._metrics_http_thread:
                self._metrics_http_thread.join(timeout=2)
        except Exception:
            pass
        finally:
            self._metrics_http_server = None
            self._metrics_http_thread = None

    # === Conectividad y estado para UI ===
    def _check_internet(self, timeout: float = 2.0) -> bool:
        """Chequeo simple de internet intentando conectar al DNS público de Google."""
        try:
            sock = socket.create_connection(("8.8.8.8", 53), timeout=timeout)
            sock.close()
            return True
        except Exception:
            return False

    def _count_pending_ops(self, only_actionable: bool = False) -> int:
        """Cuenta operaciones pendientes.

        Si only_actionable=True, solo cuenta operaciones que pueden procesarse ahora
        según disponibilidad de managers y conectividad (WhatsApp requiere cliente y red).
        """
        try:
            conn = sqlite3.connect(self.db_path, timeout=self._sqlite_conn_timeout_sec)
            conn.row_factory = sqlite3.Row
            try:
                cur = conn.cursor()
                # Traer pendientes; si solo accionables, respetar next_attempt_at (backoff)
                if only_actionable:
                    now_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
                    cur.execute(
                        "SELECT id, category, next_attempt_at FROM offline_ops WHERE status = 'pending' AND (next_attempt_at IS NULL OR next_attempt_at <= ?)",
                        (now_iso,),
                    )
                else:
                    cur.execute("SELECT id, category, next_attempt_at FROM offline_ops WHERE status = 'pending'")
                rows = cur.fetchall() or []
                if not only_actionable:
                    return len(rows)

                # Evaluar elegibilidad por categoría
                internet_ok = False
                try:
                    internet_ok = self._check_internet()
                except Exception:
                    internet_ok = False

                def eligible(row):
                    cat = row["category"]
                    if cat == "db":
                        return self._db_manager is not None
                    if cat == "whatsapp":
                        if self._whatsapp_manager is None:
                            return False
                        wa_ready = getattr(self._whatsapp_manager, 'wa_client', None) is not None
                        return wa_ready and internet_ok
                    return False

                return sum(1 for r in rows if eligible(r))
            finally:
                conn.close()
        except Exception:
            return 0

    def _get_pending_breakdown(self) -> dict:
        """Devuelve desglose de pendientes por categoría y elegibilidad.

        Incluye 'scheduled' (programados por backoff: next_attempt_at en el futuro).
        """
        summary = {
            'total': 0,
            'db': 0,
            'whatsapp': 0,
            'actionable': 0,
            'actionable_db': 0,
            'actionable_whatsapp': 0,
            'scheduled': 0,
            'scheduled_db': 0,
            'scheduled_whatsapp': 0,
        }
        try:
            conn = sqlite3.connect(self.db_path, timeout=self._sqlite_conn_timeout_sec)
            conn.row_factory = sqlite3.Row
            try:
                cur = conn.cursor()
                # Traer categoría y programación por backoff
                cur.execute("SELECT category, next_attempt_at FROM offline_ops WHERE status = 'pending'")
                rows = cur.fetchall() or []
                summary['total'] = len(rows)

                now_iso = datetime.utcnow().isoformat()

                # Contar totales y programados por backoff
                for r in rows:
                    cat = r['category']
                    next_at = r['next_attempt_at']
                    if cat == 'db':
                        summary['db'] += 1
                        if next_at is not None and next_at > now_iso:
                            summary['scheduled_db'] += 1
                            summary['scheduled'] += 1
                    elif cat == 'whatsapp':
                        summary['whatsapp'] += 1
                        if next_at is not None and next_at > now_iso:
                            summary['scheduled_whatsapp'] += 1
                            summary['scheduled'] += 1

                internet_ok = False
                try:
                    internet_ok = self._check_internet()
                except Exception:
                    internet_ok = False

                # Elegibilidad por categoría considerando backoff
                actionable_db = self._db_manager is not None
                actionable_whatsapp = (
                    self._whatsapp_manager is not None and
                    getattr(self._whatsapp_manager, 'wa_client', None) is not None and
                    internet_ok
                )
                if actionable_db:
                    summary['actionable_db'] = sum(
                        1 for r in rows
                        if r['category'] == 'db' and (r['next_attempt_at'] is None or r['next_attempt_at'] <= now_iso)
                    )
                else:
                    summary['actionable_db'] = 0

                if actionable_whatsapp:
                    summary['actionable_whatsapp'] = sum(
                        1 for r in rows
                        if r['category'] == 'whatsapp' and (r['next_attempt_at'] is None or r['next_attempt_at'] <= now_iso)
                    )
                else:
                    summary['actionable_whatsapp'] = 0

                summary['actionable'] = summary['actionable_db'] + summary['actionable_whatsapp']
            finally:
                conn.close()
        except Exception:
            pass
        return summary

    def get_connectivity_snapshot(self) -> dict:
        """Devuelve snapshot de conectividad para UI: internet, DB, WhatsApp y cola."""
        internet_ok = self._check_internet()

        db_ok = False
        if self._db_manager is not None:
            try:
                # Usa el método ligero del gestor para verificar conectividad
                with self._db_manager.get_connection_context() as conn:
                    conn.cursor().execute("SELECT 1")
                db_ok = True
            except Exception:
                db_ok = False

        whatsapp_ok = False
        if self._whatsapp_manager is not None:
            try:
                whatsapp_ok = getattr(self._whatsapp_manager, 'wa_client', None) is not None
            except Exception:
                whatsapp_ok = False

        # Pendientes: usar solo las accionables para no contar operaciones que no pueden procesarse aún
        pending_breakdown = self._get_pending_breakdown()
        pending_ops = pending_breakdown.get('actionable', 0)

        return {
            'internet_ok': internet_ok,
            'db_ok': db_ok,
            'whatsapp_ok': whatsapp_ok,
            'pending_ops': pending_ops,
            'pending_ops_total': pending_breakdown.get('total', 0),
            'pending_breakdown': pending_breakdown,
            'cache_metrics': self._cache_metrics.copy(),
            'offline_observability': self._compute_offline_observability(),
            'timestamp': datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            'critical_read_methods': sorted(list(self.CRITICAL_READ_METHODS)),
        }

    # === Caché persistente de lecturas ===
    def _to_json_safe(self, value):
        """Convierte estructuras y modelos complejos a formatos serializables JSON."""
        # Dataclasses
        if is_dataclass(value):
            return asdict(value)
        # Tipos básicos
        if isinstance(value, (str, int, float, bool)) or value is None:
            return value
        # datetime
        if isinstance(value, datetime):
            return value.isoformat()
        # dict
        if isinstance(value, dict):
            return {k: self._to_json_safe(v) for k, v in value.items()}
        # list/tuple
        if isinstance(value, (list, tuple)):
            return [self._to_json_safe(v) for v in value]
        # set -> list
        if isinstance(value, set):
            return [self._to_json_safe(v) for v in value]
        # Objetos con to_dict
        if hasattr(value, 'to_dict') and callable(getattr(value, 'to_dict')):
            try:
                return self._to_json_safe(value.to_dict())
            except Exception:
                pass
        # Fallback: representación string
        try:
            return str(value)
        except Exception:
            return None

    def _detect_dataclass_meta(self, result: Any) -> Optional[Dict[str, Any]]:
        """Detecta metadatos de tipo para reconstrucción (dataclass o lista de dataclasses)."""
        try:
            if is_dataclass(result):
                return {'kind': 'dataclass', 'cls_name': type(result).__name__}
            if isinstance(result, list) and result and all(is_dataclass(x) for x in result):
                return {'kind': 'list_dataclass', 'cls_name': type(result[0]).__name__}
        except Exception:
            return None
        return None

    def _filter_dataclass_kwargs(self, cls: Any, data: Dict[str, Any]) -> Dict[str, Any]:
        try:
            field_names = {f.name for f in fields(cls)}
            return {k: data.get(k) for k in data.keys() if k in field_names}
        except Exception:
            return data

    def _reconstruct_from_meta(self, func_name: str, meta: Optional[Dict[str, Any]], value: Any) -> Any:
        """Reconstruye objetos desde metadatos y valor JSON, con posibilidad de override por método."""
        # Override explícito
        spec = self._reconstruction_registry.get(func_name)
        if spec:
            try:
                kind = spec.get('kind')
                cls_name = spec.get('cls_name')
                cls = self._dataclass_type_map.get(cls_name) if cls_name else None
                if kind == 'dataclass' and isinstance(value, dict) and cls:
                    return cls(**self._filter_dataclass_kwargs(cls, value))
                if kind == 'list_dataclass' and isinstance(value, list) and cls:
                    return [cls(**self._filter_dataclass_kwargs(cls, v)) for v in value]
                builder = spec.get('builder')
                if callable(builder):
                    return builder(value)
            except Exception:
                return value

        # Metadatos detectados
        if meta and isinstance(meta, dict):
            cls_name = meta.get('cls_name')
            kind = meta.get('kind')
            cls = self._dataclass_type_map.get(cls_name)
            try:
                if kind == 'dataclass' and isinstance(value, dict) and cls:
                    return cls(**self._filter_dataclass_kwargs(cls, value))
                if kind == 'list_dataclass' and isinstance(value, list) and cls:
                    return [cls(**self._filter_dataclass_kwargs(cls, v)) for v in value]
            except Exception:
                return value
        return value

    def _build_cache_key(self, func_name: str, args: tuple, kwargs: dict) -> Optional[str]:
        try:
            key = {
                'func': func_name,
                'args': list(args) if args else [],
                'kwargs': kwargs or {},
            }
            return json.dumps(key, sort_keys=True)
        except Exception:
            return None

    def cache_read_result(self, func_name: str, args: tuple, kwargs: dict, result: Any) -> None:
        """Guarda resultado de lectura, aplicando serializadores robustos y metadatos para reconstrucción."""
        cache_key = self._build_cache_key(func_name, args, kwargs)
        if not cache_key:
            return
        json_safe_value = self._to_json_safe(result)
        meta = self._detect_dataclass_meta(result)
        payload = {'__meta__': meta, 'value': json_safe_value}
        try:
            conn = sqlite3.connect(self.db_path)
            try:
                cur = conn.cursor()
                cur.execute(
                    "INSERT OR REPLACE INTO offline_cache (cache_key, value_json, updated_at) VALUES (?, ?, ?)",
                    (cache_key, json.dumps(payload), datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")),
                )
                conn.commit()
                self._cache_metrics['stores'] += 1
                # Contabilizar uso de lectura para promoción automática
                self._read_usage_counter[func_name] = self._read_usage_counter.get(func_name, 0) + 1
            finally:
                conn.close()
        except Exception:
            pass

    def get_cached_read_result(self, func_name: str, args: tuple, kwargs: dict) -> Optional[Any]:
        """Obtiene resultado en caché si existe."""
        cache_key = self._build_cache_key(func_name, args, kwargs)
        if not cache_key:
            return None
        try:
            conn = sqlite3.connect(self.db_path)
            try:
                cur = conn.cursor()
                cur.execute("SELECT value_json FROM offline_cache WHERE cache_key = ?", (cache_key,))
                row = cur.fetchone()
                if row and row[0]:
                    try:
                        loaded = json.loads(row[0])
                        # Soporte retrocompatible: valor puro o envoltura con metadatos
                        if isinstance(loaded, dict) and 'value' in loaded:
                            value = self._reconstruct_from_meta(func_name, loaded.get('__meta__'), loaded.get('value'))
                        else:
                            value = loaded
                        self._cache_metrics['hits'] += 1
                        # Contabilizar uso de lectura para promoción automática
                        self._read_usage_counter[func_name] = self._read_usage_counter.get(func_name, 0) + 1
                        return value
                    except Exception:
                        self._cache_metrics['misses'] += 1
                        return None
                self._cache_metrics['misses'] += 1
                return None
            finally:
                conn.close()
        except Exception:
            return None

    def is_critical_read(self, func_name: str) -> bool:
        return func_name in self.CRITICAL_READ_METHODS

    def get_cache_metrics(self) -> dict:
        return self._cache_metrics.copy()

    def register_reconstruction(self, func_name: str, kind: str, cls_name: Optional[str] = None, builder: Optional[Callable] = None) -> None:
        """Permite registrar reconstrucción específica para un método de lectura.

        kind: 'dataclass' | 'list_dataclass' | 'custom'
        cls_name: nombre de la clase dataclass para reconstrucción automática
        builder: callable(value_json) -> objeto reconstruido (para 'custom')
        """
        self._reconstruction_registry[func_name] = {
            'kind': kind,
            'cls_name': cls_name,
            'builder': builder,
        }

    def _configure_default_reconstruction(self) -> None:
        """Registra reconstrucciones por defecto para métodos comunes con objetos complejos."""
        try:
            # Pagos
            self.register_reconstruction('obtener_pago', 'dataclass', 'Pago')
            self.register_reconstruction('obtener_todos_pagos', 'list_dataclass', 'Pago')
            self.register_reconstruction('obtener_pagos_mes', 'list_dataclass', 'Pago')
            # Rutinas
            # Las rutinas en DB retornan dicts compuestos; no reconstruimos a dataclass por defecto
            # Clases
            self.register_reconstruction('obtener_horarios_de_clase', 'list_dataclass', 'ClaseHorario')
            # Nota: obtener_todas_clases y obtener_clase_por_id retornan dicts enriquecidos
            # Usuarios (si existe dataclass Usuario)
            if self._dataclass_type_map.get('Usuario'):
                self.register_reconstruction('obtener_usuario', 'dataclass', 'Usuario')
                self.register_reconstruction('obtener_usuario_por_id', 'dataclass', 'Usuario')
                self.register_reconstruction('obtener_todos_usuarios', 'list_dataclass', 'Usuario')
        except Exception:
            pass

    def _auto_refresh_critical_methods(self, threshold: int = 10, max_promotions: int = 5) -> None:
        """Promueve métodos a críticos basándose en frecuencia de uso reciente."""
        try:
            # Ordenar por uso descendente
            sorted_usage = sorted(self._read_usage_counter.items(), key=lambda x: x[1], reverse=True)
            promotions = 0
            for func_name, count in sorted_usage:
                if count >= threshold and func_name not in self.CRITICAL_READ_METHODS:
                    self.CRITICAL_READ_METHODS.add(func_name)
                    promotions += 1
                    logging.info(f"Promovido a crítico por uso frecuente: {func_name} (conteo={count})")
                    if promotions >= max_promotions:
                        break
        except Exception:
            pass