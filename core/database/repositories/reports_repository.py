from datetime import datetime, date, timedelta
from typing import List, Optional, Dict, Set, Tuple, Any
import json
import logging
import psycopg2
import psycopg2.extras
from .base import BaseRepository
from ..connection import database_retry
from ...utils import get_gym_name

class ReportsRepository(BaseRepository):
    pass

    # --- Methods moved from DatabaseManager ---

    def _update_query_stats(self, query_time: float):
        """Actualiza estadísticas de rendimiento de consultas"""
        with self._query_stats_lock:
            self._query_stats['total_queries'] += 1
            if query_time > self._query_time_threshold:
                self._query_stats['slow_queries'] += 1
                self.logger.warning(f"Consulta lenta detectada: {query_time:.2f}s")
            # Promedio móvil
            self._query_stats['average_query_time'] = (
                (self._query_stats['average_query_time'] * (self._query_stats['total_queries'] - 1) + query_time)
                / self._query_stats['total_queries']
            )


    def get_query_performance_stats(self) -> Dict:
        """Obtiene estadísticas de rendimiento de consultas con caché ligera (TTL)."""
        now = time.time()
        # Fast-path cache válida
        with self._query_stats_lock:
            cached = self._perf_stats_cache
            if cached['value'] is not None and now < cached['expires_at']:
                return cached['value']

            # Tomar snapshot consistente y calcular métricas derivadas
            total = self._query_stats['total_queries']
            slow = self._query_stats['slow_queries']
            hits = self._query_stats['cache_hits']
            misses = self._query_stats['cache_misses']
            avg = self._query_stats['average_query_time']

            result = {
                'total_queries': total,
                'slow_queries': slow,
                'cache_hits': hits,
                'cache_misses': misses,
                'average_query_time': avg,
                'slow_query_percentage': ((slow / max(total, 1)) * 100),
                'cache_hit_ratio': ((hits / max(hits + misses, 1)) * 100),
            }

            # Actualizar caché
            self._perf_stats_cache = {
                'value': result,
                'expires_at': now + self._perf_stats_cache_ttl,
            }
            return result
        
        # Inicialización de locks y estadísticas de rendimiento
        self._cache_lock = threading.RLock()
        self._performance_stats = {
            'query_count': 0,
            'total_query_time': 0.0,
            'slow_queries': []
        }
        
        # Sistema de cola para operaciones masivas
        self.mass_operation_queue = MassOperationQueue(max_workers=2)
        
        # Sistema de contador local de sesiones (sin conexión continua a DB)
        self._sesiones_locales = {}  # {profesor_id: {'inicio': datetime, 'sesion_id': int, 'tipo_actividad': str}}
        self._sesiones_lock = threading.RLock()
        
        # Inicializar declaraciones preparadas
        try:
            self.initialize_prepared_statements()
        except Exception as e:
            self.logger.warning(f"No se pudieron inicializar declaraciones preparadas: {e}")
        
        # Inicializar índices optimizados
        try:
            self.create_optimized_indexes()
        except Exception as e:
            self.logger.warning(f"No se pudieron crear índices optimizados: {e}")
        
        # Inicialización pesada diferida y única (no bloquear UI múltiples veces)
        self._init_database_once_deferred()
        try:
            self.logger.debug("DatabaseManager: inicialización diferida solicitada")
        except Exception:
            pass

        if AUDIT_ENABLED:
            self.audit_logger = get_audit_logger(self)
            try:
                self.logger.info("DatabaseManager: auditoría habilitada")
            except Exception:
                pass
        


        self._start_cache_cleanup_thread()
        try:
            self.logger.debug("DatabaseManager: hilo de limpieza de caché iniciado")
        except Exception:
            pass

        # --- Circuit Breaker (resiliencia ante fallos de DB) ---
        self._cb_failure_count = 0
        self._cb_first_failure_ts = 0.0
        self._cb_is_open = False
        self._cb_open_until = 0.0
        self._cb_conf = {
            'failure_threshold': int(os.getenv('DB_CB_FAILURE_THRESHOLD', 3)),
            'window_seconds': int(os.getenv('DB_CB_WINDOW_SECONDS', 20)),
            'open_seconds': int(os.getenv('DB_CB_OPEN_SECONDS', 25)),
            'half_open_probe': True,
        }
        try:
            self.logger.info(f"DatabaseManager: configuración Circuit Breaker={self._cb_conf}")
        except Exception:
            pass

        # Caché liviano de credenciales del Dueño (TTL)
        try:
            self._owner_cache = {
                'password': None,
                'password_expiry': 0.0,
                'user': None,
                'user_expiry': 0.0,
            }
        except Exception:
            self._owner_cache = {}

    @staticmethod

    def obtener_kpis_generales(self) -> Dict:
        """Obtiene KPIs generales del sistema"""
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                # Total de usuarios activos
                cursor.execute("SELECT COUNT(*) FROM usuarios WHERE activo = TRUE AND rol IN ('socio','miembro','profesor')")
                total_activos = cursor.fetchone()[0] or 0
                
                # Nuevos usuarios en los últimos 30 días
                fecha_limite = datetime.now() - timedelta(days=30)
                cursor.execute("SELECT COUNT(*) FROM usuarios WHERE fecha_registro >= %s AND rol IN ('socio','miembro','profesor')", (fecha_limite,))
                nuevos_30_dias = cursor.fetchone()[0] or 0
                
                # Ingresos del mes actual
                cursor.execute(
                    """
                    SELECT COALESCE(SUM(monto), 0)
                    FROM pagos
                    WHERE date_trunc('month', COALESCE(fecha_pago, make_date(año, mes, 1))) = date_trunc('month', CURRENT_DATE)
                    """
                )
                ingresos_mes = cursor.fetchone()[0] or 0
                
                # Asistencias de hoy
                cursor.execute("SELECT COUNT(*) FROM asistencias WHERE fecha = CURRENT_DATE OR (hora_registro IS NOT NULL AND hora_registro::date = CURRENT_DATE)")
                asistencias_hoy = cursor.fetchone()[0] or 0
                
                return {
                    "total_activos": total_activos,
                    "nuevos_30_dias": nuevos_30_dias,
                    "ingresos_mes_actual": float(ingresos_mes),
                    "asistencias_hoy": asistencias_hoy
                }

    @database_retry

    def generar_reporte_automatico_periodo(self, tipo_reporte: str, fecha_inicio: date, fecha_fin: date) -> dict:
        """Genera reportes automáticos por período con una consulta optimizada."""
        try:
            with self.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    
                    if tipo_reporte == 'usuarios_nuevos':
                        table = 'usuarios'
                        date_column = 'fecha_registro'
                    elif tipo_reporte == 'ingresos':
                        table = 'pagos'
                        date_column = 'fecha_pago'
                    elif tipo_reporte == 'asistencias':
                        table = 'asistencias'
                        date_column = 'fecha'
                    else:
                        return {'error': 'Tipo de reporte no válido'}
                    
                    query = f"""
                        SELECT 
                            %s as tipo_reporte,
                            COUNT(*) as total,
                            CASE WHEN %s = 'ingresos' THEN SUM(monto) ELSE 0 END as ingresos_totales,
                            CASE WHEN %s = 'ingresos' THEN AVG(monto) ELSE NULL END as promedio_pago,
                            CASE WHEN %s = 'asistencias' THEN COUNT(DISTINCT usuario_id) ELSE NULL END as usuarios_unicos
                        FROM {table}
                        WHERE {date_column} BETWEEN %s AND %s
                    """
                    
                    params = (tipo_reporte, tipo_reporte, tipo_reporte, tipo_reporte, fecha_inicio, fecha_fin)
                    cursor.execute(query, params)
                    resultado = cursor.fetchone()
                    
                    return {
                        'tipo_reporte': tipo_reporte,
                        'periodo': {'inicio': fecha_inicio.isoformat(), 'fin': fecha_fin.isoformat()},
                        'datos': dict(zip([desc[0] for desc in cursor.description], resultado)) if resultado else {},
                        'generado_en': datetime.now().isoformat()
                    }
                    
        except Exception as e:
            logging.error(f"Error generando reporte automático: {e}")
            return {'error': str(e)}
    

    def generar_reporte_automatico_periodo(self, tipo_reporte: str, fecha_inicio: date, fecha_fin: date) -> dict:
        """Genera reportes automáticos por período con consulta optimizada PostgreSQL"""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                if tipo_reporte == 'usuarios_nuevos':
                    cursor.execute("""
                        SELECT 
                            %s as tipo_reporte,
                            COUNT(*) as total,
                            0 as ingresos_totales,
                            0 as promedio_pago,
                            COUNT(DISTINCT id) as usuarios_unicos
                        FROM usuarios
                        WHERE fecha_registro::date BETWEEN %s AND %s
                    """, (tipo_reporte, fecha_inicio, fecha_fin))
                    
                elif tipo_reporte == 'ingresos':
                    cursor.execute("""
                        SELECT 
                            %s as tipo_reporte,
                            COUNT(*) as total,
                            SUM(monto) as ingresos_totales,
                            AVG(monto) as promedio_pago,
                            COUNT(DISTINCT usuario_id) as usuarios_unicos
                        FROM pagos
                        WHERE fecha_pago::date BETWEEN %s AND %s
                    """, (tipo_reporte, fecha_inicio, fecha_fin))
                    
                elif tipo_reporte == 'asistencias':
                    cursor.execute("""
                        SELECT 
                            %s as tipo_reporte,
                            COUNT(*) as total,
                            0 as ingresos_totales,
                            0 as promedio_pago,
                            COUNT(DISTINCT usuario_id) as usuarios_unicos
                        FROM asistencias
                        WHERE fecha::date BETWEEN %s AND %s
                    """, (tipo_reporte, fecha_inicio, fecha_fin))
                else:
                    return {'error': 'Tipo de reporte no válido'}
                
                resultado = cursor.fetchone()
                
                return {
                    'tipo_reporte': tipo_reporte,
                    'periodo': {'inicio': fecha_inicio.isoformat(), 'fin': fecha_fin.isoformat()},
                    'datos': dict(zip([desc[0] for desc in cursor.description], resultado)) if resultado else {},
                    'generado_en': datetime.now().isoformat()
                }
                
        except Exception as e:
            logging.error(f"Error generando reporte automático: {e}")
            return {'error': str(e)}
    

    def generar_reporte_optimizacion(self) -> Dict[str, Any]:
        """Genera un reporte completo del estado de optimización de la base de datos PostgreSQL"""
        reporte = {
            'fecha_reporte': datetime.now().isoformat(),
            'rendimiento': self.get_performance_stats(),
            'conexiones': self.get_connection_stats(),
            'indices': self.verificar_indices_database(),
            'recomendaciones': [],
            'estado_general': 'bueno'
        }
        
        # Analizar rendimiento y generar recomendaciones
        stats = reporte['rendimiento']
        
        if stats.get('cache_hit_ratio', 0) < 0.7:
            reporte['recomendaciones'].append("Ratio de cache bajo. Considere aumentar el tamaño del cache.")
            reporte['estado_general'] = 'mejorable'
        
        if stats.get('avg_query_time', 0) > 0.5:
            reporte['recomendaciones'].append("Tiempo promedio de consulta alto. Revise las consultas más lentas.")
            reporte['estado_general'] = 'mejorable'
        
        if len(reporte['indices']['indices_faltantes']) > 0:
            reporte['recomendaciones'].append(f"Faltan {len(reporte['indices']['indices_faltantes'])} índices críticos.")
            reporte['estado_general'] = 'mejorable'
        
        # Determinar estado general
        if len(reporte['recomendaciones']) == 0:
            reporte['estado_general'] = 'excelente'
        elif len(reporte['recomendaciones']) > 3:
            reporte['estado_general'] = 'necesita_atencion'
        
        return reporte
    
    # --- MÉTODOS PARA LISTA DE ESPERA ---
    

    def obtener_kpis_dashboard(self) -> dict:
        """Obtiene KPIs principales para el dashboard."""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor()
                
                kpis = {}
                
                # Total usuarios
                cursor.execute("SELECT COUNT(*) FROM usuarios")
                result = cursor.fetchone()
                kpis['total_users'] = result[0] if result else 0
                
                # Usuarios activos
                cursor.execute("SELECT COUNT(*) FROM usuarios WHERE activo = true")
                result = cursor.fetchone()
                kpis['active_users'] = result[0] if result else 0
                
                # Ingresos totales
                cursor.execute("SELECT COALESCE(SUM(monto), 0) FROM pagos")
                result = cursor.fetchone()
                kpis['total_revenue'] = float(result[0]) if result else 0.0
                
                # Clases activas
                cursor.execute("SELECT COUNT(*) FROM clases WHERE activa = true")
                result = cursor.fetchone()
                kpis['classes_today'] = result[0] if result else 0
                
                return kpis
                
        except Exception as e:
            logging.error(f"Error obteniendo KPIs dashboard: {str(e)}")
            return {'total_users': 0, 'active_users': 0, 'total_revenue': 0.0, 'classes_today': 0}
    

    def obtener_estadisticas_base_datos(self) -> dict:
        """Obtiene estadísticas de la base de datos."""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                estadisticas = {}
                
                # Contar registros en tablas principales
                tablas = ['usuarios', 'pagos', 'asistencias', 'clases', 'rutinas', 'profesores']
                for tabla in tablas:
                    try:
                        cursor.execute(f"SELECT COUNT(*) FROM {tabla}")
                        estadisticas[f"total_{tabla}"] = cursor.fetchone()[0] or 0
                    except:
                        estadisticas[f"total_{tabla}"] = 0
                
                # Tamaño de la base de datos
                try:
                    cursor.execute("SELECT pg_size_pretty(pg_database_size(current_database()))")
                    estadisticas['tamaño_bd'] = cursor.fetchone()[0]
                except:
                    estadisticas['tamaño_bd'] = 'No disponible'
                
                # Conexiones activas
                try:
                    cursor.execute("SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()")
                    estadisticas['conexiones_activas'] = cursor.fetchone()[0] or 0
                except:
                    estadisticas['conexiones_activas'] = 0
                
                return estadisticas
                
        except Exception as e:
            logging.error(f"Error obteniendo estadísticas de base de datos: {str(e)}")
            return {}
    

    def generar_reporte_completo(self, fecha_inicio, fecha_fin) -> dict:
        """Genera un reporte completo del sistema."""
        try:
            reporte = {
                'periodo': {'inicio': fecha_inicio, 'fin': fecha_fin},
                'usuarios': {
                    'total': self.contar_usuarios_totales(),
                    'activos': self.contar_usuarios_activos(),
                    'nuevos': self.contar_usuarios_nuevos_periodo(fecha_inicio, fecha_fin)
                },
                'pagos': {
                    'total': len(self.obtener_pagos_por_rango_fechas(fecha_inicio, fecha_fin)),
                    'ingresos': sum(p['monto'] for p in self.obtener_pagos_por_rango_fechas(fecha_inicio, fecha_fin))
                },
                'clases': {
                    'total': self.contar_clases_totales(),
                    'ocupacion': self.obtener_estadisticas_ocupacion_clases()
                }
            }
            return reporte
        except Exception as e:
            logging.error(f"Error generando reporte completo: {str(e)}")
            return {}
    

    def obtener_estadisticas_automatizacion(self) -> dict:
        """Obtiene estadísticas del sistema de automatización de estados"""
        try:
            with self.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    # Estadísticas generales
                    cursor.execute("""
                        SELECT 
                            COUNT(*) as total_usuarios,
                            COUNT(CASE WHEN activo = TRUE THEN 1 END) as usuarios_activos,
                            COUNT(CASE WHEN activo = FALSE THEN 1 END) as usuarios_inactivos
                        FROM usuarios WHERE rol = 'socio'
                    """)
                    stats_usuarios = cursor.fetchone()
                    
                    # Estados activos
                    cursor.execute("""
                        SELECT estado, COUNT(*) as cantidad
                        FROM usuario_estados 
                        WHERE activo = TRUE 
                        GROUP BY estado
                        ORDER BY cantidad DESC
                    """)
                    estados_activos = dict(cursor.fetchall())
                    
                    # Usuarios próximos a vencer
                    cursor.execute("""
                        SELECT COUNT(*) 
                        FROM usuario_estados ue
                        JOIN usuarios u ON ue.usuario_id = u.id
                        WHERE ue.activo = TRUE 
                          AND ue.fecha_vencimiento IS NOT NULL 
                          AND ue.fecha_vencimiento BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '7 days'
                          AND u.rol = 'socio'
                    """)
                    proximos_vencer = cursor.fetchone()[0]
                    
                    # Usuarios vencidos
                    cursor.execute("""
                        SELECT COUNT(*) 
                        FROM usuario_estados ue
                        JOIN usuarios u ON ue.usuario_id = u.id
                        WHERE ue.activo = TRUE 
                          AND ue.fecha_vencimiento IS NOT NULL 
                          AND ue.fecha_vencimiento < CURRENT_DATE
                          AND u.rol = 'socio'
                    """)
                    vencidos = cursor.fetchone()[0]
                    
                    # Historial de automatización (últimos 30 días)
                    # Verificar si existe la tabla historial_estados
                    cursor.execute("""
                        SELECT COUNT(*) 
                        FROM information_schema.tables 
                        WHERE table_name = 'historial_estados' AND table_schema = 'public'
                    """)
                    tabla_existe = cursor.fetchone()[0] > 0
                    
                    if tabla_existe:
                        # Verificar si existe la columna motivo
                        cursor.execute("""
                            SELECT COUNT(*) 
                            FROM information_schema.columns 
                            WHERE table_name = 'historial_estados' 
                              AND column_name = 'motivo' 
                              AND table_schema = 'public'
                        """)
                        columna_existe = cursor.fetchone()[0] > 0
                        
                        if columna_existe:
                            cursor.execute("""
                                SELECT COUNT(*) 
                                FROM historial_estados 
                                WHERE motivo LIKE '%automático%' 
                                  AND fecha_accion >= CURRENT_DATE - INTERVAL '30 days'
                            """)
                            automatizaciones_mes = cursor.fetchone()[0]
                        else:
                            # Si no existe la columna motivo, usar una consulta alternativa
                            cursor.execute("""
                                SELECT COUNT(*) 
                                FROM historial_estados 
                                WHERE fecha_accion >= CURRENT_DATE - INTERVAL '30 days'
                            """)
                            automatizaciones_mes = cursor.fetchone()[0]
                    else:
                        # Si no existe la tabla, usar 0
                        automatizaciones_mes = 0
                    
                    return {
                        'usuarios': {
                            'total': stats_usuarios[0] or 0,
                            'activos': stats_usuarios[1] or 0,
                            'inactivos': stats_usuarios[2] or 0
                        },
                        'estados_activos': estados_activos,
                        'alertas': {
                            'proximos_vencer': proximos_vencer or 0,
                            'vencidos': vencidos or 0
                        },
                        'automatizacion': {
                            'ejecuciones_mes': automatizaciones_mes or 0,
                            'ultima_ejecucion': self.obtener_configuracion('ultima_automatizacion') or 'Nunca'
                        }
                    }
                    
        except Exception as e:
            logging.error(f"Error obteniendo estadísticas de automatización: {e}")
            return {
                'usuarios': {'total': 0, 'activos': 0, 'inactivos': 0},
                'estados_activos': {},
                'alertas': {'proximos_vencer': 0, 'vencidos': 0},
                'automatizacion': {'ejecuciones_mes': 0, 'ultima_ejecucion': 'Error'}
            }
    
