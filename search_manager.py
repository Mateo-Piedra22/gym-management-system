from PyQt6.QtCore import QObject, pyqtSignal, QTimer
from PyQt6.QtWidgets import QCompleter
from PyQt6.QtCore import QStringListModel
from database import DatabaseManager
import logging
from typing import Dict, List, Tuple, Any
from datetime import datetime, timedelta
import threading
import time

class SearchManager(QObject):
    """Gestor de búsqueda unificada para todo el sistema"""
    
    # Señales para comunicar resultados
    search_completed = pyqtSignal(str, list)  # query, results
    suggestions_updated = pyqtSignal(list)    # suggestions
    
    def __init__(self, db_manager: DatabaseManager):
        super().__init__()
        self.db_manager = db_manager
        self.search_cache = {}
        self.suggestion_cache = []
        self.last_cache_update = None
        
        # Timer para búsqueda con delay (evitar búsquedas excesivas)
        self.search_timer = QTimer()
        self.search_timer.setSingleShot(True)
        self.search_timer.timeout.connect(self._execute_delayed_search)
        
        # Configuración de búsqueda
        self.search_delay = 300  # ms
        self.min_query_length = 2
        self.max_results_per_category = 10
        self._search_worker_running = False
        
        # Inicializar cache de sugerencias con delay para evitar recursión
        self._update_suggestion_cache_delayed()
    
    def search(self, query: str, categories: List[str] = None, delay: bool = True) -> None:
        """Realiza búsqueda unificada con delay opcional"""
        if len(query.strip()) < self.min_query_length:
            self.search_completed.emit(query, [])
            return
        
        self.pending_query = query.strip().lower()
        self.pending_categories = categories or ['usuarios', 'pagos', 'clases', 'profesores', 'rutinas']
        
        if delay:
            self.search_timer.start(self.search_delay)
        else:
            self._execute_delayed_search()
    
    def _execute_delayed_search(self) -> None:
        """Ejecuta la búsqueda pendiente en un hilo de fondo para no bloquear la UI"""
        if self._search_worker_running:
            # Evitar múltiples ejecuciones simultáneas; la última consulta pendiente será procesada
            return

        def _worker():
            self._search_worker_running = True
            try:
                t0 = time.perf_counter()
                query = self.pending_query
                categories = self.pending_categories

                # Verificar cache
                cache_key = f"{query}_{'-'.join(sorted(categories))}"
                if cache_key in self.search_cache:
                    try:
                        self.search_completed.emit(query, self.search_cache[cache_key])
                    except Exception:
                        pass
                    return

                results = []

                # Búsqueda en cada categoría
                if 'usuarios' in categories:
                    results.extend(self._search_usuarios(query))

                if 'pagos' in categories:
                    results.extend(self._search_pagos(query))

                if 'clases' in categories:
                    results.extend(self._search_clases(query))

                if 'profesores' in categories:
                    results.extend(self._search_profesores(query))

                if 'rutinas' in categories:
                    results.extend(self._search_rutinas(query))

                # Ordenar por relevancia
                try:
                    results.sort(key=lambda x: x.get('relevance', 0), reverse=True)
                except Exception:
                    pass

                # Limitar resultados totales
                results = results[:50]

                # Guardar en cache
                try:
                    self.search_cache[cache_key] = results
                except Exception:
                    pass

                # Emitir resultados (Qt encolará la señal hacia el hilo principal)
                try:
                    self.search_completed.emit(query, results)
                    try:
                        elapsed = time.perf_counter() - t0
                        logging.info(f"SearchManager: query='{query}' cats={categories} results={len(results)} time={elapsed:.3f}s")
                    except Exception:
                        pass
                except Exception:
                    pass
            except Exception as e:
                logging.error(f"Error en búsqueda: {e}")
                try:
                    self.search_completed.emit(self.pending_query, [])
                except Exception:
                    pass
            finally:
                self._search_worker_running = False

        try:
            threading.Thread(target=_worker, daemon=True).start()
        except Exception:
            # Fallback: ejecutar en el hilo principal si el hilo falla
            try:
                _worker()
            except Exception:
                pass
    
    def _search_usuarios(self, query: str) -> List[Dict[str, Any]]:
        """Busca en usuarios"""
        results = []
        try:
            # Usar sesión de solo lectura con límites para seguridad y rendimiento
            with self.db_manager.readonly_session(lock_ms=800, statement_ms=1500, idle_s=2, seqscan_off=True) as conn:
                cursor = conn.cursor()
                
                # Búsqueda por nombre, dni, teléfono
                cursor.execute("""
                    SELECT id, nombre, dni, telefono, activo, rol
                    FROM usuarios 
                    WHERE LOWER(nombre) LIKE %s OR LOWER(dni) LIKE %s 
                       OR telefono LIKE %s
                    ORDER BY activo DESC, nombre ASC
                    LIMIT %s
                """, (f'%{query}%', f'%{query}%', f'%{query}%', self.max_results_per_category))
                
                for row in cursor.fetchall():
                    relevance = self._calculate_user_relevance(query, row)
                    results.append({
                        'type': 'usuario',
                        'id': row[0],
                        'title': row[1],
                        'subtitle': f"{row[5].title()} - DNI: {row[2]}",
                        'description': f"Tel: {row[3]} | {'Activo' if row[4] else 'Inactivo'}",
                        'icon': 'Usuario',
                        'data': {
                            'usuario_id': row[0],
                            'nombre': row[1],
                            'dni': row[2],
                            'telefono': row[3],
                            'activo': row[4],
                            'rol': row[5]
                        },
                        'relevance': relevance
                    })
        except Exception as e:
            logging.error(f"Error buscando usuarios: {e}")
        
        return results
    
    def _search_pagos(self, query: str) -> List[Dict[str, Any]]:
        """Busca en pagos"""
        results = []
        try:
            # Sesión de solo lectura para consultas de pagos
            with self.db_manager.readonly_session(lock_ms=800, statement_ms=1500, idle_s=2, seqscan_off=True) as conn:
                cursor = conn.cursor()
                
                # Búsqueda por monto, usuario
                cursor.execute("""
                    SELECT p.id, p.monto, p.fecha_pago, p.mes, p.año,
                           u.nombre
                    FROM pagos p
                    JOIN usuarios u ON p.usuario_id = u.id
                    WHERE CAST(p.monto AS TEXT) LIKE %s 
                       OR LOWER(u.nombre) LIKE %s
                    ORDER BY p.fecha_pago DESC
                    LIMIT %s
                """, (f'%{query}%', f'%{query}%', self.max_results_per_category))
                
                for row in cursor.fetchall():
                    results.append({
                        'type': 'pago',
                        'id': row[0],
                        'title': f"Pago ${row[1]:,.0f}",
                        'subtitle': f"{row[5]} - Efectivo",
                        'description': f"Fecha: {row[2]} | Período: {row[3]}/{row[4]}",
                        'icon': 'Pago',
                        'data': {
                            'pago_id': row[0],
                            'monto': row[1],
                            'fecha_pago': row[2],
                            'mes': row[3],
                            'año': row[4],
                            'usuario_nombre': row[5]
                        },
                        'relevance': 70
                    })
        except Exception as e:
            logging.error(f"Error buscando pagos: {e}")
        
        return results
    
    def _search_clases(self, query: str) -> List[Dict[str, Any]]:
        """Busca en clases"""
        results = []
        try:
            # Sesión de solo lectura para consultas de clases
            with self.db_manager.readonly_session(lock_ms=800, statement_ms=1500, idle_s=2, seqscan_off=True) as conn:
                cursor = conn.cursor()
                
                # Búsqueda por nombre de clase, profesor
                cursor.execute("""
                    SELECT c.id, c.nombre, c.descripcion, p.nombre as profesor_nombre
                    FROM clases c
                    LEFT JOIN clases_horarios ch ON c.id = ch.clase_id
                    LEFT JOIN profesor_clase_asignaciones pca ON ch.id = pca.clase_horario_id AND pca.activa = true
                    LEFT JOIN usuarios p ON pca.profesor_id = p.id
                    WHERE LOWER(c.nombre) LIKE %s OR LOWER(c.descripcion) LIKE %s
                       OR LOWER(p.nombre) LIKE %s
                    GROUP BY c.id, c.nombre, c.descripcion, p.nombre
                    ORDER BY c.nombre ASC
                    LIMIT %s
                """, (f'%{query}%', f'%{query}%', f'%{query}%', self.max_results_per_category))
                
                for row in cursor.fetchall():
                    profesor = row[3] if row[3] else "Sin asignar"
                    results.append({
                        'type': 'clase',
                        'id': row[0],
                        'title': row[1],
                        'subtitle': f"Profesor: {profesor}",
                        'description': row[2] or "Sin descripción",
                        'icon': '🏃‍♂️',
                        'data': {
                            'clase_id': row[0],
                            'nombre': row[1],
                            'descripcion': row[2],
                            'profesor': profesor
                        },
                        'relevance': 80
                    })
        except Exception as e:
            logging.error(f"Error buscando clases: {e}")
        
        return results
    
    def _search_profesores(self, query: str) -> List[Dict[str, Any]]:
        """Busca en profesores"""
        results = []
        try:
            # Sesión de solo lectura para consultas de profesores
            with self.db_manager.readonly_session(lock_ms=800, statement_ms=1500, idle_s=2, seqscan_off=True) as conn:
                cursor = conn.cursor()
                
                # Búsqueda específica en profesores
                cursor.execute("""
                    SELECT id, nombre, telefono
                    FROM usuarios 
                    WHERE rol = 'profesor' AND (
                        LOWER(nombre) LIKE %s OR telefono LIKE %s
                    )
                    ORDER BY nombre ASC
                    LIMIT %s
                """, (f'%{query}%', f'%{query}%', self.max_results_per_category))
                
                for row in cursor.fetchall():
                    results.append({
                        'type': 'profesor',
                        'id': row[0],
                        'title': row[1],
                        'subtitle': "Profesor",
                        'description': f"Tel: {row[2]}",
                        'icon': '👨‍🏫',
                        'data': {
                            'profesor_id': row[0],
                            'nombre': row[1],
                            'telefono': row[2]
                        },
                        'relevance': 85
                    })
        except Exception as e:
            logging.error(f"Error buscando profesores: {e}")
        
        return results
    
    def _search_rutinas(self, query: str) -> List[Dict[str, Any]]:
        """Busca en rutinas"""
        results = []
        try:
            # Sesión de solo lectura para consultas de rutinas
            with self.db_manager.readonly_session(lock_ms=800, statement_ms=1500, idle_s=2, seqscan_off=True) as conn:
                cursor = conn.cursor()
                
                # Búsqueda en rutinas
                cursor.execute("""
                    SELECT r.id, r.nombre_rutina, r.descripcion, u.nombre
            FROM rutinas r
            LEFT JOIN usuarios u ON r.usuario_id = u.id
            WHERE LOWER(r.nombre_rutina) LIKE %s OR LOWER(r.descripcion) LIKE %s
            ORDER BY r.nombre_rutina ASC
                    LIMIT %s
                """, (f'%{query}%', f'%{query}%', self.max_results_per_category))
                
                for row in cursor.fetchall():
                    usuario = row[3] if row[3] else "Sin asignar"
                    results.append({
                        'type': 'rutina',
                        'id': row[0],
                        'title': row[1],
                        'subtitle': f"Usuario: {usuario}",
                        'description': row[2] or "Sin descripción",
                        'icon': '📋',
                        'data': {
                            'rutina_id': row[0],
                            'nombre': row[1],
                            'descripcion': row[2],
                            'usuario': usuario
                        },
                        'relevance': 75
                    })
        except Exception as e:
            logging.error(f"Error buscando rutinas: {e}")
        
        return results
    
    def _calculate_user_relevance(self, query: str, user_data: tuple) -> int:
        """Calcula relevancia de usuario basado en coincidencias"""
        relevance = 50  # Base
        
        nombre, dni = user_data[1].lower(), user_data[2].lower()
        
        # Coincidencia exacta en nombre
        if query == nombre:
            relevance += 40
        # Coincidencia al inicio
        elif nombre.startswith(query):
            relevance += 30
        # Coincidencia en DNI
        elif query in dni:
            relevance += 20
        
        # Bonus por usuario activo
        if user_data[4]:  # activo
            relevance += 10
        
        return relevance
    
    def get_suggestions(self, query: str = "") -> List[str]:
        """Obtiene sugerencias para autocompletado"""
        if not query:
            return self.suggestion_cache[:20]
        
        query_lower = query.lower()
        suggestions = [s for s in self.suggestion_cache if query_lower in s.lower()]
        return suggestions[:10]
    
    def _update_suggestion_cache_delayed(self) -> None:
        """Actualiza cache de sugerencias con delay para evitar recursión durante inicialización"""
        if hasattr(self.db_manager, '_initializing') and self.db_manager._initializing:
            # Si la base de datos se está inicializando, retrasar la carga
            QTimer.singleShot(1000, self._update_suggestion_cache)
        else:
            self._update_suggestion_cache()
    
    def _update_suggestion_cache(self) -> None:
        """Actualiza cache de sugerencias"""
        try:
            suggestions = set()
            # Sesión de solo lectura para cache de sugerencias
            with self.db_manager.readonly_session(lock_ms=800, statement_ms=1500, idle_s=2, seqscan_off=True) as conn:
                cursor = conn.cursor()
                
                # Nombres de usuarios
                cursor.execute("SELECT nombre FROM usuarios WHERE activo = %s", (True,))
                for row in cursor.fetchall():
                    if row[0]: suggestions.add(row[0])
                
                # Nombres de clases
                cursor.execute("SELECT nombre FROM clases")
                for row in cursor.fetchall():
                    if row[0]: suggestions.add(row[0])
                
                # Nombres de rutinas
                cursor.execute("SELECT DISTINCT nombre_rutina FROM rutinas WHERE nombre_rutina IS NOT NULL")
                for row in cursor.fetchall():
                    if row[0]: suggestions.add(row[0])
            
            self.suggestion_cache = sorted(list(suggestions))
            self.last_cache_update = datetime.now()
            
        except Exception as e:
            logging.error(f"Error actualizando cache de sugerencias: {e}")
    
    def refresh_cache(self) -> None:
        """Refresca todos los caches"""
        self.search_cache.clear()
        self._update_suggestion_cache()
    
    def clear_cache(self) -> None:
        """Limpia todos los caches"""
        self.search_cache.clear()
        self.suggestion_cache.clear()
    
    def get_recent_searches(self) -> List[str]:
        """Obtiene búsquedas recientes (implementación básica)"""
        # En una implementación completa, esto se guardaría en base de datos
        return list(self.search_cache.keys())[-10:]
    
    def export_search_results(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Exporta resultados de búsqueda para análisis"""
        return {
            'timestamp': datetime.now().isoformat(),
            'total_results': len(results),
            'results_by_type': {
                result_type: len([r for r in results if r['type'] == result_type])
                for result_type in set(r['type'] for r in results)
            },
            'results': results
        }