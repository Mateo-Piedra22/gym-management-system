import sys
import logging
from datetime import datetime, timedelta
import psycopg2.extras
from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QGridLayout, QLabel,
                             QMessageBox, QGroupBox, QListWidget, QListWidgetItem,
                             QScrollArea, QPushButton, QFrame, QHBoxLayout, QApplication, QComboBox,
                             QSizePolicy, QFileDialog)
from PyQt6.QtGui import QFont, QPixmap
from PyQt6.QtCore import Qt, QSize, QTimer, QThread, pyqtSignal

from database import DatabaseManager
from payment_manager import PaymentManager
from export_manager import ExportManager
from widgets.chart_widget import MplChartWidget
from widgets.operational_reports_widget import OperationalReportsWidget
from utils import resource_path, get_gym_name
from reportlab.lib.pagesizes import letter, A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
import os
import tempfile
import matplotlib.pyplot as plt
from io import BytesIO
from reportlab.platypus import Image as RLImage

class MetricCard(QFrame):
    def __init__(self, icon_path, title, value, description, parent=None):
        super().__init__(parent)
        self.setFrameShape(QFrame.Shape.StyledPanel); self.setFrameShadow(QFrame.Shadow.Raised)
        # Se establece la propiedad 'class' para que coincida con el selector en style.txt/.qss
        self.setProperty("class", "metric-card")
        
        main_layout = QVBoxLayout(self)
        header_layout = QHBoxLayout()
        icon_label = QLabel()
        try:
            pixmap = QPixmap(resource_path(icon_path)).scaled(QSize(32, 32), Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation)
            icon_label.setPixmap(pixmap)
        except Exception: icon_label.setText("●")
        title_label = QLabel(title); title_label.setFont(QFont("Segoe UI", 10, QFont.Weight.Bold))
        header_layout.addWidget(icon_label); header_layout.addWidget(title_label); header_layout.addStretch()
        value_label = QLabel(value); value_label.setFont(QFont("Segoe UI", 22, QFont.Weight.Bold)); value_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        description_label = QLabel(description); description_label.setFont(QFont("Segoe UI", 9)); description_label.setAlignment(Qt.AlignmentFlag.AlignCenter); description_label.setWordWrap(True)
        main_layout.addLayout(header_layout); main_layout.addWidget(value_label); main_layout.addWidget(description_label)
        
class DashboardWorkerThread(QThread):
    """Hilo ligero para cargar datos del dashboard (KPIs y gráficos) sin bloquear la UI."""
    data_ready = pyqtSignal(str, dict)
    error = pyqtSignal(str)

    def __init__(self, db_manager: DatabaseManager, payment_manager: PaymentManager, task: str, params: dict | None = None):
        super().__init__()
        self.db_manager = db_manager
        self.payment_manager = payment_manager
        self.task = task
        self.params = params or {}

    def run(self):
        try:
            # Salida temprana si se solicitó interrupción
            if self.isInterruptionRequested():
                return
            if self.task == 'kpis':
                result: dict = {}
                try:
                    # Caché agresivo para KPIs generales
                    cache_key = 'general_kpis'
                    cached = getattr(self.db_manager, 'cache_manager', None) and self.db_manager.cache_manager.get(cache_key)
                    if cached and isinstance(cached, dict):
                        result['general_kpis'] = cached
                    else:
                        result['general_kpis'] = self.db_manager.obtener_kpis_generales()
                        try:
                            if getattr(self.db_manager, 'cache_manager', None):
                                # TTL corto 120s para KPIs que cambian frecuente
                                self.db_manager.cache_manager.set(cache_key, result['general_kpis'], ttl_ms=120*1000)
                        except Exception:
                            pass
                except Exception as e:
                    result['general_kpis_error'] = str(e)
                if self.isInterruptionRequested():
                    return
                try:
                    cache_key = 'arpu_morosos_mes_actual'
                    cached = getattr(self.db_manager, 'cache_manager', None) and self.db_manager.cache_manager.get(cache_key)
                    if cached and isinstance(cached, dict) and 'arpu' in cached and 'morosos' in cached:
                        arpu = cached['arpu']; morosos = cached['morosos']
                    else:
                        arpu, morosos = self.db_manager.obtener_arpu_y_morosos_mes_actual()
                        try:
                            if getattr(self.db_manager, 'cache_manager', None):
                                self.db_manager.cache_manager.set(cache_key, {'arpu': arpu, 'morosos': morosos}, ttl_ms=180*1000)
                        except Exception:
                            pass
                    result['arpu'] = arpu
                    result['morosos'] = morosos
                except Exception as e:
                    result['arpu_error'] = str(e)
                if self.isInterruptionRequested():
                    return
                try:
                    # Reutilizamos el cálculo existente que no toca UI
                    cache_key = 'advanced_kpis'
                    cached = getattr(self.db_manager, 'cache_manager', None) and self.db_manager.cache_manager.get(cache_key)
                    if cached and isinstance(cached, dict):
                        result['advanced_kpis'] = cached
                    else:
                        result['advanced_kpis'] = self._calculate_advanced_kpis_thread()
                        try:
                            if getattr(self.db_manager, 'cache_manager', None):
                                self.db_manager.cache_manager.set(cache_key, result['advanced_kpis'], ttl_ms=180*1000)
                        except Exception:
                            pass
                except Exception as e:
                    result['advanced_kpis_error'] = str(e)
                    result['advanced_kpis'] = {}
                self.data_ready.emit('kpis', result)
            elif self.task == 'charts':
                dias_periodo = int(self.params.get('dias_periodo', 90))
                result: dict = {}
                try:
                    cache_key = 'income_12m'
                    cached = getattr(self.db_manager, 'cache_manager', None) and self.db_manager.cache_manager.get(cache_key)
                    if cached and isinstance(cached, dict):
                        result['income_12m'] = cached
                    else:
                        result['income_12m'] = self.payment_manager.obtener_ingresos_ultimos_12_meses()
                        try:
                            if getattr(self.db_manager, 'cache_manager', None):
                                self.db_manager.cache_manager.set(cache_key, result['income_12m'], ttl_ms=10*60*1000)
                        except Exception:
                            pass
                except Exception as e:
                    result['income_error'] = str(e)
                    result['income_12m'] = {}
                if self.isInterruptionRequested():
                    return
                try:
                    cache_key = 'nuevos_por_mes'
                    cached = getattr(self.db_manager, 'cache_manager', None) and self.db_manager.cache_manager.get(cache_key)
                    if cached and isinstance(cached, dict):
                        result['nuevos_por_mes'] = cached
                    else:
                        result['nuevos_por_mes'] = self.db_manager.obtener_nuevos_usuarios_por_mes()
                        try:
                            if getattr(self.db_manager, 'cache_manager', None):
                                self.db_manager.cache_manager.set(cache_key, result['nuevos_por_mes'], ttl_ms=10*60*1000)
                        except Exception:
                            pass
                except Exception as e:
                    result['nuevos_error'] = str(e)
                    result['nuevos_por_mes'] = {}
                if self.isInterruptionRequested():
                    return
                try:
                    # Aplicar filtros recibidos desde la UI si corresponden
                    tipo_id = self.params.get('tipo_id', None)
                    rol_filter = self.params.get('rol_filter', 'todos') or 'todos'
                    if (tipo_id is None) and (rol_filter == 'todos'):
                        cache_key = f"asistencia_semana_d{dias_periodo}"
                        cached = getattr(self.db_manager, 'cache_manager', None) and self.db_manager.cache_manager.get(cache_key)
                        if cached and isinstance(cached, dict):
                            result['asistencia_semana'] = cached
                        else:
                            result['asistencia_semana'] = self.db_manager.obtener_asistencias_por_dia_semana(dias=dias_periodo)
                            try:
                                if getattr(self.db_manager, 'cache_manager', None):
                                    self.db_manager.cache_manager.set(cache_key, result['asistencia_semana'], ttl_ms=10*60*1000)
                            except Exception:
                                pass
                    else:
                        # Cálculo filtrado asíncrono: obtener IDs de usuarios que cumplen filtros
                        dias_semana = ['Lunes','Martes','Miércoles','Jueves','Viernes','Sábado','Domingo']
                        asistencia_contada = {d: 0 for d in dias_semana}
                        usuarios_filtrados_ids = set()
                        try:
                            from psycopg2.extras import RealDictCursor
                            with self.db_manager.readonly_session(lock_ms=800, statement_ms=2000, idle_s=2, seqscan_off=True) as conn:
                                cursor = conn.cursor(cursor_factory=RealDictCursor)
                                condiciones = ["activo = TRUE"]
                                params = []
                                if rol_filter == 'profesor':
                                    condiciones.append("LOWER(rol) = 'profesor'")
                                elif rol_filter == 'socio':
                                    condiciones.append("LOWER(rol) NOT IN ('profesor','dueño')")
                                if tipo_id is not None:
                                    condiciones.append("CAST(tipo_cuota AS TEXT) = %s")
                                    params.append(str(tipo_id))
                                where = " AND ".join(condiciones) if condiciones else "TRUE"
                                cursor.execute(f"SELECT id FROM usuarios WHERE {where}", tuple(params))
                                for row in cursor.fetchall() or []:
                                    usuarios_filtrados_ids.add(row.get('id'))
                        except Exception as e:
                            logging.warning(f"Fallo al filtrar usuarios vía SQL en hilo de dashboard: {e}")

                        if usuarios_filtrados_ids:
                            from datetime import datetime, timedelta
                            fecha_inicio = datetime.now().date() - timedelta(days=dias_periodo)
                            asistencias = self.db_manager.obtener_asistencias_por_fecha_limite(fecha_inicio) or []
                            for a in asistencias:
                                uid = a.get('usuario_id') if isinstance(a, dict) else (a[1] if isinstance(a, (list, tuple)) else None)
                                if uid in usuarios_filtrados_ids:
                                    fecha = a.get('fecha') if isinstance(a, dict) else (a[4] if isinstance(a, (list, tuple)) else None)
                                    try:
                                        fecha_dt = fecha.date() if hasattr(fecha, 'date') else fecha
                                        weekday = fecha_dt.weekday() if fecha_dt else None
                                        if weekday is not None:
                                            asistencia_contada[dias_semana[weekday]] += 1
                                    except Exception:
                                        pass
                        # Guardar resultado
                        result['asistencia_semana'] = asistencia_contada
                except Exception as e:
                    result['asistencia_semana_error'] = str(e)
                    result['asistencia_semana'] = {}
                if self.isInterruptionRequested():
                    return
                try:
                    cache_key = 'user_dist'
                    cached = getattr(self.db_manager, 'cache_manager', None) and self.db_manager.cache_manager.get(cache_key)
                    if cached and isinstance(cached, dict):
                        result['user_dist'] = cached
                    else:
                        result['user_dist'] = self.db_manager.obtener_conteo_activos_inactivos()
                        try:
                            if getattr(self.db_manager, 'cache_manager', None):
                                self.db_manager.cache_manager.set(cache_key, result['user_dist'], ttl_ms=10*60*1000)
                        except Exception:
                            pass
                except Exception as e:
                    result['user_dist_error'] = str(e)
                    result['user_dist'] = {}
                if self.isInterruptionRequested():
                    return
                try:
                    cache_key = 'membership_dist'
                    cached = getattr(self.db_manager, 'cache_manager', None) and self.db_manager.cache_manager.get(cache_key)
                    if cached and isinstance(cached, dict):
                        result['membership_dist'] = cached
                    else:
                        result['membership_dist'] = self.db_manager.obtener_conteo_tipos_cuota()
                        try:
                            if getattr(self.db_manager, 'cache_manager', None):
                                self.db_manager.cache_manager.set(cache_key, result['membership_dist'], ttl_ms=10*60*1000)
                        except Exception:
                            pass
                except Exception as e:
                    result['membership_dist_error'] = str(e)
                    result['membership_dist'] = {}
                try:
                    # ARPU mensual 12m con caché ligero similar al método existente
                    labels = []
                    arpu_values = []
                    cached = None
                    try:
                        cache_key = 'arpu_mensual_12m'
                        cached = getattr(self.db_manager, 'cache_manager', None) and self.db_manager.cache_manager.get(cache_key)
                    except Exception:
                        cached = None
                    if cached and isinstance(cached, dict) and cached.get('labels') and cached.get('values'):
                        labels = cached['labels']
                        arpu_values = cached['values']
                    else:
                        from datetime import datetime
                        with self.db_manager.get_connection_context() as conn:
                            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                            try:
                                self.db_manager._apply_readonly_timeouts(cursor)
                            except Exception:
                                pass
                            today = datetime.now()
                            for i in range(11, -1, -1):
                                month = ((today.month - i - 1) % 12) + 1
                                year = today.year + ((today.month - i - 1) // 12)
                                labels.append(f"{month:02d}/{year}")
                                cursor.execute(
                                    "SELECT COALESCE(SUM(monto), 0) AS ingresos FROM pagos WHERE EXTRACT(MONTH FROM fecha_pago) = %s AND EXTRACT(YEAR FROM fecha_pago) = %s",
                                    (month, year)
                                )
                                res_ing = cursor.fetchone()
                                ingresos_mes = float(res_ing['ingresos']) if res_ing and res_ing['ingresos'] is not None else 0.0
                                cursor.execute(
                                    "SELECT COUNT(DISTINCT usuario_id) AS pagadores FROM pagos WHERE EXTRACT(MONTH FROM fecha_pago) = %s AND EXTRACT(YEAR FROM fecha_pago) = %s",
                                    (month, year)
                                )
                                res_pay = cursor.fetchone()
                                pagadores_mes = int(res_pay['pagadores']) if res_pay and res_pay['pagadores'] is not None else 0
                                arpu_values.append(ingresos_mes / pagadores_mes if pagadores_mes > 0 else 0.0)
                        try:
                            if getattr(self.db_manager, 'cache_manager', None):
                                self.db_manager.cache_manager.set('arpu_mensual_12m', {'labels': labels, 'values': arpu_values}, ttl_ms=5*60*1000)
                        except Exception:
                            pass
                    result['arpu_12m'] = {'labels': labels, 'values': arpu_values}
                except Exception as e:
                    result['arpu_12m_error'] = str(e)
                    result['arpu_12m'] = {'labels': [], 'values': []}
                self.data_ready.emit('charts', result)
        except Exception as e:
            self.error.emit(str(e))

    def _calculate_advanced_kpis_thread(self) -> dict:
        """Ejecuta el cálculo de KPIs avanzados en hilo, sin tocar UI."""
        try:
            # Reutilizamos la lógica del método original pero aislada aquí
            from datetime import datetime
            kpis = {
                'retention_rate': 0.0, 'churn_rate': 0.0, 'avg_attendance': 0.0,
                'peak_hour': 'N/A', 'active_classes': 0, 'avg_capacity': 0.0,
                'revenue_growth': 0.0, 'avg_ltv': 0.0, 'payment_rate': 0.0,
                'weekly_active_users': 0, 'high_value_memberships': 0,
                'avg_daily_attendance': 0.0, 'inactive_30d': 0, 'active_equipment': 0,
                'satisfaction_score': 0.0
            }
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                try:
                    self.db_manager._apply_readonly_timeouts(cursor)
                except Exception:
                    pass
                mes_actual = datetime.now().month
                año_actual = datetime.now().year
                mes_anterior = mes_actual - 1 if mes_actual > 1 else 12
                año_anterior = año_actual if mes_actual > 1 else año_actual - 1
                cursor.execute(
                    "SELECT COUNT(DISTINCT usuario_id) as count FROM pagos WHERE EXTRACT(MONTH FROM fecha_pago) = %s AND EXTRACT(YEAR FROM fecha_pago) = %s",
                    (mes_anterior, año_anterior)
                )
                pagos_mes_anterior = (cursor.fetchone() or {}).get('count', 0)
                cursor.execute(
                    "SELECT COUNT(DISTINCT p1.usuario_id) as count FROM pagos p1 "
                    "INNER JOIN pagos p2 ON p1.usuario_id = p2.usuario_id "
                    "WHERE EXTRACT(MONTH FROM p1.fecha_pago) = %s AND EXTRACT(YEAR FROM p1.fecha_pago) = %s AND EXTRACT(MONTH FROM p2.fecha_pago) = %s AND EXTRACT(YEAR FROM p2.fecha_pago) = %s",
                    (mes_actual, año_actual, mes_anterior, año_anterior)
                )
                socios_retenidos = (cursor.fetchone() or {}).get('count', 0)
                if pagos_mes_anterior > 0:
                    kpis['retention_rate'] = (socios_retenidos / pagos_mes_anterior) * 100
                kpis['churn_rate'] = 100 - kpis['retention_rate']
                cursor.execute("SELECT COUNT(*) as count FROM asistencias WHERE fecha >= CURRENT_DATE - INTERVAL '30 days'")
                total_asistencias = (cursor.fetchone() or {}).get('count', 0)
                cursor.execute("SELECT COUNT(*) as count FROM usuarios WHERE activo = true AND rol IN ('socio','profesor')")
                socios_activos = (cursor.fetchone() or {}).get('count', 0) or 1
                kpis['avg_attendance'] = total_asistencias / socios_activos
                cursor.execute(
                    "SELECT EXTRACT(HOUR FROM hora_registro) as hora, COUNT(*) as total "
                    "FROM asistencias WHERE fecha >= CURRENT_DATE - INTERVAL '30 days' "
                    "GROUP BY EXTRACT(HOUR FROM hora_registro) ORDER BY total DESC LIMIT 1"
                )
                peak = cursor.fetchone()
                if peak and peak.get('hora') is not None:
                    h = int(peak['hora'])
                    kpis['peak_hour'] = f'{h:02d}:00-{h+1:02d}:00'
                else:
                    kpis['peak_hour'] = 'Sin datos'
                cursor.execute("SELECT COUNT(*) as count FROM clases WHERE activa = TRUE")
                kpis['active_classes'] = (cursor.fetchone() or {}).get('count', 0)
                cursor.execute(
                    "SELECT AVG(CAST(inscritos AS FLOAT) / CAST(cupo_maximo AS FLOAT)) * 100 as avg_capacity "
                    "FROM (SELECT ch.cupo_maximo, COUNT(cu.usuario_id) as inscritos "
                    "FROM clases_horarios ch LEFT JOIN clase_usuarios cu ON ch.id = cu.clase_horario_id "
                    "GROUP BY ch.id) as ocupacion WHERE cupo_maximo > 0"
                )
                kpis['avg_capacity'] = (cursor.fetchone() or {}).get('avg_capacity', 0.0) or 0.0
                cursor.execute(
                    "SELECT SUM(monto) as total FROM pagos WHERE EXTRACT(MONTH FROM fecha_pago) = %s AND EXTRACT(YEAR FROM fecha_pago) = %s",
                    (mes_actual, año_actual)
                )
                ingresos_actual = (cursor.fetchone() or {}).get('total', 0) or 0
                cursor.execute(
                    "SELECT SUM(monto) as total FROM pagos WHERE EXTRACT(MONTH FROM fecha_pago) = %s AND EXTRACT(YEAR FROM fecha_pago) = %s",
                    (mes_anterior, año_anterior)
                )
                ingresos_anterior = (cursor.fetchone() or {}).get('total', 0) or 1
                if ingresos_anterior > 0 and ingresos_anterior != 1:
                    kpis['revenue_growth'] = ((ingresos_actual - ingresos_anterior) / ingresos_anterior) * 100
                else:
                    kpis['revenue_growth'] = 0.0
                cursor.execute(
                    "SELECT AVG(total_pagado) as avg_ltv FROM ("
                    "SELECT usuario_id, SUM(monto) as total_pagado FROM pagos GROUP BY usuario_id"
                    ") as ltv_data"
                )
                kpis['avg_ltv'] = (cursor.fetchone() or {}).get('avg_ltv', 0.0) or 0.0
                cursor.execute("SELECT COUNT(*) as count FROM usuarios WHERE activo = true AND rol IN ('socio','profesor')")
                total_activos = (cursor.fetchone() or {}).get('count', 0)
                cursor.execute("SELECT COUNT(DISTINCT usuario_id) as pagaron FROM pagos WHERE EXTRACT(MONTH FROM fecha_pago) = %s AND EXTRACT(YEAR FROM fecha_pago) = %s",
                               (mes_actual, año_actual))
                pagaron_mes = (cursor.fetchone() or {}).get('pagaron', 0)
                kpis['payment_rate'] = ((pagaron_mes / total_activos) * 100) if total_activos > 0 else 0.0
                cursor.execute("SELECT COUNT(DISTINCT usuario_id) as count FROM asistencias WHERE fecha >= CURRENT_DATE - INTERVAL '7 days'")
                kpis['weekly_active_users'] = (cursor.fetchone() or {}).get('count', 0)
                cursor.execute("SELECT COUNT(*) as total FROM usuarios u WHERE activo = TRUE AND rol IN ('socio','profesor')")
                total_activos_row = cursor.fetchone() or {}
                total_activos2 = total_activos_row.get('total', 0)
                cursor.execute(
                    "SELECT COUNT(DISTINCT a.usuario_id) as count FROM asistencias a "
                    "JOIN usuarios u ON u.id = a.usuario_id "
                    "WHERE u.activo = TRUE AND u.rol IN ('socio','profesor') AND a.fecha >= CURRENT_DATE - INTERVAL '30 days'"
                )
                asistentes_30d = (cursor.fetchone() or {}).get('count', 0)
                kpis['inactive_30d'] = max(0, total_activos2 - asistentes_30d)
                try:
                    cursor.execute("SELECT to_regclass('public.equipos') AS tbl")
                    tbl = cursor.fetchone() or {}
                    if not tbl.get('tbl'):
                        kpis['active_equipment'] = 0
                    else:
                        cursor.execute("SELECT COUNT(*) as count FROM equipos WHERE estado = 'activo' OR estado = 'disponible'")
                        kpis['active_equipment'] = (cursor.fetchone() or {}).get('count', 0)
                except Exception:
                    kpis['active_equipment'] = 0
                cursor.execute(
                    "SELECT AVG(puntuacion) as promedio FROM profesor_evaluaciones "
                    "WHERE fecha_evaluacion >= CURRENT_DATE - INTERVAL '90 days'"
                )
                kpis['satisfaction_score'] = (cursor.fetchone() or {}).get('promedio', 0.0) or 0.0
                cursor.execute(
                    "SELECT COUNT(*) as total FROM usuarios u "
                    "INNER JOIN tipos_cuota tc ON u.tipo_cuota = tc.nombre "
                    "WHERE u.activo = true AND tc.precio >= 8000"
                )
                kpis['high_value_memberships'] = (cursor.fetchone() or {}).get('total', 0)
                cursor.execute(
                    "SELECT AVG(asistentes) as promedio FROM ("
                    "SELECT COUNT(*) as asistentes FROM asistencias "
                    "WHERE fecha >= CURRENT_DATE - INTERVAL '30 days' "
                    "GROUP BY fecha::date"
                    ") as daily_attendance"
                )
                kpis['avg_daily_attendance'] = (cursor.fetchone() or {}).get('promedio', 0.0) or 0.0
            return kpis
        except Exception as e:
            logging.error(f"Error calculando KPIs avanzados en hilo: {e}")
            return {}

class ReportsTabWidget(QWidget):
    def __init__(self, db_manager: DatabaseManager, payment_manager: PaymentManager, export_manager: ExportManager, defer_initial_load: bool = False):
        super().__init__()
        self.db_manager = db_manager; self.payment_manager = payment_manager; self.export_manager = export_manager
        self._defer_initial_load = defer_initial_load
        self._initialized = False
        self.setup_ui()
        # Carga inicial diferida para evitar trabajo pesado en arranque
        try:
            if not self._defer_initial_load:
                QTimer.singleShot(0, self.initialize_dashboard_data)
        except Exception:
            pass

    def setup_ui(self):
        top_level_layout = QVBoxLayout(self)
        top_level_layout.setContentsMargins(20, 20, 20, 20)
        
        toolbar_layout = QHBoxLayout()
# Entre los "" va el Dashboard de reportes
        title_label = QLabel(""); title_label.setFont(QFont("Segoe UI", 18, QFont.Weight.Bold))
        # Control de carga pesada: permitir lazy-loading de gráficos
        from PyQt6.QtWidgets import QCheckBox
        self.auto_load_charts_checkbox = QCheckBox("Auto cargar gráficos")
        self.auto_load_charts_checkbox.setChecked(True)
        self.refresh_button = QPushButton("🔄 Actualizar Todo")
        self.refresh_button.setObjectName("refresh_button")
        self.refresh_button.clicked.connect(self.actualizar_reportes)
        self.export_button = QPushButton("Exportar Dashboard")
        self.export_button.setObjectName("export_button")
        self.export_button.clicked.connect(self.exportar_dashboard_pdf)
        
        self.operational_reports_button = QPushButton("📈 Reportes Operativos")
        self.operational_reports_button.setObjectName("operational_reports_button")
        self.operational_reports_button.clicked.connect(self.open_operational_reports)
        # Remover estilos hardcodeados para permitir CSS dinámico
        
        toolbar_layout.addWidget(title_label); toolbar_layout.addStretch(); toolbar_layout.addWidget(self.auto_load_charts_checkbox); toolbar_layout.addWidget(self.operational_reports_button); toolbar_layout.addWidget(self.export_button); toolbar_layout.addWidget(self.refresh_button)
        top_level_layout.addLayout(toolbar_layout)
        
        top_level_layout.addSpacing(15)

        scroll_area = QScrollArea(); scroll_area.setWidgetResizable(True); scroll_area.setFrameShape(QFrame.Shape.NoFrame)
        scroll_area.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        container_widget = QWidget(); scroll_area.setWidget(container_widget)
        container_widget.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        self.grid_layout = QGridLayout(container_widget); self.grid_layout.setSpacing(20)
        
        # Gráficos con configuración automática (sin controles manuales)
        self.income_chart = MplChartWidget(figsize=(10, 5), enable_toolbar=False, enable_alerts=True)
        self.income_chart.setObjectName("income_chart")
        self.nuevos_socios_chart = MplChartWidget(figsize=(10, 5), enable_toolbar=False, enable_alerts=True)
        self.nuevos_socios_chart.setObjectName("nuevos_socios_chart")
        self.asistencia_semanal_chart = MplChartWidget(figsize=(10, 5), enable_toolbar=False, enable_alerts=True)
        self.asistencia_semanal_chart.setObjectName("asistencia_semanal_chart")
        
        # Nuevos gráficos (adicionales)
        self.arpu_mensual_chart = MplChartWidget(figsize=(10, 5), enable_toolbar=False, enable_alerts=False)
        self.arpu_mensual_chart.setObjectName("arpu_mensual_chart")
        self.morosos_mensuales_chart = MplChartWidget(figsize=(10, 5), enable_toolbar=False, enable_alerts=False)
        self.morosos_mensuales_chart.setObjectName("morosos_mensuales_chart")
        
        # Gráficos de distribución (configuración automática)
        self.user_dist_chart = MplChartWidget(figsize=(8, 5), enable_toolbar=False, enable_alerts=False)
        self.user_dist_chart.setObjectName("user_dist_chart")
        self.membership_dist_chart = MplChartWidget(figsize=(8, 5), enable_toolbar=False, enable_alerts=False)
        self.membership_dist_chart.setObjectName("membership_dist_chart")
        
        # Ajustes de tamaño para evitar recortes y aprovechar ancho disponible
        for chart in (self.income_chart, self.nuevos_socios_chart, self.asistencia_semanal_chart, self.arpu_mensual_chart, self.morosos_mensuales_chart):
            chart.setMinimumHeight(380)
            chart.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        for chart in (self.user_dist_chart, self.membership_dist_chart):
            chart.setMinimumHeight(320)
            chart.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        
        # Controles para asistencia semanal (selector de periodo y filtros)
        self.dias_periodo = 90
        self.asistencia_controls_widget = QWidget()
        controls_layout = QHBoxLayout(self.asistencia_controls_widget)
        controls_layout.setContentsMargins(0, 0, 0, 0)
        controls_layout.addWidget(QLabel("Periodo:"))
        self.asistencia_period_selector = QComboBox()
        self.asistencia_period_selector.addItem("Últimos 7 días", 7)
        self.asistencia_period_selector.addItem("Últimos 30 días", 30)
        self.asistencia_period_selector.addItem("Últimos 60 días", 60)
        self.asistencia_period_selector.addItem("Últimos 90 días", 90)
        self.asistencia_period_selector.addItem("Últimos 180 días", 180)
        index_90 = self.asistencia_period_selector.findData(90)
        if index_90 >= 0:
            self.asistencia_period_selector.setCurrentIndex(index_90)
        self.asistencia_period_selector.currentIndexChanged.connect(self._on_asistencia_period_changed)
        controls_layout.addWidget(self.asistencia_period_selector)
        
        # Filtro por Tipo de Cuota
        controls_layout.addSpacing(10)
        controls_layout.addWidget(QLabel("Tipo de Cuota:"))
        self.asistencia_tipo_selector = QComboBox()
        self.asistencia_tipo_selector.addItem("Todos", None)
        try:
            for tipo in self.db_manager.obtener_tipos_cuota_activos():
                # Guardar id del tipo de cuota como data
                self.asistencia_tipo_selector.addItem(tipo.nombre, tipo.id)
        except Exception as e:
            logging.error(f"Error cargando tipos de cuota: {e}")
        self.asistencia_tipo_selector.currentIndexChanged.connect(self._on_asistencia_filters_changed)
        controls_layout.addWidget(self.asistencia_tipo_selector)
        
        # Filtro por Rol
        controls_layout.addSpacing(10)
        controls_layout.addWidget(QLabel("Rol:"))
        self.asistencia_rol_selector = QComboBox()
        self.asistencia_rol_selector.addItem("Todos", "todos")
        self.asistencia_rol_selector.addItem("Socios", "socio")
        self.asistencia_rol_selector.addItem("Profesores", "profesor")
        self.asistencia_rol_selector.currentIndexChanged.connect(self._on_asistencia_filters_changed)
        controls_layout.addWidget(self.asistencia_rol_selector)
        
        controls_layout.addStretch()
        
        # Envolver los filtros dentro de un panel para mejorar estética/ubicación
        self.asistencia_filters_group = QGroupBox("Filtros de Asistencia")
        self.asistencia_filters_group.setProperty("class", "metric-card")
        filtros_layout = QHBoxLayout(self.asistencia_filters_group)
        filtros_layout.setContentsMargins(10, 10, 10, 10)
        filtros_layout.addWidget(self.asistencia_controls_widget)
        filtros_layout.addStretch()
        
        # Configurar umbrales de alerta para gráficos
        self._setup_chart_alerts()
        self.activity_list = QListWidget()
        self.activity_list.setObjectName("activity_list")
        
        self.setup_layout()
        top_level_layout.addWidget(scroll_area)
        
        # Evitar carga pesada si se difiere inicialización
        if not getattr(self, '_defer_initial_load', False):
            QTimer.singleShot(100, self.initialize_dashboard_data)
        
        # Conectar señales de alertas
        self._connect_alert_signals()

    def initialize_dashboard_data(self):
        """Realiza la carga inicial de KPIs y gráficos si no se hizo aún."""
        if getattr(self, '_initialized', False):
            return
        try:
            self.actualizar_reportes()
        except Exception as e:
            logging.error(f"Error inicializando dashboard de reportes: {e}")
        finally:
            self._initialized = True
    
    def _setup_chart_alerts(self):
        """Configura umbrales de alerta para los gráficos"""
        # Umbrales para ingresos (ajustados para gimnasio en desarrollo)
        self.income_chart.set_alert_thresholds(
            critical_low=5000,   # Ingresos críticos
            warning_low=10000,   # Ingresos bajos
            target=20000         # Objetivo mensual
        )
        
        # Umbrales para nuevos socios (ajustados para gimnasio en desarrollo)
        self.nuevos_socios_chart.set_alert_thresholds(
            critical_low=0,      # Muy pocos nuevos socios
            warning_low=2,       # Pocos nuevos socios
            target=8             # Objetivo mensual
        )
        
        # Umbrales para asistencia semanal (ajustados para gimnasio en desarrollo)
        self.asistencia_semanal_chart.set_alert_thresholds(
            critical_low=0,      # Asistencia muy baja
            warning_low=3,       # Asistencia baja
            target=25            # Objetivo diario
        )
    
    def _connect_alert_signals(self):
        """Conecta las señales de alerta de los gráficos"""
        self.income_chart.alert_triggered.connect(self._handle_chart_alert)
        self.nuevos_socios_chart.alert_triggered.connect(self._handle_chart_alert)
        self.asistencia_semanal_chart.alert_triggered.connect(self._handle_chart_alert)
    
    def _on_asistencia_period_changed(self):
        try:
            dias = self.asistencia_period_selector.currentData()
            if isinstance(dias, int) and dias > 0:
                self.dias_periodo = dias
            self.update_charts()
        except Exception as e:
            logging.error(f"Error al cambiar periodo de asistencia: {e}")
    
    def _on_asistencia_filters_changed(self):
        """Maneja cambios en los filtros de asistencia (tipo de cuota y rol) y refresca los gráficos."""
        try:
            # No es necesario almacenar explícitamente los filtros porque
            # _get_filtered_weekly_attendance los lee directamente de los selectores.
            # Simplemente reprocesamos los gráficos para aplicar los filtros actuales.
            self.update_charts()
        except Exception as e:
            logging.error(f"Error al cambiar filtros de asistencia: {e}")
    
    def _handle_chart_alert(self, alert_type, message):
        """Maneja las alertas emitidas por los gráficos"""
        # Log de la alerta
        logging.warning(f"Alerta del dashboard: {alert_type} - {message}")
        
        # Mostrar notificación en la interfaz (opcional)
        # Aquí se podría integrar con un sistema de notificaciones global
        print(f"ALERTA DASHBOARD: {message}")
    
    def calculate_advanced_kpis(self) -> dict:
        """Calcula KPIs avanzados para el dashboard ejecutivo"""
        try:
            from datetime import datetime, timedelta
            
            # Inicializar valores por defecto
            kpis = {
                'retention_rate': 0.0,
                'churn_rate': 0.0,
                'avg_attendance': 0.0,
                'peak_hour': 'N/A',
                'active_classes': 0,
                'avg_capacity': 0.0,
                'revenue_growth': 0.0,
                'avg_ltv': 0.0,
                'payment_rate': 0.0,
                'weekly_active_users': 0,
                'high_value_memberships': 0,
                'avg_daily_attendance': 0.0,
                'inactive_30d': 0
            }
            
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                # 1. Tasa de retención (socios que pagaron este mes y el anterior)
                mes_actual = datetime.now().month
                año_actual = datetime.now().year
                mes_anterior = mes_actual - 1 if mes_actual > 1 else 12
                año_anterior = año_actual if mes_actual > 1 else año_actual - 1
                
                cursor.execute(
                    "SELECT COUNT(DISTINCT usuario_id) as count FROM pagos WHERE EXTRACT(MONTH FROM fecha_pago) = %s AND EXTRACT(YEAR FROM fecha_pago) = %s",
                    (mes_anterior, año_anterior)
                )
                result = cursor.fetchone()
                pagos_mes_anterior = result['count'] if result else 0
                
                cursor.execute(
                    "SELECT COUNT(DISTINCT p1.usuario_id) as count FROM pagos p1 "
                    "INNER JOIN pagos p2 ON p1.usuario_id = p2.usuario_id "
                    "WHERE EXTRACT(MONTH FROM p1.fecha_pago) = %s AND EXTRACT(YEAR FROM p1.fecha_pago) = %s AND EXTRACT(MONTH FROM p2.fecha_pago) = %s AND EXTRACT(YEAR FROM p2.fecha_pago) = %s",
                    (mes_actual, año_actual, mes_anterior, año_anterior)
                )
                result = cursor.fetchone()
                socios_retenidos = result['count'] if result else 0
                
                if pagos_mes_anterior > 0:
                    kpis['retention_rate'] = (socios_retenidos / pagos_mes_anterior) * 100
                
                # 2. Tasa de abandono
                kpis['churn_rate'] = 100 - kpis['retention_rate']
                
                # 3. Asistencia promedio por socio
                cursor.execute(
                    "SELECT COUNT(*) as count FROM asistencias WHERE fecha >= CURRENT_DATE - INTERVAL '30 days'"
                )
                result = cursor.fetchone()
                total_asistencias = result['count'] if result else 0
                
                cursor.execute(
                    "SELECT COUNT(*) as count FROM usuarios WHERE activo = true AND rol IN ('socio','profesor')"
                )
                result = cursor.fetchone()
                socios_activos = result['count'] if result and result['count'] > 0 else 1
                
                kpis['avg_attendance'] = total_asistencias / socios_activos
                
                # 4. Hora pico (calculado desde datos reales de asistencias)
                cursor.execute(
                    "SELECT EXTRACT(HOUR FROM hora_registro) as hora, COUNT(*) as total "
                    "FROM asistencias WHERE fecha >= CURRENT_DATE - INTERVAL '30 days' "
                    "GROUP BY EXTRACT(HOUR FROM hora_registro) ORDER BY total DESC LIMIT 1"
                )
                peak_result = cursor.fetchone()
                if peak_result and peak_result['hora']:
                    hora_pico = int(peak_result['hora'])
                    kpis['peak_hour'] = f'{hora_pico:02d}:00-{hora_pico+1:02d}:00'
                else:
                    kpis['peak_hour'] = 'Sin datos'
                
                # 5. Clases activas (solo clases marcadas como activas)
                cursor.execute("SELECT COUNT(*) as count FROM clases WHERE activa = TRUE")
                result = cursor.fetchone()
                kpis['active_classes'] = result['count'] if result else 0
                
                # 6. Ocupación promedio de clases
                cursor.execute(
                    "SELECT AVG(CAST(inscritos AS FLOAT) / CAST(cupo_maximo AS FLOAT)) * 100 as avg_capacity "
                    "FROM (SELECT ch.cupo_maximo, COUNT(cu.usuario_id) as inscritos "
                    "FROM clases_horarios ch LEFT JOIN clase_usuarios cu ON ch.id = cu.clase_horario_id "
                    "GROUP BY ch.id) as ocupacion WHERE cupo_maximo > 0"
                )
                result = cursor.fetchone()
                kpis['avg_capacity'] = result['avg_capacity'] if result and result['avg_capacity'] else 0.0
                
                # 7. Crecimiento de ingresos
                cursor.execute(
                    "SELECT SUM(monto) as total FROM pagos WHERE EXTRACT(MONTH FROM fecha_pago) = %s AND EXTRACT(YEAR FROM fecha_pago) = %s",
                    (mes_actual, año_actual)
                )
                result = cursor.fetchone()
                ingresos_actual = result['total'] if result and result['total'] else 0
                
                cursor.execute(
                    "SELECT SUM(monto) as total FROM pagos WHERE EXTRACT(MONTH FROM fecha_pago) = %s AND EXTRACT(YEAR FROM fecha_pago) = %s",
                    (mes_anterior, año_anterior)
                )
                result = cursor.fetchone()
                ingresos_anterior = result['total'] if result and result['total'] and result['total'] > 0 else 1
                
                if ingresos_anterior > 0 and ingresos_anterior != 1:
                    kpis['revenue_growth'] = ((ingresos_actual - ingresos_anterior) / ingresos_anterior) * 100
                else:
                    kpis['revenue_growth'] = 0.0
                
                # 8. LTV promedio (valor de vida del cliente - estimado)
                cursor.execute(
                    "SELECT AVG(total_pagado) as avg_ltv FROM ("
                    "SELECT usuario_id, SUM(monto) as total_pagado FROM pagos GROUP BY usuario_id"
                    ") as ltv_data"
                )
                result = cursor.fetchone()
                kpis['avg_ltv'] = result['avg_ltv'] if result and result['avg_ltv'] else 0.0
                
                # 9. Tasa de pago del mes (socios activos que ya pagaron este mes)
                cursor.execute(
                    "SELECT COUNT(*) as count FROM usuarios WHERE activo = true AND rol IN ('socio','profesor')"
                )
                result = cursor.fetchone()
                socios_activos_total = result['count'] if result else 0
                
                cursor.execute(
                    "SELECT COUNT(DISTINCT usuario_id) as pagaron FROM pagos WHERE EXTRACT(MONTH FROM fecha_pago) = %s AND EXTRACT(YEAR FROM fecha_pago) = %s",
                    (mes_actual, año_actual)
                )
                result = cursor.fetchone()
                pagaron_mes = result['pagaron'] if result else 0
                
                kpis['payment_rate'] = ((pagaron_mes / socios_activos_total) * 100) if socios_activos_total > 0 else 0.0
                
                # 10. Socios con asistencia en últimos 7 días (distintos)
                cursor.execute(
                    "SELECT COUNT(DISTINCT usuario_id) as count FROM asistencias WHERE fecha >= CURRENT_DATE - INTERVAL '7 days'"
                )
                result = cursor.fetchone()
                kpis['weekly_active_users'] = result['count'] if result else 0
                
                # Inactivos (30d): socios activos sin asistencias en los últimos 30 días
                cursor.execute("SELECT COUNT(*) as total FROM usuarios WHERE activo = TRUE AND rol IN ('socio','profesor')")
                total_activos_row = cursor.fetchone()
                total_activos = total_activos_row['total'] if total_activos_row and 'total' in total_activos_row else 0
                cursor.execute(
                    "SELECT COUNT(DISTINCT a.usuario_id) as count FROM asistencias a "
                    "JOIN usuarios u ON u.id = a.usuario_id "
                    "WHERE u.activo = TRUE AND u.rol IN ('socio','profesor') AND a.fecha >= CURRENT_DATE - INTERVAL '30 days'"
                )
                result = cursor.fetchone()
                asistentes_30d = result['count'] if result and 'count' in result else 0
                kpis['inactive_30d'] = max(0, total_activos - asistentes_30d)
                
                # Equipos activos (equipos disponibles y en funcionamiento)
                try:
                    cursor.execute("SELECT to_regclass('public.equipos') AS tbl")
                    tbl = cursor.fetchone()
                    if not tbl or not tbl.get('tbl'):
                        kpis['active_equipment'] = 0
                    else:
                        cursor.execute(
                            "SELECT COUNT(*) as count FROM equipos WHERE estado = 'activo' OR estado = 'disponible'"
                        )
                        result = cursor.fetchone()
                        kpis['active_equipment'] = result['count'] if result and result['count'] else 0
                except Exception as e:
                    logging.warning(f"Tabla 'equipos' no disponible o inaccesible, estableciendo 0. Detalle: {e}")
                    kpis['active_equipment'] = 0
                
                # Satisfacción (puntuación promedio de profesores)
                cursor.execute(
                    "SELECT AVG(puntuacion) as promedio FROM profesor_evaluaciones "
                    "WHERE fecha_evaluacion >= CURRENT_DATE - INTERVAL '90 days'"
                )
                result = cursor.fetchone()
                kpis['satisfaction_score'] = result['promedio'] if result and result['promedio'] else 0.0
                
                # 11. Usuarios con cuotas de alto valor (>= $8000)
                # Unificar criterio de join con tipos_cuota usando nombre
                cursor.execute(
                    "SELECT COUNT(*) as total FROM usuarios u "
                    "INNER JOIN tipos_cuota tc ON u.tipo_cuota = tc.nombre "
                    "WHERE u.activo = true AND tc.precio >= 8000"
                )
                result = cursor.fetchone()
                kpis['high_value_memberships'] = result['total'] if result and result['total'] else 0
                
                # 12. Clases más populares (promedio de asistencia)
                cursor.execute(
                    "SELECT AVG(asistentes) as promedio FROM ("
                    "SELECT COUNT(*) as asistentes FROM asistencias "
                    "WHERE fecha >= CURRENT_DATE - INTERVAL '30 days' "
                    "GROUP BY fecha::date"
                    ") as daily_attendance"
                )
                result = cursor.fetchone()
                kpis['avg_daily_attendance'] = result['promedio'] if result and result['promedio'] else 0.0
                
            return kpis
            
        except Exception as e:
            logging.error(f"Error calculando KPIs avanzados: {str(e)} - Tipo: {type(e).__name__}")
            return {
                'retention_rate': 0.0, 'churn_rate': 0.0, 'avg_attendance': 0.0,
                'peak_hour': 'N/A', 'active_classes': 0, 'avg_capacity': 0.0,
                'revenue_growth': 0.0, 'avg_ltv': 0.0, 'active_equipment': 0,
                'satisfaction_score': 0.0, 'high_value_memberships': 0, 'avg_daily_attendance': 0.0
            }

    def setup_layout(self):
        # Reorganizar layout para evitar scroll horizontal y dar más espacio a los gráficos
        activity_group = QGroupBox("Actividad Reciente")
        activity_layout = QVBoxLayout(activity_group)
        activity_layout.addWidget(self.activity_list)
        activity_group.setMinimumHeight(200)
        
        # Gráficos principales a lo ancho
        self.grid_layout.addWidget(self.income_chart, 6, 0, 1, 4)
        self.grid_layout.addWidget(self.nuevos_socios_chart, 7, 0, 1, 4)
        
        # Gráficos de distribución en paralelo
        self.grid_layout.addWidget(self.user_dist_chart, 8, 0, 1, 2)
        self.grid_layout.addWidget(self.membership_dist_chart, 8, 2, 1, 2)
        
        # Filtros en panel propio (mejor estética/ubicación)
        if hasattr(self, 'asistencia_filters_group'):
            self.grid_layout.addWidget(self.asistencia_filters_group, 9, 0, 1, 4)
        else:
            self.grid_layout.addWidget(self.asistencia_controls_widget, 9, 0, 1, 4)
        
        # Gráfico de asistencia a lo ancho
        self.grid_layout.addWidget(self.asistencia_semanal_chart, 10, 0, 1, 4)
        
        # Nuevos gráficos agregados
        self.grid_layout.addWidget(self.arpu_mensual_chart, 11, 0, 1, 4)
        self.grid_layout.addWidget(self.morosos_mensuales_chart, 12, 0, 1, 4)
        
        # Actividad a lo ancho
        self.grid_layout.addWidget(activity_group, 13, 0, 1, 4)
        
        # Estirar 4 columnas
        for i in range(4): self.grid_layout.setColumnStretch(i, 1)
        # Estirar filas de gráficos/actividad
        for r in (6, 7, 10, 11, 12, 13): self.grid_layout.setRowStretch(r, 1)

    def clear_grid_layout_kpis(self):
        for i in reversed(range(self.grid_layout.count())): 
            item = self.grid_layout.itemAt(i)
            if item and isinstance(item.widget(), MetricCard):
                widget = item.widget()
                if widget:
                    widget.setParent(None)
                    widget.deleteLater()

    def actualizar_reportes(self):
        # Si la carga inicial está diferida y el widget no es visible aún, evitar trabajo pesado
        try:
            if getattr(self, '_defer_initial_load', False) and not getattr(self, '_initialized', False):
                # Solo continuar si el widget está visible (pestaña abierta)
                if not self.isVisible():
                    return
        except Exception:
            # Si no existen atributos aún, continuar normalmente
            pass
        try:
            QApplication.setOverrideCursor(Qt.CursorShape.WaitCursor)
            self.update_kpis()
            # Lazy-loading: solo cargar gráficos si el usuario lo permite
            try:
                if getattr(self, 'auto_load_charts_checkbox', None) and self.auto_load_charts_checkbox.isChecked():
                    self.update_charts()
                else:
                    # Mostrar aviso ligero en lista de actividad
                    if hasattr(self, 'activity_list'):
                        self.activity_list.addItem("Consejo: habilita 'Auto cargar gráficos' para ver visualizaciones.")
            except Exception as _e:
                logging.debug(f"Aviso de lazy-loading en gráficos: {_e}")
            self.update_activity_feed()
        except Exception as e:
            logging.exception("Error al actualizar los reportes del dashboard.")
            # Si el widget fue destruido, evitar mostrar el QMessageBox
            try:
                if hasattr(self, 'isVisible') and self.isVisible():
                    QMessageBox.critical(self, "Error de Reportes", f"No se pudieron actualizar los reportes: {e}")
            except RuntimeError:
                # Ignorar si el objeto Qt ya fue eliminado
                pass
        finally:
            QApplication.restoreOverrideCursor()

    def update_kpis(self):
        # Cargar KPIs en hilo para no bloquear la UI
        # Detener hilo previo si está corriendo
        try:
            self._stop_thread(getattr(self, '_kpis_thread', None))
        except Exception:
            pass
        self.clear_grid_layout_kpis()
        loading = QLabel("Cargando KPIs...")
        loading.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.grid_layout.addWidget(loading, 0, 0, 1, 4)
        try:
            self._kpis_thread = DashboardWorkerThread(self.db_manager, self.payment_manager, task='kpis')
            self._kpis_thread.data_ready.connect(self._on_dashboard_data_ready)
            self._kpis_thread.error.connect(self._on_dashboard_error)
            self._kpis_thread.finished.connect(lambda: setattr(self, '_kpis_thread', None))
            self._kpis_thread.start()
        except Exception as e:
            logging.error(f"No se pudo iniciar hilo de KPIs: {e}")

    def update_charts(self):
        # Cargar gráficos en un hilo para no bloquear la UI
        # Detener hilo previo si está corriendo
        try:
            self._stop_thread(getattr(self, '_charts_thread', None))
        except Exception:
            pass
        try:
            dias_periodo = getattr(self, 'dias_periodo', 90)
            # Capturar filtros actuales de asistencia para aplicar en cálculo semanal
            tipo_id = None
            rol_filter = 'todos'
            try:
                if hasattr(self, 'asistencia_tipo_selector'):
                    tipo_id = self.asistencia_tipo_selector.currentData()
                if hasattr(self, 'asistencia_rol_selector'):
                    rol_filter = self.asistencia_rol_selector.currentData() or 'todos'
            except Exception:
                pass
            self._charts_thread = DashboardWorkerThread(
                self.db_manager,
                self.payment_manager,
                task='charts',
                params={'dias_periodo': dias_periodo, 'tipo_id': tipo_id, 'rol_filter': rol_filter}
            )
            self._charts_thread.data_ready.connect(self._on_dashboard_data_ready)
            self._charts_thread.error.connect(self._on_dashboard_error)
            self._charts_thread.finished.connect(lambda: setattr(self, '_charts_thread', None))
            self._charts_thread.start()
        except Exception as e:
            logging.error(f"No se pudo iniciar hilo de gráficos: {e}")

    def _on_dashboard_data_ready(self, task: str, data: dict):
        try:
            if task == 'kpis':
                self._render_kpis(data)
            elif task == 'charts':
                self._render_charts(data)
        except Exception as e:
            logging.error(f"Error al renderizar datos de dashboard ({task}): {e}")

    def _on_dashboard_error(self, message: str):
        logging.error(f"Error en hilo de dashboard: {message}")

    def _render_kpis(self, data: dict):
        # Renderizar KPIs con datos precargados
        self.clear_grid_layout_kpis()
        kpis = data.get('general_kpis', {}) or {}
        arpu = data.get('arpu', 0) or 0
        morosos = data.get('morosos', 0) or 0
        advanced_kpis = data.get('advanced_kpis', {}) or {}

        main_cards_data = [
            ("assets/users.png", "Socios Activos", f"{kpis.get('total_activos', 0)}", "Usuarios con acceso al gimnasio"),
            ("assets/money.png", "Ingresos del Mes", f"${kpis.get('ingresos_mes_actual', 0):,.0f}", f"Facturación en {datetime.now().strftime('%B')}"),
            ("assets/attendance.png", "Asistencias Hoy", f"{kpis.get('asistencias_hoy', 0)}", "Socios que asistieron en el día")
        ]

        secondary_cards_data = [
            ("assets/new_user.png", "Nuevos Socios (30d)", f"{kpis.get('nuevos_30_dias', 0)}", "Registros en el último mes"),
            ("assets/payment_rate.png", "ARPU (Mes Actual)", f"${arpu:,.0f}", "Ingreso promedio por socio activo"),
            ("assets/pending.png", "Socios Morosos", f"{morosos}", "Activos que aún no pagaron este mes"),
            ("assets/pending.png", "Inactivos (30d)", f"{advanced_kpis.get('inactive_30d', 0)}", "Activos sin asistencia en 30 días")
        ]

        advanced_cards_data = [
            ("assets/retention_rate.png", "Tasa de Retención", f"{advanced_kpis.get('retention_rate', 0.0):.1f}%", "Socios que renovaron este mes"),
            ("assets/dropout_rate.png", "Tasa de Abandono", f"{advanced_kpis.get('churn_rate', 0.0):.1f}%", "Socios que se dieron de baja"),
            ("assets/average_attendance.png", "Asistencia Promedio", f"{advanced_kpis.get('avg_attendance', 0.0):.1f}", "Visitas promedio por socio/mes"),
            ("assets/rush_hour.png", "Hora Pico", f"{advanced_kpis.get('peak_hour', 'N/A')}", "Horario de mayor asistencia"),
            ("assets/classes.png", "Clases Activas", f"{advanced_kpis.get('active_classes', 0)}", "Clases programadas esta semana"),
            ("assets/average_occupancy.png", "Ocupación Promedio", f"{advanced_kpis.get('avg_capacity', 0.0):.1f}%", "Capacidad utilizada en clases"),
            ("assets/payment_rate.png", "Crecimiento Ingresos", f"{advanced_kpis.get('revenue_growth', 0.0):+.1f}%", "Variación vs mes anterior"),
            ("assets/routines.png", "LTV Promedio", f"${advanced_kpis.get('avg_ltv', 0.0):,.0f}", "Valor de vida del cliente"),
            ("assets/payment_rate.png", "Pago Mes (Completado)", f"{advanced_kpis.get('payment_rate', 0.0):.1f}%", "Socios activos que ya pagaron"),
            ("assets/attendance.png", "Socios con Asistencia (7d)", f"{advanced_kpis.get('weekly_active_users', 0)}", "Distintos socios con asistencia"),
            ("assets/student_icon.png", "Cuotas Alto Valor", f"{advanced_kpis.get('high_value_memberships', 0)}", "Usuarios con cuotas >= $8000"),
            ("assets/daily_attendance.png", "Asistencia Diaria", f"{advanced_kpis.get('avg_daily_attendance', 0.0):.1f}", "Promedio de asistentes por día")
        ]

        for i, data_item in enumerate(main_cards_data):
            self.grid_layout.addWidget(MetricCard(*data_item), 0, i, 1, 1)
        for i, data_item in enumerate(secondary_cards_data):
            self.grid_layout.addWidget(MetricCard(*data_item), 1, i, 1, 1)
        for i, data_item in enumerate(advanced_cards_data[:4]):
            self.grid_layout.addWidget(MetricCard(*data_item), 2, i, 1, 1)
        for i, data_item in enumerate(advanced_cards_data[4:8]):
            self.grid_layout.addWidget(MetricCard(*data_item), 3, i, 1, 1)
        for i, data_item in enumerate(advanced_cards_data[8:12]):
            self.grid_layout.addWidget(MetricCard(*data_item), 4, i, 1, 1)

    def _render_charts(self, data: dict):
        # Ingresos últimos 12 meses
        income_data = data.get('income_12m', {}) or {}
        _income_values = list(income_data.values())
        _income_avg = (sum(_income_values) / len(_income_values)) if _income_values else 0
        income_thresholds = {
            'critical_low': int(max(0, _income_avg * 0.6)),
            'warning_low': int(max(0, _income_avg * 0.85)),
            'target': int(max(0, _income_avg * 1.15))
        }
        self.income_chart.set_alert_thresholds(
            critical_low=income_thresholds['critical_low'],
            warning_low=income_thresholds['warning_low'],
            target=income_thresholds['target']
        )
        self.income_chart.plot_bar_chart(
            list(income_data.keys()),
            list(income_data.values()),
            title="Ingresos Mensuales (Últimos 12 Meses)",
            y_label="Ingresos ($)",
            is_currency=True,
            alert_thresholds=income_thresholds
        )

        # Nuevos socios
        nuevos_data = data.get('nuevos_por_mes', {}) or {}
        _nuevos_values = list(nuevos_data.values())
        _nuevos_avg = (sum(_nuevos_values) / len(_nuevos_values)) if _nuevos_values else 0
        nuevos_thresholds = {
            'critical_low': int(max(0, _nuevos_avg * 0.5)),
            'warning_low': int(max(0, _nuevos_avg * 0.75)),
            'target': int(max(0, _nuevos_avg * 1.25))
        }
        self.nuevos_socios_chart.set_alert_thresholds(
            critical_low=nuevos_thresholds['critical_low'],
            warning_low=nuevos_thresholds['warning_low'],
            target=nuevos_thresholds['target']
        )
        self.nuevos_socios_chart.plot_bar_chart(
            list(nuevos_data.keys()),
            list(nuevos_data.values()),
            title="Captación de Nuevos Socios por Mes",
            y_label="Nuevos Socios",
            alert_thresholds=nuevos_thresholds
        )

        # Asistencia semanal
        dias_periodo = getattr(self, 'dias_periodo', 90)
        asistencia_semanal_data = data.get('asistencia_semana', {}) or {}
        _asis_values = list(asistencia_semanal_data.values())
        _asis_avg = (sum(_asis_values) / len(_asis_values)) if _asis_values else 0
        asistencia_thresholds = {
            'critical_low': int(max(1, _asis_avg * 0.5)),
            'warning_low': int(max(1, _asis_avg * 0.75)),
            'target': int(max(1, _asis_avg * 1.2))
        }
        self.asistencia_semanal_chart.set_alert_thresholds(
            critical_low=asistencia_thresholds['critical_low'],
            warning_low=asistencia_thresholds['warning_low'],
            target=asistencia_thresholds['target']
        )
        self.asistencia_semanal_chart.plot_bar_chart(
            list(asistencia_semanal_data.keys()),
            list(asistencia_semanal_data.values()),
            title=f"Asistencias por Día de la Semana (Últimos {dias_periodo} Días)",
            y_label="Asistencias",
            alert_thresholds=asistencia_thresholds
        )

        # Distribuciones
        user_dist_data = data.get('user_dist', {}) or {}
        self.user_dist_chart.plot_pie_chart(
            list(user_dist_data.values()),
            list(user_dist_data.keys()),
            title="Distribución de Socios",
            colors=['#A3BE8C', '#BF616A']
        )

        membership_data = data.get('membership_dist', {}) or {}
        self.membership_dist_chart.plot_pie_chart(
            list(membership_data.values()),
            list(membership_data.keys()),
            title="Distribución por Tipo de Cuota",
            colors=['#5E81AC', '#81A1C1', '#A3BE8C', '#EBCB8B', '#B48EAD', '#88C0D0']
        )

        # ARPU mensual (últimos 12 meses)
        arpu_data = data.get('arpu_12m', {'labels': [], 'values': []})
        self.arpu_mensual_chart.plot_bar_chart(
            arpu_data.get('labels', []),
            arpu_data.get('values', []),
            title="ARPU Mensual (Últimos 12 Meses)",
            y_label="ARPU ($)",
            is_currency=True
        )

    def _stop_thread(self, thread: QThread | None):
        """Detiene un hilo de forma segura para evitar 'QThread: Destroyed...'"""
        try:
            if thread and thread.isRunning():
                thread.requestInterruption()
                thread.quit()
                # Espera limitada para no congelar UI
                thread.wait(2000)
        except Exception:
            pass

    def closeEvent(self, event):
        """Asegura limpieza de hilos al cerrar el widget."""
        try:
            self._stop_thread(getattr(self, '_kpis_thread', None))
            self._stop_thread(getattr(self, '_charts_thread', None))
            self._kpis_thread = None
            self._charts_thread = None
        except Exception:
            pass
        super().closeEvent(event)

        # Morosos por mes (últimos 12 meses)
        try:
            cache_key_m = "morosos_mensuales_12m"
            cached_m = None
            try:
                cached_m = getattr(self.db_manager, 'cache_manager', None) and self.db_manager.cache_manager.get(cache_key_m)
            except Exception:
                cached_m = None
            if cached_m and isinstance(cached_m, dict) and cached_m.get('labels') and cached_m.get('values'):
                labels = cached_m['labels']
                morosos_values = cached_m['values']
            else:
                labels = []
                morosos_values = []
                today = datetime.now()
                with self.db_manager.get_connection_context() as conn:
                    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                    # Endurecer lectura con helper centralizado
                    try:
                        self.db_manager._apply_readonly_timeouts(cursor)
                    except Exception:
                        pass
                    for i in range(11, -1, -1):
                        month = ((today.month - i - 1) % 12) + 1
                        year = today.year + ((today.month - i - 1) // 12)
                        labels.append(f"{month:02d}/{year}")
                        # Si no hay pagos registrados en ese mes/año, considerar 0 morosos (no hay datos)
                        cursor.execute(
                            "SELECT COUNT(*) AS pagos_mes FROM pagos WHERE EXTRACT(MONTH FROM fecha_pago) = %s AND EXTRACT(YEAR FROM fecha_pago) = %s",
                            (month, year)
                        )
                        res_cnt = cursor.fetchone()
                        pagos_mes = int(res_cnt['pagos_mes']) if res_cnt and res_cnt['pagos_mes'] is not None else 0
                        if pagos_mes == 0:
                            morosos_values.append(0)
                        else:
                            usuarios_morosos_mes = self.db_manager.obtener_usuarios_morosos_por_mes(month, year)
                            morosos_values.append(len(usuarios_morosos_mes) if usuarios_morosos_mes else 0)
                try:
                    if getattr(self.db_manager, 'cache_manager', None):
                        self.db_manager.cache_manager.set(cache_key_m, {'labels': labels, 'values': morosos_values}, ttl_ms=5*60*1000)
                except Exception:
                    pass
            self.morosos_mensuales_chart.plot_bar_chart(
                labels,
                morosos_values,
                title="Usuarios Morosos por Mes (Últimos 12 Meses)",
                y_label="Morosos"
            )
        except Exception as e:
            logging.warning(f"No se pudo generar el gráfico de morosos mensuales: {e}")

    def update_activity_feed(self):
        self.activity_list.clear()
        try:
            actividades = self.db_manager.obtener_actividad_reciente(limit=15, current_user_id=self.get_current_user_id())
            if not actividades:
                self.activity_list.addItem("No hay actividad reciente.")
                return

            # Normalizar a dict y asegurar orden descendente
            from datetime import datetime as _dt
            normalizadas = []
            for a in actividades:
                if isinstance(a, str):
                    normalizadas.append({
                        'actividad': a,
                        'actor': 'Desconocido',
                        'fecha': None,
                        'tipo': 'Otros'
                    })
                else:
                    actividad = a.get('actividad', 'Actividad')
                    actor = a.get('actor', 'Desconocido')
                    fecha = a.get('fecha')
                    tipo = a.get('tipo')
                    if not tipo:
                        act_lower = actividad.lower()
                        if 'pago' in act_lower:
                            tipo = 'Pago'
                        elif 'socio' in act_lower:
                            tipo = 'Socio'
                        elif 'asistencia' in act_lower:
                            tipo = 'Asistencia'
                        elif 'método de pago' in act_lower or 'metodo de pago' in act_lower:
                            tipo = 'Método de Pago'
                        elif 'concepto de pago' in act_lower:
                            tipo = 'Concepto de Pago'
                        elif 'usuario' in act_lower:
                            tipo = 'Usuario'
                        else:
                            tipo = 'Otros'
                    normalizadas.append({'actividad': actividad, 'actor': actor, 'fecha': fecha, 'tipo': tipo})

            def _parse_dt(val):
                if not val:
                    return _dt.min
                try:
                    return val if hasattr(val, 'strftime') else _dt.fromisoformat(str(val))
                except Exception:
                    return _dt.min

            normalizadas.sort(key=lambda x: _parse_dt(x['fecha']), reverse=True)

            # Agrupar por tipo
            grupos = {}
            for item in normalizadas:
                grupos.setdefault(item['tipo'], []).append(item)

            # Mapa de iconos por grupo
            iconos = {
                'Pago': '💰',
                'Socio': '🧑‍🤝‍🧑',
                'Usuario': '👤',
                'Asistencia': '📅',
                'Método de Pago': '💳',
                'Concepto de Pago': '🏷️',
                'Alerta': '🚨',
                'Otros': '🗂️'
            }

            # Insertar encabezados y elementos por grupo
            from PyQt6.QtGui import QFont
            for tipo in ['Pago', 'Socio', 'Usuario', 'Asistencia', 'Método de Pago', 'Concepto de Pago', 'Alerta', 'Otros']:
                if tipo in grupos:
                    if tipo == 'Pago':
                        header_title = 'Pagos'
                    elif tipo == 'Socio':
                        header_title = 'Nuevos Socios'
                    elif tipo == 'Usuario':
                        header_title = 'Usuarios'
                    elif tipo == 'Asistencia':
                        header_title = 'Asistencias'
                    elif tipo == 'Método de Pago':
                        header_title = 'Métodos de Pago'
                    elif tipo == 'Concepto de Pago':
                        header_title = 'Conceptos de Pago'
                    elif tipo == 'Alerta':
                        header_title = 'Alertas del Sistema'
                    else:
                        header_title = 'Otros'
                    header_text = f"{iconos.get(tipo, '•')} {header_title}"
                    header_item = QListWidgetItem(header_text)
                    font = QFont()
                    font.setBold(True)
                    header_item.setFont(font)
                    header_item.setFlags(Qt.ItemFlag.ItemIsEnabled)
                    self.activity_list.addItem(header_item)

                    for it in grupos[tipo]:
                        fecha = it.get('fecha')
                        try:
                            if hasattr(fecha, 'strftime'):
                                fecha_str = fecha.strftime('%d/%m/%Y %H:%M')
                            elif fecha:
                                fecha_dt = _dt.fromisoformat(str(fecha))
                                fecha_str = fecha_dt.strftime('%d/%m/%Y %H:%M')
                            else:
                                fecha_str = ''
                        except Exception:
                            fecha_str = str(fecha) if fecha else ''

                        texto = f"{iconos.get(tipo, '•')} {it['actividad']} • por {it.get('actor', 'Desconocido')} • {fecha_str}"
                        self.activity_list.addItem(texto)
        except Exception as e:
            logging.error(f"Error al cargar el feed de actividad: {e}")
            self.activity_list.addItem("Error al cargar datos.")
    
    def exportar_dashboard_pdf(self):
        """Exporta el dashboard completo a PDF"""
        try:
            # Diálogo para seleccionar ubicación del archivo
            fecha_actual = datetime.now().strftime("%Y%m%d_%H%M%S")
            nombre_archivo = f"dashboard_{get_gym_name('Gimnasio').replace(' ', '_').lower()}_{fecha_actual}.pdf"
            
            archivo_pdf, _ = QFileDialog.getSaveFileName(
                self,
                "Exportar Dashboard a PDF",
                nombre_archivo,
                "Archivos PDF (*.pdf)"
            )
            
            if not archivo_pdf:
                return
            
            QApplication.setOverrideCursor(Qt.CursorShape.WaitCursor)
            
            # Crear el documento PDF
            doc = SimpleDocTemplate(
                archivo_pdf,
                pagesize=A4,
                rightMargin=72,
                leftMargin=72,
                topMargin=72,
                bottomMargin=18
            )
            
            # Estilos
            styles = getSampleStyleSheet()
            title_style = ParagraphStyle(
                'CustomTitle',
                parent=styles['Heading1'],
                fontSize=24,
                spaceAfter=30,
                alignment=TA_CENTER,
                textColor=colors.HexColor('#2E3440')
            )
            
            subtitle_style = ParagraphStyle(
                'CustomSubtitle',
                parent=styles['Heading2'],
                fontSize=16,
                spaceAfter=20,
                alignment=TA_LEFT,
                textColor=colors.HexColor('#3B4252')
            )
            
            normal_style = ParagraphStyle(
                'CustomNormal',
                parent=styles['Normal'],
                fontSize=10,
                spaceAfter=12,
                alignment=TA_LEFT
            )
            
            # Contenido del PDF
            story = []
            
            # Título principal
            story.append(Paragraph(f"Dashboard Ejecutivo - {get_gym_name('Gimnasio')}", title_style))
            story.append(Paragraph(f"Reporte generado el {datetime.now().strftime('%d/%m/%Y a las %H:%M')}", normal_style))
            story.append(Spacer(1, 20))
            
            # Obtener datos actualizados
            kpis = self.db_manager.obtener_kpis_generales()
            arpu, morosos = self.db_manager.obtener_arpu_y_morosos_mes_actual()
            advanced_kpis = self.calculate_advanced_kpis()
            
            
            story.append(Paragraph("Indicadores Clave de Rendimiento (KPIs)", subtitle_style))
            
            # Tabla de KPIs principales
            kpi_data = [
                ['Métrica', 'Valor', 'Descripción'],
                ['Socios Activos', f"{kpis.get('total_activos', 0)}", 'Usuarios con acceso al gimnasio'],
                ['Ingresos del Mes', f"${kpis.get('ingresos_mes_actual', 0):,.0f}", f"Facturación en {datetime.now().strftime('%B')}"],
                ['Asistencias Hoy', f"{kpis.get('asistencias_hoy', 0)}", 'Socios que asistieron en el día'],
                ['Nuevos Socios (30d)', f"{kpis.get('nuevos_30_dias', 0)}", 'Registros en el último mes'],
                ['ARPU (Mes Actual)', f"${arpu:,.0f}", 'Ingreso promedio por socio activo'],
                ['Socios Morosos', f"{morosos}", 'Activos que aún no pagaron este mes']
            ]
            
            kpi_table = Table(kpi_data, colWidths=[2*inch, 1.5*inch, 3*inch])
            kpi_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#5E81AC')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 12),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
                ('FONTSIZE', (0, 1), (-1, -1), 10),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.lightgrey])
            ]))
            
            story.append(kpi_table)
            story.append(Spacer(1, 20))
            
            
            story.append(Paragraph("Métricas Avanzadas", subtitle_style))
            
            advanced_kpi_data = [
                ['Métrica Avanzada', 'Valor', 'Descripción'],
                ['Tasa de Retención', f"{advanced_kpis['retention_rate']:.1f}%", 'Socios que renovaron este mes'],
                ['Tasa de Abandono', f"{advanced_kpis['churn_rate']:.1f}%", 'Socios que se dieron de baja'],
                ['Asistencia Promedio', f"{advanced_kpis['avg_attendance']:.1f}", 'Visitas promedio por socio/mes'],
                ['Hora Pico', f"{advanced_kpis['peak_hour']}", 'Horario de mayor asistencia'],
                ['Clases Activas', f"{advanced_kpis['active_classes']}", 'Clases programadas esta semana'],
                ['Ocupación Promedio', f"{advanced_kpis['avg_capacity']:.1f}%", 'Capacidad utilizada en clases'],
                ['Crecimiento Ingresos', f"{advanced_kpis['revenue_growth']:+.1f}%", 'Variación vs mes anterior'],
                ['LTV Promedio', f"${advanced_kpis['avg_ltv']:,.0f}", 'Valor de vida del cliente'],
                ['Pago Mes (Completado)', f"{advanced_kpis['payment_rate']:.1f}%", 'Socios activos que ya pagaron'],
                ['Socios con Asistencia (7d)', f"{advanced_kpis['weekly_active_users']}", 'Distintos socios con asistencia'],
                ['Cuotas Alto Valor', f"{advanced_kpis['high_value_memberships']}", 'Usuarios con cuotas >= $8000'],
                ['Asistencia Diaria', f"{advanced_kpis['avg_daily_attendance']:.1f}", 'Promedio de asistentes por día']
            ]
            
            advanced_table = Table(advanced_kpi_data, colWidths=[2*inch, 1.5*inch, 3*inch])
            advanced_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#81A1C1')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 12),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
                ('FONTSIZE', (0, 1), (-1, -1), 9),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.lightgrey])
            ]))
            
            story.append(advanced_table)
            story.append(PageBreak())
            
            
            story.append(Paragraph("Análisis Gráfico", subtitle_style))
            
            # Generar gráficos como imágenes
            chart_images = self._generate_chart_images()
            
            for chart_title, chart_image in chart_images.items():
                if chart_image:
                    story.append(Paragraph(chart_title, normal_style))
                    story.append(chart_image)
                    story.append(Spacer(1, 15))
            
            
            story.append(PageBreak())
            story.append(Paragraph("Actividad Reciente", subtitle_style))
            
            try:
                actividades = self.db_manager.obtener_actividad_reciente(limit=20, current_user_id=self.get_current_user_id())
                if actividades:
                    from datetime import datetime as _dt
                    lines = []
                    for a in actividades[:15]:
                        if isinstance(a, str):
                            lines.append(f"• {a}")
                        else:
                            actividad = a.get('actividad', 'Actividad')
                            actor = a.get('actor', 'Desconocido')
                            fecha = a.get('fecha')
                            try:
                                if hasattr(fecha, 'strftime'):
                                    fecha_str = fecha.strftime('%d/%m/%Y %H:%M')
                                elif fecha:
                                    fecha_dt = _dt.fromisoformat(str(fecha))
                                    fecha_str = fecha_dt.strftime('%d/%m/%Y %H:%M')
                                else:
                                    fecha_str = ''
                            except Exception:
                                fecha_str = str(fecha) if fecha else ''
                            lines.append(f"• {actividad} — por {actor} — {fecha_str}")
                    activity_text = "\n".join(lines)
                    story.append(Paragraph(activity_text, normal_style))
                else:
                    story.append(Paragraph("No hay actividad reciente registrada.", normal_style))
            except Exception as e:
                story.append(Paragraph(f"Error al cargar actividad reciente: {e}", normal_style))
            
            # Pie de página con información adicional
            story.append(Spacer(1, 30))
            story.append(Paragraph("---", normal_style))
            story.append(Paragraph(
                f"Reporte generado automáticamente por el Sistema de Gestión {get_gym_name('Gimnasio')}<br/>"
                f"Fecha y hora: {datetime.now().strftime('%d/%m/%Y a las %H:%M:%S')}<br/>"
                f"Total de páginas: Variable según contenido",
                normal_style
            ))
            
            # Construir el PDF
            doc.build(story)
            
            QApplication.restoreOverrideCursor()
            
            QMessageBox.information(
                self,
                "Exportación Exitosa",
                f"Dashboard exportado exitosamente a:\n{archivo_pdf}"
            )
            
        except Exception as e:
            QApplication.restoreOverrideCursor()
            logging.error(f"Error al exportar dashboard a PDF: {e}")
            QMessageBox.critical(
                self,
                "Error de Exportación",
                f"No se pudo exportar el dashboard a PDF:\n{str(e)}"
            )
    
    def _generate_chart_images(self):
        """Genera imágenes de los gráficos para incluir en el PDF"""
        chart_images = {}
        
        try:
            # Configuración común para gráficos
            plt.style.use('default')
            fig_size = (8, 5)
            dpi = 100
            
            # Gráfico de ingresos mensuales
            income_data = self.payment_manager.obtener_ingresos_ultimos_12_meses()
            if income_data:
                fig, ax = plt.subplots(figsize=fig_size, dpi=dpi)
                months = list(income_data.keys())
                values = list(income_data.values())
                
                bars = ax.bar(months, values, color='#5E81AC', alpha=0.8)
                ax.set_title('Ingresos Mensuales (Últimos 12 Meses)', fontsize=14, fontweight='bold')
                ax.set_ylabel('Ingresos ($)', fontsize=12)
                ax.tick_params(axis='x', rotation=45)
                
                # Añadir valores en las barras
                for bar in bars:
                    height = bar.get_height()
                    ax.text(bar.get_x() + bar.get_width()/2., height,
                           f'${height:,.0f}', ha='center', va='bottom', fontsize=9)
                
                plt.tight_layout()
                
                # Guardar como imagen temporal
                img_buffer = BytesIO()
                plt.savefig(img_buffer, format='png', bbox_inches='tight')
                img_buffer.seek(0)
                
                chart_images['Ingresos Mensuales'] = RLImage(img_buffer, width=6*inch, height=3.5*inch)
                plt.close()
            
            # Gráfico de nuevos socios
            nuevos_data = self.db_manager.obtener_nuevos_usuarios_por_mes()
            if nuevos_data:
                fig, ax = plt.subplots(figsize=fig_size, dpi=dpi)
                months = list(nuevos_data.keys())
                values = list(nuevos_data.values())
                
                bars = ax.bar(months, values, color='#81A1C1', alpha=0.8)
                ax.set_title('Captación de Nuevos Socios por Mes', fontsize=14, fontweight='bold')
                ax.set_ylabel('Nuevos Socios', fontsize=12)
                ax.tick_params(axis='x', rotation=45)
                
                # Añadir valores en las barras
                for bar in bars:
                    height = bar.get_height()
                    ax.text(bar.get_x() + bar.get_width()/2., height,
                           f'{int(height)}', ha='center', va='bottom', fontsize=9)
                
                plt.tight_layout()
                
                img_buffer = BytesIO()
                plt.savefig(img_buffer, format='png', bbox_inches='tight')
                img_buffer.seek(0)
                
                chart_images['Nuevos Socios por Mes'] = RLImage(img_buffer, width=6*inch, height=3.5*inch)
                plt.close()
            
            # Gráfico de asistencias por día de la semana (con filtros)
            dias_periodo = getattr(self, 'dias_periodo', 90)
            asistencia_data = self._get_filtered_weekly_attendance(dias_periodo)
            if asistencia_data:
                fig, ax = plt.subplots(figsize=fig_size, dpi=dpi)
                days = list(asistencia_data.keys())
                values = list(asistencia_data.values())
                
                bars = ax.bar(days, values, color='#A3BE8C', alpha=0.8)
                ax.set_title(f'Asistencias por Día de la Semana (Últimos {dias_periodo} Días)', fontsize=14, fontweight='bold')
                ax.set_ylabel('Asistencias', fontsize=12)
                
                # Añadir valores en las barras
                for bar in bars:
                    height = bar.get_height()
                    ax.text(bar.get_x() + bar.get_width()/2., height,
                           f'{int(height)}', ha='center', va='bottom', fontsize=9)
                
                plt.tight_layout()
                
                img_buffer = BytesIO()
                plt.savefig(img_buffer, format='png', bbox_inches='tight')
                img_buffer.seek(0)
                
                chart_images['Asistencias por Día de la Semana'] = RLImage(img_buffer, width=6*inch, height=3.5*inch)
                plt.close()
            
            # Frecuencia mensual por tipo de cuota
            try:
                with self.db_manager.get_connection_context() as conn:
                    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                    mes_actual = datetime.now().month
                    año_actual = datetime.now().year
                    cursor.execute(
                        "SELECT COALESCE(tc.nombre, 'Sin tipo') AS tipo, COUNT(a.id) AS asistencias, COUNT(DISTINCT u.id) AS socios \n"
                        "FROM usuarios u \n"
                        "LEFT JOIN tipos_cuota tc ON u.tipo_cuota = tc.id::text \n"
                        "LEFT JOIN asistencias a ON a.usuario_id = u.id AND EXTRACT(MONTH FROM a.fecha) = %s AND EXTRACT(YEAR FROM a.fecha) = %s \n"
                        "WHERE u.activo = true AND u.rol IN ('socio','profesor') \n"
                        "GROUP BY tipo ORDER BY tipo",
                        (mes_actual, año_actual)
                    )
                    rows = cursor.fetchall() or []
                    if rows:
                        fig, ax = plt.subplots(figsize=fig_size, dpi=dpi)
                        tipos = [r['tipo'] for r in rows]
                        avgs = [ (r['asistencias'] / r['socios']) if r['socios'] else 0 for r in rows ]
                        bars = ax.bar(tipos, avgs, color='#B48EAD', alpha=0.85)
                        ax.set_title('Frecuencia Mensual por Tipo de Cuota', fontsize=14, fontweight='bold')
                        ax.set_ylabel('Asistencias promedio por socio', fontsize=12)
                        ax.tick_params(axis='x', rotation=30)
                        for bar in bars:
                            height = bar.get_height()
                            ax.text(bar.get_x() + bar.get_width()/2., height,
                                    f"{height:.1f}", ha='center', va='bottom', fontsize=9)
                        plt.tight_layout()
                        img_buffer = BytesIO()
                        plt.savefig(img_buffer, format='png', bbox_inches='tight')
                        img_buffer.seek(0)
                        chart_images['Frecuencia Mensual por Tipo de Cuota'] = RLImage(img_buffer, width=6*inch, height=3.5*inch)
                        plt.close()
            except Exception as e:
                logging.error(f"Error generando gráfico de frecuencia mensual por tipo de cuota: {e}")
            
            # Conversión por cohortes (últimos 6 meses)
            try:
                with self.db_manager.get_connection_context() as conn:
                    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                    mes_actual = datetime.now().month
                    año_actual = datetime.now().year
                    cursor.execute(
                        "SELECT DATE_TRUNC('month', u.fecha_registro)::date AS cohort_mes, \n"
                        "COUNT(DISTINCT u.id) AS cohort_size, \n"
                        "COUNT(DISTINCT u.id) FILTER (WHERE p.usuario_id IS NOT NULL) AS pagaron \n"
                        "FROM usuarios u \n"
                        "LEFT JOIN pagos p ON p.usuario_id = u.id AND EXTRACT(MONTH FROM p.fecha_pago) = %s AND EXTRACT(YEAR FROM p.fecha_pago) = %s \n"
                        "WHERE u.rol IN ('socio','profesor') AND u.activo = true AND u.fecha_registro >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '6 months' \n"
                        "GROUP BY cohort_mes ORDER BY cohort_mes",
                        (mes_actual, año_actual)
                    )
                    rows = cursor.fetchall() or []
                    if rows:
                        fig, ax = plt.subplots(figsize=fig_size, dpi=dpi)
                        labels = [datetime.strftime(r['cohort_mes'], '%Y-%m') for r in rows]
                        rates = [ ((r['pagaron'] / r['cohort_size']) * 100) if r['cohort_size'] else 0 for r in rows ]
                        bars = ax.bar(labels, rates, color='#88C0D0', alpha=0.85)
                        ax.set_title('Conversión a Pago del Mes por Cohortes (últ. 6 meses)', fontsize=14, fontweight='bold')
                        ax.set_ylabel('Conversión (%)', fontsize=12)
                        ax.set_ylim(0, 100)
                        ax.tick_params(axis='x', rotation=30)
                        for bar in bars:
                            height = bar.get_height()
                            ax.text(bar.get_x() + bar.get_width()/2., height,
                                    f"{height:.1f}%", ha='center', va='bottom', fontsize=9)
                        plt.tight_layout()
                        img_buffer = BytesIO()
                        plt.savefig(img_buffer, format='png', bbox_inches='tight')
                        img_buffer.seek(0)
                        chart_images['Conversión a Pago por Cohortes'] = RLImage(img_buffer, width=6*inch, height=3.5*inch)
                        plt.close()
            except Exception as e:
                logging.error(f"Error generando gráfico de conversión por cohortes: {e}")
        except Exception as e:
            logging.error(f"Error generando imágenes de gráficos: {e}")
        return chart_images
    
    def open_operational_reports(self):
        """Abre la ventana de reportes operativos avanzados"""
        try:
            from PyQt6.QtWidgets import QDialog, QVBoxLayout
            
            # Crear diálogo para reportes operativos
            dialog = QDialog(self)
            dialog.setWindowTitle(f"Reportes Operativos Avanzados - {get_gym_name('Gimnasio')}")
            dialog.setModal(True)
            dialog.resize(1200, 800)
            
            # Layout del diálogo
            layout = QVBoxLayout(dialog)
            layout.setContentsMargins(10, 10, 10, 10)
            
            # Agregar el widget de reportes operativos
            operational_widget = OperationalReportsWidget(self.db_manager, dialog)
            layout.addWidget(operational_widget)
            
            # Mostrar el diálogo
            dialog.exec()
            
        except Exception as e:
            logging.error(f"Error abriendo reportes operativos: {e}")
            QMessageBox.critical(
                self,
                "Error",
                f"No se pudieron abrir los reportes operativos:\n{str(e)}"
            )


    def _get_filtered_weekly_attendance(self, dias_periodo: int) -> dict:
        """Devuelve un dict con asistencias por día de la semana aplicando filtros de tipo de cuota y rol."""
        try:
            # Si no hay filtros, usar método optimizado de la base de datos
            tipo_id = None
            rol_filter = "todos"
            if hasattr(self, 'asistencia_tipo_selector'):
                tipo_id = self.asistencia_tipo_selector.currentData()
            if hasattr(self, 'asistencia_rol_selector'):
                rol_filter = self.asistencia_rol_selector.currentData() or "todos"
            
            if (tipo_id is None) and (rol_filter == "todos"):
                return self.db_manager.obtener_asistencias_por_dia_semana(dias=dias_periodo) or {}
            
            # Construir conjunto de usuarios según filtros usando readonly_session
            usuarios_filtrados_ids = set()
            try:
                from psycopg2.extras import RealDictCursor
                with self.db_manager.readonly_session(lock_ms=800, statement_ms=2000, idle_s=2, seqscan_off=True) as conn:
                    cursor = conn.cursor(cursor_factory=RealDictCursor)
                    condiciones = ["activo = TRUE"]
                    params = []
                    if rol_filter == "profesor":
                        condiciones.append("LOWER(rol) = 'profesor'")
                    elif rol_filter == "socio":
                        condiciones.append("LOWER(rol) NOT IN ('profesor','dueño')")
                    if tipo_id is not None:
                        condiciones.append("CAST(tipo_cuota AS TEXT) = %s")
                        params.append(str(tipo_id))
                    where = " AND ".join(condiciones) if condiciones else "TRUE"
                    cursor.execute(f"SELECT id FROM usuarios WHERE {where}", tuple(params))
                    for row in cursor.fetchall() or []:
                        usuarios_filtrados_ids.add(row.get('id'))
            except Exception as e:
                logging.warning(f"Fallo al filtrar usuarios vía SQL, se continuará sin datos: {e}")
            
            # Si no hay usuarios que cumplan filtros, devolver estructura vacía
            dias_semana = ['Lunes','Martes','Miércoles','Jueves','Viernes','Sábado','Domingo']
            resultado = {d: 0 for d in dias_semana}
            if not usuarios_filtrados_ids:
                return resultado
            
            # Obtener asistencias desde la fecha límite y filtrar por usuarios seleccionados
            fecha_inicio = datetime.now().date() - timedelta(days=dias_periodo)
            asistencias = self.db_manager.obtener_asistencias_por_fecha_limite(fecha_inicio) or []
            
            for a in asistencias:
                uid = a.get('usuario_id') if isinstance(a, dict) else (a[1] if isinstance(a, (list, tuple)) else None)
                if uid in usuarios_filtrados_ids:
                    fecha = a.get('fecha') if isinstance(a, dict) else (a[4] if isinstance(a, (list, tuple)) else None)
                    try:
                        # Normalizar fecha a date
                        if hasattr(fecha, 'date'):
                            fecha_dt = fecha.date() if hasattr(fecha, 'date') else fecha
                        else:
                            fecha_dt = fecha
                        # Calcular nombre del día en español
                        weekday = fecha_dt.weekday() if fecha_dt else None  # 0=Lunes
                        if weekday is not None:
                            resultado[dias_semana[weekday]] += 1
                    except Exception:
                        continue
            return resultado
        except Exception as e:
            logging.error(f"Error calculando asistencia semanal filtrada: {e}")
            return {}

    def get_current_user_id(self):
        """Obtiene el ID del usuario actualmente logueado"""
        try:
            main_window = self.window()
            if hasattr(main_window, 'logged_in_user') and main_window.logged_in_user:
                u = main_window.logged_in_user
                # Soportar tanto dict (flujo de login de profesor) como objeto con atributo id
                if isinstance(u, dict):
                    uid = u.get('usuario_id') or u.get('id')
                    if uid is not None:
                        try:
                            return int(uid)
                        except Exception:
                            return uid  # Si no es convertible, devolver tal cual
                elif hasattr(u, 'id'):
                    try:
                        return int(getattr(u, 'id', 1))
                    except Exception:
                        return getattr(u, 'id', 1)
            # Fallback al dueño/ID por defecto cuando no hay usuario
            return 1
        except Exception as e:
            logging.warning(f"Error al obtener ID de usuario actual: {e}")
            return 1

