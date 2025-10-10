from PyQt6.QtWidgets import QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel, QFrame
from PyQt6.QtCore import QTimer, pyqtSignal
from PyQt6.QtGui import QFont
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.backends.backend_qtagg import NavigationToolbar2QT as NavigationToolbar
from matplotlib.figure import Figure
import matplotlib.ticker as mticker
from matplotlib.ticker import MaxNLocator
import matplotlib.pyplot as plt
import numpy as np

class MplChartWidget(QFrame):
    # Señales para alertas automáticas
    alert_triggered = pyqtSignal(str, str)  # tipo_alerta, mensaje
    
    def __init__(self, parent=None, figsize=(10, 4), enable_toolbar=True, enable_alerts=True):
        super().__init__(parent)
        
        # MEJORA: Aplicar estilo de borde similar a los KPIs
        self.setFrameShape(QFrame.Shape.StyledPanel)
        self.setFrameShadow(QFrame.Shadow.Raised)
        self.setProperty("class", "metric-card")
        
        # CORRECCIÓN: Intentar obtener colores del sistema de branding desde el inicio
        # Si no está disponible, usar colores neutros por defecto
        self._initialize_colors_from_branding()
        
        # Si no se pudieron obtener colores del branding, usar valores por defecto
        if not hasattr(self, 'BG_COLOR') or not self.BG_COLOR:
            self.BG_COLOR = '#2E3440'  # Gris oscuro neutro
            self.TEXT_COLOR = '#ECEFF4'  # Blanco suave
            self.GRID_COLOR = '#4C566A'  # Gris medio
            self.BAR_COLOR = '#81A1C1'  # Azul suave neutro
            self.ALERT_COLOR = '#BF616A'  # Rojo
            self.WARNING_COLOR = '#EBCB8B'  # Amarillo
            self.SUCCESS_COLOR = '#A3BE8C'  # Verde
            self.PIE_COLORS = [
                '#88C0D0',  # Azul claro
                '#B48EAD',  # Púrpura
                '#A3BE8C',  # Verde
                '#EBCB8B',  # Amarillo
                '#81A1C1'   # Azul
            ]
        
        # CORRECCIÓN: No establecer estilo fijo, se configurará dinámicamente
        self.figure = Figure(figsize=figsize)
        # CORRECCIÓN: Establecer fondo inicial de la figura
        self.figure.patch.set_facecolor(self.BG_COLOR)
        
        # Layout principal
        main_layout = QVBoxLayout()
        main_layout.setContentsMargins(0, 0, 0, 0)
        
        # Toolbar de navegación (opcional)
        if enable_toolbar:
            self.toolbar = NavigationToolbar(FigureCanvas(self.figure), self)
            self.toolbar.setObjectName("chart_toolbar")
            main_layout.addWidget(self.toolbar)
        
        # Canvas del gráfico
        self.canvas = FigureCanvas(self.figure)
        main_layout.addWidget(self.canvas)
        
        # Panel de alertas
        if enable_alerts:
            self.alert_panel = QLabel()
            self.alert_panel.setObjectName("chart_alert_panel")
            self.alert_panel.hide()
            main_layout.addWidget(self.alert_panel)
        
        self.setLayout(main_layout)
        
        # Variables de estado
        self.annotation = None
        self.is_currency = False
        self.x_labels_full = []
        self.y_values_full = []
        self.enable_alerts = enable_alerts
        self.alert_thresholds = {}
        # Estado de hover/pick para barras
        self._current_bars = None
        self._current_ax = None
        self._hover_last_index = None
        self._hover_prev_style = None
        
        # Timers
        self.annotation_timer = QTimer(self)
        self.annotation_timer.setSingleShot(True)
        self.annotation_timer.timeout.connect(self._hide_annotation)
        
        self.alert_timer = QTimer(self)
        self.alert_timer.setSingleShot(True)
        self.alert_timer.timeout.connect(self._hide_alert)
        
        # Conectar eventos
        self.figure.canvas.mpl_connect('pick_event', self._on_pick)
        self.figure.canvas.mpl_connect('motion_notify_event', self._on_hover)

        # Conectar limpieza segura al destruir el widget
        try:
            self.destroyed.connect(self._cleanup_on_destroy)
        except Exception:
            pass

    def _cleanup_on_destroy(self):
        """Detiene timers y marca el widget como destruido para evitar dibujados tardíos."""
        try:
            self._destroyed = True
            if hasattr(self, 'annotation_timer') and self.annotation_timer is not None:
                self.annotation_timer.stop()
            if hasattr(self, 'alert_timer') and self.alert_timer is not None:
                self.alert_timer.stop()
        except Exception:
            pass

    def _safe_draw(self):
        """Dibuja de forma segura evitando errores si el canvas ya fue destruido."""
        try:
            if getattr(self, '_destroyed', False):
                return
            if hasattr(self, 'canvas') and self.canvas is not None:
                self.canvas.draw()
        except RuntimeError as re:
            if 'has been deleted' in str(re):
                return
            raise

    def _safe_draw_idle(self):
        """Solicita redibujado de forma segura evitando errores si el canvas ya fue destruido."""
        try:
            if getattr(self, '_destroyed', False):
                return
            if hasattr(self, 'canvas') and self.canvas is not None:
                self.canvas.draw_idle()
        except RuntimeError as re:
            if 'has been deleted' in str(re):
                return
            raise

    def plot_bar_chart(self, x_labels, y_values, title="Gráfico", y_label="", is_currency=False, alert_thresholds=None):
        self.x_labels_full = x_labels
        self.y_values_full = y_values
        self.is_currency = is_currency
        self.alert_thresholds = alert_thresholds or {}
        # Guardar parámetros para regeneración completa
        self._current_title = title
        self._current_y_label = y_label
        self._pie_data = None  # Limpiar datos de gráfico de pastel
        self._hide_annotation(force_draw=False)
        
        # MEJORA: Asegurar que los colores estén actualizados antes de plotear
        self._update_matplotlib_style()
        
        self.figure.clear()
        # CORRECCIÓN: Establecer fondo de la figura principal ANTES de crear subplots
        self.figure.patch.set_facecolor(self.BG_COLOR)
        # CORRECCIÓN: Aumentar espaciado superior para evitar corte de títulos
        bottom_margin = 0.28 if len(x_labels) > 8 else 0.2
        self.figure.subplots_adjust(bottom=bottom_margin, left=0.15, top=0.85, right=0.95)
        ax = self.figure.add_subplot(111, facecolor=self.BG_COLOR)
        
        # --- Lógica para acortar etiquetas de fecha (ej: 2024-06 -> Jun-24) ---
        meses_es = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]
        try:
            def format_label(label_str):
                parts = label_str.split('-')
                if len(parts) == 2 and parts[0].isdigit() and len(parts[0]) == 4: # Formato AAAA-MM
                    return f"{meses_es[int(parts[1]) - 1]}-{parts[0][-2:]}"
                elif len(parts) == 2 and parts[1].isdigit() and len(parts[1]) == 2: # Formato MM-AA
                    return f"{meses_es[int(parts[0]) - 1]}-{parts[1]}"
                return label_str # Devuelve original si no coincide
            short_labels = [format_label(label) for label in x_labels]
        except (ValueError, IndexError):
            short_labels = x_labels

        x_pos = range(len(x_labels))
        
        # MEJORA: Determinar colores basados en umbrales de alerta con fallback a colores del branding
        bar_colors = self._get_bar_colors(y_values)
        
        # CORRECCIÓN: Si no hay umbrales definidos, usar colores del branding
        if not self.alert_thresholds:
            if len(y_values) > 1:
                # Crear gradiente de colores basado en el color principal
                bar_colors = [self.BAR_COLOR] + self.PIE_COLORS[:len(y_values)-1]
                bar_colors = bar_colors[:len(y_values)]  # Ajustar al número de barras
            else:
                bar_colors = [self.BAR_COLOR]
        
        bars = ax.bar(x_pos, y_values, color=bar_colors, zorder=2, picker=5, alpha=0.8)
        # Guardar referencias para hover/pick
        self._current_bars = bars
        self._current_ax = ax
        
        # Añadir líneas de umbral si están definidas
        self._add_threshold_lines(ax, y_values)
        
        # Añadir tendencia si hay suficientes datos
        if len(y_values) >= 3:
            self._add_trend_line(ax, x_pos, y_values)
        
        # CORRECCIÓN: Aumentar padding del título para evitar cortes
        ax.set_title(title, fontsize=14, weight='bold', color=self.TEXT_COLOR, pad=25)
        ax.set_ylabel(y_label, fontsize=10, color=self.TEXT_COLOR)
        ax.tick_params(axis='y', colors=self.TEXT_COLOR)
        
        # --- ORDEN CORREGIDO PARA ELIMINAR WARNING ---
        ax.set_xticks(x_pos) # 1. Establecer las posiciones de los ticks
        ax.set_xticklabels(short_labels, rotation=45, ha='right', color=self.TEXT_COLOR) # 2. Establecer las etiquetas para esas posiciones
        
        ax.grid(axis='y', linestyle='--', alpha=0.3, zorder=1, color=self.GRID_COLOR)
        for spine in ax.spines.values(): spine.set_color(self.GRID_COLOR)
        
        if is_currency:
            ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: f'${x:,.0f}'))
        else:
            ax.yaxis.set_major_locator(MaxNLocator(integer=True))
            ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: f'{int(x)}'))
        
        # Verificar alertas
        if self.enable_alerts:
            self._check_alerts(y_values, title)
        
        # Dibujo seguro: evitar llamar a canvas si el widget fue destruido
        self._safe_draw()
    
    def set_alert_thresholds(self, **thresholds):
        """Configura umbrales para alertas automáticas"""
        self.alert_thresholds.update(thresholds)
    
    def _get_bar_colors(self, y_values):
        """Determina colores de barras basado en umbrales"""
        colors = []
        for value in y_values:
            if 'critical_low' in self.alert_thresholds and value <= self.alert_thresholds['critical_low']:
                colors.append(self.ALERT_COLOR)
            elif 'warning_low' in self.alert_thresholds and value <= self.alert_thresholds['warning_low']:
                colors.append(self.WARNING_COLOR)
            elif 'target' in self.alert_thresholds and value >= self.alert_thresholds['target']:
                colors.append(self.SUCCESS_COLOR)
            else:
                colors.append(self.BAR_COLOR)
        return colors
    
    def _add_threshold_lines(self, ax, y_values):
        """Añade líneas de umbral al gráfico"""
        if 'critical_low' in self.alert_thresholds:
            ax.axhline(y=self.alert_thresholds['critical_low'], color=self.ALERT_COLOR, 
                      linestyle='--', alpha=0.7, linewidth=2, label='Crítico')
        
        if 'warning_low' in self.alert_thresholds:
            ax.axhline(y=self.alert_thresholds['warning_low'], color=self.WARNING_COLOR, 
                      linestyle='--', alpha=0.7, linewidth=2, label='Advertencia')
        
        if 'target' in self.alert_thresholds:
            ax.axhline(y=self.alert_thresholds['target'], color=self.SUCCESS_COLOR, 
                      linestyle='--', alpha=0.7, linewidth=2, label='Objetivo')
        
        # Añadir leyenda si hay umbrales
        if self.alert_thresholds:
            ax.legend(loc='upper right', framealpha=0.8, facecolor=self.BG_COLOR, 
                     edgecolor=self.GRID_COLOR, labelcolor=self.TEXT_COLOR)
    
    def _add_trend_line(self, ax, x_pos, y_values):
        """Añade línea de tendencia al gráfico"""
        try:
            z = np.polyfit(x_pos, y_values, 1)
            p = np.poly1d(z)
            trend_color = self.SUCCESS_COLOR if z[0] > 0 else self.ALERT_COLOR
            ax.plot(x_pos, p(x_pos), color=trend_color, linestyle='-', alpha=0.6, linewidth=2)
        except:
            pass  # Ignorar errores en cálculo de tendencia
    
    def _check_alerts(self, y_values, chart_title):
        """Verifica y emite alertas basadas en los datos"""
        if not y_values:
            return
        
        latest_value = y_values[-1]
        avg_value = sum(y_values) / len(y_values)
        
        # Verificar umbrales críticos
        if 'critical_low' in self.alert_thresholds and latest_value <= self.alert_thresholds['critical_low']:
            message = f"CRÍTICO: {chart_title} - Valor actual ({latest_value}) por debajo del umbral crítico"
            self._show_alert(message, 'critical')
            self.alert_triggered.emit('critical', message)
        
        # Verificar tendencia negativa
        elif len(y_values) >= 3:
            recent_trend = (y_values[-1] - y_values[-3]) / y_values[-3] * 100 if y_values[-3] != 0 else 0
            if recent_trend < -20:  # Caída del 20% o más
                message = f"📉 TENDENCIA: {chart_title} - Caída del {abs(recent_trend):.1f}% en últimos períodos"
                self._show_alert(message, 'warning')
                self.alert_triggered.emit('warning', message)
    
    def _show_alert(self, message, alert_type):
        """Muestra alerta en el panel"""
        if hasattr(self, 'alert_panel'):
            # Usar propiedades para el tipo de alerta en lugar de estilos hardcodeados
            self.alert_panel.setProperty("alertType", alert_type)
            self.alert_panel.setText(message)
            self.alert_panel.show()
            self.alert_timer.start(8000)  # Ocultar después de 8 segundos
    
    def _hide_alert(self):
        """Oculta el panel de alertas"""
        if hasattr(self, 'alert_panel'):
            self.alert_panel.hide()
    
    def _clear_hover_highlight(self):
        """Limpia el resaltado y tooltip del hover actual si existe"""
        try:
            if self._hover_last_index is not None and self._current_bars is not None:
                bar = self._current_bars[self._hover_last_index]
                if self._hover_prev_style is not None:
                    prev_edgecolor, prev_linewidth, prev_alpha = self._hover_prev_style
                    try:
                        bar.set_edgecolor(prev_edgecolor)
                        bar.set_linewidth(prev_linewidth)
                        bar.set_alpha(prev_alpha)
                    except Exception:
                        pass
                else:
                    try:
                        bar.set_edgecolor('none')
                        bar.set_linewidth(0.8)
                        bar.set_alpha(0.8)
                    except Exception:
                        pass
                bar.set_zorder(2)
        except Exception:
            pass
        self._hide_annotation(force_draw=False)
        self._hover_last_index = None
        self._hover_prev_style = None
        # Usar redibujado seguro para evitar errores si el canvas ya fue destruido
        self._safe_draw_idle()
    
    def _on_hover(self, event):
        """Maneja eventos de hover para tooltips mejorados"""
        if event.inaxes is None or self._current_bars is None or not isinstance(self.x_labels_full, (list, tuple)):
            return
        
        # Detectar si el mouse está sobre alguna barra
        hovered_index = None
        hovered_bar = None
        for idx, bar in enumerate(self._current_bars):
            try:
                if bar.contains_point((event.x, event.y)):
                    hovered_index = idx
                    hovered_bar = bar
                    break
            except Exception:
                continue
        
        # Si no está sobre ninguna barra, limpiar estado y salir
        if hovered_index is None:
            if self._hover_last_index is not None:
                self._clear_hover_highlight()
            return
        
        # Si sigue sobre la misma barra, no hacer nada para evitar parpadeos
        if hovered_index == self._hover_last_index:
            return
        
        # Cambió de barra: limpiar anterior y resaltar nueva
        self._clear_hover_highlight()
        self._hover_last_index = hovered_index
        
        # Preparar datos del tooltip
        try:
            original_label = self.x_labels_full[hovered_index]
        except Exception:
            original_label = str(hovered_index)
        
        # Formatear etiqueta de tiempo similar a _on_pick
        try:
            parts = original_label.split('-')
            if len(parts) == 2 and parts[0].isdigit():  # AAAA-MM
                meses_es = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]
                formatted_time = f"{meses_es[int(parts[1]) - 1]}-{parts[0]}"
            else:
                formatted_time = original_label
        except Exception:
            formatted_time = original_label
        
        value = hovered_bar.get_height()
        if self.is_currency:
            formatted_value = f"${value:,.2f}"
        else:
            plural_suffix = "" if int(round(value)) == 1 else "s"
            formatted_value = f"{int(round(value))} Asistencia{plural_suffix}"
        
        text = f"{formatted_time}: {formatted_value}"
        
        # Estilos de tooltip
        tooltip_bg_color = self._get_dynamic_color('alt_background_color', '#434C5E')
        tooltip_border_color = self._get_dynamic_color('primary_color', '#5E81AC')
        
        # Resaltar barra actual (guardar estilo anterior para restaurar)
        try:
            self._hover_prev_style = (
                hovered_bar.get_edgecolor(),
                hovered_bar.get_linewidth(),
                hovered_bar.get_alpha(),
            )
            hovered_bar.set_edgecolor(tooltip_border_color)
            hovered_bar.set_linewidth(2.0)
            hovered_bar.set_alpha(1.0)
            hovered_bar.set_zorder(3)
        except Exception:
            pass
        
        # Crear/actualizar anotación cerca de la parte superior de la barra
        try:
            x = hovered_bar.get_x() + hovered_bar.get_width() / 2.0
            y = value
            # Eliminar anotación previa si existe
            self._hide_annotation(force_draw=False)
            self.annotation = self._current_ax.annotate(
                text,
                xy=(x, y),
                xytext=(0, 12),
                textcoords='offset points',
                ha='center',
                va='bottom',
                fontsize=10,
                fontweight='bold',
                color=self.TEXT_COLOR,
                bbox=dict(boxstyle="round,pad=0.5", fc=tooltip_bg_color, ec=tooltip_border_color, lw=2, alpha=1.0),
                arrowprops=dict(arrowstyle='-|>', color=tooltip_border_color, lw=1.5, alpha=0.9)
            )
        except Exception:
            # Fallback a texto en figura si falla la anotación
            self._hide_annotation(force_draw=False)
            self.annotation = self.figure.text(
                0.5, 0.95, text, ha='center', va='bottom', fontsize=10, fontweight='bold',
                color=self.TEXT_COLOR, bbox=dict(boxstyle="round,pad=0.5", fc=tooltip_bg_color, ec=tooltip_border_color, lw=2, alpha=1)
            )
        
        self._safe_draw_idle()
    
    def plot_pie_chart(self, values, labels, title="Gráfico", colors=None):
        # Guardar datos para regeneración completa
        self._pie_data = (values, labels, title)
        self.x_labels_full = None  # Limpiar datos de gráfico de barras
        self.y_values_full = None
        # Limpiar estado de hover al cambiar a gráfico de pastel
        self._current_bars = None
        self._current_ax = None
        self._hover_last_index = None
        self._hover_prev_style = None
        
        # MEJORA: Asegurar que los colores estén actualizados antes de plotear
        self._update_matplotlib_style()
        
        self.figure.clear()
        # CORRECCIÓN: Establecer fondo de la figura principal ANTES de crear subplots
        self.figure.patch.set_facecolor(self.BG_COLOR)
        # CORRECCIÓN: Ajustar espaciado para gráficos de pastel
        self.figure.subplots_adjust(bottom=0.1, left=0.1, top=0.85, right=0.9)
        ax = self.figure.add_subplot(111, facecolor=self.BG_COLOR)
        # Configurar aspecto sin warnings de matplotlib
        ax.set_aspect('equal', adjustable='box')
        
        # MEJORA: Usar colores del branding actualizados dinámicamente
        pie_colors = colors or self.PIE_COLORS
        
        # CORRECCIÓN: Asegurar que tenemos suficientes colores para todas las secciones
        if len(values) > len(pie_colors):
            # Extender la paleta de colores si es necesario
            extended_colors = pie_colors * ((len(values) // len(pie_colors)) + 1)
            pie_colors = extended_colors[:len(values)]
        
        # CORRECCIÓN: Aumentar padding del título para evitar cortes
        ax.set_title(title, fontsize=12, weight='bold', color=self.TEXT_COLOR, pad=25)
        
        if not values or sum(values) == 0:
            ax.text(0.5, 0.5, "No hay datos", ha='center', va='center', color=self.TEXT_COLOR)
            self._safe_draw()
            return
            
        def make_autopct(values):
            def my_autopct(pct):
                total = sum(values)
                val = int(round(pct*total/100.0))
                if pct < 4: return f'{pct:.1f}%'
                return f'{pct:.1f}%\n({val:d})'
            return my_autopct
        
        wedges, texts, autotexts = ax.pie(values, labels=labels, autopct=make_autopct(values),
                                          startangle=90, colors=pie_colors,
                                          pctdistance=0.8, labeldistance=1.1,
                                          textprops={'color': self.TEXT_COLOR, 'weight': 'bold', 'fontsize': 9})
        
        plt.setp(autotexts, size=8, weight="bold", color=self.BG_COLOR)
        
        self._safe_draw()

    def _on_pick(self, event):
        self._hide_annotation(force_draw=False)
        bar = event.artist
        ax = bar.axes
        height, x_pos = bar.get_height(), bar.get_x() + bar.get_width() / 2.0
        
        # --- Corrección para obtener el índice correcto ---
        bar_index = int(round(x_pos - (bar.get_width() / 2) / len(ax.patches) ))
        if bar_index >= len(self.x_labels_full):
            bar_index = int(x_pos)

        original_label = self.x_labels_full[bar_index]
        
        try:
            parts = original_label.split('-')
            if len(parts) == 2 and parts[0].isdigit(): # AAAA-MM
                meses_es = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]
                formatted_time = f"{meses_es[int(parts[1]) - 1]}-{parts[0]}"
            else: # MM-AA o texto
                formatted_time = original_label
        except:
            formatted_time = original_label
            
        if self.is_currency: formatted_value = f"${height:,.2f}"
        else: plural_suffix = "" if int(height) == 1 else "s"; formatted_value = f"{int(height)} Asistencia{plural_suffix}"
        
        text = f"{formatted_time}: {formatted_value}"
        
        # CORRECCIÓN: Usar colores dinámicos del sistema de branding para el tooltip
        tooltip_bg_color = self._get_dynamic_color('alt_background_color', '#434C5E')
        tooltip_border_color = self._get_dynamic_color('primary_color', '#5E81AC')
        
        self.annotation = self.figure.text(
            0.5, 0.95, text, ha='center', va='bottom', fontsize=10, fontweight='bold',
            color=self.TEXT_COLOR, bbox=dict(boxstyle="round,pad=0.5", fc=tooltip_bg_color, ec=tooltip_border_color, lw=2, alpha=1)
        )
        
        self._safe_draw_idle()
        self.annotation_timer.start(4000)

    def _hide_annotation(self, force_draw=True):
        if self.annotation:
            self.annotation.remove()
            self.annotation = None
        if force_draw:
            self._safe_draw_idle()
    
    def export_chart_data(self):
        """Exporta los datos del gráfico para análisis"""
        return {
            'labels': self.x_labels_full,
            'values': self.y_values_full,
            'is_currency': self.is_currency,
            'thresholds': self.alert_thresholds
        }
    
    def reset_zoom(self):
        """Resetea el zoom del gráfico"""
        if hasattr(self, 'toolbar'):
            self.toolbar.home()
    
    def _initialize_colors_from_branding(self):
        """Inicializa los colores desde el sistema de branding al crear el widget"""
        try:
            # Intentar obtener la configuración de branding desde MainWindow
            from PyQt6.QtWidgets import QApplication
            app = QApplication.instance()
            if app:
                # Buscar la ventana principal que tiene la configuración de branding
                for widget in app.allWidgets():
                    if hasattr(widget, 'branding_config') and widget.branding_config:
                        # Aplicar configuración de branding encontrada
                        self.update_colors_from_branding(widget.branding_config)
                        return
        except Exception as e:
            print(f"No se pudo inicializar colores desde branding: {e}")
    
    def _get_dynamic_color(self, color_key, fallback_color):
        """Obtiene un color del sistema de branding dinámico o usa el fallback"""
        try:
            # Intentar obtener el color del sistema de branding global desde MainWindow
            from PyQt6.QtWidgets import QApplication
            app = QApplication.instance()
            if app:
                # Buscar la ventana principal que tiene la configuración de branding
                for widget in app.allWidgets():
                    if hasattr(widget, 'branding_config') and widget.branding_config:
                        return widget.branding_config.get(color_key, fallback_color)
        except:
            pass
        return fallback_color
    
    def update_colors_from_branding(self, branding_config):
        """Actualiza los colores del gráfico desde la configuración de branding"""
        # Actualizar colores base
        self.BG_COLOR = branding_config.get('background_color', '#2E3440')
        
        # CORRECCIÓN: Calcular color de texto con contraste automático WCAG 2.1
        self.TEXT_COLOR = self._get_contrasting_text_color(self.BG_COLOR, require_aaa=True)
        
        self.GRID_COLOR = branding_config.get('border_color', '#4C566A')
        self.BAR_COLOR = branding_config.get('primary_color', '#5E81AC')
        self.ALERT_COLOR = branding_config.get('error_color', '#BF616A')
        self.WARNING_COLOR = branding_config.get('warning_color', '#EBCB8B')
        self.SUCCESS_COLOR = branding_config.get('success_color', '#A3BE8C')
        self.PIE_COLORS = [
            branding_config.get('accent_color', '#88C0D0'),
            branding_config.get('secondary_color', '#B48EAD'),
            branding_config.get('success_color', '#A3BE8C'),
            branding_config.get('warning_color', '#EBCB8B'),
            branding_config.get('primary_color', '#81A1C1')
        ]
        
        # MEJORA: Actualizar el estilo de matplotlib ANTES de modificar la figura
        self._update_matplotlib_style()
        
        # Actualizar la figura existente completamente
        if hasattr(self, 'figure'):
            # Actualizar fondo de la figura principal
            self.figure.patch.set_facecolor(self.BG_COLOR)
            
            # Actualizar todos los ejes existentes
            for ax in self.figure.get_axes():
                ax.set_facecolor(self.BG_COLOR)
                ax.tick_params(colors=self.TEXT_COLOR, which='both')
                ax.xaxis.label.set_color(self.TEXT_COLOR)
                ax.yaxis.label.set_color(self.TEXT_COLOR)
                ax.title.set_color(self.TEXT_COLOR)
                
                # Actualizar grid si existe
                if ax.get_xgridlines() or ax.get_ygridlines():
                    ax.grid(True, color=self.GRID_COLOR, alpha=0.3)
                
                # Actualizar spines (bordes)
                for spine in ax.spines.values():
                    spine.set_color(self.GRID_COLOR)
                    
                # MEJORA: Actualizar colores de elementos existentes en el gráfico
                for child in ax.get_children():
                    if hasattr(child, 'set_facecolor'):
                        try:
                            # Para barras y otros elementos con facecolor
                            if hasattr(child, 'get_facecolor'):
                                child.set_facecolor(self.BAR_COLOR)
                        except:
                            pass
                    if hasattr(child, 'set_color'):
                        try:
                            # Para líneas y texto
                            child.set_color(self.TEXT_COLOR)
                        except:
                            pass
            
            # CORRECCIÓN: Actualizar también el canvas
            if hasattr(self, 'canvas'):
                self.canvas.figure.patch.set_facecolor(self.BG_COLOR)
            
            # Forzar redibujado completo
            self._safe_draw()
            
        # Actualizar el canvas también
        if hasattr(self, 'canvas'):
            self.canvas.setStyleSheet(f"background-color: {self.BG_COLOR};")
            
        # MEJORA: Forzar redibujado completo y regeneración del gráfico
        self._force_complete_redraw()
    
    def _force_complete_redraw(self):
        """Fuerza un redibujado completo del gráfico con los nuevos colores"""
        try:
            # Si tenemos datos guardados, regenerar el gráfico completamente
            if hasattr(self, 'x_labels_full') and hasattr(self, 'y_values_full') and self.x_labels_full and self.y_values_full:
                # Regenerar gráfico de barras con nuevos colores
                title = getattr(self, '_current_title', 'Gráfico')
                y_label = getattr(self, '_current_y_label', '')
                is_currency = getattr(self, 'is_currency', False)
                alert_thresholds = getattr(self, 'alert_thresholds', {})
                
                self.plot_bar_chart(
                    self.x_labels_full, 
                    self.y_values_full, 
                    title=title, 
                    y_label=y_label, 
                    is_currency=is_currency, 
                    alert_thresholds=alert_thresholds
                )
            elif hasattr(self, '_pie_data') and self._pie_data:
                # Regenerar gráfico de pastel con nuevos colores
                values, labels, title = self._pie_data
                self.plot_pie_chart(values, labels, title)
            else:
                # Si no hay datos guardados, solo forzar redibujado
                if hasattr(self, 'canvas'):
                    self._safe_draw_idle()
                    try:
                        self.canvas.flush_events()
                    except Exception:
                        pass
        except Exception as e:
            print(f"Error en redibujado completo: {e}")
            # Fallback: solo redibujado básico
            if hasattr(self, 'canvas'):
                self._safe_draw_idle()
    
    def _update_matplotlib_style(self):
        """Actualiza el estilo global de matplotlib basándose en los colores actuales"""
        try:
            # Determinar si usar tema oscuro o claro basándose en el color de fondo
            is_dark_theme = self._is_dark_color(self.BG_COLOR)
            
            if is_dark_theme:
                plt.style.use('dark_background')
            else:
                plt.style.use('default')
            
            # MEJORA: Configurar rcParams globales más completos con los colores del tema actual
            plt.rcParams.update({
                'figure.facecolor': self.BG_COLOR,
                'figure.edgecolor': self.BG_COLOR,
                'axes.facecolor': self.BG_COLOR,
                'axes.edgecolor': self.GRID_COLOR,
                'axes.labelcolor': self.TEXT_COLOR,
                'axes.titlecolor': self.TEXT_COLOR,
                'xtick.color': self.TEXT_COLOR,
                'ytick.color': self.TEXT_COLOR,
                'xtick.labelcolor': self.TEXT_COLOR,
                'ytick.labelcolor': self.TEXT_COLOR,
                'text.color': self.TEXT_COLOR,
                'axes.prop_cycle': plt.cycler('color', self.PIE_COLORS),
                'grid.color': self.GRID_COLOR,
                'grid.alpha': 0.3,
                'savefig.facecolor': self.BG_COLOR,
                'savefig.edgecolor': self.BG_COLOR,
                # CORRECCIÓN: Configurar colores de spines
                'axes.spines.left': True,
                'axes.spines.bottom': True,
                'axes.spines.top': False,
                'axes.spines.right': False,
                # MEJORA: Configurar colores de leyenda
                'legend.facecolor': self.BG_COLOR,
                'legend.edgecolor': self.GRID_COLOR,
                'legend.labelcolor': self.TEXT_COLOR
            })
            
        except Exception as e:
            print(f"Error actualizando estilo de matplotlib: {e}")
    
    def _is_dark_color(self, hex_color):
        """Determina si un color es oscuro basado en su luminancia relativa WCAG"""
        try:
            luminance = self._get_relative_luminance(hex_color)
            return luminance < 0.179  # Umbral WCAG más preciso
        except:
            return False
    
    def _get_relative_luminance(self, hex_color):
        """Calcula la luminancia relativa según las pautas WCAG 2.1"""
        try:
            # Remover el # si está presente
            hex_color = hex_color.lstrip('#')
            
            # Convertir a RGB
            r = int(hex_color[0:2], 16) / 255.0
            g = int(hex_color[2:4], 16) / 255.0
            b = int(hex_color[4:6], 16) / 255.0
            
            # Aplicar corrección gamma según WCAG
            def gamma_correct(c):
                if c <= 0.03928:
                    return c / 12.92
                else:
                    return pow((c + 0.055) / 1.055, 2.4)
            
            r = gamma_correct(r)
            g = gamma_correct(g)
            b = gamma_correct(b)
            
            # Calcular luminancia relativa
            return 0.2126 * r + 0.7152 * g + 0.0722 * b
        except:
            return 0.5
    
    def _calculate_contrast_ratio(self, color1, color2):
        """Calcula la relación de contraste entre dos colores según WCAG"""
        try:
            lum1 = self._get_relative_luminance(color1)
            lum2 = self._get_relative_luminance(color2)
            
            # Asegurar que lum1 sea la luminancia más alta
            if lum1 < lum2:
                lum1, lum2 = lum2, lum1
            
            # Calcular relación de contraste
            return (lum1 + 0.05) / (lum2 + 0.05)
        except:
            return 1.0
    
    def _get_contrasting_text_color(self, background_color, require_aaa=False):
        """Obtiene un color de texto que contraste óptimamente con el fondo según WCAG 2.1"""
        try:
            # Calcular contraste con blanco y negro
            contrast_white = self._calculate_contrast_ratio(background_color, "#FFFFFF")
            contrast_black = self._calculate_contrast_ratio(background_color, "#000000")
            
            # WCAG AA requiere al menos 4.5:1 para texto normal
            # WCAG AAA requiere al menos 7:1 para texto normal
            min_contrast = 7.0 if require_aaa else 4.5
            
            # Si ambos cumplen el estándar requerido, elegir el de mayor contraste
            if contrast_white >= min_contrast and contrast_black >= min_contrast:
                return "#FFFFFF" if contrast_white > contrast_black else "#000000"
            
            # Si solo uno cumple el estándar, usar ese
            elif contrast_white >= min_contrast:
                return "#FFFFFF"
            elif contrast_black >= min_contrast:
                return "#000000"
            
            # Si ninguno cumple, generar un color que sí cumpla
            else:
                if self._is_dark_color(background_color):
                    # Fondo oscuro, necesitamos texto más claro
                    return self._generate_high_contrast_light_color(background_color, min_contrast)
                else:
                    # Fondo claro, necesitamos texto más oscuro
                    return self._generate_high_contrast_dark_color(background_color, min_contrast)
        except Exception as e:
            # Fallback seguro
            if self._is_dark_color(background_color):
                return "#FFFFFF"
            else:
                return "#000000"
    
    def _generate_high_contrast_light_color(self, background_color, min_contrast):
        """Genera un color claro que cumpla con el contraste mínimo requerido"""
        try:
            # Comenzar con blanco y oscurecer gradualmente hasta encontrar contraste suficiente
            for lightness in range(255, 127, -5):
                test_color = f"#{lightness:02x}{lightness:02x}{lightness:02x}"
                if self._calculate_contrast_ratio(background_color, test_color) >= min_contrast:
                    return test_color
            # Si no se encuentra, usar gris claro como último recurso
            return "#E0E0E0"
        except:
            return "#FFFFFF"
    
    def _generate_high_contrast_dark_color(self, background_color, min_contrast):
        """Genera un color oscuro que cumpla con el contraste mínimo requerido"""
        try:
            # Comenzar con negro y aclarar gradualmente hasta encontrar contraste suficiente
            for darkness in range(0, 128, 5):
                test_color = f"#{darkness:02x}{darkness:02x}{darkness:02x}"
                if self._calculate_contrast_ratio(background_color, test_color) >= min_contrast:
                    return test_color
            # Si no se encuentra, usar gris oscuro como último recurso
            return "#202020"
        except:
            return "#000000"

