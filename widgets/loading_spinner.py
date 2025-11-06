import math
import time
from PyQt6.QtWidgets import QWidget, QVBoxLayout, QLabel, QApplication, QSizePolicy
from PyQt6.QtCore import Qt, QTimer, pyqtSignal, pyqtProperty, QRectF, QSize
from PyQt6.QtGui import QPainter, QPen, QBrush, QColor, QConicalGradient, QFont, QPainterPath


class LoadingSpinner(QWidget):
    """Spinner de carga animado con soporte para mensajes personalizados"""
    
    # Señales
    started = pyqtSignal()
    stopped = pyqtSignal()
    
    def __init__(self, parent=None, 
                 spinner_type="circular", 
                 size=80, 
                 line_width=6,
                 color=None,
                 bg_color=None,
                 animation_speed=1.0,
                 show_percentage=False):
        super().__init__(parent)
        
        self.spinner_type = spinner_type
        self._size = size
        self.line_width = line_width
        self.color = color or QColor(33, 150, 243)  # Azul Material Design
        self.bg_color = bg_color or QColor(240, 240, 240, 100)
        self.animation_speed = animation_speed
        self.show_percentage = show_percentage
        
        # Estado
        self._is_spinning = False
        self._angle = 0
        self._progress = 0
        self._start_time = 0
        
        # Configuración de la interfaz
        # Evitar truncado horizontal del mensaje: ancho fijo cómodo y altura extra
        self.setFixedWidth(max(size + 20, 460))
        self.setFixedHeight(size + 80)
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
        self.setWindowFlags(Qt.WindowType.FramelessWindowHint | Qt.WindowType.WindowStaysOnTopHint)
        
        # Timer para animación
        self.timer = QTimer(self)
        self.timer.timeout.connect(self._update_animation)
        self.timer.setInterval(int(16 / animation_speed))  # ~60 FPS
        
        # Timer para mensajes de estado
        self.message_timer = QTimer(self)
        self.message_timer.timeout.connect(self._update_message)
        self.message_timer.setInterval(1000)  # Actualizar cada segundo
        
        # Mensajes de estado
        self.status_messages = [
            "Conectando a São Paulo...",
            "Cargando datos...",
            "Procesando información...",
            "Optimizando consultas...",
            "Casi listo..."
        ]
        self.current_message_index = 0
        self.custom_message = ""
        
        # Crear layout para mensaje
        self._setup_ui()
    
    def _setup_ui(self):
        """Configura la interfaz de usuario"""
        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setAlignment(Qt.AlignmentFlag.AlignCenter)
        
        # Label para mensaje de estado
        self.message_label = QLabel(self)
        self.message_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.message_label.setWordWrap(True)
        self.message_label.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        self.message_label.setMinimumWidth(360)
        self.message_label.setStyleSheet("""
            QLabel {
                color: #333;
                font-size: 12px;
                font-weight: 500;
                background-color: rgba(255, 255, 255, 200);
                padding: 8px 12px;
                border-radius: 16px;
                border: 1px solid rgba(0, 0, 0, 0.1);
            }
        """)
        self.message_label.setVisible(False)
        
        layout.addWidget(self.message_label)
    
    def start(self, message="", show_message=True):
        """Inicia la animación del spinner"""
        self._is_spinning = True
        self._start_time = time.time()
        self._angle = 0
        self._progress = 0
        
        if message:
            self.custom_message = message
        else:
            self.custom_message = ""
            self.current_message_index = 0
        
        self.message_label.setVisible(show_message)
        if show_message:
            self._update_message()
            self.message_timer.start()
        
        self.timer.start()
        self.started.emit()
        self.update()
    
    def stop(self):
        """Detiene la animación del spinner"""
        self._is_spinning = False
        self.timer.stop()
        self.message_timer.stop()
        self.message_label.setVisible(False)
        self.stopped.emit()
        self.update()
    
    def set_progress(self, progress):
        """Establece el progreso (0-100) para spinner de progreso"""
        self._progress = max(0, min(100, progress))
        self.update()
    
    def set_message(self, message):
        """Establece un mensaje personalizado"""
        self.custom_message = message
        self.message_label.setText(message)
        self.message_label.setVisible(bool(message))
    
    def _update_animation(self):
        """Actualiza el ángulo de rotación"""
        if self._is_spinning:
            self._angle = (self._angle + 10) % 360
            self.update()
    
    def _update_message(self):
        """Actualiza el mensaje de estado"""
        if self.custom_message:
            self.message_label.setText(self.custom_message)
        else:
            elapsed = int(time.time() - self._start_time)
            if elapsed > 0 and elapsed % 3 == 0:  # Cambiar mensaje cada 3 segundos
                self.current_message_index = (self.current_message_index + 1) % len(self.status_messages)
            
            self.message_label.setText(self.status_messages[self.current_message_index])
    
    def paintEvent(self, event):
        """Dibuja el spinner"""
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        
        # Obtener el rectángulo del centro
        rect = self.rect()
        center_x = rect.width() // 2
        center_y = rect.height() // 2
        radius = min(center_x, center_y) - self.line_width
        
        if self.spinner_type == "circular":
            self._draw_circular_spinner(painter, center_x, center_y, radius)
        elif self.spinner_type == "dots":
            self._draw_dots_spinner(painter, center_x, center_y, radius)
        elif self.spinner_type == "progress":
            self._draw_progress_spinner(painter, center_x, center_y, radius)
    
    def _draw_circular_spinner(self, painter, center_x, center_y, radius):
        """Dibuja spinner circular clásico"""
        if not self._is_spinning:
            return
        
        # Crear gradiente conical para efecto de rotación
        gradient = QConicalGradient(center_x, center_y, -self._angle)
        gradient.setColorAt(0.0, self.color)
        gradient.setColorAt(0.7, self.color.lighter(150))
        gradient.setColorAt(1.0, Qt.GlobalColor.transparent)
        
        # Configurar el pen
        pen = QPen()
        pen.setBrush(gradient)
        pen.setWidth(self.line_width)
        pen.setCapStyle(Qt.PenCapStyle.RoundCap)
        
        painter.setPen(pen)
        
        # Dibujar arco
        start_angle = self._angle * 16  # Qt usa 1/16 de grado
        span_angle = 300 * 16  # 300 grados de arco
        
        painter.drawArc(
            center_x - radius,
            center_y - radius,
            radius * 2,
            radius * 2,
            start_angle,
            span_angle
        )
    
    def _draw_dots_spinner(self, painter, center_x, center_y, radius):
        """Dibuja spinner de puntos"""
        if not self._is_spinning:
            return
        
        num_dots = 8
        dot_radius = self.line_width // 2
        
        for i in range(num_dots):
            angle = (self._angle + i * (360 / num_dots)) % 360
            angle_rad = math.radians(angle)
            
            x = center_x + radius * math.cos(angle_rad)
            y = center_y + radius * math.sin(angle_rad)
            
            # Calcular opacidad basada en la posición
            opacity = max(0.1, 1.0 - (i / num_dots))
            color = QColor(self.color)
            color.setAlphaF(opacity)
            
            painter.setBrush(QBrush(color))
            painter.setPen(Qt.PenStyle.NoPen)
            painter.drawEllipse(QRectF(x - dot_radius, y - dot_radius, dot_radius * 2, dot_radius * 2))
    
    def _draw_progress_spinner(self, painter, center_x, center_y, radius):
        """Dibuja spinner de progreso circular"""
        # Fondo del círculo
        painter.setBrush(QBrush(self.bg_color))
        painter.setPen(Qt.PenStyle.NoPen)
        painter.drawEllipse(center_x - radius, center_y - radius, radius * 2, radius * 2)
        
        # Barra de progreso
        if self._progress > 0:
            pen = QPen(self.color, self.line_width)
            pen.setCapStyle(Qt.PenCapStyle.RoundCap)
            painter.setPen(pen)
            painter.setBrush(Qt.BrushStyle.NoBrush)
            
            # Calcular ángulo basado en el progreso
            span_angle = int((self._progress / 100.0) * 360 * 16)
            painter.drawArc(
                center_x - radius,
                center_y - radius,
                radius * 2,
                radius * 2,
                90 * 16,  # Comenzar desde arriba
                -span_angle  # Sentido horario
            )
        
        # Texto de porcentaje
        if self.show_percentage and self._progress > 0:
            painter.setPen(QPen(self.color.darker(150)))
            painter.setFont(QFont("Arial", 10, QFont.Weight.Bold))
            text = f"{int(self._progress)}%"
            text_rect = painter.boundingRect(self.rect(), Qt.AlignmentFlag.AlignCenter, text)
            painter.drawText(text_rect, Qt.AlignmentFlag.AlignCenter, text)
    
    def sizeHint(self):
        """Tamaño preferido del widget"""
        return QSize(max(self._size + 20, 460), self._size + 80)


class LoadingOverlay(QWidget):
    """Overlay de carga que cubre toda la ventana o widget padre"""
    
    def __init__(self, parent=None, 
                 spinner_type="circular",
                 spinner_size=80,
                 background_opacity=0.7,
                 show_message=True):
        super().__init__(parent)
        
        self.background_opacity = background_opacity
        self.show_message = show_message
        
        # Configurar el overlay
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
        self.setWindowFlags(Qt.WindowType.FramelessWindowHint | Qt.WindowType.WindowStaysOnTopHint)
        
        # Crear spinner
        self.spinner = LoadingSpinner(
            self, 
            spinner_type=spinner_type,
            size=spinner_size,
            show_percentage=True
        )
        
        # Posicionar spinner en el centro
        self.spinner.move(0, 0)
        
        # Conectar señales
        self.spinner.started.connect(self.on_spinner_started)
        self.spinner.stopped.connect(self.on_spinner_stopped)
    
    def showEvent(self, event):
        """Cuando se muestra el overlay"""
        super().showEvent(event)
        if self.parent():
            # Ajustar al tamaño del padre
            self.setGeometry(self.parent().rect())
            # Centrar el spinner
            parent_rect = self.parent().rect()
            spinner_rect = self.spinner.rect()
            self.spinner.move(
                (parent_rect.width() - spinner_rect.width()) // 2,
                (parent_rect.height() - spinner_rect.height()) // 2
            )
    
    def paintEvent(self, event):
        """Dibuja el fondo semi-transparente"""
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        
        # Fondo semi-transparente
        background = QColor(0, 0, 0, int(255 * self.background_opacity))
        painter.fillRect(self.rect(), background)
    
    def start(self, message=""):
        """Inicia el overlay y el spinner"""
        self.show()
        self.raise_()
        self.spinner.start(message, self.show_message)
    
    def stop(self):
        """Detiene el overlay y el spinner"""
        self.spinner.stop()
        self.hide()
    
    def set_progress(self, progress):
        """Establece el progreso del spinner"""
        self.spinner.set_progress(progress)
    
    def set_message(self, message):
        """Establece el mensaje del spinner"""
        self.spinner.set_message(message)
    
    def on_spinner_started(self):
        """Cuando el spinner comienza"""
        pass
    
    def on_spinner_stopped(self):
        """Cuando el spinner se detiene"""
        pass


class DatabaseLoadingManager:
    """Gestor de loading spinners para operaciones de base de datos"""
    
    def __init__(self, parent_widget=None):
        self.parent_widget = parent_widget
        self.overlays = {}
        self.current_overlay = None
        
    def show_loading(self, operation_id="default", message="", 
                    spinner_type="circular", spinner_size=80,
                    background_opacity=0.7, show_message=True):
        """Muestra un overlay de carga"""
        if operation_id in self.overlays:
            self.hide_loading(operation_id)
        
        overlay = LoadingOverlay(
            self.parent_widget,
            spinner_type=spinner_type,
            spinner_size=spinner_size,
            background_opacity=background_opacity,
            show_message=show_message
        )
        
        self.overlays[operation_id] = overlay
        self.current_overlay = overlay
        
        overlay.start(message)
        
        # Asegurar que el overlay esté al frente
        if self.parent_widget:
            overlay.raise_()
            overlay.setFocus()
    
    def hide_loading(self, operation_id="default"):
        """Oculta un overlay de carga"""
        if operation_id in self.overlays:
            overlay = self.overlays[operation_id]
            overlay.stop()
            overlay.deleteLater()
            del self.overlays[operation_id]
            
            if self.current_overlay == overlay:
                self.current_overlay = None
    
    def update_progress(self, operation_id="default", progress=0):
        """Actualiza el progreso de un overlay"""
        if operation_id in self.overlays:
            self.overlays[operation_id].set_progress(progress)
    
    def update_message(self, operation_id="default", message=""):
        """Actualiza el mensaje de un overlay"""
        if operation_id in self.overlays:
            self.overlays[operation_id].set_message(message)
    
    def hide_all(self):
        """Oculta todos los overlays"""
        for operation_id in list(self.overlays.keys()):
            self.hide_loading(operation_id)


# Ejemplo de uso - COMENTADO PARA EVITAR EJECUCIÓN AUTOMÁTICA
# if __name__ == "__main__":
#     import sys
#     
#     app = QApplication(sys.argv)
#     
#     # Crear ventana de ejemplo
#     window = QWidget()
#     window.setWindowTitle("Loading Spinner Demo")
#     window.resize(400, 300)
#     
#     # Crear layout
#     layout = QVBoxLayout()
#     
#     # Botones de demo
#     from PyQt6.QtWidgets import QPushButton, QHBoxLayout
#     
#     button_layout = QHBoxLayout()
#     
#     # Botón para mostrar spinner circular
#     btn_circular = QPushButton("Mostrar Circular")
#     btn_circular.clicked.connect(lambda: loading_manager.show_loading(
#         "circular", "Cargando usuarios desde São Paulo...", "circular"
#     ))
#     button_layout.addWidget(btn_circular)
#     
#     # Botón para mostrar spinner de puntos
#     btn_dots = QPushButton("Mostrar Puntos")
#     btn_dots.clicked.connect(lambda: loading_manager.show_loading(
#         "dots", "Procesando datos...", "dots"
#     ))
#     button_layout.addWidget(btn_dots)
#     
#     # Botón para mostrar spinner de progreso
#     btn_progress = QPushButton("Mostrar Progreso")
#     btn_progress.clicked.connect(lambda: show_progress_demo(loading_manager))
#     button_layout.addWidget(btn_progress)
#     
#     # Botón para ocultar
#     btn_hide = QPushButton("Ocultar")
#     btn_hide.clicked.connect(lambda: loading_manager.hide_all())
#     button_layout.addWidget(btn_hide)
#     
#     layout.addLayout(button_layout)
#     window.setLayout(layout)
#     
#     # Crear gestor de loading
#     loading_manager = DatabaseLoadingManager(window)
#     
#     def show_progress_demo(manager):
#         """Demo de progreso"""
#         manager.show_loading("progress", "Procesando...", "progress", show_message=True)
#         
#         # Simular progreso
#         def update_progress():
#             current_progress = getattr(update_progress, 'current', 0)
#             current_progress += 10
#             if current_progress <= 100:
#                 manager.update_progress("progress", current_progress)
#                 update_progress.current = current_progress
#                 QTimer.singleShot(500, update_progress)
#             else:
#                 manager.hide_loading("progress")
#         
#         update_progress.current = 0
#         update_progress()
#     
#     window.show()
#     sys.exit(app.exec())