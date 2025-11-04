from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLineEdit, QPushButton, 
    QListWidget, QListWidgetItem, QLabel, QFrame, QScrollArea,
    QCompleter, QApplication
)
from PyQt6.QtCore import Qt, pyqtSignal, QTimer, QStringListModel
from PyQt6.QtGui import QPixmap, QIcon
from utils import resource_path
from search_manager import SearchManager
from typing import List, Dict, Any
import logging

class SearchResultItem(QFrame):
    """Widget personalizado para mostrar un resultado de búsqueda"""
    
    clicked = pyqtSignal(dict)  # Emite los datos del resultado
    
    def __init__(self, result_data: Dict[str, Any]):
        super().__init__()
        self.result_data = result_data
        self.setup_ui()
        self.setObjectName("search_result_item")
        self.setProperty("class", "search_result")
        self.setProperty("dynamic_css", "true")
        self.setCursor(Qt.CursorShape.PointingHandCursor)
    
    def setup_ui(self):
        layout = QHBoxLayout(self)
        layout.setContentsMargins(16, 12, 16, 12)
        layout.setSpacing(16)
        
        # Icono del tipo de resultado (intenta usar imagen de assets)
        icon_label = QLabel()
        icon_label.setObjectName("result_icon")
        icon_label.setProperty("class", "search_result_icon")
        icon_label.setFixedSize(32, 32)
        icon_label.setAlignment(Qt.AlignmentFlag.AlignCenter)

        icon_path = None
        try:
            # Permitir que 'icon' sea una ruta directa
            raw_icon = self.result_data.get('icon')
            if isinstance(raw_icon, str) and (raw_icon.lower().endswith('.png') or raw_icon.lower().endswith('.svg') or raw_icon.lower().endswith('.ico')):
                icon_path = raw_icon
            else:
                t = str(self.result_data.get('type', '')).lower()
                mapping = {
                    'usuario': 'assets/users.png',
                    'pago': 'assets/money.png',
                    'clase': 'assets/classes.png',
                    'profesor': 'assets/student_icon.png',
                    'rutina': 'assets/routines.png',
                    'ejercicio': 'assets/attendance.png',
                }
                icon_path = mapping.get(t, 'assets/icon.png')
            pm = QPixmap(resource_path(icon_path))
            if not pm.isNull():
                pm = pm.scaled(28, 28, Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation)
                icon_label.setPixmap(pm)
            else:
                # Fallback a emoji si no hay imagen
                icon_label.setText(self.result_data.get('icon', '📄'))
        except Exception:
            icon_label.setText(self.result_data.get('icon', '📄'))
        layout.addWidget(icon_label)
        
        # Contenido principal
        content_layout = QVBoxLayout()
        content_layout.setSpacing(4)
        
        # Título
        title_label = QLabel(self.result_data.get('title', 'Sin título'))
        title_label.setObjectName("main_text")
        title_label.setProperty("class", "search_result_title")
        title_label.setWordWrap(True)
        content_layout.addWidget(title_label)
        
        # Subtítulo
        if self.result_data.get('subtitle'):
            subtitle_label = QLabel(self.result_data['subtitle'])
            subtitle_label.setObjectName("secondary_text")
            subtitle_label.setProperty("class", "search_result_subtitle")
            subtitle_label.setWordWrap(True)
            content_layout.addWidget(subtitle_label)
        
        # Descripción
        if self.result_data.get('description'):
            desc_label = QLabel(self.result_data['description'])
            desc_label.setObjectName("secondary_text")
            desc_label.setProperty("class", "search_result_description")
            desc_label.setWordWrap(True)
            content_layout.addWidget(desc_label)
        
        layout.addLayout(content_layout)
        
        # Tipo de resultado
        type_text = self.result_data.get('type', '').upper()
        if type_text == 'USUARIO':
            type_text = 'USER'
        elif type_text == 'PROFESOR':
            type_text = 'PROF'
        elif type_text == 'RUTINA':
            type_text = 'ROUT'
        elif type_text == 'EJERCICIO':
            type_text = 'EXER'
        
        type_label = QLabel(type_text)
        type_label.setObjectName("type_indicator")
        type_label.setProperty("class", "search_result_type")
        type_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(type_label)
    
    def mousePressEvent(self, event):
        if event.button() == Qt.MouseButton.LeftButton:
            self.clicked.emit(self.result_data)
        super().mousePressEvent(event)

class GlobalSearchWidget(QWidget):
    """Widget de búsqueda global con autocompletado y resultados"""
    
    # Señales
    result_selected = pyqtSignal(dict)  # Emite cuando se selecciona un resultado
    search_requested = pyqtSignal(str)  # Emite cuando se solicita búsqueda
    user_selected = pyqtSignal(int)  # Emite ID de usuario seleccionado
    payment_selected = pyqtSignal(int)  # Emite ID de pago seleccionado
    class_selected = pyqtSignal(int)  # Emite ID de clase seleccionada
    routine_selected = pyqtSignal(int)  # Emite ID de rutina seleccionada
    
    def __init__(self, search_manager: SearchManager):
        super().__init__()
        self.search_manager = search_manager
        self.current_results = []
        self.is_expanded = False
        # Permitir que el stylesheet global aplique reglas específicas y colores dinámicos
        self.setObjectName("global_search_widget")
        self.setProperty("dynamic_css", "true")
        self.setup_ui()
        self.connect_signals()
        
        # Timer para ocultar resultados automáticamente
        self.hide_timer = QTimer()
        self.hide_timer.setSingleShot(True)
        self.hide_timer.timeout.connect(self.collapse_results)
    
    def setup_ui(self):
        # Diseño compacto para integración en pestañas con altura fija
        self.setFixedHeight(32)  # Altura ajustada para alineación con pestañas
        self.setFixedWidth(260)  # Ancho ajustado
        self.setMinimumHeight(32)  # Altura mínima igual a la fija
        self.setMaximumHeight(32)  # Altura máxima igual a la fija
        
        # Layout principal
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)
        
        # Barra de búsqueda con estilo integrado
        search_frame = QFrame()
        search_frame.setObjectName("search_container")
        search_frame.setProperty("class", "search_frame")
        search_layout = QHBoxLayout(search_frame)
        search_layout.setContentsMargins(6, 2, 6, 2)  # Márgenes más compactos
        search_layout.setSpacing(3)  # Espaciado reducido
        
        # Icono de búsqueda
        self.search_icon = QLabel("🔍")
        self.search_icon.setObjectName("search_icon")
        self.search_icon.setProperty("class", "search_icon")
        self.search_icon.setFixedSize(12, 12)  # Tamaño más compacto
        self.search_icon.setAlignment(Qt.AlignmentFlag.AlignCenter)
        search_layout.addWidget(self.search_icon)
        
        # Campo de búsqueda
        self.search_input = QLineEdit()
        self.search_input.setObjectName("search_input")  # Asignar objectName para CSS dinámico
        self.search_input.setProperty("class", "global_search_input")
        self.search_input.setPlaceholderText("Buscar...")
        # Usar QSS dinámico (sin estilos inline)
        # Remover estilos hardcodeados - usar CSS dinámico
        search_layout.addWidget(self.search_input)
        
        # Botón de limpiar
        self.clear_button = QPushButton("✕")
        self.clear_button.setFixedSize(12, 12)  # Tamaño más compacto
        self.clear_button.setObjectName("clear_button")
        self.clear_button.setProperty("class", "search_clear_btn")
        self.clear_button.hide()
        search_layout.addWidget(self.clear_button)
        
        main_layout.addWidget(search_frame)
        
        # Panel de resultados (inicialmente oculto)
        self.results_frame = QFrame()
        # Alinear con el selector en style.qss para aplicar fondo y borde
        self.results_frame.setObjectName("results_frame")
        self.results_frame.setProperty("class", "results_frame")
        # Usar QSS dinámico (sin estilos inline)
        # Remover estilos hardcodeados para permitir CSS dinámico
        self.results_frame.hide()
        
        results_layout = QVBoxLayout(self.results_frame)
        results_layout.setContentsMargins(12, 12, 12, 12)  # Márgenes aumentados
        results_layout.setSpacing(8)  # Espaciado aumentado
        
        # Header de resultados
        self.results_header = QLabel("Resultados de búsqueda")
        self.results_header.setObjectName("results_header")
        self.results_header.setProperty("class", "search_results_header")
        results_layout.addWidget(self.results_header)
        
        # Área de scroll para resultados
        self.results_scroll = QScrollArea()
        self.results_scroll.setWidgetResizable(True)
        self.results_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.results_scroll.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        self.results_scroll.setMaximumHeight(300)  # Altura máxima aumentada
        self.results_scroll.setObjectName("results_scroll")
        self.results_scroll.setProperty("class", "search_scroll_area")
        
        # Widget contenedor de resultados
        self.results_container = QWidget()
        self.results_container.setObjectName("results_list")  # Asignar objectName para CSS dinámico
        self.results_container.setProperty("class", "search_results_list")
        self.results_layout = QVBoxLayout(self.results_container)
        self.results_layout.setContentsMargins(0, 0, 0, 0)
        self.results_layout.setSpacing(4)  # Espaciado aumentado entre resultados
        self.results_scroll.setWidget(self.results_container)
        
        results_layout.addWidget(self.results_scroll)
        main_layout.addWidget(self.results_frame)
        
        # Timer para reposicionar panel flotante
        self.reposition_timer = QTimer()
        self.reposition_timer.setSingleShot(True)
        self.reposition_timer.timeout.connect(self.reposition_floating_panel)
        self.reposition_timer.setInterval(100)  # 100ms de delay para reducir parpadeo
        
        # Timer para debounce del scroll
        self.scroll_timer = QTimer()
        self.scroll_timer.setSingleShot(True)
        self.scroll_timer.timeout.connect(self.delayed_reposition)
        self.scroll_timer.setInterval(150)  # 150ms de delay para scroll
        
        # Instalar filtro de eventos para manejar clics fuera del panel
        QApplication.instance().installEventFilter(self)
        
        # Bandera para controlar el estado de navegación
        self._navigation_in_progress = False
        
        # Sin configuración responsiva - tamaños fijos
    
    # Método setup_completer eliminado para evitar selectores duplicados
    
    def connect_signals(self):
        """Conecta las señales"""
        self.search_input.textChanged.connect(self.on_text_changed)
        self.search_input.returnPressed.connect(self.perform_search)
        self.clear_button.clicked.connect(self.clear_search)
        
        # Conectar señales del search manager
        self.search_manager.search_completed.connect(self.display_results)
        
        # Eventos de foco
        self.search_input.focusInEvent = self.on_focus_in
        self.search_input.focusOutEvent = self.on_focus_out
    
    def on_text_changed(self, text: str):
        """Maneja cambios en el texto de búsqueda"""
        if text.strip():
            self.clear_button.show()
            if len(text.strip()) >= 2:
                self.search_manager.search(text.strip())
        else:
            self.clear_button.hide()
            self.collapse_results()
    
    def perform_search(self):
        """Realiza búsqueda inmediata"""
        query = self.search_input.text().strip()
        if query:
            self.search_manager.search(query, delay=False)
            self.search_requested.emit(query)
    
    def clear_search(self):
        """Limpia la búsqueda"""
        self.search_input.clear()
        self.collapse_results()
        self.search_input.setFocus()
    
    def display_results(self, query: str, results: List[Dict[str, Any]]):
        """Muestra los resultados de búsqueda"""
        self.current_results = results
        
        # Limpiar resultados anteriores
        for i in reversed(range(self.results_layout.count())):
            child = self.results_layout.itemAt(i).widget()
            if child:
                child.setParent(None)
        
        if not results:
            # Mostrar mensaje de "sin resultados"
            no_results = QLabel("No se encontraron resultados")
            no_results.setObjectName("search_no_results")
            no_results.setAlignment(Qt.AlignmentFlag.AlignCenter)
            # Usar CSS dinámico en lugar de estilos hardcodeados
            self.results_layout.addWidget(no_results)
        else:
            # Mostrar resultados
            for result in results[:15]:  # Limitar a 15 resultados
                result_item = SearchResultItem(result)
                result_item.clicked.connect(self.on_result_selected)
                self.results_layout.addWidget(result_item)
        
        # Actualizar header
        count = len(results)
        self.results_header.setText(f"Resultados para '{query}' ({count})")
        
        # Expandir panel de resultados
        self.expand_results()
    
    def on_result_selected(self, result_data: Dict[str, Any]):
        """Maneja selección de resultado"""
        # Marcar que estamos en proceso de navegación
        self._navigation_in_progress = True
        
        # Detener todos los timers para evitar conflictos
        self.hide_timer.stop()
        self.reposition_timer.stop()
        self.scroll_timer.stop()
        
        # Colapsar inmediatamente sin delay para evitar parpadeo
        self.collapse_results_immediate()
        
        self.result_selected.emit(result_data)
        
        # Emitir señal específica según el tipo de resultado
        result_type = result_data.get('type', '').lower()
        result_id = result_data.get('id')
        
        if result_id and result_type == 'usuario':
            self.user_selected.emit(result_id)
        elif result_id and result_type == 'pago':
            self.payment_selected.emit(result_id)
        elif result_id and result_type == 'clase':
            self.class_selected.emit(result_id)
        elif result_id and result_type == 'rutina':
            self.routine_selected.emit(result_id)
        
        # Resetear la bandera después de un breve delay para permitir que la navegación complete
        QTimer.singleShot(500, lambda: setattr(self, '_navigation_in_progress', False))
        
        # Opcional: actualizar el campo de búsqueda con el título seleccionado
        # self.search_input.setText(result_data.get('title', ''))
    
    def expand_results(self):
        """Expande el panel de resultados de forma flotante"""
        if not self.is_expanded:
            self.is_expanded = True
            
            # Hacer el panel flotante usando setParent(None) y posicionamiento manual
            self.results_frame.setParent(None)
            self.results_frame.setWindowFlags(Qt.WindowType.ToolTip | Qt.WindowType.FramelessWindowHint)
            
            # Calcular posición flotante debajo de la barra de búsqueda
            search_pos = self.mapToGlobal(self.rect().bottomLeft())
            self.results_frame.move(search_pos.x(), search_pos.y() + 2)
            
            # Configurar tamaño dinámico sin afectar el widget principal
            suggested_height = min(350, 60 + self.results_frame.sizeHint().height())
            # Aumentar el ancho para evitar cortes horizontales, dentro de límites de la ventana
            desired_width = max(360, self.width() + 240)
            try:
                main_window = self.parent()
                while main_window and not hasattr(main_window, 'geometry'):
                    main_window = getattr(main_window, 'parent', lambda: None)()
                if main_window:
                    geo = main_window.geometry()
                    max_width = max(280, geo.width() - 24)
                    desired_width = min(desired_width, max_width)
            except Exception:
                pass
            self.results_frame.resize(desired_width, suggested_height)
            
            # Mostrar el panel flotante
            self.results_frame.show()
            self.results_frame.raise_()

            # Reposicionar botones flotantes de la ventana principal para evitar solapes
            try:
                main_window = self.parent()
                while main_window and not hasattr(main_window, 'position_floating_button'):
                    main_window = getattr(main_window, 'parent', lambda: None)()
                if main_window and hasattr(main_window, 'position_floating_button'):
                    main_window.position_floating_button()
            except Exception:
                pass
            
            # Mantener el tamaño original del widget principal
            self.setFixedHeight(32)
    
    def collapse_results(self):
        """Colapsa el panel de resultados flotante"""
        if self.is_expanded:
            self.is_expanded = False
            self.results_frame.hide()
            
            # Restaurar el panel como hijo del widget principal
            self.results_frame.setParent(self)
            self.results_frame.setWindowFlags(Qt.WindowType.Widget)
            
            # Reposicionar botones flotantes tras colapsar
            try:
                main_window = self.parent()
                while main_window and not hasattr(main_window, 'position_floating_button'):
                    main_window = getattr(main_window, 'parent', lambda: None)()
                if main_window and hasattr(main_window, 'position_floating_button'):
                    main_window.position_floating_button()
            except Exception:
                pass

            # Mantener el tamaño compacto del widget principal
            self.setFixedHeight(32)
    
    def collapse_results_immediate(self):
        """Colapsa el panel de resultados inmediatamente sin efectos"""
        if self.is_expanded:
            self.is_expanded = False
            
            # Ocultar inmediatamente sin procesamiento de eventos
            self.results_frame.setVisible(False)
            
            # Restaurar el panel como hijo del widget principal
            self.results_frame.setParent(self)
            self.results_frame.setWindowFlags(Qt.WindowType.Widget)
            
            # Reposicionar botones flotantes tras colapsar inmediatamente
            try:
                main_window = self.parent()
                while main_window and not hasattr(main_window, 'position_floating_button'):
                    main_window = getattr(main_window, 'parent', lambda: None)()
                if main_window and hasattr(main_window, 'position_floating_button'):
                    main_window.position_floating_button()
            except Exception:
                pass

            # Mantener el tamaño compacto del widget principal
            self.setFixedHeight(32)
            
            # Procesar eventos pendientes para asegurar que el cambio sea inmediato
            QApplication.processEvents()
    
    # Método update_suggestions eliminado - no se usa completer
    
    def on_focus_in(self, event):
        """Maneja evento de foco entrante"""
        self.hide_timer.stop()
        
        # Solo expandir si no estamos en proceso de navegación
        if (self.current_results and self.search_input.text().strip() and 
            not getattr(self, '_navigation_in_progress', False)):
            self.expand_results()
            # Asegurar que el panel flotante se mantenga visible
            if self.is_expanded:
                self.results_frame.activateWindow()
                self.results_frame.raise_()
        QLineEdit.focusInEvent(self.search_input, event)
    
    def on_focus_out(self, event):
        """Maneja evento de foco saliente"""
        # Solo iniciar timer si no estamos navegando
        if not getattr(self, '_navigation_in_progress', False):
            # Delay para permitir clicks en resultados
            self.hide_timer.start(200)
        QLineEdit.focusOutEvent(self.search_input, event)
    
    def set_focus(self):
        """Establece el foco en el campo de búsqueda"""
        self.search_input.setFocus()
        self.search_input.selectAll()
    
    def reposition_floating_panel(self):
        """Reposiciona el panel flotante si está visible"""
        if self.is_expanded and self.results_frame.isVisible():
            search_pos = self.mapToGlobal(self.rect().bottomLeft())
            panel_x = search_pos.x()
            panel_y = search_pos.y() + 2
            
            # Obtener el widget padre principal para verificar límites
            main_window = self.parent()
            while main_window and not hasattr(main_window, 'geometry'):
                main_window = main_window.parent()
                
            if main_window:
                # Obtener geometría de la ventana principal
                main_geometry = main_window.geometry()
                
                # Ajustar ancho si se excede el borde derecho
                max_panel_width = max(280, main_geometry.width() - 20)
                if self.results_frame.width() > max_panel_width:
                    self.results_frame.resize(max_panel_width, self.results_frame.height())
                # Verificar si el panel se sale por la derecha
                if panel_x + self.results_frame.width() > main_geometry.right():
                    panel_x = max(main_geometry.left() + 10, main_geometry.right() - self.results_frame.width() - 10)
                
                # Verificar si el panel se sale por abajo - agregar margen de seguridad
                bottom_margin = 50  # Margen de seguridad para evitar que se corte
                if panel_y + self.results_frame.height() > main_geometry.bottom() - bottom_margin:
                    # Mostrar el panel arriba del input en lugar de abajo
                    panel_y = search_pos.y() - self.results_frame.height() - 2
                    
                    # Si tampoco cabe arriba, ajustar altura del panel
                    if panel_y < main_geometry.top() + 10:
                        panel_y = search_pos.y() + 2
                        max_height = main_geometry.bottom() - panel_y - bottom_margin
                        if max_height > 100:  # Altura mínima razonable
                            self.results_frame.setMaximumHeight(max_height)
            
            self.results_frame.move(panel_x, panel_y)

            # También pedir reposicionamiento de botones flotantes en la ventana principal
            try:
                main_window = self.parent()
                while main_window and not hasattr(main_window, 'position_floating_button'):
                    main_window = getattr(main_window, 'parent', lambda: None)()
                if main_window and hasattr(main_window, 'position_floating_button'):
                    main_window.position_floating_button()
            except Exception:
                pass
    
    def delayed_reposition(self):
        """Reposicionamiento con delay para evitar parpadeo durante scroll"""
        if self.results_frame.isVisible():
            self.reposition_floating_panel()
    
    def eventFilter(self, obj, event):
        """Filtro de eventos para manejar clics fuera del panel flotante"""
        if event.type() == event.Type.MouseButtonPress:
            if (self.is_expanded and self.results_frame.isVisible() and 
                not getattr(self, '_navigation_in_progress', False)):
                # Verificar si el clic fue fuera del panel de resultados y la barra de búsqueda
                if (not self.results_frame.geometry().contains(event.globalPosition().toPoint()) and 
                    not self.geometry().contains(self.mapFromGlobal(event.globalPosition().toPoint()))):
                    self.collapse_results()
        elif event.type() == event.Type.Scroll:
            # Manejar eventos de scroll con debounce para evitar parpadeo
            if (self.results_frame.isVisible() and 
                not getattr(self, '_navigation_in_progress', False)):
                self.scroll_timer.start()  # Reiniciar timer de scroll
        return super().eventFilter(obj, event)
    
    # Métodos responsivos eliminados - usando tamaños fijos
    
    def get_current_query(self) -> str:
        """Obtiene la consulta actual"""
        return self.search_input.text().strip()

