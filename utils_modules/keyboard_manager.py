# -*- coding: utf-8 -*-
"""
Keyboard Manager - Sistema de navegación por teclado mejorada
Maneja atajos de teclado, navegación con Tab, y accesibilidad del teclado
"""

import logging
from PyQt6.QtCore import Qt, QObject, pyqtSignal
from PyQt6.QtWidgets import (
    QWidget, QApplication, QTabWidget, QTableWidget, QLineEdit,
    QPushButton, QComboBox, QSpinBox, QDateEdit, QTextEdit,
    QCheckBox, QRadioButton, QSlider, QScrollArea
)
from PyQt6.QtGui import QKeySequence, QShortcut, QKeyEvent

class KeyboardManager(QObject):
    """Gestor centralizado de navegación por teclado y atajos"""
    
    # Señales para comunicación
    shortcut_activated = pyqtSignal(str)
    navigation_changed = pyqtSignal(QWidget)
    
    def __init__(self, main_window):
        super().__init__()
        self.main_window = main_window
        self.shortcuts = {}
        self.focus_history = []
        self.current_focus_index = -1
        
        # Configurar atajos globales
        self.setup_global_shortcuts()
        
        # Configurar navegación por pestañas
        self.setup_tab_navigation()
        
        # Configurar navegación en tablas
        self.setup_table_navigation()
        
        logging.info("KeyboardManager inicializado correctamente")
    
    def setup_global_shortcuts(self):
        """Configura atajos de teclado globales del sistema"""
        try:
            # Atajos existentes mejorados
            global_shortcuts = {
                'Ctrl+1': ('tab_usuarios', 'Ir a pestaña Usuarios'),
                'Ctrl+2': ('tab_pagos', 'Ir a pestaña Pagos'),
                'Ctrl+3': ('tab_reportes', 'Ir a pestaña Reportes'),
                'Ctrl+4': ('tab_rutinas', 'Ir a pestaña Rutinas'),
                'Ctrl+5': ('tab_clases', 'Ir a pestaña Clases'),
                'Ctrl+6': ('tab_profesores', 'Ir a pestaña Profesores'),
                'Ctrl+7': ('tab_configuracion', 'Ir a pestaña Configuración'),
                'Ctrl+F': ('global_search', 'Búsqueda global'),
                
                # Nuevos atajos para navegación mejorada
                'Ctrl+N': ('new_record', 'Crear nuevo registro'),
                'Ctrl+S': ('save_record', 'Guardar registro actual'),
                'Ctrl+E': ('edit_record', 'Editar registro seleccionado'),
                'Delete': ('delete_record', 'Eliminar registro seleccionado'),
                'F5': ('refresh_data', 'Actualizar datos'),
                'Ctrl+P': ('print_export', 'Imprimir/Exportar'),
                'Ctrl+Z': ('undo_action', 'Deshacer última acción'),
                'Ctrl+Y': ('redo_action', 'Rehacer acción'),
                'Escape': ('cancel_action', 'Cancelar acción actual'),
                'F1': ('show_help', 'Mostrar ayuda contextual'),
                'Ctrl+Q': ('quit_application', 'Salir de la aplicación'),
                
                # Navegación rápida
                'Ctrl+Home': ('go_first', 'Ir al primer registro'),
                'Ctrl+End': ('go_last', 'Ir al último registro'),
                'Ctrl+Up': ('go_previous', 'Registro anterior'),
                'Ctrl+Down': ('go_next', 'Registro siguiente'),
                
                # Filtros y búsqueda
                'Ctrl+Shift+F': ('advanced_filter', 'Filtro avanzado'),
                'Ctrl+R': ('clear_filters', 'Limpiar filtros'),
                'Ctrl+L': ('focus_search', 'Enfocar campo de búsqueda'),
                # Check-in inverso por QR
                'Ctrl+Enter': ('generate_checkin_qr', 'Generar QR de check-in para socio seleccionado'),
            }
            
            for key_sequence, (action, description) in global_shortcuts.items():
                shortcut = QShortcut(QKeySequence(key_sequence), self.main_window)
                shortcut.activated.connect(lambda a=action: self.handle_shortcut(a))
                self.shortcuts[action] = {
                    'shortcut': shortcut,
                    'key': key_sequence,
                    'description': description
                }
                
            logging.info(f"Configurados {len(global_shortcuts)} atajos de teclado globales")
            
        except Exception as e:
            logging.error(f"Error configurando atajos globales: {e}")
    
    def setup_tab_navigation(self):
        """Configura navegación mejorada entre pestañas"""
        try:
            if hasattr(self.main_window, 'tabWidget'):
                tab_widget = self.main_window.tabWidget
                
                # Conectar eventos de cambio de pestaña
                tab_widget.currentChanged.connect(self.on_tab_changed)
                
                # Configurar orden de tabulación para cada pestaña
                for i in range(tab_widget.count()):
                    tab_content = tab_widget.widget(i)
                    if tab_content:
                        self.setup_tab_order(tab_content)
                        
            logging.info("Navegación por pestañas configurada")
            
        except Exception as e:
            logging.error(f"Error configurando navegación por pestañas: {e}")
    
    def setup_tab_order(self, widget):
        """Configura el orden de tabulación óptimo para un widget"""
        try:
            focusable_widgets = self.get_focusable_widgets(widget)
            
            # Ordenar widgets por posición (izquierda a derecha, arriba a abajo)
            focusable_widgets.sort(key=lambda w: (w.y(), w.x()))
            
            # Establecer orden de tabulación
            for i in range(len(focusable_widgets) - 1):
                current_widget = focusable_widgets[i]
                next_widget = focusable_widgets[i + 1]
                widget.setTabOrder(current_widget, next_widget)
                
            # Configurar políticas de foco
            for w in focusable_widgets:
                if isinstance(w, (QPushButton, QLineEdit, QComboBox)):
                    w.setFocusPolicy(Qt.FocusPolicy.StrongFocus)
                elif isinstance(w, QTableWidget):
                    w.setFocusPolicy(Qt.FocusPolicy.StrongFocus)
                    w.setTabKeyNavigation(True)
                    
        except Exception as e:
            logging.error(f"Error configurando orden de tabulación: {e}")
    
    def get_focusable_widgets(self, parent):
        """Obtiene todos los widgets que pueden recibir foco"""
        focusable = []
        
        for child in parent.findChildren(QWidget):
            if (child.isVisible() and 
                child.isEnabled() and 
                child.focusPolicy() != Qt.FocusPolicy.NoFocus):
                focusable.append(child)
                
        return focusable
    
    def setup_table_navigation(self):
        """Configura navegación mejorada en tablas"""
        try:
            # Buscar todas las tablas en la aplicación
            tables = self.main_window.findChildren(QTableWidget)
            
            for table in tables:
                # Instalar filtro de eventos para navegación personalizada
                table.installEventFilter(self)
                
                # Configurar selección y navegación
                table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
                table.setTabKeyNavigation(True)
                
                # Conectar señales de navegación
                table.itemSelectionChanged.connect(
                    lambda t=table: self.on_table_selection_changed(t)
                )
                
            logging.info(f"Navegación configurada para {len(tables)} tablas")
            
        except Exception as e:
            logging.error(f"Error configurando navegación en tablas: {e}")
    
    def handle_shortcut(self, action):
        """Maneja la activación de atajos de teclado"""
        try:
            current_tab = self.get_current_tab()
            
            if action.startswith('tab_'):
                self.handle_tab_shortcut(action)
            elif action == 'global_search':
                self.focus_global_search()
            elif action == 'new_record':
                self.handle_new_record(current_tab)
            elif action == 'save_record':
                self.handle_save_record(current_tab)
            elif action == 'edit_record':
                self.handle_edit_record(current_tab)
            elif action == 'delete_record':
                self.handle_delete_record(current_tab)
            elif action == 'refresh_data':
                self.handle_refresh_data(current_tab)
            elif action == 'print_export':
                self.handle_print_export(current_tab)
            elif action == 'undo_action':
                self.handle_undo_action()
            elif action == 'redo_action':
                self.handle_redo_action()
            elif action == 'cancel_action':
                self.handle_cancel_action()
            elif action == 'show_help':
                self.show_contextual_help()
            elif action in ['go_first', 'go_last', 'go_previous', 'go_next']:
                self.handle_navigation_shortcut(action, current_tab)
            elif action == 'advanced_filter':
                self.handle_advanced_filter(current_tab)
            elif action == 'clear_filters':
                self.handle_clear_filters(current_tab)
            elif action == 'focus_search':
                self.focus_current_search(current_tab)
            elif action == 'generate_checkin_qr':
                self.handle_generate_checkin_qr(current_tab)
                
            # Emitir señal de activación
            self.shortcut_activated.emit(action)
            
        except Exception as e:
            logging.error(f"Error manejando atajo {action}: {e}")
    
    def handle_tab_shortcut(self, action):
        """Maneja atajos de navegación entre pestañas"""
        tab_mapping = {
            'tab_usuarios': 0,
            'tab_pagos': 1,
            'tab_reportes': 2,
            'tab_rutinas': 3,
            'tab_clases': 4,
            'tab_profesores': 5,
            'tab_configuracion': 6
        }
        
        if action in tab_mapping and hasattr(self.main_window, 'tabWidget'):
            tab_index = tab_mapping[action]
            if tab_index < self.main_window.tabWidget.count():
                self.main_window.tabWidget.setCurrentIndex(tab_index)
    
    def get_current_tab(self):
        """Obtiene la pestaña actualmente activa"""
        if hasattr(self.main_window, 'tabWidget'):
            return self.main_window.tabWidget.currentWidget()
        return None
    
    def focus_global_search(self):
        """Enfoca la barra de búsqueda global"""
        if hasattr(self.main_window, 'search_widget'):
            self.main_window.search_widget.setFocus()
            self.main_window.search_widget.selectAll()
    
    def focus_current_search(self, current_tab):
        """Enfoca el campo de búsqueda de la pestaña actual"""
        if current_tab:
            # Buscar campo de búsqueda en la pestaña actual
            search_fields = current_tab.findChildren(QLineEdit)
            for field in search_fields:
                if 'search' in field.objectName().lower():
                    field.setFocus()
                    field.selectAll()
                    break

    def _resolve_selected_user_id(self, current_tab) -> int | None:
        """Intenta resolver el usuario seleccionado según la pestaña activa."""
        try:
            # 1) Método explícito en la pestaña
            if current_tab and hasattr(current_tab, 'get_current_user_id'):
                uid = current_tab.get_current_user_id()
                if isinstance(uid, int) and uid > 0:
                    return uid
            # 2) Atributo selected_user con id
            if current_tab and hasattr(current_tab, 'selected_user'):
                su = getattr(current_tab, 'selected_user')
                uid = getattr(su, 'id', None) or (su.get('id') if isinstance(su, dict) else None)
                if isinstance(uid, int) and uid > 0:
                    return uid
            # 3) Combobox de usuarios común en pagos
            if current_tab and hasattr(current_tab, 'user_combobox'):
                combo = current_tab.user_combobox
                data = combo.currentData()
                uid = None
                if isinstance(data, dict):
                    uid = data.get('id') or data.get('usuario_id')
                else:
                    uid = getattr(data, 'id', None) or getattr(data, 'usuario_id', None)
                if isinstance(uid, int) and uid > 0:
                    return uid
        except Exception:
            pass
        return None

    def handle_generate_checkin_qr(self, current_tab):
        """Delegar la generación del token/QR al MainWindow."""
        try:
            if hasattr(self.main_window, 'generate_checkin_qr_for_selected_user'):
                self.main_window.generate_checkin_qr_for_selected_user(current_tab)
            else:
                from PyQt6.QtWidgets import QMessageBox
                QMessageBox.information(self.main_window, "No disponible", "La ventana principal no implementa el flujo de check-in por QR.")
        except Exception as e:
            import logging
            logging.error(f"Error delegando generación de QR de check-in: {e}")
    
    def handle_new_record(self, current_tab):
        """Maneja la creación de nuevo registro"""
        if current_tab:
            # Buscar botón de agregar/nuevo
            buttons = current_tab.findChildren(QPushButton)
            for button in buttons:
                if any(keyword in button.text().lower() for keyword in ['agregar', 'nuevo', 'add', 'new']):
                    button.click()
                    break
    
    def handle_save_record(self, current_tab):
        """Maneja el guardado de registro"""
        if current_tab:
            # Buscar botón de guardar
            buttons = current_tab.findChildren(QPushButton)
            for button in buttons:
                if any(keyword in button.text().lower() for keyword in ['guardar', 'save']):
                    button.click()
                    break
    
    def handle_edit_record(self, current_tab):
        """Maneja la edición de registro seleccionado"""
        if current_tab:
            # Buscar tabla y verificar selección
            tables = current_tab.findChildren(QTableWidget)
            for table in tables:
                if table.currentRow() >= 0:
                    # Simular doble clic para editar
                    table.itemDoubleClicked.emit(table.currentItem())
                    break
    
    def handle_delete_record(self, current_tab):
        """Maneja la eliminación de registro seleccionado"""
        if current_tab:
            # Buscar botón de eliminar
            buttons = current_tab.findChildren(QPushButton)
            for button in buttons:
                if any(keyword in button.text().lower() for keyword in ['eliminar', 'delete', 'borrar']):
                    button.click()
                    break
    
    def handle_refresh_data(self, current_tab):
        """Maneja la actualización de datos"""
        if current_tab and hasattr(current_tab, 'refresh_data'):
            current_tab.refresh_data()
    
    def handle_print_export(self, current_tab):
        """Maneja impresión/exportación"""
        if current_tab:
            # Buscar botones de exportación
            buttons = current_tab.findChildren(QPushButton)
            for button in buttons:
                if any(keyword in button.text().lower() for keyword in ['export', 'imprimir', 'pdf']):
                    button.click()
                    break
    
    def handle_undo_action(self):
        """Maneja deshacer acción"""
        # Implementar sistema de deshacer si existe
        if hasattr(self.main_window, 'action_history_manager'):
            self.main_window.action_history_manager.undo()
    
    def handle_redo_action(self):
        """Maneja rehacer acción"""
        # Implementar sistema de rehacer si existe
        if hasattr(self.main_window, 'action_history_manager'):
            self.main_window.action_history_manager.redo()
    
    def handle_cancel_action(self):
        """Maneja cancelación de acción actual"""
        # Cerrar diálogos abiertos o cancelar operaciones
        focused_widget = QApplication.focusWidget()
        if focused_widget:
            # Si hay un diálogo modal, cerrarlo
            parent = focused_widget.window()
            if parent != self.main_window:
                parent.close()
    
    def show_contextual_help(self):
        """Muestra ayuda contextual"""
        # Implementar sistema de ayuda contextual
        focused_widget = QApplication.focusWidget()
        if focused_widget and hasattr(focused_widget, 'toolTip'):
            tooltip = focused_widget.toolTip()
            if tooltip:
                # Mostrar tooltip expandido o ayuda
                print(f"Ayuda: {tooltip}")
    
    def handle_navigation_shortcut(self, action, current_tab):
        """Maneja atajos de navegación en registros"""
        if current_tab:
            tables = current_tab.findChildren(QTableWidget)
            for table in tables:
                if table.hasFocus() or table.currentRow() >= 0:
                    if action == 'go_first':
                        table.setCurrentRow(0)
                    elif action == 'go_last':
                        table.setCurrentRow(table.rowCount() - 1)
                    elif action == 'go_previous':
                        current_row = table.currentRow()
                        if current_row > 0:
                            table.setCurrentRow(current_row - 1)
                    elif action == 'go_next':
                        current_row = table.currentRow()
                        if current_row < table.rowCount() - 1:
                            table.setCurrentRow(current_row + 1)
                    break
    
    def handle_advanced_filter(self, current_tab):
        """Maneja filtro avanzado"""
        if current_tab:
            # Buscar botón de filtro avanzado
            buttons = current_tab.findChildren(QPushButton)
            for button in buttons:
                if any(keyword in button.text().lower() for keyword in ['filtro', 'filter', 'avanzado']):
                    button.click()
                    break
    
    def handle_clear_filters(self, current_tab):
        """Maneja limpieza de filtros"""
        if current_tab:
            # Limpiar campos de búsqueda
            search_fields = current_tab.findChildren(QLineEdit)
            for field in search_fields:
                if 'search' in field.objectName().lower():
                    field.clear()
    
    def on_tab_changed(self, index):
        """Maneja cambio de pestaña"""
        try:
            current_widget = self.main_window.tabWidget.widget(index)
            if current_widget:
                # Enfocar primer elemento focusable de la nueva pestaña
                focusable_widgets = self.get_focusable_widgets(current_widget)
                if focusable_widgets:
                    focusable_widgets[0].setFocus()
                    
                self.navigation_changed.emit(current_widget)
                
        except Exception as e:
            logging.error(f"Error en cambio de pestaña: {e}")
    
    def on_table_selection_changed(self, table):
        """Maneja cambio de selección en tabla"""
        try:
            current_row = table.currentRow()
            if current_row >= 0:
                # Actualizar historial de navegación
                self.update_focus_history(table)
                
        except Exception as e:
            logging.error(f"Error en cambio de selección de tabla: {e}")
    
    def update_focus_history(self, widget):
        """Actualiza el historial de foco para navegación"""
        try:
            # Agregar al historial si no es el último elemento
            if not self.focus_history or self.focus_history[-1] != widget:
                self.focus_history.append(widget)
                
                # Limitar tamaño del historial
                if len(self.focus_history) > 50:
                    self.focus_history.pop(0)
                    
                self.current_focus_index = len(self.focus_history) - 1
                
        except Exception as e:
            logging.error(f"Error actualizando historial de foco: {e}")
    
    def eventFilter(self, obj, event):
        """Filtro de eventos para navegación personalizada"""
        try:
            if isinstance(obj, QTableWidget) and event.type() == QKeyEvent.Type.KeyPress:
                key = event.key()
                modifiers = event.modifiers()
                
                # Navegación mejorada en tablas
                if key == Qt.Key.Key_Return or key == Qt.Key.Key_Enter:
                    # Enter para editar registro
                    if obj.currentItem():
                        obj.itemDoubleClicked.emit(obj.currentItem())
                        return True
                        
                elif key == Qt.Key.Key_Space:
                    # Espacio para seleccionar/deseleccionar
                    if obj.currentItem():
                        current_row = obj.currentRow()
                        obj.selectRow(current_row)
                        return True
                        
                elif key == Qt.Key.Key_F2:
                    # F2 para editar in-situ
                    if obj.currentItem():
                        obj.editItem(obj.currentItem())
                        return True
                        
            return super().eventFilter(obj, event)
            
        except Exception as e:
            logging.error(f"Error en filtro de eventos: {e}")
            return False
    
    def get_shortcuts_help(self):
        """Obtiene lista de atajos disponibles para mostrar ayuda"""
        help_text = "Atajos de teclado disponibles:\n\n"
        
        categories = {
            'Navegación': ['tab_usuarios', 'tab_pagos', 'tab_reportes', 'tab_rutinas', 'tab_clases', 'tab_profesores', 'tab_configuracion'],
            'Acciones': ['new_record', 'save_record', 'edit_record', 'delete_record', 'refresh_data'],
            'Búsqueda': ['global_search', 'focus_search', 'advanced_filter', 'clear_filters'],
            'Navegación en registros': ['go_first', 'go_last', 'go_previous', 'go_next'],
            'Sistema': ['undo_action', 'redo_action', 'cancel_action', 'show_help', 'quit_application']
        }
        
        for category, actions in categories.items():
            help_text += f"{category}:\n"
            for action in actions:
                if action in self.shortcuts:
                    shortcut_info = self.shortcuts[action]
                    help_text += f"  {shortcut_info['key']}: {shortcut_info['description']}\n"
            help_text += "\n"
            
        return help_text

# Instancia global del gestor de teclado
keyboard_manager = None

def initialize_keyboard_manager(main_window):
    """Inicializa el gestor de teclado global"""
    global keyboard_manager
    keyboard_manager = KeyboardManager(main_window)
    return keyboard_manager

def get_keyboard_manager():
    """Obtiene la instancia global del gestor de teclado"""
    return keyboard_manager