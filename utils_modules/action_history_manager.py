# -*- coding: utf-8 -*-
"""
Action History Manager - Sistema de historial de acciones con deshacer/rehacer
Permite rastrear y revertir acciones del usuario en el sistema
"""

import logging
from datetime import datetime
from typing import List, Dict, Any, Optional, Callable
from PyQt6.QtCore import QObject, pyqtSignal
from PyQt6.QtWidgets import QWidget, QMessageBox

class Action:
    """Representa una acción que puede ser deshecha/rehecha"""
    
    def __init__(self, 
                 action_type: str,
                 description: str,
                 execute_func: Callable,
                 undo_func: Callable,
                 data: Dict[str, Any] = None,
                 context: str = None):
        self.action_type = action_type
        self.description = description
        self.execute_func = execute_func
        self.undo_func = undo_func
        self.data = data or {}
        self.context = context
        self.timestamp = datetime.now()
        self.executed = False
        self.undone = False
    
    def execute(self):
        """Ejecuta la acción"""
        try:
            if not self.executed:
                result = self.execute_func(self.data)
                self.executed = True
                self.undone = False
                return result
        except Exception as e:
            logging.error(f"Error ejecutando acción {self.description}: {e}")
            raise
    
    def undo(self):
        """Deshace la acción"""
        try:
            if self.executed and not self.undone:
                result = self.undo_func(self.data)
                self.undone = True
                return result
        except Exception as e:
            logging.error(f"Error deshaciendo acción {self.description}: {e}")
            raise
    
    def redo(self):
        """Rehace la acción"""
        try:
            if self.undone:
                result = self.execute_func(self.data)
                self.undone = False
                return result
        except Exception as e:
            logging.error(f"Error rehaciendo acción {self.description}: {e}")
            raise
    
    def __str__(self):
        return f"{self.description} ({self.timestamp.strftime('%H:%M:%S')})"

class ActionHistoryManager(QObject):
    """Gestor del historial de acciones con capacidades de deshacer/rehacer"""
    
    # Señales para notificar cambios
    action_executed = pyqtSignal(str)  # descripción de la acción
    action_undone = pyqtSignal(str)
    action_redone = pyqtSignal(str)
    history_changed = pyqtSignal()
    
    def __init__(self, max_history_size: int = 100):
        super().__init__()
        self.max_history_size = max_history_size
        self.history: List[Action] = []
        self.current_index = -1
        self.enabled = True
        
        logging.info(f"ActionHistoryManager inicializado con tamaño máximo: {max_history_size}")
    
    def add_action(self, action: Action, execute_immediately: bool = True):
        """Añade una nueva acción al historial"""
        try:
            if not self.enabled:
                return
            
            # Eliminar acciones posteriores al índice actual (para ramificación)
            if self.current_index < len(self.history) - 1:
                self.history = self.history[:self.current_index + 1]
            
            # Ejecutar la acción si se solicita
            if execute_immediately:
                action.execute()
            
            # Añadir al historial
            self.history.append(action)
            self.current_index = len(self.history) - 1
            
            # Limitar tamaño del historial
            if len(self.history) > self.max_history_size:
                self.history.pop(0)
                self.current_index -= 1
            
            # Emitir señales
            if execute_immediately:
                self.action_executed.emit(action.description)
            self.history_changed.emit()
            
            logging.debug(f"Acción añadida al historial: {action.description}")
            
        except Exception as e:
            logging.error(f"Error añadiendo acción al historial: {e}")
    
    def undo(self) -> bool:
        """Deshace la última acción"""
        try:
            if not self.can_undo():
                return False
            
            action = self.history[self.current_index]
            action.undo()
            self.current_index -= 1
            
            self.action_undone.emit(action.description)
            self.history_changed.emit()
            
            logging.info(f"Acción deshecha: {action.description}")
            return True
            
        except Exception as e:
            logging.error(f"Error deshaciendo acción: {e}")
            return False
    
    def redo(self) -> bool:
        """Rehace la siguiente acción"""
        try:
            if not self.can_redo():
                return False
            
            self.current_index += 1
            action = self.history[self.current_index]
            action.redo()
            
            self.action_redone.emit(action.description)
            self.history_changed.emit()
            
            logging.info(f"Acción rehecha: {action.description}")
            return True
            
        except Exception as e:
            logging.error(f"Error rehaciendo acción: {e}")
            return False
    
    def can_undo(self) -> bool:
        """Verifica si se puede deshacer"""
        return self.enabled and self.current_index >= 0
    
    def can_redo(self) -> bool:
        """Verifica si se puede rehacer"""
        return self.enabled and self.current_index < len(self.history) - 1
    
    def get_undo_description(self) -> Optional[str]:
        """Obtiene la descripción de la acción que se puede deshacer"""
        if self.can_undo():
            return self.history[self.current_index].description
        return None
    
    def get_redo_description(self) -> Optional[str]:
        """Obtiene la descripción de la acción que se puede rehacer"""
        if self.can_redo():
            return self.history[self.current_index + 1].description
        return None
    
    def clear_history(self):
        """Limpia todo el historial"""
        self.history.clear()
        self.current_index = -1
        self.history_changed.emit()
        logging.info("Historial de acciones limpiado")
    
    def get_recent_actions(self, count: int = 10) -> List[Action]:
        """Obtiene las acciones más recientes"""
        if not self.history:
            return []
        
        start_index = max(0, len(self.history) - count)
        return self.history[start_index:]
    
    def get_history_summary(self) -> Dict[str, Any]:
        """Obtiene un resumen del historial"""
        return {
            'total_actions': len(self.history),
            'current_index': self.current_index,
            'can_undo': self.can_undo(),
            'can_redo': self.can_redo(),
            'undo_description': self.get_undo_description(),
            'redo_description': self.get_redo_description(),
            'recent_actions': [str(action) for action in self.get_recent_actions(5)]
        }
    
    def enable(self):
        """Habilita el historial de acciones"""
        self.enabled = True
        logging.info("Historial de acciones habilitado")
    
    def disable(self):
        """Deshabilita el historial de acciones"""
        self.enabled = False
        logging.info("Historial de acciones deshabilitado")
    
    def create_user_action(self, 
                          action_type: str,
                          description: str,
                          user_data: Dict[str, Any],
                          database_manager) -> Action:
        """Crea una acción para operaciones de usuario"""
        
        def execute_user_action(data):
            if action_type == 'add_user':
                return database_manager.add_user(
                    data['name'], data['phone'], 
                    data['membership_type'], data['start_date']
                )
            elif action_type == 'update_user':
                return database_manager.update_user(
                    data['user_id'], data['name'], 
                    data['phone'], data['membership_type']
                )
            elif action_type == 'delete_user':
                return database_manager.delete_user(data['user_id'])
        
        def undo_user_action(data):
            if action_type == 'add_user':
                return database_manager.delete_user(data['user_id'])
            elif action_type == 'update_user':
                return database_manager.update_user(
                    data['user_id'], data['original_name'], 
                    data['original_phone'], 
                    data['original_membership_type']
                )
            elif action_type == 'delete_user':
                return database_manager.add_user(
                    data['name'], data['phone'], 
                    data['membership_type'], data['start_date']
                )
        
        return Action(
            action_type=action_type,
            description=description,
            execute_func=execute_user_action,
            undo_func=undo_user_action,
            data=user_data,
            context='users'
        )
    
    def create_payment_action(self, 
                             action_type: str,
                             description: str,
                             payment_data: Dict[str, Any],
                             payment_manager) -> Action:
        """Crea una acción para operaciones de pago"""
        
        def execute_payment_action(data):
            if action_type == 'process_payment':
                return payment_manager.process_payment(
                    data['user_id'], data['amount'], data['payment_type']
                )
            elif action_type == 'refund_payment':
                return payment_manager.refund_payment(data['payment_id'])
        
        def undo_payment_action(data):
            if action_type == 'process_payment':
                return payment_manager.refund_payment(data['payment_id'])
            elif action_type == 'refund_payment':
                return payment_manager.restore_payment(data['payment_id'])
        
        return Action(
            action_type=action_type,
            description=description,
            execute_func=execute_payment_action,
            undo_func=undo_payment_action,
            data=payment_data,
            context='payments'
        )
    
    def create_routine_action(self, 
                             action_type: str,
                             description: str,
                             routine_data: Dict[str, Any],
                             database_manager) -> Action:
        """Crea una acción para operaciones de rutina"""
        
        def execute_routine_action(data):
            if action_type == 'assign_routine':
                return database_manager.assign_routine_to_user(
                    data['user_id'], data['routine_id']
                )
            elif action_type == 'unassign_routine':
                return database_manager.unassign_routine_from_user(
                    data['user_id'], data['routine_id']
                )
        
        def undo_routine_action(data):
            if action_type == 'assign_routine':
                return database_manager.unassign_routine_from_user(
                    data['user_id'], data['routine_id']
                )
            elif action_type == 'unassign_routine':
                return database_manager.assign_routine_to_user(
                    data['user_id'], data['routine_id']
                )
        
        return Action(
            action_type=action_type,
            description=description,
            execute_func=execute_routine_action,
            undo_func=undo_routine_action,
            data=routine_data,
            context='routines'
        )
    
    def show_history_dialog(self, parent_widget: QWidget = None):
        """Muestra un diálogo con el historial de acciones"""
        try:
            from PyQt6.QtWidgets import QDialog, QVBoxLayout, QListWidget, QHBoxLayout, QPushButton
            
            dialog = QDialog(parent_widget)
            dialog.setWindowTitle("Historial de Acciones")
            dialog.setMinimumSize(400, 300)
            
            layout = QVBoxLayout(dialog)
            
            # Lista de acciones
            action_list = QListWidget()
            for i, action in enumerate(self.history):
                item_text = f"{action.description} - {action.timestamp.strftime('%H:%M:%S')}"
                if i == self.current_index:
                    item_text += " (Actual)"
                elif i > self.current_index:
                    item_text += " (Deshecha)"
                action_list.addItem(item_text)
            
            layout.addWidget(action_list)
            
            # Botones
            button_layout = QHBoxLayout()
            
            undo_btn = QPushButton("Deshacer")
            undo_btn.setEnabled(self.can_undo())
            undo_btn.clicked.connect(lambda: self.undo() and dialog.accept())
            
            redo_btn = QPushButton("Rehacer")
            redo_btn.setEnabled(self.can_redo())
            redo_btn.clicked.connect(lambda: self.redo() and dialog.accept())
            
            clear_btn = QPushButton("Limpiar Historial")
            clear_btn.clicked.connect(lambda: self.clear_history() or dialog.accept())
            
            close_btn = QPushButton("Cerrar")
            close_btn.clicked.connect(dialog.accept)
            
            button_layout.addWidget(undo_btn)
            button_layout.addWidget(redo_btn)
            button_layout.addWidget(clear_btn)
            button_layout.addWidget(close_btn)
            
            layout.addLayout(button_layout)
            
            dialog.exec()
            
        except Exception as e:
            logging.error(f"Error mostrando diálogo de historial: {e}")

# Instancia global del gestor de historial
action_history_manager = None

def initialize_action_history_manager(max_size: int = 100):
    """Inicializa el gestor de historial global"""
    global action_history_manager
    action_history_manager = ActionHistoryManager(max_size)
    return action_history_manager

def get_action_history_manager():
    """Obtiene la instancia global del gestor de historial"""
    return action_history_manager