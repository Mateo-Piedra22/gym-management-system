import os
import shutil
from PyQt6.QtWidgets import (QDialog, QMessageBox, QLabel, QLineEdit, QFormLayout,
                             QDialogButtonBox, QCheckBox, QRadioButton, QHBoxLayout,
                             QVBoxLayout, QPushButton, QFileDialog, QTextEdit, QComboBox)
from PyQt6.QtGui import QPixmap
from PyQt6.QtCore import Qt
from models import Usuario
from database import DatabaseManager

class UserDialog(QDialog):
    def __init__(self, parent=None, user: Usuario = None, db_manager: DatabaseManager = None):
        super().__init__(parent)
        
        self.user = user
        self.db_manager = db_manager
        # Estado para permisos de edición de PIN y valores originales
        self._is_pin_edit_allowed = True
        self._original_pin = None
        self.setWindowTitle("Modificar Usuario" if self.user else "Agregar Nuevo Usuario")

        main_layout = QVBoxLayout(self)
        form_layout = QFormLayout()
        
        self.nombre_input = QLineEdit()
        self.dni_input = QLineEdit()
        self.telefono_input = QLineEdit()
        self.pin_input = QLineEdit()
        self.pin_input.setMaxLength(4)
        
        form_layout.addRow(QLabel("Nombre Completo:"), self.nombre_input)
        form_layout.addRow(QLabel("DNI (8 dígitos):"), self.dni_input)
        form_layout.addRow(QLabel("Teléfono:"), self.telefono_input)
        form_layout.addRow(QLabel("PIN (4 dígitos):"), self.pin_input)
        
        # --- MODIFICACIÓN: Selector de Rol ---
        self.rol_combobox = QComboBox()
        self.rol_combobox.addItem("👤 Socio", "socio")
        self.rol_combobox.addItem("🎓 Profesor", "profesor")
        # El rol de dueño no se puede asignar desde aquí para evitar conflictos
        form_layout.addRow(QLabel("Rol en el Sistema:"), self.rol_combobox)
        # --- FIN DE MODIFICACIÓN ---

        # Selector dinámico de tipos de cuota
        self.tipo_cuota_combobox = QComboBox()
        self.load_tipos_cuota()
        form_layout.addRow(QLabel("Tipo de Cuota:"), self.tipo_cuota_combobox)

        self.activo_checkbox = QCheckBox("Usuario Activo")
        form_layout.addRow(self.activo_checkbox)
        
        self.notas_input = QTextEdit()
        self.notas_input.setPlaceholderText("Anotaciones sobre el socio (lesiones, preferencias, etc.)")
        
        main_layout.addLayout(form_layout)
        main_layout.addWidget(QLabel("Notas Adicionales:"))
        main_layout.addWidget(self.notas_input)
        
        self.buttonBox = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        main_layout.addWidget(self.buttonBox)

        self.buttonBox.accepted.connect(self.accept)
        self.buttonBox.rejected.connect(self.reject)
        
        if self.user:
            self.load_user_data()
        else:
            self.telefono_input.setText("+54 ")
            self.pin_input.setText("1234")
            # Seleccionar el primer tipo de cuota por defecto
            if self.tipo_cuota_combobox.count() > 0:
                self.tipo_cuota_combobox.setCurrentIndex(0)
            self.activo_checkbox.setChecked(True)

    def _apply_role_pin_permissions(self):
        """Aplica restricciones para edición de PIN cuando el rol actual es 'profesor'."""
        # Helper local para obtener referencia confiable a MainWindow
        def _get_main_window_ref(widget):
            try:
                win = widget.window()
                if win and (hasattr(win, 'user_role') or hasattr(win, 'logged_in_user')):
                    return win
            except Exception:
                pass
            # Fallback: recorrer cadena de padres
            try:
                parent = widget.parent()
                depth = 0
                while parent is not None and depth < 6:
                    if hasattr(parent, 'user_role') or hasattr(parent, 'logged_in_user'):
                        return parent
                    parent = getattr(parent, 'parent', lambda: None)()
                    depth += 1
            except Exception:
                pass
            # Fallback adicional: buscar entre ventanas de nivel superior
            try:
                from PyQt6.QtWidgets import QApplication
                app = QApplication.instance()
                if app:
                    for w in app.topLevelWidgets():
                        if hasattr(w, 'user_role') or hasattr(w, 'logged_in_user'):
                            return w
            except Exception:
                pass
            return None

        try:
            main_window = _get_main_window_ref(self)
            current_role = getattr(main_window, 'user_role', None)
            logged_in_user = getattr(main_window, 'logged_in_user', None)

            # Resolver ID del usuario logueado (dict u objeto)
            current_user_id = None
            if isinstance(logged_in_user, dict):
                current_user_id = logged_in_user.get('usuario_id') or logged_in_user.get('id')
            else:
                current_user_id = getattr(logged_in_user, 'id', None)

            # Guardar PIN original para verificar cambios
            self._original_pin = (self.user.pin or "") if self.user else ""

            # Solo restringir cuando: el usuario logueado es profesor y el usuario editado también es profesor
            # Profesores pueden modificar el PIN de socios; solo se bloquea PIN de otros profesores
            if current_role == 'profesor' and self.user and getattr(self.user, 'rol', None) == 'profesor':
                # Normalizar IDs para comparación robusta
                try:
                    edited_id = int(self.user.id) if self.user.id is not None else None
                except Exception:
                    edited_id = self.user.id
                try:
                    current_id = int(current_user_id) if current_user_id is not None else None
                except Exception:
                    current_id = current_user_id

                if edited_id != current_id:
                    # No permitir que un profesor modifique el PIN de otro usuario
                    self._is_pin_edit_allowed = False
                    self.pin_input.setEnabled(False)
                    self.pin_input.setEchoMode(QLineEdit.EchoMode.Password)
                    # Ocultar el PIN actual: limpiar el campo y mostrar placeholder
                    try:
                        self.pin_input.clear()
                        self.pin_input.setPlaceholderText("PIN oculto")
                    except Exception:
                        pass
                    self.pin_input.setToolTip("No puede modificar el PIN de otro profesor. Solo su propio PIN.")
                else:
                    # Profesor editando su propio usuario: permitir
                    self._is_pin_edit_allowed = True
                    self.pin_input.setEnabled(True)
            else:
                # Dueño/administrador u otros roles: sin restricción
                self._is_pin_edit_allowed = True
                self.pin_input.setEnabled(True)
        except Exception:
            # En caso de cualquier error, no bloquear la edición
            self._is_pin_edit_allowed = True
            try:
                self.pin_input.setEnabled(True)
            except Exception:
                pass

    def load_tipos_cuota(self):
        """Carga los tipos de cuota disponibles desde la base de datos"""
        self.tipo_cuota_combobox.clear()
        
        if not self.db_manager:
            # Fallback a tipos por defecto si no hay conexión a BD
            self.tipo_cuota_combobox.addItem("💰 Estándar", "estandar")
            self.tipo_cuota_combobox.addItem("🎓 Estudiante", "estudiante")
            return
        
        try:
            # Obtener tipos de cuota activos desde la base de datos
            tipos_cuota = self.db_manager.obtener_tipos_cuota(solo_activos=True)
            
            if not tipos_cuota:
                # Si no hay tipos en la BD, usar los por defecto
                self.tipo_cuota_combobox.addItem("💰 Estándar", "estandar")
                self.tipo_cuota_combobox.addItem("🎓 Estudiante", "estudiante")
                return
            
            # Agregar tipos de cuota desde la base de datos
            for tipo in tipos_cuota:
                # Usar el icono del tipo de cuota si está disponible
                icono = tipo.icono_path if hasattr(tipo, 'icono_path') and tipo.icono_path else "💰"
                # Si el icono es un archivo, usar un emoji por defecto
                if icono and not icono.startswith(("💰", "🎓", "👑", "⭐", "🏆", "🎯", "💎", "🔥")):
                    icono = "💰"
                
                texto = f"{icono} {tipo.nombre} (${tipo.precio:.0f})"
                # Usar el nombre del tipo como valor de datos
                self.tipo_cuota_combobox.addItem(texto, tipo.nombre.lower())
                
        except Exception as e:
            print(f"Error al cargar tipos de cuota: {e}")
            # Fallback en caso de error - usar valor por defecto genérico
            self.tipo_cuota_combobox.addItem("💰 Cuota General", "general")

    def load_user_data(self):
        self.nombre_input.setText(self.user.nombre)
        self.dni_input.setText(self.user.dni or "")
        self.telefono_input.setText(self.user.telefono)
        self.pin_input.setText(self.user.pin or "1234")
        # Registrar PIN original para comparaciones de permisos
        self._original_pin = self.user.pin or ""
        self.activo_checkbox.setChecked(self.user.activo)
        self.notas_input.setText(self.user.notas or "")
        
        # Cargar rol
        rol_index = self.rol_combobox.findData(self.user.rol)
        if rol_index >= 0:
            self.rol_combobox.setCurrentIndex(rol_index)
        
        # Si el usuario es el dueño, deshabilitar cambios de rol
        if self.user.rol == 'dueño':
            self.rol_combobox.setEnabled(False)
            self.nombre_input.setReadOnly(True)
            self.dni_input.setReadOnly(True)

        # Cargar tipo de cuota
        if hasattr(self.user, 'tipo_cuota') and self.user.tipo_cuota:
            # Buscar el tipo de cuota en el combobox
            for i in range(self.tipo_cuota_combobox.count()):
                if self.tipo_cuota_combobox.itemData(i) == self.user.tipo_cuota:
                    self.tipo_cuota_combobox.setCurrentIndex(i)
                    break

        # Aplicar permisos de edición de PIN según el rol actual
        self._apply_role_pin_permissions()

    def get_user_data(self) -> Usuario:
        if not self.user:
            self.user = Usuario()

        self.user.nombre = self.nombre_input.text().strip().upper()
        self.user.dni = self.dni_input.text().strip()
        self.user.telefono = self.telefono_input.text().strip()
        self.user.activo = self.activo_checkbox.isChecked()
        # Si no está permitido editar el PIN (profesor sobre otro profesor), mantener el original
        if not self._is_pin_edit_allowed:
            self.user.pin = self._original_pin or ""
        else:
            self.user.pin = self.pin_input.text().strip()
        self.user.rol = self.rol_combobox.currentData()
        self.user.tipo_cuota = self.tipo_cuota_combobox.currentData()
        self.user.notas = self.notas_input.toPlainText().strip()
        
        return self.user

    def accept(self):
        nombre = self.nombre_input.text().strip()
        dni = self.dni_input.text().strip()
        telefono = self.telefono_input.text().strip()
        # Determinar el PIN a validar/guardar según permisos
        if not self._is_pin_edit_allowed:
            # Mantener el PIN original y omitir validación visual del campo
            pin = self._original_pin or ""
        else:
            pin = self.pin_input.text().strip()

        if not nombre or not dni or not telefono:
            QMessageBox.warning(self, "Datos Incompletos", "Nombre, DNI y Teléfono son campos obligatorios.")
            return

        # El DNI '00000000' se reserva para el dueño
        if dni == "00000000":
             user_id_to_ignore = self.user.id if self.user else None
             owner = self.db_manager.obtener_usuario_por_rol('dueño')
             if not owner or not user_id_to_ignore or owner.id != user_id_to_ignore:
                QMessageBox.warning(self, "DNI Reservado", "El DNI '00000000' está reservado para el sistema.")
                return

        if not dni.isdigit() or len(dni) != 8:
            QMessageBox.warning(self, "DNI Inválido", "El DNI debe ser numérico y tener exactamente 8 dígitos.")
            return

        if self.db_manager:
            user_id_to_ignore = self.user.id if self.user else None
            if self.db_manager.dni_existe(dni, user_id_to_ignore):
                QMessageBox.warning(self, "DNI Duplicado", "El DNI ingresado ya pertenece a otro usuario.")
                return

        # Validación de PIN sólo cuando se permite editar
        if self._is_pin_edit_allowed:
            if not pin.isdigit() or len(pin) != 4:
                QMessageBox.warning(self, "PIN Inválido", "El PIN debe ser numérico y tener exactamente 4 dígitos.")
                return

        # Validación adicional: si el rol es profesor y no está permitido cambiar el PIN,
        # impedir que se guarden cambios en el PIN de otro profesor
        # Protección adicional si por algún motivo el campo permitió cambios
        if not self._is_pin_edit_allowed and self._original_pin is not None:
            if pin != (self._original_pin or ""):
                QMessageBox.warning(self, "Acción No Permitida", "No puede modificar el PIN de otro profesor. Solo su propio PIN.")
                return

        super().accept()

