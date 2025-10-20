import sys
import os
import subprocess
import logging
import pandas as pd
import time
from datetime import datetime
from typing import List, Optional, Any
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QGridLayout, QGroupBox, QLabel, 
    QComboBox, QSpinBox, QDoubleSpinBox, QPushButton, QTableView, QFrame,
    QCheckBox, QMessageBox, QFileDialog, QMenu, QScrollArea,
    QSizePolicy, QSplitter, QTabWidget, QTextEdit, QDateEdit, QLineEdit,
    QProgressBar, QToolButton, QButtonGroup, QStackedWidget, QStyledItemDelegate,
    QDialog
)
from PyQt6.QtCore import Qt, QAbstractTableModel, QVariant, pyqtSignal, QDate, QTimer
from PyQt6.QtGui import QFont, QColor, QPixmap, QIcon, QAction
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from widgets.payment_dialog import PaymentDialog
from widgets.unified_filter_widget import UnifiedFilterButton
from widgets.advanced_filter_widget import FilterField
from pdf_generator import PDFGenerator
from utils_modules.async_runner import TaskThread
from utils_modules.async_utils import run_in_background
from utils_modules.users_loader import load_users_cached_async
from utils_modules.ui_constants import (
    PLACEHOLDER_SELECT_USER,
    PLACEHOLDER_LOADING_USERS,
)

class PaymentHistoryModel(QAbstractTableModel):
    """Modelo optimizado para el historial de pagos con mejor rendimiento"""
    
    def __init__(self, payment_manager=None):
        super().__init__()
        self._data = []
        self._headers = ["Período", "Monto", "Método", "Fecha", "Estado"]
        self.payment_manager = payment_manager
    
    def rowCount(self, parent=None):
        return len(self._data)
    
    def columnCount(self, parent=None):
        return len(self._headers)
    
    def data(self, index, role=Qt.ItemDataRole.DisplayRole):
        if not index.isValid() or index.row() >= len(self._data):
            return QVariant()
        
        pago = self._data[index.row()]
        col = index.column()
        
        if role == Qt.ItemDataRole.DisplayRole:
            if col == 0:  # Período
                return f"{pago.mes:02d}/{pago.año}"
            elif col == 1:  # Monto
                return f"${pago.monto:,.2f}"
            elif col == 2:  # Método
                if hasattr(pago, 'metodo_pago_id') and pago.metodo_pago_id and self.payment_manager:
                    metodo = self.payment_manager.obtener_metodo_pago(pago.metodo_pago_id)
                    return metodo.nombre if metodo else 'N/A'
                return 'Efectivo'  # Valor por defecto para pagos antiguos
            elif col == 3:  # Fecha
                if isinstance(pago.fecha_pago, str):
                    fecha = datetime.fromisoformat(pago.fecha_pago)
                else:
                    fecha = pago.fecha_pago
                return fecha.strftime("%d/%m/%Y") if fecha else "Sin fecha"
            elif col == 4:  # Estado
                return "Pagado"
        
        elif role == Qt.ItemDataRole.TextAlignmentRole:
            if col == 1:  # Monto
                return Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter
            return Qt.AlignmentFlag.AlignCenter
        
        return QVariant()
    
    def headerData(self, section, orientation, role=Qt.ItemDataRole.DisplayRole):
        if orientation == Qt.Orientation.Horizontal and role == Qt.ItemDataRole.DisplayRole:
            return self._headers[section]
        return QVariant()
    
    def update_data(self, new_data):
        """Actualiza los datos del modelo de forma eficiente"""
        self.beginResetModel()
        self._data = new_data
        self.endResetModel()

class PaymentConceptsTableModel(QAbstractTableModel):
    """Modelo para la tabla de conceptos de pago"""
    
    concept_changed = pyqtSignal()
    
    def __init__(self):
        super().__init__()
        self._data = []
        self._headers = ["Activo", "Concepto", "Cantidad", "Precio Unit. ($)", "Total ($)"]
        self._checked_concepts = set()
        self._quantities = {}
        self._prices = {}
    
    def rowCount(self, parent=None):
        return len(self._data)
    
    def columnCount(self, parent=None):
        return len(self._headers)
    
    def data(self, index, role=Qt.ItemDataRole.DisplayRole):
        if not index.isValid() or index.row() >= len(self._data):
            return QVariant()
        
        concept = self._data[index.row()]
        col = index.column()
        
        if role == Qt.ItemDataRole.DisplayRole:
            if col == 0:  # Activo - checkbox manejado por delegate
                return QVariant()
            elif col == 1:  # Concepto
                return concept.nombre
            elif col == 2:  # Cantidad
                return self._quantities.get(concept.id, 1)
            elif col == 3:  # Precio Unit.
                return f"{self._prices.get(concept.id, concept.precio_base):.2f}"
            elif col == 4:  # Total
                if concept.id in self._checked_concepts:
                    qty = self._quantities.get(concept.id, 1)
                    price = self._prices.get(concept.id, concept.precio_base)
                    return f"{qty * price:.2f}"
                return "0.00"
        
        elif role == Qt.ItemDataRole.CheckStateRole and col == 0:
            return Qt.CheckState.Checked if concept.id in self._checked_concepts else Qt.CheckState.Unchecked
        
        elif role == Qt.ItemDataRole.TextAlignmentRole:
            if col in [2, 3, 4]:  # Cantidad, Precio, Total
                return Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter
            elif col == 0:  # Checkbox
                return Qt.AlignmentFlag.AlignCenter
            return Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter
        
        return QVariant()
    
    def headerData(self, section, orientation, role=Qt.ItemDataRole.DisplayRole):
        if orientation == Qt.Orientation.Horizontal and role == Qt.ItemDataRole.DisplayRole:
            return self._headers[section]
        return QVariant()
    
    def flags(self, index):
        if not index.isValid():
            return Qt.ItemFlag.NoItemFlags
        
        flags = Qt.ItemFlag.ItemIsEnabled | Qt.ItemFlag.ItemIsSelectable
        
        if index.column() == 0:  # Checkbox column
            flags |= Qt.ItemFlag.ItemIsUserCheckable
        elif index.column() in [2, 3]:  # Cantidad y Precio columns
            concept = self._data[index.row()]
            if concept.id in self._checked_concepts:
                flags |= Qt.ItemFlag.ItemIsEditable
        
        return flags
    
    def setData(self, index, value, role=Qt.ItemDataRole.EditRole):
        if not index.isValid() or index.row() >= len(self._data):
            return False
        
        concept = self._data[index.row()]
        col = index.column()
        
        if role == Qt.ItemDataRole.CheckStateRole and col == 0:
            if value == Qt.CheckState.Checked:
                self._checked_concepts.add(concept.id)
                # Inicializar valores por defecto
                if concept.id not in self._quantities:
                    self._quantities[concept.id] = 1
                if concept.id not in self._prices:
                    self._prices[concept.id] = concept.precio_base
            else:
                self._checked_concepts.discard(concept.id)
            
            self.dataChanged.emit(index, index.sibling(index.row(), 4))
            self.concept_changed.emit()
            return True
        
        elif role == Qt.ItemDataRole.EditRole:
            if col == 2:  # Cantidad
                try:
                    qty = max(1, int(value))
                    self._quantities[concept.id] = qty
                    self.dataChanged.emit(index, index.sibling(index.row(), 4))
                    self.concept_changed.emit()
                    return True
                except (ValueError, TypeError):
                    return False
            elif col == 3:  # Precio
                try:
                    price = max(0.0, float(value))
                    self._prices[concept.id] = price
                    self.dataChanged.emit(index, index.sibling(index.row(), 4))
                    self.concept_changed.emit()
                    return True
                except (ValueError, TypeError):
                    return False
        
        return False
    
    def update_data(self, concepts):
        """Actualiza los datos del modelo"""
        self.beginResetModel()
        self._data = concepts
        self.endResetModel()
    
    def get_selected_concepts(self):
        """Obtiene los conceptos seleccionados con sus datos"""
        selected = []
        for concept in self._data:
            if concept.id in self._checked_concepts:
                qty = self._quantities.get(concept.id, 1)
                price = self._prices.get(concept.id, concept.precio_base)
                # Convertir todos los valores a float para evitar conflictos entre float y Decimal
                try:
                    qty_float = float(qty)
                    price_float = float(price)
                    total = qty_float * price_float
                except (ValueError, TypeError) as e:
                    # En caso de error de conversión, usar valores por defecto
                    qty_float = 1.0
                    price_float = float(concept.precio_base) if hasattr(concept, 'precio_base') else 0.0
                    total = qty_float * price_float
                
                selected.append({
                    'concepto_id': concept.id,
                    'cantidad': int(qty_float),
                    'precio_unitario': price_float,
                    'total': total
                })
        return selected
    
    def set_user_price_for_concept(self, concept_name, price):
        """Establece el precio específico para un concepto basado en el usuario"""
        for row, concept in enumerate(self._data):
            if concept_name.lower() in concept.nombre.lower():
                # Marcar concepto como activo para que el precio/total se muestren
                self._checked_concepts.add(concept.id)
                if concept.id not in self._quantities:
                    self._quantities[concept.id] = 1

                # Intentar parsear el precio de forma robusta (int, float, Decimal, str con separadores)
                parsed_price = None
                try:
                    parsed_price = float(price)
                except Exception:
                    try:
                        # Manejo de cadenas con símbolos, espacios y separadores locales
                        if isinstance(price, str):
                            s = price.strip()
                            # Dejar solo dígitos y separadores de decimal
                            s = ''.join(ch for ch in s if ch.isdigit() or ch in ",.")
                            # Si hay punto y coma, asumir punto como miles y coma como decimal
                            if "." in s and "," in s:
                                s = s.replace(".", "").replace(",", ".")
                            else:
                                # Si sólo hay coma, tratarla como decimal
                                if "," in s and "." not in s:
                                    s = s.replace(",", ".")
                            parsed_price = float(s) if s else None
                    except Exception:
                        parsed_price = None

                if parsed_price is None:
                    # Fallback: mantener precio previo si existe, si no usar precio_base
                    prev = self._prices.get(concept.id)
                    if prev is not None:
                        try:
                            parsed_price = float(prev)
                        except Exception:
                            parsed_price = None
                    if parsed_price is None:
                        try:
                            parsed_price = float(getattr(concept, 'precio_base', 0.0) or 0.0)
                        except Exception:
                            parsed_price = 0.0

                self._prices[concept.id] = max(0.0, parsed_price)

                # Emitir actualización para columnas de Precio y Total
                index_price = self.index(row, 3)
                index_total = self.index(row, 4)
                self.dataChanged.emit(index_price, index_total)
                self.concept_changed.emit()
                break
    
    def reset_selections(self):
        """Resetea todas las selecciones"""
        self.beginResetModel()
        self._checked_concepts.clear()
        self._quantities.clear()
        self._prices.clear()
        # Reinicializar precios base
        for concept in self._data:
            self._prices[concept.id] = concept.precio_base
        self.endResetModel()
    
    def clear_selection(self):
        """Limpia la selección de conceptos (alias para reset_selections)"""
        self.reset_selections()

class CheckBoxDelegate(QStyledItemDelegate):
    """Delegado para manejar checkboxes en la tabla de conceptos"""
    
    def __init__(self, parent=None):
        super().__init__(parent)
    
    def createEditor(self, parent, option, index):
        # No crear editor para checkboxes, se manejan con clicks
        return None
    
    def editorEvent(self, event, model, option, index):
        # Manejar clicks en checkboxes
        if index.column() == 0:  # Solo para la columna de checkboxes
            if event.type() == event.Type.MouseButtonRelease:
                # Obtener el estado actual
                current_state = model.data(index, Qt.ItemDataRole.CheckStateRole)
                # Cambiar el estado
                new_state = Qt.CheckState.Unchecked if current_state == Qt.CheckState.Checked else Qt.CheckState.Checked
                # Aplicar el cambio
                return model.setData(index, new_state, Qt.ItemDataRole.CheckStateRole)
        return super().editorEvent(event, model, option, index)

class ModernPaymentCard(QFrame):
    """Tarjeta moderna para conceptos de pago con diseño mejorado - DEPRECATED"""
    
    concept_changed = pyqtSignal()
    
    def __init__(self, concept, index=0):
        super().__init__()
        self.concept = concept
        self.index = index
        self.setup_ui()
        self.setup_connections()
    
    def setup_ui(self):
        """Configura la interfaz de la tarjeta de concepto con un QGridLayout para mayor flexibilidad."""
        self.setFrameStyle(QFrame.Shape.StyledPanel)
        self.setMinimumHeight(100)
        self.setMaximumHeight(120)
        self.setMinimumWidth(500)  # Reducido de 750px para mejor responsividad
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        self.setProperty("concept_card", True)
        self.setProperty("card_index", self.index)
        self.setObjectName(f"concept_card_{self.index}")

        main_layout = QGridLayout(self)
        main_layout.setContentsMargins(15, 15, 15, 15)  # Márgenes más uniformes
        main_layout.setSpacing(15)  # Espaciado más consistente

        # Secciones
        self.concept_section = self.create_concept_section()
        self.quantity_section = self.create_quantity_section()
        self.price_section = self.create_price_section()
        self.total_section = self.create_total_section()

        # Añadir al layout con mejor distribución
        main_layout.addWidget(self.concept_section, 0, 0)
        main_layout.addWidget(self.quantity_section, 0, 1)
        main_layout.addWidget(self.price_section, 0, 2)
        main_layout.addWidget(self.total_section, 0, 3)

        # Configurar proporciones de columnas para mejor distribución
        main_layout.setColumnStretch(0, 3)  # Concepto toma más espacio
        main_layout.setColumnStretch(1, 1)  # Cantidad
        main_layout.setColumnStretch(2, 1)  # Precio
        main_layout.setColumnStretch(3, 1)  # Total
    
    def create_concept_section(self):
        """Crea la sección del concepto con checkbox y etiqueta para texto largo"""
        section = QFrame()
        section.setMinimumWidth(180)  # Reducido para mejor adaptabilidad
        section.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        
        layout = QHBoxLayout(section)
        layout.setContentsMargins(8, 8, 8, 8)  # Márgenes más uniformes
        layout.setSpacing(12)  # Espaciado más consistente
        
        self.checkbox = QCheckBox()
        self.checkbox.setProperty("concept_id", self.concept.id)
        self.checkbox.setProperty("base_price", self.concept.precio_base)
        self.checkbox.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
        self.checkbox.setFixedSize(20, 20)  # Tamaño fijo para consistencia
        
        self.concept_label = QLabel(self.concept.nombre)
        self.concept_label.setWordWrap(True)
        self.concept_label.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        self.concept_label.setToolTip(self.concept.nombre)
        self.concept_label.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
        
        layout.addWidget(self.checkbox)
        layout.addWidget(self.concept_label, 1)  # Factor de estiramiento para el label
        
        return section
    
    def create_quantity_section(self):
        """Crea la sección de cantidad"""
        section = QFrame()
        section.setMinimumWidth(90)  # Ancho mínimo más flexible
        section.setMaximumWidth(120)  # Ancho máximo para evitar expansión excesiva
        section.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Fixed)
        
        layout = QVBoxLayout(section)
        layout.setContentsMargins(8, 8, 8, 8)  # Márgenes uniformes
        layout.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.setSpacing(6)  # Espaciado más compacto
        
        label = QLabel("Cantidad")
        label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        label.setProperty("section_label", True)
        label.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        
        self.quantity_spinbox = QSpinBox()
        self.quantity_spinbox.setMinimum(1)
        self.quantity_spinbox.setMaximum(99)
        self.quantity_spinbox.setValue(1)
        self.quantity_spinbox.setEnabled(False)
        self.quantity_spinbox.setMinimumHeight(35)  # Altura mínima en lugar de fija
        self.quantity_spinbox.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        self.quantity_spinbox.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.quantity_spinbox.setProperty("concept_control", True)
        
        layout.addWidget(label)
        layout.addWidget(self.quantity_spinbox)
        return section
    
    def create_price_section(self):
        """Crea la sección de precio"""
        section = QFrame()
        section.setMinimumWidth(110)  # Ancho mínimo más flexible
        section.setMaximumWidth(140)  # Ancho máximo para evitar expansión excesiva
        section.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Fixed)
        
        layout = QVBoxLayout(section)
        layout.setContentsMargins(8, 8, 8, 8)  # Márgenes uniformes
        layout.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.setSpacing(6)  # Espaciado más compacto
        
        label = QLabel("Precio Unit.")
        label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        label.setProperty("section_label", True)
        label.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        
        self.price_spinbox = QDoubleSpinBox()
        self.price_spinbox.setMinimum(0.00)
        self.price_spinbox.setMaximum(999999.99)
        self.price_spinbox.setDecimals(2)
        self.price_spinbox.setSuffix(" $")
        self.price_spinbox.setValue(self.concept.precio_base)
        self.price_spinbox.setEnabled(False)
        self.price_spinbox.setMinimumHeight(35)  # Altura mínima en lugar de fija
        self.price_spinbox.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        self.price_spinbox.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.price_spinbox.setProperty("concept_control", True)
        
        layout.addWidget(label)
        layout.addWidget(self.price_spinbox)
        return section
    
    def create_total_section(self):
        """Crea la sección del total"""
        section = QFrame()
        section.setMinimumWidth(110)  # Ancho mínimo más flexible
        section.setMaximumWidth(140)  # Ancho máximo para evitar expansión excesiva
        section.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Fixed)
        
        layout = QVBoxLayout(section)
        layout.setContentsMargins(8, 8, 8, 8)  # Márgenes uniformes
        layout.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.setSpacing(6)  # Espaciado más compacto
        
        label = QLabel("Total")
        label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        label.setProperty("section_label", True)
        label.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        
        self.total_label = QLabel("$0.00")
        self.total_label.setMinimumHeight(35)  # Altura mínima en lugar de fija
        self.total_label.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        self.total_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.total_label.setProperty("total_display", True)
        self.total_label.setProperty("active", False)
        self.total_label.setProperty("concept_control", True)
        
        layout.addWidget(label)
        layout.addWidget(self.total_label)
        return section
    
    def setup_connections(self):
        """Configura las conexiones de señales"""
        self.checkbox.toggled.connect(self.on_concept_toggled)
        self.quantity_spinbox.valueChanged.connect(self.update_total)
        self.price_spinbox.valueChanged.connect(self.update_total)
    
    def on_concept_toggled(self, checked):
        """Maneja el toggle del concepto"""
        self.quantity_spinbox.setEnabled(checked)
        self.price_spinbox.setEnabled(checked)
        self.total_label.setProperty("active", checked)
        
        if not checked:
            self.total_label.setText("$0.00")
        else:
            self.update_total()
        
        # Actualizar estilos
        self.total_label.style().unpolish(self.total_label)
        self.total_label.style().polish(self.total_label)
        
        self.concept_changed.emit()
    
    def update_total(self):
        """Actualiza el total del concepto"""
        if self.checkbox.isChecked():
            total = self.quantity_spinbox.value() * self.price_spinbox.value()
            self.total_label.setText(f"${total:.2f}")
            self.concept_changed.emit()
    
    def get_concept_data(self):
        """Obtiene los datos del concepto si está seleccionado"""
        if not self.checkbox.isChecked():
            return None
        
        return {
            'concepto_id': self.concept.id,
            'cantidad': self.quantity_spinbox.value(),
            'precio_unitario': self.price_spinbox.value(),
            'total': self.quantity_spinbox.value() * self.price_spinbox.value()
        }
    
    def reset(self):
        """Resetea la tarjeta a su estado inicial"""
        self.checkbox.setChecked(False)
        self.quantity_spinbox.setValue(1)
        self.price_spinbox.setValue(self.concept.precio_base)
    
    def set_user_price(self, price):
        """Establece el precio específico para un usuario"""
        self.price_spinbox.setValue(price)

class PaymentSummaryWidget(QFrame):
    """Widget moderno para mostrar el resumen del pago con diseño mejorado"""
    
    def __init__(self):
        super().__init__()
        self.setup_ui()
    
    def setup_ui(self):
        """Configura la interfaz del resumen con diseño tipo título"""
        self.setFrameStyle(QFrame.Shape.StyledPanel)
        self.setMinimumHeight(120)
        # Permitir suficiente altura para dibujar los totales
        self.setMaximumHeight(280)
        # Evitar que quede "fijo" y se recorten los contenidos
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)
        self.setProperty("payment_summary", True)
        
        layout = QVBoxLayout(self)
        layout.setContentsMargins(15, 12, 15, 12)
        layout.setSpacing(10)
        
        # Título con estilo mejorado
        title_container = QFrame()
        title_container.setProperty("summary_title_container", True)
        title_container.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        
        title_layout = QHBoxLayout(title_container)
        title_layout.setContentsMargins(8, 6, 8, 6)
        
        title = QLabel("💰 Resumen del Pago")
        title.setProperty("summary_title", True)
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        title.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        
        title_layout.addWidget(title)
        
        # Contenedor principal para los totales
        totals_container = QFrame()
        totals_container.setProperty("summary_totals_container", True)
        totals_container.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        
        totals_layout = QGridLayout(totals_container)
        totals_layout.setSpacing(8)
        totals_layout.setContentsMargins(10, 8, 10, 8)
        
        # Subtotal
        subtotal_label = QLabel("Subtotal:")
        subtotal_label.setProperty("summary_label", True)
        subtotal_label.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
        self.subtotal_value = QLabel("$0.00")
        self.subtotal_value.setProperty("summary_value", True)
        self.subtotal_value.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        
        # Comisión
        commission_label = QLabel("Comisión:")
        commission_label.setProperty("summary_label", True)
        commission_label.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
        self.commission_value = QLabel("$0.00")
        self.commission_value.setProperty("summary_value", True)
        self.commission_value.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        
        # Separador visual
        separator = QFrame()
        separator.setFrameShape(QFrame.Shape.HLine)
        separator.setProperty("summary_separator", True)
        
        # Total con estilo destacado
        total_container = QFrame()
        total_container.setProperty("summary_total_container", True)
        total_container.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        
        total_layout = QVBoxLayout(total_container)
        total_layout.setContentsMargins(8, 6, 8, 6)
        total_layout.setSpacing(6)
        
        # Fila del total calculado
        calculated_row = QHBoxLayout()
        total_label = QLabel("TOTAL CALCULADO:")
        total_label.setProperty("summary_total_label", True)
        total_label.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
        
        self.total_value = QLabel("$0.00")
        self.total_value.setProperty("summary_total_value", True)
        self.total_value.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        
        calculated_row.addWidget(total_label)
        calculated_row.addStretch()
        calculated_row.addWidget(self.total_value)
        
        # Fila del monto personalizado
        custom_row = QHBoxLayout()
        custom_label = QLabel("MONTO FINAL:")
        custom_label.setProperty("summary_total_label", True)
        custom_label.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
        
        self.custom_amount_input = QDoubleSpinBox()
        self.custom_amount_input.setRange(0.0, 999999.99)
        self.custom_amount_input.setDecimals(2)
        self.custom_amount_input.setPrefix("$")
        self.custom_amount_input.setMinimumWidth(100)
        self.custom_amount_input.setProperty("custom_amount_input", True)
        
        custom_row.addWidget(custom_label)
        custom_row.addStretch()
        custom_row.addWidget(self.custom_amount_input)
        
        total_layout.addLayout(calculated_row)
        total_layout.addLayout(custom_row)
        
        # Agregar elementos al grid
        totals_layout.addWidget(subtotal_label, 0, 0)
        totals_layout.addWidget(self.subtotal_value, 0, 1)
        totals_layout.addWidget(commission_label, 1, 0)
        totals_layout.addWidget(self.commission_value, 1, 1)
        totals_layout.addWidget(separator, 2, 0, 1, 2)
        totals_layout.addWidget(total_container, 3, 0, 1, 2)
        
        # Configurar columnas
        totals_layout.setColumnStretch(0, 1)
        totals_layout.setColumnStretch(1, 1)
        
        layout.addWidget(title_container)
        layout.addWidget(totals_container)
    
    def update_summary(self, subtotal, commission, total):
        """Actualiza el resumen con nuevos valores"""
        self.subtotal_value.setText(f"${subtotal:,.2f}")
        self.commission_value.setText(f"${commission:,.2f}")
        self.total_value.setText(f"${total:,.2f}")
        
        # Actualizar el campo personalizado solo si no está siendo editado por el usuario
        if not self.custom_amount_input.hasFocus():
            self.custom_amount_input.setValue(total)
    
    def get_final_amount(self):
        """Obtiene el monto final (personalizado o calculado)"""
        return self.custom_amount_input.value()
    
    def set_custom_amount_changed_callback(self, callback):
        """Establece el callback para cuando cambie el monto personalizado"""
        self.custom_amount_input.valueChanged.connect(callback)

class PaymentsTabWidget(QWidget):
    """Widget principal de la pestaña Pagos completamente reestructurado"""
    
    pagos_modificados = pyqtSignal()
    
    def __init__(self, db_manager, payment_manager):
        super().__init__()
        self.db_manager = db_manager
        self.payment_manager = payment_manager
        self.pdf_generator = None  # Se inicializará después
        self.selected_user = None
        self.all_users = []
        # TTL caches para evitar consultas repetidas en poco tiempo
        self._ttl_seconds_concepts = 20
        self._ttl_seconds_history = 15
        self._concepts_cache = {"data": None, "ts": 0}
        self._history_cache = {"user_id": None, "data": None, "ts": 0}
        
        # Inicializar modelo de conceptos
        self.concepts_model = PaymentConceptsTableModel()
        
        self.setup_ui()
        self.setup_connections()
        self.initialize_pdf_generator()
        self.load_initial_data()
        self.apply_modern_branding()
    
    def setup_ui(self):
        """Configura la interfaz principal con diseño modular y responsivo"""
        # Configurar políticas de tamaño del widget principal
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        self.setMinimumSize(1000, 600)  # Tamaño mínimo para evitar elementos cortados
        
        # Layout principal optimizado
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(8, 8, 8, 8)  # Márgenes más pequeños para aprovechar espacio
        main_layout.setSpacing(12)  # Espaciado optimizado
        
        # Header con información del usuario
        self.user_header = self.create_user_header()
        main_layout.addWidget(self.user_header)
        
        # Splitter principal para dividir contenido
        main_splitter = QSplitter(Qt.Orientation.Horizontal)
        main_splitter.setChildrenCollapsible(False)
        main_splitter.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        main_splitter.setHandleWidth(6)  # Ancho del divisor más delgado
        
        # Panel izquierdo: Registro de pagos
        self.payment_panel = self.create_payment_panel()
        main_splitter.addWidget(self.payment_panel)
        
        # Panel derecho: Historial y acciones
        self.history_panel = self.create_history_panel()
        main_splitter.addWidget(self.history_panel)
        
        # Configurar proporciones del splitter de forma más flexible
        main_splitter.setSizes([550, 450])  # Proporciones más equilibradas
        main_splitter.setStretchFactor(0, 2)  # Panel de registro tiene prioridad
        main_splitter.setStretchFactor(1, 1)  # Panel de historial secundario
        
        # Configurar tamaños mínimos para evitar colapso
        self.payment_panel.setMinimumWidth(400)
        self.history_panel.setMinimumWidth(350)
        
        main_layout.addWidget(main_splitter)
        # Posicionar overlay de estado tras construir el layout
        QTimer.singleShot(0, self.position_payment_status_overlay)
        # Inicializar valores del resumen para que no quede vacío
        QTimer.singleShot(0, self.calculate_totals)
    
    def create_user_header(self):
        """Crea un header de usuario flexible y bien espaciado."""
        header = QFrame()
        header.setFrameStyle(QFrame.Shape.StyledPanel)
        header.setProperty("user_header", True)
        header.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        header.setMinimumHeight(60)  # Altura mínima para consistencia
        header.setMaximumHeight(80)  # Altura máxima para evitar expansión excesiva

        layout = QHBoxLayout(header)
        layout.setContentsMargins(20, 15, 20, 15)
        layout.setSpacing(15)  # Espaciado más consistente

        user_label = QLabel("Usuario:")
        user_label.setProperty("header_label", True)
        user_label.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)

        self.user_combobox = QComboBox()
        self.user_combobox.setEditable(True)
        self.user_combobox.setMinimumWidth(250)  # Ancho mínimo más flexible
        self.user_combobox.setMaximumWidth(400)  # Ancho máximo para evitar expansión excesiva
        self.user_combobox.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        self.user_combobox.setMinimumHeight(35)  # Altura mínima en lugar de fija
        self.user_combobox.setProperty("user_selector", True)

        self.payment_status_label = QLabel(PLACEHOLDER_SELECT_USER)
        # Unificar estilos con User Tab y consolidarlo dentro del header (no flotante)
        self.payment_status_label.setObjectName("payment_status_label")
        self.payment_status_label.setProperty("payment_status", True)
        self.payment_status_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.payment_status_label.setWordWrap(False)  # Mantener siempre en una sola línea
        self.payment_status_label.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
        self.payment_status_label.setMinimumHeight(50)
        self.payment_status_label.setMinimumWidth(420)  # Un poco más largo para evitar salto de línea
        self.payment_status_label.setMaximumWidth(520)

        layout.addWidget(user_label)
        layout.addWidget(self.user_combobox, 1)
        layout.addStretch(1)
        layout.addWidget(self.payment_status_label)

        return header

    def position_payment_status_overlay(self):
        """Posiciona el label de estado como overlay en la esquina superior derecha del header."""
        try:
            if not hasattr(self, "payment_status_label") or self.payment_status_label is None:
                return
            if not hasattr(self, "user_header") or self.user_header is None:
                return
            header_geom = self.user_header.geometry()
            # Calcular tamaño preferido respetando límites
            hint = self.payment_status_label.sizeHint()
            width = min(hint.width(), self.payment_status_label.maximumWidth())
            height = max(hint.height(), self.payment_status_label.minimumHeight())
            self.payment_status_label.resize(width, height)
            # Márgenes sutiles respecto al header
            margin_right = 20
            # Subirlo un poco más para que no se corte por abajo
            margin_top = -6
            x = header_geom.right() - self.payment_status_label.width() - margin_right
            y = header_geom.top() + margin_top
            self.payment_status_label.move(x, y)
            self.payment_status_label.raise_()
        except Exception:
            pass

    
    
    def create_payment_panel(self):
        """Crea el panel de registro de pagos con scroll"""
        # Panel principal contenedor
        panel = QFrame()
        panel.setProperty("payment_panel", True)
        panel.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        
        # Layout principal del panel
        main_panel_layout = QVBoxLayout(panel)
        main_panel_layout.setContentsMargins(15, 15, 15, 10)  # Márgenes más uniformes
        main_panel_layout.setSpacing(20)  # Espaciado más consistente
        
        # Título del panel con contenedor mejorado
        title_container = QGroupBox()
        title_container.setProperty("title_container", True)
        title_container.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        title_layout = QVBoxLayout(title_container)
        title_layout.setContentsMargins(15, 15, 15, 15)  # Márgenes más uniformes
        
        title = QLabel("Registro de Pago")
        title.setProperty("panel_title", True)
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        title.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        title_layout.addWidget(title)
        
        main_panel_layout.addWidget(title_container)
        
        # Crear scroll area para el contenido
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        scroll_area.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        scroll_area.setFrameStyle(QFrame.Shape.NoFrame)
        scroll_area.setProperty("payment_scroll", True)
        scroll_area.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        
        # Widget contenedor del contenido scrolleable
        scroll_content = QWidget()
        scroll_content.setProperty("scroll_content", True)
        scroll_content.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        
        # Layout del contenido scrolleable
        content_layout = QVBoxLayout(scroll_content)
        content_layout.setContentsMargins(10, 10, 10, 15)  # Márgenes más uniformes
        content_layout.setSpacing(15)  # Espaciado más consistente
        content_layout.setAlignment(Qt.AlignmentFlag.AlignTop)
        
        # Formulario de pago
        form_group = self.create_payment_form()
        content_layout.addWidget(form_group)
        
        # Conceptos de pago
        concepts_group = self.create_concepts_section()
        content_layout.addWidget(concepts_group)
        
        # Resumen y botón de registro
        summary_section = self.create_summary_section()
        content_layout.addWidget(summary_section)
        
        # Agregar stretch para empujar contenido hacia arriba
        content_layout.addStretch()
        
        # Configurar el scroll area
        scroll_area.setWidget(scroll_content)
        main_panel_layout.addWidget(scroll_area)
        
        return panel
    
    def create_payment_form(self):
        """Crea el formulario de información de pago con un layout de grid mejorado."""
        group = QGroupBox("Información del Pago")
        group.setProperty("form_group", True)
        group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)

        layout = QGridLayout(group)
        layout.setContentsMargins(20, 30, 20, 20)  # Márgenes más uniformes
        layout.setSpacing(15)
        layout.setColumnStretch(1, 1)

        # Labels
        period_label = QLabel("Período:")
        period_label.setProperty("form_label", True)
        period_label.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
        
        method_label = QLabel("Método de Pago:")
        method_label.setProperty("form_label", True)
        method_label.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
        
        receipt_label = QLabel("Comprobante:")
        receipt_label.setProperty("form_label", True)
        receipt_label.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)

        # Controles de Período con tamaños más flexibles
        self.month_spinbox = QSpinBox()
        self.month_spinbox.setRange(1, 12)
        self.month_spinbox.setValue(datetime.now().month)
        self.month_spinbox.setMinimumWidth(70)  # Ancho mínimo en lugar de fijo
        self.month_spinbox.setMaximumWidth(90)  # Ancho máximo para evitar expansión excesiva
        self.month_spinbox.setMinimumHeight(35)  # Altura mínima en lugar de fija
        self.month_spinbox.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Fixed)
        self.month_spinbox.setAlignment(Qt.AlignmentFlag.AlignCenter)

        self.year_spinbox = QSpinBox()
        self.year_spinbox.setRange(2020, 2030)
        self.year_spinbox.setValue(datetime.now().year)
        self.year_spinbox.setMinimumWidth(80)  # Ancho mínimo en lugar de fijo
        self.year_spinbox.setMaximumWidth(110)  # Ancho máximo para evitar expansión excesiva
        self.year_spinbox.setMinimumHeight(35)  # Altura mínima en lugar de fija
        self.year_spinbox.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Fixed)
        self.year_spinbox.setAlignment(Qt.AlignmentFlag.AlignCenter)

        period_layout = QHBoxLayout()
        period_layout.setSpacing(10)  # Espaciado consistente
        period_layout.addWidget(self.month_spinbox)
        period_layout.addWidget(self.year_spinbox)
        period_layout.addStretch()

        # Control de Método de Pago con tamaño más flexible
        self.payment_method_combo = QComboBox()
        self.payment_method_combo.setMinimumWidth(200)  # Ancho mínimo en lugar de fijo
        self.payment_method_combo.setMaximumWidth(300)  # Ancho máximo para evitar expansión excesiva
        self.payment_method_combo.setMinimumHeight(35)  # Altura mínima en lugar de fija
        self.payment_method_combo.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        
        # Controles de Comprobante
        receipt_layout = QHBoxLayout()
        receipt_layout.setSpacing(10)
        
        # Mostrar próximo número de comprobante
        self.receipt_number_label = QLabel("Próximo: ---")
        self.receipt_number_label.setProperty("receipt_number", True)
        self.receipt_number_label.setMinimumHeight(35)
        self.receipt_number_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.receipt_number_label.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        
        # Botón para configurar numeración
        self.config_numbering_button = QPushButton("⚙️ Configurar")
        self.config_numbering_button.setMinimumHeight(35)
        self.config_numbering_button.setMinimumWidth(100)
        self.config_numbering_button.setMaximumWidth(120)
        self.config_numbering_button.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Fixed)
        self.config_numbering_button.setProperty("config_button", True)
        
        receipt_layout.addWidget(self.receipt_number_label)
        receipt_layout.addWidget(self.config_numbering_button)

        # Añadir widgets al layout
        layout.addWidget(period_label, 0, 0)
        layout.addLayout(period_layout, 0, 1)
        layout.addWidget(method_label, 1, 0)
        layout.addWidget(self.payment_method_combo, 1, 1)
        layout.addWidget(receipt_label, 2, 0)
        layout.addLayout(receipt_layout, 2, 1)

        return group
    
    def create_concepts_section(self):
        """Crea la sección de conceptos de pago con tabla moderna y responsiva"""
        group = QGroupBox("Conceptos de Pago")
        group.setProperty("concepts_group", True)
        group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        group.setMinimumHeight(250)  # Altura mínima para evitar colapso
        
        layout = QVBoxLayout(group)
        layout.setContentsMargins(15, 20, 15, 15)  # Márgenes optimizados
        layout.setSpacing(12)
        
        # Crear tabla de conceptos
        self.concepts_table = QTableView()
        self.concepts_model = PaymentConceptsTableModel()
        self.concepts_table.setModel(self.concepts_model)
        
        # Configurar delegado para checkboxes
        checkbox_delegate = CheckBoxDelegate()
        self.concepts_table.setItemDelegateForColumn(0, checkbox_delegate)
        
        # Configurar tabla con mejor responsividad
        self.concepts_table.setAlternatingRowColors(True)
        self.concepts_table.setSelectionBehavior(QTableView.SelectionBehavior.SelectRows)
        self.concepts_table.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        self.concepts_table.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        self.concepts_table.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        # Permitir expansión adaptativa de la tabla de conceptos
        # self.concepts_table.setMinimumHeight(180)  # Removido para permitir adaptación
        # self.concepts_table.setMaximumHeight(400)  # Removido para permitir expansión completa
        self.concepts_table.setProperty("concepts_table", True)
        
        # Configurar columnas con mejor adaptabilidad
        header = self.concepts_table.horizontalHeader()
        header.setStretchLastSection(True)  # Última columna se estira
        header.setSectionResizeMode(0, header.ResizeMode.Fixed)  # Activo
        header.setSectionResizeMode(1, header.ResizeMode.Stretch)  # Concepto
        header.setSectionResizeMode(2, header.ResizeMode.ResizeToContents)  # Cantidad
        header.setSectionResizeMode(3, header.ResizeMode.ResizeToContents)  # Precio
        header.setSectionResizeMode(4, header.ResizeMode.ResizeToContents)  # Total
        
        # Establecer anchos mínimos de columnas
        self.concepts_table.setColumnWidth(0, 50)   # Activo - más compacto
        header.setMinimumSectionSize(50)  # Ancho mínimo para todas las columnas
        
        # Configurar altura de filas
        self.concepts_table.verticalHeader().setDefaultSectionSize(35)
        self.concepts_table.verticalHeader().setVisible(False)  # Ocultar números de fila
        
        # Conectar señales
        self.concepts_model.concept_changed.connect(self.calculate_totals)
        
        # Placeholder no modal cuando no hay conceptos
        self.concepts_placeholder_label = QLabel("⚠️ No hay conceptos de pago configurados")
        self.concepts_placeholder_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.concepts_placeholder_label.setProperty("emptyState", "concepts")
        self.concepts_placeholder_label.setVisible(False)

        layout.addWidget(self.concepts_placeholder_label)
        layout.addWidget(self.concepts_table)
        
        return group
    
    def create_summary_section(self):
        """Crea la sección de resumen y registro con contenedores adaptativos"""
        section = QFrame()
        section.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)
        layout = QVBoxLayout(section)
        layout.setSpacing(12)
        layout.setContentsMargins(0, 8, 0, 10)
        
        # Widget de resumen con políticas adaptativas
        self.payment_summary = PaymentSummaryWidget()
        self.payment_summary.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)
        layout.addWidget(self.payment_summary)
        
        # Contenedor para el botón con políticas adaptativas
        button_container = QFrame()
        button_container.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)
        button_layout = QHBoxLayout(button_container)
        button_layout.setContentsMargins(10, 10, 10, 10)
        
        # Botón de registro con tamaño adaptativo
        self.register_button = QPushButton("💳 Registrar Pago")
        self.register_button.setMinimumHeight(50)
        self.register_button.setMinimumWidth(220)
        self.register_button.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        self.register_button.setEnabled(False)
        self.register_button.setProperty("register_button", True)
        
        button_layout.addWidget(self.register_button)
        layout.addWidget(button_container)
        
        return section
    
    def create_history_panel(self):
        """Crea el panel de historial de pagos con layout mejorado y responsivo"""
        panel = QFrame()
        panel.setProperty("history_panel", True)
        panel.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(12, 12, 12, 12)  # Márgenes optimizados
        layout.setSpacing(15)  # Espaciado reducido
        
        # Contenedor del título optimizado
        title_container = QGroupBox()
        title_container.setProperty("title_container", True)
        title_container.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        title_layout = QVBoxLayout(title_container)
        title_layout.setContentsMargins(12, 15, 12, 15)  # Márgenes reducidos
        
        title = QLabel("Historial de Pagos")
        title.setProperty("panel_title", True)
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        title.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        title_layout.addWidget(title)
        
        layout.addWidget(title_container)
        
        # Contenedor de acciones compacto
        actions_container = QFrame()
        actions_container.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        actions_layout = QHBoxLayout(actions_container)
        actions_layout.setContentsMargins(5, 8, 5, 8)  # Márgenes más pequeños
        actions_layout.setSpacing(10)  # Espaciado reducido
        
        # Barra de búsqueda para el historial de pagos
        self.history_search = QLineEdit()
        self.history_search.setObjectName("history_search")
        self.history_search.setPlaceholderText("Buscar por período, monto, método o fecha...")
        self.history_search.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Fixed)
        actions_layout.addWidget(self.history_search)
        actions_layout.addStretch()
        
        # Botones de exportación más compactos
        self.pdf_button = QPushButton("📄 PDF")
        self.pdf_button.setMinimumWidth(80)  # Más compacto
        self.pdf_button.setMaximumWidth(100)
        self.pdf_button.setMinimumHeight(32)  # Altura reducida
        self.pdf_button.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Fixed)
        self.pdf_button.setProperty("action_button", True)
        
        self.excel_button = QPushButton("Excel")
        self.excel_button.setMinimumWidth(80)  # Más compacto
        self.excel_button.setMaximumWidth(100)
        self.excel_button.setMinimumHeight(32)  # Altura reducida
        self.excel_button.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Fixed)
        self.excel_button.setProperty("action_button", True)
        
        actions_layout.addWidget(self.pdf_button)
        actions_layout.addWidget(self.excel_button)
        
        layout.addWidget(actions_container)
        
        # Indicadores de carga y vacío para historial
        self.history_loading_label = QLabel("Cargando historial de pagos...")
        self.history_loading_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.history_loading_label.setVisible(False)
        layout.addWidget(self.history_loading_label)

        self.history_empty_label = QLabel("Sin pagos para mostrar")
        self.history_empty_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.history_empty_label.setVisible(False)
        layout.addWidget(self.history_empty_label)

        # Barra de progreso indeterminada para carga de historial
        self.history_progress = QProgressBar()
        self.history_progress.setRange(0, 0)
        self.history_progress.setTextVisible(False)
        self.history_progress.setFixedHeight(8)
        self.history_progress.setVisible(False)
        layout.addWidget(self.history_progress)

        # Tabla de historial optimizada
        self.history_table = QTableView()
        self.history_model = PaymentHistoryModel(self.payment_manager)
        self.history_table.setModel(self.history_model)
        self.history_table.setAlternatingRowColors(True)
        self.history_table.setSelectionBehavior(QTableView.SelectionBehavior.SelectRows)
        self.history_table.setWordWrap(False)
        self.history_table.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        self.history_table.setMinimumHeight(200)  # Altura mínima
        self.history_table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)  # Habilitar menú contextual
        
        # Configurar header de la tabla
        header = self.history_table.horizontalHeader()
        header.setStretchLastSection(True)
        header.setSectionResizeMode(header.ResizeMode.ResizeToContents)
        header.setMinimumSectionSize(60)  # Ancho mínimo de columnas
        
        # Configurar altura de filas
        self.history_table.verticalHeader().setDefaultSectionSize(30)
        self.history_table.verticalHeader().setVisible(False)
        
        layout.addWidget(self.history_table)
        return panel
    
    def create_history_toolbar(self):
        """Crea la barra de herramientas del historial"""
        toolbar = QFrame()
        # Permitir adaptación flexible de la barra de herramientas
        # toolbar.setMinimumHeight(45)  # Removido para permitir adaptación
        # toolbar.setMaximumHeight(60)  # Removido para permitir expansión adaptativa
        toolbar.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        toolbar.setProperty("history_toolbar", True)
        
        layout = QHBoxLayout(toolbar)
        layout.setContentsMargins(10, 8, 10, 8)  # Márgenes más uniformes
        layout.setSpacing(12)  # Espaciado más consistente
        
        # Eliminado botón de filtros en la barra de historial; se usa la barra de búsqueda superior
        
        # Botones de exportación con tamaños más flexibles
        self.export_pdf_button = QPushButton("📄 PDF")
        self.export_pdf_button.setMinimumWidth(70)  # Ancho mínimo en lugar de fijo
        self.export_pdf_button.setMaximumWidth(90)  # Ancho máximo para evitar expansión excesiva
        self.export_pdf_button.setMinimumHeight(32)  # Altura mínima en lugar de fija
        self.export_pdf_button.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Fixed)
        self.export_pdf_button.setEnabled(False)
        self.export_pdf_button.setProperty("export_button", True)
        
        self.export_excel_button = QPushButton("Excel")
        self.export_excel_button.setMinimumWidth(70)  # Ancho mínimo en lugar de fijo
        self.export_excel_button.setMaximumWidth(90)  # Ancho máximo para evitar expansión excesiva
        self.export_excel_button.setMinimumHeight(32)  # Altura mínima en lugar de fija
        self.export_excel_button.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Fixed)
        self.export_excel_button.setEnabled(False)
        self.export_excel_button.setProperty("export_button", True)
        
        layout.addWidget(self.filter_button)
        layout.addStretch()
        layout.addWidget(self.export_pdf_button)
        layout.addWidget(self.export_excel_button)
        
        return toolbar
    
    def setup_connections(self):
        """Configura todas las conexiones de señales"""
        # Usuario
        self.user_combobox.currentIndexChanged.connect(self.on_user_selected)
        
        # Formulario
        self.month_spinbox.valueChanged.connect(self.calculate_totals)
        self.year_spinbox.valueChanged.connect(self.calculate_totals)
        self.payment_method_combo.currentIndexChanged.connect(self.on_payment_method_changed)
        
        # Registro
        self.register_button.clicked.connect(self.register_payment)
        
        # Historial
        self.history_table.customContextMenuRequested.connect(self.show_history_context_menu)
        
        # Exportación
        self.pdf_button.clicked.connect(lambda: self.exportar_historial('pdf'))
        self.excel_button.clicked.connect(lambda: self.exportar_historial('excel'))
        
        # Conectar barra de búsqueda del historial
        self.history_search.textChanged.connect(self.on_history_search_text_changed)
        
        # Monto personalizado
        self.payment_summary.set_custom_amount_changed_callback(self.on_custom_amount_changed)
        
        # Configuración de numeración de comprobantes
        self.config_numbering_button.clicked.connect(self.configure_receipt_numbering)
    
    def load_initial_data(self):
        """Carga los datos iniciales"""
        try:
            # Cargar usuarios en segundo plano para no bloquear la UI
            current_selection_id = self.selected_user.id if self.selected_user else None
            self._load_users_async(current_selection_id)
            
            # Cargar métodos de pago
            self.load_payment_methods()
            
            # Cargar conceptos de pago
            self.load_payment_concepts()
            
            # Cargar valores por defecto
            self.load_defaults()
            
        except Exception as e:
            QMessageBox.critical(self, "Error", "No se pudo cargar la lista de usuarios.")

    def _load_users_async(self, current_selection_id: Optional[int] = None):
        """Carga la lista de usuarios en hilo y actualiza el combobox al finalizar."""
        try:
            # Placeholder mientras carga
            self.user_combobox.blockSignals(True)
            self.user_combobox.clear()
            self.user_combobox.addItem(PLACEHOLDER_LOADING_USERS, userData=None)
            self.user_combobox.blockSignals(False)
            load_users_cached_async(
                self.db_manager,
                on_success=lambda usuarios: self._populate_users_combobox(usuarios, current_selection_id),
                on_error=lambda msg: self._populate_users_combobox([], current_selection_id, error=msg),
                parent=self,
            )
        except Exception:
            # Fallback sincrónico en caso de error iniciando hilo
            try:
                if hasattr(self.db_manager, 'obtener_usuarios_con_cache'):
                    usuarios = self.db_manager.obtener_usuarios_con_cache()
                else:
                    usuarios = self.db_manager.obtener_todos_usuarios()
            except Exception:
                usuarios = []
            self._populate_users_combobox(usuarios, current_selection_id)

    def _populate_users_combobox(self, usuarios: List[Any], current_selection_id: Optional[int] = None, error: Optional[str] = None):
        """Rellena el combobox de usuarios con la lista proporcionada."""
        try:
            self.all_users = usuarios or []
            self.user_combobox.blockSignals(True)
            self.user_combobox.clear()
            # Placeholder estándar
            self.user_combobox.addItem(PLACEHOLDER_SELECT_USER, userData=None)

            index_to_select = 0
            for i, user in enumerate(self.all_users, 1):
                uid = getattr(user, 'id', None)
                nombre = getattr(user, 'nombre', '')
                self.user_combobox.addItem(f"{uid} - {nombre}", userData=user)
                if current_selection_id and uid == current_selection_id:
                    index_to_select = i

            self.user_combobox.setCurrentIndex(index_to_select)
            if index_to_select == 0:
                if self.user_combobox.lineEdit():
                    self.user_combobox.lineEdit().setText("")
                self.reset_tab()

            self.user_combobox.blockSignals(False)

            if error:
                QMessageBox.warning(self, "Usuarios", f"No se pudo cargar la lista de usuarios: {error}")
        except Exception as e:
            try:
                self.user_combobox.blockSignals(False)
            except Exception:
                pass
            QMessageBox.warning(self, "Usuarios", f"Error mostrando usuarios: {e}")
    
    def load_payment_methods(self):
        """Carga los métodos de pago disponibles sin bloquear la UI"""
        try:
            def _load_methods():
                return self.payment_manager.obtener_metodos_pago()

            def _on_done(methods):
                try:
                    self.payment_method_combo.clear()
                    self.payment_method_combo.addItem("-- Seleccionar método de pago --", None)
                    for method in methods or []:
                        try:
                            self.payment_method_combo.addItem(method.nombre, method.id)
                        except Exception:
                            # Fallback seguro ante objetos parciales
                            self.payment_method_combo.addItem(str(method), getattr(method, 'id', None))
                except Exception as e:
                    QMessageBox.warning(self, "Advertencia", f"Error al aplicar métodos de pago: {str(e)}")

            def _on_error(err):
                QMessageBox.warning(self, "Advertencia", f"Error al cargar métodos de pago: {str(err)}")

            TaskThread(_load_methods, on_success=_on_done, on_error=_on_error, parent=self).start()
        except Exception as e:
            QMessageBox.warning(self, "Advertencia", f"Error al iniciar carga de métodos de pago: {str(e)}")
    
    def load_payment_concepts(self):
        """Carga los conceptos de pago de forma asíncrona con TTL corto"""
        try:
            now = time.time()
            if self._concepts_cache["data"] is not None and (now - self._concepts_cache["ts"]) < self._ttl_seconds_concepts:
                concepts = self._concepts_cache["data"]
                self.concepts_model.update_data(concepts)
                # Inicializar precios base
                for concept in concepts or []:
                    if hasattr(concept, 'precio_base'):
                        self.concepts_model._prices[concept.id] = concept.precio_base
                # Mostrar tabla o placeholder según disponibilidad
                has_concepts = bool(concepts)
                self.concepts_table.setVisible(has_concepts)
                self.concepts_placeholder_label.setVisible(not has_concepts)
                return

            def _load():
                return self.payment_manager.obtener_conceptos_pago()

            def _on_success(concepts):
                try:
                    self._concepts_cache = {"data": concepts, "ts": time.time()}
                    self.concepts_model.update_data(concepts)
                    for concept in concepts or []:
                        if hasattr(concept, 'precio_base'):
                            self.concepts_model._prices[concept.id] = concept.precio_base
                    # Mostrar tabla o placeholder según disponibilidad
                    has_concepts = bool(concepts)
                    self.concepts_table.setVisible(has_concepts)
                    self.concepts_placeholder_label.setVisible(not has_concepts)
                except Exception as e:
                    QMessageBox.warning(self, "Advertencia", f"Error al aplicar conceptos de pago: {str(e)}")

            def _on_error(err):
                QMessageBox.warning(self, "Advertencia", f"Error al cargar conceptos de pago: {str(err)}")

            run_in_background(
                _load,
                on_success=_on_success,
                on_error=_on_error,
                parent=self,
                timeout_ms=6000,
                description="Cargar conceptos de pago",
            )
        except Exception as e:
            QMessageBox.warning(self, "Advertencia", f"Error al cargar conceptos de pago: {str(e)}")
        
    def on_user_selected(self, index):
        """Maneja la selección de usuario"""
        user_data = self.user_combobox.itemData(index)
        
        if user_data is None:
            self.selected_user = None
            self.register_button.setEnabled(False)
            self.pdf_button.setEnabled(False)
            self.excel_button.setEnabled(False)
            self.payment_status_label.setText("💡 Seleccione un usuario para ver su estado de pago y gestionar pagos")
            self.history_model.update_data([])
            return
        
        self.selected_user = user_data
        if self.selected_user:
            self.register_button.setEnabled(True)
            self.pdf_button.setEnabled(True)
            self.excel_button.setEnabled(True)
            self.update_current_view()
    
    def update_current_view(self):
        """Actualiza la vista actual con datos del usuario seleccionado"""
        if not self.selected_user:
            return
        
        # Actualizar primero el historial para que el estado considere "sin pagos"
        self.update_payment_history()
        self.update_payment_status()
        self.load_defaults_for_user()
    
    def update_payment_status(self):
        """Actualiza el estado del pago del usuario"""
        # Si no hay pagos en el historial, mostrar estado vacío coherente
        try:
            if self.history_model.rowCount() == 0:
                self.payment_status_label.setText("<b>📭 SIN PAGOS REGISTRADOS</b>")
                self.payment_status_label.setProperty("paymentStatus", "no_payments")
                self.payment_status_label.style().unpolish(self.payment_status_label)
                self.payment_status_label.style().polish(self.payment_status_label)
                return
        except Exception:
            pass

        mes_actual, año_actual = datetime.now().month, datetime.now().year
        pago_realizado = self.payment_manager.verificar_pago_actual(
            self.selected_user.id, mes_actual, año_actual
        )
        
        if pago_realizado:
            self.payment_status_label.setText(
                f"<b>✅ AL DÍA (Cuota de {mes_actual:02d}/{año_actual} pagada)</b>"
            )
            self.payment_status_label.setProperty("paymentStatus", "up_to_date")
        else:
            # Determinar si la cuota está vencida usando fecha_proximo_vencimiento del usuario
            vencida = False
            dias_vencida = None
            try:
                fpv = getattr(self.selected_user, 'fecha_proximo_vencimiento', None)
                if fpv:
                    # Normalizar a objeto date
                    if isinstance(fpv, str):
                        fpv_str = fpv.replace('Z', '+00:00')
                        try:
                            fecha_venc = datetime.fromisoformat(fpv_str).date()
                        except Exception:
                            # Intentar con formato YYYY-MM-DD
                            try:
                                fecha_venc = datetime.strptime(fpv.split()[0], '%Y-%m-%d').date()
                            except Exception:
                                fecha_venc = None
                    else:
                        try:
                            # Puede venir como datetime/date
                            fecha_venc = fpv if hasattr(fpv, 'year') else fpv.date()
                        except Exception:
                            fecha_venc = None

                    if fecha_venc:
                        hoy = datetime.now().date()
                        if hoy > fecha_venc:
                            vencida = True
                            dias_vencida = (hoy - fecha_venc).days

            except Exception:
                vencida = False

            if vencida:
                estado_prop = "overdue_multiple" if getattr(self.selected_user, "cuotas_vencidas", 0) > 1 else "overdue_single"
                if dias_vencida is not None and dias_vencida >= 0:
                    plural = "S" if estado_prop == "overdue_multiple" else ""
                    self.payment_status_label.setText(
                        f"<b>🔴 CUOTA{plural} VENCIDA{plural} (hace {dias_vencida} día(s))</b>"
                    )
                else:
                    plural = "S" if estado_prop == "overdue_multiple" else ""
                    self.payment_status_label.setText(
                        f"<b>🔴 CUOTA{plural} VENCIDA{plural}</b>"
                    )
                self.payment_status_label.setProperty("paymentStatus", estado_prop)
            else:
                self.payment_status_label.setText(
                    f"<b>CUOTA PENDIENTE PARA {mes_actual:02d}/{año_actual}</b>"
                )
                self.payment_status_label.setProperty("paymentStatus", "pending")
        
        # Refrescar estilos
        self.payment_status_label.style().unpolish(self.payment_status_label)
        self.payment_status_label.style().polish(self.payment_status_label)
    
    def update_payment_history(self):
        """Actualiza el historial de pagos de forma asíncrona con TTL corto"""
        try:
            if not self.selected_user:
                self.history_model.update_data([])
                self.pdf_button.setEnabled(False)
                self.excel_button.setEnabled(False)
                if hasattr(self, 'history_loading_label'):
                    self.history_loading_label.setVisible(False)
                if hasattr(self, 'history_empty_label'):
                    self.history_empty_label.setText("Seleccione un usuario para ver su historial")
                    self.history_empty_label.setVisible(True)
                if hasattr(self, 'history_progress'):
                    self.history_progress.setVisible(False)
                if hasattr(self, 'history_table'):
                    self.history_table.setEnabled(False)
                return

            now = time.time()
            # Mostrar indicador de carga inicialmente
            if hasattr(self, 'history_loading_label'):
                self.history_loading_label.setText("Cargando historial de pagos...")
                self.history_loading_label.setVisible(True)
            if hasattr(self, 'history_empty_label'):
                self.history_empty_label.setVisible(False)
            if hasattr(self, 'history_progress'):
                self.history_progress.setVisible(True)
            if hasattr(self, 'history_table'):
                self.history_table.setEnabled(False)
            if (
                self._history_cache["user_id"] == getattr(self.selected_user, 'id', None)
                and self._history_cache["data"] is not None
                and (now - self._history_cache["ts"]) < self._ttl_seconds_history
            ):
                history = self._history_cache["data"]
                self.history_model.update_data(history)
                has_data = len(history) > 0
                self.pdf_button.setEnabled(has_data)
                self.excel_button.setEnabled(has_data)
                if hasattr(self, 'history_loading_label'):
                    self.history_loading_label.setVisible(False)
                if hasattr(self, 'history_empty_label'):
                    self.history_empty_label.setVisible(not has_data)
                if hasattr(self, 'history_progress'):
                    self.history_progress.setVisible(False)
                if hasattr(self, 'history_table'):
                    self.history_table.setEnabled(True)
                if not has_data:
                    self.payment_status_label.setText("<b>📭 SIN PAGOS REGISTRADOS</b>")
                    self.payment_status_label.setProperty("paymentStatus", "no_payments")
                    self.payment_status_label.style().unpolish(self.payment_status_label)
                    self.payment_status_label.style().polish(self.payment_status_label)
                # Fuerza actualización del estado cuando se usa caché
                try:
                    self.update_payment_status()
                except Exception:
                    pass
                return

            def _load():
                return self.payment_manager.obtener_historial_pagos(self.selected_user.id)

            def _on_success(history):
                try:
                    self._history_cache = {
                        "user_id": getattr(self.selected_user, 'id', None),
                        "data": history,
                        "ts": time.time(),
                    }
                    self.history_model.update_data(history)
                    has_data = len(history) > 0
                    self.pdf_button.setEnabled(has_data)
                    self.excel_button.setEnabled(has_data)
                    if hasattr(self, 'history_loading_label'):
                        self.history_loading_label.setVisible(False)
                    if hasattr(self, 'history_empty_label'):
                        self.history_empty_label.setVisible(not has_data)
                    if hasattr(self, 'history_progress'):
                        self.history_progress.setVisible(False)
                    if hasattr(self, 'history_table'):
                        self.history_table.setEnabled(True)
                    if not has_data:
                        self.payment_status_label.setText("<b>📭 SIN PAGOS REGISTRADOS</b>")
                        self.payment_status_label.setProperty("paymentStatus", "no_payments")
                        self.payment_status_label.style().unpolish(self.payment_status_label)
                        self.payment_status_label.style().polish(self.payment_status_label)
                    # Refrescar estado tras finalizar la carga asíncrona del historial
                    try:
                        self.update_payment_status()
                    except Exception:
                        pass
                except Exception as e:
                    logging.exception(f"Error al aplicar historial de pagos: {e}")
                    self.history_model.update_data([])

            def _on_error(err):
                logging.exception(f"Error al cargar historial de pagos: {err}")
                self.history_model.update_data([])
                self.pdf_button.setEnabled(False)
                self.excel_button.setEnabled(False)
                if hasattr(self, 'history_loading_label'):
                    self.history_loading_label.setVisible(False)
                if hasattr(self, 'history_empty_label'):
                    self.history_empty_label.setText("Error al cargar historial")
                    self.history_empty_label.setVisible(True)
                if hasattr(self, 'history_progress'):
                    self.history_progress.setVisible(False)
                if hasattr(self, 'history_table'):
                    self.history_table.setEnabled(True)
                # Asegurar coherencia del estado si hubo error
                try:
                    self.update_payment_status()
                except Exception:
                    pass

            run_in_background(
                _load,
                on_success=_on_success,
                on_error=_on_error,
                parent=self,
                timeout_ms=8000,
                description="Cargar historial de pagos",
            )
        except Exception as e:
            logging.exception(f"Error general al actualizar historial de pagos: {e}")
            self.history_model.update_data([])
            self.pdf_button.setEnabled(False)
            self.excel_button.setEnabled(False)
    
    def on_payment_method_changed(self):
        """Maneja el cambio de método de pago"""
        self.calculate_totals()
    
    def calculate_totals(self):
        """Calcula y actualiza los totales del pago"""
        try:
            subtotal = 0.0
            
            # Calcular subtotal de conceptos seleccionados desde la tabla
            selected_concepts = self.concepts_model.get_selected_concepts()
            for concept_data in selected_concepts:
                subtotal += concept_data['total']
            
            # Calcular comisión y total
            method_id = self.payment_method_combo.currentData()
            commission = 0.0
            total = subtotal
            
            if method_id and subtotal > 0:
                result = self.payment_manager.calcular_total_con_comision(subtotal, method_id)
                total = result['total']
                commission = total - subtotal
            
            # Actualizar resumen
            self.payment_summary.update_summary(subtotal, commission, total)
            
            # Habilitar/deshabilitar botón de registro
            can_register = (
                subtotal > 0 and method_id is not None and self.selected_user is not None
                and getattr(self.selected_user, 'rol', None) != 'dueño'
            )
            self.register_button.setEnabled(can_register)
            
        except Exception as e:
            QMessageBox.warning(self, "Advertencia", f"Error al calcular totales: {str(e)}")
    
    def on_custom_amount_changed(self):
        """Maneja el cambio en el monto personalizado"""
        # Habilitar/deshabilitar botón de registro basado en el monto final
        final_amount = self.payment_summary.get_final_amount()
        method_id = self.payment_method_combo.currentData()
        can_register = (
            final_amount > 0 and method_id is not None and self.selected_user is not None
            and getattr(self.selected_user, 'rol', None) != 'dueño'
        )
        self.register_button.setEnabled(can_register)
    
    def register_payment(self):
        """Registra un nuevo pago"""
        if not self.selected_user:
            QMessageBox.warning(self, "Advertencia", "Seleccione un usuario primero.")
            return
        # Bloquear pagos para usuario 'dueño'
        if getattr(self.selected_user, 'rol', None) == 'dueño':
            QMessageBox.warning(self, "Acción no permitida", "No se puede registrar pagos/cuotas para el usuario Dueño.")
            return
        
        try:
            month = self.month_spinbox.value()
            year = self.year_spinbox.value()
            method_id = self.payment_method_combo.currentData()
            
            if not method_id:
                QMessageBox.warning(self, "Advertencia", "Seleccione un método de pago.")
                return
            
            # Recopilar conceptos seleccionados desde la tabla
            selected_concepts = self.concepts_model.get_selected_concepts()
            
            if not selected_concepts:
                QMessageBox.warning(self, "Advertencia", "Seleccione al menos un concepto de pago.")
                return
            
            # Verificar pago existente
            existing_payment = self.payment_manager.obtener_pago_actual(
                self.selected_user.id, month, year
            )
            
            if existing_payment:
                reply = QMessageBox.question(
                    self, "Pago Existente",
                    f"Ya existe un pago para {month}/{year}. ¿Desea reemplazarlo?",
                    QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
                )
                if reply == QMessageBox.StandardButton.No:
                    return
                
                self.payment_manager.eliminar_pago(existing_payment.id)
            
            # Obtener monto final (personalizado o calculado)
            final_amount = self.payment_summary.get_final_amount()
            
            # Crear fecha de pago a partir del mes y año seleccionados
            fecha_pago = datetime(year, month, 1)
            
            # Registrar nuevo pago
            payment_id = self.payment_manager.registrar_pago_avanzado(
                usuario_id=self.selected_user.id,
                metodo_pago_id=method_id,
                conceptos=selected_concepts,
                fecha_pago=fecha_pago,
                monto_personalizado=final_amount
            )
            
            if payment_id:
                pago = self.db_manager.obtener_pago(payment_id)
                if pago:
                    self.show_receipt_confirmation(pago, self.selected_user)
                
                self.update_current_view()
                self.reset_payment_form()
                self.pagos_modificados.emit()
            else:
                QMessageBox.critical(self, "Error", "No se pudo registrar el pago.")
        
        except ValueError as e:
            QMessageBox.warning(self, "Pago Duplicado", str(e))
        except Exception as e:
            QMessageBox.critical(self, "Error Crítico", f"Ocurrió un error inesperado: {e}")
    
    def reset_payment_form(self):
        """Resetea el formulario de pago"""
        self.concepts_model.reset_selections()
        self.calculate_totals()
    
    def load_defaults(self):
        """Carga valores por defecto"""
        now = datetime.now()
        self.month_spinbox.setValue(now.month)
        self.year_spinbox.setValue(now.year)
        
        # Establecer el primer método de pago válido (no el placeholder)
        if self.payment_method_combo.count() > 1:
            self.payment_method_combo.setCurrentIndex(1)  # Saltar el placeholder
        elif self.payment_method_combo.count() == 1:
            self.payment_method_combo.setCurrentIndex(0)
        
        self.reset_payment_form()
        self.load_default_payment_amount()
    
    def load_default_payment_amount(self):
        """Carga conceptos por defecto sin bloquear la UI"""
        try:
            def _fetch_types():
                return self.db_manager.obtener_tipos_cuota_activos()

            def _on_done(tipos_activos):
                try:
                    if tipos_activos:
                        primer_tipo = tipos_activos[0]
                        # Buscar y activar concepto en la tabla
                        for row in range(self.concepts_model.rowCount()):
                            concept = self.concepts_model._data[row]
                            if primer_tipo.nombre.lower() in concept.nombre.lower():
                                index = self.concepts_model.index(row, 0)
                                self.concepts_model.setData(index, Qt.CheckState.Checked, Qt.ItemDataRole.CheckStateRole)
                                break
                except Exception:
                    logging.exception("Error al aplicar conceptos por defecto.")

            def _on_error(err):
                logging.exception(f"Error al cargar conceptos por defecto: {err}")

            TaskThread(_fetch_types, on_success=_on_done, on_error=_on_error, parent=self).start()
        except Exception as e:
            logging.exception("Error al iniciar carga de conceptos por defecto.")
    
    def load_defaults_for_user(self):
        """Carga valores por defecto para el usuario seleccionado"""
        if not self.selected_user:
            return
        
        self.reset_payment_form()
        
        try:
            tipo_cuota = self.db_manager.obtener_tipo_cuota_por_nombre(self.selected_user.tipo_cuota)
            if tipo_cuota and tipo_cuota.activo:
                # Activar el concepto "Cuota Mensual" y aplicar el precio del tipo de cuota del usuario
                for row in range(self.concepts_model.rowCount()):
                    concept = self.concepts_model._data[row]
                    if "cuota mensual" in concept.nombre.lower():
                        index = self.concepts_model.index(row, 0)
                        self.concepts_model.setData(index, Qt.CheckState.Checked, Qt.ItemDataRole.CheckStateRole)
                        if hasattr(tipo_cuota, 'precio') and tipo_cuota.precio:
                            self.concepts_model.set_user_price_for_concept(concept.nombre, tipo_cuota.precio)
                        break
        except Exception as e:
            logging.exception("Error al cargar conceptos por defecto para el usuario.")
        
        self.calculate_totals()
    
    def show_history_context_menu(self, pos):
        """Muestra el menú contextual del historial"""
        index = self.history_table.indexAt(pos)
        if not index.isValid():
            return
        
        menu = QMenu(self)
        menu.setObjectName("contextMenu")
        menu.setProperty("menuType", "payment_history")
        selected_payment = self.history_model._data[index.row()]
        
        modify_action = QAction("✏️ Modificar Pago", self)
        delete_action = QAction("🗑️ Eliminar Pago", self)
        export_action = QAction("📄 Exportar Recibo (PDF)", self)
        
        modify_action.triggered.connect(lambda: self.modificar_pago_seleccionado(selected_payment))
        delete_action.triggered.connect(lambda: self.eliminar_pago_seleccionado(selected_payment))
        export_action.triggered.connect(lambda: self.exportar_recibo(selected_payment))
        
        menu.addAction(modify_action)
        menu.addAction(delete_action)
        menu.addSeparator()
        menu.addAction(export_action)
        
        menu.exec(self.history_table.viewport().mapToGlobal(pos))
    
    def modificar_pago_seleccionado(self, pago):
        """Modifica un pago seleccionado"""
        dialog = PaymentDialog(self, pago=pago)
        if dialog.exec():
            try:
                self.payment_manager.modificar_pago(dialog.get_payment_data())
                self.update_current_view()
                self.pagos_modificados.emit()
            except Exception as e:
                QMessageBox.critical(self, "Error", f"No se pudo modificar el pago: {e}")
    
    def eliminar_pago_seleccionado(self, pago):
        """Elimina un pago seleccionado"""
        reply = QMessageBox.question(
            self, "Confirmar", 
            f"¿Seguro que desea eliminar el pago?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            try:
                self.payment_manager.eliminar_pago(pago.id)
                self.update_current_view()
                self.pagos_modificados.emit()
            except Exception as e:
                QMessageBox.critical(self, "Error", f"No se pudo eliminar el pago: {e}")
    
    def exportar_recibo(self, pago):
        """Exporta el recibo de un pago con sistema de comprobantes avanzado"""
        usuario = self.db_manager.obtener_usuario(pago.usuario_id)
        if not usuario:
            QMessageBox.warning(self, "Error", "No se encontró el usuario asociado a este pago.")
            return
        try:
            # Crear comprobante en la base de datos
            comprobante_id = self.db_manager.crear_comprobante(
                tipo_comprobante='recibo',
                pago_id=pago.id,
                usuario_id=pago.usuario_id,
                monto_total=pago.monto,
                plantilla_id=None,  # Usar plantilla predeterminada
                datos_comprobante=None,
                emitido_por=self.get_current_user_id()  # Usuario actual del sistema
            )
            
            # Obtener el comprobante creado para obtener el número
            comprobante = self.db_manager.obtener_comprobante(comprobante_id)
            
            self.show_receipt_confirmation(pago, usuario, comprobante)
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error al crear comprobante: {str(e)}")
            # Fallback al método anterior si falla
            self.show_receipt_confirmation(pago, usuario, None)
        
    def show_receipt_confirmation(self, pago, usuario, comprobante=None):
        """Muestra confirmación y genera recibo con número de comprobante"""
        try:
            self.initialize_pdf_generator()
            # Generar recibo con número de comprobante si está disponible
            detalles = self.payment_manager.obtener_detalles_pago(pago.id)
            subtotal = sum(d.subtotal for d in (detalles or [])) if detalles else float(getattr(pago, 'monto', 0) or 0)
            metodo_id = getattr(pago, 'metodo_pago_id', None)
            totales = self.payment_manager.calcular_total_con_comision(subtotal, metodo_id)
            if comprobante:
                filepath = self.pdf_generator.generar_recibo(pago, usuario, comprobante['numero_comprobante'], detalles=detalles, totales=totales)
                mensaje = f"Comprobante N° {comprobante['numero_comprobante']}\nRecibo guardado en:\n{filepath}"
            else:
                filepath = self.pdf_generator.generar_recibo(pago, usuario, detalles=detalles, totales=totales)
                mensaje = f"Recibo guardado en:\n{filepath}"
            
            msg_box = QMessageBox(self)
            msg_box.setIcon(QMessageBox.Icon.Information)
            msg_box.setText(mensaje)
            msg_box.setWindowTitle("Éxito")
            
            open_button = msg_box.addButton("Abrir Recibo", QMessageBox.ButtonRole.ActionRole)
            
            # Agregar botón de reimpresión si hay comprobante
            if comprobante:
                reprint_button = msg_box.addButton("Reimprimir", QMessageBox.ButtonRole.ActionRole)
            
            msg_box.addButton("Aceptar", QMessageBox.ButtonRole.AcceptRole)
            
            msg_box.exec()
            
            if msg_box.clickedButton() == open_button:
                if sys.platform == "win32":
                    os.startfile(os.path.realpath(filepath))
                else:
                    subprocess.call(["open" if sys.platform == "darwin" else "xdg-open", 
                                   os.path.realpath(filepath)])
            elif comprobante and msg_box.clickedButton() == reprint_button:
                self.reimprimir_comprobante(comprobante['id'])
        
        except Exception as e:
            logging.exception("Error al generar o mostrar confirmación de recibo.")
            QMessageBox.critical(self, "Error", f"Error al generar recibo: {str(e)}")
    
    def reimprimir_comprobante(self, comprobante_id):
        """Reimprime un comprobante existente"""
        try:
            self.initialize_pdf_generator()
            comprobante = self.db_manager.obtener_comprobante(comprobante_id)
            if not comprobante:
                QMessageBox.warning(self, "Error", "No se encontró el comprobante.")
                return
            
            # Verificar que el comprobante no esté cancelado
            if comprobante['estado'] == 'cancelado':
                QMessageBox.warning(self, "Error", "No se puede reimprimir un comprobante cancelado.")
                return
            
            # Obtener datos del pago y usuario
            pago = self.db_manager.obtener_pago(comprobante['pago_id'])
            usuario = self.db_manager.obtener_usuario(comprobante['usuario_id'])
            
            if not pago or not usuario:
                QMessageBox.warning(self, "Error", "No se encontraron los datos asociados al comprobante.")
                return
            
            # Generar PDF con marca de reimpresión
            detalles = self.payment_manager.obtener_detalles_pago(pago.id)
            subtotal = sum(d.subtotal for d in (detalles or [])) if detalles else float(getattr(pago, 'monto', 0) or 0)
            metodo_id = getattr(pago, 'metodo_pago_id', None)
            totales = self.payment_manager.calcular_total_con_comision(subtotal, metodo_id)
            filepath = self.pdf_generator.generar_recibo(
                pago, usuario, 
                f"{comprobante['numero_comprobante']} (REIMPRESIÓN)",
                detalles=detalles,
                totales=totales
            )
            
            msg_box = QMessageBox(self)
            msg_box.setIcon(QMessageBox.Icon.Information)
            msg_box.setText(f"Comprobante N° {comprobante['numero_comprobante']} reimpreso\nArchivo guardado en:\n{filepath}")
            msg_box.setWindowTitle("Reimpresión Exitosa")
            
            open_button = msg_box.addButton("Abrir Recibo", QMessageBox.ButtonRole.ActionRole)
            msg_box.addButton("Aceptar", QMessageBox.ButtonRole.AcceptRole)
            
            msg_box.exec()
            
            if msg_box.clickedButton() == open_button:
                if sys.platform == "win32":
                    os.startfile(os.path.realpath(filepath))
                else:
                    subprocess.call(["open" if sys.platform == "darwin" else "xdg-open", 
                                   os.path.realpath(filepath)])
                                   
        except Exception as e:
            logging.exception("Error al reimprimir comprobante.")
            QMessageBox.critical(self, "Error", f"Error al reimprimir comprobante: {str(e)}")
    
    def exportar_historial(self, file_format: str):
        """Exporta el historial de pagos"""
        if not self.selected_user or not self.history_model._data:
            QMessageBox.warning(self, "Sin Datos", "No hay historial de pagos para exportar.")
            return
        
        pagos = self.history_model._data
        user_name = self.selected_user.nombre.replace(" ", "_")
        timestamp = datetime.now().strftime("%Y-%m-%d")
        default_filename = f"historial_pagos_{user_name}_{timestamp}"
        
        if file_format == 'pdf':
            filepath, _ = QFileDialog.getSaveFileName(
                self, "Guardar Historial como PDF", 
                f"{default_filename}.pdf", "PDF Files (*.pdf)"
            )
            if filepath:
                self.exportar_pagos_a_pdf(pagos, filepath)
        
        elif file_format == 'excel':
            filepath, _ = QFileDialog.getSaveFileName(
                self, "Guardar Historial como Excel", 
                f"{default_filename}.xlsx", "Excel Files (*.xlsx)"
            )
            if filepath:
                self.exportar_pagos_a_excel(pagos, filepath)
    
    def exportar_pagos_a_pdf(self, pagos: List, filepath: str):
        """Exporta pagos a PDF"""
        try:
            doc = SimpleDocTemplate(filepath, pagesize=letter)
            styles = getSampleStyleSheet()
            
            elements = [
                Paragraph(f"Historial de Pagos de: {self.selected_user.nombre}", styles['h1']),
                Spacer(1, 24)
            ]
            
            headers = ["Período", "Monto", "Fecha de Pago"]
            table_data = [headers]
            
            for pago in pagos:
                fecha_pago_str = (
                    datetime.fromisoformat(pago.fecha_pago).strftime("%d/%m/%Y")
                    if isinstance(pago.fecha_pago, str) and pago.fecha_pago
                    else (pago.fecha_pago.strftime("%d/%m/%Y") if pago.fecha_pago else "Sin fecha")
                )
                table_data.append([
                    f"{pago.mes:02d}/{pago.año}",
                    f"${pago.monto:,.0f}",
                    fecha_pago_str
                ])
            
            # Obtener colores del sistema de branding
            try:
                main_window = self.window()
                if hasattr(main_window, 'branding_config'):
                    branding_config = main_window.branding_config
                    header_bg = branding_config.get('primary_color', '#434C5E')
                    table_bg = branding_config.get('alt_background_color', '#D8DEE9')
                else:
                    header_bg = '#434C5E'
                    table_bg = '#D8DEE9'
            except:
                header_bg = '#434C5E'
                table_bg = '#D8DEE9'
            
            table = Table(table_data)
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor(header_bg)),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor(table_bg)),
                ('GRID', (0, 0), (-1, -1), 1, colors.black)
            ]))
            
            elements.append(table)
            doc.build(elements)
            
            QMessageBox.information(
                self, "Éxito", 
                f"Historial de pagos exportado a PDF en:\n{filepath}"
            )
        
        except Exception as e:
            QMessageBox.critical(self, "Error", f"No se pudo exportar a PDF: {e}")
    
    def exportar_pagos_a_excel(self, pagos: List, filepath: str):
        """Exporta pagos a Excel"""
        try:
            pagos_data = []
            for pago in pagos:
                fecha_pago_str = (
                    datetime.fromisoformat(pago.fecha_pago).strftime("%Y-%m-%d")
                    if isinstance(pago.fecha_pago, str) and pago.fecha_pago
                    else (pago.fecha_pago.strftime("%Y-%m-%d") if pago.fecha_pago else "Sin fecha")
                )
                pagos_data.append({
                    "Período": f"{pago.mes:02d}/{pago.año}",
                    "Monto": pago.monto,
                    "Fecha de Pago": fecha_pago_str
                })
            
            pd.DataFrame(pagos_data).to_excel(filepath, index=False, engine='openpyxl')
            
            QMessageBox.information(
                self, "Éxito", 
                f"Historial de pagos exportado a Excel en:\n{filepath}"
            )
        
        except Exception as e:
            QMessageBox.critical(self, "Error", f"No se pudo exportar a Excel: {e}")
    
    def apply_payment_filters(self, filters):
        """Aplica filtros al historial de pagos"""
        if not self.selected_user:
            return
        
        all_payments = self.payment_manager.obtener_historial_pagos(self.selected_user.id)
        filtered_payments = []
        
        for pago in all_payments:
            # Aplicar filtros
            if filters.get("periodo"):
                periodo_filter = filters["periodo"].lower()
                periodo_pago = f"{pago.mes:02d}/{pago.año}"
                if periodo_filter not in periodo_pago:
                    continue
            
            if filters.get("monto_min"):
                try:
                    monto_min = float(filters["monto_min"])
                    if pago.monto < monto_min:
                        continue
                except ValueError:
                    pass
            
            if filters.get("monto_max"):
                try:
                    monto_max = float(filters["monto_max"])
                    if pago.monto > monto_max:
                        continue
                except ValueError:
                    pass
            
            if filters.get("fecha_pago"):
                filter_date = filters["fecha_pago"]
                if hasattr(filter_date, 'toPython'):
                    filter_date = filter_date.toPython()
                    pago_date = (
                        datetime.fromisoformat(pago.fecha_pago).date()
                        if isinstance(pago.fecha_pago, str)
                        else pago.fecha_pago.date()
                    )
                    if pago_date != filter_date:
                        continue
            
            filtered_payments.append(pago)
        
        self.history_model.update_data(filtered_payments)

    def on_history_search_text_changed(self, text: str):
        """Mapea texto de búsqueda a filtros compatibles con apply_payment_filters, con heurísticas cuidadosas."""
        import re
        from datetime import datetime, date, timedelta
        query = (text or "").strip()
        filters = {}
        if query:
            ql = query.lower()

            # Detectar período formato MM/YYYY (ej: 02/2024)
            periodo_match = re.search(r"\b(\d{1,2})\/(\d{4})\b", query)
            if periodo_match:
                filters['periodo'] = periodo_match.group(0)

            # Detectar rango de montos "100-300" con soporte a coma/punto y símbolo $
            range_match = re.search(r"\$?\s*(\d+(?:[\.,]\d+)?)\s*-\s*\$?\s*(\d+(?:[\.,]\d+)?)", query)
            if range_match:
                try:
                    m1 = float(range_match.group(1).replace(',', '.'))
                    m2 = float(range_match.group(2).replace(',', '.'))
                    filters['monto_min'] = min(m1, m2)
                    filters['monto_max'] = max(m1, m2)
                except Exception:
                    pass
            else:
                # Detectar monto único si el texto sugiere búsqueda por monto
                if any(k in ql for k in ['monto', 'pago', '$']):
                    num_match = re.search(r"\d+(?:[\.,]\d+)?", query.replace(',', '.'))
                    if num_match:
                        try:
                            monto = float(num_match.group(0))
                            filters['monto_min'] = monto
                            filters['monto_max'] = monto
                        except Exception:
                            pass

            # Detectar fecha exacta YYYY-MM-DD o DD/MM/YYYY
            iso_match = re.search(r"\b\d{4}-\d{2}-\d{2}\b", query)
            if iso_match:
                try:
                    filters['fecha_pago'] = datetime.fromisoformat(iso_match.group(0)).date()
                except Exception:
                    pass
            else:
                dmy_match = re.search(r"\b(\d{1,2})\/(\d{1,2})\/(\d{2,4})\b", query)
                if dmy_match:
                    try:
                        d, m, y = dmy_match.groups()
                        y = int(y)
                        if y < 100:  # normalizar año corto
                            y += 2000
                        filters['fecha_pago'] = date(y, int(m), int(d))
                    except Exception:
                        pass

            # Palabras clave naturales para fecha ("hoy", "ayer") -> fecha exacta
            today = datetime.now().date()
            if 'hoy' in ql:
                filters['fecha_pago'] = today
            elif 'ayer' in ql:
                filters['fecha_pago'] = today - timedelta(days=1)

            # Meses en español para período (ej: "enero 2024") -> MM/YYYY
            meses = {
                'enero': '01', 'febrero': '02', 'marzo': '03', 'abril': '04',
                'mayo': '05', 'junio': '06', 'julio': '07', 'agosto': '08',
                'septiembre': '09', 'setiembre': '09', 'octubre': '10',
                'noviembre': '11', 'diciembre': '12', 'ene': '01', 'feb': '02',
                'mar': '03', 'abr': '04', 'may': '05', 'jun': '06', 'jul': '07',
                'ago': '08', 'sep': '09', 'oct': '10', 'nov': '11', 'dic': '12'
            }
            mes_regex = r"\b(" + '|'.join(meses.keys()) + r")\b\s*(\d{4})"
            mes_match = re.search(mes_regex, ql)
            if mes_match:
                mm = meses.get(mes_match.group(1), None)
                yyyy = mes_match.group(2)
                if mm and yyyy:
                    filters['periodo'] = f"{mm}/{yyyy}"

        # Aplicar filtros (solo se usan claves soportadas por apply_payment_filters)
        self.apply_payment_filters(filters)
    
    def reset_tab(self):
        """Resetea la pestaña"""
        self.user_combobox.setCurrentIndex(0)
        self.user_combobox.lineEdit().setText("")
    
    def set_user_for_payment(self, user_id: int):
        """Establece un usuario específico para pago"""
        self.load_defaults()
        for i in range(self.user_combobox.count()):
            user_data = self.user_combobox.itemData(i)
            if user_data and user_data.id == user_id:
                self.user_combobox.setCurrentIndex(i)
                self.selected_user = user_data
                self.load_defaults_for_user()
                self.activate_monthly_quota_concept()
                # Si es 'dueño', deshabilitar acciones de registro y avisar
                try:
                    if getattr(self.selected_user, 'rol', None) == 'dueño':
                        self.register_button.setEnabled(False)
                        self.register_button.setToolTip("Bloqueado para usuario Dueño")
                    else:
                        self.register_button.setToolTip("")
                    
                    # Recalcular totales para asegurar coherencia del botón
                    self.calculate_totals()
                except Exception:
                    pass
                return
    
    def activate_monthly_quota_concept(self):
        """Activa automáticamente el concepto de cuota mensual"""
        try:
            # Buscar y activar concepto de cuota mensual en la tabla
            for row in range(self.concepts_model.rowCount()):
                concept = self.concepts_model._data[row]
                if "cuota mensual" in concept.nombre.lower():
                    # Activar el concepto
                    index = self.concepts_model.index(row, 0)
                    self.concepts_model.setData(index, Qt.CheckState.Checked, Qt.ItemDataRole.CheckStateRole)
                    
                    # Establecer precio según tipo de cuota del usuario (robusto: por nombre o ID)
                    if self.selected_user:
                        try:
                            precio_to_apply = None
                            tipo_val = getattr(self.selected_user, 'tipo_cuota', None)
                            tipo_obj = None
                            
                            # Intentar obtener por ID si viene numérico
                            if isinstance(tipo_val, (int, float)) or (isinstance(tipo_val, str) and tipo_val.strip().isdigit()):
                                try:
                                    tipo_obj = self.db_manager.obtener_tipo_cuota_por_id(int(str(tipo_val).strip()))
                                except Exception:
                                    tipo_obj = None
                            
                            # Si no, intentar obtener por nombre (con resolución case-insensitive)
                            if not tipo_obj and isinstance(tipo_val, str) and tipo_val.strip():
                                try:
                                    tipo_obj = self.db_manager.obtener_tipo_cuota_por_nombre(tipo_val.strip())
                                except Exception:
                                    tipo_obj = None
                                # Resolver contra lista de tipos activos, normalizando a minúsculas
                                if not tipo_obj:
                                    try:
                                        tipos_activos = []
                                        if hasattr(self.db_manager, 'obtener_tipos_cuota'):
                                            tipos_activos = self.db_manager.obtener_tipos_cuota(solo_activos=True) or []
                                        elif hasattr(self.db_manager, 'obtener_tipos_cuota_activos'):
                                            tipos_activos = self.db_manager.obtener_tipos_cuota_activos() or []
                                        tv_norm = tipo_val.strip().lower()
                                        for t in tipos_activos:
                                            nombre_t = getattr(t, 'nombre', '') or ''
                                            if nombre_t.strip().lower() == tv_norm:
                                                tipo_obj = t
                                                break
                                    except Exception:
                                        pass
                            
                            # Fallback: atributo tipo_cuota_id si existe
                            if not tipo_obj:
                                tipo_id = getattr(self.selected_user, 'tipo_cuota_id', None)
                                if tipo_id:
                                    try:
                                        tipo_obj = self.db_manager.obtener_tipo_cuota_por_id(int(tipo_id))
                                    except Exception:
                                        tipo_obj = None
                            
                            # Determinar precio a aplicar solo si se resolvió el tipo
                            if tipo_obj and hasattr(tipo_obj, 'precio'):
                                precio_to_apply = tipo_obj.precio
                            
                            # Fallback final: usar precio_base del concepto
                            if precio_to_apply is None:
                                precio_to_apply = getattr(concept, 'precio_base', None)
                            
                            if precio_to_apply is not None:
                                self.concepts_model.set_user_price_for_concept(concept.nombre, precio_to_apply)
                        except Exception as e:
                            logging.warning(f"No se pudo cargar el precio del tipo de cuota: {e}")
                    
                    self.calculate_totals()
                    break
        
        except Exception as e:
            logging.exception(f"Error al activar concepto Cuota Mensual: {e}")
    
    def select_payment(self, payment_id: int):
        """Selecciona un pago específico en el historial"""
        try:
            pago = self.db_manager.obtener_pago(payment_id)
            if not pago:
                logging.warning(f"No se encontró el pago con ID {payment_id}")
                return
            
            self.set_user_for_payment(pago.usuario_id)
            
            if self.selected_user:
                for row in range(self.history_model.rowCount(None)):
                    pago_en_tabla = self.history_model._data[row]
                    if pago_en_tabla.id == payment_id:
                        index = self.history_model.index(row, 0)
                        self.history_table.selectRow(row)
                        self.history_table.scrollTo(index)
                        break
        
        except Exception as e:
            logging.exception(f"Error al seleccionar el pago {payment_id}: {e}")
            QMessageBox.warning(self, "Error", f"No se pudo seleccionar el pago: {e}")
    
    def apply_modern_branding(self):
        """Aplica el branding moderno automático"""
        try:
            main_window = self.window()
            if hasattr(main_window, 'branding_config'):
                # El branding se aplicará automáticamente a través del sistema de estilos
                # y las propiedades CSS definidas en el archivo de estilos
                pass
        except Exception as e:
            logging.exception(f"Error aplicando branding moderno: {e}")
    
    def initialize_pdf_generator(self):
        """Inicializa el generador de PDF con la configuración de branding"""
        try:
            # Obtener configuración de branding desde main_window
            main_window = self.window()
            branding_config = None
            
            if hasattr(main_window, 'branding_config'):
                branding_config = main_window.branding_config
            
            # Inicializar PDFGenerator con configuración de branding
            from pdf_generator import PDFGenerator
            self.pdf_generator = PDFGenerator(branding_config)
            
        except Exception as e:
            logging.exception("Error al inicializar PDFGenerator con configuración de branding")
            # Fallback: inicializar sin configuración
            from pdf_generator import PDFGenerator
            self.pdf_generator = PDFGenerator()
    
    def configure_receipt_numbering(self):
        """Abre el diálogo de configuración de numeración de comprobantes"""
        try:
            from widgets.receipt_numbering_dialog import ReceiptNumberingDialog
            dialog = ReceiptNumberingDialog(self.db_manager, self)
            if dialog.exec() == QDialog.DialogCode.Accepted:
                # Actualizar el número de comprobante mostrado
                self.update_receipt_number_display()
                QMessageBox.information(
                    self, "Éxito", 
                    "Configuración de numeración actualizada correctamente."
                )
        except ImportError:
            QMessageBox.warning(
                self, "Advertencia", 
                "El diálogo de configuración de numeración no está disponible."
            )
        except Exception as e:
            QMessageBox.critical(
                self, "Error", 
                f"Error al abrir configuración de numeración: {str(e)}"
            )
    
    def update_receipt_number_display(self):
        """Actualiza la visualización del próximo número de comprobante"""
        try:
            # Obtener el próximo número de comprobante
            next_number = self.db_manager.get_next_receipt_number()
            self.receipt_number_label.setText(f"Próximo: #{next_number}")
        except Exception as e:
            logging.warning(f"Error al actualizar número de comprobante: {e}")
            self.receipt_number_label.setText("Próximo: #---")
    
    def load_defaults_for_user(self):
        """Carga valores por defecto para el usuario seleccionado"""
        if not self.selected_user:
            return
        
        try:
            # Actualizar número de comprobante
            self.update_receipt_number_display()
            
            # Cargar período actual
            current_date = datetime.now()
            self.month_spinbox.setValue(current_date.month)
            self.year_spinbox.setValue(current_date.year)
            
            # Resetear método de pago
            self.payment_method_combo.setCurrentIndex(0)
            
            # Limpiar selección de conceptos
            self.concepts_model.clear_selection()

            # Activar y valorizar "Cuota Mensual" con el precio del tipo de cuota del usuario
            # (Este es el método efectivo dentro de la clase, por lo que centralizamos aquí la lógica)
            self.activate_monthly_quota_concept()
            
            # Recalcular totales
            self.calculate_totals()
            
        except Exception as e:
            logging.exception(f"Error al cargar valores por defecto para usuario: {e}")
    
    def get_current_user_id(self):
        """Obtiene el ID del usuario actualmente logueado"""
        try:
            main_window = self.window()
            if hasattr(main_window, 'logged_in_user') and main_window.logged_in_user:
                return main_window.logged_in_user.id
            return 1  # Fallback al usuario por defecto
        except Exception as e:
            logging.warning(f"Error al obtener ID de usuario actual: {e}")
            return 1

