# -*- coding: utf-8 -*-
"""
Tooltip Manager - Sistema de tooltips informativos para el Gym Management System

Este módulo proporciona un sistema centralizado para gestionar tooltips informativos
en toda la aplicación, mejorando la usabilidad y proporcionando ayuda contextual.
"""

import logging
from typing import Dict, Optional
from PyQt6.QtWidgets import QWidget, QPushButton, QLineEdit, QComboBox, QCheckBox, QTableView, QTabWidget
from PyQt6.QtCore import Qt

class TooltipManager:
    """
    Gestor centralizado de tooltips informativos para mejorar la usabilidad del sistema.
    
    Proporciona tooltips contextuales con información útil, shortcuts de teclado,
    y ayuda sobre funcionalidades específicas.
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Diccionario de tooltips por categoría y elemento
        self.tooltips = {
            'navigation': {
                'tab_usuarios': 'Gestión de socios y registro de asistencias (Ctrl+1)',
                'tab_pagos': 'Procesamiento de pagos y gestión financiera (Ctrl+2)',
                'tab_reportes': 'Dashboard de métricas y reportes ejecutivos (Ctrl+3)',
                'tab_rutinas': 'Creación y asignación de rutinas de ejercicios (Ctrl+4)',
                'tab_clases': 'Programación de clases grupales y horarios (Ctrl+5)',
                'tab_profesores': 'Gestión de profesores y evaluación (Ctrl+6)',
                'tab_configuracion': 'Configuración del sistema y personalización (Ctrl+7)'
            },
            'search': {
                'global_search': 'Búsqueda global en todo el sistema (Ctrl+F)\nBusca en usuarios, pagos, rutinas y clases',
                'user_search': 'Buscar por nombre, DNI, ID o teléfono\nLa búsqueda es en tiempo real',
                'payment_search': 'Buscar pagos por socio, fecha o monto\nUse filtros para búsquedas avanzadas',
                'filter_button': 'Filtros avanzados\nCombine múltiples criterios para búsquedas precisas'
            },
            'actions': {
                'add_user': 'Agregar nuevo socio al sistema\nSe abrirá un formulario de registro completo',
                'edit_user': 'Editar información del socio seleccionado\nDoble clic en la fila para editar rápidamente',
                'delete_user': 'Eliminar socio del sistema\nEsta acción requiere confirmación',
                'register_attendance': 'Registrar asistencia del socio seleccionado\nSe guardará la fecha y hora actual',
                'process_payment': 'Procesar pago para el socio seleccionado\nSe generará un recibo automáticamente',
                'assign_routine': 'Asignar rutina personalizada al socio\nPuede crear nuevas rutinas o usar plantillas'
            },
            'export': {
                'export_pdf': 'Exportar datos a PDF\nIncluye todos los registros visibles con filtros aplicados',
                'export_excel': 'Exportar datos a Excel\nPerfecto para análisis externos y reportes',
                'print_receipt': 'Imprimir recibo de pago\nSe abrirá una vista previa antes de imprimir'
            },
            'status': {
                'active_user': 'Usuario activo\nPuede acceder a todas las funcionalidades del gimnasio',
                'inactive_user': 'Usuario inactivo\nAcceso restringido por falta de pago o suspensión',
                'payment_due': 'Pago vencido\nSe requiere actualizar el pago para reactivar',
                'payment_current': 'Pago al día\nMembresía activa hasta la fecha indicada',
                'attendance_today': 'Asistió hoy\nRegistro de asistencia del día actual'
            },
            'forms': {
                'required_field': 'Campo obligatorio\nDebe completarse para continuar',
                'optional_field': 'Campo opcional\nPuede dejarse vacío si no aplica',
                'email_field': 'Dirección de correo electrónico\nSe usará para comunicaciones importantes',
                'phone_field': 'Número de teléfono\nIncluya código de área para mejor contacto',
                'dni_field': 'Documento Nacional de Identidad\nDebe ser único en el sistema'
            },
            'reports': {
                'kpi_card': 'Indicador clave de rendimiento\nHaga clic para ver detalles y tendencias',
                'chart_interaction': 'Gráfico interactivo\nPase el mouse sobre los puntos para ver valores exactos',
                'date_filter': 'Filtro de fechas\nSeleccione el período para analizar',
                'export_dashboard': 'Exportar dashboard completo\nIncluye todos los gráficos y métricas actuales'
            },
            'configuration': {
                'theme_selector': 'Selector de tema visual\nCambia la apariencia de toda la aplicación',
                'font_size': 'Tamaño de fuente\nAjusta la legibilidad según sus preferencias',
                'backup_settings': 'Configuración de respaldo\nPrograme copias de seguridad automáticas',
                'notification_settings': 'Configuración de notificaciones\nPersonalice alertas y recordatorios'
            }
        }
    
    def get_tooltip(self, category: str, element: str) -> str:
        """
        Obtiene el tooltip para un elemento específico.
        
        Args:
            category: Categoría del tooltip (navigation, search, actions, etc.)
            element: Elemento específico dentro de la categoría
            
        Returns:
            Texto del tooltip o cadena vacía si no se encuentra
        """
        try:
            return self.tooltips.get(category, {}).get(element, '')
        except Exception as e:
            self.logger.warning(f"Error obteniendo tooltip {category}.{element}: {e}")
            return ''
    
    def apply_tooltip(self, widget: QWidget, category: str, element: str, custom_text: Optional[str] = None) -> None:
        """
        Aplica un tooltip a un widget específico.
        
        Args:
            widget: Widget al que aplicar el tooltip
            category: Categoría del tooltip
            element: Elemento específico
            custom_text: Texto personalizado (opcional, sobrescribe el predefinido)
        """
        try:
            tooltip_text = custom_text or self.get_tooltip(category, element)
            if tooltip_text:
                widget.setToolTip(tooltip_text)
                # Configurar duración del tooltip para textos largos
                if len(tooltip_text) > 100:
                    widget.setToolTipDuration(8000)  # 8 segundos para tooltips largos
                else:
                    widget.setToolTipDuration(4000)  # 4 segundos para tooltips cortos
        except Exception as e:
            self.logger.warning(f"Error aplicando tooltip a widget: {e}")
    
    def apply_navigation_tooltips(self, tab_widget: QTabWidget) -> None:
        """
        Aplica tooltips a las pestañas de navegación principal.
        
        Args:
            tab_widget: Widget de pestañas principal
        """
        try:
            tab_tooltips = [
                ('tab_usuarios', 0),
                ('tab_pagos', 1),
                ('tab_reportes', 2),
                ('tab_rutinas', 3),
                ('tab_clases', 4),
                ('tab_profesores', 5),
                ('tab_configuracion', 6)
            ]
            
            for tooltip_key, tab_index in tab_tooltips:
                if tab_index < tab_widget.count():
                    tooltip_text = self.get_tooltip('navigation', tooltip_key)
                    if tooltip_text:
                        tab_widget.setTabToolTip(tab_index, tooltip_text)
                        
        except Exception as e:
            logging.error(f"Error aplicando tooltips de navegación: {str(e)}")
    
    def apply_table_tooltips(self, table_view: QTableView, table_type: str) -> None:
        """
        Aplica tooltips a las columnas de una tabla.
        
        Args:
            table_view: Vista de tabla
            table_type: Tipo de tabla (users, payments, routines, etc.)
        """
        try:
            table_tooltips = {
                'users': {
                    0: 'ID único del usuario en el sistema',
                    1: 'Nombre completo del socio',
                    2: 'Rol en el sistema (Socio, Profesor, Dueño)',
                    3: 'Documento Nacional de Identidad',
                    4: 'Número de teléfono de contacto',
                    5: 'Tipo de membresía contratada',
                    6: 'Estado actual de la membresía',
                    7: 'Registro de asistencia del día actual'
                },
                'payments': {
                    0: 'ID único del pago',
                    1: 'Nombre del socio que realizó el pago',
                    2: 'Monto pagado en la moneda local',
                    3: 'Método utilizado para el pago',
                    4: 'Fecha en que se realizó el pago',
                    5: 'Fecha de vencimiento del próximo pago',
                    6: 'Estado actual del pago'
                }
            }
            
            if table_type in table_tooltips:
                model = table_view.model()
                if model:
                    for column, tooltip in table_tooltips[table_type].items():
                        if column < model.columnCount():
                            table_view.horizontalHeader().setToolTip(tooltip)
                            
        except Exception as e:
            logging.error(f"Error aplicando tooltips de tabla {table_type}: {str(e)}")
    
    def apply_form_tooltips(self, form_widget: QWidget, form_type: str) -> None:
        """
        Aplica tooltips a los campos de un formulario.
        
        Args:
            form_widget: Widget contenedor del formulario
            form_type: Tipo de formulario (user_form, payment_form, etc.)
        """
        try:
            # Buscar campos comunes en el formulario
            for child in form_widget.findChildren(QWidget):
                object_name = child.objectName().lower()
                
                # Aplicar tooltips basados en el nombre del objeto
                if 'email' in object_name:
                    self.apply_tooltip(child, 'forms', 'email_field')
                elif 'phone' in object_name or 'telefono' in object_name:
                    self.apply_tooltip(child, 'forms', 'phone_field')
                elif 'dni' in object_name:
                    self.apply_tooltip(child, 'forms', 'dni_field')
                elif 'required' in object_name or child.property('required'):
                    self.apply_tooltip(child, 'forms', 'required_field')
                    
        except Exception as e:
            logging.error(f"Error aplicando tooltips de formulario {form_type}: {str(e)}")
    
    def create_contextual_tooltip(self, base_text: str, shortcuts: list = None, additional_info: str = None) -> str:
        """
        Crea un tooltip contextual combinando información base con shortcuts y datos adicionales.
        
        Args:
            base_text: Texto base del tooltip
            shortcuts: Lista de shortcuts de teclado
            additional_info: Información adicional
            
        Returns:
            Tooltip formateado con toda la información
        """
        tooltip_parts = [base_text]
        
        if shortcuts:
            shortcut_text = ' | '.join(shortcuts)
            tooltip_parts.append(f"\nShortcuts: {shortcut_text}")
        
        if additional_info:
            tooltip_parts.append(f"\n\n{additional_info}")
        
        return ''.join(tooltip_parts)

# Instancia global del gestor de tooltips
tooltip_manager = TooltipManager()