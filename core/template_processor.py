#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Template Processor - Procesador de plantillas de mensajes WhatsApp
Maneja el procesamiento de plantillas con variables dinámicas
"""

import re
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from .database import DatabaseManager
from .utils import get_gym_name

class TemplateProcessor:
    """Procesador de plantillas de mensajes con variables dinámicas"""
    
    def __init__(self, database_manager: DatabaseManager):
        self.db = database_manager
        self.variables_sistema = self._cargar_variables_sistema()
    
    def _cargar_variables_sistema(self) -> Dict[str, Any]:
        """Carga variables del sistema desde la configuración"""
        try:
            # Usar datos reales del gimnasio
            return {
                'nombre_gimnasio': get_gym_name('Gimnasio'),
                'direccion_gimnasio': 'Av. Libertador 1234, Buenos Aires',
                'telefono_gimnasio': '+54 11 4567-8901',
                'email_gimnasio': 'info@gimnasiozurka.com',
                'horarios_atencion': 'Lunes a Viernes 6:00-22:00, Sábados 8:00-20:00',
                'moneda': 'ARS',
                'simbolo_moneda': '$',
                'sitio_web': 'www.gimnasiozurka.com',
                'fecha_actual': datetime.now().strftime('%d/%m/%Y'),
                'hora_actual': datetime.now().strftime('%H:%M'),
                'año_actual': datetime.now().year,
                'mes_actual': datetime.now().strftime('%B'),
                'dia_semana': self._obtener_dia_semana()
            }
        except Exception as e:
            logging.error(f"Error al cargar variables del sistema: {e}")
            return {}
    
    def _obtener_dia_semana(self) -> str:
        """Obtiene el día de la semana en español"""
        dias = {
            0: 'Lunes', 1: 'Martes', 2: 'Miércoles', 3: 'Jueves',
            4: 'Viernes', 5: 'Sábado', 6: 'Domingo'
        }
        return dias.get(datetime.now().weekday(), 'Desconocido')
    
    def procesar_plantilla(self, contenido: str, variables: Dict[str, Any] = None) -> str:
        """Procesa una plantilla reemplazando variables dinámicas"""
        try:
            if not contenido:
                return ""
            
            # Combinar variables del sistema con variables específicas
            todas_variables = self.variables_sistema.copy()
            if variables:
                todas_variables.update(variables)
            
            # Procesar variables con formato {{variable}}
            contenido_procesado = self._reemplazar_variables(contenido, todas_variables)
            
            # Procesar funciones especiales
            contenido_procesado = self._procesar_funciones_especiales(contenido_procesado, todas_variables)
            
            return contenido_procesado
            
        except Exception as e:
            logging.error(f"Error al procesar plantilla: {e}")
            return contenido  # Devolver contenido original en caso de error
    
    def procesar_plantilla_whatsapp(self, template_name: str, parametros: List[str]) -> Dict[str, Any]:
        """Procesa plantillas de WhatsApp con variables {{1}}, {{2}}, {{3}} del archivo SISTEMA WHATSAPP.txt"""
        try:
            # Obtener plantilla desde base de datos
            plantilla = self.db.obtener_plantilla_whatsapp(template_name)
            if not plantilla:
                logging.error(f"Plantilla {template_name} no encontrada")
                return None
            
            # Las plantillas de WhatsApp usan variables numeradas {{1}}, {{2}}, {{3}}
            contenido = plantilla.get('body_text', '')
            
            # Reemplazar variables numeradas con los parámetros proporcionados
            for i, parametro in enumerate(parametros, 1):
                contenido = contenido.replace(f"{{{{{i}}}}}", str(parametro))
            
            return {
                'template_name': template_name,
                'content': contenido,
                'parameters': parametros,
                'processed_content': contenido,
                'header_text': plantilla.get('header_text', ''),
                'variables': plantilla.get('variables', {})
            }
            
        except Exception as e:
            logging.error(f"Error al procesar plantilla WhatsApp {template_name}: {e}")
            return None
    
    def _reemplazar_variables(self, contenido: str, variables: Dict[str, Any]) -> str:
        """Reemplaza variables con formato {{variable}}"""
        def reemplazar(match):
            variable = match.group(1).strip()
            # Skip variables that contain colons (special functions)
            if ':' in variable:
                return match.group(0)
            valor = variables.get(variable, f"{{{{VARIABLE_NO_ENCONTRADA: {variable}}}}}")
            return str(valor) if valor is not None else ""
        
        # Patrón para variables: {{variable}}
        patron = r'\{\{\s*([^}]+)\s*\}\}'
        return re.sub(patron, reemplazar, contenido)
    
    def _procesar_funciones_especiales(self, contenido: str, variables: Dict[str, Any]) -> str:
        """Procesa funciones especiales en las plantillas"""
        # Función para formatear fechas: {{fecha:formato}}
        contenido = self._procesar_funcion_fecha(contenido)
        
        # Función para formatear montos: {{monto:variable}}
        contenido = self._procesar_funcion_monto(contenido, variables)
        
        # Función condicional: {{si:variable:texto_si_true:texto_si_false}}
        contenido = self._procesar_funcion_condicional(contenido, variables)
        
        # Función para días transcurridos: {{dias_desde:fecha}}
        contenido = self._procesar_funcion_dias_desde(contenido, variables)
        
        return contenido
    
    def _procesar_funcion_fecha(self, contenido: str) -> str:
        """Procesa funciones de fecha: {{fecha:dd/mm/yyyy}}"""
        def reemplazar_fecha(match):
            formato = match.group(1).strip()
            try:
                return datetime.now().strftime(formato)
            except:
                return datetime.now().strftime('%d/%m/%Y')
        
        patron = r'\{\{fecha:([^}]+)\}\}'
        return re.sub(patron, reemplazar_fecha, contenido)
    
    def _procesar_funcion_monto(self, contenido: str, variables: Dict[str, Any]) -> str:
        """Procesa funciones de monto: {{monto:variable}}"""
        def reemplazar_monto(match):
            variable = match.group(1).strip()
            valor = variables.get(variable, 0)
            try:
                monto = float(valor)
                return f"${monto:,.0f}"
            except:
                return str(valor)
        
        patron = r'\{\{monto:([^}]+)\}\}'
        return re.sub(patron, reemplazar_monto, contenido)
    
    def _procesar_funcion_condicional(self, contenido: str, variables: Dict[str, Any]) -> str:
        """Procesa funciones condicionales: {{si:variable:texto_true:texto_false}}"""
        def reemplazar_condicional(match):
            partes = match.group(1).split(':')
            if len(partes) < 3:
                return match.group(0)  # Devolver original si formato incorrecto
            
            variable = partes[0].strip()
            texto_true = partes[1].strip()
            texto_false = partes[2].strip() if len(partes) > 2 else ""
            
            valor = variables.get(variable, False)
            # Evaluar como verdadero si no es None, 0, False, o string vacío
            es_verdadero = bool(valor) and valor != "" and valor != 0
            
            return texto_true if es_verdadero else texto_false
        
        patron = r'\{\{si:([^}]+)\}\}'
        return re.sub(patron, reemplazar_condicional, contenido)
    
    def _procesar_funcion_dias_desde(self, contenido: str, variables: Dict[str, Any]) -> str:
        """Procesa función de días transcurridos: {{dias_desde:variable_fecha}}"""
        def reemplazar_dias(match):
            variable = match.group(1).strip()
            fecha_str = variables.get(variable, '')
            
            try:
                # Intentar parsear diferentes formatos de fecha
                formatos = ['%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y']
                fecha = None
                
                for formato in formatos:
                    try:
                        fecha = datetime.strptime(str(fecha_str), formato)
                        break
                    except:
                        continue
                
                if fecha:
                    dias = (datetime.now() - fecha).days
                    return str(dias)
                else:
                    return "0"
                    
            except Exception as e:
                logging.error(f"Error al calcular días desde {fecha_str}: {e}")
                return "0"
        
        patron = r'\{\{dias_desde:([^}]+)\}\}'
        return re.sub(patron, reemplazar_dias, contenido)
    
    def validar_plantilla(self, contenido: str) -> Dict[str, Any]:
        """Valida una plantilla y devuelve información sobre variables encontradas"""
        try:
            # Encontrar todas las variables
            patron_variables = r'\{\{\s*([^}]+)\s*\}\}'
            variables_encontradas = re.findall(patron_variables, contenido)
            
            # Clasificar variables
            variables_sistema = []
            variables_usuario = []
            funciones_especiales = []
            
            for var in variables_encontradas:
                var_limpia = var.strip()
                if ':' in var_limpia:
                    # Es una función especial
                    funciones_especiales.append(var_limpia)
                elif var_limpia in self.variables_sistema:
                    variables_sistema.append(var_limpia)
                else:
                    variables_usuario.append(var_limpia)
            
            return {
                'valida': True,
                'variables_encontradas': len(variables_encontradas),
                'variables_sistema': list(set(variables_sistema)),
                'variables_usuario': list(set(variables_usuario)),
                'funciones_especiales': list(set(funciones_especiales)),
                'errores': []
            }
            
        except Exception as e:
            return {
                'valida': False,
                'error': str(e),
                'variables_encontradas': 0,
                'variables_sistema': [],
                'variables_usuario': [],
                'funciones_especiales': [],
                'errores': [str(e)]
            }
    
    def obtener_datos_gimnasio(self) -> Dict[str, Any]:
        """Obtiene datos del gimnasio para usar en plantillas"""
        return self.variables_sistema
    
    def obtener_variables_disponibles(self) -> Dict[str, Dict[str, str]]:
        """Obtiene lista de variables disponibles organizadas por categoría"""
        return {
            'sistema': {
                'nombre_gimnasio': 'Nombre del gimnasio',
                'telefono_gimnasio': 'Teléfono del gimnasio',
                'direccion_gimnasio': 'Dirección del gimnasio',
                'email_gimnasio': 'Email del gimnasio',
                'horarios_atencion': 'Horarios de atención',
                'sitio_web': 'Sitio web del gimnasio',
                'fecha_actual': 'Fecha actual (dd/mm/yyyy)',
                'hora_actual': 'Hora actual (HH:MM)',
                'año_actual': 'Año actual',
                'mes_actual': 'Mes actual',
                'dia_semana': 'Día de la semana'
            },
            'usuario': {
                'nombre_usuario': 'Nombre del usuario',
                'telefono_usuario': 'Teléfono del usuario',
                'dni_usuario': 'DNI del usuario',
                'tipo_cuota': 'Tipo de cuota del usuario'
            },
            'pago': {
                'monto_pago': 'Monto del pago',
                'concepto_pago': 'Concepto del pago',
                'fecha_pago': 'Fecha del pago',
                'metodo_pago': 'Método de pago utilizado',
                'numero_recibo': 'Número de recibo'
            },
            'cuota': {
                'dias_vencido': 'Días de vencimiento',
                'monto_cuota': 'Monto de la cuota',
                'fecha_vencimiento': 'Fecha de vencimiento',
                'periodo_cuota': 'Período de la cuota (mes/año)'
            }
        }
    
    def obtener_funciones_especiales(self) -> Dict[str, str]:
        """Obtiene lista de funciones especiales disponibles"""
        return {
            '{{fecha:formato}}': 'Fecha actual con formato personalizado (ej: {{fecha:%d/%m/%Y}})',
            '{{monto:variable}}': 'Formatea una variable como monto (ej: {{monto:precio}})',
            '{{si:variable:texto_true:texto_false}}': 'Condicional simple (ej: {{si:activo:Activo:Inactivo}})',
            '{{dias_desde:fecha}}': 'Días transcurridos desde una fecha '
                                   '(ej: {{dias_desde:fecha_vencimiento}})'
        }

    def generar_vista_previa(self, contenido: str,
                           variables_ejemplo: Dict[str, Any] = None) -> str:
        """Genera una vista previa de la plantilla con datos de ejemplo"""
        if not variables_ejemplo:
            variables_ejemplo = {
                'nombre_usuario': 'Juan Pérez',
                'telefono_usuario': '+54911234567',
                'dni_usuario': '12345678',
                'monto_pago': 15000,
                'concepto_pago': 'Cuota Mensual',
                'fecha_pago': datetime.now().strftime('%d/%m/%Y'),
                'metodo_pago': 'Efectivo',
                'dias_vencido': 5,
                'monto_cuota': 15000,
                'fecha_vencimiento': (datetime.now() -
                                     timedelta(days=5)).strftime('%d/%m/%Y'),
                'periodo_cuota': f"{datetime.now().strftime('%m/%Y')}"
            }

        return self.procesar_plantilla(contenido, variables_ejemplo)

    def limpiar_plantilla(self, contenido: str) -> str:
        """Limpia y normaliza el contenido de una plantilla"""
        if not contenido:
            return ""
        
        # Normalizar espacios en blanco
        contenido = re.sub(r'\s+', ' ', contenido.strip())
        
        # Normalizar variables (eliminar espacios extra dentro de {{}})
        def normalizar_variable(match):
            variable = match.group(1).strip()
            return f"{{{{{variable}}}}}"
        
        patron = r'\{\{\s*([^}]+)\s*\}\}'
        contenido = re.sub(patron, normalizar_variable, contenido)
        
        return contenido

    def extraer_variables_de_plantilla(self, contenido: str) -> List[str]:
        """Extrae todas las variables únicas de una plantilla"""
        patron = r'\{\{\s*([^}:]+)(?::[^}]*)?\s*\}\}'
        variables = re.findall(patron, contenido)
        return list(set([var.strip() for var in variables]))


# Función de utilidad
def crear_template_processor(database_manager: DatabaseManager) -> \
        TemplateProcessor:
    """Crea una instancia del procesador de plantillas"""
    return TemplateProcessor(database_manager)