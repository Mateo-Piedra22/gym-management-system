#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Message Logger - Sistema de registro y control anti-spam de mensajes WhatsApp
Maneja el registro de mensajes enviados y recibidos, y controla el anti-spam
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from .database import DatabaseManager

class MessageLogger:
    """Gestor de registro y control anti-spam de mensajes WhatsApp"""
    
    def __init__(self, database_manager: DatabaseManager):
        self.db = database_manager
        self.config_antispam = self._cargar_configuracion_antispam()
    
    def _cargar_configuracion_antispam(self) -> Dict[str, int]:
        """Carga la configuración anti-spam desde la base de datos"""
        try:
            return {
                'max_mensajes_por_hora': int(self.db.obtener_configuracion('whatsapp_max_mensajes_hora') or 10),
                'max_mensajes_por_dia': int(self.db.obtener_configuracion('whatsapp_max_mensajes_dia') or 50),
                'intervalo_minimo_minutos': int(self.db.obtener_configuracion('whatsapp_intervalo_minimo') or 5),
                'max_intentos_fallidos': int(self.db.obtener_configuracion('whatsapp_max_intentos_fallidos') or 5)
            }
        except Exception as e:
            logging.error(f"Error cargando configuración anti-spam: {e}")
            return {
                'max_mensajes_por_hora': 10,
                'max_mensajes_por_dia': 50,
                'intervalo_minimo_minutos': 5
            }
    
    def _obtener_user_id_por_telefono(self, telefono: str) -> int:
        """Obtiene el ID de usuario por número de teléfono"""
        try:
            with self.db.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    # Intentar buscar como string primero, luego como integer
                    cursor.execute("SELECT id FROM usuarios WHERE telefono = %s", (str(telefono),))
                    result = cursor.fetchone()
                    return result[0] if result else None
        except Exception as e:
            logging.error(f"Error obteniendo user_id por teléfono: {e}")
            return None
    
    def registrar_mensaje_enviado(self, telefono: str, mensaje: str, tipo_mensaje: str = "welcome", 
                                  message_id: str = None) -> bool:
        """Registra un mensaje enviado exitosamente"""
        try:
            user_id = self._obtener_user_id_por_telefono(telefono)
            
            # Validar tipo_mensaje
            tipos_validos = ['overdue', 'payment', 'welcome', 'deactivation', 'class_reminder', 'waitlist']
            if tipo_mensaje not in tipos_validos:
                tipo_mensaje = 'welcome'
            
            return self.db.registrar_mensaje_whatsapp(
                user_id=user_id,
                message_type=tipo_mensaje,
                template_name="manual",
                phone_number=telefono,
                message_content=mensaje,
                status="sent",
                message_id=message_id
            )
        except Exception as e:
            logging.error(f"Error al registrar mensaje enviado: {e}")
            return False
    
    def registrar_mensaje_recibido(self, telefono: str, mensaje: str, tipo_mensaje: str = "welcome", message_id: str = None) -> bool:
        """Registra un mensaje recibido"""
        try:
            user_id = self._obtener_user_id_por_telefono(telefono)
            
            # Validar tipo_mensaje
            tipos_validos = ['overdue', 'payment', 'welcome', 'deactivation', 'class_reminder', 'waitlist']
            if tipo_mensaje not in tipos_validos:
                tipo_mensaje = 'welcome'
            
            return self.db.registrar_mensaje_whatsapp(
                user_id=user_id,
                message_type=tipo_mensaje,
                template_name="incoming",
                phone_number=telefono,
                message_content=mensaje,
                status="received",
                message_id=message_id
            )
        except Exception as e:
            logging.error(f"Error al registrar mensaje recibido: {e}")
            return False
    
    def registrar_mensaje_fallido(self, telefono: str, mensaje: str, error: str, tipo_mensaje: str = "welcome") -> bool:
        """Registra un mensaje que falló al enviarse"""
        try:
            user_id = self._obtener_user_id_por_telefono(telefono)
            
            # Validar tipo_mensaje
            tipos_validos = ['overdue', 'payment', 'welcome', 'deactivation', 'class_reminder', 'waitlist']
            if tipo_mensaje not in tipos_validos:
                tipo_mensaje = 'welcome'
            
            return self.db.registrar_mensaje_whatsapp(
                user_id=user_id,
                message_type=tipo_mensaje,
                template_name="failed",
                phone_number=telefono,
                message_content=f"{mensaje} - Error: {error}",
                status="failed"
            )
        except Exception as e:
            logging.error(f"Error al registrar mensaje fallido: {e}")
            return False
    
    def puede_enviar_mensaje(self, telefono: str, tipo_mensaje: str = None) -> bool:
        """Verifica si se puede enviar un mensaje según las reglas anti-spam"""
        try:
            # Verificar mensajes por hora
            if not self._verificar_limite_por_hora(telefono):
                logging.warning(f"Límite por hora excedido para {telefono}")
                return False
            
            # Verificar mensajes por día
            if not self._verificar_limite_por_dia(telefono):
                logging.warning(f"Límite diario excedido para {telefono}")
                return False
            
            # Verificar intervalo mínimo
            if not self._verificar_intervalo_minimo(telefono):
                logging.warning(f"Intervalo mínimo no cumplido para {telefono}")
                return False
            
            # Verificar intentos fallidos
            if not self._verificar_intentos_fallidos(telefono):
                logging.warning(f"Demasiados intentos fallidos para {telefono}")
                return False
            
            return True
            
        except Exception as e:
            logging.error(f"Error al verificar si puede enviar mensaje: {e}")
            return False
    
    def _verificar_limite_por_hora(self, telefono: str) -> bool:
        """Verifica el límite de mensajes por hora"""
        try:
            hora_limite = datetime.now() - timedelta(hours=1)
            mensajes_hora = self.db.contar_mensajes_whatsapp_periodo(
                telefono=telefono,
                direccion='enviado',
                fecha_desde=hora_limite
            )
            return mensajes_hora < self.config_antispam['max_mensajes_por_hora']
        except Exception as e:
            logging.error(f"Error al verificar límite por hora: {e}")
            return True  # En caso de error, permitir envío
    
    def _verificar_limite_por_dia(self, telefono: str) -> bool:
        """Verifica el límite de mensajes por día"""
        try:
            dia_limite = datetime.now() - timedelta(days=1)
            mensajes_dia = self.db.contar_mensajes_whatsapp_periodo(
                telefono=telefono,
                direccion='enviado',
                fecha_desde=dia_limite
            )
            return mensajes_dia < self.config_antispam['max_mensajes_por_dia']
        except Exception as e:
            logging.error(f"Error al verificar límite por día: {e}")
            return True
    
    def _verificar_intervalo_minimo(self, telefono: str) -> bool:
        """Verifica el intervalo mínimo entre mensajes"""
        try:
            ultimo_mensaje = self.db.obtener_ultimo_mensaje_whatsapp(
                telefono=telefono,
                direccion='enviado'
            )
            
            if not ultimo_mensaje:
                return True
            
            # Asegurar que ambos datetime sean naive para evitar errores de timezone
            fecha_envio = ultimo_mensaje['fecha_envio']
            if hasattr(fecha_envio, 'replace') and fecha_envio.tzinfo is not None:
                fecha_envio = fecha_envio.replace(tzinfo=None)
            
            tiempo_transcurrido = datetime.now() - fecha_envio
            intervalo_minimo = timedelta(minutes=self.config_antispam['intervalo_minimo_minutos'])
            
            return tiempo_transcurrido >= intervalo_minimo
            
        except Exception as e:
            logging.error(f"Error al verificar intervalo mínimo: {e}")
            return True
    
    def _verificar_intentos_fallidos(self, telefono: str) -> bool:
        """Verifica si hay demasiados intentos fallidos recientes"""
        try:
            # Verificar intentos fallidos en las últimas 24 horas
            dia_limite = datetime.now() - timedelta(days=1)
            intentos_fallidos = self.db.contar_mensajes_whatsapp_periodo(
                telefono=telefono,
                direccion='enviado',
                estado='fallido',
                fecha_desde=dia_limite
            )
            
            return intentos_fallidos < self.config_antispam['max_intentos_fallidos']
            
        except Exception as e:
            logging.error(f"Error al verificar intentos fallidos: {e}")
            return True
    
    def verificar_mensaje_enviado_reciente(self, telefono: str, tipo_mensaje: str, horas: int = 24) -> bool:
        """Verifica si ya se envió un mensaje del tipo especificado recientemente"""
        try:
            user_id = self._obtener_user_id_por_telefono(telefono)
            if not user_id:
                return False
            
            return self.db.verificar_mensaje_enviado_reciente(
                user_id=user_id,
                message_type=tipo_mensaje,
                horas_limite=horas
            )
        except Exception as e:
            logging.error(f"Error al verificar mensaje reciente: {e}")
            return False
    
    def obtener_historial_mensajes(self, telefono: str = None, usuario_id: int = None, 
                                  limite: int = 50) -> List[Dict[str, Any]]:
        """Obtiene el historial de mensajes"""
        try:
            # Si se proporciona teléfono, obtener user_id
            if telefono and not usuario_id:
                usuario_id = self._obtener_user_id_por_telefono(telefono)
            
            return self.db.obtener_historial_mensajes_whatsapp(
                user_id=usuario_id,
                limit=limite
            )
        except Exception as e:
            logging.error(f"Error al obtener historial de mensajes: {e}")
            return []
    
    def obtener_estadisticas_diarias(self, fecha: datetime = None) -> Dict[str, int]:
        """Obtiene estadísticas de mensajes del día"""
        try:
            if not fecha:
                fecha = datetime.now()
            
            inicio_dia = fecha.replace(hour=0, minute=0, second=0, microsecond=0)
            fin_dia = inicio_dia + timedelta(days=1)
            
            enviados = self.db.contar_mensajes_whatsapp_periodo(
                direccion='enviado',
                fecha_desde=inicio_dia,
                fecha_hasta=fin_dia
            )
            
            recibidos = self.db.contar_mensajes_whatsapp_periodo(
                direccion='recibido',
                fecha_desde=inicio_dia,
                fecha_hasta=fin_dia
            )
            
            fallidos = self.db.contar_mensajes_whatsapp_periodo(
                direccion='enviado',
                estado='fallido',
                fecha_desde=inicio_dia,
                fecha_hasta=fin_dia
            )
            
            total_intentos = enviados + fallidos
            tasa_exito = 0
            if total_intentos > 0:
                tasa_exito = round((enviados / total_intentos) * 100, 2)
            
            return {
                'enviados': enviados,
                'recibidos': recibidos,
                'fallidos': fallidos,
                'total': enviados + recibidos,
                'tasa_exito': tasa_exito
            }
            
        except Exception as e:
            logging.error(f"Error al obtener estadísticas diarias: {e}")
            return {
                'enviados': 0, 'recibidos': 0, 'fallidos': 0,
                'total': 0, 'tasa_exito': 0
            }
    
    def obtener_estadisticas_semanales(self, fecha: datetime = None) -> Dict[str, int]:
        """Obtiene estadísticas de mensajes de la semana"""
        try:
            if not fecha:
                fecha = datetime.now()
            
            # Calcular inicio de la semana (lunes)
            dias_desde_lunes = fecha.weekday()
            inicio_semana = fecha - timedelta(days=dias_desde_lunes)
            inicio_semana = inicio_semana.replace(hour=0, minute=0, second=0, microsecond=0)
            fin_semana = inicio_semana + timedelta(days=7)
            
            enviados = self.db.contar_mensajes_whatsapp_periodo(
                direccion='enviado',
                fecha_desde=inicio_semana,
                fecha_hasta=fin_semana
            )
            
            recibidos = self.db.contar_mensajes_whatsapp_periodo(
                direccion='recibido',
                fecha_desde=inicio_semana,
                fecha_hasta=fin_semana
            )
            
            fallidos = self.db.contar_mensajes_whatsapp_periodo(
                direccion='enviado',
                estado='fallido',
                fecha_desde=inicio_semana,
                fecha_hasta=fin_semana
            )
            
            total_intentos = enviados + fallidos
            tasa_exito = 0
            if total_intentos > 0:
                tasa_exito = round((enviados / total_intentos) * 100, 2)
            
            return {
                'enviados': enviados,
                'recibidos': recibidos,
                'fallidos': fallidos,
                'total': enviados + recibidos,
                'tasa_exito': tasa_exito
            }
            
        except Exception as e:
            logging.error(f"Error al obtener estadísticas semanales: {e}")
            return {
                'enviados': 0, 'recibidos': 0, 'fallidos': 0,
                'total': 0, 'tasa_exito': 0
            }
    
    def obtener_estadisticas_por_tipo(self, dias: int = 7) -> Dict[str, Dict[str, int]]:
        """Obtiene estadísticas de mensajes por tipo en los últimos días"""
        try:
            fecha_limite = datetime.now() - timedelta(days=dias)
            
            tipos_mensaje = ['welcome', 'payment', 'overdue', 'deactivation', 'class_reminder', 'waitlist']
            estadisticas = {}
            
            for tipo in tipos_mensaje:
                # Obtener mensajes enviados
                enviados = self.db.contar_mensajes_whatsapp_periodo(
                    tipo_mensaje=tipo,
                    direccion='enviado',
                    fecha_desde=fecha_limite
                )
                
                # Obtener mensajes fallidos
                fallidos = self.db.contar_mensajes_whatsapp_periodo(
                    tipo_mensaje=tipo,
                    direccion='fallido',
                    fecha_desde=fecha_limite
                )
                
                # Asegurar que los valores sean enteros
                enviados = enviados if isinstance(enviados, int) else 0
                fallidos = fallidos if isinstance(fallidos, int) else 0
                
                estadisticas[tipo] = {
                    'enviados': enviados,
                    'fallidos': fallidos
                }
            
            return estadisticas
            
        except Exception as e:
            logging.error(f"Error al obtener estadísticas por tipo: {e}")
            return {}
    
    def limpiar_mensajes_antiguos(self, dias_antiguedad: int = 90) -> int:
        """Limpia mensajes antiguos de la base de datos"""
        try:
            return self.db.limpiar_mensajes_antiguos_whatsapp(dias_antiguedad)
        except Exception as e:
            logging.error(f"Error al limpiar mensajes antiguos: {e}")
            return 0
    
    def marcar_mensaje_como_leido(self, message_id: str) -> bool:
        """Marca un mensaje como leído"""
        try:
            return self.db.actualizar_estado_mensaje_whatsapp(message_id, 'read')
        except Exception as e:
            logging.error(f"Error al marcar mensaje como leído: {e}")
            return False
    
    def marcar_mensaje_como_entregado(self, message_id: str) -> bool:
        """Marca un mensaje como entregado"""
        try:
            return self.db.actualizar_estado_mensaje_whatsapp(message_id, 'delivered')
        except Exception as e:
            logging.error(f"Error al marcar mensaje como entregado: {e}")
            return False
    
    def obtener_usuarios_bloqueados(self) -> List[str]:
        """Obtiene lista de teléfonos bloqueados por anti-spam"""
        try:
            # Usuarios con demasiados intentos fallidos en las últimas 24 horas
            fecha_limite = datetime.now() - timedelta(days=1)
            
            usuarios_bloqueados = []
            
            # Obtener todos los teléfonos únicos con mensajes fallidos recientes
            telefonos_con_fallos = self.db.obtener_telefonos_con_mensajes_fallidos(fecha_limite)
            
            for telefono in telefonos_con_fallos:
                if not self._verificar_intentos_fallidos(telefono):
                    usuarios_bloqueados.append(telefono)
            
            return usuarios_bloqueados
            
        except Exception as e:
            logging.error(f"Error al obtener usuarios bloqueados: {e}")
            return []
    
    def desbloquear_usuario(self, telefono: str) -> bool:
        """Desbloquea un usuario eliminando sus mensajes fallidos recientes"""
        try:
            fecha_limite = datetime.now() - timedelta(hours=1)
            return self.db.limpiar_mensajes_fallidos_usuario(telefono, fecha_limite)
        except Exception as e:
            logging.error(f"Error al desbloquear usuario {telefono}: {e}")
            return False
    
    def actualizar_configuracion_antispam(self, nueva_config: Dict[str, int]) -> bool:
        """Actualiza la configuración anti-spam en la tabla genérica de configuración."""
        try:
            # Validar configuración
            campos_requeridos = [
                'max_mensajes_por_hora', 'max_mensajes_por_dia',
                'intervalo_minimo_minutos', 'max_intentos_fallidos'
            ]

            for campo in campos_requeridos:
                if (campo not in nueva_config or
                        not isinstance(nueva_config[campo], int)):
                    raise ValueError(
                        f"Campo {campo} requerido y debe ser entero"
                    )

            # Mapear a claves almacenadas en `configuracion`
            mapping = {
                'max_mensajes_por_hora': 'whatsapp_max_mensajes_hora',
                'max_mensajes_por_dia': 'whatsapp_max_mensajes_dia',
                'intervalo_minimo_minutos': 'whatsapp_intervalo_minimo',
                'max_intentos_fallidos': 'whatsapp_max_intentos_fallidos',
            }

            # Actualizar configuración en base de datos (tabla genérica)
            for campo, valor in nueva_config.items():
                if campo in mapping:
                    self.db.actualizar_configuracion(mapping[campo], str(int(valor)))

            # Recargar configuración local
            self.config_antispam = self._cargar_configuracion_antispam()

            logging.info("Configuración anti-spam actualizada exitosamente")
            return True

        except Exception as e:
            logging.error(f"Error al actualizar configuración anti-spam: {e}")
            return False


# Función de utilidad
def crear_message_logger(database_manager: DatabaseManager) -> MessageLogger:
    """Crea una instancia del logger de mensajes"""
    return MessageLogger(database_manager)