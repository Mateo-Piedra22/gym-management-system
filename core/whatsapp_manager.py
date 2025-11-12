#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
WhatsApp Manager - Sistema de mensajería WhatsApp Business
Gestiona el envío de mensajes automáticos para el gimnasio
"""

import os
import logging
import asyncio
import threading
import psycopg2.extras
import json
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from pywa import WhatsApp
from pywa.types import Message
from pywa.types.templates import TemplateLanguage, BodyText, HeaderImage, HeaderText
from .database import DatabaseManager
from .template_processor import TemplateProcessor
from .message_logger import MessageLogger
from typing import Any, Dict, List, Optional
try:
    import requests  # type: ignore
except Exception:
    requests = None  # type: ignore

class WhatsAppManager:
    """Gestor de mensajes WhatsApp usando PyWa"""
    
    def __init__(self, database_manager: DatabaseManager, defer_init: bool = True):
        """Inicializa el gestor; permite diferir la creación del cliente para no bloquear la UI"""
        # Cargar configuración desde base de datos y entorno (sin hardcodes)
        self.db = database_manager
        cfg = {}
        try:
            cfg = self.db.obtener_configuracion_whatsapp_completa() or {}
        except Exception:
            cfg = {}

        # IDs de WhatsApp Business
        self.phone_number_id = (cfg.get('phone_id') or os.getenv('WHATSAPP_PHONE_NUMBER_ID') or "")
        self.whatsapp_business_account_id = (cfg.get('waba_id') or os.getenv('WHATSAPP_BUSINESS_ACCOUNT_ID') or "")

        # Token de acceso (prefiere configuración completa; fallback a entorno seguro)
        token = cfg.get('access_token')
        if not token:
            try:
                from .secure_config import config as secure_config
                token = secure_config.get_whatsapp_access_token()
            except Exception:
                token = None
        self.access_token = token
        
        self.template_processor = TemplateProcessor(database_manager)
        self.message_logger = MessageLogger(database_manager)
        self.client = None
        self.wa_client = None
        self.servidor_activo = False
        self._server_thread = None
        self._config = None
        self._init_deferred = defer_init
        self._client_initialized = False
        # Non-blocking y timeouts para envíos
        try:
            self._send_timeout_seconds = float(os.getenv("WHATSAPP_SEND_TIMEOUT_SECONDS", "1.5"))
        except Exception:
            self._send_timeout_seconds = 1.5
        self._nonblocking_send = (os.getenv("NONBLOCKING_WHATSAPP_SEND", "1") == "1")
        self._send_max_blocking_retries = 0  # evitar duplicados si hay timeout

        # Preferencias avanzadas y listas
        self._allowlist_enabled = False
        self._allowlist: set[str] = set()
        self._max_init_retries = 3
        self._retry_base_delay_seconds = 5
        self._init_lock = asyncio.Lock()
        self._stop_init = False
        
        # Configurar logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # No inicializar cliente inmediatamente si está diferido; esto evita bloqueos en arranque
        if not self._init_deferred:
            self._initialize_client()

    def reinicializar_configuracion(self) -> None:
        """Recarga configuración desde DB y aplica preferencias sin bloquear UI."""
        try:
            cfg = None
            try:
                # Cargar configuración completa si está disponible
                cfg = self.db.obtener_configuracion_whatsapp_completa()
            except Exception:
                cfg = {}

            self._config = cfg or {}

            # Allowlist: números separados por comas en 'allowlist_numbers' y flag 'allowlist_enabled'
            raw_allow = str(self._config.get('allowlist_numbers', '') or '').strip()
            numbers = [n.strip() for n in raw_allow.split(',') if n.strip()]
            self._allowlist = set(numbers)
            try:
                self._allowlist_enabled = str(self._config.get('allowlist_enabled', 'false')).lower() == 'true'
            except Exception:
                self._allowlist_enabled = False

            # Reintentos/backoff configurables
            try:
                self._max_init_retries = int(self._config.get('max_retries', self._max_init_retries))
            except Exception:
                pass
            try:
                self._retry_base_delay_seconds = int(self._config.get('retry_delay_seconds', self._retry_base_delay_seconds))
            except Exception:
                pass

            # Si ya hay cliente, no bloquear; solo actualizar handlers según config
            try:
                if self.wa_client:
                    self._setup_message_handlers()
            except Exception:
                pass
        except Exception as e:
            logging.error(f"Error al reinicializar configuración de WhatsAppManager: {e}")

    def _numero_permitido(self, telefono: str) -> bool:
        """Devuelve True si allowlist está deshabilitado o el número está permitido."""
        try:
            if not self._allowlist_enabled:
                return True
            tel = str(telefono or '').strip()
            return tel in self._allowlist
        except Exception:
            return False

    def initialize_async(self, max_retries: Optional[int] = None, delay_seconds: Optional[float] = None) -> None:
        """Inicializa el cliente en segundo plano con reintentos/backoff y jitter."""
        import threading, time, random

        max_r = max_retries if isinstance(max_retries, int) and max_retries >= 0 else self._max_init_retries
        base_delay = delay_seconds if (isinstance(delay_seconds, (int, float)) and delay_seconds >= 0) else self._retry_base_delay_seconds

        def _runner():
            try:
                # Evitar ejecuciones concurrentes
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                async def _init_with_lock():
                    async with self._init_lock:
                        attempt = 0
                        while not self._client_initialized and attempt <= max_r and not self._stop_init:
                            attempt += 1
                            try:
                                self._initialize_client()
                                self._client_initialized = bool(self.wa_client)
                                if self._client_initialized:
                                    logging.info("WhatsApp client inicializado (async)")
                                    break
                            except Exception as err:
                                logging.error(f"Fallo inicializando WhatsApp client (intento {attempt}/{max_r}): {err}")
                            # Backoff con jitter
                            sleep_s = base_delay * (2 ** (attempt - 1))
                            jitter = random.uniform(0, base_delay)
                            time.sleep(min(300, sleep_s + jitter))
                loop.run_until_complete(_init_with_lock())
            except Exception as e:
                logging.error(f"Error en initialize_async: {e}")

        t = threading.Thread(target=_runner, daemon=True)
        t.start()

    # --- Fallback cuando WhatsApp no está disponible: no-op sin gestor anterior ---
    def _enqueue_offline_op(self, func_name: str, kwargs: Dict[str, Any]) -> bool:
        """Registra la intención de enviar cuando WhatsApp no está disponible.

        Se elimina el acoplamiento con el gestor anterior OfflineSyncManager.
        Retorna False para indicar que no se encoló.
        """
        try:
            logging.info(f"WhatsApp no disponible; operación '{func_name}' no encolada")
        except Exception:
            pass
        return False
    
    def _initialize_client(self):
        """Inicializa el cliente de WhatsApp con los datos configurados"""
        try:
            if not self.access_token:
                logging.error("WhatsApp: Access Token no configurado")
                return False
            # Inicializar cliente PyWa con los parámetros correctos
            self.wa_client = WhatsApp(
                phone_id=self.phone_number_id,
                token=self.access_token,
                business_account_id=self.whatsapp_business_account_id
            )
            logging.info("WhatsApp client inicializado correctamente")
            logging.info(f"Phone ID: {self.phone_number_id} | WABA ID: {self.whatsapp_business_account_id}")
            self._client_initialized = True
            # Cargar/actualizar configuración tras init
            try:
                self.reinicializar_configuracion()
            except Exception:
                pass
            return True
        except Exception as e:
            logging.error(f"Error al inicializar cliente WhatsApp: {e}")
            return False

    # (El método initialize_async con reintentos/backoff ya está definido más arriba; se elimina duplicado)
    
    def _setup_message_handlers(self):
        """Configura los manejadores de mensajes entrantes (si webhook está habilitado)"""
        if not self.wa_client:
            return
        # Respetar flag de configuración para habilitar webhook/handlers y disponibilidad de servidor
        try:
            enable_webhook = str((self._config or {}).get('enable_webhook', 'false')).lower() == 'true'
        except Exception:
            enable_webhook = False
        if not enable_webhook:
            return

        # Algunos entornos requieren un servidor web embebido; si no está disponible, no registrar handlers
        try:
            has_server = hasattr(self.wa_client, 'server') and bool(getattr(self.wa_client, 'server'))
        except Exception:
            has_server = False
        if not has_server:
            logging.info("WhatsApp handlers omitidos: servidor webhook no configurado")
            return

        @self.wa_client.on_message
        def handle_message(client: WhatsApp, message: Message):
            """Maneja mensajes entrantes de WhatsApp"""
            try:
                # Registrar mensaje recibido
                self.message_logger.registrar_mensaje_recibido(
                    telefono=message.from_user.wa_id,
                    mensaje=message.text or "[Mensaje multimedia]",
                    tipo_mensaje="welcome",
                    message_id=getattr(message, 'id', None)
                )
                
                # Procesar respuestas automáticas si están habilitadas
                if self._config.get('respuestas_automaticas', False):
                    self._procesar_respuesta_automatica(message)
                    
            except Exception as e:
                logging.error(f"Error al procesar mensaje entrante: {e}")

    def _call_with_timeout(self, fn):
        """Ejecuta una llamada potencialmente bloqueante con un timeout corto.
        Si expira, no reintenta para evitar duplicados; retorna (ok, resp_or_err).
        """
        import threading
        result = {"ok": False, "resp": None, "err": None}
        def _runner():
            try:
                result["resp"] = fn()
                result["ok"] = True
            except Exception as e:
                result["err"] = e
        t = threading.Thread(target=_runner, name="WA-SendCall", daemon=True)
        t.start()
        t.join(self._send_timeout_seconds)
        if result["ok"]:
            return True, result["resp"]
        if t.is_alive():
            # Evitar duplicados: no reintentar si el hilo sigue activo
            logging.warning("WhatsApp send call excedió timeout; liberando UI y continuando en background")
            return False, TimeoutError("send timeout")
        else:
            return False, result["err"]

    def _send_message(self, to: str, text: str):
        """Envía mensaje simple con política non-blocking/timeout."""
        if not self.wa_client:
            raise RuntimeError("wa_client no inicializado")
        if self._nonblocking_send:
            import threading
            threading.Thread(
                target=lambda: self.wa_client.send_message(to=to, text=text),
                name="WA-NonBlockingSendMessage",
                daemon=True,
            ).start()
            return True, None
        else:
            return self._call_with_timeout(lambda: self.wa_client.send_message(to=to, text=text))

    def _get_language_code(self, language: Any) -> str:
        """Obtiene el código de idioma para Graph API de forma segura."""
        try:
            if isinstance(language, str):
                return language
            code = getattr(language, "value", None) or getattr(language, "code", None)
            if code:
                return str(code)
        except Exception:
            pass
        # Valor por defecto razonable
        return "es"

    def _send_template_http_basic(self, to: str, name: str, language: Any, body_params: List[str], header_image_url: Optional[str] = None, header_text: Optional[str] = None):
        """Fallback HTTP directo al Graph API para enviar una plantilla básica.

        Solo soporta parámetros de cuerpo como texto y, opcionalmente, header de imagen o texto.
        Retorna (ok, resp_dict).
        """
        try:
            if not requests:
                return False, {"error": "requests no disponible"}
            if not self.access_token:
                return False, {"error": "access_token no configurado"}
            phone_id = str(self.phone_number_id or "").strip()
            if not phone_id:
                return False, {"error": "phone_number_id no configurado"}

            url = f"https://graph.facebook.com/v19.0/{phone_id}/messages"
            headers = {
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "application/json"
            }
            components: List[Dict[str, Any]] = []
            # Header image o texto si aplica
            if header_image_url:
                components.append({
                    "type": "header",
                    "parameters": [
                        {"type": "image", "image": {"link": header_image_url}}
                    ]
                })
            elif header_text:
                components.append({
                    "type": "header",
                    "parameters": [
                        {"type": "text", "text": header_text}
                    ]
                })
            # Body con parámetros de texto
            if body_params and isinstance(body_params, list):
                components.append({
                    "type": "body",
                    "parameters": [{"type": "text", "text": str(p)} for p in body_params]
                })

            payload = {
                "messaging_product": "whatsapp",
                "to": to,
                "type": "template",
                "template": {
                    "name": name,
                    "language": {"code": self._get_language_code(language)},
                    "components": components
                }
            }

            resp = requests.post(url, headers=headers, json=payload, timeout=self._send_timeout_seconds)
            ok = 200 <= int(getattr(resp, "status_code", 500)) < 300
            try:
                data = resp.json() if hasattr(resp, "json") else {}
            except Exception:
                data = {"text": getattr(resp, "text", "")}
            # Normalizar id si viene en messages
            try:
                if isinstance(data, dict) and "messages" in data and data["messages"]:
                    msg_id = (data["messages"][0] or {}).get("id")
                    data["id"] = msg_id
            except Exception:
                pass
            return bool(ok), data
        except Exception as e:
            logging.error(f"Error en HTTP fallback de plantilla: {e}")
            return False, {"error": str(e)}

    def _send_template(self, to: str, name: str, language: TemplateLanguage, params: list):
        """Envía plantilla con política non-blocking/timeout."""
        if not self.wa_client:
            raise RuntimeError("wa_client no inicializado")
        if self._nonblocking_send:
            import threading
            threading.Thread(
                target=lambda: self.wa_client.send_template(to=to, name=name, language=language, params=params),
                name="WA-NonBlockingSendTemplate",
                daemon=True,
            ).start()
            return True, None
        else:
            return self._call_with_timeout(lambda: self.wa_client.send_template(to=to, name=name, language=language, params=params))

    def _procesar_respuesta_automatica(self, message: Message):
        """Procesa respuestas automáticas básicas"""
        texto = (message.text or "").lower().strip()
        
        respuestas = {
            "hola": "¡Hola! Gracias por contactarnos. Para consultas sobre membresías, horarios o pagos, puedes llamarnos o visitarnos en el gimnasio.",
            "horarios": "Nuestros horarios de atención son de Lunes a Viernes de 6:00 a 22:00 y Sábados de 8:00 a 20:00.",
            "precios": "Para información sobre precios y planes, por favor contacta con recepción o visita nuestras instalaciones.",
            "ubicacion": "Puedes encontrarnos en nuestra dirección. ¡Te esperamos!"
        }
        
        for palabra_clave, respuesta in respuestas.items():
            if palabra_clave in texto:
                self.enviar_mensaje_simple(
                    telefono=message.from_user.wa_id,
                    mensaje=respuesta
                )
                break

    def enviar_mensaje_simple(self, telefono: str, mensaje: str) -> bool:
        """Envía un mensaje de texto simple"""
        if not self.wa_client:
            # Fallback: encolar para envío posterior
            return self._enqueue_offline_op('enviar_mensaje_simple', {'telefono': telefono, 'mensaje': mensaje})
        
        try:
            # Verificar allowlist
            if not self._numero_permitido(telefono):
                logging.warning(f"Número no permitido por allowlist: {telefono}")
                return False
            # Verificar anti-spam
            if not self.message_logger.puede_enviar_mensaje(telefono):
                logging.warning(f"Mensaje bloqueado por anti-spam para {telefono}")
                return False
            
            # Enviar mensaje con política non-blocking/timeout
            ok, response = self._send_message(to=telefono, text=mensaje)
            if not ok:
                # Si falló rápido o por timeout, no bloquear; registrar y continuar
                logging.warning(f"WhatsApp mensaje simple no confirmado inmediatamente para {telefono}: {response}")
                # No encolamos para evitar duplicados si el hilo continúa
                return True
            
            # Registrar mensaje enviado
            self.message_logger.registrar_mensaje_enviado(
                telefono=telefono,
                mensaje=mensaje,
                tipo_mensaje="welcome",
                message_id=response.id if hasattr(response, 'id') else None
            )
            
            logging.info(f"Mensaje enviado exitosamente a {telefono}")
            return True
            
        except Exception as e:
            logging.error(f"Error al enviar mensaje a {telefono}: {e}")
            return self._enqueue_offline_op('enviar_mensaje_simple', {'telefono': telefono, 'mensaje': mensaje})
    
    def enviar_mensaje_con_plantilla(self, telefono: str, plantilla_id: int, variables: Dict[str, Any] = None) -> bool:
        """Envía un mensaje usando una plantilla con variables dinámicas"""
        try:
            # Obtener y procesar plantilla
            plantilla = self.db.obtener_plantilla_whatsapp(plantilla_id)
            if not plantilla:
                logging.error(f"Plantilla {plantilla_id} no encontrada")
                return False
            
            mensaje_procesado = self.template_processor.procesar_plantilla(
                plantilla['contenido'],
                variables or {}
            )
            
            # Enviar mensaje
            return self.enviar_mensaje_simple(telefono, mensaje_procesado)
            
        except Exception as e:
            logging.error(f"Error al enviar mensaje con plantilla: {e}")
            return self._enqueue_offline_op('enviar_mensaje_con_plantilla', {
                'telefono': telefono,
                'plantilla_id': plantilla_id,
                'variables': variables or {}
            })
    
    def enviar_confirmacion_pago(self, usuario_id: int, pago_info: Dict[str, Any], force_send: bool = False) -> bool:
        """Envía confirmación de pago recibido usando plantilla real del SISTEMA WHATSAPP.txt"""
        try:
            if not self.wa_client:
                # Registrar fallo por cliente no inicializado y encolar operación completa
                try:
                    u = self.db.obtener_usuario(usuario_id)
                    tel = getattr(u, 'telefono', None) if u else None
                    if tel:
                        self.message_logger.registrar_mensaje_fallido(
                            telefono=tel,
                            mensaje=f"Confirmación de pago - {getattr(u,'nombre', '')}",
                            error="Cliente WhatsApp no inicializado",
                            tipo_mensaje="payment"
                        )
                except Exception:
                    pass
                return self._enqueue_offline_op('enviar_confirmacion_pago', {'usuario_id': usuario_id, 'pago_info': pago_info})
            usuario = self.db.obtener_usuario(usuario_id)
            if not usuario or not usuario.telefono:
                logging.warning(f"Usuario {usuario_id} sin teléfono registrado")
                try:
                    self.message_logger.registrar_mensaje_fallido(
                        telefono=str(getattr(usuario, 'telefono', '') or ''),
                        mensaje=f"Confirmación de pago - {getattr(usuario,'nombre','')}",
                        error="Usuario sin teléfono",
                        tipo_mensaje="payment"
                    )
                except Exception:
                    pass
                return False
            # Verificar allowlist
            if not self._numero_permitido(usuario.telefono):
                logging.warning(f"Número no permitido por allowlist: {usuario.telefono}")
                try:
                    self.message_logger.registrar_mensaje_fallido(
                        telefono=usuario.telefono,
                        mensaje=f"Confirmación de pago - {usuario.nombre}",
                        error="Número fuera de allowlist",
                        tipo_mensaje="payment"
                    )
                except Exception:
                    pass
                return False
            
            # Verificar anti-spam antes de enviar (permitir forzar en pruebas)
            if not force_send and not self.message_logger.puede_enviar_mensaje(usuario.telefono):
                logging.warning(f"Mensaje bloqueado por anti-spam para {usuario.telefono}")
                try:
                    self.message_logger.registrar_mensaje_fallido(
                        telefono=usuario.telefono,
                        mensaje=f"Confirmación de pago - {usuario.nombre}",
                        error="Bloqueado por anti-spam",
                        tipo_mensaje="payment"
                    )
                except Exception:
                    pass
                return False
            
            # Plantilla 1 del archivo SISTEMA WHATSAPP.txt - EXACTA
            # Nombre: aviso_de_confirmacion_de_pago_de_cuota_gimnasio_para_usuario_especifico_en_sistema_de_management_de_gimnasios_profesional
            template_name = "aviso_de_confirmacion_de_pago_de_cuota_gimnasio_para_usuario_especifico_en_sistema_de_management_de_gimnasios_profesional"
            
            # Usar PyWa para enviar plantilla con variables con política non-blocking/timeout
            try:
                ok, response = self._send_template(
                    to=usuario.telefono,
                    name=template_name,
                    language=TemplateLanguage.SPANISH_ARG,
                    params=[
                        BodyText.params(
                            usuario.nombre,
                            f"{pago_info.get('monto', 0):,.0f}",
                            pago_info.get('fecha', datetime.now().strftime('%d/%m/%Y'))
                        )
                    ]
                )
                if not ok:
                    logging.warning(f"WhatsApp plantilla de confirmación no confirmada inmediatamente para {usuario.telefono}: {response}")
                    return True
                
                # Registrar mensaje enviado
                self.message_logger.registrar_mensaje_enviado(
                    telefono=usuario.telefono,
                    mensaje=f"Confirmación de pago - {usuario.nombre}",
                    tipo_mensaje="payment",
                    message_id=getattr(response, 'id', None)
                )
                
                logging.info(f"Confirmación de pago enviada exitosamente a {usuario.telefono}")
                return True
                
            except Exception as template_error:
                logging.error(f"Error al enviar plantilla: {template_error}")
                try:
                    self.message_logger.registrar_mensaje_fallido(
                        telefono=usuario.telefono,
                        mensaje=f"Confirmación de pago - {usuario.nombre}",
                        error=str(template_error),
                        tipo_mensaje="payment"
                    )
                except Exception:
                    pass
                return self._enqueue_offline_op('enviar_confirmacion_pago', {'usuario_id': usuario_id, 'pago_info': pago_info})
            
        except Exception as e:
            logging.error(f"Error al enviar confirmación de pago: {e}")
            try:
                u = self.db.obtener_usuario(usuario_id)
                if u and u.telefono:
                    self.message_logger.registrar_mensaje_fallido(
                        telefono=u.telefono,
                        mensaje=f"Confirmación de pago - {getattr(u,'nombre','')}",
                        error=str(e),
                        tipo_mensaje="payment"
                    )
            except Exception:
                pass
            return self._enqueue_offline_op('enviar_confirmacion_pago', {'usuario_id': usuario_id, 'pago_info': pago_info})
    
    def enviar_recordatorio_cuota_vencida(self, usuario_id: int) -> bool:
        """Envía recordatorio de cuota vencida usando plantilla real del SISTEMA WHATSAPP.txt"""
        try:
            usuario = self.db.obtener_usuario(usuario_id)
            if not usuario or not usuario.telefono:
                logging.warning(f"Usuario {usuario_id} sin teléfono registrado")
                return False
            # Verificar allowlist
            if not self._numero_permitido(usuario.telefono):
                logging.warning(f"Número no permitido por allowlist: {usuario.telefono}")
                return False
            
            # Verificar anti-spam antes de enviar
            if not self.message_logger.puede_enviar_mensaje(usuario.telefono):
                logging.warning(f"Mensaje bloqueado por anti-spam para {usuario.telefono}")
                return False
            
            # Obtener información de cuota vencida
            from datetime import datetime
            now = datetime.now()
            pago_actual = self.db.obtener_pago_actual(usuario_id, now.month, now.year)
            if not pago_actual:
                logging.warning(f"No se encontró información de pago para usuario {usuario_id}")
                return False
            
            # Plantilla 2 del archivo SISTEMA WHATSAPP.txt - EXACTA
            # Nombre: aviso_de_vencimiento_de_cuota_gimnasio_para_usuario_especifico_en_sistema_de_management_de_gimnasios_profesional
            template_name = "aviso_de_vencimiento_de_cuota_gimnasio_para_usuario_especifico_en_sistema_de_management_de_gimnasios_profesional"
            
            # Si no hay cliente, usar fallback HTTP directo
            if not self.wa_client:
                ok, resp = self._send_template_http_basic(
                    to=usuario.telefono,
                    name=template_name,
                    language=TemplateLanguage.SPANISH_ARG,
                    body_params=[
                        usuario.nombre,
                        pago_actual.get('fecha_vencimiento', 'No disponible'),
                        f"{(pago_actual.get('monto', 0) or 0):,.0f}"
                    ]
                )
                if ok:
                    try:
                        self.message_logger.registrar_mensaje_enviado(
                            telefono=usuario.telefono,
                            mensaje=f"Recordatorio cuota vencida - {usuario.nombre}",
                            tipo_mensaje="overdue",
                            message_id=(resp or {}).get("id")
                        )
                    except Exception:
                        pass
                    logging.info(f"Recordatorio de cuota vencida enviado (HTTP) a {usuario.telefono}")
                    return True
                else:
                    try:
                        self.message_logger.registrar_mensaje_fallido(
                            telefono=usuario.telefono,
                            mensaje=f"Recordatorio cuota vencida - {usuario.nombre}",
                            error=str((resp or {}).get('error', 'fallback_http_failed')),
                            tipo_mensaje="overdue"
                        )
                    except Exception:
                        pass
                    return self._enqueue_offline_op('enviar_recordatorio_cuota_vencida', {'usuario_id': usuario_id})
            
            # Usar PyWa para enviar plantilla con variables con política non-blocking/timeout
            try:
                ok, response = self._send_template(
                    to=usuario.telefono,
                    name=template_name,
                    language=TemplateLanguage.SPANISH_ARG,
                    params=[
                        BodyText.params(
                            usuario.nombre,
                            pago_actual.get('fecha_vencimiento', 'No disponible'),
                            f"{pago_actual.get('monto', 0) or 0:,.0f}"
                        )
                    ]
                )
                if not ok:
                    logging.warning(f"WhatsApp plantilla de recordatorio no confirmada inmediatamente para {usuario.telefono}: {response}")
                    return True
                
                # Registrar mensaje enviado
                self.message_logger.registrar_mensaje_enviado(
                    telefono=usuario.telefono,
                    mensaje=f"Recordatorio cuota vencida - {usuario.nombre}",
                    tipo_mensaje="overdue"
                )
                
                logging.info(f"Recordatorio de cuota vencida enviado exitosamente a {usuario.telefono}")
                return True
                
            except Exception as template_error:
                logging.error(f"Error al enviar plantilla: {template_error}")
                # Fallback HTTP si el cliente falla de inmediato
                ok, resp = self._send_template_http_basic(
                    to=usuario.telefono,
                    name=template_name,
                    language=TemplateLanguage.SPANISH_ARG,
                    body_params=[
                        usuario.nombre,
                        pago_actual.get('fecha_vencimiento', 'No disponible'),
                        f"{(pago_actual.get('monto', 0) or 0):,.0f}"
                    ]
                )
                if ok:
                    try:
                        self.message_logger.registrar_mensaje_enviado(
                            telefono=usuario.telefono,
                            mensaje=f"Recordatorio cuota vencida - {usuario.nombre}",
                            tipo_mensaje="overdue",
                            message_id=(resp or {}).get("id")
                        )
                    except Exception:
                        pass
                    logging.info(f"Recordatorio de cuota vencida enviado (HTTP) a {usuario.telefono}")
                    return True
                else:
                    try:
                        self.message_logger.registrar_mensaje_fallido(
                            telefono=usuario.telefono,
                            mensaje=f"Recordatorio cuota vencida - {usuario.nombre}",
                            error=str((resp or {}).get('error', 'fallback_http_failed')),
                            tipo_mensaje="overdue"
                        )
                    except Exception:
                        pass
                    return self._enqueue_offline_op('enviar_recordatorio_cuota_vencida', {'usuario_id': usuario_id})
            
        except Exception as e:
            logging.error(f"Error al enviar recordatorio de cuota vencida: {e}")
            return self._enqueue_offline_op('enviar_recordatorio_cuota_vencida', {'usuario_id': usuario_id})
    
    def enviar_mensaje_bienvenida(self, usuario_id: int) -> bool:
        """Envía mensaje de bienvenida a nuevo usuario usando plantilla real del SISTEMA WHATSAPP.txt"""
        try:
            usuario = self.db.obtener_usuario(usuario_id)
            if not usuario or not usuario.telefono:
                logging.warning(f"Usuario {usuario_id} sin teléfono registrado")
                return False
            # Verificar allowlist
            if not self._numero_permitido(usuario.telefono):
                logging.warning(f"Número no permitido por allowlist: {usuario.telefono}")
                return False
            
            # Verificar anti-spam antes de enviar
            if not self.message_logger.puede_enviar_mensaje(usuario.telefono):
                logging.warning(f"Mensaje bloqueado por anti-spam para {usuario.telefono}")
                return False
            
            # Obtener nombre del gimnasio desde configuración
            gym_data = self.template_processor.obtener_datos_gimnasio()
            gym_name = gym_data.get('nombre_gimnasio', 'nuestro gimnasio')
            
            # Plantilla 3 del archivo SISTEMA WHATSAPP.txt - ACTUALIZADA
            # Nombre: aviso_de_confirmacion_de_ingreso_a_gimnasio_para_usuario_especifico_en_sistema_de_management_de_gimnasios_profesional
            template_name = "aviso_de_confirmacion_de_ingreso_a_gimnasio_para_usuario_especifico_en_sistema_de_management_de_gimnasios_profesional"
            
            # Si no hay cliente, intentar fallback HTTP
            if not self.wa_client:
                ok, resp = self._send_template_http_basic(
                    to=usuario.telefono,
                    name=template_name,
                    language=TemplateLanguage.SPANISH_ARG,
                    body_params=[usuario.nombre, gym_name]
                )
                if ok:
                    try:
                        self.message_logger.registrar_mensaje_enviado(
                            telefono=usuario.telefono,
                            mensaje=f"Mensaje de bienvenida - {usuario.nombre}",
                            tipo_mensaje="welcome",
                            message_id=(resp or {}).get("id")
                        )
                    except Exception:
                        pass
                    logging.info(f"Mensaje de bienvenida enviado (HTTP) a {usuario.telefono}")
                    return True
                else:
                    try:
                        self.message_logger.registrar_mensaje_fallido(
                            telefono=usuario.telefono,
                            mensaje=f"Mensaje de bienvenida - {usuario.nombre}",
                            error=str((resp or {}).get('error', 'fallback_http_failed')),
                            tipo_mensaje="welcome"
                        )
                    except Exception:
                        pass
                    return self._enqueue_offline_op('enviar_mensaje_bienvenida', {'usuario_id': usuario_id})

            # Usar PyWa para enviar plantilla
            try:
                ok, response = self._send_template(
                    to=usuario.telefono,
                    name=template_name,
                    language=TemplateLanguage.SPANISH_ARG,
                    params=[
                        BodyText.params(
                            usuario.nombre,
                            gym_name
                        )
                    ]
                )
                if not ok:
                    logging.warning(f"WhatsApp plantilla bienvenida no confirmada inmediatamente para {usuario.telefono}: {response}")
                    return True
                
                # Registrar mensaje enviado
                self.message_logger.registrar_mensaje_enviado(
                    telefono=usuario.telefono,
                    mensaje=f"Mensaje de bienvenida - {usuario.nombre}",
                    tipo_mensaje="welcome"
                )
                
                logging.info(f"Mensaje de bienvenida enviado exitosamente a {usuario.telefono}")
                return True
                
            except Exception as template_error:
                logging.error(f"Error al enviar plantilla: {template_error}")
                ok, resp = self._send_template_http_basic(
                    to=usuario.telefono,
                    name=template_name,
                    language=TemplateLanguage.SPANISH_ARG,
                    body_params=[usuario.nombre, gym_name]
                )
                if ok:
                    try:
                        self.message_logger.registrar_mensaje_enviado(
                            telefono=usuario.telefono,
                            mensaje=f"Mensaje de bienvenida - {usuario.nombre}",
                            tipo_mensaje="welcome",
                            message_id=(resp or {}).get("id")
                        )
                    except Exception:
                        pass
                    logging.info(f"Mensaje de bienvenida enviado (HTTP) a {usuario.telefono}")
                    return True
                else:
                    try:
                        self.message_logger.registrar_mensaje_fallido(
                            telefono=usuario.telefono,
                            mensaje=f"Mensaje de bienvenida - {usuario.nombre}",
                            error=str((resp or {}).get('error', 'fallback_http_failed')),
                            tipo_mensaje="welcome"
                        )
                    except Exception:
                        pass
                    return self._enqueue_offline_op('enviar_mensaje_bienvenida', {'usuario_id': usuario_id})
            
        except Exception as e:
            logging.error(f"Error al enviar mensaje de bienvenida: {e}")
            return self._enqueue_offline_op('enviar_mensaje_bienvenida', {'usuario_id': usuario_id})

    def enviar_notificacion_desactivacion(self, usuario_id: int, motivo: str = "Falta de pago", fecha_desactivacion: Optional[str] = None, force_send: bool = False) -> bool:
        """Envía notificación de desactivación de usuario por falta de pago"""
        try:
            if not self.wa_client:
                try:
                    u = self.db.obtener_usuario(usuario_id)
                    tel = getattr(u, 'telefono', None) if u else None
                    if tel:
                        self.message_logger.registrar_mensaje_fallido(
                            telefono=tel,
                            mensaje=f"Desactivación de usuario - {getattr(u,'nombre','')}",
                            error="Cliente WhatsApp no inicializado",
                            tipo_mensaje="deactivation"
                        )
                except Exception:
                    pass
                return self._enqueue_offline_op('enviar_notificacion_desactivacion', {
                    'usuario_id': usuario_id,
                    'motivo': motivo,
                    'fecha_desactivacion': fecha_desactivacion
                })
            usuario = self.db.obtener_usuario(usuario_id)
            if not usuario or not usuario.telefono:
                logging.warning(f"Usuario {usuario_id} sin teléfono registrado")
                try:
                    self.message_logger.registrar_mensaje_fallido(
                        telefono=str(getattr(usuario, 'telefono', '') or ''),
                        mensaje=f"Desactivación de usuario - {getattr(usuario,'nombre','')}",
                        error="Usuario sin teléfono",
                        tipo_mensaje="deactivation"
                    )
                except Exception:
                    pass
                return False

            # Verificar anti-spam antes de enviar (permitir forzar en pruebas)
            if not force_send and not self.message_logger.puede_enviar_mensaje(usuario.telefono):
                logging.warning(f"Mensaje bloqueado por anti-spam para {usuario.telefono}")
                try:
                    self.message_logger.registrar_mensaje_fallido(
                        telefono=usuario.telefono,
                        mensaje=f"Desactivación de usuario - {usuario.nombre}",
                        error="Bloqueado por anti-spam",
                        tipo_mensaje="deactivation"
                    )
                except Exception:
                    pass
                return False

            template_name = "aviso_de_desactivacion_por_falta_de_pago_para_usuario_especifico_en_sistema_de_management_de_gimnasios_profesional"
            fecha = fecha_desactivacion or datetime.now().strftime('%d/%m/%Y')

            try:
                response = self.wa_client.send_template(
                    to=usuario.telefono,
                    name=template_name,
                    language=TemplateLanguage.SPANISH_ARG,
                    params=[
                        BodyText.params(
                            usuario.nombre,  # {{1}}
                            fecha,           # {{2}}
                            motivo           # {{3}}
                        )
                    ]
                )

                self.message_logger.registrar_mensaje_enviado(
                    telefono=usuario.telefono,
                    mensaje=f"Desactivación de usuario - {usuario.nombre}",
                    tipo_mensaje="deactivation",
                    message_id=getattr(response, 'id', None)
                )

                logging.info(f"Notificación de desactivación enviada exitosamente a {usuario.telefono}")
                return True

            except Exception as template_error:
                logging.error(f"Error al enviar plantilla de desactivación: {template_error}")
                try:
                    self.message_logger.registrar_mensaje_fallido(
                        telefono=usuario.telefono,
                        mensaje=f"Desactivación de usuario - {usuario.nombre}",
                        error=str(template_error),
                        tipo_mensaje="deactivation"
                    )
                except Exception:
                    pass
                return self._enqueue_offline_op('enviar_notificacion_desactivacion', {
                    'usuario_id': usuario_id,
                    'motivo': motivo,
                    'fecha_desactivacion': fecha_desactivacion
                })

        except Exception as e:
            logging.error(f"Error al enviar notificación de desactivación: {e}")
            try:
                u = self.db.obtener_usuario(usuario_id)
                if u and u.telefono:
                    self.message_logger.registrar_mensaje_fallido(
                        telefono=u.telefono,
                        mensaje=f"Desactivación de usuario - {getattr(u,'nombre','')}",
                        error=str(e),
                        tipo_mensaje="deactivation"
                    )
            except Exception:
                pass
            return self._enqueue_offline_op('enviar_notificacion_desactivacion', {
                'usuario_id': usuario_id,
                'motivo': motivo,
                'fecha_desactivacion': fecha_desactivacion
            })

    def enviar_recordatorio_horario_clase(self, usuario_id: int, clase_info: Dict[str, Any], force_send: bool = False) -> bool:
        """Envía recordatorio de horario de clase (tipo, fecha y hora)"""
        try:
            if not self.wa_client:
                return self._enqueue_offline_op('enviar_recordatorio_horario_clase', {
                    'usuario_id': usuario_id,
                    'clase_info': clase_info
                })
            usuario = self.db.obtener_usuario(usuario_id)
            if not usuario or not usuario.telefono:
                logging.warning(f"Usuario {usuario_id} sin teléfono registrado")
                return False

            # Allowlist (permitir forzar en pruebas)
            if not force_send and not self._numero_permitido(usuario.telefono):
                logging.warning(f"Número no permitido por allowlist: {usuario.telefono}")
                return False

            # Verificar anti-spam (permitir forzar en pruebas)
            if not force_send and not self.message_logger.puede_enviar_mensaje(usuario.telefono):
                logging.warning(f"Mensaje bloqueado por anti-spam para {usuario.telefono}")
                return False

            template_name = "aviso_de_recordatorio_de_horario_de_clase_para_usuario_especifico_en_sistema_de_management_de_gimnasios_profesional"

            # Sanitización de parámetros para evitar MissingRequiredParameter
            def _safe_text(val, default):
                try:
                    s = (val if val is not None else '').strip() if isinstance(val, str) else str(val or '').strip()
                except Exception:
                    s = ''
                return s if s else default

            tipo_clase = _safe_text(clase_info.get('tipo_clase') or clase_info.get('clase_nombre'), 'Clase')
            fecha = _safe_text(clase_info.get('fecha'), 'Por confirmar')
            hora = _safe_text(clase_info.get('hora'), 'Por confirmar')

            try:
                response = self.wa_client.send_template(
                    to=usuario.telefono,
                    name=template_name,
                    language=TemplateLanguage.SPANISH_ARG,
                    params=[
                        BodyText.params(
                            _safe_text(getattr(usuario, 'nombre', None), 'Alumno'),  # {{1}}
                            tipo_clase,      # {{2}}
                            fecha,           # {{3}}
                            hora             # {{4}}
                        )
                    ]
                )

                self.message_logger.registrar_mensaje_enviado(
                    telefono=usuario.telefono,
                    mensaje=f"Recordatorio de clase - {usuario.nombre}",
                    tipo_mensaje="class_reminder",
                    message_id=getattr(response, 'id', None)
                )

                logging.info(f"Recordatorio de clase enviado exitosamente a {usuario.telefono}")
                return True

            except Exception as template_error:
                logging.error(f"Error al enviar plantilla de recordatorio de clase: {template_error}")
                return self._enqueue_offline_op('enviar_recordatorio_horario_clase', {
                    'usuario_id': usuario_id,
                    'clase_info': clase_info
                })

        except Exception as e:
            logging.error(f"Error al enviar recordatorio de clase: {e}")
            return self._enqueue_offline_op('enviar_recordatorio_horario_clase', {
                'usuario_id': usuario_id,
                'clase_info': clase_info
            })

    def enviar_promocion_lista_espera(self, usuario_id: int, clase_info: Dict[str, Any], force_send: bool = False) -> bool:
        """Aviso a primer persona en lista de espera: se liberó un cupo"""
        try:
            if not self.wa_client:
                return self._enqueue_offline_op('enviar_promocion_lista_espera', {
                    'usuario_id': usuario_id,
                    'clase_info': clase_info
                })
            usuario = self.db.obtener_usuario(usuario_id)
            if not usuario or not usuario.telefono:
                logging.warning(f"Usuario {usuario_id} sin teléfono registrado")
                return False

            # Allowlist (permitir forzar en pruebas)
            if not force_send and not self._numero_permitido(usuario.telefono):
                logging.warning(f"Número no permitido por allowlist: {usuario.telefono}")
                return False

            # Verificar anti-spam (permitir forzar en pruebas)
            if not force_send and not self.message_logger.puede_enviar_mensaje(usuario.telefono):
                logging.warning(f"Mensaje bloqueado por anti-spam para {usuario.telefono}")
                return False

            # Plantilla para avisar que hay cupo disponible al primero de la lista de espera
            template_name = "aviso_de_promocion_de_lista_de_espera_para_clase_para_usuario_especifico_en_sistema_de_management_de_gimnasios_profesional"

            # Sanitización de parámetros para evitar MissingRequiredParameter
            def _safe_text(val, default):
                try:
                    s = (val if val is not None else '').strip() if isinstance(val, str) else str(val or '').strip()
                except Exception:
                    s = ''
                return s if s else default

            tipo_clase = _safe_text(clase_info.get('tipo_clase') or clase_info.get('clase_nombre'), 'Clase')
            fecha = _safe_text(clase_info.get('fecha'), 'Por confirmar')
            hora = _safe_text(clase_info.get('hora'), 'Por confirmar')

            try:
                response = self.wa_client.send_template(
                    to=usuario.telefono,
                    name=template_name,
                    language=TemplateLanguage.SPANISH_ARG,
                    params=[
                        BodyText.params(
                            _safe_text(getattr(usuario, 'nombre', None), 'Alumno'),  # {{1}}
                            tipo_clase,      # {{2}}
                            fecha,           # {{3}}
                            hora             # {{4}}
                        )
                    ]
                )

                self.message_logger.registrar_mensaje_enviado(
                    telefono=usuario.telefono,
                    mensaje=f"Cupo disponible (lista de espera) - {usuario.nombre}",
                    tipo_mensaje="waitlist",
                    message_id=getattr(response, 'id', None)
                )

                logging.info(f"Aviso de cupo disponible enviado exitosamente a {usuario.telefono}")
                return True

            except Exception as template_error:
                logging.error(f"Error al enviar plantilla de cupo disponible: {template_error}")
                return self._enqueue_offline_op('enviar_promocion_lista_espera', {
                    'usuario_id': usuario_id,
                    'clase_info': clase_info
                })

        except Exception as e:
            logging.error(f"Error al enviar aviso de cupo disponible: {e}")
            return self._enqueue_offline_op('enviar_promocion_lista_espera', {
                'usuario_id': usuario_id,
                'clase_info': clase_info
            })

    def enviar_promocion_a_lista_principal(self, usuario_id: int, clase_info: Dict[str, Any], force_send: bool = False) -> bool:
        """Aviso cuando un usuario pasa de lista de espera a lista principal"""
        try:
            if not self.wa_client:
                # Reutilizamos el mismo op offline para no romper el manejador existente
                return self._enqueue_offline_op('enviar_promocion_lista_espera', {
                    'usuario_id': usuario_id,
                    'clase_info': clase_info
                })
            usuario = self.db.obtener_usuario(usuario_id)
            if not usuario or not usuario.telefono:
                logging.warning(f"Usuario {usuario_id} sin teléfono registrado")
                return False

            # Allowlist (permitir forzar en pruebas)
            if not force_send and not self._numero_permitido(usuario.telefono):
                logging.warning(f"Número no permitido por allowlist: {usuario.telefono}")
                return False

            # Verificar anti-spam (permitir forzar en pruebas)
            if not force_send and not self.message_logger.puede_enviar_mensaje(usuario.telefono):
                logging.warning(f"Mensaje bloqueado por anti-spam para {usuario.telefono}")
                return False

            # Plantilla para avisar promoción a lista principal
            template_name = "aviso_de_promocion_a_lista_principal_para_clase_para_usuario_especifico_en_sistema_de_management_de_gimnasios_profesional"

            # Sanitización de parámetros
            def _safe_text(val, default):
                try:
                    s = (val if val is not None else '').strip() if isinstance(val, str) else str(val or '').strip()
                except Exception:
                    s = ''
                return s if s else default

            tipo_clase = _safe_text(clase_info.get('tipo_clase') or clase_info.get('clase_nombre'), 'Clase')
            fecha = _safe_text(clase_info.get('fecha'), 'Por confirmar')
            hora = _safe_text(clase_info.get('hora'), 'Por confirmar')

            try:
                response = self.wa_client.send_template(
                    to=usuario.telefono,
                    name=template_name,
                    language=TemplateLanguage.SPANISH_ARG,
                    params=[
                        BodyText.params(
                            _safe_text(getattr(usuario, 'nombre', None), 'Alumno'),  # {{1}}
                            tipo_clase,      # {{2}}
                            fecha,           # {{3}}
                            hora             # {{4}}
                        )
                    ]
                )

                self.message_logger.registrar_mensaje_enviado(
                    telefono=usuario.telefono,
                    mensaje=f"Promoción a lista principal - {usuario.nombre}",
                    tipo_mensaje="waitlist",
                    message_id=getattr(response, 'id', None)
                )

                logging.info(f"Aviso de promoción a lista principal enviado exitosamente a {usuario.telefono}")
                return True

            except Exception as template_error:
                logging.error(f"Error al enviar plantilla de promoción a lista principal: {template_error}")
                return self._enqueue_offline_op('enviar_promocion_lista_espera', {
                    'usuario_id': usuario_id,
                    'clase_info': clase_info
                })

        except Exception as e:
            logging.error(f"Error al enviar aviso de promoción a lista principal: {e}")
            return self._enqueue_offline_op('enviar_promocion_lista_espera', {
                'usuario_id': usuario_id,
                'clase_info': clase_info
            })
    
    def send_overdue_payment_notification(self, user_data: dict, to: str | None = None) -> bool:
        """
        Envía notificación de cuota vencida según documentación de arquitectura
        
        Args:
            user_data: {
                'phone': str,
                'name': str, 
                'due_date': str,
                'amount': float
            }
            to: str opcional, número explícito de destino; si no se proporciona, se usa user_data['phone']
        
        Returns:
            bool: True si el mensaje se envió correctamente
        """
        try:
            if not self.wa_client:
                return self._enqueue_offline_op('send_overdue_payment_notification', {
                    'user_data': user_data,
                    'to': user_data.get('phone')
                })
            
            # Verificar anti-spam
            destino = to or user_data.get('phone')
            if self.message_logger.verificar_mensaje_enviado_reciente(
                destino, "overdue", 24
            ):
                logging.info(f"Mensaje anti-spam bloqueado para usuario {user_data.get('user_id')}")
                return False
            
            template_name = "aviso_de_vencimiento_de_cuota_gimnasio_para_usuario_especifico_en_sistema_de_management_de_gimnasios_profesional"
            
            # Usar PyWa para enviar plantilla
            response = self.wa_client.send_template(
                to=destino,
                name=template_name,
                language=TemplateLanguage.SPANISH_ARG,
                params=[
                    BodyText.params(
                        user_data['name'],  # {{1}}
                        user_data['due_date'],  # {{2}}
                        f"{user_data['amount']:,.0f}"  # {{3}}
                    )
                ]
            )
            
            # Registrar mensaje enviado
            self.message_logger.registrar_mensaje_enviado(
                telefono=destino,
                mensaje=f"Recordatorio cuota vencida - {user_data.get('name', '')}",
                tipo_mensaje="overdue",
                message_id=getattr(response, 'id', None)
            )
            
            logging.info(f"Notificación de cuota vencida enviada a {destino}")
            return True
            
        except Exception as e:
            logging.error(f"Error al enviar notificación de cuota vencida: {e}")
            return self._enqueue_offline_op('send_overdue_payment_notification', {
                'user_data': user_data,
                'to': user_data.get('phone')
            })

    def send_payment_confirmation(self, payment_data: dict) -> bool:
        """
        Envía confirmación de pago realizado según documentación de arquitectura
        
        Args:
            payment_data: {
                'phone': str,
                'name': str,
                'amount': float,
                'date': str
            }
        
        Returns:
            bool: True si el mensaje se envió correctamente
        """
        try:
            template_name = "aviso_de_confirmacion_de_pago_de_cuota_gimnasio_para_usuario_especifico_en_sistema_de_management_de_gimnasios_profesional"
            # Si no hay cliente, intentar fallback HTTP
            if not self.wa_client:
                ok, resp = self._send_template_http_basic(
                    to=payment_data['phone'],
                    name=template_name,
                    language=TemplateLanguage.SPANISH_ARG,
                    body_params=[
                        payment_data['name'],
                        f"{payment_data['amount']:,.0f}",
                        payment_data['date']
                    ]
                )
                if ok:
                    try:
                        self.message_logger.registrar_mensaje_enviado(
                            telefono=payment_data['phone'],
                            mensaje=f"Confirmación de pago - {payment_data.get('name', '')}",
                            tipo_mensaje="payment",
                            message_id=(resp or {}).get("id")
                        )
                    except Exception:
                        pass
                    logging.info(f"Confirmación de pago enviada (HTTP) a {payment_data['phone']}")
                    return True
                else:
                    try:
                        self.message_logger.registrar_mensaje_fallido(
                            telefono=str(payment_data.get('phone') or ''),
                            mensaje=f"Confirmación de pago - {payment_data.get('name', '')}",
                            error=str((resp or {}).get('error', 'fallback_http_failed')),
                            tipo_mensaje="payment"
                        )
                    except Exception:
                        pass
                    return self._enqueue_offline_op('send_payment_confirmation', {'payment_data': payment_data})

            # Usar PyWa para enviar plantilla
            ok, response = self._send_template(
                to=payment_data['phone'],
                name=template_name,
                language=TemplateLanguage.SPANISH_ARG,
                params=[
                    BodyText.params(
                        payment_data['name'],
                        f"{payment_data['amount']:,.0f}",
                        payment_data['date']
                    )
                ]
            )
            if not ok:
                logging.warning(f"WhatsApp plantilla confirmación no confirmada inmediatamente para {payment_data['phone']}: {response}")
                return True
            
            # Registrar mensaje enviado
            self.message_logger.registrar_mensaje_enviado(
                telefono=payment_data['phone'],
                mensaje=f"Confirmación de pago - {payment_data.get('name', '')}",
                tipo_mensaje="payment",
            )
            
            logging.info(f"Confirmación de pago enviada a {payment_data['phone']}")
            return True
            
        except Exception as e:
            logging.error(f"Error al enviar confirmación de pago: {e}")
            ok, resp = self._send_template_http_basic(
                to=payment_data.get('phone', ''),
                name="aviso_de_confirmacion_de_pago_de_cuota_gimnasio_para_usuario_especifico_en_sistema_de_management_de_gimnasios_profesional",
                language=TemplateLanguage.SPANISH_ARG,
                body_params=[
                    payment_data.get('name', ''),
                    f"{payment_data.get('amount', 0):,.0f}",
                    payment_data.get('date', '')
                ]
            )
            if ok:
                try:
                    self.message_logger.registrar_mensaje_enviado(
                        telefono=payment_data.get('phone', ''),
                        mensaje=f"Confirmación de pago - {payment_data.get('name', '')}",
                        tipo_mensaje="payment",
                        message_id=(resp or {}).get("id")
                    )
                except Exception:
                    pass
                logging.info(f"Confirmación de pago enviada (HTTP) a {payment_data.get('phone', '')}")
                return True
            else:
                try:
                    self.message_logger.registrar_mensaje_fallido(
                        telefono=str(payment_data.get('phone') or ''),
                        mensaje=f"Confirmación de pago - {payment_data.get('name', '')}",
                        error=str((resp or {}).get('error', 'fallback_http_failed')),
                        tipo_mensaje="payment"
                    )
                except Exception:
                    pass
                return self._enqueue_offline_op('send_payment_confirmation', {'payment_data': payment_data})

    def send_welcome_message(self, user_data: dict) -> bool:
        """
        Envía mensaje de bienvenida a usuario nuevo según documentación de arquitectura
        
        Args:
            user_data: {
                'phone': str,
                'name': str,
                'gym_name': str
            }
        
        Returns:
            bool: True si el mensaje se envió correctamente
        """
        try:
            template_name = "mensaje_de_bienvenida_a_gimnasio_para_usuario_especifico_en_sistema_de_management_de_gimnasios_profesional"
            header_img = "https://scontent.whatsapp.net/v/t61.29466-34/534423186_1473851737199264_5735585923517038205_n.jpg?ccb=1-7&_nc_sid=8b1bef&_nc_eui2=AeFoE1d4rKfFrSN8BE7_b3tf3Y0fQUMBEBfdjR9BQwEQF8Ax0e3gytRS7qWnLKIi5oUH-QVMX592JK57XYymNeix&_nc_ohc=chf40SP38C8Q7kNvwEFl7nT&_nc_oc=AdmrcU6XgW2jmC-YWO7D1UiHeeCxuhGlR6EElDkYrPDAFG43PJ7eU0L02xt9QXDDItA&_nc_zt=3&_nc_ht=scontent.whatsapp.net&edm=AH51TzQEAAAA&_nc_gid=RGRikAJcMRz3oD800NMXOQ&oh=01_Q5Aa2gEnN1SVJBv_Df-egpMeF4jG_FmxGtFRkXy0zEZOkwtGow&oe=68EFCA3A"

            # Si no hay cliente, intentar fallback HTTP
            if not self.wa_client:
                ok, resp = self._send_template_http_basic(
                    to=user_data['phone'],
                    name=template_name,
                    language=TemplateLanguage.SPANISH_ARG,
                    body_params=[user_data['name'], user_data['gym_name']],
                    header_image_url=header_img
                )
                if ok:
                    try:
                        self.message_logger.registrar_mensaje_enviado(
                            telefono=user_data['phone'],
                            mensaje=f"Mensaje de bienvenida - {user_data.get('name', '')}",
                            tipo_mensaje="welcome",
                            message_id=(resp or {}).get("id")
                        )
                    except Exception:
                        pass
                    logging.info(f"Mensaje de bienvenida enviado (HTTP) a {user_data['phone']}")
                    return True
                else:
                    try:
                        self.message_logger.registrar_mensaje_fallido(
                            telefono=str(user_data.get('phone') or ''),
                            mensaje=f"Mensaje de bienvenida - {user_data.get('name', '')}",
                            error=str((resp or {}).get('error', 'fallback_http_failed')),
                            tipo_mensaje="welcome"
                        )
                    except Exception:
                        pass
                    return self._enqueue_offline_op('send_welcome_message', {'user_data': user_data})

            # Usar PyWa (con timeout/non-blocking control)
            ok, response = self._send_template(
                to=user_data['phone'],
                name=template_name,
                language=TemplateLanguage.SPANISH_ARG,
                params=[
                    HeaderImage.params(image=header_img),
                    BodyText.params(user_data['name'], user_data['gym_name'])
                ]
            )
            if not ok:
                logging.warning(f"WhatsApp plantilla bienvenida no confirmada inmediatamente para {user_data['phone']}: {response}")
                return True
            
            # Registrar mensaje enviado
            self.message_logger.registrar_mensaje_enviado(
                telefono=user_data['phone'],
                mensaje=f"Mensaje de bienvenida - {user_data.get('name', '')}",
                tipo_mensaje="welcome",
                message_id=getattr(response, 'id', None)
            )
            
            logging.info(f"Mensaje de bienvenida enviado a {user_data['phone']}")
            return True
            
        except Exception as e:
            logging.error(f"Error al enviar mensaje de bienvenida: {e}")
            ok, resp = self._send_template_http_basic(
                to=user_data.get('phone', ''),
                name=template_name,
                language=TemplateLanguage.SPANISH_ARG,
                body_params=[user_data.get('name', ''), user_data.get('gym_name', '')],
                header_image_url=header_img
            )
            if ok:
                try:
                    self.message_logger.registrar_mensaje_enviado(
                        telefono=user_data.get('phone', ''),
                        mensaje=f"Mensaje de bienvenida - {user_data.get('name', '')}",
                        tipo_mensaje="welcome",
                        message_id=(resp or {}).get("id")
                    )
                except Exception:
                    pass
                logging.info(f"Mensaje de bienvenida enviado (HTTP) a {user_data.get('phone', '')}")
                return True
            else:
                try:
                    self.message_logger.registrar_mensaje_fallido(
                        telefono=str(user_data.get('phone') or ''),
                        mensaje=f"Mensaje de bienvenida - {user_data.get('name', '')}",
                        error=str((resp or {}).get('error', 'fallback_http_failed')),
                        tipo_mensaje="welcome"
                    )
                except Exception:
                    pass
                return self._enqueue_offline_op('send_welcome_message', {'user_data': user_data})

    def procesar_usuarios_morosos(self) -> int:
        """[DEPRECADO] Mantenido por compatibilidad. Redirige al PaymentManager."""
        try:
            if hasattr(self, 'payment_manager') and self.payment_manager:
                logging.info("Delegando procesamiento de morosos a PaymentManager")
                return self.payment_manager.procesar_usuarios_morosos()
            logging.warning("PaymentManager no disponible para procesar morosos")
            return 0
        except Exception as e:
            logging.error(f"Error delegando a PaymentManager: {e}")
            return 0
    
    def procesar_recordatorios_proximos_vencimientos(self) -> int:
        """[DEPRECADO] Mantenido por compatibilidad. Redirige al PaymentManager."""
        try:
            if hasattr(self, 'payment_manager') and self.payment_manager:
                logging.info("Delegando recordatorios de vencimientos a PaymentManager")
                return self.payment_manager.procesar_recordatorios_proximos_vencimientos()
            logging.warning("PaymentManager no disponible para procesar recordatorios")
            return 0
        except Exception as e:
            logging.error(f"Error delegando recordatorios a PaymentManager: {e}")
            return 0
    
    def send_deactivation_message(self, user_data: dict) -> bool:
        """
        Envía mensaje de desactivación a usuario (preparado para futura implementación)
        
        Args:
            user_data: {
                'phone': str,
                'name': str,
                'reason': str,
                'gym_name': str
            }
        
        Returns:
            bool: True si el mensaje se envió correctamente
        """
        try:
            if not self.wa_client:
                logging.error("Cliente WhatsApp no inicializado")
                return False
            
            # NOTA: Esta plantilla debe ser creada en el futuro en WhatsApp Business Manager
            # template_name = "mensaje_de_desactivacion_de_usuario_por_morosidad_en_sistema_de_management_de_gimnasios_profesional"
            
            # Por ahora, usar mensaje simple hasta que se cree la plantilla oficial
            mensaje = f"Hola {user_data['name']}, lamentamos informarte que tu membresía en {user_data.get('gym_name', 'nuestro gimnasio')} ha sido suspendida por {user_data.get('reason', 'falta de pago')}. Para reactivar tu cuenta, por favor contacta con recepción."
            
            success = self.enviar_mensaje_simple(user_data['phone'], mensaje)
            
            if success:
                # Registrar mensaje enviado
                self.message_logger.registrar_mensaje_enviado(
                    user_data['phone'],
                    f"Desactivación - {user_data['name']}",
                    "welcome"  # Usar welcome temporalmente hasta crear nuevo tipo
                )
                
                logging.info(f"Mensaje de desactivación enviado a {user_data['phone']}")
            
            return success
            
        except Exception as e:
            logging.error(f"Error al enviar mensaje de desactivación: {e}")
            return False
    
    def iniciar_servidor_webhook(self):
        """Inicia el servidor webhook en segundo plano para recibir mensajes"""
        if not self.wa_client:
            logging.error("Cliente WhatsApp no inicializado")
            return

        # Si ya hay un hilo corriendo, no iniciar otro
        try:
            if self._server_thread and self._server_thread.is_alive():
                logging.info("Servidor webhook de WhatsApp ya está activo")
                self.servidor_activo = True
                return
        except Exception:
            pass

        def _run_server():
            try:
                logging.info("[WA] Hilo de servidor webhook iniciando...")
                self.wa_client.run()
                logging.info("[WA] Hilo de servidor webhook finalizado")
            except Exception as e:
                logging.error(f"Error en hilo de servidor webhook: {e}")
            finally:
                try:
                    self.servidor_activo = False
                except Exception:
                    pass

        try:
            self._server_thread = threading.Thread(target=_run_server, name="WhatsAppWebhookServer", daemon=True)
            self.servidor_activo = True
            self._server_thread.start()
            logging.info("Servidor webhook de WhatsApp iniciado en segundo plano")
        except Exception as e:
            logging.error(f"Error al iniciar servidor webhook: {e}")
            try:
                self.servidor_activo = False
            except Exception:
                pass

    def detener_servidor_webhook(self):
        """Detiene el servidor webhook"""
        if self.wa_client:
            try:
                self.wa_client.stop()
                logging.info("Solicitud de detención enviada al servidor webhook")
            except Exception as e:
                logging.error(f"Error al detener servidor webhook: {e}")
        # Intentar unir el hilo si existe
        try:
            if self._server_thread and self._server_thread.is_alive():
                self._server_thread.join(timeout=2.0)
            self.servidor_activo = False
        except Exception:
            try:
                self.servidor_activo = False
            except Exception:
                pass

    def iniciar_servidor(self) -> bool:
        """Wrapper seguro para iniciar el servidor webhook y actualizar estado interno"""
        try:
            # Validar configuración antes de iniciar
            if not self.verificar_configuracion():
                logging.error("Configuración de WhatsApp inválida; no se inicia servidor")
                return False
            # Asegurar cliente inicializado
            if not self.wa_client:
                ok = self._initialize_client()
                if not ok:
                    logging.error("No se pudo inicializar cliente WhatsApp")
                    return False
            # Iniciar servidor webhook
            self.iniciar_servidor_webhook()
            # Confirmar estado inicial sin bloquear
            try:
                if self._server_thread and self._server_thread.is_alive():
                    return True
            except Exception:
                pass
            return bool(self.servidor_activo)
        except Exception as e:
            logging.error(f"Error al iniciar servidor WhatsApp: {e}")
            try:
                self.servidor_activo = False
            except Exception:
                pass
            return False

    def detener_servidor(self) -> bool:
        """Wrapper seguro para detener el servidor webhook y actualizar estado interno"""
        try:
            self.detener_servidor_webhook()
            return True
        except Exception as e:
            logging.error(f"Error al detener servidor WhatsApp: {e}")
            return False

    def _mensaje_audit_ya_registrado(self, message_id: str) -> bool:
        """Devuelve True si ya existe un registro de mensaje con ese message_id."""
        try:
            with self.db.get_connection_context() as conn:
                cur = conn.cursor()
                cur.execute("SELECT 1 FROM whatsapp_messages WHERE message_id = %s LIMIT 1", (message_id,))
                return cur.fetchone() is not None
        except Exception:
            return False

    def _componer_confirmacion_waitlist(self, action: str, usuario: Any, clase_info: Optional[Dict[str, Any]], new_values: Optional[str]) -> str:
        """Crea el texto de confirmación para SI/NO basándose en acción y datos disponibles."""
        nombre = str(getattr(usuario, 'nombre', None) or 'Alumno')
        tipo_clase = None
        fecha = None
        hora = None

        try:
            if isinstance(clase_info, dict):
                # Preferir claves reales de DB: tipo_clase_nombre, clase_nombre, dia_semana, hora_inicio
                tipo_clase = clase_info.get('tipo_clase_nombre') or clase_info.get('clase_nombre')
                fecha = clase_info.get('fecha') or clase_info.get('dia_semana')
                h_in = clase_info.get('hora') or clase_info.get('hora_inicio')
                try:
                    hora = (str(h_in)[:5] if h_in is not None else None)
                except Exception:
                    hora = str(h_in) if h_in is not None else None
        except Exception:
            pass

        try:
            if new_values and isinstance(new_values, str):
                data = json.loads(new_values)
                tipo_clase = tipo_clase or data.get('tipo_clase_nombre') or data.get('clase_nombre') or data.get('tipo_clase')
                fecha = fecha or data.get('fecha') or data.get('dia_semana')
                h_in = data.get('hora') or data.get('hora_inicio')
                try:
                    hora = hora or (str(h_in)[:5] if h_in is not None else None)
                except Exception:
                    hora = hora or (str(h_in) if h_in is not None else None)
        except Exception:
            pass

        tipo = str(tipo_clase or 'Clase')
        fecha_s = str(fecha or 'por confirmar')
        hora_s = str(hora or 'por confirmar')

        if action == 'auto_promote_waitlist':
            return f"¡{nombre}! Confirmamos tu promoción desde lista de espera a la clase de {tipo} del {fecha_s} a las {hora_s}. ¡Nos vemos!"
        elif action == 'decline_waitlist_promotion':
            return f"¡Gracias {nombre}! Registramos tu NO a la clase de {tipo} del {fecha_s} a las {hora_s}. Tu lugar en espera se mantiene para otra oportunidad."
        else:
            return "Actualización de lista de espera registrada."

    def process_pending_sends(self) -> int:
        """Procesa envíos pendientes basados en auditorías de la webapp (SI/NO lista de espera)."""
        try:
            with self.db.get_connection_context() as conn:
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur.execute(
                    """
                    SELECT id, user_id, action, table_name, record_id, new_values, timestamp
                    FROM audit_logs
                    WHERE action IN ('auto_promote_waitlist','decline_waitlist_promotion')
                    ORDER BY id DESC
                    LIMIT 200
                    """
                )
                rows = cur.fetchall()
        except Exception as e:
            logging.error(f"Error consultando auditorías: {e}")
            return 0

        if not rows:
            return 0

        enviados = 0
        for row in reversed(rows):
            audit_id = row.get('id')
            action = row.get('action')
            user_id = row.get('user_id')
            record_id = row.get('record_id')
            new_values = row.get('new_values')

            message_id = f"audit:{audit_id}"
            if self._mensaje_audit_ya_registrado(message_id):
                continue

            try:
                usuario = self.db.obtener_usuario(user_id)
            except Exception:
                usuario = None

            if not usuario or not getattr(usuario, 'telefono', None):
                logging.warning(f"Auditoría {audit_id}: usuario {user_id} sin teléfono, se omite")
                try:
                    self.message_logger.registrar_mensaje_fallido(
                        telefono=getattr(usuario, 'telefono', '') or '',
                        mensaje=f"[{action}] Confirmación no enviada: teléfono faltante",
                        error="telefono_faltante",
                        tipo_mensaje="waitlist",
                        message_id=message_id
                    )
                except Exception:
                    pass
                continue

            if not self._numero_permitido(usuario.telefono):
                logging.warning(f"Número no permitido por allowlist: {usuario.telefono}")
                try:
                    self.message_logger.registrar_mensaje_fallido(
                        telefono=usuario.telefono,
                        mensaje=f"[{action}] Omitido por allowlist",
                        error="allowlist_block",
                        tipo_mensaje="waitlist",
                        message_id=message_id
                    )
                except Exception:
                    pass
                continue

            clase_info = None
            try:
                if record_id:
                    clase_info = self.db.obtener_horario_por_id(int(record_id))
            except Exception:
                clase_info = None

            # Realizar promoción desde desktop según auditoría
            texto = None
            if action == 'auto_promote_waitlist' and record_id and user_id:
                try:
                    enrolled = bool(self.db.inscribir_usuario_en_clase(int(record_id), int(user_id)))
                except Exception as e:
                    logging.error(f"Error inscribiendo en auto-promoción audit_id={audit_id}: {e}")
                    enrolled = False
                if enrolled:
                    try:
                        self.db.quitar_de_lista_espera_completo(int(record_id), int(user_id))
                    except Exception as e:
                        logging.error(f"Error quitando de lista de espera audit_id={audit_id}: {e}")
                    texto = self._componer_confirmacion_waitlist(action, usuario, clase_info, new_values)
                else:
                    nombre = str(getattr(usuario, 'nombre', None) or 'Alumno')
                    tipo = str((clase_info or {}).get('tipo_clase_nombre') or (clase_info or {}).get('clase_nombre') or 'Clase')
                    fecha_s = str((clase_info or {}).get('fecha') or (clase_info or {}).get('dia_semana') or 'por confirmar')
                    h_in = (clase_info or {}).get('hora') or (clase_info or {}).get('hora_inicio')
                    try:
                        hora_s = str(h_in)[:5] if h_in is not None else 'por confirmar'
                    except Exception:
                        hora_s = str(h_in) if h_in is not None else 'por confirmar'
                    texto = f"¡{nombre}! Confirmaste tu lugar para {tipo} del {fecha_s} a las {hora_s}, pero el cupo no está disponible en este momento. Te mantenemos en lista de espera y te avisaremos ante la próxima disponibilidad."
            else:
                texto = self._componer_confirmacion_waitlist(action, usuario, clase_info, new_values)

            if not self.message_logger.puede_enviar_mensaje(usuario.telefono):
                logging.info(f"Anti-spam bloqueó confirmación para {usuario.telefono}")
                continue

            try:
                ok, resp = self._send_message(to=usuario.telefono, text=texto)
                self.message_logger.registrar_mensaje_enviado(
                    telefono=usuario.telefono,
                    mensaje=texto,
                    tipo_mensaje="waitlist",
                    message_id=message_id if ok else getattr(resp, 'id', None)
                )
                enviados += 1 if ok else 0
            except Exception as send_err:
                logging.error(f"Error enviando confirmación de auditoría {audit_id}: {send_err}")
                try:
                    self.message_logger.registrar_mensaje_fallido(
                        telefono=usuario.telefono,
                        mensaje=texto,
                        error=str(send_err),
                        tipo_mensaje="waitlist",
                        message_id=message_id
                    )
                except Exception:
                    pass
                continue

        return enviados
    
    def verificar_configuracion(self):
        """Verifica que la configuración de WhatsApp esté completa"""
        errores = []
        
        if not self.phone_number_id:
            errores.append("Phone ID no configurado")
        
        if not self.access_token:
            errores.append("Access Token no configurado")
        
        if not self.whatsapp_business_account_id:
            errores.append("WhatsApp Business Account ID no configurado")
        
        if errores:
            print("❌ Errores de configuración de WhatsApp:")
            for error in errores:
                print(f"   - {error}")
            return False
        
        print("✅ Configuración de WhatsApp verificada correctamente")
        return True
    
    def obtener_estadisticas(self) -> Dict[str, Any]:
        """Obtiene estadísticas del sistema de mensajería"""
        try:
            stats_diarias = self.message_logger.obtener_estadisticas_diarias()
            stats_semanales = self.message_logger.obtener_estadisticas_semanales()
            
            return {
                'hoy': stats_diarias,
                'esta_semana': stats_semanales,
                'plantillas_activas': len(self.db.obtener_plantillas_whatsapp(activas_solo=True)),
                'configuracion_valida': self.wa_client is not None
            }
        except Exception as e:
            logging.error(f"Error al obtener estadísticas: {e}")
            return {}

# Función de utilidad para crear instancia global
def crear_whatsapp_manager(db_manager):
    """Función de conveniencia para crear una instancia de WhatsAppManager"""
    # Crear instancia con datos hardcodeados
    return WhatsAppManager(db_manager)