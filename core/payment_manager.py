from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any

import logging
import json
import psycopg2.extras
from .models import Pago, Usuario, MetodoPago, ConceptoPago, PagoDetalle
# Importar sistema de alertas con fallback si PyQt6 no está disponible (entornos como Railway)
try:
    from utils_modules.alert_system import alert_manager, AlertLevel, AlertCategory
except Exception:
    import logging as _logging
    from enum import Enum as _Enum
    class AlertLevel(_Enum):
        INFO = "info"
        WARNING = "warning"
        CRITICAL = "critical"
        ERROR = "error"
    class AlertCategory(_Enum):
        SYSTEM = "system"
        DATABASE = "database"
        MAINTENANCE = "maintenance"
        SECURITY = "security"
        PERFORMANCE = "performance"
        BACKUP = "backup"
        MEMBERSHIP = "membership"
        PAYMENT = "payment"
    class _StubAlertManager:
        def generate_alert(self, level, category, title: str, message: str, source: str = None):
            try:
                lvl = getattr(level, 'value', str(level))
                cat = getattr(category, 'value', str(category))
                _logging.info(f"[ALERT:{lvl}/{cat}] {title} - {message} (source={source})")
            except Exception:
                pass
            return None
    alert_manager = _StubAlertManager()
from .database import DatabaseManager, database_retry

# Importar módulos WhatsApp (importación condicional para evitar errores si no están disponibles)
try:
    from .whatsapp_manager import WhatsAppManager
    from .message_logger import MessageLogger
    WHATSAPP_AVAILABLE = True
except ImportError:
    WHATSAPP_AVAILABLE = False
    logging.warning("Módulos WhatsApp no disponibles. Funcionalidad de notificaciones deshabilitada.")

class PaymentManager:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        
        # Inicializar componentes WhatsApp si están disponibles
        self.whatsapp_manager = None
        self.message_logger = None
        self.whatsapp_enabled = False
        
        if WHATSAPP_AVAILABLE:
            try:
                # Inicializar logger de mensajes y gestor WhatsApp en modo perezoso
                self.message_logger = MessageLogger(db_manager)
                self.whatsapp_manager = WhatsAppManager(db_manager, defer_init=True)
                # Vincular referencias cruzadas para delegación correcta
                try:
                    setattr(self.whatsapp_manager, 'payment_manager', self)
                except Exception:
                    pass
                # Verificar configuración ligera sin crear cliente
                self.whatsapp_enabled = self.whatsapp_manager.verificar_configuracion()
                if self.whatsapp_enabled:
                    logging.info("WhatsApp configurado correctamente (init diferido)")
                else:
                    logging.warning("Sistema WhatsApp no configurado correctamente")
            except Exception as e:
                logging.error(f"Error al inicializar sistema WhatsApp: {e}")
                self.whatsapp_enabled = False

    def start_whatsapp_initialization(self, background: bool = True, delay_seconds: float = 1.5):
        """Inicia la inicialización del cliente WhatsApp de forma diferida para evitar bloquear la UI"""
        try:
            if not WHATSAPP_AVAILABLE or not self.whatsapp_manager:
                return
            if background:
                self.whatsapp_manager.initialize_async(delay_seconds=delay_seconds)
            else:
                # Inicialización directa si se necesita bloquear (evitar en UI)
                self.whatsapp_manager._initialize_client()
        except Exception as e:
            logging.error(f"No se pudo iniciar la inicialización de WhatsApp: {e}")

    @database_retry(max_retries=3, base_delay=1.0)
    def registrar_pago(self, usuario_id: int, monto: float, mes: int, año: int, metodo_pago_id: Optional[int] = None) -> int:
        usuario = self.db_manager.obtener_usuario(usuario_id)
        if not usuario:
            raise ValueError(f"No existe usuario con ID: {usuario_id}")
        try:
            # Usar transacción atómica para garantizar consistencia de pago y actualización de usuario
            with self.db_manager.atomic_transaction(isolation_level="REPEATABLE READ") as conn:
                cursor = conn.cursor()
                # Crear o actualizar pago idempotentemente por (usuario_id, mes, año)
                cursor.execute(
                    """
                    INSERT INTO pagos (usuario_id, monto, mes, año, metodo_pago_id)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (usuario_id, mes, año) DO UPDATE
                    SET monto = EXCLUDED.monto,
                        metodo_pago_id = COALESCE(EXCLUDED.metodo_pago_id, pagos.metodo_pago_id),
                        fecha_pago = CURRENT_TIMESTAMP
                    RETURNING id
                    """,
                    (usuario_id, monto, mes, año, metodo_pago_id)
                )
                result = cursor.fetchone()
                if not result:
                    raise ValueError("Error al crear el pago: no se obtuvo ID")
                pago_id = result[0]

                # Calcular próximo vencimiento usando duracion_dias del tipo de cuota del usuario
                cursor.execute(
                    """
                    SELECT tc.duracion_dias
                    FROM usuarios u
                    JOIN tipos_cuota tc ON u.tipo_cuota = tc.nombre
                    WHERE u.id = %s
                    """,
                    (usuario_id,)
                )
                row = cursor.fetchone()
                duracion_dias = row[0] if row and len(row) > 0 else 30

                # Usar la fecha actual del sistema (fecha del pago) para calcular el próximo vencimiento
                fecha_pago = datetime.now().date()
                proximo_vencimiento = fecha_pago + timedelta(days=duracion_dias)

                # Actualizar usuario: próximo vencimiento, último pago, reactivar y resetear cuotas vencidas
                cursor.execute(
                    """
                    UPDATE usuarios
                    SET fecha_proximo_vencimiento = %s,
                        ultimo_pago = %s,
                        activo = TRUE,
                        cuotas_vencidas = 0
                    WHERE id = %s
                    """,
                    (proximo_vencimiento, fecha_pago, usuario_id)
                )

            # Fuera de la transacción: registrar auditoría y enviar notificación (cada uno maneja su propia persistencia)
            if hasattr(self.db_manager, 'audit_logger') and self.db_manager.audit_logger:
                new_values = {
                    'id': pago_id,
                    'usuario_id': usuario_id,
                    'monto': monto,
                    'mes': mes,
                    'año': año,
                    'metodo_pago_id': metodo_pago_id
                }
                self.db_manager.audit_logger.log_operation('CREATE', 'pagos', pago_id, None, new_values)


            # Enviar notificación WhatsApp de confirmación de pago
            self._enviar_notificacion_pago_confirmado(usuario_id, monto, mes, año)

            # Generar alerta de pago registrado (flujo básico)
            try:
                alert_manager.generate_alert(
                    level=AlertLevel.INFO,
                    category=AlertCategory.PAYMENT,
                    title="Pago registrado",
                    message=f"{usuario.nombre} pagó {float(monto):.2f} para {mes:02d}/{año}",
                    source="payment_manager"
                )
            except Exception as e:
                logging.warning(f"No se pudo generar alerta de pago (flujo básico): {e}")

            # Invalidar cache de usuario si el gestor de base de datos lo soporta
            try:
                if hasattr(self.db_manager, 'cache') and hasattr(self.db_manager.cache, 'invalidate'):
                    self.db_manager.cache.invalidate('usuarios', usuario_id)
                    # Invalidate pagos-related caches to ensure latest payment history/stats
                    self.db_manager.cache.invalidate('pagos')
                    self.db_manager.cache.invalidate('reportes')
            except Exception as cache_err:
                logging.warning(f"No se pudo invalidar cache para el usuario {usuario_id}: {cache_err}")

            return pago_id
        except psycopg2.errors.UniqueViolation:
            raise ValueError("Este usuario ya tiene un pago registrado para el período seleccionado (mes y año).")
        except Exception as e:
            raise

    def modificar_pago(self, pago: Pago):
        self.db_manager.modificar_pago(pago)
        # Generar alerta de pago modificado
        try:
            usuario = self.db_manager.obtener_usuario(pago.usuario_id)
            nombre = usuario.nombre if usuario else str(pago.usuario_id)
            monto = getattr(pago, 'monto', None)
            monto_txt = f" por {float(monto):.2f}" if monto is not None else ""
            alert_manager.generate_alert(
                level=AlertLevel.WARNING,
                category=AlertCategory.PAYMENT,
                title="Pago modificado",
                message=f"Se modificó el pago de {nombre}{monto_txt}",
                source="payment_manager"
            )
        except Exception as e:
            logging.warning(f"No se pudo generar alerta de modificación de pago: {e}")
        # Recalcular estado del usuario afectado
        try:
            self._recalcular_estado_usuario(pago.usuario_id)
        except Exception as e:
            logging.error(f"Error al recalcular estado de usuario tras modificar pago: {e}")

    def modificar_pago_avanzado(self, pago_id: int, usuario_id: int, 
                                metodo_pago_id: Optional[int], 
                                conceptos: List[Dict[str, Any]], 
                                fecha_pago: Optional[datetime] = None,
                                monto_personalizado: Optional[float] = None) -> bool:
        """Actualiza un pago existente con una lista de conceptos (ítems) y método de pago.

        - Recalcula el total con comisión según el método de pago, salvo que se provea monto_personalizado.
        - Actualiza el registro principal en `pagos` (usuario, monto, fecha, método, mes/año).
        - Reemplaza completamente los `pago_detalles` asociados al pago.
        - Invalida caches relevantes y recalcula estado del usuario.
        """
        # Validaciones mínimas
        if not isinstance(conceptos, list) or len(conceptos) == 0:
            raise ValueError("Se requiere una lista de 'conceptos' para la modificación avanzada")

        try:
            usuario = self.db_manager.obtener_usuario(usuario_id)
            if not usuario:
                raise ValueError(f"No existe usuario con ID: {usuario_id}")

            # Si no se proporciona fecha, intentar obtener del pago existente
            if fecha_pago is None:
                try:
                    pago_existente = self.obtener_pago(pago_id)
                    if pago_existente and getattr(pago_existente, 'fecha_pago', None):
                        fecha_pago = pago_existente.fecha_pago if not isinstance(pago_existente.fecha_pago, str) else datetime.fromisoformat(pago_existente.fecha_pago)
                    else:
                        fecha_pago = datetime.now()
                except Exception:
                    fecha_pago = datetime.now()

            # Derivar mes/año de la fecha
            mes = fecha_pago.month
            año = fecha_pago.year

            # Calcular total de conceptos y comisión
            if monto_personalizado is not None:
                total_final = float(monto_personalizado)
            else:
                total_conceptos = 0.0
                for c in conceptos:
                    cantidad = float(c.get('cantidad', 1))
                    precio = float(c.get('precio_unitario', 0.0))
                    if cantidad <= 0 or precio < 0:
                        raise ValueError("Cantidad/precio inválidos en conceptos")
                    total_conceptos += cantidad * precio
                comision = self.calcular_comision(float(total_conceptos), metodo_pago_id)
                total_final = float(total_conceptos) + comision

            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor()

                # Actualizar pago principal, incluyendo mes/año para mantener consistencia con fecha
                cursor.execute(
                    """
                    UPDATE pagos
                    SET usuario_id = %s,
                        monto = %s,
                        fecha_pago = %s,
                        metodo_pago_id = %s,
                        mes = %s,
                        año = %s
                    WHERE id = %s
                    """,
                    (usuario_id, total_final, fecha_pago, metodo_pago_id, mes, año, pago_id)
                )

                # Reemplazar detalles
                cursor.execute("DELETE FROM pago_detalles WHERE pago_id = %s", (pago_id,))

                filas = []
                for concepto in conceptos:
                    cantidad = float(concepto.get('cantidad', 1))
                    precio = float(concepto.get('precio_unitario', 0.0))
                    subtotal = cantidad * precio
                    cid_raw = concepto.get('concepto_id', None)
                    try:
                        cid_val = int(cid_raw) if cid_raw is not None else None
                    except Exception:
                        cid_val = None
                    descripcion = concepto.get('descripcion')
                    filas.append((pago_id, cid_val, descripcion, cantidad, precio, subtotal, subtotal))

                if filas:
                    try:
                        psycopg2.extras.execute_values(
                            cursor,
                            "INSERT INTO pago_detalles (pago_id, concepto_id, descripcion, cantidad, precio_unitario, subtotal, total) VALUES %s",
                            filas,
                            page_size=250
                        )
                    except Exception:
                        # Fallback secuencial
                        for row in filas:
                            cursor.execute(
                                "INSERT INTO pago_detalles (pago_id, concepto_id, descripcion, cantidad, precio_unitario, subtotal, total) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                                row
                            )

                conn.commit()

            # Alertar modificación avanzada
            try:
                nombre = usuario.nombre if usuario else str(usuario_id)
                alert_manager.generate_alert(
                    level=AlertLevel.WARNING,
                    category=AlertCategory.PAYMENT,
                    title="Pago modificado",
                    message=f"Se actualizó el pago de {nombre} con conceptos (total {total_final:.2f})",
                    source="payment_manager"
                )
            except Exception as e:
                logging.warning(f"No se pudo generar alerta de modificación de pago avanzado: {e}")

            # Invalidar caches relevantes
            try:
                if hasattr(self.db_manager, 'cache') and hasattr(self.db_manager.cache, 'invalidate'):
                    self.db_manager.cache.invalidate('usuarios', usuario_id)
                    self.db_manager.cache.invalidate('pagos')
                    self.db_manager.cache.invalidate('reportes')
            except Exception as cache_err:
                logging.warning(f"No se pudo invalidar caché tras modificar pago {pago_id}: {cache_err}")

            # Recalcular estado del usuario afectado
            try:
                self._recalcular_estado_usuario(usuario_id)
            except Exception as e:
                logging.error(f"Error al recalcular estado de usuario tras modificar pago avanzado: {e}")

            return True
        except Exception:
            raise

    def eliminar_pago(self, pago_id: int):
        # Obtener información del pago antes de eliminar para construir la alerta
        pago = self.obtener_pago(pago_id)
        self.db_manager.eliminar_pago(pago_id)
        # Generar alerta de pago eliminado
        try:
            if pago:
                usuario = self.db_manager.obtener_usuario(pago.usuario_id)
                nombre = usuario.nombre if usuario else str(pago.usuario_id)
                monto = getattr(pago, 'monto', None)
                monto_txt = f" de {float(monto):.2f}" if monto is not None else ""
                alert_manager.generate_alert(
                    level=AlertLevel.WARNING,
                    category=AlertCategory.PAYMENT,
                    title="Pago eliminado",
                    message=f"Se eliminó el pago{monto_txt} de {nombre}",
                    source="payment_manager"
                )
            else:
                alert_manager.generate_alert(
                    level=AlertLevel.WARNING,
                    category=AlertCategory.PAYMENT,
                    title="Pago eliminado",
                    message=f"Se eliminó un pago (ID {pago_id})",
                    source="payment_manager"
                )
        except Exception as e:
            logging.warning(f"No se pudo generar alerta de eliminación de pago: {e}")
        # Recalcular estado del usuario afectado
        try:
            if pago:
                self._recalcular_estado_usuario(pago.usuario_id)
        except Exception as e:
            logging.error(f"Error al recalcular estado de usuario tras eliminar pago: {e}")

    def verificar_pago_actual(self, usuario_id: int, mes: int, anio: int) -> bool:
        """Verifica si un usuario ha pagado en el mes y año especificados usando columnas mes/año."""
        with self.db_manager.get_connection_context() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT 1 FROM pagos WHERE usuario_id = %s AND mes = %s AND año = %s",
                (usuario_id, mes, anio)
            )
            return cursor.fetchone() is not None
    
    def obtener_pago_actual(self, usuario_id: int, mes: int, anio: int) -> Optional[Pago]:
        """Obtiene el pago de un usuario para el mes y año especificados usando columnas mes/año."""
        with self.db_manager.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute(
                "SELECT id, usuario_id, monto, mes, año, fecha_pago, metodo_pago_id FROM pagos WHERE usuario_id = %s AND mes = %s AND año = %s",
                (usuario_id, mes, anio)
            )
            row = cursor.fetchone()
            return self._crear_pago_desde_fila(row) if row else None

    def _crear_pago_desde_fila(self, row: dict) -> Pago:
        pago_data_raw = dict(row or {})
        # Normalizar fecha_pago
        try:
            fp = pago_data_raw.get('fecha_pago')
            pago_data_raw['fecha_pago'] = datetime.fromisoformat(fp) if isinstance(fp, str) else fp
        except (TypeError, ValueError):
            pago_data_raw['fecha_pago'] = datetime.now()
        # Asegurar que metodo_pago_id esté presente
        if 'metodo_pago_id' not in pago_data_raw:
            pago_data_raw['metodo_pago_id'] = None
        # Filtrar únicamente las claves del modelo Pago
        allowed = {'id', 'usuario_id', 'monto', 'mes', 'año', 'fecha_pago', 'metodo_pago_id'}
        pago_clean = {k: pago_data_raw.get(k) for k in allowed if k in pago_data_raw}
        return Pago(**pago_clean)

    def obtener_pago(self, pago_id: int) -> Optional[Pago]:
        with self.db_manager.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute(
                "SELECT id, usuario_id, monto, mes, año, fecha_pago, metodo_pago_id FROM pagos WHERE id = %s",
                (pago_id,)
            )
            row = cursor.fetchone()
            return self._crear_pago_desde_fila(row) if row else None

    def _recalcular_estado_usuario(self, usuario_id: int) -> Dict[str, Any]:
        """Recalcula fecha_proximo_vencimiento, ultimo_pago y cuotas_vencidas del usuario.
        - Usa el último pago si existe; si no, usa fecha_registro para calcular vencimiento.
        - No dispara notificaciones; solo actualiza estado. Desactiva si alcanza 3 cuotas vencidas.
        Devuelve un resumen con los valores aplicados.
        """
        from datetime import date
        with self.db_manager.get_connection_context() as conn:
            cursor = conn.cursor()

            # Obtener último pago
            cursor.execute(
                "SELECT fecha_pago FROM pagos WHERE usuario_id = %s ORDER BY fecha_pago DESC LIMIT 1",
                (usuario_id,)
            )
            row_pago = cursor.fetchone()
            ultimo_pago_dt = row_pago[0] if row_pago else None

            # Obtener fecha de registro (fallback), tipo de cuota y cuotas_vencidas previas
            cursor.execute(
                "SELECT fecha_registro, tipo_cuota, COALESCE(cuotas_vencidas, 0), rol FROM usuarios WHERE id = %s",
                (usuario_id,)
            )
            row_usr = cursor.fetchone()
            fecha_registro_dt = row_usr[0] if row_usr else None
            tipo_raw = row_usr[1] if row_usr else None
            cuotas_previas = int(row_usr[2]) if row_usr and row_usr[2] is not None else 0
            rol_lower = str(row_usr[3] if row_usr and len(row_usr) > 3 and row_usr[3] is not None else "").lower()
            exento = rol_lower in ("profesor", "dueño", "owner")

            # Obtener duracion_dias desde tipos_cuota (acepta nombre o id)
            cursor.execute(
                """
                SELECT COALESCE(tc.duracion_dias, 30)
                FROM tipos_cuota tc
                WHERE tc.nombre = %s OR tc.id::text = %s
                LIMIT 1
                """,
                (tipo_raw, str(tipo_raw) if tipo_raw is not None else None)
            )
            row_tc = cursor.fetchone()
            duracion_dias = int(row_tc[0]) if row_tc and row_tc[0] is not None else 30

            # Base para cálculo de vencimiento
            base_date = None
            if ultimo_pago_dt is not None:
                try:
                    base_date = (ultimo_pago_dt.date() if hasattr(ultimo_pago_dt, 'date') else datetime.fromisoformat(str(ultimo_pago_dt)).date())
                except Exception:
                    base_date = date.today()
            else:
                # Sin pagos: usar fecha_registro si existe, sino hoy
                try:
                    base_date = (fecha_registro_dt.date() if hasattr(fecha_registro_dt, 'date') else datetime.fromisoformat(str(fecha_registro_dt)).date()) if fecha_registro_dt else date.today()
                except Exception:
                    base_date = date.today()

            # Calcular próximo vencimiento avanzando por ciclos completos hasta el próximo futuro
            primer_vencimiento = base_date + timedelta(days=duracion_dias)
            hoy = date.today()
            if hoy <= primer_vencimiento:
                proximo_vencimiento = primer_vencimiento
                nuevas_cuotas_vencidas = 0
            else:
                # Ciclos vencidos completos desde el primer vencimiento
                dias_desde_primer_venc = (hoy - primer_vencimiento).days
                ciclos_vencidos = (dias_desde_primer_venc + max(duracion_dias, 1) - 1) // max(duracion_dias, 1)  # ceil
                nuevas_cuotas_vencidas = max(ciclos_vencidos, 1)
                proximo_vencimiento = primer_vencimiento + timedelta(days=duracion_dias * ciclos_vencidos)

            # Exención: dueños y profesores no acumulan cuotas vencidas ni se desactivan por morosidad
            if exento:
                nuevas_cuotas_vencidas = 0
                cursor.execute(
                    """
                    UPDATE usuarios
                    SET fecha_proximo_vencimiento = %s,
                        ultimo_pago = %s,
                        cuotas_vencidas = 0
                    WHERE id = %s
                    """,
                    (
                        proximo_vencimiento,
                        ultimo_pago_dt.date() if (ultimo_pago_dt is not None and hasattr(ultimo_pago_dt, 'date')) else None,
                        usuario_id,
                    )
                )
            else:
                # No forzar activación; solo desactivar si cruza umbral
                cursor.execute(
                    """
                    UPDATE usuarios
                    SET fecha_proximo_vencimiento = %s,
                        ultimo_pago = %s,
                        cuotas_vencidas = %s,
                        activo = CASE WHEN %s >= 3 THEN FALSE ELSE activo END
                    WHERE id = %s
                    """,
                    (
                        proximo_vencimiento,
                        ultimo_pago_dt.date() if (ultimo_pago_dt is not None and hasattr(ultimo_pago_dt, 'date')) else None,
                        nuevas_cuotas_vencidas,
                        nuevas_cuotas_vencidas,
                        usuario_id,
                    )
                )
            conn.commit()

            # Verificación post-operación: si cruza umbral de morosidad, disparar revisión
            try:
                self._verificar_y_procesar_morosidad(usuario_id, cuotas_previas, nuevas_cuotas_vencidas)
            except Exception as e:
                logging.error(f"Error verificando morosidad post-recalculo: {e}")

            resumen = {
                'usuario_id': usuario_id,
                'duracion_dias': duracion_dias,
                'base_date': base_date,
                'fecha_proximo_vencimiento': proximo_vencimiento,
                'ultimo_pago': (ultimo_pago_dt.date() if (ultimo_pago_dt is not None and hasattr(ultimo_pago_dt, 'date')) else None),
                'cuotas_vencidas': nuevas_cuotas_vencidas,
                'desactivado': nuevas_cuotas_vencidas >= 3
            }
            try:
                alert_manager.generate_alert(
                    level=AlertLevel.INFO,
                    category=AlertCategory.PAYMENT,
                    title="Estado de usuario recalculado",
                    message=(
                        f"Usuario {usuario_id}: cv={nuevas_cuotas_vencidas}, "
                        f"próximo={proximo_vencimiento}, último_pago={resumen['ultimo_pago']}"
                    ),
                    source="payment_manager"
                )
            except Exception:
                pass
            return resumen

    def _verificar_y_procesar_morosidad(self, usuario_id: int, cuotas_previas: int, cuotas_actuales: int) -> bool:
        """Si el usuario cruza el umbral de morosidad (de <3 a >=3 cuotas vencidas),
        dispara el flujo de revisión y desactivación correspondiente. El envío de WhatsApp
        queda delegado en el flujo dedicado (PaymentManager -> WhatsAppManager).
        Devuelve True si se procesó la desactivación.
        """
        try:
            if cuotas_previas < 3 and cuotas_actuales >= 3:
                # Desactivar en base de datos (maneja excepciones de roles como profesor/dueño)
                try:
                    self.db_manager.desactivar_usuario_por_cuotas_vencidas(usuario_id)
                except Exception as e:
                    logging.error(f"Error desactivando usuario {usuario_id} por cuotas vencidas: {e}")

                # Enviar notificación de desactivación con plantilla correcta
                try:
                    if getattr(self, 'whatsapp_enabled', False) and getattr(self, 'whatsapp_manager', None):
                        self.whatsapp_manager.enviar_notificacion_desactivacion(
                            usuario_id=usuario_id,
                            motivo="3 cuotas vencidas",
                            force_send=True
                        )
                except Exception as e:
                    logging.error(f"Error enviando notificación de desactivación a usuario {usuario_id}: {e}")

                # Alerta del sistema
                try:
                    alert_manager.generate_alert(
                        level=AlertLevel.WARNING,
                        category=AlertCategory.PAYMENT,
                        title="Usuario desactivado por morosidad",
                        message=f"Usuario {usuario_id} alcanzó {cuotas_actuales} cuotas vencidas",
                        source="payment_manager"
                    )
                except Exception:
                    pass
                return True
            return False
        except Exception:
            return False

    def recalcular_estado_usuario(self, usuario_id: int) -> Dict[str, Any]:
        """Wrapper público para recalcular estado de usuario y refrescar cachés relacionadas.

        - Llama al método interno para actualizar `fecha_proximo_vencimiento`, `ultimo_pago`, `cuotas_vencidas` y `activo`.
        - Invalida caché de `usuarios` y `reportes` para asegurar que la UI obtenga datos frescos.
        - Devuelve el resumen del recalculo.
        """
        try:
            resumen = self._recalcular_estado_usuario(usuario_id)
            # Invalidar caches relevantes para reflejar cambios inmediatos en UI
            try:
                if hasattr(self.db_manager, 'cache') and hasattr(self.db_manager.cache, 'invalidate'):
                    self.db_manager.cache.invalidate('usuarios', usuario_id)
                    self.db_manager.cache.invalidate('reportes')
            except Exception as cache_err:
                logging.warning(f"No se pudo invalidar caché tras recalculo de usuario {usuario_id}: {cache_err}")
            return resumen
        except Exception as e:
            logging.error(f"Error recalculando estado de usuario {usuario_id}: {e}")
            return {}

    # --- NUEVO: Historial de pagos por usuario ---
    def obtener_historial_pagos(self, usuario_id: int, limit: Optional[int] = None) -> List[Pago]:
        """Obtiene el historial de pagos del usuario ordenado por fecha más reciente.
        Devuelve una lista de objetos Pago con campos: id, usuario_id, monto, mes, año, fecha_pago, metodo_pago_id.
        """
        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                query = (
                    "SELECT id, usuario_id, monto, mes, año, fecha_pago, metodo_pago_id "
                    "FROM pagos WHERE usuario_id = %s "
                    "ORDER BY fecha_pago DESC, año DESC, mes DESC"
                )
                if limit and isinstance(limit, int) and limit > 0:
                    query += " LIMIT %s"
                    cursor.execute(query, (usuario_id, limit))
                else:
                    cursor.execute(query, (usuario_id,))
                rows = cursor.fetchall() or []
                return [self._crear_pago_desde_fila(dict(r)) for r in rows]
        except Exception as e:
            logging.error(f"Error obteniendo historial de pagos del usuario {usuario_id}: {e}")
            return []


    @database_retry(max_retries=3, base_delay=1.0)
    def registrar_pago_avanzado(self, usuario_id: int, metodo_pago_id: int, 
                               conceptos: List[Dict[str, Any]], fecha_pago: Optional[datetime] = None, 
                               monto_personalizado: Optional[float] = None) -> int:
        """Registra un pago con múltiples conceptos y método de pago específico.

        Cada elemento de "conceptos" puede incluir:
        - concepto_id (opcional, puede ser None)
        - descripcion (opcional, usada cuando no hay concepto_id)
        - cantidad
        - precio_unitario
        """
        usuario = self.db_manager.obtener_usuario(usuario_id)
        if not usuario:
            raise ValueError(f"No existe usuario con ID: {usuario_id}")
        
        # Usar fecha actual si no se proporciona
        if fecha_pago is None:
            fecha_pago = datetime.now()
        
        # Usar monto personalizado si se proporciona, sino calcular automáticamente
        if monto_personalizado is not None:
            total_final = float(monto_personalizado)
        else:
            # Calcular total de conceptos
            total_conceptos = 0.0
            try:
                total_conceptos = sum(float(c.get('cantidad', 1)) * float(c.get('precio_unitario', 0.0)) for c in conceptos)
            except Exception:
                total_conceptos = sum(c['cantidad'] * c['precio_unitario'] for c in conceptos)
            
            # Calcular comisión
            comision = self.calcular_comision(float(total_conceptos), metodo_pago_id)
            total_final = float(total_conceptos) + comision
        
        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor()
                
                # Extraer mes y año de la fecha_pago
                mes = fecha_pago.month
                año = fecha_pago.year
                
# Crear el pago principal con UPSERT idempotente
                cursor.execute(
                    """
                    INSERT INTO pagos (usuario_id, monto, mes, año, fecha_pago, metodo_pago_id)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (usuario_id, mes, año) DO UPDATE SET
                        monto = EXCLUDED.monto,
                        metodo_pago_id = COALESCE(EXCLUDED.metodo_pago_id, pagos.metodo_pago_id),
                        fecha_pago = EXCLUDED.fecha_pago
                    RETURNING id
                    """,
                    (usuario_id, total_final, mes, año, fecha_pago, metodo_pago_id)
                )
                result = cursor.fetchone()
                if not result:
                    raise ValueError("Error al crear/actualizar el pago avanzado: no se obtuvo ID")
                pago_id = result[0]
                # Crear los detalles de pago en lote
                try:
                    filas = []
                    for concepto in conceptos:
                        cantidad = float(concepto.get('cantidad', 1))
                        precio = float(concepto.get('precio_unitario', 0.0))
                        subtotal = cantidad * precio
                        # Permitir concepto_id nulo y descripción personalizada
                        cid_raw = concepto.get('concepto_id', None)
                        try:
                            cid_val = int(cid_raw) if cid_raw is not None else None
                        except Exception:
                            cid_val = None
                        descripcion = concepto.get('descripcion')
                        filas.append((pago_id, cid_val, descripcion, cantidad, precio, subtotal, subtotal))
                    if filas:
                        psycopg2.extras.execute_values(
                            cursor,
                            "INSERT INTO pago_detalles (pago_id, concepto_id, descripcion, cantidad, precio_unitario, subtotal, total) VALUES %s",
                            filas,
                            page_size=250
                        )
                except Exception as e:
                    logging.error(f"No se pudieron insertar detalles en lote, intentando secuencial: {e}")
                    for concepto in conceptos:
                        cantidad = float(concepto.get('cantidad', 1))
                        precio = float(concepto.get('precio_unitario', 0.0))
                        subtotal = cantidad * precio
                        cid_raw = concepto.get('concepto_id', None)
                        try:
                            cid_val = int(cid_raw) if cid_raw is not None else None
                        except Exception:
                            cid_val = None
                        descripcion = concepto.get('descripcion')
                        cursor.execute(
                            "INSERT INTO pago_detalles (pago_id, concepto_id, descripcion, cantidad, precio_unitario, subtotal, total) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                            (pago_id, cid_val, descripcion, cantidad, precio, subtotal, subtotal)
                        )
                # --- Ajuste de estado del usuario y contador de cuotas vencidas ---
                try:
                    # Obtener duracion_dias desde el tipo de cuota del usuario (acepta nombre o id)
                    cursor.execute(
                        """
                        SELECT COALESCE(tc.duracion_dias, 30)
                        FROM usuarios u
                        LEFT JOIN tipos_cuota tc ON u.tipo_cuota = tc.nombre OR u.tipo_cuota::text = tc.id::text
                        WHERE u.id = %s
                        """,
                        (usuario_id,)
                    )
                    row = cursor.fetchone()
                    duracion_dias = int(row[0]) if row and row[0] is not None else 30

                    # Calcular próximo vencimiento basado en la fecha del pago y duracion_dias
                    fecha_pago_date = fecha_pago.date() if isinstance(fecha_pago, datetime) else datetime.now().date()
                    proximo_vencimiento = fecha_pago_date + timedelta(days=duracion_dias)

                    # Obtener contador actual de cuotas vencidas
                    cursor.execute(
                        "SELECT COALESCE(cuotas_vencidas, 0) FROM usuarios WHERE id = %s",
                        (usuario_id,)
                    )
                    cv_row = cursor.fetchone()
                    cuotas_actuales = int(cv_row[0]) if cv_row and cv_row[0] is not None else 0

                    # Regla de ajuste del contador:
                    # - Si el usuario queda al día (hoy <= próximo vencimiento), resetear a 0
                    # - Si el pago corresponde a una cuota vencida y aún queda atraso, decrementar en 1 (mínimo 0)
                    hoy = datetime.now().date()
                    if hoy <= proximo_vencimiento:
                        nuevas_cuotas_vencidas = 0
                    else:
                        nuevas_cuotas_vencidas = max(cuotas_actuales - 1, 0)

                    # Actualizar estado del usuario
                    cursor.execute(
                        """
                        UPDATE usuarios
                        SET fecha_proximo_vencimiento = %s,
                            ultimo_pago = %s,
                            activo = TRUE,
                            cuotas_vencidas = %s
                        WHERE id = %s
                        """,
                        (proximo_vencimiento, fecha_pago_date, nuevas_cuotas_vencidas, usuario_id)
                    )
                    # Verificación post-operación: si cruza umbral de morosidad, disparar revisión
                    try:
                        self._verificar_y_procesar_morosidad(usuario_id, cuotas_actuales, nuevas_cuotas_vencidas)
                    except Exception as e:
                        logging.error(f"Error verificando morosidad post-pago avanzado: {e}")
                except Exception as e:
                    # No bloquear el registro de pago si falla el ajuste; registrar en logs
                    logging.error(f"Error ajustando estado de usuario tras pago avanzado: {e}")

                conn.commit()

                # Invalidar caché tras registrar pago avanzado para reflejar datos actualizados en UI
                try:
                    if hasattr(self.db_manager, 'cache') and hasattr(self.db_manager.cache, 'invalidate'):
                        # Usuario: refleja nuevo próximo vencimiento, último pago, activo y cuotas_vencidas
                        self.db_manager.cache.invalidate('usuarios', usuario_id)
                        # Pagos: historial y resúmenes
                        self.db_manager.cache.invalidate('pagos')
                        # Reportes: estadísticas agregadas
                        self.db_manager.cache.invalidate('reportes')
                except Exception as cache_err:
                    logging.warning(f"No se pudo invalidar cache post-pago avanzado para usuario {usuario_id}: {cache_err}")


                # Enviar notificación WhatsApp de confirmación de pago
                self._enviar_notificacion_pago_confirmado(usuario_id, total_final, mes, año)
                
                # Generar alerta de pago registrado
                try:
                    alert_manager.generate_alert(
                        level=AlertLevel.INFO,
                        category=AlertCategory.PAYMENT,
                        title=f"Pago registrado",
                        message=f"{usuario.nombre} pagó {total_final:.2f} para {mes:02d}/{año}",
                        source="payment_manager"
                    )
                except Exception as e:
                    logging.warning(f"No se pudo generar alerta de pago: {e}")
                
                return pago_id
                
        except psycopg2.errors.UniqueViolation:
            raise ValueError("Este usuario ya tiene un pago registrado para este período.")
        except Exception:
            raise

    def obtener_resumen_pago_completo(self, pago_id: int) -> Optional[Dict[str, Any]]:
        """Obtiene un resumen completo de un pago en un solo viaje: pago + usuario + detalles.

        Devuelve dict con claves: 'pago' (Pago), 'usuario' (Usuario), 'detalles' (List[PagoDetalle]),
        'total_conceptos' (float) y 'cantidad_conceptos' (int).
        """
        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cursor.execute(
                    """
                    SELECT 
                        -- Pago
                        p.id AS pago_id,
                        p.usuario_id,
                        p.monto,
                        p.mes,
                        p.año,
                        p.fecha_pago,
                        p.metodo_pago_id,
                        -- Usuario
                        u.id AS usuario_id_real,
                        u.nombre AS usuario_nombre,
                        u.dni AS usuario_dni,
                        u.telefono AS usuario_telefono,
                        u.pin AS usuario_pin,
                        u.rol AS usuario_rol,
                        u.notas AS usuario_notas,
                        u.fecha_registro AS usuario_fecha_registro,
                        u.activo AS usuario_activo,
                        u.tipo_cuota AS usuario_tipo_cuota,
                        u.fecha_proximo_vencimiento AS usuario_fecha_proximo_vencimiento,
                        u.cuotas_vencidas AS usuario_cuotas_vencidas,
                        u.ultimo_pago AS usuario_ultimo_pago,
                        -- Detalles agregados como JSON
                        (
                            SELECT json_agg(
                                json_build_object(
                                    'id', pd.id,
                                    'pago_id', pd.pago_id,
                                    'concepto_id', pd.concepto_id,
                                    'descripcion', pd.descripcion,
                                    'cantidad', COALESCE(pd.cantidad, 1),
                                    'precio_unitario', COALESCE(pd.precio_unitario, 0),
                                    'subtotal', COALESCE(pd.subtotal, COALESCE(pd.cantidad,1) * COALESCE(pd.precio_unitario,0)),
                                    'concepto_nombre', COALESCE(cp.nombre, pd.descripcion)
                                )
                            )
                            FROM pago_detalles pd
                            LEFT JOIN conceptos_pago cp ON cp.id = pd.concepto_id
                            WHERE pd.pago_id = p.id
                            ORDER BY pd.id
                        ) AS detalles_json
                    FROM pagos p
                    JOIN usuarios u ON u.id = p.usuario_id
                    WHERE p.id = %s
                    LIMIT 1
                    """,
                    (pago_id,)
                )
                row = cursor.fetchone()
                if not row:
                    return None

                # Construir Pago
                pago = Pago(
                    id=row.get('pago_id'),
                    usuario_id=row.get('usuario_id'),
                    monto=float(row.get('monto') or 0.0),
                    mes=int(row.get('mes') or 0),
                    año=int(row.get('año') or 0),
                    fecha_pago=row.get('fecha_pago'),
                    metodo_pago_id=row.get('metodo_pago_id')
                )

                # Normalizar nombre de usuario: usar solo nombre (apellido no existe en esquema actual)
                nombre = str(row.get('usuario_nombre') or "").strip()
                nombre_full = nombre

                usuario = Usuario(
                    id=row.get('usuario_id_real'),
                    nombre=nombre_full,
                    dni=row.get('usuario_dni'),
                    telefono=row.get('usuario_telefono') or "",
                    pin=row.get('usuario_pin'),
                    rol=row.get('usuario_rol') or "socio",
                    notas=row.get('usuario_notas'),
                    fecha_registro=str(row.get('usuario_fecha_registro')) if row.get('usuario_fecha_registro') is not None else None,
                    activo=bool(row.get('usuario_activo')) if 'usuario_activo' in row else True,
                    tipo_cuota=row.get('usuario_tipo_cuota') or "estandar",
                    fecha_proximo_vencimiento=str(row.get('usuario_fecha_proximo_vencimiento')) if row.get('usuario_fecha_proximo_vencimiento') is not None else None,
                    cuotas_vencidas=int(row.get('usuario_cuotas_vencidas') or 0),
                    ultimo_pago=str(row.get('usuario_ultimo_pago')) if row.get('usuario_ultimo_pago') is not None else None
                )

                # Parsear detalles
                detalles_json = row.get('detalles_json')
                detalles_list: List[PagoDetalle] = []
                try:
                    detalles_data = detalles_json if isinstance(detalles_json, list) else json.loads(detalles_json) if detalles_json else []
                except Exception:
                    detalles_data = []
                for d in detalles_data or []:
                    cantidad = float((d.get('cantidad') if isinstance(d, dict) else d['cantidad']) or 1.0)
                    precio_unitario = float((d.get('precio_unitario') if isinstance(d, dict) else d['precio_unitario']) or 0.0)
                    subtotal = float((d.get('subtotal') if isinstance(d, dict) else d['subtotal']) or (cantidad * precio_unitario))
                    concepto_nombre = (d.get('concepto_nombre') if isinstance(d, dict) else d['concepto_nombre']) or ""
                    detalles_list.append(PagoDetalle(
                        id=(d.get('id') if isinstance(d, dict) else d['id']),
                        pago_id=(d.get('pago_id') if isinstance(d, dict) else d['pago_id']) or pago_id,
                        concepto_id=(d.get('concepto_id') if isinstance(d, dict) else d['concepto_id']),
                        concepto_nombre=str(concepto_nombre),
                        cantidad=cantidad,
                        precio_unitario=precio_unitario,
                        subtotal=subtotal,
                        notas=None
                    ))

                total_conceptos = sum(d.subtotal for d in detalles_list)
                cantidad_conceptos = len(detalles_list)

                return {
                    'pago': pago,
                    'usuario': usuario,
                    'detalles': detalles_list,
                    'total_conceptos': total_conceptos,
                    'cantidad_conceptos': cantidad_conceptos
                }
        except Exception as e:
            logging.error(f"Error obteniendo resumen completo de pago {pago_id}: {e}")
            return None

    # --- NUEVOS MÉTODOS: COMISIONES Y DETALLES DE PAGO ---
    def calcular_comision(self, monto_base: float, metodo_pago_id: Optional[int]) -> float:
        """Calcula el monto de la comisión según el método de pago (porcentaje en metodos_pago.comision)."""
        try:
            if not metodo_pago_id:
                return 0.0
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT comision FROM metodos_pago WHERE id = %s AND activo = TRUE", (metodo_pago_id,))
                row = cursor.fetchone()
                if not row or row[0] is None:
                    return 0.0
                porcentaje = float(row[0])
                if porcentaje <= 0:
                    return 0.0
                # Redondear a 2 decimales para evitar diferencias por flotantes
                return round(monto_base * (porcentaje / 100.0), 2)
        except Exception:
            # En caso de error, no bloquear el flujo de pago por comisión
            return 0.0

    def obtener_detalles_pago(self, pago_id: int) -> List[PagoDetalle]:
        """Obtiene la lista de detalles (PagoDetalle) asociados a un pago."""
        detalles: List[PagoDetalle] = []
        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cursor.execute(
                    """
                    SELECT 
                        pd.id, pd.pago_id, pd.concepto_id, pd.descripcion, 
                        pd.cantidad, pd.precio_unitario, pd.subtotal, pd.total, pd.descuento, pd.fecha_creacion,
                        cp.nombre AS concepto_nombre
                    FROM pago_detalles pd
                    LEFT JOIN conceptos_pago cp ON cp.id = pd.concepto_id
                    WHERE pd.pago_id = %s
                    ORDER BY pd.id
                    """,
                    (pago_id,)
                )
                rows = cursor.fetchall() or []
                for row in rows:
                    concepto_nombre = row.get('concepto_nombre') or row.get('descripcion') or ""
                    cantidad = float(row['cantidad']) if row.get('cantidad') is not None else 1.0
                    precio_unitario = float(row['precio_unitario']) if row.get('precio_unitario') is not None else 0.0
                    subtotal = float(row['subtotal']) if row.get('subtotal') is not None else (cantidad * precio_unitario)
                    detalle = PagoDetalle(
                        id=row.get('id'),
                        pago_id=row.get('pago_id') or pago_id,
                        concepto_id=row.get('concepto_id'),
                        concepto_nombre=str(concepto_nombre),
                        cantidad=cantidad,
                        precio_unitario=precio_unitario,
                        subtotal=subtotal,
                        notas=None
                    )
                    # Asegurar que el subtotal refleja exactamente el valor de DB si estaba presente
                    detalle.subtotal = subtotal
                    detalles.append(detalle)
        except Exception:
            # En caso de error, devolver lista vacía para no romper flujos de UI
            return []
        return detalles

    # --- NUEVOS MÉTODOS: CRUD de Métodos de Pago y Conceptos de Pago ---
    def _crear_metodo_pago_desde_row(self, row: Dict[str, Any]) -> MetodoPago:
        """Convierte un diccionario/row de DB a dataclass MetodoPago."""
        if not row:
            return MetodoPago()
        return MetodoPago(
            id=row.get('id'),
            nombre=row.get('nombre') or "",
            icono=row.get('icono'),
            color=row.get('color') or "#3498db",
            comision=float(row.get('comision') or 0.0),
            activo=bool(row.get('activo')) if 'activo' in row else True,
            fecha_creacion=str(row.get('fecha_creacion')) if row.get('fecha_creacion') is not None else None,
            descripcion=row.get('descripcion')
        )

    def _crear_concepto_pago_desde_row(self, row: Dict[str, Any]) -> ConceptoPago:
        """Convierte un diccionario/row de DB a dataclass ConceptoPago."""
        if not row:
            return ConceptoPago()
        return ConceptoPago(
            id=row.get('id'),
            nombre=row.get('nombre') or "",
            descripcion=row.get('descripcion'),
            precio_base=float(row.get('precio_base') or 0.0),
            tipo=row.get('tipo') or "fijo",
            activo=bool(row.get('activo')) if 'activo' in row else True,
            fecha_creacion=str(row.get('fecha_creacion')) if row.get('fecha_creacion') is not None else None,
            # La columna categoria podría no existir en la tabla; usamos valor por defecto
            categoria=row.get('categoria') or "general",
        )

    # --- NUEVO: Obtener un método de pago por ID ---
    def obtener_metodo_pago(self, metodo_id: int) -> Optional[MetodoPago]:
        """Obtiene un método de pago por su ID como objeto MetodoPago."""
        try:
            try:
                ck = f"id:{int(metodo_id)}"
                cached = self.db_manager.cache.get('metodos_pago', ck)
                if cached:
                    return cached
            except Exception:
                pass
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cursor.execute(
                    "SELECT id, nombre, icono, color, comision, activo, fecha_creacion, descripcion FROM metodos_pago WHERE id = %s",
                    (metodo_id,)
                )
                row = cursor.fetchone()
                result = self._crear_metodo_pago_desde_row(dict(row)) if row else None
                if result:
                    try:
                        self.db_manager.cache.set('metodos_pago', ck, result)
                    except Exception:
                        pass
                return result
        except Exception as e:
            logging.error(f"Error obteniendo método de pago {metodo_id}: {e}")
            return None

    def obtener_metodos_pago(self, solo_activos: bool = True) -> List[MetodoPago]:
        """Obtiene lista de métodos de pago como dataclasses, opcionalmente filtrando por activos."""
        try:
            # Reusar implementación de DatabaseManager si está disponible
            if hasattr(self.db_manager, 'obtener_metodos_pago'):
                rows: List[Dict[str, Any]] = self.db_manager.obtener_metodos_pago(solo_activos=solo_activos) or []
                return [self._crear_metodo_pago_desde_row(r) for r in rows]
            # Fallback directo a SQL
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                query = "SELECT id, nombre, icono, color, comision, activo, fecha_creacion, descripcion FROM metodos_pago"
                params: List[Any] = []
                if solo_activos:
                    query += " WHERE activo = TRUE"
                query += " ORDER BY nombre"
                cursor.execute(query, params)
                rows = cursor.fetchall() or []
                return [self._crear_metodo_pago_desde_row(dict(r)) for r in rows]
        except Exception as e:
            logging.error(f"Error obteniendo métodos de pago: {e}")
            return []

    @database_retry(max_retries=3, base_delay=0.8)
    def crear_metodo_pago(self, metodo: MetodoPago) -> int:
        """Crea un nuevo método de pago y devuelve su ID."""
        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT INTO metodos_pago (nombre, icono, color, comision, activo, descripcion)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (metodo.nombre, metodo.icono, metodo.color, float(metodo.comision or 0.0), bool(metodo.activo), metodo.descripcion)
                )
                new_id_row = cursor.fetchone()
                if not new_id_row:
                    raise ValueError("No se obtuvo ID del nuevo método de pago")
                new_id = int(new_id_row[0])
                conn.commit()

            if hasattr(self.db_manager, 'audit_logger') and self.db_manager.audit_logger:
                self.db_manager.audit_logger.log_operation('CREATE', 'metodos_pago', new_id, None, {
                    'nombre': metodo.nombre,
                    'icono': metodo.icono,
                    'color': metodo.color,
                    'comision': metodo.comision,
                    'activo': metodo.activo,
                    'descripcion': metodo.descripcion,
                })
            try:
                self.db_manager.cache.invalidate('metodos_pago')
            except Exception:
                pass
            return new_id
        except psycopg2.errors.UniqueViolation:
            raise ValueError("Ya existe un método de pago con ese nombre.")
        except Exception:
            raise

    @database_retry(max_retries=3, base_delay=0.8)
    def actualizar_metodo_pago(self, metodo: MetodoPago) -> bool:
        """Actualiza un método de pago existente por ID."""
        if not metodo.id:
            raise ValueError("Se requiere ID para actualizar el método de pago")
        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    UPDATE metodos_pago
                    SET nombre = %s, icono = %s, color = %s, comision = %s, activo = %s, descripcion = %s
                    WHERE id = %s
                    """,
                    (metodo.nombre, metodo.icono, metodo.color, float(metodo.comision or 0.0), bool(metodo.activo), metodo.descripcion, metodo.id)
                )
                updated = cursor.rowcount
                conn.commit()

            if hasattr(self.db_manager, 'audit_logger') and self.db_manager.audit_logger:
                self.db_manager.audit_logger.log_operation('UPDATE', 'metodos_pago', metodo.id, None, {
                    'nombre': metodo.nombre,
                    'icono': metodo.icono,
                    'color': metodo.color,
                    'comision': metodo.comision,
                    'activo': metodo.activo,
                    'descripcion': metodo.descripcion,
                })
            try:
                self.db_manager.cache.invalidate('metodos_pago')
            except Exception:
                pass
            return updated > 0
        except psycopg2.errors.UniqueViolation:
            raise ValueError("Ya existe un método de pago con ese nombre.")
        except Exception:
            raise

    @database_retry(max_retries=3, base_delay=0.8)
    def eliminar_metodo_pago(self, metodo_id: int) -> bool:
        """Elimina un método de pago por ID. Si está referenciado, puede fallar por restricciones de FK."""
        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM metodos_pago WHERE id = %s", (metodo_id,))
                deleted = cursor.rowcount
                conn.commit()
            if hasattr(self.db_manager, 'audit_logger') and self.db_manager.audit_logger and deleted:
                self.db_manager.audit_logger.log_operation('DELETE', 'metodos_pago', metodo_id, None, None)
            if deleted:
                try:
                    self.db_manager.cache.invalidate('metodos_pago')
                except Exception:
                    pass
            return deleted > 0
        except psycopg2.errors.ForeignKeyViolation:
            raise ValueError("No se puede eliminar el método de pago porque está en uso.")
        except Exception:
            raise

    def obtener_conceptos_pago(self, solo_activos: bool = True) -> List[ConceptoPago]:
        """Obtiene lista de conceptos de pago como dataclasses, opcionalmente filtrando por activos."""
        try:
            if hasattr(self.db_manager, 'obtener_conceptos_pago'):
                rows: List[Dict[str, Any]] = self.db_manager.obtener_conceptos_pago(solo_activos=solo_activos) or []
                return [self._crear_concepto_pago_desde_row(r) for r in rows]
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                query = "SELECT id, nombre, descripcion, precio_base, tipo, activo, fecha_creacion FROM conceptos_pago"
                if solo_activos:
                    query += " WHERE activo = TRUE"
                query += " ORDER BY nombre"
                cursor.execute(query)
                rows = cursor.fetchall() or []
                return [self._crear_concepto_pago_desde_row(dict(r)) for r in rows]
        except Exception as e:
            logging.error(f"Error obteniendo conceptos de pago: {e}")
            return []

    @database_retry(max_retries=3, base_delay=0.8)
    def crear_concepto_pago(self, concepto: ConceptoPago) -> int:
        """Crea un nuevo concepto de pago y devuelve su ID."""
        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT INTO conceptos_pago (nombre, descripcion, precio_base, tipo, activo)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (concepto.nombre, concepto.descripcion, float(concepto.precio_base or 0.0), concepto.tipo, bool(concepto.activo))
                )
                new_id_row = cursor.fetchone()
                if not new_id_row:
                    raise ValueError("No se obtuvo ID del nuevo concepto de pago")
                new_id = int(new_id_row[0])
                conn.commit()
            if hasattr(self.db_manager, 'audit_logger') and self.db_manager.audit_logger:
                self.db_manager.audit_logger.log_operation('CREATE', 'conceptos_pago', new_id, None, {
                    'nombre': concepto.nombre,
                    'descripcion': concepto.descripcion,
                    'precio_base': concepto.precio_base,
                    'tipo': concepto.tipo,
                    'activo': concepto.activo,
                })
            try:
                self.db_manager.cache.invalidate('conceptos_pago')
            except Exception:
                pass
            return new_id
        except psycopg2.errors.UniqueViolation:
            raise ValueError("Ya existe un concepto de pago con ese nombre.")
        except Exception:
            raise

    @database_retry(max_retries=3, base_delay=0.8)
    def actualizar_concepto_pago(self, concepto: ConceptoPago) -> bool:
        """Actualiza un concepto de pago existente por ID."""
        if not concepto.id:
            raise ValueError("Se requiere ID para actualizar el concepto de pago")
        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    UPDATE conceptos_pago
                    SET nombre = %s, descripcion = %s, precio_base = %s, tipo = %s, activo = %s
                    WHERE id = %s
                    """,
                    (concepto.nombre, concepto.descripcion, float(concepto.precio_base or 0.0), concepto.tipo, bool(concepto.activo), concepto.id)
                )
                updated = cursor.rowcount
                conn.commit()
            if hasattr(self.db_manager, 'audit_logger') and self.db_manager.audit_logger:
                self.db_manager.audit_logger.log_operation('UPDATE', 'conceptos_pago', concepto.id, None, {
                    'nombre': concepto.nombre,
                    'descripcion': concepto.descripcion,
                    'precio_base': concepto.precio_base,
                    'tipo': concepto.tipo,
                    'activo': concepto.activo,
                })
            try:
                self.db_manager.cache.invalidate('conceptos_pago')
            except Exception:
                pass
            return updated > 0
        except psycopg2.errors.UniqueViolation:
            raise ValueError("Ya existe un concepto de pago con ese nombre.")
        except Exception:
            raise

    @database_retry(max_retries=3, base_delay=0.8)
    def eliminar_concepto_pago(self, concepto_id: int) -> bool:
        """Elimina un concepto de pago por ID. Si está referenciado, puede fallar por restricciones de FK."""
        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM conceptos_pago WHERE id = %s", (concepto_id,))
                deleted = cursor.rowcount
                conn.commit()
            if hasattr(self.db_manager, 'audit_logger') and self.db_manager.audit_logger and deleted:
                self.db_manager.audit_logger.log_operation('DELETE', 'conceptos_pago', concepto_id, None, None)
            if deleted:
                try:
                    self.db_manager.cache.invalidate('conceptos_pago')
                except Exception:
                    pass
            return deleted > 0
        except psycopg2.errors.ForeignKeyViolation:
            raise ValueError("No se puede eliminar el concepto de pago porque está en uso.")
        except Exception:
            raise

    def asegurar_concepto_cuota_mensual(self) -> Optional[int]:
        """
        Asegura que exista el concepto 'Cuota Mensual' en la tabla conceptos_pago.
        Si no existe, lo crea con valores por defecto y devuelve su ID.
        Si ya existe, devuelve None.
        """
        try:
            conceptos = self.obtener_conceptos_pago(solo_activos=False) or []
            for c in conceptos:
                if c.nombre.strip().lower() == "cuota mensual":
                    # Activar si estaba inactivo (no crítico si falla)
                    if not c.activo:
                        try:
                            c.activo = True
                            self.actualizar_concepto_pago(c)
                        except Exception:
                            pass
                    return None
            nuevo = ConceptoPago(
                nombre="Cuota Mensual",
                descripcion="Pago mensual de cuota",
                precio_base=0.0,
                tipo="variable",
                activo=True,
                categoria="cuota",
            )
            return self.crear_concepto_pago(nuevo)
        except Exception as e:
            logging.warning(f"No se pudo asegurar concepto 'Cuota Mensual': {e}")
            return None

    # --- MÉTODOS DE INTEGRACIÓN WHATSAPP ---
    def _enviar_notificacion_pago_confirmado(self, usuario_id: int, monto: float, mes: int, año: int):
        """Envía notificación WhatsApp de confirmación de pago"""
        if not self.whatsapp_enabled:
            return
        
        try:
            usuario = self.db_manager.obtener_usuario(usuario_id)
            if not usuario or not usuario.telefono:
                logging.warning(f"Usuario {usuario_id} no tiene teléfono registrado")
                return
            
            # Verificar si ya se envió notificación reciente
            if self.message_logger.verificar_mensaje_enviado_reciente(
                usuario.telefono, 'payment', 24
            ):
                logging.info(f"Notificación de pago ya enviada recientemente a {usuario.telefono}")
                return
            
            # Preparar datos para el nuevo método de la documentación
            payment_data = {
                'user_id': usuario_id,
                'phone': usuario.telefono,
                'name': usuario.nombre,
                'amount': monto,
                'date': f"{mes:02d}/{año}"
            }
            
            # Enviar mensaje de confirmación usando el método de la documentación
            self.whatsapp_manager.send_payment_confirmation(payment_data)
            
            logging.info(f"Notificación de pago enviada a {usuario.nombre} ({usuario.telefono})")
            
        except Exception as e:
            logging.error(f"Error al enviar notificación de pago: {e}")

    # --- NUEVO: PROCESAMIENTO DE MOROSOS (fuera de whatsapp_manager) ---
    def procesar_usuarios_morosos(self) -> int:
        """Procesa usuarios morosos: envía recordatorios, incrementa cuotas vencidas y desactiva si corresponde."""
        try:
            usuarios_morosos = self.db_manager.obtener_usuarios_morosos()
            mensajes_enviados = 0

            for usuario in usuarios_morosos:
                try:
                    telefono = usuario.get('telefono')
                    if not telefono:
                        logging.warning(f"Usuario {usuario.get('id')} sin teléfono registrado")
                        continue

                    # Anti-spam: no enviar más de un mensaje por 30 días
                    if self.message_logger.verificar_mensaje_enviado_reciente(
                        telefono, "overdue", 24 * 30
                    ):
                        logging.info(f"Mensaje anti-spam bloqueado para usuario {usuario.get('id')}")
                        continue

                    # Preparar datos y enviar mensaje de cuota vencida
                    user_data = {
                        'user_id': usuario['id'],
                        'phone': telefono,
                        'name': usuario.get('nombre', ''),
                        'due_date': usuario.get('fecha_vencimiento', ''),
                        'amount': usuario.get('monto', 0) or 0,
                    }

                    if self.whatsapp_enabled and self.whatsapp_manager.send_overdue_payment_notification(user_data):
                        mensajes_enviados += 1
                        # Incrementar cuotas vencidas
                        self.db_manager.incrementar_cuotas_vencidas(usuario['id'])

                        # Desactivar si alcanza 3 cuotas vencidas
                        cuotas_vencidas = (usuario.get('cuotas_vencidas') or 0) + 1
                        if cuotas_vencidas >= 3:
                            self.db_manager.desactivar_usuario_por_cuotas_vencidas(usuario['id'])
                            # Enviar notificación de desactivación (plantilla correcta)
                            self.whatsapp_manager.enviar_notificacion_desactivacion(
                                usuario_id=usuario['id'],
                                motivo="3 cuotas vencidas",
                                force_send=True
                            )
                    else:
                        logging.error(f"Error al enviar recordatorio a {usuario.get('nombre', 'desconocido')}")

                except Exception as e:
                    logging.error(f"Error procesando usuario moroso {usuario.get('id')}: {e}")
                    continue

            logging.info(f"Proceso morosos completado: {mensajes_enviados} recordatorios enviados")
            return mensajes_enviados
        except Exception as e:
            logging.error(f"Error en procesar_usuarios_morosos (PaymentManager): {e}")
            return 0

    # --- NUEVO: RECORDATORIOS DE PRÓXIMOS VENCIMIENTOS ---
    def procesar_recordatorios_proximos_vencimientos(self, dias_anticipacion: int = 3) -> int:
        """Envía recordatorios a usuarios cuyas cuotas vencen pronto."""
        try:
            usuarios_por_vencer = self.db_manager.obtener_usuarios_con_cuotas_por_vencer(
                dias_anticipacion=dias_anticipacion
            )
            mensajes_enviados = 0

            for usuario in usuarios_por_vencer:
                try:
                    telefono = usuario.get('telefono')
                    if not telefono:
                        continue

                    # Anti-spam: un recordatorio por semana
                    if self.message_logger.verificar_mensaje_enviado_reciente(
                        telefono, "overdue", 24 * 7
                    ):
                        logging.info(f"Recordatorio bloqueado por anti-spam para usuario {usuario.get('id')}")
                        continue

                    user_data = {
                        'user_id': usuario['id'],
                        'phone': telefono,
                        'name': usuario.get('nombre', ''),
                        'due_date': usuario.get('fecha_vencimiento', ''),
                        'amount': usuario.get('monto', 0) or 0,
                    }

                    if self.whatsapp_enabled and self.whatsapp_manager.send_overdue_payment_notification(user_data):
                        mensajes_enviados += 1
                    else:
                        logging.error(f"Error al enviar recordatorio a {usuario.get('nombre', 'desconocido')}")

                except Exception as e:
                    logging.error(f"Error procesando usuario por vencer {usuario.get('id')}: {e}")
                    continue

            logging.info(f"Recordatorios de próximos vencimientos enviados: {mensajes_enviados}")
            return mensajes_enviados
        except Exception as e:
            logging.error(f"Error en procesar_recordatorios_proximos_vencimientos (PaymentManager): {e}")
            return 0
    
    def procesar_usuarios_morosos_whatsapp(self, enviar_recordatorios: bool = True) -> Dict[str, Any]:
        """Procesa usuarios morosos y opcionalmente envía recordatorios por WhatsApp"""
        try:
            usuarios_morosos = self.obtener_usuarios_morosos()
            
            resultado = {
                'total_morosos': len(usuarios_morosos),
                'notificaciones_enviadas': 0,
                'errores': 0,
                'usuarios_sin_telefono': 0,
                'usuarios_bloqueados': 0
            }
            
            if not self.whatsapp_enabled or not enviar_recordatorios:
                resultado['mensaje'] = "WhatsApp no habilitado o envío desactivado"
                return resultado
            
            for moroso in usuarios_morosos:
                usuario = moroso['usuario']
                periodo = moroso['periodo_pendiente']
                fecha_venc_str = moroso.get('fecha_vencimiento')
                
                try:
                    # Verificar si el usuario tiene teléfono
                    if not usuario.telefono:
                        resultado['usuarios_sin_telefono'] += 1
                        logging.warning(f"Usuario {usuario.nombre} no tiene teléfono registrado")
                        continue
                    
                    # Verificar si puede enviar mensaje (anti-spam)
                    if not self.message_logger.puede_enviar_mensaje(usuario.telefono, 'overdue'):
                        resultado['usuarios_bloqueados'] += 1
                        logging.warning(f"Usuario {usuario.telefono} bloqueado por anti-spam")
                        continue
                    
                    # Verificar si ya se envió recordatorio reciente
                    if self.message_logger.verificar_mensaje_enviado_reciente(
                        usuario.telefono, 'overdue', 72  # 3 días
                    ):
                        logging.info(f"Recordatorio ya enviado recientemente a {usuario.telefono}")
                        continue
                    
                    # Calcular días de atraso usando la fecha de vencimiento exacta si está disponible
                    hoy = datetime.now()
                    dias_atraso = 1
                    if fecha_venc_str:
                        try:
                            fecha_vencimiento = datetime.strptime(fecha_venc_str, '%d/%m/%Y')
                            dias_atraso = max((hoy - fecha_vencimiento).days, 1)
                        except Exception:
                            dias_atraso = 1
                    else:
                        # Fallback: calcular usando periodo (MM/YYYY)
                        try:
                            from calendar import monthrange
                            mes_vencido = int(periodo.split('/')[0])
                            año_vencido = int(periodo.split('/')[1])
                            ultimo_dia = monthrange(año_vencido, mes_vencido)[1]
                            fecha_vencimiento = datetime(año_vencido, mes_vencido, ultimo_dia)
                            dias_atraso = max((hoy - fecha_vencimiento).days, 1)
                        except Exception:
                            dias_atraso = 1
                    
                    # Enviar mensaje de recordatorio
                    self.whatsapp_manager.send_overdue_reminder({
                        'user_id': usuario.id,
                        'phone': usuario.telefono,
                        'name': usuario.nombre,
                        'period': periodo,
                        'days_overdue': dias_atraso
                    })
                    resultado['notificaciones_enviadas'] += 1
                except Exception as e:
                    resultado['errores'] += 1
                    logging.error(f"Error procesando moroso {usuario.id}: {e}")
            
            return resultado
        except Exception as e:
            logging.error(f"Error en procesamiento de morosos: {e}")
            return {
                'total_morosos': 0,
                'notificaciones_enviadas': 0,
                'errores': 1,
                'usuarios_sin_telefono': 0,
                'usuarios_bloqueados': 0,
                'mensaje': 'Error inesperado'
            }
    
    def enviar_mensaje_bienvenida_whatsapp(self, usuario_id: int) -> bool:
        """Envía mensaje de bienvenida por WhatsApp a un nuevo usuario"""
        if not self.whatsapp_enabled:
            return False
        
        try:
            usuario = self.db_manager.obtener_usuario(usuario_id)
            if not usuario or not usuario.telefono:
                logging.warning(f"Usuario {usuario_id} no tiene teléfono registrado")
                return False
            
            # Verificar si ya se envió mensaje de bienvenida
            if self.message_logger.verificar_mensaje_enviado_reciente(
                usuario.telefono, 'welcome', 24 * 7  # 1 semana
            ):
                logging.info(f"Mensaje de bienvenida ya enviado a {usuario.telefono}")
                return True
            
            # Preparar información de bienvenida
            bienvenida_info = {
                'fecha': datetime.now().strftime('%d/%m/%Y')
            }
            
            # Enviar mensaje de bienvenida
            return self.whatsapp_manager.enviar_mensaje_bienvenida(usuario_id)
            
        except Exception as e:
            logging.error(f"Error al enviar mensaje de bienvenida: {e}")
            return False
    
    def obtener_estadisticas_whatsapp(self) -> Dict[str, Any]:
        """Obtiene estadísticas del sistema WhatsApp"""
        if not self.whatsapp_enabled:
            return {'error': 'Sistema WhatsApp no habilitado'}
        
        try:
            # Estadísticas diarias
            stats_diarias = self.message_logger.obtener_estadisticas_diarias()
            
            # Estadísticas semanales
            stats_semanales = self.message_logger.obtener_estadisticas_semanales()
            
            # Estadísticas por tipo de mensaje
            stats_por_tipo = self.message_logger.obtener_estadisticas_por_tipo(7)
            
            # Usuarios bloqueados
            usuarios_bloqueados = self.message_logger.obtener_usuarios_bloqueados()
            
            return {
                'sistema_habilitado': True,
                'estadisticas_diarias': stats_diarias,
                'estadisticas_semanales': stats_semanales,
                'estadisticas_por_tipo': stats_por_tipo,
                'usuarios_bloqueados': len(usuarios_bloqueados),
                'lista_usuarios_bloqueados': usuarios_bloqueados
            }
            
        except Exception as e:
            logging.error(f"Error al obtener estadísticas WhatsApp: {e}")
            return {'error': f'Error al obtener estadísticas: {str(e)}'}
    
    def configurar_whatsapp(self, configuracion: Dict[str, str]) -> bool:
        """Configura el sistema WhatsApp"""
        if not WHATSAPP_AVAILABLE:
            return False
        
        try:
            # Mapear claves específicas de WhatsApp a la tabla dedicada
            phone_id = None
            waba_id = None
            access_token = None

            if 'phone_number_id' in configuracion:
                phone_id = str(configuracion.get('phone_number_id') or '').strip() or None
            if 'whatsapp_business_account_id' in configuracion:
                waba_id = str(configuracion.get('whatsapp_business_account_id') or '').strip() or None
            if 'access_token' in configuracion:
                access_token = str(configuracion.get('access_token') or '').strip() or None

            if phone_id is not None or waba_id is not None or access_token is not None:
                self.db_manager.actualizar_configuracion_whatsapp(phone_id=phone_id, waba_id=waba_id, access_token=access_token)

            # Otras preferencias se guardan en la tabla genérica `configuracion`
            for k in ('allowlist_numbers', 'allowlist_enabled', 'enable_webhook', 'max_retries', 'retry_delay_seconds'):
                if k in configuracion:
                    val = configuracion.get(k)
                    # Normalizar a cadena
                    try:
                        val_str = str(val)
                    except Exception:
                        val_str = ''
                    self.db_manager.actualizar_configuracion(k, val_str)
            
            # Reinicializar WhatsApp manager si es necesario
            if self.whatsapp_manager:
                # Recargar configuración y verificar
                try:
                    self.whatsapp_manager.reinicializar_configuracion()
                except Exception:
                    pass
                self.whatsapp_enabled = self.whatsapp_manager.verificar_configuracion()
            
            return self.whatsapp_enabled
            
        except Exception as e:
            logging.error(f"Error al configurar WhatsApp: {e}")
            return False
    
    def iniciar_servidor_whatsapp(self) -> bool:
        """Inicia el servidor webhook de WhatsApp"""
        if not self.whatsapp_enabled:
            return False
        
        try:
            return self.whatsapp_manager.iniciar_servidor()
        except Exception as e:
            logging.error(f"Error al iniciar servidor WhatsApp: {e}")
            return False
    
    def detener_servidor_whatsapp(self) -> bool:
        """Detiene el servidor webhook de WhatsApp"""
        if not self.whatsapp_enabled:
            return False
        
        try:
            return self.whatsapp_manager.detener_servidor()
        except Exception as e:
            logging.error(f"Error al detener servidor WhatsApp: {e}")
            return False
    
    def obtener_estado_whatsapp(self) -> Dict[str, Any]:
        """Obtiene el estado actual del sistema WhatsApp"""
        try:
            cfg_full: Dict[str, Any] = {}
            try:
                cfg_full = self.db_manager.obtener_configuracion_whatsapp_completa() or {}
            except Exception:
                cfg_full = {}

            # Mapear a las claves esperadas por la UI
            config_ui = {
                'phone_number_id': cfg_full.get('phone_id') or '',
                'whatsapp_business_account_id': cfg_full.get('waba_id') or '',
                # Nota: el token puede no estar presente si se gestiona por entorno seguro
                'access_token': cfg_full.get('access_token') or '',
                'allowlist_numbers': cfg_full.get('allowlist_numbers') or '',
                'allowlist_enabled': cfg_full.get('allowlist_enabled'),
                'enable_webhook': cfg_full.get('enable_webhook'),
                'max_retries': cfg_full.get('max_retries'),
                'retry_delay_seconds': cfg_full.get('retry_delay_seconds'),
            }

            return {
                'disponible': WHATSAPP_AVAILABLE,
                'habilitado': self.whatsapp_enabled,
                'servidor_activo': self.whatsapp_manager.servidor_activo if self.whatsapp_manager else False,
                'configuracion_valida': self.whatsapp_manager.verificar_configuracion() if self.whatsapp_manager else False,
                'config': config_ui,
            }
        except Exception:
            return {
                'disponible': WHATSAPP_AVAILABLE,
                'habilitado': self.whatsapp_enabled,
                'servidor_activo': self.whatsapp_manager.servidor_activo if self.whatsapp_manager else False,
                'configuracion_valida': self.whatsapp_manager.verificar_configuracion() if self.whatsapp_manager else False
            }
    def calcular_total_con_comision(self, subtotal: float, metodo_pago_id: Optional[int]) -> Dict[str, float]:
        """Devuelve subtotal, comisión y total aplicando la comisión del método de pago.
        
        Args:
            subtotal (float): Importe base sobre el cual calcular la comisión.
            metodo_pago_id (Optional[int]): ID del método de pago (usa metodos_pago.comision).
        
        Returns:
            Dict[str, float]: Diccionario con claves 'subtotal', 'comision' y 'total'.
        """
        try:
            base = float(subtotal or 0.0)
        except Exception:
            base = 0.0
        comision = self.calcular_comision(base, metodo_pago_id)
        total = round(base + comision, 2)
        return {
            'subtotal': round(base, 2),
            'comision': comision,
            'total': total
        }

    def obtener_ultimo_pago_usuario(self, usuario_id: int) -> Optional[Pago]:
        """Obtiene el último pago (por fecha_pago) de un usuario como objeto Pago."""
        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cursor.execute(
                    """
                    SELECT id, usuario_id, monto, mes, año, fecha_pago, metodo_pago_id FROM pagos
                    WHERE usuario_id = %s
                    ORDER BY fecha_pago DESC
                    LIMIT 1
                    """,
                    (usuario_id,)
                )
                row = cursor.fetchone()
                return self._crear_pago_desde_fila(row) if row else None
        except Exception:
            return None

    def obtener_ingresos_ultimos_12_meses(self) -> Dict[str, float]:
        """Retorna un diccionario con los ingresos por mes para los últimos 12 meses.
        Claves en formato 'Mes Año' (ej: 'Ene 2024') y valores como montos float.
        """
        try:
            # Inicializar con los últimos 12 meses en 0 para asegurar continuidad en gráficos
            from datetime import date
            meses_nombres = ['Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun', 'Jul', 'Ago', 'Sep', 'Oct', 'Nov', 'Dic']
            hoy = date.today()
            # Construir claves por defecto
            resultados: Dict[str, float] = {}
            for i in range(11, -1, -1):
                # retroceder i meses
                year = hoy.year
                month = hoy.month - i
                # ajustar año/mes
                while month <= 0:
                    month += 12
                    year -= 1
                clave = f"{meses_nombres[month - 1]} {year}"
                resultados[clave] = 0.0

            # Consultar sumas reales desde la base de datos
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cursor.execute(
                    """
                    SELECT 
                        EXTRACT(YEAR FROM fecha_pago) as año,
                        EXTRACT(MONTH FROM fecha_pago) as mes,
                        SUM(monto) as total
                    FROM pagos 
                    WHERE fecha_pago >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '11 months')
                    GROUP BY EXTRACT(YEAR FROM fecha_pago), EXTRACT(MONTH FROM fecha_pago)
                    ORDER BY año, mes
                    """
                )
                rows = cursor.fetchall() or []
                for row in rows:
                    año = int(row['año']) if row.get('año') is not None else hoy.year
                    mes = int(row['mes']) if row.get('mes') is not None else hoy.month
                    total = float(row['total'] or 0.0)
                    clave = f"{meses_nombres[mes - 1]} {año}"
                    resultados[clave] = total
            return resultados
        except Exception:
            return {}
