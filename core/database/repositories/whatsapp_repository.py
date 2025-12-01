from datetime import datetime, date, timedelta
from typing import List, Optional, Dict, Set, Tuple, Any
import json
import logging
import psycopg2
import psycopg2.extras
from .base import BaseRepository
from ..connection import database_retry

class WhatsappRepository(BaseRepository):
    pass

    # --- Methods moved from DatabaseManager ---

    def marcar_notificacion_leida(self, notificacion_id: int) -> bool:
        """Marca una notificación como leída PostgreSQL"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            sql = """
            UPDATE profesor_notificaciones 
            SET leida = TRUE, fecha_lectura = CURRENT_TIMESTAMP 
            WHERE id = %s
            """
            cursor.execute(sql, (notificacion_id,))
            conn.commit()
            return cursor.rowcount > 0
    

    def obtener_count_notificaciones_no_leidas(self, profesor_id: int) -> int:
        """Obtiene el número de notificaciones no leídas de un profesor PostgreSQL"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            sql = "SELECT COUNT(*) FROM profesor_notificaciones WHERE profesor_id = %s AND leida = FALSE"
            cursor.execute(sql, (profesor_id,))
            result = cursor.fetchone()
            # RealDictCursor devuelve la columna como 'count'
            return result['count'] if result and 'count' in result else (result[0] if result else 0)
    
    # === MÉTODOS PARA HORAS TRABAJADAS DE PROFESORES ===
    

    def crear_notificacion_cupo_completa(self, usuario_id: int, clase_horario_id: int, 
                               tipo_notificacion: str, mensaje: str) -> int:
        """Crea una notificación de cupo para un usuario PostgreSQL."""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            sql = """
            INSERT INTO notificaciones_cupos 
            (usuario_id, clase_horario_id, tipo_notificacion, mensaje)
            VALUES (%s, %s, %s, %s)
            RETURNING id
            """
            cursor.execute(sql, (usuario_id, clase_horario_id, tipo_notificacion, mensaje))
            result = cursor.fetchone()
            conn.commit()
            # Acceso por clave con RealDictCursor
            return result['id'] if result else None
    

    def marcar_notificacion_leida_completa(self, notificacion_id: int) -> bool:
        """Marca una notificación como leída PostgreSQL."""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            sql = """
            UPDATE notificaciones_cupos 
            SET leida = true, fecha_lectura = CURRENT_TIMESTAMP 
            WHERE id = %s
            """
            cursor.execute(sql, (notificacion_id,))
            conn.commit()
            return cursor.rowcount > 0
    

    def obtener_count_notificaciones_no_leidas_completo(self, usuario_id: int) -> int:
        """Obtiene el número de notificaciones no leídas de un usuario PostgreSQL."""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            sql = """
            SELECT COUNT(*) as count
            FROM notificaciones_cupos 
            WHERE usuario_id = %s AND leida = false AND activa = true
            """
            cursor.execute(sql, (usuario_id,))
            result = cursor.fetchone()
            return result['count'] if result else 0
    
    # --- MÉTODOS PARA POLÍTICAS DE CANCELACIÓN ---
    

    def _crear_tablas_whatsapp(self):
        """Crea las tablas necesarias para el sistema de mensajería WhatsApp"""
        try:
            with self.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    # Tabla de mensajes WhatsApp
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS whatsapp_messages (
                            id SERIAL PRIMARY KEY,
                            user_id INTEGER REFERENCES usuarios(id),
                            message_type VARCHAR(50) NOT NULL CHECK (message_type IN ('overdue','payment','welcome','deactivation','class_reminder','waitlist')),
                            template_name VARCHAR(255) NOT NULL,
                            phone_number VARCHAR(20) NOT NULL,
                            message_id VARCHAR(100),
                            sent_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                            status VARCHAR(20) DEFAULT 'sent' CHECK (status IN ('sent', 'delivered', 'read', 'failed', 'received')),
                            message_content TEXT,
                            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                        )
                    """)
                    
                    # Índices para optimizar consultas
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_whatsapp_messages_user_id ON whatsapp_messages(user_id)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_whatsapp_messages_type_date ON whatsapp_messages(message_type, sent_at DESC)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_whatsapp_messages_phone ON whatsapp_messages(phone_number)")
                    try:
                        cursor.execute("ALTER TABLE whatsapp_messages ADD COLUMN IF NOT EXISTS message_id VARCHAR(100)")
                    except Exception:
                        pass
                    try:
                        cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_whatsapp_messages_message_id ON whatsapp_messages(message_id)")
                    except Exception:
                        pass
                    try:
                        cursor.execute("""
                            DO $$
                            BEGIN
                                IF EXISTS (
                                    SELECT 1 FROM information_schema.table_constraints
                                    WHERE table_name = 'whatsapp_messages'
                                      AND constraint_name = 'whatsapp_messages_status_check'
                                ) THEN
                                    ALTER TABLE whatsapp_messages DROP CONSTRAINT whatsapp_messages_status_check;
                                END IF;
                            END $$;
                        """)
                        cursor.execute("ALTER TABLE whatsapp_messages ADD CONSTRAINT whatsapp_messages_status_check CHECK (status IN ('sent','delivered','read','failed','received'))")
                    except Exception:
                        pass

                    # Asegurar restricción de tipos de mensaje con valores actualizados
                    try:
                        cursor.execute(
                            """
                            DO $$
                            DECLARE r RECORD;
                            BEGIN
                                FOR r IN
                                    SELECT conname
                                    FROM pg_constraint
                                    WHERE contype = 'c'
                                      AND conrelid = 'whatsapp_messages'::regclass
                                      AND pg_get_constraintdef(oid) LIKE '%message_type%'
                                LOOP
                                    EXECUTE format('ALTER TABLE whatsapp_messages DROP CONSTRAINT %I', r.conname);
                                END LOOP;
                                BEGIN
                                    ALTER TABLE whatsapp_messages ADD CONSTRAINT whatsapp_messages_type_check CHECK (message_type IN ('overdue','payment','welcome','deactivation','class_reminder','waitlist'));
                                EXCEPTION WHEN others THEN
                                    -- Si la restricción ya existe o falla por algún motivo, continuar sin interrumpir
                                    NULL;
                                END;
                            END $$;
                            """
                        )
                    except Exception:
                        pass
                    
                    # Tabla de plantillas WhatsApp
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS whatsapp_templates (
                            id SERIAL PRIMARY KEY,
                            template_name VARCHAR(255) UNIQUE NOT NULL,
                            header_text VARCHAR(60),
                            body_text TEXT NOT NULL,
                            variables JSONB,
                            active BOOLEAN DEFAULT TRUE,
                            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                        )
                    """)
                    
                    # Tabla de configuración WhatsApp
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS whatsapp_config (
                            id SERIAL PRIMARY KEY,
                            phone_id VARCHAR(50) NOT NULL,
                            waba_id VARCHAR(50) NOT NULL,
                            access_token TEXT,
                            active BOOLEAN DEFAULT TRUE,
                            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                        )
                    """)
                    
                    # Insertar plantillas predefinidas
                    cursor.execute("""
                        INSERT INTO whatsapp_templates (template_name, header_text, body_text, variables) VALUES
                        ('aviso_de_vencimiento_de_cuota_gimnasio_para_usuario_especifico_en_sistema_de_management_de_gimnasios_profesional', 
                         'Vencimiento de cuota', 
                         'Hola {{1}}, recordá que tu cuota vence el {{2}}. El monto actual de tu cuota es de $ {{3}}. Saludos!',
                         '{"1": "nombre_completo", "2": "fecha_vencimiento", "3": "monto_cuota"}'),
                        ('aviso_de_confirmacion_de_pago_de_cuota_gimnasio_para_usuario_especifico_en_sistema_de_management_de_gimnasios_profesional',
                         'Confirmación de Pago',
                         'Información de pago:\nNombre: {{1}}\nMonto: $ {{2}}\nFecha {{3}}\n\nSaludos!',
                         '{"1": "nombre_completo", "2": "monto_cuota", "3": "fecha_pago"}'),
                        ('mensaje_de_bienvenida_a_gimnasio_para_usuario_especifico_en_sistema_de_management_de_gimnasios_profesional',
                         '',
                         'Hola {{1}}. Bienvenido al {{2}} !\n\nSi recibiste este mensaje por error, contactate por este mismo medio.',
                         '{"1": "nombre_completo", "2": "nombre_gimnasio"}'),
                        ('aviso_de_promocion_a_lista_principal_para_clase_para_usuario_especifico_en_sistema_de_management_de_gimnasios_profesional',
                         'Promoción a clase',
                         'Hola {{1}}, Fuiste promovido desde la lista de espera a la clase de {{2}} del {{3}} a las {{4}}. Te esperamos!',
                         '{"1": "nombre_completo", "2": "tipo_clase", "3": "fecha", "4": "hora"}'),
                        ('aviso_de_promocion_de_lista_de_espera_para_clase_para_usuario_especifico_en_sistema_de_management_de_gimnasios_profesional',
                         'Cupo disponible',
                         'Hola {{1}}, se liberó un cupo para la clase de {{2}} del {{3}} a las {{4}}. ¿Querés confirmar tu inscripción?',
                         '{"1": "nombre_completo", "2": "tipo_clase", "3": "fecha", "4": "hora"}'),
                        ('aviso_de_recordatorio_de_horario_de_clase_para_usuario_especifico_en_sistema_de_management_de_gimnasios_profesional',
                         'Recordatorio de clase',
                         'Hola {{1}}, te recordamos tu clase de {{2}} el {{3}} a las {{4}}.',
                         '{"1": "nombre_completo", "2": "tipo_clase", "3": "fecha", "4": "hora"}'),
                        ('aviso_de_desactivacion_por_falta_de_pago_para_usuario_especifico_en_sistema_de_management_de_gimnasios_profesional',
                         'Desactivación',
                         'Hola {{1}}, tu cuenta será desactivada el {{2}} por {{3}}.',
                         '{"1": "nombre_completo", "2": "fecha_desactivacion", "3": "motivo"}')
                        ON CONFLICT (template_name) DO NOTHING
                    """)
                    
                    
                    
                    conn.commit()
                    logging.info("Tablas de WhatsApp creadas exitosamente")
                    
        except Exception as e:
            logging.error(f"Error creando tablas de WhatsApp: {e}")
            raise
    

    def registrar_mensaje_whatsapp(self, user_id: int, message_type: str, template_name: str, 
                                 phone_number: str, message_content: str = None, status: str = 'sent', message_id: str = None) -> bool:
        """Registra un mensaje WhatsApp enviado"""
        try:
            with self.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO whatsapp_messages 
                        (user_id, message_type, template_name, phone_number, message_content, status, message_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (user_id, message_type, template_name, phone_number, message_content, status, message_id))
                    
                    conn.commit()
                    return True
                    
        except Exception as e:
            logging.error(f"Error registrando mensaje WhatsApp: {e}")
            return False
    

    def obtener_plantilla_whatsapp(self, template_name: str) -> Optional[Dict]:
        """Obtiene una plantilla de WhatsApp por nombre"""
        try:
            with self.get_connection_context() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT 
                            id, template_name, header_text, body_text, variables, active, created_at
                        FROM whatsapp_templates 
                        WHERE template_name = %s AND active = TRUE
                    """, (template_name,))
                    
                    result = cursor.fetchone()
                    return dict(result) if result else None
                    
        except Exception as e:
            logging.error(f"Error obteniendo plantilla WhatsApp: {e}")
            return None
    

    def obtener_plantillas_whatsapp(self, activas_solo: bool = True) -> List[Dict]:
        """Obtiene todas las plantillas de WhatsApp"""
        try:
            with self.get_connection_context() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    if activas_solo:
                        cursor.execute("""
                            SELECT 
                                id, template_name, header_text, body_text, variables, active, created_at
                            FROM whatsapp_templates 
                            WHERE active = TRUE
                            ORDER BY template_name
                        """)
                    else:
                        cursor.execute("""
                            SELECT 
                                id, template_name, header_text, body_text, variables, active, created_at
                            FROM whatsapp_templates 
                            ORDER BY template_name
                        """)
                    
                    return [dict(row) for row in cursor.fetchall()]
                    
        except Exception as e:
            logging.error(f"Error obteniendo plantillas WhatsApp: {e}")
            return []
    

    def verificar_mensaje_enviado_reciente(self, user_id: int, message_type: str, 
                                         horas_limite: int = 24) -> bool:
        """Verifica si ya se envió un mensaje del mismo tipo recientemente (control anti-spam)"""
        try:
            with self.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT COUNT(*) FROM whatsapp_messages 
                        WHERE user_id = %s 
                        AND message_type = %s 
                        AND sent_at > NOW() - INTERVAL '%s hours'
                        AND status != 'failed'
                    """, (user_id, message_type, horas_limite))
                    
                    count = cursor.fetchone()[0]
                    return count > 0
                    
        except Exception as e:
            logging.error(f"Error verificando mensaje reciente: {e}")
            return False
    

    def obtener_historial_mensajes_whatsapp(self, user_id: int = None, 
                                           message_type: str = None, 
                                           limit: int = 100) -> List[Dict]:
        """Obtiene el historial de mensajes WhatsApp"""
        try:
            with self.get_connection_context() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    query = """
                        SELECT 
                            wm.id, wm.user_id, wm.message_type, wm.template_name, wm.phone_number,
                            wm.message_id, wm.sent_at, wm.status, wm.message_content, wm.created_at,
                            u.nombre AS usuario_nombre
                        FROM whatsapp_messages wm
                        LEFT JOIN usuarios u ON wm.user_id = u.id
                        WHERE 1=1
                    """
                    params = []
                    
                    if user_id:
                        query += " AND wm.user_id = %s"
                        params.append(user_id)
                    
                    if message_type:
                        query += " AND wm.message_type = %s"
                        params.append(message_type)
                    
                    query += " ORDER BY wm.sent_at DESC LIMIT %s"
                    params.append(limit)
                    
                    cursor.execute(query, params)
                    return [dict(row) for row in cursor.fetchall()]
                    
        except Exception as e:
            logging.error(f"Error obteniendo historial de mensajes WhatsApp: {e}")
            return []
    

    def _enviar_mensaje_bienvenida_automatico(self, usuario_id: int, nombre: str, telefono: str):
        """Envía mensaje de bienvenida automático cuando se crea un usuario"""
        try:
            # Solo enviar si el usuario tiene teléfono
            if not telefono or telefono.strip() == '':
                return
            
            # Importar WhatsAppManager de forma lazy para evitar dependencias circulares
            try:
                from .whatsapp_manager import WhatsAppManager
                
                # Crear instancia temporal de WhatsApp Manager
                wm = WhatsAppManager(self)
                
                # Enviar mensaje de bienvenida usando el método correcto
                wm.enviar_mensaje_bienvenida(usuario_id)
                logging.info(f"Mensaje de bienvenida enviado automáticamente a {nombre} ({telefono})")
                
            except ImportError:
                logging.warning("WhatsApp Manager no disponible para mensaje de bienvenida automático")
            except Exception as e:
                logging.error(f"Error enviando mensaje de bienvenida automático: {e}")
                
        except Exception as e:
            logging.error(f"Error en _enviar_mensaje_bienvenida_automatico: {e}")
    

    def obtener_estadisticas_whatsapp(self) -> Dict:
        try:
            with self.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    # Mensajes por tipo
                    cursor.execute("""
                        SELECT message_type, COUNT(*) as count
                        FROM whatsapp_messages
                        GROUP BY message_type
                    """)
                    mensajes_por_tipo = {row[0]: row[1] for row in cursor.fetchall()}
                    
                    # Mensajes por estado
                    cursor.execute("""
                        SELECT status, COUNT(*) as count
                        FROM whatsapp_messages
                        GROUP BY status
                    """)
                    mensajes_por_estado = {row[0]: row[1] for row in cursor.fetchall()}
                    
                    # Mensajes del último mes
                    cursor.execute("""
                        SELECT COUNT(*) FROM whatsapp_messages
                        WHERE sent_at > NOW() - INTERVAL '30 days'
                    """)
                    mensajes_ultimo_mes = cursor.fetchone()[0]
                    
                    # Total de mensajes
                    cursor.execute("SELECT COUNT(*) FROM whatsapp_messages")
                    total_mensajes = cursor.fetchone()[0]
                    
                    return {
                        'total_mensajes': total_mensajes,
                        'mensajes_ultimo_mes': mensajes_ultimo_mes,
                        'mensajes_por_tipo': mensajes_por_tipo,
                        'mensajes_por_estado': mensajes_por_estado
                    }
                    
        except Exception as e:
            logging.error(f"Error obteniendo estadísticas WhatsApp: {e}")
            return {}
    

    def limpiar_mensajes_antiguos_whatsapp(self, dias_antiguedad: int = 90) -> int:
        """Limpia mensajes WhatsApp antiguos para mantener la base de datos optimizada"""
        try:
            with self.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        DELETE FROM whatsapp_messages
                        WHERE sent_at < NOW() - INTERVAL '%s days'
                    """, (dias_antiguedad,))
                    
                    deleted_count = cursor.rowcount
                    conn.commit()
                    
                    logging.info(f"Eliminados {deleted_count} mensajes WhatsApp antiguos")
                    return deleted_count
                    
        except Exception as e:
            logging.error(f"Error limpiando mensajes antiguos WhatsApp: {e}")
            return 0
    

    def contar_mensajes_whatsapp_periodo(self, user_id: int = None, telefono: str = None, 
                                       start_time: datetime = None, end_time: datetime = None,
                                       fecha_desde: datetime = None, fecha_hasta: datetime = None,
                                       direccion: str = None, tipo_mensaje: str = None, estado: str = None) -> int:
        """Cuenta los mensajes WhatsApp enviados en un período específico"""
        try:
            # Compatibilidad con parámetros antiguos
            if fecha_desde and not start_time:
                start_time = fecha_desde
            if fecha_hasta and not end_time:
                end_time = fecha_hasta
            if not end_time:
                end_time = datetime.now()
                
            # Obtener user_id si se proporciona teléfono
            if telefono and not user_id:
                user_id = self._obtener_user_id_por_telefono_whatsapp(telefono)
                if not user_id:
                    return 0
            
            with self.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    query = "SELECT COUNT(*) FROM whatsapp_messages WHERE 1=1"
                    params = []
                    
                    if user_id:
                        query += " AND user_id = %s"
                        params.append(user_id)
                    
                    if start_time:
                        query += " AND sent_at >= %s"
                        params.append(start_time)
                    
                    if end_time:
                        query += " AND sent_at <= %s"
                        params.append(end_time)
                    
                    # Manejo de direccion y estado
                    if direccion == 'enviado' and not estado:
                        query += " AND status != 'failed'"
                    elif direccion == 'recibido' and not estado:
                        query += " AND status = 'received'"
                    
                    if estado:
                        if estado == 'fallido':
                            query += " AND status = 'failed'"
                        else:
                            query += " AND status = %s"
                            params.append(estado)
                    
                    if tipo_mensaje:
                        query += " AND message_type = %s"
                        params.append(tipo_mensaje)
                    
                    cursor.execute(query, params)
                    count = cursor.fetchone()[0]
                    return count
                    
        except Exception as e:
            logging.error(f"Error contando mensajes WhatsApp en período: {e}")
            return 0
    

    def obtener_ultimo_mensaje_whatsapp(self, user_id: int = None, telefono: str = None, 
                                      message_type: str = None, direccion: str = None) -> Optional[Dict]:
        """Obtiene el último mensaje WhatsApp enviado, opcionalmente filtrado por tipo"""
        try:
            # Obtener user_id si se proporciona teléfono
            if telefono and not user_id:
                user_id = self._obtener_user_id_por_telefono_whatsapp(telefono)
                if not user_id:
                    return None
            
            with self.get_connection_context() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    query = "SELECT *, sent_at as fecha_envio FROM whatsapp_messages WHERE 1=1"
                    params = []
                    
                    if user_id:
                        query += " AND user_id = %s"
                        params.append(user_id)
                    
                    if message_type:
                        query += " AND message_type = %s"
                        params.append(message_type)
                    
                    if direccion == 'enviado':
                        query += " AND status != 'failed'"
                    
                    query += " ORDER BY sent_at DESC LIMIT 1"
                    
                    cursor.execute(query, params)
                    result = cursor.fetchone()
                    return dict(result) if result else None
                    
        except Exception as e:
            logging.error(f"Error obteniendo último mensaje WhatsApp: {e}")
            return None


    def obtener_mensaje_whatsapp_por_pk(self, user_id: int, pk_id: int) -> Optional[Dict]:
        """Obtiene un mensaje de WhatsApp por clave primaria y usuario."""
        try:
            with self.get_connection_context() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    cursor.execute(
                        """
                        SELECT id, user_id, message_type, template_name, phone_number, message_id,
                               sent_at, status, message_content, created_at
                        FROM whatsapp_messages
                        WHERE id = %s AND user_id = %s
                        """,
                        (int(pk_id), int(user_id))
                    )
                    row = cursor.fetchone()
                    return dict(row) if row else None
        except Exception as e:
            logging.error(f"Error obteniendo mensaje WhatsApp por pk id={pk_id} user_id={user_id}: {e}")
            return None


    def obtener_mensaje_whatsapp_por_message_id(self, user_id: int, message_id: str) -> Optional[Dict]:
        """Obtiene un mensaje de WhatsApp por message_id y usuario."""
        try:
            with self.get_connection_context() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    cursor.execute(
                        """
                        SELECT id, user_id, message_type, template_name, phone_number, message_id,
                               sent_at, status, message_content, created_at
                        FROM whatsapp_messages
                        WHERE message_id = %s AND user_id = %s
                        """,
                        (str(message_id), int(user_id))
                    )
                    row = cursor.fetchone()
                    return dict(row) if row else None
        except Exception as e:
            logging.error(f"Error obteniendo mensaje WhatsApp por message_id={message_id} user_id={user_id}: {e}")
            return None
    

    def obtener_telefonos_con_mensajes_fallidos(self, fecha_limite: datetime) -> List[str]:
        """Obtiene teléfonos con mensajes fallidos desde fecha límite"""
        try:
            with self.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """SELECT DISTINCT wm.phone_number 
                           FROM whatsapp_messages wm
                           WHERE wm.status = 'failed' AND wm.sent_at >= %s""",
                        (fecha_limite,)
                    )
                    return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logging.error(f"Error al obtener teléfonos con mensajes fallidos: {e}")
            return []
    

    def actualizar_estado_mensaje_whatsapp(self, message_id: str, nuevo_estado: str) -> bool:
        """Actualiza el estado de un mensaje de WhatsApp"""
        try:
            with self.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "UPDATE whatsapp_messages SET status = %s WHERE message_id = %s",
                        (nuevo_estado, message_id)
                    )
                    return cursor.rowcount > 0
        except Exception as e:
            logging.error(f"Error al actualizar estado del mensaje {message_id}: {e}")
            return False


    def eliminar_mensaje_whatsapp_por_pk(self, user_id: int, pk_id: int) -> bool:
        """Elimina un mensaje de WhatsApp por ID interno y usuario."""
        try:
            with self.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "DELETE FROM whatsapp_messages WHERE id = %s AND user_id = %s",
                        (pk_id, user_id)
                    )
                    try:
                        conn.commit()
                    except Exception:
                        pass
                    return cursor.rowcount > 0
        except Exception as e:
            logging.error(f"Error al eliminar mensaje WhatsApp id={pk_id} user_id={user_id}: {e}")
            return False


    def eliminar_mensaje_whatsapp_por_message_id(self, user_id: int, message_id: str) -> bool:
        """Elimina un mensaje de WhatsApp por message_id y usuario."""
        try:
            with self.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "DELETE FROM whatsapp_messages WHERE message_id = %s AND user_id = %s",
                        (str(message_id), int(user_id))
                    )
                    try:
                        conn.commit()
                    except Exception:
                        pass
                    return cursor.rowcount > 0
        except Exception as e:
            logging.error(f"Error al eliminar mensaje WhatsApp message_id={message_id} user_id={user_id}: {e}")
            return False

