import os
import tempfile
from datetime import datetime
from .models import Pago, Usuario, Rutina, PagoDetalle
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_RIGHT, TA_CENTER, TA_LEFT
from typing import Optional, List, Dict
from .utils import get_gym_name

class PDFGenerator:
    def __init__(self, branding_config=None):
        # Directorios de salida preferidos (permiten override por variable de entorno)
        pref_recibos = os.environ.get("RECEIPTS_DIR", "recibos")
        pref_rutinas = os.environ.get("RUTINAS_DIR", "rutinas_exportadas")

        def ensure_writable_dir(dir_path: str, fallback_subdir: str) -> str:
            try:
                os.makedirs(dir_path, exist_ok=True)
                return dir_path
            except Exception:
                # En entornos de despliegue con FS de solo lectura, usar directorio temporal
                tmp_dir = os.path.join(tempfile.gettempdir(), fallback_subdir)
                try:
                    os.makedirs(tmp_dir, exist_ok=True)
                except Exception:
                    # Si también falla, devolver el directorio temporal base sin crear subcarpeta
                    tmp_dir = tempfile.gettempdir()
                return tmp_dir

        self.output_dir_recibos = ensure_writable_dir(pref_recibos, "recibos")
        self.output_dir_rutinas = ensure_writable_dir(pref_rutinas, "rutinas_exportadas")
        
        # Usar logo del sistema de branding si está disponible
        self.branding_config = branding_config or {}
        self.logo_path = self.branding_config.get('main_logo_path') or os.path.join("assets", "gym_logo.png")
        # Usar gym_name del branding si está, sino cargarlo del sistema, con fallback a "Gimnasio"
        self.gym_name = self.branding_config.get('gym_name') or get_gym_name('Gimnasio')
        self.gym_address = self.branding_config.get('gym_address', 'Saavedra 2343, Santa Fe')
    
    def _get_dynamic_color(self, color_key, fallback_color):
        """Obtiene un color del sistema de branding dinámico o usa el fallback"""
        try:
            if self.branding_config:
                return self.branding_config.get(color_key, fallback_color)
        except:
            pass
        return fallback_color 

    def generar_recibo(self, pago: Pago, usuario: Usuario, numero_comprobante: str = None, detalles: Optional[List[PagoDetalle]] = None, totales: Optional[Dict[str, float]] = None, observaciones: Optional[str] = None, emitido_por: Optional[str] = None,
                       titulo: Optional[str] = None, gym_name: Optional[str] = None, gym_address: Optional[str] = None, fecha_emision: Optional[str] = None,
                       metodo_pago: Optional[str] = None, usuario_nombre: Optional[str] = None, usuario_dni: Optional[str] = None,
                       detalles_override: Optional[List[Dict]] = None, mostrar_logo: Optional[bool] = True, mostrar_metodo: Optional[bool] = True, mostrar_dni: Optional[bool] = True,
                       tipo_cuota: Optional[str] = None, periodo: Optional[str] = None) -> str:
        fecha_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"recibo_{pago.id}_{fecha_str}.pdf"
        filepath = os.path.join(self.output_dir_recibos, filename)

        doc = SimpleDocTemplate(
            filepath,
            pagesize=letter,
            rightMargin=inch/2,
            leftMargin=inch/2,
            topMargin=inch/2,
            bottomMargin=inch/2,
        )
        styles = getSampleStyleSheet()
        elements = []

        # Colores consistentes con el branding
        header_bg_color = colors.HexColor(self._get_dynamic_color('alt_background_color', '#434C5E'))
        body_bg_color = colors.HexColor(self._get_dynamic_color('table_body_color', '#D8DEE9'))

        # Header (titulo, logo y bloque de número/fecha integrado)
        header_text = (titulo or 'RECIBO DE PAGO')
        # Usar número de comprobante si está disponible, sino usar ID del pago
        recibo_numero = numero_comprobante if numero_comprobante else str(pago.id)
        # Fecha de emisión (override si viene, sino usar fecha del pago)
        try:
            fecha_str_disp = (fecha_emision or pago.fecha_pago.strftime('%d/%m/%Y'))
        except Exception:
            fecha_str_disp = (fecha_emision or datetime.now().strftime('%d/%m/%Y'))

        right_info_style = ParagraphStyle(name='RightInfo', parent=styles['Normal'], alignment=TA_RIGHT)
        right_info_para = Paragraph(f"Comprobante N°: {recibo_numero}<br/>Fecha: {fecha_str_disp}", right_info_style)

        header_data = [['', header_text, right_info_para]]
        if mostrar_logo is not False and os.path.exists(self.logo_path):
            from reportlab.platypus import Image
            logo = Image(self.logo_path, width=1*inch, height=1*inch)
            header_data[0][0] = logo

        header_table = Table(header_data, colWidths=[1.7*inch, 4.3*inch, 1.5*inch])
        header_table.setStyle(TableStyle([
            ('ALIGN', (0, 0), (0, 0), 'LEFT'), ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('ALIGN', (1, 0), (1, 0), 'CENTER'), ('TEXTCOLOR', (1, 0), (1, 0), colors.darkblue),
            ('FONTNAME', (1, 0), (1, 0), 'Helvetica-Bold'), ('FONTSIZE', (1, 0), (1, 0), 24),
            ('ALIGN', (2, 0), (2, 0), 'RIGHT'),
        ]))
        elements.append(header_table)
        elements.append(Spacer(1, 0.25*inch))

        # Usar número de comprobante si está disponible, sino usar ID del pago
        recibo_numero = numero_comprobante if numero_comprobante else str(pago.id)
        
        gym_name_disp = (gym_name or self.gym_name)
        gym_addr_disp = (gym_address or self.gym_address)
        elements.append(Spacer(1, 0.3*inch))

        # Resolver método de pago para detalles del pago
        metodo_nombre = None
        try:
            # Si el nombre del método de pago ya viene en el objeto pago, usarlo
            if hasattr(pago, 'metodo_pago_nombre') and getattr(pago, 'metodo_pago_nombre'):
                metodo_nombre = getattr(pago, 'metodo_pago_nombre')
            # Si tenemos el ID del método, intentar resolver el nombre desde la base de datos
            elif getattr(pago, 'metodo_pago_id', None):
                try:
                    from .database import DatabaseManager
                    db = DatabaseManager()
                    with db.get_connection_context() as conn:
                        cur = conn.cursor()
                        cur.execute("SELECT nombre FROM metodos_pago WHERE id = %s", (pago.metodo_pago_id,))
                        r = cur.fetchone()
                        if r:
                            metodo_nombre = r[0]
                except Exception:
                    metodo_nombre = None
            # Override si viene
            if metodo_pago:
                metodo_nombre = metodo_pago
        except Exception:
            metodo_nombre = metodo_pago or None

        # Panel de INFORMACIÓN DEL RECIBO en una sola tabla
        try:
            nombre_disp = (usuario_nombre or getattr(usuario, 'nombre', '') or '')
            dni_disp = (usuario_dni if usuario_dni is not None else getattr(usuario, 'dni', None))

            meses = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
            periodo_def = f"{meses[(getattr(pago, 'mes', 1) or 1) - 1]} {getattr(pago, 'año', datetime.now().year)}"
            periodo_disp = (periodo or periodo_def)
            tipo_cuota_disp = (tipo_cuota or getattr(usuario, 'tipo_cuota', None) or "No especificado")

            info_rows = [[Paragraph('<b>INFORMACIÓN DEL RECIBO</b>', styles['Normal']), '']]
            info_rows.append(['Nombre', nombre_disp])
            if mostrar_dni is not False and dni_disp:
                info_rows.append(['DNI', str(dni_disp)])
            if mostrar_metodo is not False:
                info_rows.append(['Método de Pago', (metodo_nombre or 'No especificado')])
            info_rows.append(['Tipo de Cuota', tipo_cuota_disp])
            info_rows.append(['Periodo', periodo_disp])

            info_table_unificada = Table(info_rows, colWidths=[2.4*inch, 5.1*inch])
            info_table_unificada.setStyle(TableStyle([
                ('SPAN', (0, 0), (1, 0)),
                ('BACKGROUND', (0, 0), (1, 0), header_bg_color),
                ('TEXTCOLOR', (0, 0), (1, 0), colors.whitesmoke),
                ('FONTNAME', (0, 0), (1, 0), 'Helvetica-Bold'),
                ('ALIGN', (0, 0), (1, 0), 'LEFT'),
                ('BACKGROUND', (0, 1), (1, -1), body_bg_color),
                ('GRID', (0, 0), (1, -1), 1, colors.black),
                ('VALIGN', (0, 1), (1, -1), 'MIDDLE'),
                ('LEFTPADDING', (0, 1), (1, -1), 6),
                ('RIGHTPADDING', (0, 1), (1, -1), 6),
            ]))
            elements.append(info_table_unificada)
            elements.append(Spacer(1, 0.35*inch))
        except Exception:
            pass

        # Tabla de detalles
        pago_details_data = [['Descripción', 'Cantidad', 'Precio Unitario', 'Subtotal']]
        if detalles_override and isinstance(detalles_override, list) and len(detalles_override) > 0:
            for det in detalles_override:
                try:
                    desc = str(det.get('descripcion') or det.get('concepto') or 'Concepto')
                    cantidad = float(det.get('cantidad') or 1)
                    precio = float(det.get('precio_unitario') or det.get('precio') or 0)
                    subtotal = cantidad * precio
                    pago_details_data.append([
                        desc,
                        f"{cantidad:g}",
                        f"${precio:,.2f} ARS",
                        f"${subtotal:,.2f} ARS",
                    ])
                except Exception:
                    continue
        elif detalles and len(detalles) > 0:
            for det in detalles:
                desc = det.concepto_nombre or 'Concepto'
                cantidad_str = f"{det.cantidad:g}"
                precio_str = f"${det.precio_unitario:,.2f} ARS"
                subtotal_str = f"${det.subtotal:,.2f} ARS"
                pago_details_data.append([desc, cantidad_str, precio_str, subtotal_str])
        else:
            # Fallback a la fila de cuota mensual si no hay detalles disponibles
            meses = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
            mes_nombre = meses[pago.mes - 1]
            monto_formateado = f"${pago.monto:,.2f} ARS"
            pago_details_data.append([f"Cuota mensual: {mes_nombre} {pago.año}", '1', monto_formateado, monto_formateado])

        pago_table = Table(pago_details_data, colWidths=[3.5*inch, 1*inch, 1.5*inch, 1.5*inch])
        pago_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), header_bg_color),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), body_bg_color),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            # Alineación por columna para presentación profesional
            ('ALIGN', (0, 1), (0, -1), 'LEFT'),
            ('ALIGN', (1, 1), (1, -1), 'CENTER'),
            ('ALIGN', (2, 1), (3, -1), 'RIGHT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('LEFTPADDING', (0, 1), (0, -1), 8),
            ('RIGHTPADDING', (0, 1), (0, -1), 8),
        ]))
        elements.append(pago_table)
        elements.append(Spacer(1, 0.4*inch))

        # Resumen de totales
        subtotal_val = None
        comision_val = 0.0
        total_val = None
        if totales:
            subtotal_val = float(totales.get('subtotal', 0.0))
            comision_val = float(totales.get('comision', 0.0))
            total_val = float(totales.get('total', subtotal_val + comision_val))
        else:
            # Fallback si no se provee totales
            if detalles_override and len(detalles_override) > 0:
                try:
                    subtotal_val = sum([(float(d.get('cantidad') or 1) * float(d.get('precio_unitario') or d.get('precio') or 0)) for d in detalles_override])
                except Exception:
                    subtotal_val = float(pago.monto or 0.0)
            else:
                subtotal_val = sum([d.subtotal for d in detalles]) if detalles else float(pago.monto or 0.0)
            total_val = subtotal_val
            comision_val = 0.0

        subtotal_str = f"${subtotal_val:,.2f} ARS"
        comision_str = f"${comision_val:,.2f} ARS"
        total_str = f"${total_val:,.2f} ARS"

        total_rows = [['', 'SUBTOTAL:', subtotal_str]]
        if comision_val and comision_val != 0.0:
            total_rows.append(['', 'COMISIÓN:', comision_str])
        total_rows.append(['', 'TOTAL:', total_str])

        total_table = Table(total_rows, colWidths=[5*inch, 1*inch, 1.5*inch])
        total_table.setStyle(TableStyle([
            ('ALIGN', (1, 0), (-1, -1), 'RIGHT'),
            ('FONTNAME', (1, 0), (-1, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (1, 0), (-1, -1), 12),
            # Resaltar el TOTAL con los colores del sistema
            ('BACKGROUND', (-3, -1), (-1, -1), header_bg_color),
            ('TEXTCOLOR', (-3, -1), (-1, -1), colors.whitesmoke),
        ]))
        elements.append(total_table)
        elements.append(Spacer(1, 0.5*inch))

        # Observaciones y emitido por unificados en panel (si se proporcionan)
        try:
            if observaciones or emitido_por:
                header_notes = Paragraph('<b>OBSERVACIONES Y EMISIÓN</b>', styles['Normal'])
                notes_text_parts = []
                if observaciones:
                    notes_text_parts.append(f"Observaciones: {observaciones}")
                if emitido_por:
                    notes_text_parts.append(f"Emitido por: {emitido_por}")
                notes_para = Paragraph('<br/>'.join(notes_text_parts), styles['Normal'])
                notes_table = Table([[header_notes], [notes_para]], colWidths=[7.5*inch])
                notes_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), header_bg_color),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('ALIGN', (0, 0), (-1, 0), 'LEFT'),
                    ('BACKGROUND', (0, 1), (-1, 1), body_bg_color),
                    ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ]))
                elements.append(notes_table)
                elements.append(Spacer(1, 0.3*inch))
        except Exception:
            pass

        elements.append(Paragraph("¡Gracias por tu pago!", styles['h3']))
        elements.append(Paragraph(gym_name_disp, styles['Normal']))
        elements.append(Paragraph(gym_addr_disp, styles['Normal']))

        doc.build(elements, onFirstPage=self._footer, onLaterPages=self._footer)
        return filepath

    def _footer(self, canvas, doc):
        try:
            canvas.saveState()
            width, height = doc.pagesize
            # Línea superior del pie de página con color de branding
            footer_color = colors.HexColor(self._get_dynamic_color('alt_background_color', '#434C5E'))
            canvas.setStrokeColor(footer_color)
            canvas.setLineWidth(0.5)
            canvas.line(inch/2, 0.75*inch, width - inch/2, 0.75*inch)
            # Texto centrado con nombre y dirección del gimnasio
            canvas.setFillColor(colors.black)
            canvas.setFont("Helvetica", 9)
            text = f"{self.gym_name} • {self.gym_address}"
            canvas.drawCentredString(width/2, 0.55*inch, text)
            canvas.restoreState()
        except Exception:
            # Si por algún motivo falla el pie de página, continuar sin interrumpir
            pass

    # --- NUEVO MÉTODO PARA EXPORTAR RUTINAS ---
    def generar_pdf_rutina(self, rutina: Rutina, usuario: Usuario, exercises_by_day: dict) -> str:
        # Formato: rutina_{nombreRutina}_{CantDias}_{Nombre_Apellido}_{dd-mm-aaaa}.pdf
        rname = (getattr(rutina, "nombre_rutina", None) or getattr(rutina, "nombre", None) or "Rutina")
        uname = (getattr(usuario, "nombre", "") or "").strip()
        parts = uname.split() if uname else []
        first = parts[0] if parts else ""
        last = parts[-1] if len(parts) > 1 else ""
        def _safe_slug(s: str) -> str:
            import re as _re
            s = (s or "").strip()
            s = _re.sub(r"[^\w\s-]", "", s)
            s = _re.sub(r"\s+", "_", s)
            return s
        base = _safe_slug(rname) or "Rutina"
        user_seg = _safe_slug(first) + (("_" + _safe_slug(last)) if last else "")
        # Cantidad de días profesional y entendible, clamped a 1-5
        try:
            num_days = min(5, max(1, len(exercises_by_day or {})))
        except Exception:
            num_days = 1
        days_seg = f"{num_days}-dias"
        fc = getattr(rutina, "fecha_creacion", None)
        try:
            from datetime import date as _date
            if isinstance(fc, (datetime, _date)):
                fecha_str = f"{getattr(fc, 'day', fc.day):02d}-{getattr(fc, 'month', fc.month):02d}-{getattr(fc, 'year', fc.year)}"
            else:
                fecha_str = datetime.now().strftime("%d-%m-%Y")
        except Exception:
            fecha_str = datetime.now().strftime("%d-%m-%Y")
        filename = f"rutina_{base}_{days_seg}_{user_seg or 'Usuario'}_{fecha_str}.pdf"
        filepath = os.path.join(self.output_dir_rutinas, filename)

        doc = SimpleDocTemplate(filepath, pagesize=letter, rightMargin=inch/2, leftMargin=inch/2, topMargin=inch/2, bottomMargin=inch/2)
        styles = getSampleStyleSheet()
        elements = []

        # Estilos personalizados
        title_style = styles['h1']
        title_style.alignment = TA_CENTER
        user_style = ParagraphStyle(name='UserStyle', parent=styles['h2'], alignment=TA_CENTER)
        day_title_style = ParagraphStyle(name='DayTitle', parent=styles['h3'], spaceBefore=20, spaceAfter=10, textColor=colors.darkblue)

        # Encabezado
        elements.append(Paragraph(rutina.nombre_rutina, title_style))
        elements.append(Paragraph(f"Plan de Entrenamiento para: {usuario.nombre}", user_style))
        elements.append(Spacer(1, 0.3*inch))

        # Iterar por cada día y crear una tabla
        for day in sorted(exercises_by_day.keys()):
            elements.append(Paragraph(f"DÍA {day}", day_title_style))
            
            table_data = [['Ejercicio', 'Grupo Muscular', 'Series', 'Repeticiones']]
            ejercicios_del_dia = exercises_by_day[day]

            for ej in ejercicios_del_dia:
                table_data.append([
                    Paragraph(ej.ejercicio.nombre, styles['Normal']),
                    Paragraph(ej.ejercicio.grupo_muscular or 'N/A', styles['Normal']),
                    Paragraph(str(ej.series), styles['Normal']),
                    Paragraph(ej.repeticiones, styles['Normal'])
                ])

            table = Table(table_data, colWidths=[2.5*inch, 2*inch, 1*inch, 2*inch])
            # Obtener color dinámico para el encabezado de la tabla
            header_bg_color = self._get_dynamic_color('alt_background_color', '#434C5E')
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor(header_bg_color)),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor("#D8DEE9")),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('LEFTPADDING', (0, 1), (0, -1), 8),
                ('RIGHTPADDING', (0, 1), (0, -1), 8)
            ]))
            elements.append(table)

        doc.build(elements)
        return filepath
