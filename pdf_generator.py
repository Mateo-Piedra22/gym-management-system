import os
from datetime import datetime
from models import Pago, Usuario, Rutina, PagoDetalle
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_RIGHT, TA_CENTER, TA_LEFT
from typing import Optional, List, Dict
from utils import get_gym_name

class PDFGenerator:
    def __init__(self, branding_config=None):
        self.output_dir_recibos = "recibos"
        self.output_dir_rutinas = "rutinas_exportadas"
        for directory in [self.output_dir_recibos, self.output_dir_rutinas]:
            if not os.path.exists(directory):
                os.makedirs(directory)
        
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

    def generar_recibo(self, pago: Pago, usuario: Usuario, numero_comprobante: str = None, detalles: Optional[List[PagoDetalle]] = None, totales: Optional[Dict[str, float]] = None) -> str:
        fecha_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"recibo_{pago.id}_{fecha_str}.pdf"
        filepath = os.path.join(self.output_dir_recibos, filename)

        doc = SimpleDocTemplate(filepath, pagesize=letter)
        styles = getSampleStyleSheet()
        elements = []

        header_data = [['', 'RECIBO DE PAGO']]
        if os.path.exists(self.logo_path):
            from reportlab.platypus import Image
            logo = Image(self.logo_path, width=1*inch, height=1*inch)
            header_data[0][0] = logo

        header_table = Table(header_data, colWidths=[1.7*inch, 5.8*inch])
        header_table.setStyle(TableStyle([
            ('ALIGN', (0, 0), (0, 0), 'LEFT'), ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('ALIGN', (1, 0), (1, 0), 'RIGHT'), ('TEXTCOLOR', (1, 0), (1, 0), colors.darkblue),
            ('FONTNAME', (1, 0), (1, 0), 'Helvetica-Bold'), ('FONTSIZE', (1, 0), (1, 0), 24),
        ]))
        elements.append(header_table)
        elements.append(Spacer(1, 0.25*inch))

        # Usar número de comprobante si está disponible, sino usar ID del pago
        recibo_numero = numero_comprobante if numero_comprobante else str(pago.id)
        
        info_data = [
            [self.gym_name, f"Comprobante N°: {recibo_numero}"],
            [self.gym_address, f"Fecha: {pago.fecha_pago.strftime('%d/%m/%Y')}"]
        ]
        info_table = Table(info_data, colWidths=[4*inch, 3.5*inch])
        info_table.setStyle(TableStyle([
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica'), ('FONTNAME', (1, 0), (1, 1), 'Helvetica-Bold'),
            ('ALIGN', (1, 0), (1, 1), 'RIGHT'),
        ]))
        elements.append(info_table)
        elements.append(Spacer(1, 0.4*inch))

        # Mostrar método de pago
        try:
            metodo_nombre = None
            # Si el nombre del método de pago ya viene en el objeto pago, usarlo
            if hasattr(pago, 'metodo_pago_nombre') and getattr(pago, 'metodo_pago_nombre'):
                metodo_nombre = getattr(pago, 'metodo_pago_nombre')
            # Si tenemos el ID del método, intentar resolver el nombre desde la base de datos
            elif getattr(pago, 'metodo_pago_id', None):
                try:
                    from database import DatabaseManager
                    db = DatabaseManager()
                    with db.get_connection_context() as conn:
                        cur = conn.cursor()
                        cur.execute("SELECT nombre FROM metodos_pago WHERE id = %s", (pago.metodo_pago_id,))
                        r = cur.fetchone()
                        if r:
                            metodo_nombre = r[0]
                except Exception:
                    metodo_nombre = None
            if not metodo_nombre:
                metodo_nombre = "No especificado"
            elements.append(Paragraph(f"Método de Pago: {metodo_nombre}", styles['Normal']))
            elements.append(Spacer(1, 0.2*inch))
        except Exception:
            # En caso de cualquier error, continuar sin bloquear la generación del PDF
            pass

        elements.append(Paragraph("<b>FACTURAR A:</b>", styles['Normal']))
        elements.append(Paragraph(usuario.nombre, styles['Normal']))
        if getattr(usuario, 'dni', None):
            elements.append(Paragraph(f"DNI: {usuario.dni}", styles['Normal']))
        elements.append(Spacer(1, 0.4*inch))

        # Tabla de detalles
        pago_details_data = [['Descripción', 'Cantidad', 'Precio Unitario', 'Subtotal']]
        if detalles and len(detalles) > 0:
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
            ('BACKGROUND', (0, 0), (-1, 0), colors.darkblue), ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'), ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12), ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
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
            ('ALIGN', (1, 0), (-1, -1), 'RIGHT'), ('FONTNAME', (1, 0), (-1, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (1, 0), (-1, -1), 12),
        ]))
        elements.append(total_table)
        elements.append(Spacer(1, 0.8*inch))
        elements.append(Paragraph("¡Gracias por tu pago!", styles['h3']))
        elements.append(Paragraph(self.gym_name, styles['Normal']))
        
        doc.build(elements)
        return filepath

    # --- NUEVO MÉTODO PARA EXPORTAR RUTINAS ---
    def generar_pdf_rutina(self, rutina: Rutina, usuario: Usuario, exercises_by_day: dict) -> str:
        fecha_str = datetime.now().strftime("%Y%m%d")
        filename = f"rutina_{usuario.nombre.replace(' ', '_')}_{fecha_str}.pdf"
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