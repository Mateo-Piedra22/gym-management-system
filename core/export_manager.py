import os
import pandas as pd
from typing import List, Dict
from .models import Pago, Rutina, RutinaEjercicio, Ejercicio
import logging

class ExportManager:
    # ... (código existente)
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.export_dir = "exports"
        if not os.path.exists(self.export_dir):
            os.makedirs(self.export_dir)
            logging.info(f"Directorio '{self.export_dir}' creado.")
    def exportar_usuarios_csv(self) -> str:
        try:
            usuarios = self.db_manager.obtener_todos_usuarios()
            if not usuarios: raise ValueError("No hay usuarios para exportar.")
            user_data = [vars(u) for u in usuarios]
            df = pd.DataFrame(user_data)
            filepath = os.path.join(self.export_dir, "usuarios_exportados.csv")
            df.to_csv(filepath, index=False, encoding='utf-8-sig')
            logging.info(f"Usuarios exportados exitosamente a {filepath}")
            return filepath
        except Exception as e: logging.exception("Error al exportar usuarios a CSV."); raise e
    def exportar_reporte_pagos_excel(self, pagos: List[Pago], mes: int, año: int) -> str:
        try:
            if not pagos: raise ValueError("No hay pagos en el período seleccionado para exportar.")
            pagos_data = []
            for pago in pagos:
                usuario = self.db_manager.obtener_usuario(pago.usuario_id)
                pagos_data.append({"ID Pago": pago.id, "ID Usuario": pago.usuario_id, "Nombre Usuario": usuario.nombre if usuario else "N/A", "Monto": pago.monto, "Fecha de Pago": pago.fecha_pago.strftime('%Y-%m-%d') if hasattr(pago.fecha_pago, 'strftime') else pago.fecha_pago})
            df_detalle = pd.DataFrame(pagos_data)
            total_ingresos = df_detalle['Monto'].sum()
            resumen_data = {"Concepto": ["Período del Reporte", "Total de Ingresos", "Cantidad de Pagos"], "Valor": [f"{mes:02d}/{año}", f"${total_ingresos:,.2f} ARS", len(df_detalle)]}
            df_resumen = pd.DataFrame(resumen_data)
            filepath = os.path.join(self.export_dir, f"reporte_pagos_{año}_{mes:02d}.xlsx")
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                df_resumen.to_excel(writer, sheet_name='Resumen', index=False)
                df_detalle.to_excel(writer, sheet_name='Detalle de Pagos', index=False)
                workbook = writer.book; ws_resumen = workbook['Resumen']; ws_detalle = workbook['Detalle de Pagos']
                ws_resumen.column_dimensions['A'].width = 30; ws_resumen.column_dimensions['B'].width = 25; ws_detalle.column_dimensions['C'].width = 30; ws_detalle.column_dimensions['D'].width = 15; ws_detalle.column_dimensions['E'].width = 15
            logging.info(f"Reporte de pagos exportado exitosamente a {filepath}"); return filepath
        except Exception as e: logging.exception("Error al exportar reporte de pagos a Excel."); raise e
    def exportar_rutina_excel(self, rutina: Rutina, exercises_by_day: Dict[int, List[RutinaEjercicio]]) -> str:
        try:
            rutinas_export_dir = "rutinas_exportadas"
            if not os.path.exists(rutinas_export_dir): os.makedirs(rutinas_export_dir)
            usuario = self.db_manager.obtener_usuario(rutina.usuario_id)
            user_name = usuario.nombre.replace(' ', '_') if usuario else 'desconocido'
            filepath = os.path.join(rutinas_export_dir, f"rutina_{user_name}_{rutina.nombre_rutina.replace(' ', '_')}.xlsx")
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                for day, exercises in sorted(exercises_by_day.items()):
                    sheet_name = f"Día {day}"; day_data = [{"Ejercicio": ex.ejercicio.nombre, "Grupo Muscular": ex.ejercicio.grupo_muscular or "N/A", "Ser": ex.series, "Rep": ex.repeticiones} for ex in exercises]
                    df_day = pd.DataFrame(day_data); df_day.to_excel(writer, sheet_name=sheet_name, index=False)
                    worksheet = writer.sheets[sheet_name]; worksheet.column_dimensions['A'].width = 35; worksheet.column_dimensions['B'].width = 25; worksheet.column_dimensions['C'].width = 10; worksheet.column_dimensions['D'].width = 20
            logging.info(f"Rutina exportada a Excel en {filepath}"); return filepath
        except Exception as e: logging.exception("Error al exportar rutina a Excel."); raise e
    def exportar_banco_ejercicios_excel(self, filepath: str, ejercicios: List[Ejercicio]) -> str:
        try:
            if not ejercicios: raise ValueError("No hay ejercicios en el banco para exportar.")
            data = [{"id": ej.id, "nombre": ej.nombre, "grupo_muscular": ej.grupo_muscular, "descripcion": ej.descripcion, "objetivo": getattr(ej, 'objetivo', None)} for ej in ejercicios]
            df = pd.DataFrame(data); df.to_excel(filepath, index=False, engine='openpyxl'); logging.info(f"Banco de ejercicios exportado a {filepath}"); return filepath
        except Exception as e: logging.exception("Error al exportar el banco de ejercicios a Excel."); raise e

    # --- NUEVO MÉTODO ---
    def exportar_ejercicios_a_excel(self, filepath: str, ejercicios: List[Ejercicio]):
        try:
            if not ejercicios: raise ValueError("No hay ejercicios para exportar.")
            data = [{"nombre": ej.nombre, "grupo_muscular": ej.grupo_muscular, "descripcion": ej.descripcion, "objetivo": getattr(ej, 'objetivo', None)} for ej in ejercicios]
            df = pd.DataFrame(data)
            df.to_excel(filepath, index=False, engine='openpyxl')
            logging.info(f"Lista de ejercicios exportada a {filepath}")
        except Exception as e:
            logging.exception("Error al exportar la lista de ejercicios a Excel.")
            raise e
