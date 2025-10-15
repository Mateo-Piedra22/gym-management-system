#!/usr/bin/env python3
"""
Script para migrar datos de la tabla legacy 'lista_espera' a 'clase_lista_espera'.
No toca el archivo de respaldo. Elimina la tabla legacy al finalizar.
"""

from database import DatabaseManager

def main():
    print("Iniciando migración de lista_espera -> clase_lista_espera...")
    db = DatabaseManager()
    resultados = db.migrar_lista_espera_legacy(drop_legacy=True)
    print("Resultados de la migración:")
    print(f"- Registros procesados: {resultados.get('procesados', 0)}")
    print(f"- Insertados/actualizados: {resultados.get('insertados_actualizados', 0)}")
    print(f"- Errores: {resultados.get('errores', 0)}")
    print("Migración completada.")

if __name__ == "__main__":
    main()