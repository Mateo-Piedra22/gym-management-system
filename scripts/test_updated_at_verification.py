#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Verifica que las operaciones desde la app actualizan correctamente updated_at
para tablas clave de replicación: custom_themes y scheduling_config.

Imprime los timestamps antes y después para confirmar cambios.
"""

import json
import datetime
import sys
import os
from pathlib import Path

# Asegurar que el repo root está en sys.path para importar `database`
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from database import DatabaseManager


def fmt(ts):
    if ts is None:
        return "NULL"
    try:
        return str(ts)
    except Exception:
        return repr(ts)


def verify_custom_themes(db):
    name = "__prueba_theme_updated_at__"
    data1 = {"color": "#123456", "title": "Primera"}
    data2 = {"color": "#654321", "title": "Segunda"}
    colores1 = {
        "primary_color": "#123456",
        "secondary_color": "#2980b9",
        "accent_color": "#e74c3c",
        "background_color": "#ffffff",
        "alt_background_color": "#ecf0f1"
    }
    colores2 = {
        "primary_color": "#654321",
        "secondary_color": "#8e44ad",
        "accent_color": "#e67e22",
        "background_color": "#f4f4f4",
        "alt_background_color": "#ffffff"
    }
    print("\n[custom_themes]")
    with db.get_connection_context() as conn:
        cur = conn.cursor()
        # Mostrar esquema para diagnóstico
        cur.execute(
            """
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'custom_themes'
            ORDER BY ordinal_position
            """
        )
        cols = cur.fetchall() or []
        print("schema custom_themes:", cols)
        cur.execute("SELECT updated_at FROM custom_themes WHERE nombre = %s OR name = %s", (name, name))
        r = cur.fetchone()
        print("antes_insert:", fmt(r[0]) if r else "no_existe")

        # INSERT (si no existe)
        cur.execute("DELETE FROM custom_themes WHERE nombre = %s OR name = %s", (name, name))
        cur.execute(
            """
            INSERT INTO custom_themes (nombre, name, data, colores)
            VALUES (%s, %s, %s, %s)
            """,
            (name, name, json.dumps(data1, ensure_ascii=False), json.dumps(colores1, ensure_ascii=False)),
        )
        conn.commit()

        cur.execute("SELECT updated_at, data FROM custom_themes WHERE nombre = %s OR name = %s", (name, name))
        r = cur.fetchone()
        print("despues_insert:", fmt(r[0]))

        # UPDATE (cambio de data)
        cur.execute(
            """
            UPDATE custom_themes SET data = %s, colores = %s, nombre = %s, name = %s WHERE nombre = %s OR name = %s
            """,
            (json.dumps(data2, ensure_ascii=False), json.dumps(colores2, ensure_ascii=False), name, name, name, name),
        )
        conn.commit()

        cur.execute("SELECT updated_at, data FROM custom_themes WHERE nombre = %s OR name = %s", (name, name))
        r2 = cur.fetchone()
        print("despues_update:", fmt(r2[0]))


def verify_scheduling_config(db):
    print("\n[scheduling_config]")
    with db.get_connection_context() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS scheduling_config (
                id SERIAL PRIMARY KEY,
                enabled BOOLEAN DEFAULT FALSE,
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
            """
        )
        conn.commit()

        # Asegurar trigger BEFORE INSERT OR UPDATE para updated_at
        cur.execute(
            """
            CREATE OR REPLACE FUNCTION public.scheduling_config_set_updated_at()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at := NOW();
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            """
        )
        cur.execute("DROP TRIGGER IF EXISTS scheduling_config_set_updated_at ON scheduling_config")
        cur.execute(
            """
            CREATE TRIGGER scheduling_config_set_updated_at
            BEFORE INSERT OR UPDATE ON scheduling_config
            FOR EACH ROW EXECUTE FUNCTION public.scheduling_config_set_updated_at()
            """
        )
        conn.commit()

        cur.execute("SELECT enabled, updated_at FROM scheduling_config WHERE id = 1")
        r = cur.fetchone()
        print("antes_upsert:", fmt(r[1]) if r else "no_existe")

        cur.execute(
            """
            INSERT INTO scheduling_config (id, enabled)
            VALUES (1, %s)
            ON CONFLICT (id) DO UPDATE SET enabled = EXCLUDED.enabled
            """,
            (True,),
        )
        conn.commit()

        cur.execute("SELECT enabled, updated_at FROM scheduling_config WHERE id = 1")
        r2 = cur.fetchone()
        print("despues_upsert_true:", fmt(r2[1]))

        # Flip value to force update
        cur.execute(
            """
            INSERT INTO scheduling_config (id, enabled)
            VALUES (1, %s)
            ON CONFLICT (id) DO UPDATE SET enabled = EXCLUDED.enabled
            """,
            (False,),
        )
        conn.commit()
        cur.execute("SELECT enabled, updated_at FROM scheduling_config WHERE id = 1")
        r3 = cur.fetchone()
        print("despues_upsert_false:", fmt(r3[1]))


def main():
    db = DatabaseManager()
    verify_custom_themes(db)
    verify_scheduling_config(db)


if __name__ == "__main__":
    main()