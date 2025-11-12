import logging
from typing import Any, Dict, List, Optional

import psycopg2
import psycopg2.extras
from fastapi import Request, HTTPException, Depends
from fastapi.responses import JSONResponse

# Intentar usar DatabaseManager directamente para evitar import circular con server
try:
    from database import DatabaseManager  # type: ignore
except Exception:
    DatabaseManager = None  # type: ignore

_db_local: Optional[DatabaseManager] = None

def _get_db_local() -> Optional[DatabaseManager]:
    global _db_local
    if _db_local is not None:
        return _db_local
    if DatabaseManager is None:
        return None
    try:
        _db_local = DatabaseManager()
        return _db_local
    except Exception:
        return None

def _require_gestion_access(request: Request):
    # Validación mínima de sesión (replica de server.require_gestion_access)
    try:
        if request.session.get("logged_in") or request.session.get("gestion_profesor_id"):
            return True
    except Exception:
        pass
    raise HTTPException(status_code=401, detail="Acceso restringido a Gestión")

def _ensure_bloques_schema(conn) -> None:
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS clase_bloques (
                    id SERIAL PRIMARY KEY,
                    clase_id INTEGER NOT NULL,
                    nombre TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS idx_clase_bloques_clase ON clase_bloques(clase_id);
                
                CREATE TABLE IF NOT EXISTS clase_bloque_items (
                    id SERIAL PRIMARY KEY,
                    bloque_id INTEGER NOT NULL REFERENCES clase_bloques(id) ON DELETE CASCADE,
                    ejercicio_id INTEGER NOT NULL,
                    orden INTEGER NOT NULL DEFAULT 0,
                    series INTEGER DEFAULT 0,
                    repeticiones TEXT,
                    descanso_segundos INTEGER DEFAULT 0,
                    notas TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_bloque_items_bloque ON clase_bloque_items(bloque_id);
                """
            )
    except Exception as e:
        logging.error(f"Error asegurando esquema de bloques: {e}")

def register_bloques_routes(app):
    """Registra rutas FastAPI para bloques de ejercicios por clase."""

    @app.get("/api/clases/{clase_id}/bloques")
    async def api_clase_bloques_list(clase_id: int, _=Depends(_require_gestion_access)):
        db = _get_db_local()
        if db is None:
            raise HTTPException(status_code=503, detail="DB no disponible")
        try:
            with db.get_connection_context() as conn:  # type: ignore
                _ensure_bloques_schema(conn)
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur.execute("""
                    SELECT id, nombre
                    FROM clase_bloques
                    WHERE clase_id = %s
                    ORDER BY nombre ASC, id DESC
                """, (clase_id,))
                rows = cur.fetchall() or []
                # Normalizar claves
                return [{"id": int(r["id"]), "nombre": (r.get("nombre") or "Bloque").strip()} for r in rows]
        except HTTPException:
            raise
        except Exception as e:
            logging.exception("Error listando bloques")
            return JSONResponse({"error": str(e)}, status_code=500)

    @app.get("/api/clases/{clase_id}/bloques/{bloque_id}")
    async def api_clase_bloque_items(clase_id: int, bloque_id: int, _=Depends(_require_gestion_access)):
        db = _get_db_local()
        if db is None:
            raise HTTPException(status_code=503, detail="DB no disponible")
        try:
            with db.get_connection_context() as conn:  # type: ignore
                _ensure_bloques_schema(conn)
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                # Validar relación bloque-clase
                cur.execute("SELECT id FROM clase_bloques WHERE id = %s AND clase_id = %s", (bloque_id, clase_id))
                if not cur.fetchone():
                    raise HTTPException(status_code=404, detail="Bloque no encontrado")
                cur.execute(
                    """
                    SELECT ejercicio_id, orden, series, repeticiones, descanso_segundos, notas
                    FROM clase_bloque_items
                    WHERE bloque_id = %s
                    ORDER BY orden ASC, id ASC
                    """,
                    (bloque_id,)
                )
                rows = cur.fetchall() or []
                return [
                    {
                        "ejercicio_id": int(r.get("ejercicio_id") or 0),
                        "orden": int(r.get("orden") or 0),
                        "series": int(r.get("series") or 0),
                        "repeticiones": str(r.get("repeticiones") or ""),
                        "descanso_segundos": int(r.get("descanso_segundos") or 0),
                        "notas": str(r.get("notas") or ""),
                    }
                    for r in rows
                ]
        except HTTPException:
            raise
        except Exception as e:
            logging.exception("Error obteniendo items del bloque")
            return JSONResponse({"error": str(e)}, status_code=500)

    @app.post("/api/clases/{clase_id}/bloques")
    async def api_clase_bloque_create(clase_id: int, request: Request, _=Depends(_require_gestion_access)):
        db = _get_db_local()
        if db is None:
            raise HTTPException(status_code=503, detail="DB no disponible")
        payload = await request.json()
        try:
            nombre = (payload.get("nombre") or "").strip()
            items = payload.get("items") or []
            if not nombre:
                raise HTTPException(status_code=400, detail="'nombre' es obligatorio")
            if not isinstance(items, list):
                items = []
            with db.atomic_transaction() as conn:  # type: ignore
                _ensure_bloques_schema(conn)
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur.execute(
                    "INSERT INTO clase_bloques (clase_id, nombre) VALUES (%s, %s) RETURNING id",
                    (clase_id, nombre)
                )
                row = cur.fetchone()
                bloque_id = int(row["id"]) if row else None
                if not bloque_id:
                    raise HTTPException(status_code=500, detail="No se pudo crear bloque")
                # Insertar items
                for idx, it in enumerate(items):
                    try:
                        cur.execute(
                            """
                            INSERT INTO clase_bloque_items (bloque_id, ejercicio_id, orden, series, repeticiones, descanso_segundos, notas)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """,
                            (
                                bloque_id,
                                int(it.get("ejercicio_id") or it.get("id") or 0),
                                int(it.get("orden") or idx),
                                int(it.get("series") or 0),
                                str(it.get("repeticiones") or ""),
                                int(it.get("descanso_segundos") or 0),
                                str(it.get("notas") or ""),
                            )
                        )
                    except Exception:
                        # Continuar con el resto de items
                        pass
                return {"ok": True, "id": bloque_id}
        except HTTPException:
            raise
        except Exception as e:
            logging.exception("Error creando bloque")
            return JSONResponse({"error": str(e)}, status_code=500)

    @app.put("/api/clases/{clase_id}/bloques/{bloque_id}")
    async def api_clase_bloque_update(clase_id: int, bloque_id: int, request: Request, _=Depends(_require_gestion_access)):
        db = _get_db_local()
        if db is None:
            raise HTTPException(status_code=503, detail="DB no disponible")
        payload = await request.json()
        try:
            items = payload.get("items") or []
            nombre_raw = payload.get("nombre")
            nombre = (nombre_raw or "").strip() if isinstance(nombre_raw, str) else None
            if not isinstance(items, list):
                items = []
            with db.atomic_transaction() as conn:  # type: ignore
                _ensure_bloques_schema(conn)
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                # Validar relación bloque-clase
                cur.execute("SELECT id FROM clase_bloques WHERE id = %s AND clase_id = %s", (bloque_id, clase_id))
                if not cur.fetchone():
                    raise HTTPException(status_code=404, detail="Bloque no encontrado")
                # Reemplazar items
                cur.execute("DELETE FROM clase_bloque_items WHERE bloque_id = %s", (bloque_id,))
                for idx, it in enumerate(items):
                    try:
                        cur.execute(
                            """
                            INSERT INTO clase_bloque_items (bloque_id, ejercicio_id, orden, series, repeticiones, descanso_segundos, notas)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """,
                            (
                                bloque_id,
                                int(it.get("ejercicio_id") or it.get("id") or 0),
                                int(it.get("orden") or idx),
                                int(it.get("series") or 0),
                                str(it.get("repeticiones") or ""),
                                int(it.get("descanso_segundos") or 0),
                                str(it.get("notas") or ""),
                            )
                        )
                    except Exception:
                        pass
                # Actualizar nombre si se envía y timestamp
                try:
                    if nombre is not None and nombre != "":
                        cur.execute("UPDATE clase_bloques SET nombre = %s, updated_at = NOW() WHERE id = %s", (nombre, bloque_id))
                    else:
                        cur.execute("UPDATE clase_bloques SET updated_at = NOW() WHERE id = %s", (bloque_id,))
                except Exception:
                    pass
                return {"ok": True}
        except HTTPException:
            raise
        except Exception as e:
            logging.exception("Error actualizando bloque")
            return JSONResponse({"error": str(e)}, status_code=500)

    @app.delete("/api/clases/{clase_id}/bloques/{bloque_id}")
    async def api_clase_bloque_delete(clase_id: int, bloque_id: int, _=Depends(_require_gestion_access)):
        db = _get_db_local()
        if db is None:
            raise HTTPException(status_code=503, detail="DB no disponible")
        try:
            with db.atomic_transaction() as conn:  # type: ignore
                _ensure_bloques_schema(conn)
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                # Validar relación bloque-clase
                cur.execute("SELECT id FROM clase_bloques WHERE id = %s AND clase_id = %s", (bloque_id, clase_id))
                if not cur.fetchone():
                    raise HTTPException(status_code=404, detail="Bloque no encontrado")
                cur.execute("DELETE FROM clase_bloque_items WHERE bloque_id = %s", (bloque_id,))
                cur.execute("DELETE FROM clase_bloques WHERE id = %s", (bloque_id,))
                return {"ok": True}
        except HTTPException:
            raise
        except Exception as e:
            logging.exception("Error eliminando bloque")
            return JSONResponse({"error": str(e)}, status_code=500)