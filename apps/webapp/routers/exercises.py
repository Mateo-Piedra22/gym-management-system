import logging
import time
import os
from fastapi import APIRouter, Request, Depends, HTTPException, UploadFile, File, Form
from fastapi.responses import JSONResponse
from apps.webapp.dependencies import get_db, require_gestion_access
from apps.webapp.utils import _get_tenant_from_request
from core.services.storage_service import StorageService
import psycopg2.extras

router = APIRouter()
logger = logging.getLogger(__name__)

@router.post("/api/exercises/video")
async def api_upload_exercise_video(request: Request, file: UploadFile = File(...), _=Depends(require_gestion_access)):
    try:
        ctype = str(getattr(file, 'content_type', '') or '').lower()
        if not ctype.startswith("video/"):
            return JSONResponse({"ok": False, "error": "El archivo debe ser un video"}, status_code=400)
            
        # Limit size logic could be here (e.g. via middleware or stream check)
        
        data = await file.read()
        if not data:
             return JSONResponse({"ok": False, "error": "Archivo vacío"}, status_code=400)
        
        if len(data) > 50 * 1024 * 1024: # 50MB limit hardcoded for safety
             return JSONResponse({"ok": False, "error": "El video es demasiado grande (max 50MB)"}, status_code=400)

        storage = StorageService()
        tenant = _get_tenant_from_request(request) or "common"
        
        ext = os.path.splitext(file.filename)[1] if file.filename else ".mp4"
        filename = f"exercise_{int(time.time())}_{os.urandom(4).hex()}{ext}"
        
        # Upload to 'exercises/<tenant>/...'
        public_url = storage.upload_file(data, filename, ctype, subfolder=f"exercises/{tenant}")
        
        if not public_url:
            return JSONResponse({"ok": False, "error": "Error subiendo el video a la nube"}, status_code=500)
            
        return JSONResponse({"ok": True, "url": public_url, "mime": ctype})
        
    except Exception as e:
        logger.error(f"Error uploading exercise video: {e}")
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

@router.get("/api/exercises")
async def api_list_exercises(request: Request, q: str = "", page: int = 1, page_size: int = 20, _=Depends(require_gestion_access)):
    db = get_db()
    if not db:
        return {"items": [], "total": 0}
    try:
        p = max(1, int(page))
        ps = max(1, int(page_size))
        offset = (p - 1) * ps
        
        search = str(q or "").strip().lower()
        
        with db.get_connection_context() as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            # Ensure schema columns exist
            cols = db.get_table_columns('ejercicios') or []
            if 'video_url' not in cols:
                try:
                    with db.atomic_transaction() as tconn:
                        tconn.cursor().execute("ALTER TABLE ejercicios ADD COLUMN IF NOT EXISTS video_url TEXT, ADD COLUMN IF NOT EXISTS video_mime TEXT")
                except Exception:
                    pass

            where_clause = ""
            params = []
            if search:
                where_clause = "WHERE LOWER(nombre) LIKE %s OR LOWER(grupo_muscular) LIKE %s"
                params = [f"%{search}%", f"%{search}%"]
            
            cur.execute(f"SELECT COUNT(*) FROM ejercicios {where_clause}", params)
            total = cur.fetchone()['count']
            
            cur.execute(f"SELECT * FROM ejercicios {where_clause} ORDER BY nombre ASC LIMIT %s OFFSET %s", params + [ps, offset])
            rows = cur.fetchall()
            
            # Fix URLs if needed (ensure full CDN url)
            storage = StorageService()
            items = []
            for r in rows:
                item = dict(r)
                if item.get("video_url"):
                    item["video_url"] = storage.get_file_url(item["video_url"])
                items.append(item)
                
            return {"items": items, "total": total, "page": p}
    except Exception as e:
        logger.error(f"Error listing exercises: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)

@router.post("/api/exercises")
async def api_create_exercise(request: Request, _=Depends(require_gestion_access)):
    db = get_db()
    if not db: return JSONResponse({"error": "DB error"}, status_code=503)
    try:
        data = await request.json()
        nombre = str(data.get("nombre") or "").strip()
        grupo = str(data.get("grupo_muscular") or "").strip()
        video_url = str(data.get("video_url") or "").strip() or None
        video_mime = str(data.get("video_mime") or "").strip() or None
        
        if not nombre:
             return JSONResponse({"ok": False, "error": "Nombre requerido"}, status_code=400)
             
        with db.get_connection_context() as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO ejercicios (nombre, grupo_muscular, video_url, video_mime) VALUES (%s, %s, %s, %s) RETURNING id",
                (nombre, grupo, video_url, video_mime)
            )
            new_id = cur.fetchone()[0]
            conn.commit()
            
        return {"ok": True, "id": new_id}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

@router.put("/api/exercises/{eid}")
async def api_update_exercise(eid: int, request: Request, _=Depends(require_gestion_access)):
    db = get_db()
    if not db: return JSONResponse({"error": "DB error"}, status_code=503)
    try:
        data = await request.json()
        nombre = str(data.get("nombre") or "").strip()
        grupo = str(data.get("grupo_muscular") or "").strip()
        video_url = str(data.get("video_url") or "").strip() or None
        
        sets = ["nombre = %s", "grupo_muscular = %s"]
        params = [nombre, grupo]
        
        if video_url is not None: # Only update if provided (even empty string to clear)
             sets.append("video_url = %s")
             params.append(video_url)
             if "video_mime" in data:
                 sets.append("video_mime = %s")
                 params.append(data.get("video_mime"))

        params.append(int(eid))
             
        with db.get_connection_context() as conn:
            cur = conn.cursor()
            cur.execute(f"UPDATE ejercicios SET {', '.join(sets)} WHERE id = %s", params)
            conn.commit()
            
        return {"ok": True}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

@router.delete("/api/exercises/{eid}")
async def api_delete_exercise(eid: int, _=Depends(require_gestion_access)):
    db = get_db()
    if not db: return JSONResponse({"error": "DB error"}, status_code=503)
    try:
        with db.get_connection_context() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM ejercicios WHERE id = %s", (int(eid),))
            conn.commit()
        return {"ok": True}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)
