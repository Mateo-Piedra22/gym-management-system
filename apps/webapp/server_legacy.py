import os
import sys
import csv
import json
import base64
import base64
import zlib
import secrets
from pathlib import Path
from typing import Optional, Dict, Any, Callable
from datetime import datetime, timezone
import contextvars

from fastapi import FastAPI, Request, HTTPException, Depends, UploadFile, File, Form
import logging
from fastapi.responses import JSONResponse, RedirectResponse, Response, FileResponse
from fastapi.templating import Jinja2Templates
from fastapi.encoders import jsonable_encoder
from starlette.middleware.sessions import SessionMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware
from starlette.middleware.httpsredirect import HTTPSRedirectMiddleware
from starlette.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware
from starlette.middleware.cors import CORSMiddleware
import uuid
import threading
import psutil
import shutil
import subprocess
import psycopg2
import psycopg2.extras
import time
import hmac
import hashlib
import urllib.parse

# Configuración de logging
try:
    from core.logger_config import setup_logging
    setup_logging()
except ImportError:
    logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

try:
    _here = Path(__file__).resolve()
    for _cand in (_here.parent, _here.parent.parent, _here.parent.parent.parent, _here.parent.parent.parent.parent):
        try:
            if ((_cand / "database.py").exists() or (_cand / "models.py").exists()) and str(_cand) not in sys.path:
                sys.path.insert(0, str(_cand))
                break
        except Exception:
            logger.debug(f"Error verificando candidato de ruta {_cand}", exc_info=True)
            continue
except Exception:
    logger.warning("Error al configurar sys.path", exc_info=True)

# HTTP client para probes (con fallback si no está disponible)
try:
    import requests  # type: ignore
except Exception:
    logger.debug("requests no disponible")
    requests = None  # type: ignore

# Cliente de Google Cloud Storage (opcional)
try:
    from google.cloud import storage as gcs_storage  # type: ignore
except Exception:
    logger.debug("google-cloud-storage no disponible")
    gcs_storage = None  # type: ignore

from core.database import DatabaseManager  # type: ignore

try:
    from core.models import Usuario, Pago, MetodoPago, ConceptoPago, Ejercicio, Rutina, RutinaEjercicio  # type: ignore
except Exception:
    logger.error("Error importando modelos core", exc_info=True)
    Usuario = None  # type: ignore
    Pago = None  # type: ignore
    MetodoPago = None  # type: ignore
    ConceptoPago = None  # type: ignore
    Ejercicio = None  # type: ignore
    Rutina = None  # type: ignore
    RutinaEjercicio = None  # type: ignore

try:
    from core.utils import get_gym_name  # type: ignore
except Exception:
    def get_gym_name(default: str = "Gimnasio") -> str:  # type: ignore
        return default

try:
    from core.utils import get_gym_value as _get_gym_value  # type: ignore
    def get_gym_address(default: str = "Dirección del gimnasio") -> str:  # type: ignore
        try:
            v = _get_gym_value("gym_address")
            return (v or default).strip()
        except Exception:
            logger.warning("Error obteniendo gym_address", exc_info=True)
            return default
except Exception:
    def get_gym_address(default: str = "Dirección del gimnasio") -> str:  # type: ignore
        return default

try:
    from core.secure_config import SecureConfig as _SC  # type: ignore
    DEV_PASSWORD = _SC.get_dev_password()
except Exception:
    logger.warning("SecureConfig no disponible", exc_info=True)
    DEV_PASSWORD = ""

try:
    from core.models import Clase, ClaseHorario  # type: ignore
except Exception:
    logger.error("Error importando modelos de Clases", exc_info=True)
    Clase = None  # type: ignore
    ClaseHorario = None  # type: ignore

try:
    from core.payment_manager import PaymentManager  # type: ignore
except Exception:
    logger.error("Error importando PaymentManager", exc_info=True)
    PaymentManager = None  # type: ignore

try:
    from core.utils import get_webapp_base_url  # type: ignore
except Exception:
    def get_webapp_base_url(default: str = "") -> str:  # type: ignore
        import os as _os
        env_url = _os.getenv("WEBAPP_BASE_URL", "").strip()
        if env_url:
            return env_url
        vercel = (_os.getenv("VERCEL_URL") or _os.getenv("VERCEL_BRANCH_URL") or _os.getenv("VERCEL_PROJECT_PRODUCTION_URL") or "").strip()
        if vercel:
            if vercel.startswith("http://") or vercel.startswith("https://"):
                return vercel
            return f"https://{vercel}"
        return (default or "").strip()

# Secreto para enlaces de previsualización (firmas HMAC) reutilizable en entornos serverless
def _get_preview_secret() -> str:
    try:
        env = os.getenv("WEBAPP_PREVIEW_SECRET", "").strip()
        if env:
            return env
    except Exception:
        logger.warning("Error leyendo WEBAPP_PREVIEW_SECRET", exc_info=True)
    # Fallbacks suaves: usar valores ya configurados si existen
    for k in ("SESSION_SECRET", "SECRET_KEY", "VERCEL_GITHUB_COMMIT_SHA"):
        try:
            v = os.getenv(k, "").strip()
            if v:
                return v
        except Exception:
            continue
    try:
        if DEV_PASSWORD:
            return str(DEV_PASSWORD)
    except Exception:
        pass
    return "preview-secret"

def _sign_excel_view(rutina_id: int, weeks: int, filename: str, ts: int, qr_mode: str = "auto", sheet: str | None = None) -> str:
    try:
        qr = str(qr_mode or "inline").strip().lower()
        # Mapear valores antiguos a nuevas ubicaciones
        if qr in ("auto", "real", "preview"):
            qr = "inline"
        if qr not in ("inline", "sheet", "none"):
            qr = "inline"
    except Exception:
        logger.debug("Error normalizando qr_mode en _sign_excel_view", exc_info=True)
        qr = "inline"
    # Incorporar el nombre de hoja como string en la firma (protegido y truncado)
    try:
        sh = (str(sheet).strip()[:64]) if (sheet is not None and str(sheet).strip()) else ""
    except Exception:
        sh = ""
    try:
        base = f"{int(rutina_id)}|{int(weeks)}|{filename}|{int(ts)}|{qr}|{sh}".encode("utf-8")
    except Exception:
        base = f"{rutina_id}|{weeks}|{filename}|{ts}|{qr}|{sh}".encode("utf-8")
    secret = _get_preview_secret().encode("utf-8")
    return hmac.new(secret, base, hashlib.sha256).hexdigest()

# Firma para previsualización efímera (borrador) basada en un payload ID
def _sign_excel_view_draft(payload_id: str, weeks: int, filename: str, ts: int, qr_mode: str = "auto", sheet: str | None = None) -> str:
    try:
        pid = str(payload_id)
    except Exception:
        pid = payload_id
    try:
        qr = str(qr_mode or "inline").strip().lower()
        if qr in ("auto", "real", "preview"):
            qr = "inline"
        if qr not in ("inline", "sheet", "none"):
            qr = "inline"
    except Exception:
        qr = "inline"
    try:
        sh = (str(sheet).strip()[:64]) if (sheet is not None and str(sheet).strip()) else ""
    except Exception:
        sh = ""
    try:
        base = f"{pid}|{int(weeks)}|{filename}|{int(ts)}|{qr}|{sh}".encode("utf-8")
    except Exception:
        base = f"{pid}|{weeks}|{filename}|{ts}|{qr}|{sh}".encode("utf-8")
    secret = _get_preview_secret().encode("utf-8")
    return hmac.new(secret, base, hashlib.sha256).hexdigest()

# Firma stateless basada en payload codificado
def _sign_excel_view_draft_data(data: str, weeks: int, filename: str, ts: int, qr_mode: str = "auto", sheet: str | None = None) -> str:
    try:
        d = str(data)
    except Exception:
        d = data
    try:
        qr = str(qr_mode or "inline").strip().lower()
        if qr in ("auto", "real", "preview"):
            qr = "inline"
        if qr not in ("inline", "sheet", "none"):
            qr = "inline"
    except Exception:
        qr = "inline"
    try:
        sh = (str(sheet).strip()[:64]) if (sheet is not None and str(sheet).strip()) else ""
    except Exception:
        sh = ""
    try:
        base = f"{d}|{int(weeks)}|{filename}|{int(ts)}|{qr}|{sh}".encode("utf-8")
    except Exception:
        base = f"{d}|{weeks}|{filename}|{ts}|{qr}|{sh}".encode("utf-8")
    secret = _get_preview_secret().encode("utf-8")
    return hmac.new(secret, base, hashlib.sha256).hexdigest()

# Construcción segura del nombre de archivo de exportación (incluye N-dias y usuario)
def _sanitize_filename_component(val: Any, max_len: int = 64) -> str:
    try:
        s = str(val or "").strip()
    except Exception:
        s = ""
    if not s:
        return ""
    try:
        s = s.replace(" ", "_")
        # Reemplazar caracteres inválidos comunes
        for ch in ("\\", "/", ":", "*", "?", '"', "<", ">", "|"):
            s = s.replace(ch, "_")
        # Colapsar guiones bajos duplicados
        while "__" in s:
            s = s.replace("__", "_")
        return s[:max_len]
    except Exception:
        return s[:max_len]

def _dias_segment(dias: Any) -> str:
    try:
        d = int(dias)
    except Exception:
        d = 1
    try:
        d = max(1, min(d, 5))
    except Exception:
        d = 1
    return f"{d}-dias"

def _build_excel_export_filename(nombre_rutina: str, dias: Any, usuario_nombre: str) -> str:
    try:
        date_str = datetime.now().strftime("%d-%m-%Y")
    except Exception:
        date_str = ""
    nr = _sanitize_filename_component(nombre_rutina or "rutina", max_len=60) or "rutina"
    seg_d = _dias_segment(dias)
    user_seg = _sanitize_filename_component(usuario_nombre or "", max_len=60)
    parts = ["rutina", nr, seg_d]
    if user_seg:
        parts.append(user_seg)
    if date_str:
        parts.append(date_str)
    base = "_".join([p for p in parts if p])
    # Limitar longitud global y asegurar extensión
    base = base[:150]
    return f"{base}.xlsx"

# PINs en texto plano: sin hashing ni migración; usar la verificación del DatabaseManager directamente.
# Esto hay que corregirlo a futuro si escala y se llegan a usar por ejemplo subdomains, pero por ahora 

# --- Rate limiting simple para login de usuarios (por IP y DNI) ---
_login_attempts_lock = threading.Lock()
_login_attempts_by_ip: Dict[str, list] = {}
_login_attempts_by_dni: Dict[str, list] = {}

def _get_client_ip(request: Request) -> str:
    try:
        trust_proxy = str(os.getenv("PROXY_HEADERS_ENABLED", "0")).strip().lower() in ("1", "true", "yes", "on")
        if trust_proxy:
            xff = request.headers.get("x-forwarded-for")
            if xff:
                try:
                    return xff.split(",")[0].strip()
                except Exception:
                    return xff.strip()
            xri = request.headers.get("x-real-ip")
            if xri:
                return xri.strip()
        c = getattr(request, "client", None)
        if c and getattr(c, "host", None):
            return c.host
        return "0.0.0.0"
    except Exception:
        logger.warning("Error al obtener IP del cliente", exc_info=True)
        return "0.0.0.0"

def _prune_attempts(store: Dict[str, list], window_s: int) -> None:
    try:
        now = time.time()
        for k, lst in list(store.items()):
            try:
                store[k] = [t for t in lst if (now - float(t)) <= window_s]
                if not store[k]:
                    store.pop(k, None)
            except Exception:
                store.pop(k, None)
    except Exception:
        pass

def _is_rate_limited(key: str, store: Dict[str, list], max_attempts: int, window_s: int) -> bool:
    with _login_attempts_lock:
        _prune_attempts(store, window_s)
        lst = store.get(key, [])
        try:
            return len(lst) >= int(max_attempts)
        except Exception:
            return len(lst) >= max_attempts

def _register_attempt(key: str, store: Dict[str, list]) -> None:
    try:
        now = time.time()
        with _login_attempts_lock:
            lst = store.get(key)
            if lst is None:
                store[key] = [now]
            else:
                lst.append(now)
                # Limitar memoria: conservar últimas 50
                if len(lst) > 50:
                    store[key] = lst[-50:]
    except Exception:
        pass

def _clear_attempts(key: str, store: Dict[str, list]) -> None:
    try:
        with _login_attempts_lock:
            store.pop(key, None)
    except Exception:
        pass

# Codificación/decodificación del payload efímero para URLs
def _encode_preview_payload(payload: Dict[str, Any]) -> str:
    try:
        # Compactar estructura para minimizar tamaño de URL
        compact: Dict[str, Any] = {
            "n": payload.get("nombre_rutina"),
            "d": payload.get("descripcion"),
            "ds": payload.get("dias_semana"),
            "c": payload.get("categoria"),
            # Datos de usuario compactados (opcionales)
            # ui: usuario_id, un: nombre de usuario/override, ud: DNI, ut: teléfono
            "ui": (
                (payload.get("usuario_id")
                 if payload.get("usuario_id") is not None else (payload.get("usuario") or {}).get("id"))
            ),
            "un": (
                payload.get("usuario_nombre_override")
                if (payload.get("usuario_nombre_override") not in (None, ""))
                else ((payload.get("usuario") or {}).get("nombre"))
            ),
            "ud": (
                payload.get("usuario_dni")
                if payload.get("usuario_dni") is not None else (payload.get("usuario") or {}).get("dni")
            ),
            "ut": (
                payload.get("usuario_telefono")
                if payload.get("usuario_telefono") is not None else (payload.get("usuario") or {}).get("telefono")
            ),
            "e": [
                [
                    int(x.get("ejercicio_id")),
                    int(x.get("dia_semana", 1)),
                    x.get("series"),
                    x.get("repeticiones"),
                    int(x.get("orden", 1)),
                    # Incluir nombre visible del ejercicio si está disponible
                    (
                        (x.get("nombre_ejercicio"))
                        or (
                            (x.get("ejercicio") or {}).get("nombre")
                            if isinstance(x.get("ejercicio"), dict) else None
                        )
                        or None
                    ),
                ]
                for x in (payload.get("ejercicios") or [])
            ],
        }
        raw = json.dumps(compact, separators=(",", ":")).encode("utf-8")
        comp = zlib.compress(raw, level=6)
        return base64.urlsafe_b64encode(comp).decode("ascii")
    except Exception:
        try:
            return base64.urlsafe_b64encode(json.dumps(payload, separators=(",", ":")).encode("utf-8")).decode("ascii")
        except Exception:
            logger.error("Error codificando preview payload", exc_info=True)
            return ""

def _decode_preview_payload(data: str) -> Optional[Dict[str, Any]]:
    # Intentar descomprimir; si falla, tratar como base64 plano
    try:
        comp = base64.urlsafe_b64decode(str(data))
        try:
            raw = zlib.decompress(comp)
        except Exception:
            raw = comp
        obj = json.loads(raw.decode("utf-8"))
        if not isinstance(obj, dict):
            return None
        # Expandir estructura compacta
        if "e" in obj and isinstance(obj.get("e"), list):
            ejercicios = []
            for arr in (obj.get("e") or []):
                try:
                    item = {
                        "ejercicio_id": int(arr[0]),
                        "dia_semana": int(arr[1]),
                        "series": arr[2],
                        "repeticiones": arr[3],
                        "orden": int(arr[4]),
                    }
                    # Si hay nombre incluido en el array compactado, restaurarlo
                    try:
                        if len(arr) > 5 and arr[5] not in (None, ""):
                            item["nombre_ejercicio"] = str(arr[5])
                            # Alias para compatibilidad con lógica existente que pueda usar nombre_actual
                            item["nombre_actual"] = item["nombre_ejercicio"]
                    except Exception:
                        pass
                    ejercicios.append(item)
                except Exception:
                    continue
            # Reconstruir datos de usuario si están presentes
            ui = obj.get("ui")
            un = obj.get("un")
            ud = obj.get("ud")
            ut = obj.get("ut")
            try:
                ui_int = int(ui) if ui is not None else None
            except Exception:
                ui_int = None
            return {
                "nombre_rutina": obj.get("n"),
                "descripcion": obj.get("d"),
                "dias_semana": obj.get("ds"),
                "categoria": obj.get("c"),
                # Top‑level para compatibilidad con constructores existentes
                "usuario_id": ui_int,
                "usuario_nombre_override": un,
                "usuario_dni": ud,
                "usuario_telefono": ut,
                # Objeto usuario opcional
                "usuario": {
                    "id": ui_int,
                    "nombre": un,
                    "dni": ud,
                    "telefono": ut,
                },
                "ejercicios": ejercicios,
            }
        return obj
    except Exception:
        logger.warning("Error decodificando preview payload", exc_info=True)
        return None

# Almacenamiento efímero de borradores para previsualización
_excel_preview_drafts_lock = threading.RLock()
_excel_preview_drafts: Dict[str, Dict[str, Any]] = {}

def _clean_preview_drafts() -> None:
    try:
        now = int(time.time())
        to_del = []
        with _excel_preview_drafts_lock:
            for k, v in list(_excel_preview_drafts.items()):
                exp = int(v.get("expires_at", 0) or 0)
                if exp and now > exp:
                    to_del.append(k)
            for k in to_del:
                _excel_preview_drafts.pop(k, None)
    except Exception:
        pass

def _save_excel_preview_draft(payload: Dict[str, Any]) -> str:
    # Limitar tamaño para evitar abusos
    try:
        raw_len = len(json.dumps(payload, ensure_ascii=False))
        if raw_len > 500_000:
            raise ValueError("Payload demasiado grande para previsualización")
    except Exception:
        pass
    pid = secrets.token_urlsafe(18)
    now = int(time.time())
    entry = {
        "payload": payload,
        "created_at": now,
        "expires_at": now + 600,  # 10 minutos
    }
    with _excel_preview_drafts_lock:
        _excel_preview_drafts[pid] = entry
        _clean_preview_drafts()
    return pid

def _get_excel_preview_draft(pid: str) -> Optional[Dict[str, Any]]:
    try:
        with _excel_preview_drafts_lock:
            entry = _excel_preview_drafts.get(str(pid))
            if not entry:
                return None
            exp = int(entry.get("expires_at", 0) or 0)
            now = int(time.time())
            if exp and now > exp:
                _excel_preview_drafts.pop(str(pid), None)
                return None
            return entry.get("payload")
    except Exception:
        return None

# Almacenamiento efímero de rutinas por UUID para QR en previsualización
_excel_preview_routines_lock = threading.RLock()
_excel_preview_routines: Dict[str, Dict[str, Any]] = {}

# Catálogo de ejercicios para enriquecer video_url/mime en previsualización
_ejercicios_catalog_lock = threading.RLock()
_ejercicios_catalog_cache: Dict[str, Any] = {"ts": 0, "by_id": {}, "by_name": {}}

def _load_ejercicios_catalog(force: bool = False) -> Dict[str, Any]:
    """Carga catálogo de ejercicios con video_url y video_mime desde DB o JSON.

    TTL simple para evitar consultas repetidas.
    """
    try:
        now = int(time.time())
        with _ejercicios_catalog_lock:
            ts = int(_ejercicios_catalog_cache.get("ts", 0) or 0)
            if (not force) and ts and (now - ts) < 300:
                return _ejercicios_catalog_cache
            by_id: Dict[int, Dict[str, Any]] = {}
            by_name: Dict[str, Dict[str, Any]] = {}

            rows = None
            db = _get_db()
            if db is not None:
                try:
                    with db.get_connection_context() as conn:  # type: ignore
                        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                        cols = db.get_table_columns('ejercicios') or []  # type: ignore
                        select_cols = ['id', 'nombre']
                        if 'video_url' in cols:
                            select_cols.append('video_url')
                        if 'video_mime' in cols:
                            select_cols.append('video_mime')
                        cur.execute(f"SELECT {', '.join(select_cols)} FROM ejercicios")
                        rows = cur.fetchall()
                except Exception:
                    logger.warning("Error cargando ejercicios desde DB para catálogo", exc_info=True)
                    rows = None

            if rows:
                for r in rows:
                    try:
                        eid = int(r.get('id') or 0)
                    except Exception:
                        eid = 0
                    name = (r.get('nombre') or '').strip().lower()
                    info = {
                        'video_url': r.get('video_url'),
                        'video_mime': r.get('video_mime'),
                    }
                    if eid:
                        by_id[eid] = info
                    if name:
                        by_name[name] = info
            else:
                # Fallback a ejercicios.json
                try:
                    p = Path(__file__).resolve().parent.parent / 'ejercicios.json'
                    data = json.loads(p.read_text(encoding='utf-8'))
                    for it in (data or []):
                        try:
                            eid = int(it.get('id') or 0)
                        except Exception:
                            eid = 0
                        name = (it.get('nombre') or '').strip().lower()
                        info = {
                            'video_url': it.get('video_url'),
                            'video_mime': it.get('video_mime'),
                        }
                        if eid:
                            by_id[eid] = info
                        if name:
                            by_name[name] = info
                except Exception:
                    pass

            _ejercicios_catalog_cache = {"ts": now, "by_id": by_id, "by_name": by_name}
            return _ejercicios_catalog_cache
    except Exception:
        logger.error("Error general en _load_ejercicios_catalog", exc_info=True)
        return {"ts": 0, "by_id": {}, "by_name": {}}

def _lookup_video_info(ejercicio_id: Any, nombre: Optional[str]) -> Dict[str, Any]:
    """Busca video_url/mime por id o nombre en el catálogo."""
    try:
        cat = _load_ejercicios_catalog()
        info = None
        if ejercicio_id is not None:
            try:
                info = cat.get('by_id', {}).get(int(ejercicio_id))
            except Exception:
                info = None
        if (not info) and nombre:
            try:
                info = cat.get('by_name', {}).get(str(nombre).strip().lower())
            except Exception:
                info = None
        return info or {'video_url': None, 'video_mime': None}
    except Exception:
        return {'video_url': None, 'video_mime': None}

def _clean_preview_routines() -> None:
    try:
        now = int(time.time())
        to_del = []
        with _excel_preview_routines_lock:
            for k, v in list(_excel_preview_routines.items()):
                exp = int(v.get("expires_at", 0) or 0)
                if exp and now > exp:
                    to_del.append(k)
            for k in to_del:
                _excel_preview_routines.pop(k, None)
    except Exception:
        pass

def _save_excel_preview_routine(uuid_str: str, rutina_dict: Dict[str, Any]) -> None:
    if not uuid_str:
        return
    now = int(time.time())
    entry = {
        "rutina": rutina_dict,
        "created_at": now,
        "expires_at": now + 600,  # 10 minutos
    }
    with _excel_preview_routines_lock:
        _excel_preview_routines[str(uuid_str)] = entry
        _clean_preview_routines()

def _get_excel_preview_routine(uuid_str: str) -> Optional[Dict[str, Any]]:
    try:
        with _excel_preview_routines_lock:
            entry = _excel_preview_routines.get(str(uuid_str))
            if not entry:
                return None
            exp = int(entry.get("expires_at", 0) or 0)
            now = int(time.time())
            if exp and now > exp:
                _excel_preview_routines.pop(str(uuid_str), None)
                return None
            return entry.get("rutina")
    except Exception:
        return None

def _build_rutina_from_draft(payload: Dict[str, Any]) -> tuple:
    """Construye Rutina, Usuario y ejercicios_por_dia desde un payload efímero."""
    # Usuario
    u_raw = payload.get("usuario") or {}
    # Normalizar nombre del usuario con múltiples posibles claves de entrada
    try:
        u_nombre = (
            (u_raw.get("nombre") or u_raw.get("Nombre"))
            or (payload.get("usuario_nombre") or payload.get("nombre_usuario"))
            or (payload.get("usuarioNombre") or payload.get("NombreUsuario"))
            or (payload.get("usuario_nombre_override") or None)
        )
        u_nombre = (u_nombre or "").strip()
    except Exception:
        u_nombre = ""
    # Intentar obtener usuario_id y completar datos desde la base si el nombre no está
    try:
        u_id_raw = (
            u_raw.get("id")
            or u_raw.get("usuario_id")
            or payload.get("usuario_id")
            or ((payload.get("rutina") or {}).get("usuario_id") if isinstance(payload.get("rutina"), dict) else None)
        )
        u_id = int(u_id_raw) if u_id_raw is not None else None
    except Exception:
        u_id = None
    # DNI y Teléfono primarios del payload
    try:
        u_dni = (u_raw.get("dni") or u_raw.get("DNI") or payload.get("usuario_dni") or payload.get("dni_usuario") or None)
    except Exception:
        u_dni = None
    try:
        u_tel = (u_raw.get("telefono") or u_raw.get("Teléfono") or payload.get("usuario_telefono") or "")
    except Exception:
        u_tel = ""
    # Si tenemos usuario_id y el nombre está vacío, consultar DB para completar
    if (not u_nombre) and (u_id is not None):
        db = _get_db()
        try:
            if db is not None:
                try:
                    u_obj = db.obtener_usuario(int(u_id))  # type: ignore
                except Exception:
                    logger.warning(f"Error obteniendo usuario {u_id} en _build_rutina_from_draft", exc_info=True)
                    u_obj = None
                if u_obj is not None:
                    try:
                        u_nombre = (getattr(u_obj, "nombre", "") or "").strip() or u_nombre
                        u_dni = getattr(u_obj, "dni", None) if (getattr(u_obj, "dni", None) or None) else u_dni
                        u_tel = getattr(u_obj, "telefono", "") if (getattr(u_obj, "telefono", "") or "") else u_tel
                    except Exception:
                        logger.warning("Error leyendo atributos de usuario objeto", exc_info=True)
                # Si aún sin nombre, intentar JOIN directo por rutina_id si existe en payload
                if not u_nombre:
                    try:
                        r_id_raw = payload.get("rutina_id") or ((payload.get("rutina") or {}).get("id") if isinstance(payload.get("rutina"), dict) else None)
                        r_id = int(r_id_raw) if r_id_raw is not None else None
                    except Exception:
                        r_id = None
                    if r_id is not None:
                        try:
                            with db.get_connection_context() as conn:  # type: ignore
                                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                                cur.execute(
                                    """
                                    SELECT COALESCE(u.nombre,'') AS usuario_nombre,
                                           COALESCE(u.dni,'')     AS dni,
                                           COALESCE(u.telefono,'') AS telefono
                                    FROM rutinas r
                                    LEFT JOIN usuarios u ON u.id = r.usuario_id
                                    WHERE r.id = %s
                                    """,
                                    (int(r_id),)
                                )
                                row = cur.fetchone() or {}
                                _u_nombre = (row.get("usuario_nombre") or "").strip()
                                _u_dni = (row.get("dni") or "").strip() or None
                                _u_tel = (row.get("telefono") or "").strip()
                                if _u_nombre:
                                    u_nombre = _u_nombre
                                    u_dni = _u_dni if _u_dni is not None else u_dni
                                    u_tel = _u_tel or u_tel
                        except Exception:
                            logger.warning("Error consultando usuario por rutina_id", exc_info=True)
        except Exception:
            logger.warning("Error general resolviendo usuario en _build_rutina_from_draft", exc_info=True)
    # Si sigue sin nombre y SIN usuario_id, usar fallback 'Plantilla' (estamos en modo plantilla)
    if not u_nombre and u_id is None:
        u_nombre = "Plantilla"
    try:
        usuario = Usuario(
            nombre=u_nombre,
            dni=u_dni,
            telefono=u_tel,
        )
    except Exception:
        class _Usr:
            def __init__(self, nombre: str):
                self.nombre = nombre
                self.dni = None
                self.telefono = ""
        # Respetar el usuario_id: solo usar 'Plantilla' cuando no hay usuario asociado
        _nombre_final = u_nombre if (u_nombre or (u_id is not None)) else "Plantilla"
        usuario = _Usr(_nombre_final)
    # Rutina básica
    r_raw = payload.get("rutina") or payload
    try:
        rutina = Rutina(
            nombre_rutina=(r_raw.get("nombre_rutina") or r_raw.get("nombre") or "Rutina"),
            descripcion=r_raw.get("descripcion") or None,
            dias_semana=int(r_raw.get("dias_semana") or 1),
            categoria=(r_raw.get("categoria") or "general"),
        )
    except Exception:
        class _Rut:
            def __init__(self, nombre: str):
                self.nombre_rutina = nombre
                self.descripcion = r_raw.get("descripcion")
                self.dias_semana = int(r_raw.get("dias_semana") or 1)
                self.categoria = r_raw.get("categoria") or "general"
                self.ejercicios = []
        rutina = _Rut(r_raw.get("nombre_rutina") or r_raw.get("nombre") or "Rutina")
    # Permitir atributo de semana actual para placeholders
    try:
        semana_val = int(payload.get("semana") or r_raw.get("semana") or 1)
    except Exception:
        semana_val = 1
    try:
        setattr(rutina, "semana", semana_val)
    except Exception:
        pass
    # Asegurar un uuid efímero para previsualización (habilita qr_link en Excel)
    try:
        ruuid = (
            (r_raw.get("uuid_rutina") or r_raw.get("uuid"))
            or (payload.get("uuid_rutina") or payload.get("uuid"))
        )
        ruuid = str(ruuid).strip() if ruuid else ""
    except Exception:
        ruuid = ""
    if not ruuid:
        try:
            ruuid = str(uuid.uuid4())
        except Exception:
            ruuid = ""
    try:
        if ruuid:
            setattr(rutina, "uuid_rutina", ruuid)
            setattr(rutina, "uuid", ruuid)
    except Exception:
        pass
    ejercicios: list = []
    # Contadores por día para generar nombres por defecto legibles
    day_counts: Dict[int, int] = {}
    # Fuente 1: lista plana 'ejercicios'
    items = payload.get("ejercicios") or []
    if isinstance(items, list) and items:
        for idx, it in enumerate(items):
            try:
                dia = it.get("dia_semana") if isinstance(it, dict) else None
                if dia is None:
                    dia = it.get("dia") if isinstance(it, dict) else None
                try:
                    dia = int(dia) if dia is not None else 1
                except Exception:
                    dia = 1
                nombre_e = (it.get("nombre_ejercicio") or it.get("nombre") or None)
                series = it.get("series")
                repes = it.get("repeticiones") or it.get("reps")
                orden = it.get("orden")
                ejercicio_id = it.get("ejercicio_id")
                ej_obj_raw = it.get("ejercicio") or None
                re = RutinaEjercicio(
                    rutina_id=getattr(rutina, "id", 0) or 0,
                    ejercicio_id=int(ejercicio_id) if (isinstance(ejercicio_id, (int, str)) and str(ejercicio_id).isdigit()) else 0,
                    dia_semana=dia,
                    series=str(series) if series is not None else "",
                    repeticiones=str(repes) if repes is not None else "",
                    orden=int(orden) if (isinstance(orden, (int, str)) and str(orden).isdigit()) else idx + 1,
                )
                # Enriquecer con video_url si corresponde
                info = _lookup_video_info(ejercicio_id, nombre_e or (ej_obj_raw.get("nombre") if isinstance(ej_obj_raw, dict) else None))
                vid = (ej_obj_raw.get("video_url") if isinstance(ej_obj_raw, dict) else None) or info.get("video_url")
                mime = (ej_obj_raw.get("video_mime") if isinstance(ej_obj_raw, dict) else None) or info.get("video_mime")
                if ej_obj_raw and isinstance(ej_obj_raw, dict):
                    try:
                        re.ejercicio = Ejercicio(nombre=(ej_obj_raw.get("nombre") or nombre_e or "Ejercicio"), video_url=vid, video_mime=mime)  # type: ignore
                    except Exception:
                        try:
                            re.ejercicio = Ejercicio(nombre=(ej_obj_raw.get("nombre") or nombre_e or "Ejercicio"))  # type: ignore
                        except Exception:
                            pass
                if nombre_e:
                    try:
                        setattr(re, "nombre_ejercicio", nombre_e)
                    except Exception:
                        pass
                # Fallback: asignar nombre por defecto si no viene en el payload
                try:
                    has_nombre = bool(getattr(re, "nombre_ejercicio", None)) or bool(getattr(getattr(re, "ejercicio", None), "nombre", None))
                except Exception:
                    has_nombre = False
                if not has_nombre:
                    try:
                        day_counts[dia] = day_counts.get(int(dia), 0) + 1
                        setattr(re, "nombre_ejercicio", f"Ejercicio {day_counts[int(dia)]}")
                    except Exception:
                        try:
                            setattr(re, "nombre_ejercicio", "Ejercicio")
                        except Exception:
                            pass
                ejercicios.append(re)
            except Exception:
                logger.warning("Error procesando item de ejercicio (lista plana)", exc_info=True)
                continue
    # Fuente 2: por días 'dias' (dict o lista)
    dias = payload.get("dias") or payload.get("dias_semana_detalle") or []
    if isinstance(dias, dict):
        for k, arr in dias.items():
            try:
                dia = int(k)
            except Exception:
                continue
            if not isinstance(arr, list):
                continue
            for j, it in enumerate(arr):
                try:
                    nombre_e = (it.get("nombre_ejercicio") or it.get("nombre") or None)
                    series = it.get("series")
                    repes = it.get("repeticiones") or it.get("reps")
                    orden = it.get("orden")
                    ejercicio_id = it.get("ejercicio_id")
                    ej_obj_raw = it.get("ejercicio") or None
                    re = RutinaEjercicio(
                        rutina_id=getattr(rutina, "id", 0) or 0,
                        ejercicio_id=int(ejercicio_id) if (isinstance(ejercicio_id, (int, str)) and str(ejercicio_id).isdigit()) else 0,
                        dia_semana=dia,
                        series=str(series) if series is not None else "",
                        repeticiones=str(repes) if repes is not None else "",
                        orden=int(orden) if (isinstance(orden, (int, str)) and str(orden).isdigit()) else j + 1,
                    )
                    info = _lookup_video_info(ejercicio_id, nombre_e or (ej_obj_raw.get("nombre") if isinstance(ej_obj_raw, dict) else None))
                    vid = (ej_obj_raw.get("video_url") if isinstance(ej_obj_raw, dict) else None) or info.get("video_url")
                    mime = (ej_obj_raw.get("video_mime") if isinstance(ej_obj_raw, dict) else None) or info.get("video_mime")
                    if ej_obj_raw and isinstance(ej_obj_raw, dict):
                        try:
                            re.ejercicio = Ejercicio(nombre=(ej_obj_raw.get("nombre") or nombre_e or "Ejercicio"), video_url=vid, video_mime=mime)  # type: ignore
                        except Exception:
                            try:
                                re.ejercicio = Ejercicio(nombre=(ej_obj_raw.get("nombre") or nombre_e or "Ejercicio"))  # type: ignore
                            except Exception:
                                pass
                    if nombre_e:
                        try:
                            setattr(re, "nombre_ejercicio", nombre_e)
                        except Exception:
                            pass
                    # Fallback de nombre si viene vacío
                    try:
                        has_nombre = bool(getattr(re, "nombre_ejercicio", None)) or bool(getattr(getattr(re, "ejercicio", None), "nombre", None))
                    except Exception:
                        has_nombre = False
                    if not has_nombre:
                        try:
                            day_counts[dia] = day_counts.get(int(dia), 0) + 1
                            setattr(re, "nombre_ejercicio", f"Ejercicio {day_counts[int(dia)]}")
                        except Exception:
                            try:
                                setattr(re, "nombre_ejercicio", "Ejercicio")
                            except Exception:
                                pass
                    ejercicios.append(re)
                except Exception:
                    logger.warning("Error procesando item de ejercicio (dict dias)", exc_info=True)
                    continue
    elif isinstance(dias, list):
        for d in dias:
            try:
                dia = d.get("numero") if isinstance(d, dict) else None
                if dia is None:
                    dia = d.get("dia") if isinstance(d, dict) else None
                try:
                    dia = int(dia) if dia is not None else 1
                except Exception:
                    dia = 1
                arr = d.get("ejercicios") or []
                if not isinstance(arr, list):
                    continue
                for j, it in enumerate(arr):
                    try:
                        nombre_e = (it.get("nombre_ejercicio") or it.get("nombre") or None)
                        series = it.get("series")
                        repes = it.get("repeticiones") or it.get("reps")
                        orden = it.get("orden")
                        ejercicio_id = it.get("ejercicio_id")
                        ej_obj_raw = it.get("ejercicio") or None
                        re = RutinaEjercicio(
                            rutina_id=getattr(rutina, "id", 0) or 0,
                            ejercicio_id=int(ejercicio_id) if (isinstance(ejercicio_id, (int, str)) and str(ejercicio_id).isdigit()) else 0,
                            dia_semana=dia,
                            series=str(series) if series is not None else "",
                            repeticiones=str(repes) if repes is not None else "",
                            orden=int(orden) if (isinstance(orden, (int, str)) and str(orden).isdigit()) else j + 1,
                        )
                        if ej_obj_raw and isinstance(ej_obj_raw, dict):
                            try:
                                re.ejercicio = Ejercicio(nombre=(ej_obj_raw.get("nombre") or nombre_e or "Ejercicio"))
                            except Exception:
                                pass
                        if nombre_e:
                            try:
                                setattr(re, "nombre_ejercicio", nombre_e)
                            except Exception:
                                pass
                        # Fallback de nombre si falta
                        try:
                            has_nombre = bool(getattr(re, "nombre_ejercicio", None)) or bool(getattr(getattr(re, "ejercicio", None), "nombre", None))
                        except Exception:
                            has_nombre = False
                        if not has_nombre:
                            try:
                                day_counts[dia] = day_counts.get(int(dia), 0) + 1
                                setattr(re, "nombre_ejercicio", f"Ejercicio {day_counts[int(dia)]}")
                            except Exception:
                                try:
                                    setattr(re, "nombre_ejercicio", "Ejercicio")
                                except Exception:
                                    pass
                        ejercicios.append(re)
                    except Exception:
                        logger.warning("Error procesando item de ejercicio (lista dias)", exc_info=True)
                        continue
            except Exception:
                continue
    # Adjuntar y agrupar
    try:
        rutina.ejercicios = ejercicios
    except Exception:
        logger.warning("Error adjuntando ejercicios a rutina", exc_info=True)
    ejercicios_por_dia = _build_exercises_by_day(rutina)
    return rutina, usuario, ejercicios_por_dia

# Sistema de túneles removido - aplicación usa base de datos Neon única sin túneles

from .qss_to_css import generate_css_from_qss, read_theme_vars

_db: Optional[DatabaseManager] = None
# Bloqueo para inicialización segura de la instancia global de DB y evitar condiciones de carrera
_db_lock = threading.RLock()
_db_initializing = False

# Gestor de pagos (se instancia perezosamente sobre la DB)
_pm: Optional[PaymentManager] = None

# Gestor de rutinas (plantillas y exportación)
try:
    from core.routine_manager import RoutineTemplateManager, create_routine_manager  # type: ignore
except Exception:
    RoutineTemplateManager = None  # type: ignore
    create_routine_manager = None  # type: ignore
_rm: Optional[RoutineTemplateManager] = None

def _get_pm() -> Optional[PaymentManager]:
    global _pm
    try:
        if _pm is not None:
            return _pm
        db = _get_db()
        if db is None or PaymentManager is None:
            return None
        _pm = PaymentManager(db)
        return _pm
    except Exception:
        return None

# Gestor de rutinas: getter perezoso ligado a la misma DB
def _get_rm() -> Optional[RoutineTemplateManager]:
    global _rm
    try:
        if _rm is not None:
            return _rm
        # Validar dependencias
        if RoutineTemplateManager is None or create_routine_manager is None:
            return None
        db = _get_db()
        if db is None:
            return None
        _rm = create_routine_manager(database_manager=db)
        return _rm
    except Exception:
        return None

CURRENT_TENANT: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar("CURRENT_TENANT", default=None)
_tenant_dbs: Dict[str, DatabaseManager] = {}
_tenant_lock = threading.RLock()

def _get_multi_tenant_mode() -> bool:
    try:
        v = os.getenv("MULTI_TENANT_MODE", "false").strip().lower()
        return v in ("1", "true", "yes", "on")
    except Exception:
        return False

def _get_request_host(request: Request) -> str:
    try:
        h = request.headers.get("x-forwarded-host") or request.headers.get("host") or ""
        h = h.strip()
        if h:
            return h.split(":")[0].strip().lower()
        try:
            return (request.url.hostname or "").strip().lower()
        except Exception:
            return ""
    except Exception:
        return ""

def _extract_tenant_from_host(host: str) -> Optional[str]:
    try:
        base = os.getenv("TENANT_BASE_DOMAIN", "gymms-motiona.xyz").strip().lower().lstrip(".")
    except Exception:
        base = "gymms-motiona.xyz"
    h = (host or "").strip().lower()
    if not h:
        return None
    if "localhost" in h or h.endswith(".localhost"):
        return None
    def _extract_with_base(hh: str, bb: str) -> Optional[str]:
        if not bb or not hh.endswith(bb):
            return None
        try:
            pref = hh[: max(0, len(hh) - len(bb))].rstrip(".")
        except Exception:
            pref = ""
        if not pref:
            return None
        try:
            s = pref.split(".")[0].strip()
        except Exception:
            s = pref
        if not s:
            return None
        try:
            if s.lower() == "www":
                return None
        except Exception:
            pass
        return s
    sub = _extract_with_base(h, base)
    if sub:
        return sub
    try:
        v = (os.getenv("VERCEL_URL") or os.getenv("VERCEL_BRANCH_URL") or os.getenv("VERCEL_PROJECT_PRODUCTION_URL") or "").strip()
        if v:
            import urllib.parse as _up
            try:
                u = _up.urlparse(v if (v.startswith("http://") or v.startswith("https://")) else ("https://" + v))
                vb = (u.hostname or "").strip().lower()
            except Exception:
                vb = v.split("/")[0].strip().lower()
            if vb:
                sub = _extract_with_base(h, vb)
                if sub:
                    return sub
    except Exception:
        pass
    return None

def _resolve_base_db_params() -> Dict[str, Any]:
    host = os.getenv("DB_HOST", "localhost").strip()
    try:
        port = int(os.getenv("DB_PORT", 5432))
    except Exception:
        port = 5432
    user = os.getenv("DB_USER", "postgres").strip()
    password = os.getenv("DB_PASSWORD", "")
    sslmode = os.getenv("DB_SSLMODE", "prefer").strip()
    try:
        connect_timeout = int(os.getenv("DB_CONNECT_TIMEOUT", 10))
    except Exception:
        connect_timeout = 10
    application_name = os.getenv("DB_APPLICATION_NAME", "gym_management_system").strip()
    database = os.getenv("DB_NAME", "gimnasio").strip()
    try:
        h = host.lower()
        if ("neon.tech" in h) or ("neon" in h):
            if not sslmode or sslmode.lower() in ("disable", "prefer"):
                sslmode = "require"
    except Exception:
        pass
    params: Dict[str, Any] = {
        "host": host,
        "port": port,
        "database": database,
        "user": user,
        "password": password,
        "sslmode": sslmode,
        "connect_timeout": connect_timeout,
        "application_name": application_name,
    }
    return params

def _get_db_for_tenant(tenant: str) -> Optional[DatabaseManager]:
    t = (tenant or "").strip().lower()
    if not t:
        return None
    with _tenant_lock:
        dm = _tenant_dbs.get(t)
        if dm is not None:
            return dm
        base = _resolve_base_db_params()
        db_name = None
        adm = _get_admin_db_manager()
        if adm is not None:
            try:
                with adm.db.get_connection_context() as conn:  # type: ignore
                    cur = conn.cursor()
                    cur.execute("SELECT db_name FROM gyms WHERE subdominio = %s", (t,))
                    row = cur.fetchone()
                    if row:
                        db_name = str(row[0] or "").strip()
            except Exception:
                db_name = None
        if not db_name:
            return None
        base["database"] = db_name
        try:
            dm = DatabaseManager(connection_params=base)  # type: ignore
        except Exception:
            return None
        try:
            with dm.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor()
                cur.execute("SELECT 1")
                _ = cur.fetchone()
        except Exception:
            return None
        _tenant_dbs[t] = dm
        return dm

_admin_db_cached = None

def _get_admin_db_manager():
    global _admin_db_cached
    if _admin_db_cached is not None:
        return _admin_db_cached
    try:
        from core.services.admin_service import AdminService
        params = AdminService.resolve_admin_db_params()
        db = DatabaseManager(connection_params=params)
        _admin_db_cached = AdminService(db)
        return _admin_db_cached
    except Exception:
        return None

def _is_tenant_suspended(tenant: str) -> bool:
    adm = _get_admin_db_manager()
    if adm is None:
        return False
    try:
        return bool(adm.is_gym_suspended(tenant))
    except Exception:
        return False

def _get_tenant_maintenance_message(tenant: str):
    adm = _get_admin_db_manager()
    if adm is None:
        return None
    try:
        return adm.get_mantenimiento(tenant)
    except Exception:
        return None

# Ajuste de stdout/stderr para ejecutables sin consola en Windows (runw.exe)
# Evita fallos de configuración de logging en Uvicorn cuando sys.stdout/sys.stderr es None.
try:
    if os.name == "nt":
        import os as _os
        import io as _io
        # Asegurar stdout/stderr válidos aunque estén cerrados en ejecutables sin consola
        _sout = getattr(sys, "stdout", None)
        try:
            if _sout is None or getattr(_sout, "closed", False):
                sys.stdout = open(_os.devnull, "w", encoding="utf-8")
            elif hasattr(_sout, "buffer") and not isinstance(_sout, _io.TextIOWrapper):
                sys.stdout = _io.TextIOWrapper(_sout.buffer, encoding="utf-8", errors="replace")
        except Exception:
            pass
        _serr = getattr(sys, "stderr", None)
        try:
            if _serr is None or getattr(_serr, "closed", False):
                sys.stderr = open(_os.devnull, "w", encoding="utf-8")
            elif hasattr(_serr, "buffer") and not isinstance(_serr, _io.TextIOWrapper):
                sys.stderr = _io.TextIOWrapper(_serr.buffer, encoding="utf-8", errors="replace")
        except Exception:
            pass
        # Forzar política de event loop compatible en Windows a nivel global
        try:
            import asyncio as _asyncio
            _asyncio.set_event_loop_policy(_asyncio.WindowsSelectorEventLoopPolicy())
        except Exception:
            pass
except Exception:
    pass

# Fallback defensivo: rebind local de print a versión segura que no falle
# si stdout/stderr está cerrado en ejecutables sin consola.
try:
    import builtins as _builtins
    def _safe_print(*args, **kwargs):
        try:
            _builtins.print(*args, **kwargs)
        except Exception:
            try:
                import logging as _logging
                # Registrar el mensaje concatenado para no perder diagnóstico
                _logging.info(" ".join(str(a) for a in args))
            except Exception:
                # No romper si logging aún no está configurado
                pass
    print = _safe_print  # type: ignore
except Exception:
    pass

def _compute_base_dir() -> Path:
    """Determina la carpeta base desde la cual resolver recursos.
    - En ejecutable PyInstaller (onedir): junto al exe.
    - En modo onefile: carpeta temporal _MEIPASS.
    - En desarrollo: raíz del proyecto.
    """
    try:
        if getattr(sys, "frozen", False):
            exe_dir = Path(sys.executable).resolve().parent
            meipass = getattr(sys, "_MEIPASS", None)
            if meipass:
                return Path(meipass)
            return exe_dir
    except Exception:
        pass
    # Desarrollo: carpeta del proyecto
    try:
        # server.py está en webapp/, subimos un nivel
        return Path(__file__).resolve().parent.parent
    except Exception:
        return Path('.')

def _resolve_existing_dir(*parts: str) -> Path:
    candidates = []
    try:
        if parts and parts[0] == "webapp":
            try:
                local = Path(__file__).resolve().parent.joinpath(*parts[1:])
                candidates.append(local)
            except Exception:
                pass
    except Exception:
        pass
    try:
        candidates.append(BASE_DIR.joinpath(*parts))
    except Exception:
        pass
    try:
        exe_dir = Path(sys.executable).resolve().parent
        candidates.append(exe_dir.joinpath(*parts))
    except Exception:
        pass
    try:
        repo_root = Path(__file__).resolve().parent.parent.parent
        candidates.append(repo_root.joinpath(*parts))
    except Exception:
        pass
    try:
        proj_root = Path(__file__).resolve().parent.parent
        candidates.append(proj_root.joinpath(*parts))
    except Exception:
        pass
    for c in candidates:
        try:
            if c.exists():
                return c
        except Exception:
            continue
    return candidates[0] if candidates else Path(*parts)

# Helper para secreto de sesion estable

def _get_session_secret() -> str:
    # 1) Prefer an explicit, stable secret via environment
    try:
        env = os.getenv("WEBAPP_SECRET_KEY", "").strip()
        if env:
            return env
    except Exception:
        pass

    # 2) In serverless environments (e.g., Vercel) the filesystem is ephemeral.
    #    Avoid generating a per-process random secret which invalidates sessions
    #    across requests. Instead derive a stable fallback from existing env vars.
    try:
        if os.getenv("VERCEL") or os.getenv("VERCEL_ENV"):
            base = (os.getenv("WHATSAPP_APP_SECRET", "") + "|" + os.getenv("WHATSAPP_VERIFY_TOKEN", "")).strip()
            if base:
                import hashlib
                return hashlib.sha256(base.encode("utf-8")).hexdigest()
    except Exception:
        pass
    # 3) Non-serverless: try to read/persist in config/config.json for stability
    try:
        from core.utils import resource_path  # type: ignore
        cfg_path = resource_path("config/config.json")
        cfg = {}
        if os.path.exists(cfg_path):
            import json as _json
            try:
                with open(cfg_path, "r", encoding="utf-8") as f:
                    cfg = _json.load(f) or {}
                if not isinstance(cfg, dict):
                    cfg = {}
            except Exception:
                cfg = {}
        secret = str(cfg.get("webapp_session_secret") or cfg.get("session_secret") or "").strip()
        if not secret:
            import secrets as _secrets
            secret = _secrets.token_urlsafe(32)
            cfg["webapp_session_secret"] = secret
            try:
                with open(cfg_path, "w", encoding="utf-8") as f:
                    _json.dump(cfg, f, ensure_ascii=False, indent=2)
            except Exception:
                # Si no podemos persistir, devolver el generado
                pass
        return secret
    except Exception:
        pass
    # 4) Absolute last resort: ephemeral secret (will reset sessions on process restart)
    import secrets as _secrets
    return _secrets.token_urlsafe(32)

# Inicialización de la app web
app = FastAPI(
    title="GymMS WebApp",
    version="2.0",
    # Permite servir detrás de reverse proxy con subpath
    root_path=os.getenv("ROOT_PATH", "").strip(),
)
app.add_middleware(SessionMiddleware, secret_key=_get_session_secret(), https_only=True, same_site="lax")
try:
    _base_dom = (os.getenv("TENANT_BASE_DOMAIN") or "").strip().lower()
    _allow_hosts = [f"*.{_base_dom}", _base_dom, "localhost", "127.0.0.1", "*.vercel.app"] if _base_dom else ["*"]
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=_allow_hosts)
except Exception:
    pass
try:
    _cors_origins = []
    if _base_dom:
        _cors_origins = [f"https://{_base_dom}", f"https://*.{_base_dom}"]
    app.add_middleware(CORSMiddleware, allow_origins=_cors_origins or ["*"], allow_credentials=True, allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"], allow_headers=["*"])
except Exception:
    pass

class TenantMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: Callable):
        sub = None
        token = None
        if _get_multi_tenant_mode():
            host = _get_request_host(request)
            sub = _extract_tenant_from_host(host)
        else:
            try:
                base = _resolve_base_db_params()
                dbn = str((base.get("database") or "")).strip()
            except Exception:
                dbn = ""
            if not sub and dbn:
                try:
                    adm = _get_admin_db_manager()
                except Exception:
                    adm = None
                if adm is not None:
                    try:
                        with adm.db.get_connection_context() as conn:  # type: ignore
                            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                            cur.execute("SELECT subdominio FROM gyms WHERE db_name = %s", (dbn,))
                            row = cur.fetchone() or {}
                            sub = str(row.get("subdominio") or "").strip().lower() or None
                    except Exception:
                        sub = None
        if sub:
            token = CURRENT_TENANT.set(sub)
            try:
                request.state.tenant = sub
            except Exception:
                pass
            try:
                path = str(getattr(request.url, "path", "/"))
                adm = _get_admin_db_manager()
                active_maint = False
                maint_msg = None
                maint_until = None
                if adm is not None:
                    try:
                        with adm.db.get_connection_context() as conn:  # type: ignore
                            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                            cur.execute("SELECT status, suspended_reason, suspended_until FROM gyms WHERE subdominio = %s", (str(sub).strip().lower(),))
                            row = cur.fetchone() or {}
                            st = str((row.get("status") or "")).lower()
                            maint_msg = row.get("suspended_reason")
                            maint_until = row.get("suspended_until")
                            if st == "maintenance":
                                try:
                                    from datetime import datetime, timezone
                                    if maint_until:
                                        dt = maint_until if hasattr(maint_until, "tzinfo") else datetime.fromisoformat(str(maint_until))
                                        now = datetime.utcnow().replace(tzinfo=timezone.utc)
                                        active_maint = bool(dt <= now)
                                    else:
                                        active_maint = True
                                except Exception:
                                    active_maint = True
                    except Exception:
                        active_maint = False
                if _is_tenant_suspended(sub):
                    info = {} #_get_tenant_suspension_info(sub) or {} # TODO: Fix this
                    allow = path.startswith("/static/") or (path == "/favicon.ico")
                    if not allow:
                        theme_vars = {} #_resolve_theme_vars()
                        ctx = {
                            "request": request,
                            "theme": theme_vars,
                            "gym_name": get_gym_name("Gimnasio"),
                            "logo_url": "", #_resolve_logo_url(),
                            "reason": str(info.get("reason") or ""),
                            "until": str(info.get("until") or ""),
                        }
                        return templates.TemplateResponse("suspension.html", ctx, status_code=403)
                if active_maint:
                    allow = path.startswith("/static/") or (path == "/favicon.ico")
                    if not allow:
                        theme_vars = {} #_resolve_theme_vars()
                        ctx = {
                            "request": request,
                            "theme": theme_vars,
                            "gym_name": get_gym_name("Gimnasio"),
                            "logo_url": "", #_resolve_logo_url(),
                            "message": str(maint_msg or ""),
                            "until": str(maint_until or ""),
                        }
                        return templates.TemplateResponse("maintenance.html", ctx, status_code=503)
            except Exception:
                pass
        try:
            response = await call_next(request)
        finally:
            if token is not None:
                try:
                    CURRENT_TENANT.reset(token)
                except Exception:
                    pass
        return response


class ForceHTTPSProtoMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: Callable):
        try:
            xfproto = (request.headers.get("x-forwarded-proto") or "").strip().lower()
            if xfproto == "https":
                try:
                    request.scope["scheme"] = "https"
                except Exception:
                    pass
        except Exception:
            pass
        response = await call_next(request)
        return response


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: Callable):
        resp = await call_next(request)
        try:
            resp.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains; preload"
            resp.headers["X-Content-Type-Options"] = "nosniff"
            resp.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
            path = str(getattr(request.url, "path", "/"))
            if path.startswith("/admin"):
                csp = (
                    "default-src 'self' https:; "
                    "img-src 'self' data: https:; "
                    "media-src 'self' https: blob: data:; "
                    "style-src 'self' https: 'unsafe-inline'; "
                    "font-src 'self' https:; "
                    "script-src 'self' https: 'unsafe-inline' 'unsafe-eval'; "
                    "connect-src 'self' https:;"
                )
            else:
                csp = (
                    "default-src 'self' https:; "
                    "img-src 'self' data: https:; "
                    "media-src 'self' https: blob: data:; "
                    "style-src 'self' https: 'unsafe-inline'; "
                    "font-src 'self' https:; "
                    "script-src 'self' https: 'unsafe-inline' 'unsafe-eval'; "
                    "connect-src 'self' https:;"
                )
            resp.headers["Content-Security-Policy"] = csp
        except Exception:
            pass
        return resp


class TenantGuardMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: Callable):
        try:
            path = request.url.path or "/"
            if path.startswith("/static/") or path == "/favicon.ico":
                return await call_next(request)
            sub = None
            try:
                sub = CURRENT_TENANT.get()
            except Exception:
                sub = None
            if not sub:
                try:
                    p = path or "/"
                except Exception:
                    p = "/"
                try:
                    host = _get_request_host(request)
                except Exception:
                    host = ""
                try:
                    base = (os.getenv("TENANT_BASE_DOMAIN") or "").strip().lower().lstrip(".")
                except Exception:
                    base = ""
                is_base_host = bool(base and (host == base or host == ("www." + base))) or (host in ("localhost", "127.0.0.1"))
                if p == "/" and is_base_host:
                    return RedirectResponse(url="/admin", status_code=303)
                if p.startswith("/admin"):
                    if is_base_host:
                        return await call_next(request)
                    return JSONResponse({"error": "tenant_not_found"}, status_code=404)
                return JSONResponse({"error": "tenant_not_found"}, status_code=404)
            try:
                if _is_tenant_suspended(sub):
                    return JSONResponse({"error": "tenant_suspended"}, status_code=423)
            except Exception:
                pass
        except Exception:
            pass
        resp = await call_next(request)
        return resp

class TenantApiPrefixMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        try:
            if scope.get("type") == "http":
                path = scope.get("path") or "/"
                if path.startswith("/api/"):
                    try:
                        headers = dict((k.decode('latin1'), v.decode('latin1')) for k, v in (scope.get('headers') or []))
                    except Exception:
                        headers = {}
                    host = (headers.get('host') or headers.get('Host') or '').strip().lower()
                    sub = _extract_tenant_from_host(host) or ''
                    parts = path.split('/')
                    if len(parts) >= 4 and parts[2] and parts[2] == sub:
                        rest = '/' + '/'.join(['api'] + parts[3:])
                        try:
                            scope = dict(scope)
                            scope['path'] = rest
                        except Exception:
                            pass
                        if sub:
                            try:
                                hdrs = list(scope.get('headers') or [])
                                hdrs.append((b'X-Tenant-ID', sub.encode('latin1')))
                                scope['headers'] = hdrs
                            except Exception:
                                pass
        except Exception:
            pass
        return await self.app(scope, receive, send)

class TenantHeaderEnforcerMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: Callable):
        try:
            p = request.url.path or "/"
            if p.startswith("/api/"):
                expected = None
                try:
                    expected = CURRENT_TENANT.get()
                except Exception:
                    expected = None
                try:
                    provided = request.headers.get('X-Tenant-ID') or request.headers.get('x-tenant-id') or ''
                except Exception:
                    provided = ''
                if not expected:
                    try:
                        host = _get_request_host(request)
                        expected = _extract_tenant_from_host(host) or None
                    except Exception:
                        expected = None
                if expected:
                    if (not provided) or (provided.strip().lower() != str(expected).strip().lower()):
                        return JSONResponse({"error": "invalid_tenant_header"}, status_code=400)
        except Exception:
            pass
        return await call_next(request)


# Redirección amigable de 401 según la sección
@app.exception_handler(HTTPException)
async def _http_exc_redirect_handler(request: Request, exc: HTTPException):
    try:
        if exc.status_code == 401:
            path = (request.url.path or "")
            if path.startswith("/usuario/"):
                return RedirectResponse(url="/usuario/login", status_code=303)
            if path.startswith("/gestion"):
                return RedirectResponse(url="/gestion/login", status_code=303)
            if path.startswith("/dashboard"):
                return RedirectResponse(url="/login", status_code=303)
    except Exception:
        pass
    return JSONResponse({"detail": exc.detail}, status_code=exc.status_code)

# Verificación de LibreOffice en el arranque
@app.on_event("startup")
def _check_libreoffice_startup():
    try:
        path = shutil.which("soffice") or shutil.which("soffice.exe")
        if path:
            try:
                res = subprocess.run([path, "--version"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
                ver = (res.stdout or "").strip()
            except Exception:
                ver = "(versión no disponible)"
            logging.info(f"LibreOffice disponible: {path} | {ver}")
        else:
            logging.warning("LibreOffice NO encontrado en el sistema")
    except Exception as e:
        logging.exception(f"Error comprobando LibreOffice en startup: {e}")

# Webhooks de WhatsApp: verificación (GET) y recepción (POST)
@app.get("/webhooks/whatsapp")
async def whatsapp_verify(request: Request):
    try:
        mode = request.query_params.get("hub.mode")
        token = request.query_params.get("hub.verify_token")
        challenge = request.query_params.get("hub.challenge")
        expected = os.getenv("WHATSAPP_VERIFY_TOKEN", "")
        if mode == "subscribe" and expected and token == expected and challenge:
            return Response(content=str(challenge), media_type="text/plain")
    except Exception as e:
        logging.getLogger(__name__).error(f"WhatsApp verify error: {e}")
    raise HTTPException(status_code=403, detail="Invalid verify token")

@app.post("/webhooks/whatsapp")
async def whatsapp_webhook(request: Request):
    # Verificación de firma (si se configura WHATSAPP_APP_SECRET)
    logger = logging.getLogger(__name__)
    try:
        raw = await request.body()
        app_secret = os.getenv("WHATSAPP_APP_SECRET", "")
        if app_secret:
            try:
                import hmac, hashlib
                sig_header = request.headers.get("X-Hub-Signature-256") or ""
                expected = "sha256=" + hmac.new(app_secret.encode("utf-8"), raw, hashlib.sha256).hexdigest()
                if not hmac.compare_digest(expected, sig_header):
                    raise HTTPException(status_code=403, detail="Invalid signature")
            except HTTPException:
                raise
            except Exception as e:
                logger.error(f"WhatsApp signature check error: {e}")
                raise HTTPException(status_code=400, detail="Signature verification error")
        import json as _json
        try:
            payload = _json.loads(raw.decode("utf-8"))
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid JSON")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"WhatsApp webhook read error: {e}")
        raise HTTPException(status_code=400, detail="Bad Request")

    # DB
    try:
        db = _get_db() # DEPRECATED but needed for legacy webhook logic in server_legacy.py
        # Ideally we should use Services here too but it's a big refactor.
    except Exception:
        db = None

    # Procesamiento de estados y mensajes
    try:
        for entry in (payload.get("entry") or []):
            for change in (entry.get("changes") or []):
                value = change.get("value") or {}
                # Actualizaciones de estado
                for status in (value.get("statuses") or []):
                    mid = status.get("id")
                    st = status.get("status")
                    if db and mid and st:
                        try:
                            db.actualizar_estado_mensaje_whatsapp(mid, st)
                        except Exception as e:
                            logger.error(f"Estado WA update failed id={mid} status={st}: {e}")
                # Mensajes entrantes
                for msg in (value.get("messages") or []):
                    mid = msg.get("id")
                    mtype = msg.get("type")
                    wa_from = msg.get("from")
                    ctx_id = None
                    try:
                        ctx_id = ((msg.get("context") or {}).get("id"))
                    except Exception:
                        ctx_id = None

                    text = None
                    button_id = None
                    button_title = None
                    list_id = None
                    list_title = None
                    try:
                        if mtype == "text":
                            text = (msg.get("text") or {}).get("body")
                        elif mtype == "button":
                            text = (msg.get("button") or {}).get("text")
                        elif mtype == "interactive":
                            ir = msg.get("interactive") or {}
                            br = ir.get("button_reply") or {}
                            lr = ir.get("list_reply") or {}
                            button_id = br.get("id")
                            button_title = br.get("title")
                            list_id = lr.get("id")
                            list_title = lr.get("title")
                            text = button_title or list_title
                        elif mtype == "image":
                            text = "[imagen]"
                        elif mtype == "audio":
                            text = "[audio]"
                        elif mtype == "video":
                            text = "[video]"
                        elif mtype == "document":
                            text = "[documento]"
                    except Exception:
                        pass

                    # Registrar en DB (mensaje recibido)
                    if db:
                        try:
                            uid = None
                            try:
                                uid = db._obtener_user_id_por_telefono_whatsapp(wa_from)
                            except Exception:
                                uid = None
                            db.registrar_mensaje_whatsapp(
                                user_id=uid,
                                message_type="welcome",
                                template_name="incoming",
                                phone_number=wa_from,
                                message_content=(text or ""),
                                status="received",
                                message_id=mid,
                            )
                        except Exception as e:
                            logger.error(f"WA incoming log failed id={mid}: {e}")

                    # Auto-acciones: promoción/declinación desde lista de espera
                    try:
                        import unicodedata as _unic
                        def _sanitize_text(s: str) -> str:
                            s = (s or "").strip()
                            s = "".join(c for c in _unic.normalize("NFD", s) if _unic.category(c) != "Mn")
                            s = s.lower()
                            # Quitar signos de puntuación comunes
                            for ch in [".", ",", ";", "!", "?", "¡", "¿"]:
                                s = s.replace(ch, "")
                            return s

                        # Envío de confirmación delegado al desktop; no se envía desde servidor

                        # Señales de SI/NO desde texto o interacción
                        tid = button_id or list_id or ""
                        ttitle = button_title or list_title or text or ""
                        stext = _sanitize_text(ttitle)
                        yes_signal = stext == "si"
                        no_signal = stext == "no"

                        # IDs de interacción con clase_horario_id: WAITLIST_PROMOTE:<id> / WAITLIST_DECLINE:<id>
                        target_clase_id = None
                        try:
                            if isinstance(tid, str):
                                if tid.startswith("WAITLIST_PROMOTE:"):
                                    target_clase_id = int(tid.split(":", 1)[1])
                                    yes_signal = True
                                elif tid.startswith("WAITLIST_DECLINE:"):
                                    target_clase_id = int(tid.split(":", 1)[1])
                                    no_signal = True
                        except Exception:
                            target_clase_id = None

                        if db and (yes_signal or no_signal):
                            # Resolver usuario desde teléfono con normalización mejorada
                            uid = None
                            try:
                                uid = db._obtener_user_id_por_telefono_whatsapp(wa_from)
                            except Exception:
                                uid = None
                            if uid:
                                # Fallback para clase objetivo: primera lista de espera activa del usuario
                                if target_clase_id is None:
                                    try:
                                        with db.get_connection_context() as conn:  # type: ignore
                                            cur = conn.cursor()
                                            cur.execute(
                                                """
                                                SELECT clase_horario_id
                                                FROM clase_lista_espera
                                                WHERE usuario_id = %s AND activo = true
                                                ORDER BY posicion ASC
                                                LIMIT 1
                                                """,
                                                (int(uid),)
                                            )
                                            row = cur.fetchone()
                                            if row:
                                                try:
                                                    target_clase_id = int(row[0])
                                                except Exception:
                                                    target_clase_id = None
                                    except Exception:
                                        target_clase_id = None

                                # Obtener info de clase para mensajes
                                clase_info = None
                                try:
                                    if target_clase_id:
                                        clase_info = db.obtener_horario_por_id(int(target_clase_id))  # type: ignore
                                except Exception:
                                    clase_info = None
                                tipo_txt = (clase_info or {}).get("tipo_clase_nombre") or (clase_info or {}).get("clase_nombre") or "la clase"
                                dia_txt = (clase_info or {}).get("dia_semana") or ""
                                hora_raw = (clase_info or {}).get("hora_inicio")
                                try:
                                    hora_txt = str(hora_raw) if hora_raw is not None else ""
                                except Exception:
                                    hora_txt = ""
                                gym_name = None
                                try:
                                    gym_name = get_gym_name("Gimnasio")
                                except Exception:
                                    gym_name = "Gimnasio"

                                if target_clase_id:
                                    if yes_signal:
                                        # Registrar auditoría de confirmación; desktop realiza inscripción y mensajería
                                        try:
                                            ip_addr = (getattr(request, 'client', None).host if getattr(request, 'client', None) else None)
                                            ua = request.headers.get('user-agent', '')
                                            sid = getattr(getattr(request, 'session', {}), 'get', lambda *a, **k: None)('session_id')
                                            db.registrar_audit_log(  # type: ignore
                                                user_id=int(uid),
                                                action="auto_promote_waitlist",
                                                table_name="clase_lista_espera",
                                                record_id=int(target_clase_id),
                                                old_values=None,
                                                new_values=json.dumps({"confirmado": True}),
                                                ip_address=ip_addr,
                                                user_agent=ua,
                                                session_id=sid,
                                            )
                                        except Exception as e:
                                            logger.error(f"WA auto-promote audit log failed uid={uid} clase_id={target_clase_id}: {e}")
                                        logger.info(f"WA auto-promote: audit registrada usuario_id={uid} clase_horario_id={target_clase_id} from={wa_from} mid={mid}")
                                    elif no_signal:
                                        # Declinación explícita: mantener en lista; envío delegado al desktop
                                        try:
                                            ip_addr = (getattr(request, 'client', None).host if getattr(request, 'client', None) else None)
                                            ua = request.headers.get('user-agent', '')
                                            sid = getattr(getattr(request, 'session', {}), 'get', lambda *a, **k: None)('session_id')
                                            db.registrar_audit_log(  # type: ignore
                                                user_id=int(uid),
                                                action="decline_waitlist_promotion",
                                                table_name="clase_lista_espera",
                                                record_id=int(target_clase_id),
                                                old_values=None,
                                                new_values=json.dumps({"declinado": True}),
                                                ip_address=ip_addr,
                                                user_agent=ua,
                                                session_id=sid,
                                            )
                                        except Exception:
                                            pass
                    except Exception:
                        pass
        return JSONResponse({"status": "ok"})
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"WhatsApp webhook processing error: {e}")
        return JSONResponse({"status": "error"}, status_code=500)

# Middleware de Request ID para trazabilidad en logs
class RequestIDMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        rid = request.headers.get("X-Request-ID") or uuid.uuid4().hex[:12]
        try:
            request.state.request_id = rid
        except Exception:
            pass
        response = await call_next(request)
        try:
            response.headers["X-Request-ID"] = rid
        except Exception:
            pass
        return response
