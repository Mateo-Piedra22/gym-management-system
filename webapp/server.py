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

from fastapi import FastAPI, Request, HTTPException, Depends, UploadFile, File, Form
import logging
from fastapi.responses import JSONResponse, RedirectResponse, Response, FileResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware
from starlette.middleware.httpsredirect import HTTPSRedirectMiddleware
from starlette.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware
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
# Asegurar que el directorio raíz del proyecto esté en sys.path para imports (models, payment_manager, database)
try:
    _proj_root = Path(__file__).resolve().parent.parent
    if str(_proj_root) not in sys.path:
        sys.path.insert(0, str(_proj_root))
except Exception:
    pass
# HTTP client para probes (con fallback si no está disponible)
try:
    import requests  # type: ignore
except Exception:
    requests = None  # type: ignore

# Cliente de Google Cloud Storage (opcional)
try:
    from google.cloud import storage as gcs_storage  # type: ignore
except Exception:
    gcs_storage = None  # type: ignore

# Importar gestor de base de datos SIN modificar el programa principal
try:
    from database import DatabaseManager  # type: ignore
except Exception as e:
    try:
        logging.exception(f"Import de DatabaseManager falló: {e}")
    except Exception:
        pass
    DatabaseManager = None  # type: ignore

# Importar dataclasses de modelos para payloads del API
try:
    from models import Usuario, Pago, MetodoPago, ConceptoPago, Ejercicio, Rutina, RutinaEjercicio  # type: ignore
except Exception:
    Usuario = None  # type: ignore
    Pago = None  # type: ignore
    MetodoPago = None  # type: ignore
    ConceptoPago = None  # type: ignore
    Ejercicio = None  # type: ignore
    Rutina = None  # type: ignore
    RutinaEjercicio = None  # type: ignore

# Importar utilidades del proyecto principal para branding y contraseña de desarrollador
try:
    from utils import get_gym_name  # type: ignore
except Exception:
    def get_gym_name(default: str = "Gimnasio") -> str:  # type: ignore
        try:
            path = Path("gym_data.txt")
            if path.exists():
                for line in path.read_text(encoding="utf-8").splitlines():
                    if line.startswith("gym_name="):
                        return line.split("=", 1)[1].strip()
        except Exception:
            pass
        return default

# Dirección del gimnasio (fallback si no se puede importar desde utils)
try:
    from utils import get_gym_value as _get_gym_value  # type: ignore
    def get_gym_address(default: str = "Dirección del gimnasio") -> str:  # type: ignore
        try:
            v = _get_gym_value("gym_address")
            return (v or default).strip()
        except Exception:
            return default
except Exception:
    def get_gym_address(default: str = "Dirección del gimnasio") -> str:  # type: ignore
        try:
            path = Path("gym_data.txt")
            if path.exists():
                for line in path.read_text(encoding="utf-8").splitlines():
                    if line.startswith("gym_address="):
                        return line.split("=", 1)[1].strip()
        except Exception:
            pass
        return default

try:
    from managers import DEV_PASSWORD  # type: ignore
except Exception:
    DEV_PASSWORD = None  # type: ignore

try:
    from payment_manager import PaymentManager  # type: ignore
except Exception:
    PaymentManager = None  # type: ignore

# Base URL pública (Railway/Vercel) sin túneles
try:
    from utils import get_webapp_base_url  # type: ignore
except Exception:
    def get_webapp_base_url(default: str = "") -> str:  # type: ignore
        import os as _os
        # Preferir WEBAPP_BASE_URL explícita
        env_url = _os.getenv("WEBAPP_BASE_URL", "").strip()
        if env_url:
            return env_url
        # Detectar dominio de Vercel si existe
        vercel = (_os.getenv("VERCEL_URL") or _os.getenv("VERCEL_BRANCH_URL") or _os.getenv("VERCEL_PROJECT_PRODUCTION_URL") or "").strip()
        if vercel:
            if vercel.startswith("http://") or vercel.startswith("https://"):
                return vercel
            return f"https://{vercel}"
        # Fallback antiguo (Railway) solo si se proporciona por default
        return (default or "").strip()

# Secreto para enlaces de previsualización (firmas HMAC) reutilizable en entornos serverless
def _get_preview_secret() -> str:
    try:
        env = os.getenv("WEBAPP_PREVIEW_SECRET", "").strip()
        if env:
            return env
    except Exception:
        pass
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

def _sign_excel_view(rutina_id: int, weeks: int, filename: str, ts: int) -> str:
    try:
        base = f"{int(rutina_id)}|{int(weeks)}|{filename}|{int(ts)}".encode("utf-8")
    except Exception:
        base = f"{rutina_id}|{weeks}|{filename}|{ts}".encode("utf-8")
    secret = _get_preview_secret().encode("utf-8")
    return hmac.new(secret, base, hashlib.sha256).hexdigest()

# Firma para previsualización efímera (borrador) basada en un payload ID
def _sign_excel_view_draft(payload_id: str, weeks: int, filename: str, ts: int) -> str:
    try:
        pid = str(payload_id)
        base = f"{pid}|{int(weeks)}|{filename}|{int(ts)}".encode("utf-8")
    except Exception:
        base = f"{payload_id}|{weeks}|{filename}|{ts}".encode("utf-8")
    secret = _get_preview_secret().encode("utf-8")
    return hmac.new(secret, base, hashlib.sha256).hexdigest()

# Firma stateless basada en payload codificado
def _sign_excel_view_draft_data(data: str, weeks: int, filename: str, ts: int) -> str:
    try:
        d = str(data)
        base = f"{d}|{int(weeks)}|{filename}|{int(ts)}".encode("utf-8")
    except Exception:
        base = f"{data}|{weeks}|{filename}|{ts}".encode("utf-8")
    secret = _get_preview_secret().encode("utf-8")
    return hmac.new(secret, base, hashlib.sha256).hexdigest()

# PINs en texto plano: sin hashing ni migración; usar la verificación del DatabaseManager directamente.

# --- Rate limiting simple para login de usuarios (por IP y DNI) ---
_login_attempts_lock = threading.Lock()
_login_attempts_by_ip: Dict[str, list] = {}
_login_attempts_by_dni: Dict[str, list] = {}

def _get_client_ip(request: Request) -> str:
    try:
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
                    u_obj = None
                if u_obj is not None:
                    try:
                        u_nombre = (getattr(u_obj, "nombre", "") or "").strip() or u_nombre
                        u_dni = getattr(u_obj, "dni", None) if (getattr(u_obj, "dni", None) or None) else u_dni
                        u_tel = getattr(u_obj, "telefono", "") if (getattr(u_obj, "telefono", "") or "") else u_tel
                    except Exception:
                        pass
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
                            pass
        except Exception:
            pass
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
                        continue
            except Exception:
                continue
    # Adjuntar y agrupar
    try:
        rutina.ejercicios = ejercicios
    except Exception:
        pass
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
    from routine_manager import RoutineTemplateManager, create_routine_manager  # type: ignore
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
    """Devuelve el primer directorio existente entre varias ubicaciones candidatas.
    Prioriza BASE_DIR, luego el directorio del ejecutable (onedir) y por último el proyecto.
    """
    candidates = []
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
    # Fallback: primera opción aunque no exista
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
        from utils import resource_path  # type: ignore
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
app.add_middleware(SessionMiddleware, secret_key=_get_session_secret())

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
        db = _get_db()
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

try:
    app.add_middleware(RequestIDMiddleware)
except Exception:
    pass

# Middleware de latencia y estado del circuito para observabilidad
class TimingAndCircuitMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        import time
        start = time.perf_counter()
        rid = getattr(getattr(request, 'state', object()), 'request_id', '-')
        path = request.url.path
        method = request.method
        circuit_state = None
        try:
            db = _get_db()
            if db and hasattr(db, 'get_circuit_state'):
                circuit_state = db.get_circuit_state()  # type: ignore
        except Exception:
            circuit_state = None
        response = await call_next(request)
        dur_ms = int((time.perf_counter() - start) * 1000)
        try:
            logging.info(f"req {method} {path} rid={rid} duration_ms={dur_ms} circuit={circuit_state}")
        except Exception:
            pass
        return response

try:
    app.add_middleware(TimingAndCircuitMiddleware)
except Exception:
    pass

# Middlewares de producción (opcionales via ENV, cambios mínimos)
try:
    # Restringir hosts confiables. Si no hay ENV, añadir dominio Railway por defecto
    th = os.getenv("TRUSTED_HOSTS", "").strip()
    hosts = [h.strip() for h in th.split(",") if h.strip()] if th else []
    if not hosts:
        # Permitir dominios de desarrollo/producción comunes
        hosts = [
            "localhost",
            "127.0.0.1",
            "*.loca.lt",
            "*.vercel.app",
            "*.vercel.dev",
        ]
        # Añadir dominio dinámico de Vercel si está disponible
        _vercel = (os.getenv("VERCEL_URL") or os.getenv("VERCEL_BRANCH_URL") or os.getenv("VERCEL_PROJECT_PRODUCTION_URL") or "").strip()
        if _vercel:
            hosts.append(_vercel)
        # Mantener compatibilidad con despliegues anteriores en Railway
        hosts.append("gym-ms-zrk.up.railway.app")
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=hosts)
    # Forzar HTTPS en producción si se indica
    if (os.getenv("FORCE_HTTPS", "0").strip() in ("1", "true", "yes")):
        app.add_middleware(HTTPSRedirectMiddleware)
    # Nota: Gestión de cabeceras de proxy delegada a Uvicorn (proxy_headers=True)
    if (os.getenv("PROXY_HEADERS_ENABLED", "1").strip() in ("1", "true", "yes")):
        logging.info("Cabeceras de proxy gestionadas por Uvicorn (proxy_headers=True)")
except Exception:
    pass

# Static y assets basados en BASE_DIR
BASE_DIR = _compute_base_dir()

# Detectar entornos serverless (Vercel/Railway) donde el FS es de solo lectura
def _is_serverless_env() -> bool:
    try:
        return bool(os.getenv("VERCEL") or os.getenv("VERCEL_ENV") or os.getenv("RAILWAY"))
    except Exception:
        return False

# mkdir seguro que no bloquea el arranque si falla
def _safe_mkdir(path: Path) -> bool:
    try:
        path.mkdir(parents=True, exist_ok=True)
        return True
    except Exception as e:
        try:
            logging.warning(f"No se pudo crear directorio {path}: {e}")
        except Exception:
            pass
        return False

static_dir = _resolve_existing_dir("webapp", "static")
try:
    _safe_mkdir(static_dir)
except Exception:
    pass

uploads_dir = _resolve_existing_dir("webapp", "uploads")
# En serverless, usar /tmp si la ruta del proyecto no existe o no es escribible
if _is_serverless_env() and not uploads_dir.exists():
    try:
        tmp_base = Path(os.getenv("TMPDIR") or os.getenv("TEMP") or "/tmp")
        uploads_dir = tmp_base / "uploads"
    except Exception:
        pass
_safe_mkdir(uploads_dir)

templates_dir = _resolve_existing_dir("webapp", "templates")
templates = Jinja2Templates(directory=str(templates_dir))

try:
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
except Exception:
    pass
# Preferir assets del proyecto raíz; fallback a assets dentro de webapp
try:
    app.mount("/assets", StaticFiles(directory=str(_resolve_existing_dir("assets"))), name="assets")
except Exception:
    try:
        app.mount("/assets", StaticFiles(directory=str(_resolve_existing_dir("webapp", "assets"))), name="assets")
    except Exception:
        pass
# Exponer siempre los assets de webapp bajo una ruta dedicada
try:
    app.mount("/webapp-assets", StaticFiles(directory=str(_resolve_existing_dir("webapp", "assets"))), name="webapp-assets")
except Exception:
    pass
try:
    app.mount("/uploads", StaticFiles(directory=str(uploads_dir)), name="uploads")
except Exception:
    pass

# Utilidad de sincronización anterior eliminada
# Nota: Las operaciones se gestionan directamente por la base de datos.

# Endpoint de salud ligero para probes y monitores
@app.get("/healthz")
async def healthz():
    """Devuelve estado 200 si la app responde. Incluye mínimos detalles."""
    try:
        details: Dict[str, Any] = {
            "status": "ok",
            "time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }
        # Comprobación opcional de DB (no bloqueante)
        try:
            db = _get_db()
            if db is not None:
                details["db"] = "ok"
        except Exception:
            details["db"] = "error"
        return JSONResponse(details)
    except Exception:
        # Fallback defensivo: responder 200 para evitar cascadas de reinicios
        return JSONResponse({"status": "ok"})

# Endpoint para exponer la URL base pública (Railway/Vercel)
@app.get("/webapp/base_url")
async def webapp_base_url():
    try:
        url = get_webapp_base_url()
        return JSONResponse({"base_url": url})
    except Exception:
        # Fallback amigable: intentar Vercel y luego localhost
        try:
            v = (os.getenv("VERCEL_URL") or os.getenv("VERCEL_BRANCH_URL") or os.getenv("VERCEL_PROJECT_PRODUCTION_URL") or "").strip()
            if v:
                if v.startswith("http://") or v.startswith("https://"):
                    return JSONResponse({"base_url": v})
                return JSONResponse({"base_url": f"https://{v}"})
        except Exception:
            pass
        return JSONResponse({"base_url": "http://127.0.0.1:8000/"})

# Endpoints de sincronización removidos - sistema usa base de datos Neon única sin replicación

# Helpers para aplicar cambios de forma segura e idempotente

# Helpers de sincronización anteriores eliminados.
 


# Evitar 404 de clientes de Vite durante desarrollo: devolver stub vacío
@app.get("/@vite/client")
async def vite_client_stub():
    return Response("// Vite client stub (deshabilitado en esta app)", media_type="application/javascript")

# Endpoint de estado de replicación externa retirado; usamos replicación lógica nativa de PostgreSQL.

# Generar CSS desde QSS de forma automática evitando sobrescritura
try:
    css_path = static_dir / "style.css"
    # Solo regenerar si no existe o si se fuerza explícitamente por ENV
    if (not css_path.exists()) or (os.getenv("REGENERATE_STYLE_CSS", "0").strip() == "1"):
        qss_path = _resolve_existing_dir("styles", "style.qss")
        generate_css_from_qss(qss_path, css_path)
except Exception:
    # Fallback silencioso; la UI seguirá mostrando colores por defecto
    pass


# Configuración y utilidades para subida de medios a Google Cloud Storage
def _get_gcs_settings() -> Dict[str, Any]:
    """Lee configuración de GCS desde variables de entorno.
    Devuelve un dict con claves: bucket, project_id, prefix, public_base_url, make_public.
    """
    try:
        bucket = (os.getenv("GCS_BUCKET_NAME") or os.getenv("GOOGLE_CLOUD_STORAGE_BUCKET") or "").strip()
        project_id = (os.getenv("GCS_PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT") or "").strip()
        prefix = (os.getenv("GCS_MEDIA_PREFIX") or "ejercicios").strip().strip("/")
        public_base = (os.getenv("GCS_PUBLIC_BASE_URL") or "https://storage.googleapis.com").strip().rstrip("/")
        make_public = (os.getenv("GCS_PUBLIC_READ", "1").strip() in ("1", "true", "yes"))
        return {
            "bucket": bucket,
            "project_id": project_id,
            "prefix": prefix,
            "public_base_url": public_base,
            "make_public": make_public,
        }
    except Exception:
        return {"bucket": "", "project_id": "", "prefix": "ejercicios", "public_base_url": "https://storage.googleapis.com", "make_public": True}


def _upload_media_to_gcs(dest_name: str, data: bytes, content_type: str) -> Optional[str]:
    """Sube el contenido a GCS si está configurado y disponible.
    Devuelve la URL pública resultante o None si GCS no está configurado.
    """
    settings = _get_gcs_settings()
    bucket_name = settings.get("bucket") or ""
    if not bucket_name:
        return None
    if gcs_storage is None:
        raise HTTPException(status_code=500, detail="Dependencia 'google-cloud-storage' no instalada. Agregue 'google-cloud-storage' al requirements.")
    try:
        client = gcs_storage.Client(project=(settings.get("project_id") or None))
        bucket = client.bucket(bucket_name)
        prefix = settings.get("prefix") or "ejercicios"
        blob_path = f"{prefix}/{dest_name}" if prefix else dest_name
        blob = bucket.blob(blob_path)
        blob.upload_from_string(data, content_type=content_type)
        if settings.get("make_public", True):
            try:
                blob.make_public()
            except Exception:
                # Si el bucket usa UBLA, confiar en política pública del bucket
                pass
        public_base = settings.get("public_base_url") or "https://storage.googleapis.com"
        # Estilo path por defecto
        return f"{public_base}/{bucket_name}/{blob_path}"
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error subiendo a GCS: {e}")
        raise HTTPException(status_code=500, detail=f"Error subiendo a GCS: {e}")


# Configuración y utilidades para subida de medios a Backblaze B2 (fallback)
def _get_b2_settings() -> Dict[str, Any]:
    """Lee configuración de Backblaze B2 desde variables de entorno.
    Devuelve un dict con claves: account_id, application_key, bucket_id, bucket_name, prefix, public_base_url.
    """
    try:
        # B2 usa applicationKeyId (keyId) para b2_authorize_account; mantener también accountId para otras llamadas
        key_id = (os.getenv("B2_KEY_ID") or "").strip()
        account_id = (os.getenv("B2_ACCOUNT_ID") or "").strip()
        application_key = (os.getenv("B2_APPLICATION_KEY") or "").strip()
        bucket_id = (os.getenv("B2_BUCKET_ID") or "").strip()
        bucket_name = (os.getenv("B2_BUCKET_NAME") or "").strip()
        prefix = (os.getenv("B2_MEDIA_PREFIX") or "ejercicios").strip().strip("/")
        public_base = (os.getenv("B2_PUBLIC_BASE_URL") or "").strip().rstrip("/")
        # Sanitizar prefijo si accidentalmente incluye el nombre del bucket
        try:
            if bucket_name and prefix and prefix.lower().startswith((bucket_name + "/").lower()):
                prefix = prefix[len(bucket_name) + 1 :]
        except Exception:
            pass
        return {
            "key_id": key_id,
            "account_id": account_id,
            "application_key": application_key,
            "bucket_id": bucket_id,
            "bucket_name": bucket_name,
            "prefix": prefix,
            "public_base_url": public_base,
        }
    except Exception:
        return {
            "key_id": "",
            "account_id": "",
            "application_key": "",
            "bucket_id": "",
            "bucket_name": "",
            "prefix": "ejercicios",
            "public_base_url": "",
        }


def _upload_media_to_b2(dest_name: str, data: bytes, content_type: str) -> Optional[str]:
    """Sube el contenido a Backblaze B2 usando su API nativa.
    Devuelve la URL pública resultante o None si B2 no está configurado.
    """
    settings = _get_b2_settings()
    if not (settings.get("application_key") and settings.get("bucket_id")):
        return None
    if requests is None:
        raise HTTPException(status_code=500, detail="Dependencia 'requests' no instalada para usar B2.")
    try:
        # 1) Autorizar cuenta (GET + Basic Auth según especificación de B2)
        import base64 as _b64
        # Autenticación con applicationKeyId (key_id) si existe; fallback a account_id
        user_id = (settings.get("key_id") or settings.get("account_id") or "").strip()
        basic = _b64.b64encode(f"{user_id}:{settings['application_key']}".encode("ascii")).decode("ascii")
        auth_resp = requests.get(
            "https://api.backblazeb2.com/b2api/v2/b2_authorize_account",
            headers={"Authorization": f"Basic {basic}"},
            timeout=8,
        )
        if auth_resp.status_code != 200:
            try:
                txt = auth_resp.text
            except Exception:
                txt = ""
            raise HTTPException(status_code=502, detail=f"b2_authorize_account fallo: {auth_resp.status_code} {txt}")
        auth_json = auth_resp.json()
        api_url = auth_json.get("apiUrl", "")
        download_url = auth_json.get("downloadUrl", "")
        auth_token = auth_json.get("authorizationToken", "")
        if not api_url or not auth_token:
            raise HTTPException(status_code=502, detail="Respuesta de autorización B2 inválida")

        # 1.1) Verificar que bucket_id corresponde al nombre real (evita URLs públicas erróneas)
        bucket_name_eff = (settings.get("bucket_name") or "").strip()
        try:
            acct_id_for_list = (settings.get("account_id") or auth_json.get("accountId") or "").strip()
            if acct_id_for_list:
                list_resp = requests.post(
                    f"{api_url}/b2api/v2/b2_list_buckets",
                    headers={"Authorization": auth_token},
                    json={"accountId": acct_id_for_list},
                    timeout=8,
                )
                if list_resp.status_code == 200:
                    buckets = list_resp.json().get("buckets", [])
                    for b in (buckets or []):
                        if str(b.get("bucketId", "")) == str(settings.get("bucket_id", "")):
                            bn = (b.get("bucketName") or "").strip()
                            if bn:
                                bucket_name_eff = bn
                            break
        except Exception:
            pass

        # 2) Obtener URL de subida
        up_resp = requests.post(
            f"{api_url}/b2api/v2/b2_get_upload_url",
            headers={"Authorization": auth_token},
            json={"bucketId": settings["bucket_id"]},
            timeout=8,
        )
        if up_resp.status_code != 200:
            try:
                txt = up_resp.text
            except Exception:
                txt = ""
            raise HTTPException(status_code=502, detail=f"b2_get_upload_url fallo: {up_resp.status_code} {txt}")
        up_json = up_resp.json()
        upload_url = up_json.get("uploadUrl", "")
        upload_token = up_json.get("authorizationToken", "")
        if not upload_url or not upload_token:
            try:
                dbg = up_resp.text
            except Exception:
                dbg = ""
            raise HTTPException(status_code=502, detail=f"Respuesta de upload URL B2 inválida {dbg}")

        # 3) Subir archivo
        file_name = f"{settings['prefix']}/{dest_name}" if settings.get("prefix") else dest_name
        # Sanitizar: evitar que el prefijo incluya accidentalmente el nombre del bucket
        try:
            bn_lower = (bucket_name_eff or "").strip().lower()
            if bn_lower and file_name.lower().startswith(bn_lower + "/"):
                file_name = file_name[len(bucket_name_eff) + 1 :]
        except Exception:
            pass
        try:
            import urllib.parse as _urlparse  # import local para evitar polución global
            file_name_header = _urlparse.quote(file_name)
        except Exception:
            file_name_header = file_name.replace(" ", "%20")
        headers = {
            "Authorization": upload_token,
            "X-Bz-File-Name": file_name_header,
            "Content-Type": (content_type or "application/octet-stream"),
            # Evita calcular sha1 en servidor (válido para B2)
            "X-Bz-Content-Sha1": "do_not_verify",
        }
        put_resp = requests.post(upload_url, headers=headers, data=data, timeout=30)
        if put_resp.status_code != 200:
            # Si falla, devolver None para permitir siguientes fallbacks
            try:
                txt = put_resp.text
            except Exception:
                txt = ""
            raise HTTPException(status_code=502, detail=f"b2_upload_file fallo: {put_resp.status_code} {txt}")

        # Construir URL pública
        public_base = (settings.get("public_base_url") or "").strip().rstrip("/")
        if not public_base:
            public_base = f"{(download_url or '').rstrip('/')}/file" if download_url else "https://f000.backblazeb2.com/file"
        # Normalizar para evitar duplicar el nombre del bucket o el segmento /file
        base = public_base.rstrip('/')
        # Si el host ya incluye el bucket como subdominio (estilo S3 virtual-hosted), no añadir /file
        if f"://{bucket_name_eff}." in base:
            final_base = base
            return f"{final_base}/{file_name}"
        # Si base ya termina con /file/<bucket>, no añadir el bucket de nuevo
        if base.endswith(f"/file/{bucket_name_eff}"):
            final_base = base
        elif base.endswith("/file"):
            final_base = f"{base}/{bucket_name_eff}"
        elif "/file/" in base:
            # Si el base contiene /file/<algo>, simplificar a /file y añadir bucket correcto
            idx = base.find("/file/")
            final_base = f"{base[:idx]}/file/{bucket_name_eff}"
        else:
            final_base = f"{base}/file/{bucket_name_eff}"
        return f"{final_base}/{file_name}"
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error subiendo a Backblaze B2: {e}")
        raise HTTPException(status_code=500, detail=f"Error subiendo a Backblaze B2: {e}")

def _get_password() -> str:
    # Leer directamente desde la base de datos para sincronización inmediata
    try:
        db = _get_db()
        if db and hasattr(db, 'obtener_configuracion'):
            pwd = db.obtener_configuracion('owner_password', timeout_ms=700)  # type: ignore
            if isinstance(pwd, str) and pwd.strip():
                return pwd.strip()
    except Exception:
        pass
    # Fallback: variables de entorno si están definidas (solo si DB no devuelve valor)
    try:
        env_pwd = (os.getenv("WEBAPP_OWNER_PASSWORD", "") or os.getenv("OWNER_PASSWORD", "")).strip()
    except Exception:
        env_pwd = ""
    if env_pwd:
        return env_pwd
    # Fallback: contraseña de desarrollador si existe (solo si DB no devuelve valor)
    try:
        if DEV_PASSWORD:
            return str(DEV_PASSWORD).strip()
    except Exception:
        pass
    # Último recurso
    return "admin"

def _verify_owner_password(password: str) -> bool:
    """Verifica la contraseña del dueño usando bcrypt con fallback a texto plano."""
    try:
        # Importar aquí para evitar dependencias circulares
        from security_utils import SecurityUtils
        
        # Obtener la contraseña almacenada (puede ser hash o texto plano)
        stored_password = _get_password()
        
        # Intentar verificar con bcrypt primero
        if SecurityUtils.verify_password(password, stored_password):
            return True
            
        # Fallback: comparación directa para contraseñas antiguas en texto plano
        if password == stored_password:
            return True
            
        return False
    except Exception:
        # Fallback final: comparación directa
        try:
            return password == _get_password()
        except Exception:
            return False


def _resolve_logo_url() -> str:
    # Resuelve el logo desde assets del proyecto principal
    candidates = [
        _resolve_existing_dir("assets") / "gym_logo.png",
        _resolve_existing_dir("assets") / "logo.svg",
        _resolve_existing_dir("webapp", "assets") / "logo.svg",
    ]
    for p in candidates:
        try:
            if p.exists():
                return "/assets/" + p.name
        except Exception:
            continue
    return "/assets/logo.svg"


@app.get("/api/gym/data")
async def api_gym_data(request: Request):
    try:
        # Validar acceso a Gestión sin depender del orden de definición
        require_gestion_access(request)
        return JSONResponse({
            "gym_name": get_gym_name(),
            "gym_address": get_gym_address(),
            "logo_url": _resolve_logo_url(),
        })
    except Exception as e:
        logging.error(f"api_gym_data error: {e}")
        return JSONResponse({
            "gym_name": get_gym_name(),
            "gym_address": get_gym_address(),
            "logo_url": _resolve_logo_url(),
        })


def _get_db() -> Optional[DatabaseManager]:
    global _db, _db_initializing
    # Fast path
    if _db is not None:
        return _db
    if DatabaseManager is None:
        try:
            logging.error("_get_db: DatabaseManager no disponible (import falló)")
        except Exception:
            pass
        return None
    # Inicialización con bloqueo para evitar carreras entre hilos
    with _db_lock:
        if _db is not None:
            return _db
        _db_initializing = True
        try:
            logging.debug("_get_db: inicializando DatabaseManager (lazy, locked)")
            _db = DatabaseManager()
            # Opcional: crear índices de rendimiento de forma diferida y no bloqueante
            try:
                if hasattr(_db, 'ensure_indexes'):
                    import threading
                    def _defer_ensure_indexes():
                        try:
                            import time, random, logging as _logging
                            # Jitter para evitar competir con otras tareas de arranque
                            time.sleep(random.uniform(1.5, 4.0))
                            try:
                                _db.ensure_indexes()  # type: ignore
                            except Exception as ie:
                                try:
                                    _logging.exception(f"ensure_indexes diferido falló: {ie}")
                                except Exception:
                                    pass
                        except Exception:
                            # No bloquear la inicialización por errores aquí
                            pass
                    threading.Thread(target=_defer_ensure_indexes, daemon=True).start()
            except Exception:
                # No bloquear la inicialización por errores al programar el hilo
                pass
            try:
                # Verificación ligera para asegurar que la conexión está saludable (con timeouts de lectura)
                with _db.get_connection_context() as conn:  # type: ignore
                    cur = conn.cursor()
                    try:
                        if hasattr(_db, '_apply_readonly_timeouts'):
                            _db._apply_readonly_timeouts(cur, lock_ms=500, statement_ms=1200, idle_s=2)  # type: ignore
                    except Exception:
                        pass
                    try:
                        cur.execute("SELECT 1")
                        _ = cur.fetchone()
                    except Exception:
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                logging.debug("_get_db: verificación SELECT 1 OK")
            except Exception as e:
                logging.exception(f"_get_db: verificación de conexión falló tras init: {e}")
                # Invalidar si la verificación falla para permitir reintentos controlados
                _db = None
            # Prefetch asíncrono de credenciales del dueño para respuesta más rápida
            try:
                if _db is not None and hasattr(_db, 'prefetch_owner_credentials_async'):
                    _db.prefetch_owner_credentials_async(ttl_seconds=600)  # type: ignore
            except Exception:
                pass
            if _db is not None:
                try:
                    logging.info("_get_db: DatabaseManager inicializado")
                except Exception:
                    pass
                # Seed inicial: si hay WEBAPP_OWNER_PASSWORD y la DB no tiene valor, guardarlo en configuración
                try:
                    env_pwd = os.getenv("WEBAPP_OWNER_PASSWORD", "").strip() or os.getenv("OWNER_PASSWORD", "").strip()
                    if env_pwd and hasattr(_db, 'obtener_configuracion') and hasattr(_db, 'actualizar_configuracion'):
                        try:
                            current = _db.obtener_configuracion('owner_password')  # type: ignore
                        except Exception:
                            current = None
                        if not current:
                            try:
                                # Hashear la contraseña antes de almacenarla
                                from security_utils import SecurityUtils
                                hashed_pwd = SecurityUtils.hash_password(env_pwd)
                                ok = _db.actualizar_configuracion('owner_password', hashed_pwd)  # type: ignore
                                if ok:
                                    try:
                                        logging.info("_get_db: owner_password hasheada sembrada desde variable de entorno (solo inicial)")
                                    except Exception:
                                        pass
                            except Exception:
                                # Fallback: guardar sin hashear si hay error
                                try:
                                    ok = _db.actualizar_configuracion('owner_password', env_pwd)  # type: ignore
                                    if ok:
                                        try:
                                            logging.info("_get_db: owner_password sembrada desde variable de entorno (sin hash)")
                                        except Exception:
                                            pass
                                except Exception:
                                    # No bloquear si falla el seed
                                    pass
                except Exception:
                    pass
        except Exception as e:
            try:
                logging.exception(f"_get_db: error inicializando DatabaseManager: {e}")
            except Exception:
                pass
            _db = None
        finally:
            _db_initializing = False
    return _db


# Guard sencillo para responder 503 cuando el Circuit Breaker esté abierto
def _circuit_guard_json(db: DatabaseManager, endpoint: str = "") -> Optional[JSONResponse]:
    try:
        if hasattr(db, "is_circuit_open") and callable(getattr(db, "is_circuit_open")):
            if db.is_circuit_open():  # type: ignore
                state = {}
                try:
                    if hasattr(db, "get_circuit_state") and callable(getattr(db, "get_circuit_state")):
                        state = db.get_circuit_state()  # type: ignore
                except Exception:
                    state = {"open": True}
                try:
                    logging.warning(f"{endpoint or '[endpoint]'}: circuito abierto -> 503; state={state}")
                except Exception:
                    pass
                return JSONResponse({
                    "error": "Servicio temporalmente no disponible",
                    "circuit": state,
                }, status_code=503)
    except Exception as e:
        try:
            logging.exception(f"{endpoint or '[endpoint]'}: error comprobando circuito: {e}")
        except Exception:
            pass
    return None


def _force_db_init() -> Optional[DatabaseManager]:
    """Inicializa la instancia global de DB y valida una consulta ligera.

    Se usa en el arranque y como reintento defensivo antes de devolver 500.
    """
    global _db, _db_initializing
    # Log de parámetros de conexión (sin exponer credenciales)
    def _redact_dsn(dsn: str) -> str:
        try:
            import re as _re
            return _re.sub(r"//([^:@]+):([^@]+)@", lambda m: f"//{m.group(1)}:***@", dsn)
        except Exception:
            return "<redacted>"
    try:
        dsn = os.getenv("DATABASE_URL", "").strip()
        db_host = os.getenv("DB_HOST", "").strip()
        db_port = os.getenv("DB_PORT", "").strip()
        db_name = os.getenv("DB_NAME", "").strip()
        try:
            logging.info(f"_force_db_init: intentando iniciar DB host={db_host or '-'} port={db_port or '-'} name={db_name or '-'} url={'present' if dsn else 'missing'} dsn_masked={_redact_dsn(dsn) if dsn else ''}")
        except Exception:
            pass
    except Exception:
        pass
    with _db_lock:
        try:
            # Si ya está inicializado, devolverlo
            if _db is not None:
                return _db
            _db_initializing = True
            # Evitar NoneType si la importación falló
            if DatabaseManager is None:
                try:
                    logging.error("_force_db_init: DatabaseManager no disponible (import falló)")
                except Exception:
                    pass
                _db = None
            else:
                _db = DatabaseManager()
            try:
                if hasattr(_db, 'ensure_indexes'):
                    _db.ensure_indexes()  # type: ignore
            except Exception:
                pass
            # Verificación ligera de conexión para evitar estados a medio inicializar
            try:
                with _db.get_connection_context() as conn:  # type: ignore
                    cur = conn.cursor()
                    cur.execute("SELECT 1")
                    _ = cur.fetchone()
                    try:
                        logging.debug("_force_db_init: verificación SELECT 1 OK")
                    except Exception:
                        pass
            except Exception as e:
                try:
                    logging.exception(f"Verificación de DB tras inicializar falló: {e}")
                except Exception:
                    pass
                # Invalidar para que el caller pueda manejar el fallo
                _db = None
            if _db is not None:
                try:
                    logging.info("DatabaseManager inicializado correctamente en startup")
                except Exception:
                    pass
                return _db
        except Exception as e:
            try:
                logging.error(f"Error inicializando DatabaseManager: {e}")
            except Exception:
                pass
            _db = None
            return None
        finally:
            _db_initializing = False


# Inicializar DB en el arranque del servidor para evitar 500 intermitentes
@app.on_event("startup")
async def _startup_init_db():
    try:
        if _get_db() is None:
            _force_db_init()
        pm = _get_pm()
        if pm is not None:
            try:
                pm.asegurar_concepto_cuota_mensual()
            except Exception:
                # No bloquear el arranque si falla la autocreación
                pass
        # Eliminado: aseguramiento anterior de updated_at en remoto.
    except Exception:
        # No bloquear el arranque; los endpoints intentarán reintentar
        pass

# Inicio/apagado de motores externos de replicación retirado.
# La replicación se administra desde el servidor PostgreSQL (publications/subscriptions).


def require_owner(request: Request):
    if not request.session.get("logged_in"):
        raise HTTPException(status_code=401, detail="Acceso restringido al dueño")
    return True


def require_gestion_access(request: Request):
    if request.session.get("logged_in") or request.session.get("gestion_profesor_id") or request.session.get("gestion_profesor_user_id"):
        return True
    raise HTTPException(status_code=401, detail="Acceso restringido a Gestión")

def require_usuario(request: Request):
    uid = request.session.get("usuario_id")
    if not uid:
        raise HTTPException(status_code=401, detail="Acceso restringido a Usuario")
    return True

# Helper: agrupa ejercicios de una rutina por día, ordenados
def _build_exercises_by_day(rutina: "Rutina") -> Dict[int, list]:
    try:
        grupos: Dict[int, list] = {}
        ejercicios = getattr(rutina, "ejercicios", []) or []
        for r in ejercicios:
            try:
                # Soportar tanto objetos RutinaEjercicio como dicts provenientes de DB/plantillas
                if isinstance(r, dict):
                    # Construir objeto si las dataclasses están disponibles; si no, mantener dict y completar nombre
                    rid_val = getattr(rutina, "id", None)
                    rid = int(rid_val) if rid_val is not None else 0
                    if 'RutinaEjercicio' in globals() and RutinaEjercicio is not None and 'Ejercicio' in globals() and Ejercicio is not None:
                        try:
                            r_obj = RutinaEjercicio(
                                id=r.get("id"),
                                rutina_id=int(r.get("rutina_id") or rid or 0),
                                ejercicio_id=int(r.get("ejercicio_id") or 0),
                                dia_semana=int(r.get("dia_semana") or 1),
                                series=r.get("series"),
                                repeticiones=r.get("repeticiones"),
                                orden=int(r.get("orden") or 0),
                                ejercicio=None
                            )
                        except Exception:
                            # Fallback mínimo en caso de tipos no convertibles
                            r_obj = RutinaEjercicio(rutina_id=rid, ejercicio_id=int(r.get("ejercicio_id") or 0), dia_semana=int(r.get("dia_semana") or 1))
                        ej = r.get("ejercicio")
                        try:
                            if isinstance(ej, dict):
                                r_obj.ejercicio = Ejercicio(
                                    id=int(ej.get("id") or r_obj.ejercicio_id or 0),
                                    nombre=str(ej.get("nombre") or ""),
                                    grupo_muscular=ej.get("grupo_muscular"),
                                    descripcion=ej.get("descripcion")
                                )
                            elif ej is not None:
                                # Si ya es un objeto Ejercicio
                                r_obj.ejercicio = ej  # type: ignore
                            else:
                                r_obj.ejercicio = Ejercicio(id=int(r_obj.ejercicio_id or 0))
                        except Exception:
                            r_obj.ejercicio = None
                        # Determinar nombre visible
                        nombre_actual = r.get("nombre_ejercicio")
                        if not nombre_actual:
                            nombre_nested = getattr(r_obj.ejercicio, "nombre", None) if r_obj.ejercicio is not None else None
                            if nombre_nested:
                                nombre_actual = nombre_nested
                            else:
                                eid = r_obj.ejercicio_id
                                nombre_actual = f"Ejercicio {eid}" if eid else "Ejercicio"
                        try:
                            setattr(r_obj, "nombre_ejercicio", nombre_actual)
                        except Exception:
                            pass
                        r = r_obj
                    else:
                        # Completar nombre en dict si no podemos convertir a objeto
                        nombre_actual = r.get("nombre_ejercicio")
                        if not nombre_actual:
                            ej = r.get("ejercicio")
                            if isinstance(ej, dict):
                                nombre_actual = ej.get("nombre") or None
                            if not nombre_actual:
                                eid = r.get("ejercicio_id")
                                nombre_actual = f"Ejercicio {eid}" if eid is not None else "Ejercicio"
                        r["nombre_ejercicio"] = nombre_actual
                else:
                    # Objeto: asegurar nombre visible con nested fallback
                    nombre_actual = getattr(r, "nombre_ejercicio", None)
                    if not nombre_actual:
                        nombre_nested = getattr(getattr(r, "ejercicio", None), "nombre", None)
                        if nombre_nested:
                            try:
                                setattr(r, "nombre_ejercicio", nombre_nested)
                            except Exception:
                                pass
                        else:
                            eid = getattr(r, "ejercicio_id", None)
                            try:
                                setattr(r, "nombre_ejercicio", f"Ejercicio {eid}" if eid is not None else "Ejercicio")
                            except Exception:
                                pass
            except Exception:
                # No bloquear por errores de normalización de un ejercicio
                pass
            # Día
            dia = getattr(r, "dia_semana", None) if not isinstance(r, dict) else r.get("dia_semana")
            if dia is None:
                continue
            try:
                grupos.setdefault(int(dia), []).append(r)
            except Exception:
                # Si no se puede convertir a int, ignorar
                continue
        # Ordenar cada día por 'orden' y nombre para consistencia visual
        for dia, arr in grupos.items():
            try:
                def _orden_val(e):
                    try:
                        return int(getattr(e, "orden", 0) or 0) if not isinstance(e, dict) else int(e.get("orden") or 0)
                    except Exception:
                        return 0
                def _nombre_val(e):
                    try:
                        if isinstance(e, dict):
                            return str(e.get("nombre_ejercicio") or ((e.get("ejercicio") or {}).get("nombre") if isinstance(e.get("ejercicio"), dict) else ""))
                        return str(getattr(e, "nombre_ejercicio", None) or getattr(getattr(e, "ejercicio", None), "nombre", ""))
                    except Exception:
                        return ""
                arr.sort(key=lambda e: (_orden_val(e), _nombre_val(e)))
            except Exception:
                # Si falla el sort por algún dato, dejar tal cual
                pass
        return grupos
    except Exception:
        return {}

@app.get("/api/rutinas/{rutina_id}/export/pdf")
async def api_rutina_export_pdf(rutina_id: int, weeks: int = 1, filename: Optional[str] = None, _=Depends(require_gestion_access)):
    # Desactivado en la webapp: usar la previsualización de Excel con Google Viewer
    raise HTTPException(status_code=410, detail="Exportación PDF desactivada en la webapp; use la previsualización de Excel.")

@app.get("/api/rutinas/{rutina_id}/export/excel")
async def api_rutina_export_excel(rutina_id: int, weeks: int = 1, filename: Optional[str] = None, _=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="Base de datos no disponible")
    guard = _circuit_guard_json(db, "/api/rutinas/{rutina_id}/export/excel")
    if guard:
        return guard
    rm = _get_rm()
    if rm is None:
        raise HTTPException(status_code=500, detail="Gestor de rutinas no disponible")
    try:
        rutina = db.obtener_rutina_completa(rutina_id)  # type: ignore
        if rutina is None:
            raise HTTPException(status_code=404, detail="Rutina no encontrada")
        # Obtener usuario asociado (con respaldo si no existe)
        # Soportar objeto o dict
        u_raw = getattr(rutina, "usuario_id", None)
        if u_raw is None and isinstance(rutina, dict):
            try:
                u_raw = rutina.get("usuario_id") or (rutina.get("usuario") or {}).get("id")
            except Exception:
                u_raw = None
        try:
            u_id = int(u_raw) if u_raw is not None else None
        except Exception:
            u_id = None
        usuario = None
        if u_id is not None:
            try:
                usuario = db.obtener_usuario(u_id)  # type: ignore
            except Exception:
                usuario = None
        # Si no se pudo obtener o el nombre está vacío, intentar por JOIN directo
        try:
            nombre_ok = (getattr(usuario, "nombre", None) or "").strip() if usuario else ""
        except Exception:
            nombre_ok = ""
        if not nombre_ok:
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
                        (int(rutina_id),)
                    )
                    row = cur.fetchone() or {}
                    u_nombre = (row.get("usuario_nombre") or "").strip()
                    u_dni = (row.get("dni") or "").strip() or None
                    u_tel = (row.get("telefono") or "").strip()
                    if u_nombre:
                        try:
                            usuario = Usuario(id=u_id, nombre=u_nombre, dni=u_dni, telefono=u_tel)  # type: ignore
                        except Exception:
                            class _Usr2:
                                def __init__(self, id=None, nombre="", dni=None, telefono=""):
                                    self.id = id; self.nombre = nombre; self.dni = dni; self.telefono = telefono
                            usuario = _Usr2(id=u_id, nombre=u_nombre, dni=u_dni, telefono=u_tel)
            except Exception:
                pass
        # Fallback final si sigue sin nombre: usar 'Plantilla' SOLO si no hay usuario_id (plantilla pura)
        if usuario is None or not (getattr(usuario, "nombre", "") or "").strip():
            try:
                if u_id is None:
                    usuario = Usuario(nombre="Plantilla")  # type: ignore
                else:
                    usuario = Usuario(nombre="")  # No forzar 'Plantilla' para rutinas de usuario
            except Exception:
                class _Usr:
                    def __init__(self, nombre: str):
                        self.nombre = nombre
                usuario = _Usr("" if u_id is not None else "Plantilla")
        ejercicios_por_dia = _build_exercises_by_day(rutina)
        # Asegurar semanas dentro de un rango razonable (evita valores extremos del cliente)
        try:
            weeks = max(1, min(int(weeks), 4))
        except Exception:
            weeks = 1
        out_path = rm.generate_routine_excel(rutina, usuario, ejercicios_por_dia, weeks=weeks)
        # Determinar nombre de archivo final (permite override por query param)
        try:
            final_name = os.path.basename(filename) if filename else os.path.basename(out_path)
            if not final_name.lower().endswith(".xlsx"):
                final_name = f"{final_name}.xlsx"
            final_name = final_name[:150]
        except Exception:
            final_name = os.path.basename(out_path)
        return FileResponse(
            out_path,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=final_name,
        )
    except HTTPException:
        raise
    except Exception as e:
        logging.exception("Error generando Excel de rutina")
        raise HTTPException(status_code=500, detail=str(e))

# URL firmada para incrustar Excel en Google Viewer (requiere sesión para generar la URL)
@app.get("/api/rutinas/{rutina_id}/export/excel_view_url")
async def api_rutina_excel_view_url(rutina_id: int, request: Request, weeks: int = 1, filename: Optional[str] = None, _=Depends(require_gestion_access)):
    # Normalizar nombre
    try:
        base_name = os.path.basename(filename) if filename else f"rutina_{rutina_id}.xlsx"
        if not base_name.lower().endswith(".xlsx"):
            base_name = f"{base_name}.xlsx"
        # Limitar longitud y sanitizar caracteres
        base_name = base_name[:150]
        base_name = base_name.replace("\\", "_").replace("/", "_").replace(":", "_").replace("*", "_").replace("?", "_").replace("\"", "_").replace("<", "_").replace(">", "_").replace("|", "_")
    except Exception:
        base_name = f"rutina_{rutina_id}.xlsx"
    # Semanas dentro de rango
    try:
        weeks = max(1, min(int(weeks), 4))
    except Exception:
        weeks = 1
    ts = int(time.time())
    sig = _sign_excel_view(rutina_id, weeks, base_name, ts)
    base_url = get_webapp_base_url("")
    # Construir URL absoluta requerida por Google Viewer
    if not base_url:
        # Usar la URL base de la petición actual (incluye host:puerto correcto)
        try:
            base_url = str(request.base_url).rstrip('/')
        except Exception:
            base_url = "http://localhost:8000"
    params = {
        "weeks": str(weeks),
        "filename": base_name,
        "ts": str(ts),
        "sig": sig,
    }
    qs = urllib.parse.urlencode(params, safe="")
    full = f"{base_url}/api/rutinas/{rutina_id}/export/excel_view?{qs}"
    return JSONResponse({"url": full})

# Endpoint público (sin sesión) que sirve el XLSX firmado en modo inline para el visor
@app.get("/api/rutinas/{rutina_id}/export/excel_view")
async def api_rutina_excel_view(rutina_id: int, weeks: int = 1, filename: Optional[str] = None, ts: int = 0, sig: Optional[str] = None):
    if not sig:
        raise HTTPException(status_code=403, detail="Firma requerida")
    try:
        weeks = max(1, min(int(weeks), 4))
    except Exception:
        weeks = 1
    # Normalizar nombre de archivo
    try:
        base_name = os.path.basename(filename) if filename else f"rutina_{rutina_id}.xlsx"
        if not base_name.lower().endswith(".xlsx"):
            base_name = f"{base_name}.xlsx"
        base_name = base_name[:150]
        base_name = base_name.replace("\\", "_").replace("/", "_").replace(":", "_").replace("*", "_").replace("?", "_").replace("\"", "_").replace("<", "_").replace(">", "_").replace("|", "_")
    except Exception:
        base_name = f"rutina_{rutina_id}.xlsx"
    # Verificar ventana de tiempo (10 minutos)
    try:
        now = int(time.time())
        if abs(now - int(ts)) > 600:
            raise HTTPException(status_code=403, detail="Link de previsualización expirado")
    except Exception:
        raise HTTPException(status_code=403, detail="Timestamp inválido")
    # Verificar firma
    expected = _sign_excel_view(rutina_id, weeks, base_name, int(ts))
    if not hmac.compare_digest(expected, str(sig)):
        raise HTTPException(status_code=403, detail="Firma inválida")
    # Generar Excel como en el endpoint autenticado
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="Base de datos no disponible")
    rm = _get_rm()
    if rm is None:
        raise HTTPException(status_code=500, detail="Gestor de rutinas no disponible")
    try:
        rutina = db.obtener_rutina_completa(rutina_id)  # type: ignore
        if rutina is None:
            raise HTTPException(status_code=404, detail="Rutina no encontrada")
        # Usuario asociado (con respaldo)
        # Soportar objeto o dict
        u_raw = getattr(rutina, "usuario_id", None)
        if u_raw is None and isinstance(rutina, dict):
            try:
                u_raw = rutina.get("usuario_id") or (rutina.get("usuario") or {}).get("id")
            except Exception:
                u_raw = None
        try:
            u_id = int(u_raw) if u_raw is not None else None
        except Exception:
            u_id = None
        usuario = None
        if u_id is not None:
            try:
                usuario = db.obtener_usuario(u_id)  # type: ignore
            except Exception:
                usuario = None
        # Si no se pudo obtener o el nombre está vacío, intentar por JOIN directo
        try:
            nombre_ok = (getattr(usuario, "nombre", None) or "").strip() if usuario else ""
        except Exception:
            nombre_ok = ""
        if not nombre_ok:
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
                        (int(rutina_id),)
                    )
                    row = cur.fetchone() or {}
                    u_nombre = (row.get("usuario_nombre") or "").strip()
                    u_dni = (row.get("dni") or "").strip() or None
                    u_tel = (row.get("telefono") or "").strip()
                    if u_nombre:
                        try:
                            usuario = Usuario(id=u_id, nombre=u_nombre, dni=u_dni, telefono=u_tel)  # type: ignore
                        except Exception:
                            class _Usr2:
                                def __init__(self, id=None, nombre="", dni=None, telefono=""):
                                    self.id = id; self.nombre = nombre; self.dni = dni; self.telefono = telefono
                            usuario = _Usr2(id=u_id, nombre=u_nombre, dni=u_dni, telefono=u_tel)
            except Exception:
                pass
        # Fallback final si sigue sin nombre: usar 'Plantilla' SOLO si no hay usuario_id (plantilla pura)
        if usuario is None or not (getattr(usuario, "nombre", "") or "").strip():
            try:
                if u_id is None:
                    usuario = Usuario(nombre="Plantilla")  # type: ignore
                else:
                    usuario = Usuario(nombre="")
            except Exception:
                class _Usr:
                    def __init__(self, nombre: str):
                        self.nombre = nombre
                usuario = _Usr("" if u_id is not None else "Plantilla")
        ejercicios_por_dia = _build_exercises_by_day(rutina)
        out_path = rm.generate_routine_excel(rutina, usuario, ejercicios_por_dia, weeks=weeks)
        # Servir inline para permitir incrustación en Google Viewer
        resp = FileResponse(
            out_path,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=base_name,
        )
        try:
            resp.headers["Content-Disposition"] = f'inline; filename="{base_name}"'
            # Evitar caché agresivo en enlaces temporales
            resp.headers["Cache-Control"] = "private, max-age=60"
        except Exception:
            pass
        return resp
    except HTTPException:
        raise
    except Exception as e:
        logging.exception("Error generando Excel inline de rutina")
        raise HTTPException(status_code=500, detail=str(e))


# URL firmada para previsualización con datos efímeros (borrador en memoria)
@app.post("/api/rutinas/preview/excel_view_url")
async def api_rutina_preview_excel_view_url(request: Request, weeks: int = 1, filename: Optional[str] = None, _=Depends(require_gestion_access)):
    # Leer payload efímero desde el cuerpo
    try:
        data = await request.json()
        if not isinstance(data, dict):
            raise HTTPException(status_code=400, detail="Payload inválido")
        payload = data.get("payload") if "payload" in data else data
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="Payload inválido")
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=400, detail="Cuerpo JSON requerido")
    # Generar URL firmada con manejo de errores detallado
    try:
        # Semanas dentro de rango
        try:
            weeks = max(1, min(int(weeks), 4))
        except Exception:
            weeks = 1
        # Normalizar nombre
        try:
            base_name = os.path.basename(filename) if filename else (payload.get("filename") or "rutina_preview.xlsx")
            if not base_name.lower().endswith(".xlsx"):
                base_name = f"{base_name}.xlsx"
            base_name = base_name[:150]
            base_name = base_name.replace("\\", "_").replace("/", "_").replace(":", "_").replace("*", "_").replace("?", "_").replace("\"", "_").replace("<", "_").replace(">", "_").replace("|", "_")
        except Exception:
            base_name = "rutina_preview.xlsx"
        # Codificar payload y firmar URL de forma stateless (compatible con serverless)
        data = _encode_preview_payload(payload)
        try:
            if not data or len(data) > 64_000:
                # Limitar tamaño por seguridad de URL y visor
                raise HTTPException(status_code=400, detail="Payload demasiado grande para previsualización")
        except HTTPException:
            raise
        except Exception:
            pass
        ts = int(time.time())
        sig = _sign_excel_view_draft_data(data, weeks, base_name, ts)
        try:
            base_url = get_webapp_base_url("")
        except Exception:
            base_url = ""
        if not base_url:
            try:
                base_url = str(request.base_url)
            except Exception:
                base_url = "http://localhost:8000"
        # Normalizar para evitar doble barra en la concatenación
        base_url = str(base_url).rstrip('/')
        params = {
            "data": data,
            "weeks": str(weeks),
            "filename": base_name,
            "ts": str(ts),
            "sig": sig,
        }
        qs = urllib.parse.urlencode(params, safe="")
        full = f"{base_url}/api/rutinas/preview/excel_view.xlsx?{qs}"
        return JSONResponse({"url": full})
    except HTTPException:
        # Propagar errores de validación conocidos
        raise
    except Exception as e:
        logging.exception("Error generando URL firmada de previsualización")
        raise HTTPException(status_code=500, detail=str(e))

# Endpoint público para servir XLSX firmado desde borrador efímero
@app.get("/api/rutinas/preview/excel_view")
async def api_rutina_preview_excel_view(request: Request, pid: Optional[str] = None, data: Optional[str] = None, weeks: int = 1, filename: Optional[str] = None, ts: int = 0, sig: Optional[str] = None):
    if not sig:
        raise HTTPException(status_code=403, detail="Firma requerida")
    if not (pid or data):
        raise HTTPException(status_code=400, detail="Datos de previsualización requeridos")
    try:
        weeks = max(1, min(int(weeks), 4))
    except Exception:
        weeks = 1
    # Normalizar nombre de archivo
    try:
        base_name = os.path.basename(filename) if filename else "rutina_preview.xlsx"
        if not base_name.lower().endswith(".xlsx"):
            base_name = f"{base_name}.xlsx"
        base_name = base_name[:150]
        base_name = base_name.replace("\\", "_").replace("/", "_").replace(":", "_").replace("*", "_").replace("?", "_").replace("\"", "_").replace("<", "_").replace(">", "_").replace("|", "_")
    except Exception:
        base_name = "rutina_preview.xlsx"
    # Verificar ventana de tiempo (10 minutos)
    try:
        now = int(time.time())
        if abs(now - int(ts)) > 600:
            raise HTTPException(status_code=403, detail="Link de previsualización expirado")
    except Exception:
        raise HTTPException(status_code=403, detail="Timestamp inválido")
    # Verificar firma
    if data:
        expected = _sign_excel_view_draft_data(str(data), weeks, base_name, int(ts))
        if not hmac.compare_digest(expected, str(sig)):
            raise HTTPException(status_code=403, detail="Firma inválida")
    else:
        expected = _sign_excel_view_draft(str(pid), weeks, base_name, int(ts))
        if not hmac.compare_digest(expected, str(sig)):
            raise HTTPException(status_code=403, detail="Firma inválida")
    rm = _get_rm()
    if rm is None:
        raise HTTPException(status_code=500, detail="Gestor de rutinas no disponible")
    try:
        if data:
            draft_payload = _decode_preview_payload(str(data))
            if not isinstance(draft_payload, dict):
                raise HTTPException(status_code=400, detail="Payload de previsualización inválido")
            rutina, usuario, ejercicios_por_dia = _build_rutina_from_draft(draft_payload)
        else:
            draft = _get_excel_preview_draft(str(pid))
            if not draft:
                raise HTTPException(status_code=404, detail="Borrador no encontrado o expirado")
            rutina, usuario, ejercicios_por_dia = _build_rutina_from_draft(draft)
        # Decidir si usar QR real (rutina existente en DB) o QR efímero de preview
        use_preview_qr = True
        try:
            uuid_actual = (getattr(rutina, "uuid_rutina", "") or getattr(rutina, "uuid", "") or "")
        except Exception:
            uuid_actual = ""
        try:
            db = _get_db()
        except Exception:
            db = None
        if isinstance(uuid_actual, str) and uuid_actual and db is not None:
            try:
                rutina_db = db.obtener_rutina_completa_por_uuid_dict(uuid_actual)  # type: ignore
                if rutina_db:
                    use_preview_qr = False
            except Exception:
                use_preview_qr = True

        if not use_preview_qr:
            # Mantener UUID real y apuntar al endpoint de QR real
            try:
                setattr(rutina, "qr_is_preview", False)
            except Exception:
                pass
        else:
            # Estabilizar UUID efímero en sesión para que el QR no cambie aunque se modifique el contenido
            try:
                sess_uuid = request.session.get("preview_rutina_uuid")
            except Exception:
                sess_uuid = None
            if not (isinstance(sess_uuid, str) and sess_uuid):
                try:
                    sess_uuid = str(uuid.uuid4())
                    request.session["preview_rutina_uuid"] = sess_uuid
                except Exception:
                    sess_uuid = (getattr(rutina, "uuid_rutina", "") or getattr(rutina, "uuid", "") or "")
            try:
                if isinstance(sess_uuid, str) and sess_uuid:
                    setattr(rutina, "uuid_rutina", sess_uuid)
                    setattr(rutina, "uuid", sess_uuid)
            except Exception:
                pass
            # Marcar rutina en modo previsualización para que el QR apunte al endpoint de preview
            try:
                setattr(rutina, "qr_is_preview", True)
            except Exception:
                pass
            # Guardar representación efímera por UUID para habilitar el escaneo del QR en preview
            try:
                uuid_val = (getattr(rutina, "uuid_rutina", "") or getattr(rutina, "uuid", "") or "")
            except Exception:
                uuid_val = ""
            if isinstance(uuid_val, str) and uuid_val:
                try:
                    ejercicios_list = []
                    for d, items in (ejercicios_por_dia or {}).items():
                        for re in items or []:
                            try:
                                ejercicios_list.append({
                                    "dia_semana": int(getattr(re, "dia_semana", d) or d),
                                    "series": getattr(re, "series", "") or "",
                                    "repeticiones": getattr(re, "repeticiones", "") or "",
                                    "orden": int(getattr(re, "orden", 0) or 0),
                                    "ejercicio_id": int(getattr(re, "ejercicio_id", 0) or 0),
                                    "ejercicio": {
                                        "nombre": (getattr(re, "nombre_ejercicio", None) or (getattr(getattr(re, "ejercicio", None), "nombre", None))),
                                        "descripcion": None,
                                        "video_url": None,
                                    }
                                })
                            except Exception:
                                continue
                    rutina_dict = {
                        "id": None,
                        "usuario_id": None,
                        "uuid_rutina": uuid_val,
                        "nombre_rutina": getattr(rutina, "nombre_rutina", "") or "",
                        "descripcion": getattr(rutina, "descripcion", None),
                        "dias_semana": getattr(rutina, "dias_semana", 1) or 1,
                        "categoria": getattr(rutina, "categoria", "general") or "general",
                        "activa": True,
                        "ejercicios": ejercicios_list,
                    }
                    _save_excel_preview_routine(uuid_val, rutina_dict)
                except Exception:
                    pass
        out_path = rm.generate_routine_excel(rutina, usuario, ejercicios_por_dia, weeks=weeks)
        resp = FileResponse(
            out_path,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=base_name,
        )
        try:
            resp.headers["Content-Disposition"] = f'inline; filename="{base_name}"'
            # Permitir caché pública breve para compatibilidad con Office Viewer/CDN
            resp.headers["Cache-Control"] = "public, max-age=120"
            resp.headers["X-Content-Type-Options"] = "nosniff"
            resp.headers["Accept-Ranges"] = "bytes"
        except Exception:
            pass
        return resp
    except HTTPException:
        raise
    except Exception as e:
        logging.exception("Error generando Excel inline de borrador de rutina")
        raise HTTPException(status_code=500, detail=str(e))

# Variante con extensión .xlsx para compatibilidad con Office Viewer
@app.get("/api/rutinas/preview/excel_view.xlsx")
async def api_rutina_preview_excel_view_ext(request: Request, pid: Optional[str] = None, data: Optional[str] = None, weeks: int = 1, filename: Optional[str] = None, ts: int = 0, sig: Optional[str] = None):
    return await api_rutina_preview_excel_view(request=request, pid=pid, data=data, weeks=weeks, filename=filename, ts=ts, sig=sig)

# --- API Rutinas ---
@app.get("/api/rutinas")
async def api_rutinas_get(usuario_id: Optional[int] = None, _=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        return []
    guard = _circuit_guard_json(db, "/api/rutinas")
    if guard:
        return guard
    try:
        if usuario_id is not None:
            rutinas = db.obtener_rutinas_por_usuario(int(usuario_id))  # type: ignore
            items = []
            for r in rutinas or []:
                is_dict = isinstance(r, dict)
                rid = (r.get("id") if is_dict else getattr(r, "id", None))
                usuario = (r.get("usuario_id") if is_dict else getattr(r, "usuario_id", None))
                nombre = (r.get("nombre_rutina") if is_dict else getattr(r, "nombre_rutina", None))
                descripcion = (r.get("descripcion") if is_dict else getattr(r, "descripcion", None))
                dias = (r.get("dias_semana") if is_dict else getattr(r, "dias_semana", None))
                categoria = (r.get("categoria") if is_dict else getattr(r, "categoria", "general"))
                fecha_creacion = (r.get("fecha_creacion") if is_dict else getattr(r, "fecha_creacion", None))
                activa = (r.get("activa") if is_dict else getattr(r, "activa", True))
                items.append({
                    "id": int(rid) if rid is not None else None,
                    "usuario_id": usuario,
                    "nombre_rutina": nombre,
                    "descripcion": descripcion,
                    "dias_semana": dias,
                    "categoria": categoria,
                    "fecha_creacion": fecha_creacion,
                    "activa": bool(activa),
                })
            return items
        else:
            rows = db.obtener_todas_rutinas()  # type: ignore
            return [
                {
                    "id": int(row.get("id")) if row.get("id") is not None else None,
                    "usuario_id": row.get("usuario_id"),
                    "nombre_rutina": row.get("nombre_rutina"),
                    "descripcion": row.get("descripcion"),
                    "dias_semana": row.get("dias_semana"),
                    "categoria": row.get("categoria"),
                    "fecha_creacion": row.get("fecha_creacion"),
                    "activa": bool(row.get("activa", True)),
                }
                for row in rows or []
            ]
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/rutinas/plantillas")
async def api_rutinas_plantillas(_=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        return []
    guard = _circuit_guard_json(db, "/api/rutinas/plantillas")
    if guard:
        return guard
    try:
        plantillas = db.obtener_plantillas_rutina()  # type: ignore
        return [
            {
                "id": int(r.id) if r.id is not None else None,
                "usuario_id": r.usuario_id,
                "nombre_rutina": r.nombre_rutina,
                "descripcion": r.descripcion,
                "dias_semana": r.dias_semana,
                "categoria": getattr(r, "categoria", "general"),
                "fecha_creacion": r.fecha_creacion,
                "activa": bool(getattr(r, "activa", True)),
            }
            for r in plantillas
        ]
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/rutinas/{rutina_id}")
async def api_rutina_detalle(rutina_id: int, _=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, f"/api/rutinas/{rutina_id}")
    if guard:
        return guard
    try:
        r = db.obtener_rutina_completa(int(rutina_id))  # type: ignore
        if not r:
            raise HTTPException(status_code=404, detail="Rutina no encontrada")
        ejercicios_out = []
        for re in getattr(r, "ejercicios", []) or []:
            ej = getattr(re, "ejercicio", None)
            ejercicios_out.append({
                "id": re.id,
                "rutina_id": re.rutina_id,
                "ejercicio_id": re.ejercicio_id,
                "dia_semana": re.dia_semana,
                "series": re.series,
                "repeticiones": re.repeticiones,
                "orden": re.orden,
                "ejercicio": {
                    "id": getattr(ej, "id", re.ejercicio_id) if ej is not None else re.ejercicio_id,
                    "nombre": getattr(ej, "nombre", None) if ej is not None else None,
                    "grupo_muscular": getattr(ej, "grupo_muscular", None) if ej is not None else None,
                    "descripcion": getattr(ej, "descripcion", None) if ej is not None else None,
                }
            })
        return {
            "id": int(r.id) if r.id is not None else None,
            "usuario_id": r.usuario_id,
            "nombre_rutina": r.nombre_rutina,
            "descripcion": r.descripcion,
            "dias_semana": r.dias_semana,
            "categoria": getattr(r, "categoria", "general"),
            "fecha_creacion": r.fecha_creacion,
            "activa": bool(getattr(r, "activa", True)),
            "ejercicios": ejercicios_out,
        }
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/rutinas")
async def api_rutinas_create(request: Request, _=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, "/api/rutinas[POST]")
    if guard:
        return guard
    if Rutina is None:
        raise HTTPException(status_code=503, detail="Modelo Rutina no disponible")
    payload = await request.json()
    try:
        nombre_rutina = (payload.get("nombre_rutina") or "").strip()
        if not nombre_rutina:
            raise HTTPException(status_code=400, detail="'nombre_rutina' es obligatorio")
        descripcion = payload.get("descripcion")
        dias_semana = int(payload.get("dias_semana") or 1)
        categoria = (payload.get("categoria") or "general").strip() or "general"
        usuario_id = payload.get("usuario_id")
        rutina = Rutina(usuario_id=usuario_id, nombre_rutina=nombre_rutina, descripcion=descripcion, dias_semana=dias_semana, categoria=categoria)  # type: ignore
        new_id = db.crear_rutina(rutina)  # type: ignore

        ejercicios = payload.get("ejercicios")
        if ejercicios and isinstance(ejercicios, list):
            rutina_ejs = []
            for item in ejercicios:
                try:
                    rutina_ejs.append(RutinaEjercicio(
                        rutina_id=int(new_id),
                        ejercicio_id=int(item.get("ejercicio_id")),
                        dia_semana=int(item.get("dia_semana") or 1),
                        series=item.get("series"),
                        repeticiones=item.get("repeticiones"),
                        orden=item.get("orden"),
                    ))  # type: ignore
                except Exception:
                    # Ignorar entradas inválidas sin detener creación
                    pass
            if rutina_ejs:
                db.guardar_ejercicios_de_rutina(int(new_id), rutina_ejs)  # type: ignore
        return {"ok": True, "id": int(new_id)}
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)


@app.put("/api/rutinas/{rutina_id}")
async def api_rutinas_update(rutina_id: int, request: Request, _=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, f"/api/rutinas/{rutina_id}[PUT]")
    if guard:
        return guard
    if Rutina is None:
        raise HTTPException(status_code=503, detail="Modelo Rutina no disponible")
    payload = await request.json()
    try:
        # Obtener existente para actualización parcial (sin tocar usuario_id aquí)
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(
                "SELECT id, usuario_id, nombre_rutina, descripcion, dias_semana, categoria, fecha_creacion, activa FROM rutinas WHERE id = %s",
                (int(rutina_id),),
            )
            existing = cur.fetchone()
        if not existing:
            raise HTTPException(status_code=404, detail="Rutina no encontrada")

        nombre_rutina = (payload.get("nombre_rutina") or existing.get("nombre_rutina") or "").strip() or (existing.get("nombre_rutina") or "")
        if not nombre_rutina:
            raise HTTPException(status_code=400, detail="'nombre_rutina' es obligatorio")
        descripcion = payload.get("descripcion") if ("descripcion" in payload) else existing.get("descripcion")
        dias_semana = int(payload.get("dias_semana") or existing.get("dias_semana") or 1)
        categoria = (payload.get("categoria") or existing.get("categoria") or "general").strip() or (existing.get("categoria") or "general")
        rutina = Rutina(id=int(rutina_id), usuario_id=existing.get("usuario_id"), nombre_rutina=nombre_rutina, descripcion=descripcion, dias_semana=dias_semana, categoria=categoria)  # type: ignore
        ok = db.actualizar_rutina(rutina)  # type: ignore
        if not ok:
            raise HTTPException(status_code=404, detail="No se pudo actualizar la rutina")

        ejercicios = payload.get("ejercicios")
        if ejercicios and isinstance(ejercicios, list):
            rutina_ejs = []
            for item in ejercicios:
                try:
                    rutina_ejs.append(RutinaEjercicio(
                        rutina_id=int(rutina_id),
                        ejercicio_id=int(item.get("ejercicio_id")),
                        dia_semana=int(item.get("dia_semana") or 1),
                        series=item.get("series"),
                        repeticiones=item.get("repeticiones"),
                        orden=item.get("orden"),
                    ))  # type: ignore
                except Exception:
                    pass
            db.guardar_ejercicios_de_rutina(int(rutina_id), rutina_ejs)  # type: ignore
        return {"ok": True, "id": int(rutina_id)}
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)


@app.delete("/api/rutinas/{rutina_id}")
async def api_rutinas_delete(rutina_id: int, _=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, f"/api/rutinas/{rutina_id}[DELETE]")
    if guard:
        return guard
    try:
        ok = db.eliminar_rutina(int(rutina_id))  # type: ignore
        if not ok:
            raise HTTPException(status_code=404, detail="No se pudo eliminar la rutina")
        return {"ok": True}
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/rutinas/{rutina_id}/assign")
async def api_rutina_assign(rutina_id: int, request: Request, _=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, f"/api/rutinas/{rutina_id}/assign[POST]")
    if guard:
        return guard
    payload = await request.json()
    try:
        usuario_id = payload.get("usuario_id")
        if usuario_id is None:
            raise HTTPException(status_code=400, detail="'usuario_id' es obligatorio")

        # Verificar existencia y estado del usuario
        if not db.usuario_id_existe(int(usuario_id)):  # type: ignore
            raise HTTPException(status_code=404, detail="Usuario no encontrado")
        try:
            u = db.obtener_usuario_por_id(int(usuario_id))  # type: ignore
            if u and not bool(getattr(u, "activo", True)):
                raise HTTPException(status_code=400, detail="Usuario inactivo: no se puede asignar rutina")
        except Exception:
            pass

        # Verificar existencia de la rutina
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("SELECT id FROM rutinas WHERE id = %s", (int(rutina_id),))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Rutina no encontrada")
            cur.execute("UPDATE rutinas SET usuario_id = %s WHERE id = %s", (int(usuario_id), int(rutina_id)))
            conn.commit()
        return {"ok": True}
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/rutinas/{rutina_id}/unassign")
async def api_rutina_unassign(rutina_id: int, _=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, f"/api/rutinas/{rutina_id}/unassign[POST]")
    if guard:
        return guard
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("SELECT id FROM rutinas WHERE id = %s", (int(rutina_id),))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Rutina no encontrada")
            cur.execute("UPDATE rutinas SET usuario_id = NULL WHERE id = %s", (int(rutina_id),))
            conn.commit()
        return {"ok": True}
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)


def start_web_server(db_manager: Optional[DatabaseManager] = None, host: str = "127.0.0.1", port: int = 8003, log_level: str = "info") -> None:
    """Arranca el servidor web en un hilo daemon para no bloquear la UI.

    Si se pasa una instancia de DatabaseManager, se inyecta para que la web la reutilice.
    """
    try:
        global _db
        if db_manager is not None:
            _db = db_manager

        def _run():
            import time as _t
            while True:
                try:
                    # Liberar/limpiar el puerto si está ocupado por otro proceso
                    try:
                        for conn in psutil.net_connections(kind='inet'):
                            laddr = getattr(conn, 'laddr', None)
                            if not laddr:
                                continue
                            if int(getattr(laddr, 'port', 0)) == int(port):
                                pid = conn.pid
                                if pid and pid != os.getpid():
                                    try:
                                        p = psutil.Process(pid)
                                        p.terminate()
                                        p.wait(timeout=2)
                                    except Exception:
                                        try:
                                            p.kill()
                                        except Exception:
                                            pass
                        # pequeña espera para liberar el puerto completamente
                        _t.sleep(0.5)
                    except Exception:
                        pass
                    import uvicorn
                    # Respetar PORT/HOST de entorno en plataformas de hosting
                    env_port = os.getenv("PORT")
                    try:
                        port = int(env_port) if env_port else port
                    except Exception:
                        pass
                    host = os.getenv("HOST", host)
                    config = uvicorn.Config(
                        app,
                        host=host,
                        port=port,
                        log_level=log_level,
                        log_config=None,  # Evita errores de logging sin TTY en ejecutables sin consola
                        loop="asyncio",
                        http="h11",
                        lifespan="off",
                        proxy_headers=(os.getenv("PROXY_HEADERS_ENABLED", "1").strip() in ("1", "true", "yes")),
                    )
                    server = uvicorn.Server(config=config)
                    # Desactivar instalación de manejadores de señales (no permitidos fuera del hilo principal)
                    try:
                        server.install_signal_handlers = lambda: None
                    except Exception:
                        pass
                    # Crear explícitamente un event loop en este hilo y servir
                    try:
                        import asyncio as _asyncio
                        loop = _asyncio.new_event_loop()
                        _asyncio.set_event_loop(loop)
                        loop.run_until_complete(server.serve())
                    except Exception:
                        # Fallback al modo síncrono
                        server.run()
                except Exception:
                    pass
                # Si el servidor se detiene o falla, esperar y reintentar mientras el programa siga abierto
                _t.sleep(1)

        # En ejecutables congelados en Windows, preferir proceso separado para robustez
        try:
            # Importante: en ejecutables congelados en Windows, evitar multiprocessing.
            # Usar siempre hilo dedicado para prevenir re-ejecución del binario (bucle de relanzamiento).
            t = threading.Thread(target=_run, daemon=True)
            t.start()
        except Exception:
            # Fallback defensivo: no bloquear la UI si el hilo falla
            try:
                t = threading.Thread(target=_run, daemon=True)
                t.start()
            except Exception:
                pass
    except Exception:
        pass


# Funciones de túnel público eliminadas: se usaban en configuraciones anteriores con túneles SSH/LocalTunnel.

        # Comando inicial según proveedor seleccionado
        cmd = _build_cmd()

        # Log del comando inicial
        try:
            logging.debug(f"[PublicTunnel] comando inicial={' '.join(cmd)}")
        except Exception:
            pass

        attempt = 0

        # Última URL pública detectada desde la salida del proveedor
        last_public_url: Optional[str] = None
        # Contador de fallas para proveedores SSH
        ssh_failures = 0

        def _run():
            # Variables capturadas desde el ámbito superior (closure)
            ssh_port = ssh_port
            attempt = attempt
            provider = provider
            last_public_url = last_public_url
            ssh_failures = ssh_failures
            # Supervisor con backoff exponencial para reconectar si el proceso termina
            backoff = 2.0
            max_backoff = 300.0  # 5 minutos máximo
            # Estado para notificaciones de reconexión con anti-spam
            first_start = True
            last_notify_ts = 0.0
            min_notify_interval = 90.0  # segundos
            while True:
                try:
                    attempt += 1
                    had_banner_refused = False
                    # Reconstruir comando por intento para reflejar cambios de puerto o proveedor
                    cmd = _build_cmd()
                    try:
                        logging.info(f"[PublicTunnel] intento={attempt} lanzando proceso provider={provider} puerto={ssh_port}")
                        logging.debug(f"[PublicTunnel] comando={' '.join(cmd)}")
                    except Exception:
                        pass
                    p = subprocess.Popen(
                        cmd,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,
                        text=True,
                        # En Windows, solo CREATE_NO_WINDOW para evitar consola; quitar flags que pueden provocar ventanas.
                        creationflags=(
                            getattr(subprocess, "CREATE_NO_WINDOW", 0)
                        ),
                        startupinfo=_startupinfo,
                        stdin=subprocess.DEVNULL,
                    )
                    try:
                        logging.info(f"[PublicTunnel] proceso lanzado pid={getattr(p, 'pid', None)}")
                    except Exception:
                        pass
                    # Si el primer arranque falla de inmediato y el puerto era 22, probar 443 en siguiente intento
                    try:
                        start_ts = time.time()
                    except Exception:
                        start_ts = 0.0
                    # Notificar reconexión (si no es el primer arranque) con anti-spam
                    cb = on_reconnect or _public_tunnel_on_reconnect_cb
                    if cb and not first_start:
                        now = time.time()
                        if (now - last_notify_ts) >= min_notify_interval:
                            last_notify_ts = now
                            try:
                                cb(f"https://{subdomain}.loca.lt/")
                                logging.info("[PublicTunnel] notificación de reconexión enviada")
                            except Exception:
                                pass
                    # Parsear salida del proceso para capturar URL pública (LocalTunnel)
                    if p.stdout:
                        refused_pattern = re.compile(r"banner exchange:.*Connection refused", re.IGNORECASE)
                        url_pattern = re.compile(r"https://[^\s]+", re.IGNORECASE)
                        try:
                            for line in p.stdout:
                                try:
                                    ln = (line or "").strip()
                                    if ln:
                                        logging.info(f"[PublicTunnel][stdout] {ln}")
                                        if refused_pattern.search(ln):
                                            had_banner_refused = True
                                        # Intentar capturar la URL pública impresa por el proveedor
                                        murl = url_pattern.search(ln)
                                        if murl:
                                            last_public_url = murl.group(0)
                                except Exception:
                                    pass
                        except Exception:
                            # Continuar aunque el parsing de salida falle
                            try:
                                logging.warning("[PublicTunnel] fallo leyendo stdout del proceso (continuando)")
                            except Exception:
                                pass
                            pass
                    # Esperar a que el proceso termine (por desconexión o error)
                    try:
                        p.wait()
                        try:
                            logging.info(f"[PublicTunnel] proceso terminado rc={getattr(p, 'returncode', None)}")
                        except Exception:
                            pass
                    except Exception:
                        pass
                except Exception:
                    # Fallo al lanzar SSH, continuar con backoff
                    try:
                        logging.error("[PublicTunnel] excepción al lanzar proceso", exc_info=True)
                    except Exception:
                        pass
                    pass
                # Marcar que siguientes lanzamientos serán reconexiones
                try:
                    first_start = False
                except Exception:
                    pass
                # Si el proceso terminó muy rápido y estábamos en 22, probar 443 como siguiente intento (solo serveo)
                # Sin alternancia de puertos ni fallback: centrado en LocalTunnel
                try:
                    pass
                except Exception:
                    pass
                # Dormir y aumentar backoff antes de reintentar
                try:
                    time.sleep(backoff)
                except Exception:
                    pass
                backoff = min(backoff * 2.0, max_backoff)

        threading.Thread(target=_run, daemon=True).start()
        # Retornar última URL pública detectada si existe; de lo contrario, URL por defecto según proveedor
        try:
            if last_public_url:
                return last_public_url
            if provider == "localtunnel":
                return f"https://{subdomain}.loca.lt/"
            else:
                return None
        except Exception:
            try:
                if provider == "localtunnel":
                    return f"https://{subdomain}.loca.lt/"
                else:
                    return None
            except Exception:
                return None
    except Exception:
        return None


@app.get("/")
async def root_selector(request: Request):
    """Página de selección: Dashboard (lleva a login) o Check-in."""
    theme_vars = read_theme_vars(static_dir / "style.css")
    ctx = {
        "request": request,
        "theme": theme_vars,
        "gym_name": get_gym_name("Gimnasio"),
        "logo_url": _resolve_logo_url(),
    }
    return templates.TemplateResponse("index.html", ctx)



@app.get("/login")
async def login_page_get(request: Request):
    """Muestra el formulario de login del dueño."""
    theme_vars = read_theme_vars(static_dir / "style.css")
    ctx = {
        "request": request,
        "theme": theme_vars,
        "error": request.query_params.get("error"),
        "gym_name": get_gym_name("Gimnasio"),
        "logo_url": _resolve_logo_url(),
    }
    return templates.TemplateResponse("login.html", ctx)


# --- Helpers de autenticación de usuario (DNI) ---
def _get_usuario_id_by_dni(dni: str) -> Optional[int]:
    try:
        d = str(dni or "").strip()
    except Exception:
        d = str(dni)
    if not d:
        return None
    db = _get_db()
    if db is None:
        return None
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)  # type: ignore
            cur.execute("SELECT id FROM usuarios WHERE dni = %s LIMIT 1", (d,))
            row = cur.fetchone()
            if row and ("id" in row):
                try:
                    val = int(row["id"] or 0)
                    return val if val > 0 else None
                except Exception:
                    pass
    except Exception:
        try:
            logging.exception("Error buscando usuario por DNI")
        except Exception:
            pass
    return None

def _issue_usuario_jwt(usuario_id: int) -> Optional[str]:
    try:
        hdr = {"alg": "HS256", "typ": "JWT"}
        now = int(time.time())
        pl = {"sub": int(usuario_id), "role": "usuario", "iat": now, "exp": now + 86400}
        hdr_b = json.dumps(hdr, separators=(",", ":")).encode("utf-8")
        pl_b = json.dumps(pl, separators=(",", ":")).encode("utf-8")
        h_b64 = base64.urlsafe_b64encode(hdr_b).rstrip(b"=").decode("utf-8")
        p_b64 = base64.urlsafe_b64encode(pl_b).rstrip(b"=").decode("utf-8")
        signing_input = f"{h_b64}.{p_b64}".encode("utf-8")
        secret = _get_session_secret().encode("utf-8")
        sig = hmac.new(secret, signing_input, hashlib.sha256).digest()
        s_b64 = base64.urlsafe_b64encode(sig).rstrip(b"=").decode("utf-8")
        return f"{h_b64}.{p_b64}.{s_b64}"
    except Exception:
        return None


@app.get("/usuario/login")
async def usuario_login_get(request: Request):
    """Formulario de login para usuarios (DNI + PIN)."""
    theme_vars = read_theme_vars(static_dir / "style.css")
    ctx = {
        "request": request,
        "theme": theme_vars,
        "error": request.query_params.get("error"),
        "gym_name": get_gym_name("Gimnasio"),
        "logo_url": _resolve_logo_url(),
    }
    return templates.TemplateResponse("usuario_login.html", ctx)


@app.post("/usuario/login")
async def usuario_login_post(request: Request):
    content_type = request.headers.get("content-type", "")
    if content_type.startswith("application/x-www-form-urlencoded"):
        data = await request.form()
    elif content_type.startswith("application/json"):
        data = await request.json()
    else:
        data = {}

    dni = str(data.get("dni", "")).strip()
    pin = str(data.get("pin", "")).strip()
    if not dni or not pin:
        return RedirectResponse(url="/usuario/login?error=Ingrese%20DNI%20y%20PIN", status_code=303)

    ip = _get_client_ip(request)
    ip_key = f"ip:{ip}"
    dni_key = f"dni:{dni}"
    # Limites: IP 10/5min, DNI 5/5min
    if _is_rate_limited(ip_key, _login_attempts_by_ip, max_attempts=10, window_s=300) or _is_rate_limited(dni_key, _login_attempts_by_dni, max_attempts=5, window_s=300):
        return RedirectResponse(url="/usuario/login?error=Demasiados%20intentos.%20Intente%20m%C3%A1s%20tarde", status_code=303)

    _register_attempt(ip_key, _login_attempts_by_ip)
    _register_attempt(dni_key, _login_attempts_by_dni)

    db = _get_db()
    if db is None:
        return RedirectResponse(url="/usuario/login?error=Base%20de%20datos%20no%20disponible", status_code=303)

    usuario_id = _get_usuario_id_by_dni(dni)
    if not usuario_id:
        return RedirectResponse(url="/usuario/login?error=DNI%20no%20encontrado", status_code=303)

    ok = False
    try:
        ok = bool(db.verificar_pin_usuario(int(usuario_id), pin))  # type: ignore
    except Exception:
        ok = False
    if not ok:
        return RedirectResponse(url="/usuario/login?error=PIN%20inv%C3%A1lido", status_code=303)

    # Éxito: limpiar intentos y establecer sesión
    _clear_attempts(ip_key, _login_attempts_by_ip)
    _clear_attempts(dni_key, _login_attempts_by_dni)

    try:
        request.session.clear()
    except Exception:
        pass
    request.session["usuario_id"] = int(usuario_id)
    request.session["role"] = "usuario"

    try:
        u = db.obtener_usuario_por_id(int(usuario_id))  # type: ignore
        if u is not None:
            nombre = getattr(u, 'nombre', None) or (u.get('nombre') if isinstance(u, dict) else None) or ""
            request.session["usuario_nombre"] = nombre
    except Exception:
        pass

    try:
        tok = _issue_usuario_jwt(int(usuario_id))
        if tok:
            request.session["usuario_jwt"] = tok
    except Exception:
        pass

    return RedirectResponse(url="/usuario/panel", status_code=303)


@app.get("/usuario/panel")
async def usuario_panel_get(request: Request, _=Depends(require_usuario)):
    theme_vars = read_theme_vars(static_dir / "style.css")
    db = _get_db()
    usuario_id = request.session.get("usuario_id")
    usuario = None
    rutinas = []
    try:
        if db is not None and usuario_id:
            usuario = db.obtener_usuario_por_id(int(usuario_id))  # type: ignore
            rutinas = db.obtener_rutinas_por_usuario(int(usuario_id))  # type: ignore
    except Exception:
        rutinas = []

    rutinas_data = []
    web_base = None
    try:
        web_base = get_webapp_base_url()
    except Exception:
        web_base = None
    try:
        for r in (rutinas or []):
            nombre = (
                getattr(r, 'nombre_rutina', None)
                or getattr(r, 'nombre', None)
                or (r.get('nombre_rutina') if isinstance(r, dict) else None)
                or (r.get('nombre') if isinstance(r, dict) else None)
                or "Rutina"
            )
            dias = getattr(r, 'dias_semana', None) or (r.get('dias_semana') if isinstance(r, dict) else None) or []
            activa = getattr(r, 'activa', None) or (r.get('activa') if isinstance(r, dict) else None)
            ejercicios = getattr(r, 'ejercicios', None) or (r.get('ejercicios') if isinstance(r, dict) else None)
            uuid_rutina = getattr(r, 'uuid_rutina', None) or (r.get('uuid_rutina') if isinstance(r, dict) else None)
            por_dia: Dict[int, int] = {}
            if ejercicios:
                try:
                    grupos = _build_exercises_by_day(r)
                    for d, items in grupos.items():
                        por_dia[int(d)] = len(items or [])
                except Exception:
                    por_dia = {}
            rutinas_data.append({
                "nombre": nombre,
                "dias": dias,
                "activa": bool(activa),
                "ejercicios_por_dia": por_dia,
                "uuid_rutina": uuid_rutina,
                "qr_url": (f"{web_base}/api/rutinas/qr_scan/{uuid_rutina}" if web_base and uuid_rutina else None)
            })
    except Exception:
        rutinas_data = []

    # Datos de vencimiento y estado
    proximo_vencimiento = None
    dias_restantes = None
    cuotas_vencidas = None
    ultimo_pago = None
    activo_usuario = None
    try:
        if usuario is not None:
            # obtener campos según sea dataclass o dict
            def _get(u, key):
                try:
                    return getattr(u, key)
                except Exception:
                    try:
                        return u.get(key)  # type: ignore
                    except Exception:
                        return None

            fv_str = _get(usuario, 'fecha_proximo_vencimiento')
            cuotas_vencidas = _get(usuario, 'cuotas_vencidas')
            ultimo_pago = _get(usuario, 'ultimo_pago')
            activo_usuario = bool(_get(usuario, 'activo') if _get(usuario, 'activo') is not None else True)
            if fv_str:
                from datetime import datetime
                fv = None
                s = str(fv_str)
                # Intentar varios formatos comunes
                for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%d/%m/%Y"):
                    try:
                        fv = datetime.strptime(s.split('.')[0], fmt)
                        break
                    except Exception:
                        continue
                if fv:
                    now = datetime.now()
                    try:
                        dias_restantes = (fv.date() - now.date()).days
                    except Exception:
                        dias_restantes = None
                    try:
                        proximo_vencimiento = fv.strftime("%d/%m/%Y")
                    except Exception:
                        proximo_vencimiento = s
    except Exception:
        pass

    # Historial de pagos (últimos 12)
    pagos_list = []
    try:
        if db is not None and usuario_id:
            pm = PaymentManager(db)
            pagos_hist = pm.obtener_historial_pagos(int(usuario_id), limit=12)
            for p in (pagos_hist or []):
                try:
                    pagos_list.append({
                        "fecha": getattr(p, 'fecha_pago', None),
                        "monto": float(getattr(p, 'monto', 0.0)),
                        "mes": getattr(p, 'mes', None),
                        "anio": getattr(p, 'año', None),
                        "metodo_pago_id": getattr(p, 'metodo_pago_id', None),
                    })
                except Exception:
                    continue
    except Exception:
        pagos_list = []

    # Seleccionar QR de rutina activa para banda de impresión
    rutina_activa_qr_url = None
    try:
        for r in rutinas_data:
            if r.get("activa") and r.get("qr_url"):
                rutina_activa_qr_url = r.get("qr_url")
                break
    except Exception:
        rutina_activa_qr_url = None

    ctx = {
        "request": request,
        "theme": theme_vars,
        "gym_name": get_gym_name("Gimnasio"),
        "logo_url": _resolve_logo_url(),
        "usuario": usuario,
        "rutinas": rutinas_data,
        "jwt": request.session.get("usuario_jwt"),
        # Estado y pagos
        "proximo_vencimiento": proximo_vencimiento,
        "dias_restantes": dias_restantes,
        "cuotas_vencidas": cuotas_vencidas,
        "ultimo_pago": ultimo_pago,
        "activo_usuario": activo_usuario,
        "pagos": pagos_list,
        # QR para impresión
        "rutina_activa_qr_url": rutina_activa_qr_url,
    }
    return templates.TemplateResponse("usuario_panel.html", ctx)


@app.get("/usuario/logout")
async def usuario_logout_get(request: Request):
    try:
        request.session.pop("usuario_id", None)
        request.session.pop("usuario_nombre", None)
        request.session.pop("usuario_jwt", None)
    except Exception:
        pass
    return RedirectResponse(url="/usuario/login", status_code=303)


@app.post("/login")
async def do_login(request: Request):
    content_type = request.headers.get("content-type", "")
    if content_type.startswith("application/x-www-form-urlencoded"):
        data = await request.form()
    elif content_type.startswith("application/json"):
        data = await request.json()
    else:
        data = {}
    password = str(data.get("password", "")).strip()
    if not password:
        return RedirectResponse(url="/login?error=Ingrese%20la%20contrase%C3%B1a", status_code=303)
    ok = False
    # Verificar contraseña usando bcrypt con fallback a texto plano
    if _verify_owner_password(password):
        ok = True
    # Nota: Se ha eliminado el uso del PIN del dueño para autenticación web
    if ok:
        request.session["logged_in"] = True
        request.session["role"] = "dueño"
        return RedirectResponse(url="/dashboard", status_code=303)
    return RedirectResponse(url="/login?error=Credenciales%20inv%C3%A1lidas", status_code=303)


@app.post("/logout")
async def do_logout(request: Request, _=Depends(require_owner)):
    request.session.clear()
    return RedirectResponse(url="/gestion/login", status_code=303)

@app.get("/logout")
async def logout_get(request: Request):
    request.session.clear()
    return RedirectResponse(url="/gestion/login", status_code=303)

# Rutas de logout específicas por sección
@app.post("/dashboard/logout")
async def dashboard_logout_post(request: Request, _=Depends(require_owner)):
    request.session.clear()
    return RedirectResponse(url="/login", status_code=303)

@app.get("/dashboard/logout")
async def dashboard_logout_get(request: Request):
    request.session.clear()
    return RedirectResponse(url="/login", status_code=303)

@app.get("/gestion/logout")
async def gestion_logout_get(request: Request):
    # Finalizar automáticamente sesión de trabajo del profesor si existe
    try:
        db = _get_db()
        pid = request.session.get("gestion_profesor_id")
        if db is not None and pid is not None:
            try:
                pid_int = int(pid)
                db.finalizar_sesion_trabajo_profesor(pid_int)  # type: ignore
            except Exception:
                pass
    except Exception:
        pass
    request.session.clear()
    return RedirectResponse(url="/gestion/login", status_code=303)

@app.get("/checkin/logout")
async def checkin_logout_get(request: Request):
    # Solo limpiar sesión de checkin, si existe
    try:
        request.session.pop("checkin_user_id", None)
    except Exception:
        pass
    # Limpiar todo por seguridad
    request.session.clear()
    return RedirectResponse(url="/checkin", status_code=303)

# --- Endpoint admin para actualizar la contraseña del dueño ---
@app.post("/api/admin/owner-password")
async def set_owner_password(request: Request):
    """
    Actualiza la contraseña del dueño en la base de datos.
    Acceso restringido exclusivamente mediante DEV_PASSWORD.
    """
    try:
        content_type = request.headers.get("content-type", "")
        if content_type.startswith("application/json"):
            data = await request.json()
        else:
            # Aceptar x-www-form-urlencoded como fallback
            data = await request.form()
    except Exception:
        data = {}

    dev_pwd = str(data.get("dev_password", "")).strip()
    new_pwd = str(data.get("new_password", "")).strip()

    if not dev_pwd or not new_pwd:
        return JSONResponse({"success": False, "message": "Parámetros inválidos"}, status_code=400)
    if len(new_pwd) < 4:
        return JSONResponse({"success": False, "message": "La nueva contraseña debe tener al menos 4 caracteres"}, status_code=400)

    # Validar fortaleza de la contraseña
    try:
        from security_utils import SecurityUtils
        is_strong, message = SecurityUtils.validate_password_strength(new_pwd)
        if not is_strong:
            return JSONResponse({"success": False, "message": f"Contraseña débil: {message}"}, status_code=400)
    except Exception:
        # Si falla la validación, continuar sin ella
        pass

    # Resolver DEV_PASSWORD real
    real_dev = None
    try:
        if DEV_PASSWORD:
            real_dev = str(DEV_PASSWORD).strip()
    except Exception:
        real_dev = None
    if not real_dev:
        try:
            from managers import DeveloperManager  # type: ignore
            real_dev = str(getattr(DeveloperManager, "DEV_PASSWORD", "") or "").strip()
        except Exception:
            real_dev = None
    if not real_dev:
        # Último intento: variable de entorno
        real_dev = os.getenv("DEV_PASSWORD", "").strip()

    if not real_dev or dev_pwd != real_dev:
        return JSONResponse({"success": False, "message": "No autorizado"}, status_code=401)

    db = _get_db()
    if not db:
        return JSONResponse({"success": False, "message": "DatabaseManager no disponible"}, status_code=500)

    try:
        ok = False
        
        # Hashear la nueva contraseña antes de almacenarla
        try:
            from security_utils import SecurityUtils
            hashed_pwd = SecurityUtils.hash_password(new_pwd)
        except Exception:
            # Fallback: guardar sin hashear si hay error
            hashed_pwd = new_pwd
        
        if hasattr(db, 'actualizar_configuracion'):
            ok = bool(db.actualizar_configuracion('owner_password', hashed_pwd))  # type: ignore
        else:
            # Fallback SQL directo si el método no existe
            with db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor()
                # Crear tabla/config si fuera necesario (idempotente)
                try:
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS configuracion (
                            clave TEXT PRIMARY KEY,
                            valor TEXT
                        )
                    """)
                except Exception:
                    pass
                cur.execute(
                    """
                    INSERT INTO configuracion (clave, valor)
                    VALUES ('owner_password', %s)
                    ON CONFLICT (clave) DO UPDATE SET valor = EXCLUDED.valor
                    """,
                    (hashed_pwd,),
                )
                conn.commit()
                ok = True
        if ok:
            # Refrescar caché si existe
            try:
                if hasattr(db, 'prefetch_owner_credentials_async'):
                    db.prefetch_owner_credentials_async(ttl_seconds=0)  # type: ignore
            except Exception:
                pass
            return JSONResponse({"success": True, "message": "Contraseña actualizada"})
        return JSONResponse({"success": False, "message": "No se pudo actualizar la contraseña"}, status_code=500)
    except Exception as e:
        try:
            logging.exception("Error en /api/admin/owner-password")
        except Exception:
            pass
        return JSONResponse({"success": False, "message": str(e)}, status_code=500)


@app.post("/api/admin/renumerar_usuarios")
async def api_admin_renumerar_usuarios(request: Request, _=Depends(require_owner)):
    """
    Renumera de forma segura los IDs de usuarios empezando desde "start_id".
    Actualiza referencias en todas las tablas relacionadas.
    Acceso: dueño (sesión web de Gestión).
    """
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, "/api/admin/renumerar_usuarios")
    if guard:
        return guard
    # Leer payload
    try:
        content_type = (request.headers.get("content-type", "") or "").strip()
        if content_type.startswith("application/json"):
            payload = await request.json()
        else:
            try:
                payload = await request.form()
            except Exception:
                payload = {}
    except Exception:
        payload = {}
    # Validar parámetros
    try:
        start_id_raw = payload.get("start_id", 1)
        try:
            start_id = int(start_id_raw)
        except Exception:
            start_id = 1
        if start_id < 1:
            return JSONResponse({"success": False, "message": "start_id inválido"}, status_code=400)
        res = db.renumerar_usuario_ids(start_id)  # type: ignore
        if isinstance(res, dict):
            return JSONResponse(res, status_code=200)
        return JSONResponse({"success": True, "result": res}, status_code=200)
    except HTTPException:
        raise
    except Exception as e:
        try:
            logging.exception("Error en /api/admin/renumerar_usuarios")
        except Exception:
            pass
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)


@app.post("/api/admin/secure_owner")
async def api_admin_secure_owner(request: Request, _=Depends(require_owner)):
    """
    Asegura la existencia del usuario Dueño y su protección:
    - Crea el usuario con rol 'dueño' si no existe
    - Fuerza que el Dueño tenga ID=1 (migrando referencias del ocupante previo)
    - Restaura políticas RLS y triggers defensivos
    - Sembrado inicial de 'owner_password' en configuracion si falta
    Acceso: dueño (sesión web de Gestión).
    """
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, "/api/admin/secure_owner")
    if guard:
        return guard
    try:
        import os
        changes = []
        owner_id = None
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            # Desactivar triggers y RLS temporalmente
            try:
                cur.execute("SET LOCAL session_replication_role = 'replica'")
            except Exception:
                pass
            try:
                cur.execute("ALTER TABLE usuarios NO FORCE ROW LEVEL SECURITY")
            except Exception:
                pass
            try:
                cur.execute("ALTER TABLE usuarios DISABLE ROW LEVEL SECURITY")
            except Exception:
                pass
            # Asegurar existencia del Dueño
            cur.execute("SELECT id FROM usuarios WHERE rol = 'dueño' LIMIT 1")
            row = cur.fetchone()
            if row is None:
                # Preferir ID=1 si está libre
                cur.execute("SELECT 1 FROM usuarios WHERE id = 1")
                id1_exists = cur.fetchone() is not None
                target_id = 1
                if id1_exists:
                    cur.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM usuarios")
                    target_id = int((cur.fetchone() or [1])[0] or 1)
                # Establecer PIN plano para el Dueño
                _owner_pin = "2203"
                cur.execute(
                    """
                    INSERT INTO usuarios (id, nombre, dni, telefono, pin, rol, activo, tipo_cuota)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (target_id, "DUEÑO DEL GIMNASIO", "00000000", "N/A", _owner_pin, "dueño", True, "estandar"),
                )
                owner_id = target_id
                changes.append({"created_owner_id": target_id})
            else:
                try:
                    owner_id = int(row[0])
                except Exception:
                    owner_id = row[0]
            # Forzar ID=1 para el Dueño
            if owner_id != 1:
                cur.execute("SELECT 1 FROM usuarios WHERE id = 1")
                id1_exists = cur.fetchone() is not None
                if id1_exists:
                    # Migrar ocupante del ID=1 a un ID libre y actualizar referencias
                    cur.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM usuarios")
                    new_id_for_old1 = int((cur.fetchone() or [2])[0] or 2)
                    updates = [
                        ("UPDATE pagos SET usuario_id = %s WHERE usuario_id = %s", (new_id_for_old1, 1)),
                        ("UPDATE asistencias SET usuario_id = %s WHERE usuario_id = %s", (new_id_for_old1, 1)),
                        ("UPDATE rutinas SET usuario_id = %s WHERE usuario_id = %s", (new_id_for_old1, 1)),
                        ("UPDATE clase_usuarios SET usuario_id = %s WHERE usuario_id = %s", (new_id_for_old1, 1)),
                        ("UPDATE clase_lista_espera SET usuario_id = %s WHERE usuario_id = %s", (new_id_for_old1, 1)),
                        ("UPDATE usuario_notas SET usuario_id = %s WHERE usuario_id = %s", (new_id_for_old1, 1)),
                        ("UPDATE usuario_etiquetas SET usuario_id = %s WHERE usuario_id = %s", (new_id_for_old1, 1)),
                        ("UPDATE usuario_estados SET usuario_id = %s WHERE usuario_id = %s", (new_id_for_old1, 1)),
                        ("UPDATE profesores SET usuario_id = %s WHERE usuario_id = %s", (new_id_for_old1, 1)),
                        ("UPDATE notificaciones_cupos SET usuario_id = %s WHERE usuario_id = %s", (new_id_for_old1, 1)),
                        ("UPDATE audit_logs SET user_id = %s WHERE user_id = %s", (new_id_for_old1, 1)),
                        ("UPDATE checkin_pending SET usuario_id = %s WHERE usuario_id = %s", (new_id_for_old1, 1)),
                        ("UPDATE whatsapp_messages SET user_id = %s WHERE user_id = %s", (new_id_for_old1, 1)),
                        ("UPDATE usuario_notas SET autor_id = %s WHERE autor_id = %s", (new_id_for_old1, 1)),
                        ("UPDATE usuario_etiquetas SET asignado_por = %s WHERE asignado_por = %s", (new_id_for_old1, 1)),
                        ("UPDATE usuario_estados SET creado_por = %s WHERE creado_por = %s", (new_id_for_old1, 1)),
                    ]
                    for sql, params in updates:
                        try:
                            cur.execute(sql, params)
                        except Exception:
                            pass
                    try:
                        cur.execute(
                            "UPDATE acciones_masivas_pendientes SET usuario_ids = array_replace(usuario_ids, %s, %s) WHERE %s = ANY(usuario_ids)",
                            (1, new_id_for_old1, 1),
                        )
                    except Exception:
                        pass
                    cur.execute("UPDATE usuarios SET id = %s WHERE id = %s AND rol <> 'dueño'", (new_id_for_old1, 1))
                    changes.append({"moved_user": {"from": 1, "to": new_id_for_old1}})
                cur.execute("UPDATE usuarios SET id = 1 WHERE id = %s AND rol = 'dueño'", (owner_id,))
                changes.append({"owner_id_changed": {"from": owner_id, "to": 1}})
                owner_id = 1
            # Sembrar/asegurar owner_password en configuracion
            try:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS configuracion (
                        clave TEXT PRIMARY KEY,
                        valor TEXT
                    )
                    """
                )
                # Obtener contraseña del owner desde variables de entorno
                from secure_config import config as secure_config
                try:
                    env_pwd = secure_config.get_owner_password()
                except ValueError:
                    # Fallback a variables de entorno anteriores
                    env_pwd = os.getenv("WEBAPP_OWNER_PASSWORD", "").strip() or os.getenv("OWNER_PASSWORD", "").strip()
                    if not env_pwd:
                        logger.error("No se encontró contraseña de owner en variables de entorno")
                        raise ValueError("Contraseña de owner no configurada")
                
                # Hash de la contraseña antes de almacenarla
                try:
                    from security_utils import SecurityUtils
                    hashed_pwd = SecurityUtils.hash_password(env_pwd)
                except Exception as e:
                    logger.warning(f"Error al hashear contraseña: {e}, usando contraseña en texto plano")
                    hashed_pwd = env_pwd
                
                cur.execute(
                    """
                    INSERT INTO configuracion (clave, valor)
                    VALUES ('owner_password', %s)
                    ON CONFLICT (clave) DO NOTHING
                    """,
                    (hashed_pwd,),
                )
            except Exception:
                pass
            # Restaurar RLS y triggers defensivos
            try:
                cur.execute(
                    """
                    ALTER TABLE usuarios ENABLE ROW LEVEL SECURITY;
                    ALTER TABLE usuarios FORCE ROW LEVEL SECURITY;

                    DROP POLICY IF EXISTS usuarios_block_owner_select ON usuarios;
                    DROP POLICY IF EXISTS usuarios_block_owner_update ON usuarios;
                    DROP POLICY IF EXISTS usuarios_block_owner_delete ON usuarios;
                    DROP POLICY IF EXISTS usuarios_block_owner_insert ON usuarios;

                    CREATE POLICY usuarios_block_owner_select ON usuarios
                        FOR SELECT
                        USING (rol IS DISTINCT FROM 'dueño');

                    CREATE POLICY usuarios_block_owner_update ON usuarios
                        FOR UPDATE
                        USING (rol IS DISTINCT FROM 'dueño')
                        WITH CHECK (rol IS DISTINCT FROM 'dueño');

                    CREATE POLICY usuarios_block_owner_delete ON usuarios
                        FOR DELETE
                        USING (rol IS DISTINCT FROM 'dueño');

                    CREATE POLICY usuarios_block_owner_insert ON usuarios
                        FOR INSERT
                        WITH CHECK (rol IS DISTINCT FROM 'dueño');

                    DROP TRIGGER IF EXISTS trg_usuarios_bloquear_ins_upd_dueno ON usuarios;
                    DROP TRIGGER IF EXISTS trg_usuarios_bloquear_del_dueno ON usuarios;
                    DROP FUNCTION IF EXISTS usuarios_bloquear_dueno_ins_upd();
                    DROP FUNCTION IF EXISTS usuarios_bloquear_dueno_delete();

                    CREATE FUNCTION usuarios_bloquear_dueno_ins_upd() RETURNS trigger AS $$
                    BEGIN
                        IF NEW.rol = 'dueño' THEN
                            RAISE EXCEPTION 'Operación no permitida: los usuarios con rol "dueño" son inafectables';
                        END IF;
                        RETURN NEW;
                    END;
                    $$ LANGUAGE plpgsql;

                    CREATE FUNCTION usuarios_bloquear_dueno_delete() RETURNS trigger AS $$
                    BEGIN
                        IF OLD.rol = 'dueño' THEN
                            RAISE EXCEPTION 'Operación no permitida: los usuarios con rol "dueño" no pueden eliminarse';
                        END IF;
                        RETURN OLD;
                    END;
                    $$ LANGUAGE plpgsql;

                    CREATE TRIGGER trg_usuarios_bloquear_ins_upd_dueno
                    BEFORE INSERT OR UPDATE ON usuarios
                    FOR EACH ROW EXECUTE FUNCTION usuarios_bloquear_dueno_ins_upd();

                    CREATE TRIGGER trg_usuarios_bloquear_del_dueno
                    BEFORE DELETE ON usuarios
                    FOR EACH ROW EXECUTE FUNCTION usuarios_bloquear_dueno_delete();
                    """
                )
            except Exception:
                pass
            try:
                cur.execute("SET LOCAL session_replication_role = 'origin'")
            except Exception:
                pass
            conn.commit()
        # Refrescar cachés si existen
        try:
            if hasattr(db, 'prefetch_owner_credentials_async'):
                db.prefetch_owner_credentials_async(ttl_seconds=0)  # type: ignore
        except Exception:
            pass
        return JSONResponse({"success": True, "owner_id": owner_id or 1, "changes": changes}, status_code=200)
    except HTTPException:
        raise
    except Exception as e:
        try:
            logging.exception("Error en /api/admin/secure_owner")
        except Exception:
            pass
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)


@app.get("/dashboard")
async def dashboard(request: Request):
    # Redirigir a login si no hay sesión activa
    if not request.session.get("logged_in"):
        return RedirectResponse(url="/", status_code=303)
    theme_vars = read_theme_vars(static_dir / "style.css")
    ctx = {
        "request": request,
        "theme": theme_vars,
        "gym_name": get_gym_name("Gimnasio"),
        "logo_url": _resolve_logo_url(),
    }
    return templates.TemplateResponse("dashboard.html", ctx)

@app.get("/gestion")
async def gestion(request: Request):
    # Redirigir a login de Gestión si no hay sesión activa (dueño o profesor)
    if not (request.session.get("logged_in") or request.session.get("gestion_profesor_id") or request.session.get("gestion_profesor_user_id")):
        return RedirectResponse(url="/gestion/login", status_code=303)
    theme_vars = read_theme_vars(static_dir / "style.css")
    ctx = {
        "request": request,
        "theme": theme_vars,
        "gym_name": get_gym_name("Gimnasio"),
        "logo_url": _resolve_logo_url(),
    }
    return templates.TemplateResponse("gestion.html", ctx)

# --- API Usuarios (CRUD) ---
@app.get("/api/usuarios")
async def api_usuarios_list(q: Optional[str] = None, limit: int = 50, offset: int = 0, _=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        return []
    guard = _circuit_guard_json(db, "/api/usuarios")
    if guard:
        return guard
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            sql = """
                SELECT id, nombre, dni, telefono, rol, tipo_cuota, activo, fecha_registro
                FROM usuarios
                WHERE TRUE
            """
            params: list = []
            if q:
                q_like = f"%{q.strip()}%"
                sql += " AND (LOWER(nombre) LIKE LOWER(%s) OR CAST(dni AS TEXT) LIKE %s OR CAST(telefono AS TEXT) LIKE %s)"
                params.extend([q_like, q_like, q_like])
            sql += " ORDER BY nombre ASC LIMIT %s OFFSET %s"
            params.extend([limit, offset])
            cur.execute(sql, params)
            rows = cur.fetchall() or []
            for r in rows:
                r["nombre"] = (r.get("nombre") or "").strip()
                r["rol"] = (r.get("rol") or "").strip().lower()
            return rows
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/usuarios/{usuario_id}")
async def api_usuario_get(usuario_id: int, _=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, "/api/usuarios/{id}")
    if guard:
        return guard
    try:
        u = db.obtener_usuario_por_id(usuario_id)  # type: ignore
        if not u:
            raise HTTPException(status_code=404, detail="Usuario no encontrado")
        # Aplicar visibilidad de PIN: profesor no ve PIN de otro profesor
        pin_value = getattr(u, "pin", None)
        try:
            prof_uid = request.session.get("gestion_profesor_user_id")
            if prof_uid and str(getattr(u, "rol", "")).strip().lower() == "profesor" and int(usuario_id) != int(prof_uid):
                pin_value = None
        except Exception:
            pass
        return {
            "id": u.id,
            "nombre": u.nombre,
            "dni": u.dni,
            "telefono": u.telefono,
            "pin": pin_value,
            "rol": u.rol,
            "activo": bool(u.activo),
            "tipo_cuota": u.tipo_cuota,
            "notas": u.notas,
            "fecha_registro": u.fecha_registro,
            "fecha_proximo_vencimiento": u.fecha_proximo_vencimiento,
            "cuotas_vencidas": u.cuotas_vencidas,
            "ultimo_pago": u.ultimo_pago,
        }
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.post("/api/usuarios")
async def api_usuario_create(request: Request, _=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, "/api/usuarios")
    if guard:
        return guard
    payload = await request.json()
    try:
        nombre = ((payload.get("nombre") or "").strip()).upper()
        dni = str(payload.get("dni") or "").strip()
        telefono = str(payload.get("telefono") or "").strip() or None
        pin = str(payload.get("pin") or "").strip() or None
        # Aplicar regla: profesor no puede modificar PIN de otro profesor
        try:
            orig = db.obtener_usuario_por_id(usuario_id)  # type: ignore
            session_prof_uid = request.session.get("gestion_profesor_user_id")
            if session_prof_uid and orig and str(getattr(orig, "rol", "")).strip().lower() == "profesor" and int(usuario_id) != int(session_prof_uid):
                pin = getattr(orig, "pin", None)
        except Exception:
            pass
        rol = (payload.get("rol") or "socio").strip().lower()
        activo = bool(payload.get("activo", True))
        tipo_cuota = payload.get("tipo_cuota")
        notas = payload.get("notas")
        if not nombre or not dni:
            raise HTTPException(status_code=400, detail="'nombre' y 'dni' son obligatorios")
        if db.dni_existe(dni):  # type: ignore
            raise HTTPException(status_code=400, detail="DNI ya existe")
        usuario = Usuario(  # type: ignore
            id=None,
            nombre=nombre,
            dni=dni,
            telefono=telefono,
            pin=pin,
            rol=rol,
            notas=notas,
            fecha_registro=datetime.now(timezone.utc).isoformat(),
            activo=activo,
            tipo_cuota=tipo_cuota,
            fecha_proximo_vencimiento=None,
            cuotas_vencidas=0,
            ultimo_pago=None,
        )
        new_id = db.crear_usuario(usuario)  # type: ignore
        return {"id": new_id}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.put("/api/usuarios/{usuario_id}")
async def api_usuario_update(usuario_id: int, request: Request, _=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, "/api/usuarios/{id}")
    if guard:
        return guard
    payload = await request.json()
    try:
        if not db.usuario_id_existe(usuario_id):  # type: ignore
            raise HTTPException(status_code=404, detail="Usuario no encontrado")
        nombre = ((payload.get("nombre") or "").strip()).upper()
        dni = str(payload.get("dni") or "").strip()
        telefono = str(payload.get("telefono") or "").strip() or None
        pin = str(payload.get("pin") or "").strip() or None
        rol = (payload.get("rol") or "socio").strip().lower()
        activo = bool(payload.get("activo", True))
        tipo_cuota = payload.get("tipo_cuota")
        notas = payload.get("notas")
        if not nombre or not dni:
            raise HTTPException(status_code=400, detail="'nombre' y 'dni' son obligatorios")
        if db.dni_existe(dni, usuario_id):  # type: ignore
            raise HTTPException(status_code=400, detail="DNI ya existe")
        usuario = Usuario(  # type: ignore
            id=usuario_id,
            nombre=nombre,
            dni=dni,
            telefono=telefono,
            pin=pin,
            rol=rol,
            notas=notas,
            fecha_registro=None,
            activo=activo,
            tipo_cuota=tipo_cuota,
            fecha_proximo_vencimiento=None,
            cuotas_vencidas=None,
            ultimo_pago=None,
        )
        db.actualizar_usuario(usuario)  # type: ignore
        # Manejar cambio de ID si el dueño lo solicita
        try:
            new_id_raw = payload.get("new_id")
        except Exception:
            new_id_raw = None
        new_id_val = None
        try:
            new_id_val = int(new_id_raw) if new_id_raw is not None else None
        except Exception:
            new_id_val = None
        if new_id_val is not None and int(new_id_val) != int(usuario_id):
            is_owner = bool(request.session.get("logged_in")) and str(request.session.get("role") or "").strip().lower() == "dueño"
            if not is_owner:
                raise HTTPException(status_code=403, detail="Solo el dueño puede cambiar el ID de usuario")
            if int(new_id_val) <= 0:
                raise HTTPException(status_code=400, detail="El nuevo ID debe ser un entero positivo")
            try:
                db.cambiar_usuario_id(int(usuario_id), int(new_id_val))  # type: ignore
                usuario_id = int(new_id_val)
            except PermissionError as e:
                raise HTTPException(status_code=403, detail=str(e))
            except ValueError as e:
                raise HTTPException(status_code=400, detail=str(e))
            except Exception as e:
                # Responder genéricamente si ocurre otro error
                raise HTTPException(status_code=500, detail=str(e))
        return {"ok": True, "id": usuario_id}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.delete("/api/usuarios/{usuario_id}")
async def api_usuario_delete(usuario_id: int, _=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, "/api/usuarios/{id}")
    if guard:
        return guard
    try:
        db.eliminar_usuario(usuario_id)  # type: ignore
        return {"ok": True}
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

# --- API Etiquetas de usuario ---
@app.get("/api/usuarios/{usuario_id}/etiquetas")
async def api_usuario_etiquetas_get(usuario_id: int, _=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, "/api/usuarios/{id}/etiquetas")
    if guard:
        return guard
    try:
        if not db.usuario_id_existe(usuario_id):  # type: ignore
            raise HTTPException(status_code=404, detail="Usuario no encontrado")
        etiquetas = db.obtener_etiquetas_usuario(usuario_id)  # type: ignore
        items = []
        for e in etiquetas:
            try:
                items.append({
                    "id": getattr(e, "id", None),
                    "nombre": getattr(e, "nombre", None),
                    "color": getattr(e, "color", None),
                    "descripcion": getattr(e, "descripcion", None),
                    "activo": getattr(e, "activo", True),
                })
            except Exception:
                items.append(e)
        return {"items": items}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.post("/api/usuarios/{usuario_id}/etiquetas")
async def api_usuario_etiquetas_add(usuario_id: int, request: Request, _=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, "/api/usuarios/{id}/etiquetas")
    if guard:
        return guard
    payload = await request.json()
    try:
        if not db.usuario_id_existe(usuario_id):  # type: ignore
            raise HTTPException(status_code=404, detail="Usuario no encontrado")
        etiqueta_id = payload.get("etiqueta_id")
        nombre = (payload.get("nombre") or "").strip()
        asignado_por = None
        try:
            asignado_por = int(request.session.get("user_id")) if request.session.get("user_id") else None
        except Exception:
            asignado_por = None
        if etiqueta_id is None and not nombre:
            raise HTTPException(status_code=400, detail="Se requiere 'etiqueta_id' o 'nombre'")
        if etiqueta_id is None and nombre:
            try:
                et = db.obtener_o_crear_etiqueta(nombre)  # type: ignore
                etiqueta_id = getattr(et, "id", None)
            except Exception:
                etiqueta_id = None
        if etiqueta_id is None:
            raise HTTPException(status_code=400, detail="Etiqueta inválida")
        ok = db.asignar_etiqueta_usuario(usuario_id, int(etiqueta_id), asignado_por)  # type: ignore
        return {"ok": bool(ok)}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.delete("/api/usuarios/{usuario_id}/etiquetas/{etiqueta_id}")
async def api_usuario_etiquetas_remove(usuario_id: int, etiqueta_id: int, _=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, "/api/usuarios/{id}/etiquetas/{etiqueta_id}")
    if guard:
        return guard
    try:
        if not db.usuario_id_existe(usuario_id):  # type: ignore
            raise HTTPException(status_code=404, detail="Usuario no encontrado")
        ok = db.desasignar_etiqueta_usuario(usuario_id, etiqueta_id)  # type: ignore
        return {"ok": bool(ok)}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

# --- API Estados de usuario ---
@app.get("/api/usuarios/{usuario_id}/estados")
async def api_usuario_estados_get(usuario_id: int, _=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, "/api/usuarios/{id}/estados")
    if guard:
        return guard
    try:
        if not db.usuario_id_existe(usuario_id):  # type: ignore
            raise HTTPException(status_code=404, detail="Usuario no encontrado")
        estados = db.obtener_estados_usuario(usuario_id, solo_activos=True)  # type: ignore
        items = []
        for est in estados:
            try:
                items.append({
                    "id": getattr(est, "id", None),
                    "usuario_id": getattr(est, "usuario_id", None),
                    "estado": getattr(est, "estado", None),
                    "descripcion": getattr(est, "descripcion", None),
                    "fecha_inicio": getattr(est, "fecha_inicio", None),
                    "fecha_vencimiento": getattr(est, "fecha_vencimiento", None),
                    "activo": getattr(est, "activo", True),
                    "creado_por": getattr(est, "creado_por", None),
                })
            except Exception:
                items.append(est)
        return {"items": items}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/usuarios_morosidad_ids")
async def api_usuarios_morosidad_ids(_=Depends(require_gestion_access)):
    """Devuelve IDs de usuarios con estado activo 'desactivado_por_morosidad'."""
    db = _get_db()
    if db is None:
        return []
    guard = _circuit_guard_json(db, "/api/usuarios_morosidad_ids")
    if guard:
        return guard
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(
                """
                SELECT usuario_id
                FROM usuario_estados
                WHERE activo = TRUE
                  AND LOWER(estado) = 'desactivado_por_morosidad'
                """
            )
            rows = cur.fetchall() or []
            ids = []
            for r in rows:
                try:
                    uid = int(r.get("usuario_id"))
                    ids.append(uid)
                except Exception:
                    continue
            return ids
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.post("/api/usuarios/{usuario_id}/estados")
async def api_usuario_estados_add(usuario_id: int, request: Request, _=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, "/api/usuarios/{id}/estados")
    if guard:
        return guard
    payload = await request.json()
    try:
        if not db.usuario_id_existe(usuario_id):  # type: ignore
            raise HTTPException(status_code=404, detail="Usuario no encontrado")
        nombre = (payload.get("estado") or payload.get("nombre") or "").strip()
        descripcion = (payload.get("descripcion") or "").strip() or None
        fecha_vencimiento = payload.get("fecha_vencimiento") or payload.get("fecha_fin")
        creado_por = None
        try:
            creado_por = int(request.session.get("user_id")) if request.session.get("user_id") else None
        except Exception:
            creado_por = None
        if not nombre:
            raise HTTPException(status_code=400, detail="'estado' es obligatorio")
        try:
            from models import UsuarioEstado
            estado = UsuarioEstado(usuario_id=usuario_id, estado=nombre, descripcion=descripcion, fecha_vencimiento=fecha_vencimiento, creado_por=creado_por)  # type: ignore
        except Exception:
            estado = type("E", (), {"usuario_id": usuario_id, "estado": nombre, "descripcion": descripcion, "fecha_vencimiento": fecha_vencimiento, "creado_por": creado_por})()
        eid = db.crear_estado_usuario(estado)  # type: ignore
        return {"ok": True, "id": int(eid)}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.put("/api/usuarios/{usuario_id}/estados/{estado_id}")
async def api_usuario_estados_update(usuario_id: int, estado_id: int, request: Request, _=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, "/api/usuarios/{id}/estados/{estado_id}")
    if guard:
        return guard
    payload = await request.json()
    try:
        if not db.usuario_id_existe(usuario_id):  # type: ignore
            raise HTTPException(status_code=404, detail="Usuario no encontrado")
        nombre = (payload.get("estado") or payload.get("nombre") or "").strip()
        descripcion = (payload.get("descripcion") or "").strip() or None
        fecha_vencimiento = payload.get("fecha_vencimiento") or payload.get("fecha_fin")
        activo = bool(payload.get("activo", True))
        usuario_modificador = None
        try:
            usuario_modificador = int(request.session.get("user_id")) if request.session.get("user_id") else None
        except Exception:
            usuario_modificador = None
        try:
            from models import UsuarioEstado
            estado = UsuarioEstado(id=estado_id, usuario_id=usuario_id, estado=nombre, descripcion=descripcion, fecha_vencimiento=fecha_vencimiento, activo=activo)  # type: ignore
        except Exception:
            estado = type("E", (), {"id": estado_id, "usuario_id": usuario_id, "estado": nombre, "descripcion": descripcion, "fecha_vencimiento": fecha_vencimiento, "activo": activo})()
        ok = db.actualizar_estado_usuario(estado, usuario_modificador=usuario_modificador)  # type: ignore
        return {"ok": bool(ok)}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.delete("/api/usuarios/{usuario_id}/estados/{estado_id}")
async def api_usuario_estados_delete(usuario_id: int, estado_id: int, request: Request, _=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, "/api/usuarios/{id}/estados/{estado_id}")
    if guard:
        return guard
    try:
        if not db.usuario_id_existe(usuario_id):  # type: ignore
            raise HTTPException(status_code=404, detail="Usuario no encontrado")
        usuario_modificador = None
        try:
            usuario_modificador = int(request.session.get("user_id")) if request.session.get("user_id") else None
        except Exception:
            usuario_modificador = None
        ok = db.eliminar_estado_usuario(int(estado_id), usuario_modificador=usuario_modificador)  # type: ignore
        return {"ok": bool(ok)}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/estados/plantillas")
async def api_estados_plantillas(_=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        return {"items": []}
    guard = _circuit_guard_json(db, "/api/estados/plantillas")
    if guard:
        return guard
    try:
        items = db.obtener_plantillas_estados()  # type: ignore
        return {"items": items}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

# --- API Metadatos de pago ---
@app.get("/api/metodos_pago")
async def api_metodos_pago(_=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        return []
    guard = _circuit_guard_json(db, "/api/metodos_pago")
    if guard:
        return guard
    try:
        rows = db.obtener_metodos_pago(solo_activos=True)  # type: ignore
        return [
            {
                'id': r.get('id'),
                'nombre': r.get('nombre'),
                'activo': r.get('activo'),
                'color': r.get('color'),
                'comision': r.get('comision'),
                'icono': r.get('icono'),
            }
            for r in (rows or [])
        ]
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.post("/api/metodos_pago")
async def api_metodos_pago_create(request: Request, _=Depends(require_gestion_access)):
    pm = _get_pm()
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, "/api/metodos_pago[POST]")
    if guard:
        return guard
    if pm is None or MetodoPago is None:
        raise HTTPException(status_code=503, detail="PaymentManager o modelo MetodoPago no disponible")
    payload = await request.json()
    try:
        nombre = (payload.get("nombre") or "").strip()
        if not nombre:
            raise HTTPException(status_code=400, detail="'nombre' es obligatorio")
        icono = payload.get("icono")
        color = (payload.get("color") or "#3498db").strip() or "#3498db"
        comision_raw = payload.get("comision")
        comision = float(comision_raw) if comision_raw is not None else 0.0
        if comision < 0 or comision > 100:
            raise HTTPException(status_code=400, detail="'comision' debe estar entre 0 y 100")
        activo = bool(payload.get("activo", True))
        descripcion = payload.get("descripcion")
        metodo = MetodoPago(nombre=nombre, icono=icono, color=color, comision=comision, activo=activo, descripcion=descripcion)  # type: ignore
        new_id = pm.crear_metodo_pago(metodo)
        return {"ok": True, "id": int(new_id)}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

@app.put("/api/metodos_pago/{metodo_id}")
async def api_metodos_pago_update(metodo_id: int, request: Request, _=Depends(require_gestion_access)):
    pm = _get_pm()
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, f"/api/metodos_pago/{metodo_id}[PUT]")
    if guard:
        return guard
    if pm is None or MetodoPago is None:
        raise HTTPException(status_code=503, detail="PaymentManager o modelo MetodoPago no disponible")
    payload = await request.json()
    try:
        existing = pm.obtener_metodo_pago(int(metodo_id))
        if not existing:
            raise HTTPException(status_code=404, detail="Método de pago no encontrado")
        nombre = (payload.get("nombre") or existing.nombre or "").strip() or existing.nombre
        icono = payload.get("icono") if ("icono" in payload) else existing.icono
        color = (payload.get("color") or existing.color or "#3498db").strip() or existing.color
        comision = float(payload.get("comision")) if (payload.get("comision") is not None) else float(existing.comision or 0.0)
        if comision < 0 or comision > 100:
            raise HTTPException(status_code=400, detail="'comision' debe estar entre 0 y 100")
        activo = bool(payload.get("activo")) if ("activo" in payload) else bool(existing.activo)
        descripcion = payload.get("descripcion") if ("descripcion" in payload) else existing.descripcion
        metodo = MetodoPago(id=int(metodo_id), nombre=nombre, icono=icono, color=color, comision=comision, activo=activo, descripcion=descripcion)  # type: ignore
        updated = pm.actualizar_metodo_pago(metodo)
        if not updated:
            raise HTTPException(status_code=404, detail="No se pudo actualizar el método de pago")
        return {"ok": True, "id": int(metodo_id)}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

@app.delete("/api/metodos_pago/{metodo_id}")
async def api_metodos_pago_delete(metodo_id: int, _=Depends(require_gestion_access)):
    pm = _get_pm()
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, f"/api/metodos_pago/{metodo_id}[DELETE]")
    if guard:
        return guard
    pm_ready = (pm is not None)
    try:
        if pm_ready:
            deleted = pm.eliminar_metodo_pago(int(metodo_id))
            if not deleted:
                raise HTTPException(status_code=404, detail="No se pudo eliminar el método de pago")
            return {"ok": True}
        else:
            with db.get_connection_context() as conn:  # type: ignore
                ok = _apply_change_idempotent(
                    conn,
                    schema="public",
                    table="metodos_pago",
                    operation="DELETE",
                    key_column="id",
                    key_value=int(metodo_id),
                    where=[("id", int(metodo_id))],
                )
                if not ok:
                    raise HTTPException(status_code=404, detail="No se pudo eliminar el método de pago")
                conn.commit()
                return {"ok": True}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

# --- API Ejercicios ---
@app.get("/api/ejercicios")
async def api_ejercicios_get(filtro: str = "", objetivo: str = "", grupo_muscular: str = "", _=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        return []
    guard = _circuit_guard_json(db, "/api/ejercicios")
    if guard:
        return guard
    try:
        ejercicios = db.obtener_ejercicios(filtro=filtro or "", objetivo=objetivo or "", grupo_muscular=grupo_muscular or "")  # type: ignore
        return [
            {
                "id": int(e.id) if e.id is not None else None,
                "nombre": e.nombre,
                "grupo_muscular": e.grupo_muscular,
                "descripcion": e.descripcion,
                "objetivo": getattr(e, "objetivo", "general"),
                "video_url": getattr(e, "video_url", None),
                "video_mime": getattr(e, "video_mime", None),
            }
            for e in ejercicios
        ]
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.post("/api/ejercicios")
async def api_ejercicios_create(request: Request, _=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, "/api/ejercicios[POST]")
    if guard:
        return guard
    if Ejercicio is None:
        raise HTTPException(status_code=503, detail="Modelo Ejercicio no disponible")
    payload = await request.json()
    try:
        nombre = (payload.get("nombre") or "").strip()
        if not nombre:
            raise HTTPException(status_code=400, detail="'nombre' es obligatorio")
        grupo_muscular = payload.get("grupo_muscular")
        descripcion = payload.get("descripcion")
        objetivo = (payload.get("objetivo") or "general").strip() or "general"
        video_url = payload.get("video_url")
        video_mime = payload.get("video_mime")
        ejercicio = Ejercicio(nombre=nombre, grupo_muscular=grupo_muscular, descripcion=descripcion, objetivo=objetivo, video_url=video_url, video_mime=video_mime)  # type: ignore
        new_id = db.crear_ejercicio(ejercicio)  # type: ignore
        return {"ok": True, "id": int(new_id)}
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

@app.put("/api/ejercicios/{ejercicio_id}")
async def api_ejercicios_update(ejercicio_id: int, request: Request, _=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, f"/api/ejercicios/{ejercicio_id}[PUT]")
    if guard:
        return guard
    if Ejercicio is None:
        raise HTTPException(status_code=503, detail="Modelo Ejercicio no disponible")
    payload = await request.json()
    try:
        # Obtener columnas disponibles y seleccionar dinámicamente
        try:
            cols = db.get_table_columns('ejercicios')  # type: ignore
        except Exception:
            cols = []
        base_cols = ["id", "nombre", "grupo_muscular", "descripcion"]
        opt_cols = []
        if "objetivo" in cols:
            opt_cols.append("objetivo")
        if "video_url" in cols:
            opt_cols.append("video_url")
        if "video_mime" in cols:
            opt_cols.append("video_mime")
        select_cols = base_cols + opt_cols
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(
                f"SELECT {', '.join(select_cols)} FROM ejercicios WHERE id = %s",
                (int(ejercicio_id),),
            )
            existing = cur.fetchone()
        if not existing:
            raise HTTPException(status_code=404, detail="Ejercicio no encontrado")
        nombre = (payload.get("nombre") or existing.get("nombre") or "").strip() or (existing.get("nombre") or "")
        if not nombre:
            raise HTTPException(status_code=400, detail="'nombre' es obligatorio")
        grupo_muscular = payload.get("grupo_muscular") if ("grupo_muscular" in payload) else existing.get("grupo_muscular")
        descripcion = payload.get("descripcion") if ("descripcion" in payload) else existing.get("descripcion")
        objetivo = (payload.get("objetivo") or existing.get("objetivo") or "general").strip() or (existing.get("objetivo") or "general")
        video_url = payload.get("video_url") if ("video_url" in payload) else (existing.get("video_url") if existing and ("video_url" in select_cols) else None)
        video_mime = payload.get("video_mime") if ("video_mime" in payload) else (existing.get("video_mime") if existing and ("video_mime" in select_cols) else None)
        ejercicio = Ejercicio(id=int(ejercicio_id), nombre=nombre, grupo_muscular=grupo_muscular, descripcion=descripcion, objetivo=objetivo, video_url=video_url, video_mime=video_mime)  # type: ignore
        db.actualizar_ejercicio(ejercicio)  # type: ignore
        return {"ok": True, "id": int(ejercicio_id)}
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

@app.post("/api/ejercicios/{ejercicio_id}/media")
async def api_ejercicio_upload_media(ejercicio_id: int, file: UploadFile = File(...), overwrite: bool = Form(True), _=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, f"/api/ejercicios/{ejercicio_id}/media[POST]")
    if guard:
        return guard
    try:
        # Validar existencia del ejercicio
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("SELECT id FROM ejercicios WHERE id = %s", (int(ejercicio_id),))
            if cur.fetchone() is None:
                raise HTTPException(status_code=404, detail="Ejercicio no encontrado")

        content_type = (file.content_type or "").lower()
        if not content_type or not any(content_type.startswith(prefix) for prefix in ("video/", "image/")):
            raise HTTPException(status_code=400, detail="Tipo de archivo no permitido")

        # Nombre destino seguro
        orig_name = (file.filename or "media").replace("\r", "").replace("\n", "")
        # Extraer extensión de filename o deducir por MIME
        ext = ""
        if "." in orig_name:
            ext = orig_name.split(".")[-1].lower()
        if not ext:
            # Deducción básica
            if content_type == "image/gif":
                ext = "gif"
            elif content_type.startswith("video/"):
                ext = content_type.split("/")[-1]
            else:
                ext = "bin"
        ts = int(time.time())
        dest_name = f"ej_{int(ejercicio_id)}_{ts}.{ext}"
        dest_path = uploads_dir / dest_name

        # Leer contenido
        data = await file.read()

        # Intentar subir a GCS si está configurado
        url: Optional[str] = None
        storage: str = ""
        try:
            url = _upload_media_to_gcs(dest_name, data, content_type)
            if url:
                storage = "gcs"
        except HTTPException:
            raise
        except Exception:
            url = None

        # Fallback: Backblaze B2 si GCS no devuelve URL
        b2_err_detail: Optional[str] = None
        if not url:
            try:
                url = _upload_media_to_b2(dest_name, data, content_type)
                if url:
                    storage = "b2"
            except HTTPException as he:
                # Tratar B2 como opcional: no abortar, continuar al fallback local
                try:
                    b2_err_detail = str(getattr(he, 'detail', he))
                    logging.warning(f"B2 fallback error: {b2_err_detail}")
                except Exception:
                    pass
                url = None
            except Exception as e:
                try:
                    b2_err_detail = str(e)
                    logging.warning(f"B2 fallback error: {b2_err_detail}")
                except Exception:
                    pass
                url = None

        if not url:
            # En entornos de producción, permitir exigir almacenamiento remoto
            try:
                require_remote = (os.getenv("REQUIRE_REMOTE_MEDIA", "0").strip().lower() in ("1", "true", "yes"))
            except Exception:
                require_remote = False
            if require_remote:
                raise HTTPException(status_code=502, detail=f"Falló la subida remota (GCS/B2). {b2_err_detail or ''}".strip())
            # Guardar localmente
            try:
                with open(dest_path, "wb") as f:
                    f.write(data)
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Error guardando archivo: {e}")
            # URL relativa local
            url = f"/uploads/{dest_name}"
            storage = "local"
        try:
            # Intentar garantizar columnas de medios (rápido y seguro)
            try:
                cols0 = []
                try:
                    cols0 = db.get_table_columns('ejercicios')  # type: ignore
                except Exception:
                    cols0 = []
                if ('video_url' not in cols0) or ('video_mime' not in cols0):
                    with db.get_connection_context() as conn:  # type: ignore
                        cur0 = conn.cursor()
                        try:
                            if 'video_url' not in cols0:
                                cur0.execute("ALTER TABLE ejercicios ADD COLUMN video_url VARCHAR(512)")
                        except Exception:
                            pass
                        try:
                            if 'video_mime' not in cols0:
                                cur0.execute("ALTER TABLE ejercicios ADD COLUMN video_mime VARCHAR(50)")
                        except Exception:
                            pass
                        conn.commit()
            except Exception:
                pass

            # Actualizar usando solo columnas existentes
            try:
                cols = db.get_table_columns('ejercicios')  # type: ignore
            except Exception:
                cols = []
            with db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor()
                if ('video_url' in cols) and ('video_mime' in cols):
                    cur.execute("UPDATE ejercicios SET video_url = %s, video_mime = %s WHERE id = %s", (url, content_type, int(ejercicio_id)))
                elif ('video_url' in cols):
                    cur.execute("UPDATE ejercicios SET video_url = %s WHERE id = %s", (url, int(ejercicio_id)))
                elif ('video_mime' in cols):
                    cur.execute("UPDATE ejercicios SET video_mime = %s WHERE id = %s", (content_type, int(ejercicio_id)))
                else:
                    # Si no existen columnas de medios, no fallar; el archivo ya está subido
                    pass
                conn.commit()
                try:
                    db.cache.invalidate('ejercicios')  # type: ignore
                except Exception:
                    pass
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error actualizando ejercicio: {e}")

        resp = {"ok": True, "url": url, "mime": content_type, "filename": dest_name, "storage": storage}
        if storage == "local" and b2_err_detail:
            resp["warning"] = f"Remote upload failed; using local storage. {b2_err_detail}"
        return resp
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

@app.delete("/api/ejercicios/{ejercicio_id}")
async def api_ejercicios_delete(ejercicio_id: int, _=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, f"/api/ejercicios/{ejercicio_id}[DELETE]")
    if guard:
        return guard
    try:
        db.eliminar_ejercicio(int(ejercicio_id))  # type: ignore
        return {"ok": True}
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/conceptos_pago")
async def api_conceptos_pago(_=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        return []
    guard = _circuit_guard_json(db, "/api/conceptos_pago")
    if guard:
        return guard
    try:
        # Asegurar que exista el concepto "Cuota Mensual" de tipo variable (no bloquear si falla)
        try:
            with db.get_connection_context() as conn:  # type: ignore
                c = conn.cursor()
                c.execute("SELECT id FROM conceptos_pago WHERE LOWER(nombre) = LOWER(%s) LIMIT 1", ("Cuota Mensual",))
                exists = c.fetchone()
                if not exists:
                    c.execute(
                        "INSERT INTO conceptos_pago (nombre, descripcion, precio_base, tipo, activo) VALUES (%s, %s, %s, %s, %s)",
                        ("Cuota Mensual", "Cuota mensual estándar", 0.0, "variable", True)
                    )
                    conn.commit()
                try:
                    db.cache.invalidate('conceptos_pago')  # type: ignore
                except Exception:
                    pass
        except Exception:
            pass

        rows = db.obtener_conceptos_pago(solo_activos=True)  # type: ignore
        return [
            {
                'id': r.get('id'),
                'nombre': r.get('nombre'),
                'descripcion': r.get('descripcion'),
                'precio_base': r.get('precio_base'),
                'tipo': r.get('tipo'),
                'activo': r.get('activo'),
            }
            for r in (rows or [])
        ]
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.post("/api/conceptos_pago")
async def api_conceptos_pago_create(request: Request, _=Depends(require_gestion_access)):
    pm = _get_pm()
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, "/api/conceptos_pago[POST]")
    if guard:
        return guard
    pm_ready = (pm is not None and ConceptoPago is not None)
    payload = await request.json()
    try:
        nombre = (payload.get("nombre") or "").strip()
        if not nombre:
            raise HTTPException(status_code=400, detail="'nombre' es obligatorio")
        descripcion = payload.get("descripcion")
        precio_base_raw = payload.get("precio_base")
        precio_base = float(precio_base_raw) if precio_base_raw is not None else 0.0
        if precio_base < 0:
            raise HTTPException(status_code=400, detail="'precio_base' no puede ser negativo")
        tipo = (payload.get("tipo") or "fijo").strip().lower()
        if tipo not in ["fijo", "variable"]:
            raise HTTPException(status_code=400, detail="'tipo' debe ser 'fijo' o 'variable'")
        activo = bool(payload.get("activo", True))
        if pm_ready:
            concepto = ConceptoPago(nombre=nombre, descripcion=descripcion, precio_base=precio_base, tipo=tipo, activo=activo)  # type: ignore
            new_id = pm.crear_concepto_pago(concepto)
            return {"ok": True, "id": int(new_id)}
        else:
            from psycopg2 import sql as _sql
            with db.get_connection_context() as conn:  # type: ignore
                data = {
                    "nombre": nombre,
                    "descripcion": descripcion,
                    "precio_base": precio_base,
                    "tipo": tipo,
                    "activo": activo,
                    "categoria": (payload.get("categoria") or "general").strip().lower(),
                }
                filtered = _filter_existing_columns(conn, "public", "conceptos_pago", data)
                if not filtered:
                    raise HTTPException(status_code=400, detail="No hay columnas válidas para insertar")
                cols = list(filtered.keys())
                stmt = _sql.SQL("INSERT INTO {}.{} ({}) VALUES ({}) RETURNING id").format(
                    _sql.Identifier("public"), _sql.Identifier("conceptos_pago"),
                    _sql.SQL(", ").join([_sql.Identifier(c) for c in cols]),
                    _sql.SQL(", ").join([_sql.Placeholder() for _ in cols]),
                )
                cur = conn.cursor()
                cur.execute(stmt, [filtered[c] for c in cols])
                row_id = cur.fetchone()
                new_id = int(row_id[0]) if row_id else None
                if new_id is None:
                    raise HTTPException(status_code=500, detail="No se pudo crear el concepto de pago")
                conn.commit()
                return {"ok": True, "id": new_id}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

@app.put("/api/conceptos_pago/{concepto_id}")
async def api_conceptos_pago_update(concepto_id: int, request: Request, _=Depends(require_gestion_access)):
    pm = _get_pm()
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, f"/api/conceptos_pago/{concepto_id}[PUT]")
    if guard:
        return guard
    pm_ready = (pm is not None and ConceptoPago is not None)
    payload = await request.json()
    try:
        # Como no hay método para obtener un concepto individual, consultamos SQL para merge
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(
                "SELECT id, nombre, descripcion, precio_base, tipo, activo FROM conceptos_pago WHERE id = %s",
                (int(concepto_id),)
            )
            row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Concepto de pago no encontrado")
        nombre = (payload.get("nombre") or row.get("nombre") or "").strip() or row.get("nombre")
        descripcion = payload.get("descripcion") if ("descripcion" in payload) else row.get("descripcion")
        precio_base = float(payload.get("precio_base")) if (payload.get("precio_base") is not None) else float(row.get("precio_base") or 0.0)
        if precio_base < 0:
            raise HTTPException(status_code=400, detail="'precio_base' no puede ser negativo")
        tipo = (payload.get("tipo") or row.get("tipo") or "fijo").strip().lower()
        if tipo not in ["fijo", "variable"]:
            raise HTTPException(status_code=400, detail="'tipo' debe ser 'fijo' o 'variable'")
        activo = bool(payload.get("activo")) if ("activo" in payload) else bool(row.get("activo"))
        categoria = (payload.get("categoria") or row.get("categoria") or "general").strip().lower()
        if pm_ready:
            concepto = ConceptoPago(id=int(concepto_id), nombre=nombre, descripcion=descripcion, precio_base=precio_base, tipo=tipo, activo=activo, categoria=categoria)  # type: ignore
            updated = pm.actualizar_concepto_pago(concepto)
            if not updated:
                raise HTTPException(status_code=404, detail="No se pudo actualizar el concepto de pago")
            return {"ok": True, "id": int(concepto_id)}
        else:
            updates = {
                "nombre": nombre,
                "descripcion": descripcion,
                "precio_base": precio_base,
                "tipo": tipo,
                "activo": activo,
                "categoria": categoria,
            }
            with db.get_connection_context() as conn:  # type: ignore
                ok = _apply_change_idempotent(
                    conn,
                    schema="public",
                    table="conceptos_pago",
                    operation="UPDATE",
                    key_column="id",
                    key_value=int(concepto_id),
                    update_fields=updates,
                    where=[("id", int(concepto_id))],
                )
                if not ok:
                    raise HTTPException(status_code=404, detail="No se pudo actualizar el concepto de pago")
                conn.commit()
                return {"ok": True, "id": int(concepto_id)}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

@app.delete("/api/conceptos_pago/{concepto_id}")
async def api_conceptos_pago_delete(concepto_id: int, _=Depends(require_gestion_access)):
    pm = _get_pm()
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, f"/api/conceptos_pago/{concepto_id}[DELETE]")
    if guard:
        return guard
    pm_ready = (pm is not None)
    try:
        # Bloquear eliminación del concepto predeterminado "Cuota Mensual"
        try:
            with db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur.execute("SELECT nombre FROM conceptos_pago WHERE id = %s", (int(concepto_id),))
                row = cur.fetchone()
        except Exception:
            row = None
        if not row:
            raise HTTPException(status_code=404, detail="Concepto de pago no encontrado")
        nombre_concepto = (row.get("nombre") or "").strip().lower()
        if nombre_concepto == "cuota mensual":
            raise HTTPException(status_code=400, detail="No se puede eliminar el concepto 'Cuota Mensual'")

        if pm_ready:
            deleted = pm.eliminar_concepto_pago(int(concepto_id))
            if not deleted:
                raise HTTPException(status_code=404, detail="No se pudo eliminar el concepto de pago")
            return {"ok": True}
        else:
            with db.get_connection_context() as conn:  # type: ignore
                ok = _apply_change_idempotent(
                    conn,
                    schema="public",
                    table="conceptos_pago",
                    operation="DELETE",
                    key_column="id",
                    key_value=int(concepto_id),
                    where=[("id", int(concepto_id))],
                )
                if not ok:
                    raise HTTPException(status_code=404, detail="No se pudo eliminar el concepto de pago")
                conn.commit()
                return {"ok": True}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/tipos_cuota_activos")
async def api_tipos_cuota_activos(_=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        return []
    guard = _circuit_guard_json(db, "/api/tipos_cuota_activos")
    if guard:
        return guard
    try:
        tipos = db.obtener_tipos_cuota_activos()  # type: ignore
        tipos = sorted(tipos or [], key=lambda t: (float(getattr(t, 'precio', 0.0) or 0.0), (getattr(t, 'nombre', '') or '')))
        return [
            {
                "id": int(getattr(t, 'id')) if getattr(t, 'id') is not None else None,
                "nombre": (getattr(t, 'nombre', '') or '').strip(),
                "precio": float(getattr(t, 'precio', 0.0) or 0.0),
                "duracion_dias": int(getattr(t, 'duracion_dias', 30) or 30),
                "activo": bool(getattr(t, 'activo', True)),
            }
            for t in tipos
        ]
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

# --- CRUD Tipos de Cuota ---
@app.get("/api/tipos_cuota_catalogo")
async def api_tipos_cuota_catalogo(_=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        return []
    guard = _circuit_guard_json(db, "/api/tipos_cuota_catalogo")
    if guard:
        return guard
    try:
        tipos = db.obtener_tipos_cuota(solo_activos=False)  # type: ignore
        tipos = sorted(tipos or [], key=lambda t: (
            0 if bool(getattr(t, 'activo', True)) else 1,
            float(getattr(t, 'precio', 0.0) or 0.0),
            (getattr(t, 'nombre', '') or '')
        ))
        return [
            {
                "id": int(getattr(t, 'id')) if getattr(t, 'id') is not None else None,
                "nombre": (getattr(t, 'nombre', '') or '').strip(),
                "precio": float(getattr(t, 'precio', 0.0) or 0.0),
                "duracion_dias": int(getattr(t, 'duracion_dias', 30) or 30),
                "activo": bool(getattr(t, 'activo', True)),
                "descripcion": getattr(t, 'descripcion', None),
                "icono_path": getattr(t, 'icono_path', None),
            }
            for t in tipos
        ]
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

@app.post("/api/tipos_cuota")
async def api_tipos_cuota_create(request: Request, _=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, "/api/tipos_cuota[POST]")
    if guard:
        return guard
    payload = await request.json()
    try:
        nombre = (payload.get("nombre") or "").strip()
        if not nombre:
            raise HTTPException(status_code=400, detail="'nombre' es obligatorio")
        precio_raw = payload.get("precio")
        precio = float(precio_raw) if precio_raw is not None else 0.0
        if precio < 0:
            raise HTTPException(status_code=400, detail="'precio' no puede ser negativo")
        duracion_raw = payload.get("duracion_dias")
        duracion_dias = int(duracion_raw) if duracion_raw is not None else 30
        if duracion_dias <= 0:
            raise HTTPException(status_code=400, detail="'duracion_dias' debe ser > 0")
        activo = bool(payload.get("activo", True))
        descripcion = payload.get("descripcion")
        icono_path = payload.get("icono_path")
        with db.get_connection_context() as conn:  # type: ignore
            from psycopg2 import sql as _sql
            data = {
                "nombre": nombre,
                "precio": precio,
                "duracion_dias": duracion_dias,
                "activo": activo,
                "descripcion": descripcion,
                "icono_path": icono_path,
            }
            filtered = _filter_existing_columns(conn, "public", "tipos_cuota", data)
            if not filtered:
                raise HTTPException(status_code=400, detail="No hay columnas válidas para insertar")
            cols = list(filtered.keys())
            stmt = _sql.SQL("INSERT INTO {}.{} ({}) VALUES ({}) RETURNING id").format(
                _sql.Identifier("public"),
                _sql.Identifier("tipos_cuota"),
                _sql.SQL(", ").join([_sql.Identifier(c) for c in cols]),
                _sql.SQL(", ").join([_sql.Placeholder() for _ in cols]),
            )
            cur = conn.cursor()
            cur.execute(stmt, [filtered[c] for c in cols])
            new_id_row = cur.fetchone()
            new_id = int(new_id_row[0]) if new_id_row else None
            if new_id is None:
                raise HTTPException(status_code=500, detail="No se pudo crear el tipo de cuota")
            conn.commit()
            try:
                db.cache.invalidate('tipos')  # type: ignore
            except Exception:
                pass
            return {"ok": True, "id": new_id}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

@app.put("/api/tipos_cuota/{tipo_id}")
async def api_tipos_cuota_update(tipo_id: int, request: Request, _=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, f"/api/tipos_cuota/{tipo_id}[PUT]")
    if guard:
        return guard
    payload = await request.json()
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("SELECT id, nombre, precio, duracion_dias, activo, descripcion, icono_path FROM tipos_cuota WHERE id = %s", (int(tipo_id),))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Tipo de cuota no encontrado")
        nombre = (payload.get("nombre") or row.get("nombre") or "").strip() or row.get("nombre")
        precio = float(payload.get("precio")) if (payload.get("precio") is not None) else float(row.get("precio") or 0.0)
        if precio < 0:
            raise HTTPException(status_code=400, detail="'precio' no puede ser negativo")
        duracion_dias = int(payload.get("duracion_dias")) if (payload.get("duracion_dias") is not None) else int(row.get("duracion_dias") or 30)
        if duracion_dias <= 0:
            raise HTTPException(status_code=400, detail="'duracion_dias' debe ser > 0")
        activo = bool(payload.get("activo")) if ("activo" in payload) else bool(row.get("activo"))
        descripcion = payload.get("descripcion") if ("descripcion" in payload) else row.get("descripcion")
        icono_path = payload.get("icono_path") if ("icono_path" in payload) else row.get("icono_path")
        updates = {
            "nombre": nombre,
            "precio": precio,
            "duracion_dias": duracion_dias,
            "activo": activo,
            "descripcion": descripcion,
            "icono_path": icono_path,
        }
        with db.get_connection_context() as conn:  # type: ignore
            ok = _apply_change_idempotent(conn, "public", "tipos_cuota", "UPDATE", {"id": int(tipo_id)}, updates)
            if not ok:
                raise HTTPException(status_code=500, detail="No se pudo actualizar el tipo de cuota")
            conn.commit()
            try:
                db.cache.invalidate('tipos')  # type: ignore
            except Exception:
                pass
            return {"ok": True, "id": int(tipo_id)}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

@app.delete("/api/tipos_cuota/{tipo_id}")
async def api_tipos_cuota_delete(tipo_id: int, _=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, f"/api/tipos_cuota/{tipo_id}[DELETE]")
    if guard:
        return guard
    try:
        with db.get_connection_context() as conn:  # type: ignore
            ok = _apply_change_idempotent(conn, "public", "tipos_cuota", "DELETE", {"id": int(tipo_id)}, {})
            if not ok:
                raise HTTPException(status_code=500, detail="No se pudo eliminar el tipo de cuota")
            conn.commit()
            try:
                db.cache.invalidate('tipos')  # type: ignore
            except Exception:
                pass
            return {"ok": True}
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/gestion/login")
async def gestion_login_get(request: Request):
    theme_vars = read_theme_vars(static_dir / "style.css")
    ctx = {
        "request": request,
        "theme": theme_vars,
        "error": request.query_params.get("error"),
        "gym_name": get_gym_name("Gimnasio"),
        "logo_url": _resolve_logo_url(),
    }
    return templates.TemplateResponse("gestion_login.html", ctx)

@app.get("/api/profesores_basico")
async def api_profesores_basico():
    db = _get_db()
    if db is None:
        return []
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            cur.execute(
                """
                SELECT p.id AS profesor_id,
                       u.id AS usuario_id,
                       COALESCE(u.nombre,'') AS nombre
                FROM profesores p
                JOIN usuarios u ON u.id = p.usuario_id
                ORDER BY p.id
                """
            )
            res = []
            for r in cur.fetchall():
                res.append({
                    "profesor_id": int(r[0] or 0),
                    "usuario_id": int(r[1] or 0),
                    "nombre": r[2] or ""
                })
            return res
    except Exception:
        try:
            logging.exception("Error en /api/profesores_basico")
        except Exception:
            pass
        return []

@app.post("/gestion/auth")
async def gestion_auth(request: Request):
    try:
        content_type = request.headers.get("content-type", "")
        if content_type.startswith("application/json"):
            data = await request.json()
        else:
            data = await request.form()
    except Exception:
        data = {}
    usuario_id_raw = data.get("usuario_id")
    owner_password = str(data.get("owner_password", "")).strip()
    pin_raw = data.get("pin")

    db = _get_db()
    if db is None:
        return RedirectResponse(url="/gestion/login?error=Base%20de%20datos%20no%20disponible", status_code=303)

    # Modo Dueño: usuario_id == "__OWNER__" y contraseña
    if isinstance(usuario_id_raw, str) and usuario_id_raw == "__OWNER__":
        if not owner_password:
            return RedirectResponse(url="/gestion/login?error=Ingrese%20la%20contrase%C3%B1a", status_code=303)
        if _verify_owner_password(owner_password):
            request.session.clear()
            request.session["logged_in"] = True
            request.session["role"] = "dueño"
            # Dueño también puede entrar a Gestión
            return RedirectResponse(url="/gestion", status_code=303)
        return RedirectResponse(url="/gestion/login?error=Credenciales%20inv%C3%A1lidas", status_code=303)

    # Modo Profesor: usuario_id numérico y PIN
    try:
        usuario_id = int(usuario_id_raw) if usuario_id_raw is not None else None
    except Exception:
        usuario_id = None
    pin = str(pin_raw or "").strip()
    if not usuario_id or not pin:
        return RedirectResponse(url="/gestion/login?error=Par%C3%A1metros%20inv%C3%A1lidos", status_code=303)

    ok = False
    try:
        ok = bool(db.verificar_pin_usuario(usuario_id, pin))  # type: ignore
    except Exception:
        ok = False
    if not ok:
        return RedirectResponse(url="/gestion/login?error=PIN%20inv%C3%A1lido", status_code=303)

    # Asegurar que cualquier sesión previa de dueño no contamine el login de profesor
    try:
        request.session.clear()
    except Exception:
        try:
            request.session.pop("logged_in", None)
            request.session.pop("role", None)
        except Exception:
            pass

    profesor_id = None
    prof = None
    try:
        prof = db.obtener_profesor_por_usuario_id(usuario_id)  # type: ignore
    except Exception:
        prof = None
    # Derivar rol del usuario para crear perfil si falta
    user_role = None
    try:
        u = db.obtener_usuario_por_id(usuario_id)  # type: ignore
        if u is not None:
            user_role = getattr(u, 'rol', None) or (u.get('rol') if isinstance(u, dict) else None)
    except Exception:
        user_role = None
    try:
        # Obtener profesor_id si existe
        if prof:
            profesor_id = getattr(prof, 'profesor_id', None)
            if profesor_id is None and isinstance(prof, dict):
                profesor_id = prof.get('profesor_id')
        # Crear perfil automáticamente si el usuario es profesor y no tiene perfil
        if (profesor_id is None) and (user_role == 'profesor'):
            profesor_id = db.crear_profesor(usuario_id)  # type: ignore
    except Exception:
        profesor_id = None

    request.session["gestion_profesor_user_id"] = usuario_id
    # Marcar rol para la UI y controles de sesión
    try:
        request.session["role"] = "profesor"
    except Exception:
        pass
    if profesor_id:
        request.session["gestion_profesor_id"] = int(profesor_id)
        try:
            # Auto-inicio de sesión de trabajo (idempotente)
            db.iniciar_sesion_trabajo_profesor(int(profesor_id), 'Trabajo')  # type: ignore
        except Exception:
            pass
    return RedirectResponse(url="/gestion", status_code=303)


# --- API de autenticación basada en JSON ---
@app.post("/api/auth/login")
async def api_auth_login(request: Request):
    try:
        data = await request.json()
    except Exception:
        data = {}
    usuario_id_raw = data.get("usuario_id")
    owner_password = str(data.get("owner_password", "")).strip()
    pin_raw = data.get("pin")

    db = _get_db()
    if db is None:
        return JSONResponse({"ok": False, "error": "DB no disponible"}, status_code=503)

    # Modo Dueño (owner)
    if isinstance(usuario_id_raw, str) and usuario_id_raw == "__OWNER__":
        if not owner_password:
            return JSONResponse({"ok": False, "error": "Ingrese la contraseña"}, status_code=400)
        if _verify_owner_password(owner_password):
            request.session.clear()
            request.session["logged_in"] = True
            request.session["role"] = "dueño"
            return JSONResponse({"ok": True, "role": "dueño"})
        return JSONResponse({"ok": False, "error": "Credenciales inválidas"}, status_code=401)

    # Modo Profesor: usuario_id + PIN
    try:
        usuario_id = int(usuario_id_raw) if usuario_id_raw is not None else None
    except Exception:
        usuario_id = None
    pin = str(pin_raw or "").strip()
    if not usuario_id or not pin:
        return JSONResponse({"ok": False, "error": "Parámetros inválidos"}, status_code=400)

    try:
        ok = bool(db.verificar_pin_usuario(usuario_id, pin))  # type: ignore (usa hash + migración)
    except Exception:
        ok = False
    if not ok:
        return JSONResponse({"ok": False, "error": "PIN inválido"}, status_code=401)

    # Limpiar sesión previa y setear sesión de profesor
    try:
        request.session.clear()
    except Exception:
        try:
            request.session.pop("logged_in", None)
            request.session.pop("role", None)
        except Exception:
            pass

    profesor_id = None
    try:
        prof = db.obtener_profesor_por_usuario_id(usuario_id)  # type: ignore
    except Exception:
        prof = None
    user_role = None
    try:
        u = db.obtener_usuario_por_id(usuario_id)  # type: ignore
        if u is not None:
            user_role = getattr(u, 'rol', None) or (u.get('rol') if isinstance(u, dict) else None)
    except Exception:
        user_role = None
    try:
        if prof:
            profesor_id = getattr(prof, 'profesor_id', None)
            if profesor_id is None and isinstance(prof, dict):
                profesor_id = prof.get('profesor_id')
        if (profesor_id is None) and (user_role == 'profesor'):
            profesor_id = db.crear_profesor(usuario_id)  # type: ignore
    except Exception:
        profesor_id = None

    request.session["gestion_profesor_user_id"] = usuario_id
    request.session["role"] = "profesor"
    if profesor_id:
        request.session["gestion_profesor_id"] = int(profesor_id)
        try:
            db.iniciar_sesion_trabajo_profesor(int(profesor_id), 'Trabajo')  # type: ignore
        except Exception:
            pass

    return JSONResponse({
        "ok": True,
        "role": "profesor",
        "usuario_id": int(usuario_id),
        "profesor_id": int(profesor_id) if profesor_id is not None else None
    })


@app.post("/api/auth/logout")
async def api_auth_logout(request: Request):
    try:
        request.session.clear()
    except Exception:
        pass
    return JSONResponse({"ok": True})


@app.get("/api/theme")
async def api_theme(_=Depends(require_owner)):
    try:
        theme_vars = read_theme_vars(static_dir / "style.css")
        return JSONResponse(theme_vars)
    except Exception as e:
        logging.exception("Error in /api/theme")
        return JSONResponse({"error": str(e)}, status_code=500)

# Verificación de LibreOffice en runtime
@app.get("/api/system/libreoffice")
async def api_system_libreoffice(_=Depends(require_owner)):
    try:
        path = shutil.which("soffice") or shutil.which("soffice.exe")
        available = bool(path)
        version = None
        if path:
            try:
                res = subprocess.run([path, "--version"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
                version = (res.stdout or "").strip()
            except Exception:
                version = None
        return JSONResponse({"available": available, "path": path, "version": version})
    except Exception as e:
        logging.exception("Error en /api/system/libreoffice")
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/kpis")
async def api_kpis(_=Depends(require_owner)):
    db = _get_db()
    if db is None:
        return {"kpis": {"total_activos": 0, "nuevos_30_dias": 0, "ingresos_mes_actual": 0.0, "asistencias_hoy": 0}, "arpu": 0.0, "morosos": 0}
    guard = _circuit_guard_json(db, "/api/kpis")
    if guard:
        return guard
    try:
        data = db.obtener_kpis_generales()
        arpu, morosos = db.obtener_arpu_y_morosos_mes_actual()
        return {"kpis": data, "arpu": arpu, "morosos": morosos}
    except Exception as e:
        logging.exception("Error in /api/kpis")
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/ingresos12m")
async def api_ingresos12m(request: Request, _=Depends(require_owner)):
    db = _get_db()
    result: Dict[str, float] = {}
    # Sin DB no podemos calcular; devolvemos estructura vacía
    if db is None:
        return {"ingresos": result}
    guard = _circuit_guard_json(db, "/api/ingresos12m")
    if guard:
        return guard
    try:
        # Calcular ingresos por mes con SQL directo respetando rango si se provee
        start = request.query_params.get("start")
        end = request.query_params.get("end")
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            if start and end:
                cur.execute(
                    """
                    WITH bounds AS (
                      SELECT date_trunc('month', %s::date) AS s, date_trunc('month', %s::date) AS e
                    )
                    SELECT to_char(date_trunc('month', COALESCE(p.fecha_pago::timestamp, make_timestamp(p.año, p.mes, 1, 0, 0, 0))), 'YYYY-MM') AS ym,
                           COALESCE(SUM(p.monto), 0) AS total
                    FROM pagos p, bounds b
                    WHERE date_trunc('month', COALESCE(p.fecha_pago::timestamp, make_timestamp(p.año, p.mes, 1, 0, 0, 0))) BETWEEN b.s AND b.e
                    GROUP BY 1
                    ORDER BY 1
                    """,
                    (start, end)
                )
            else:
                cur.execute(
                    """
                    SELECT to_char(date_trunc('month', COALESCE(p.fecha_pago::timestamp, make_timestamp(p.año, p.mes, 1, 0, 0, 0))), 'YYYY-MM') AS ym,
                           COALESCE(SUM(p.monto), 0) AS total
                    FROM pagos p
                    WHERE date_trunc('month', COALESCE(p.fecha_pago::timestamp, make_timestamp(p.año, p.mes, 1, 0, 0, 0))) 
                          >= date_trunc('month', CURRENT_DATE) - INTERVAL '11 months'
                    GROUP BY 1
                    ORDER BY 1
                    """
                )
            rows = cur.fetchall() or []

        # Construir base del rango solicitado o últimos 12 meses y completar faltantes
        try:
            from datetime import datetime as dt, date, timedelta
            base: Dict[str, float] = {}
            if start and end:
                s = dt.strptime(start, "%Y-%m-%d").date().replace(day=1)
                e = dt.strptime(end, "%Y-%m-%d").date().replace(day=1)
                curm = s
                while curm <= e:
                    base[curm.strftime("%Y-%m")] = 0.0
                    curm = (curm.replace(day=28) + timedelta(days=4)).replace(day=1)
            else:
                hoy = date.today().replace(day=1)
                curm = hoy
                for _ in range(12):
                    base[curm.strftime("%Y-%m")] = 0.0
                    curm = (curm.replace(day=28) + timedelta(days=4)).replace(day=1)
            for r in rows:
                ym = str(r.get('ym'))
                base[ym] = float(r.get('total') or 0.0)
            result = dict(sorted(base.items()))
        except Exception:
            # Si algo falla en el relleno, devolvemos la agregación tal cual
            result = {str(r.get('ym')): float(r.get('total') or 0.0) for r in rows}
        return {"ingresos": result}
    except Exception as e:
        logging.exception("Error en /api/ingresos12m")
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/nuevos12m")
async def api_nuevos12m(request: Request, _=Depends(require_owner)):
    db = _get_db()
    result: Dict[str, int] = {}
    if db is None:
        return {"nuevos": result}
    guard = _circuit_guard_json(db, "/api/nuevos12m")
    if guard:
        return guard
    try:
        # Delegar completamente al backend de database.py para mantener consistencia
        start = request.query_params.get("start")
        end = request.query_params.get("end")
        from datetime import datetime
        if start and end:
            try:
                inicio = datetime.strptime(start, "%Y-%m-%d").date()
                fin = datetime.strptime(end, "%Y-%m-%d").date()
                result = db.obtener_nuevos_usuarios_por_mes_rango(inicio, fin)  # type: ignore
            except Exception:
                # Si el rango no es válido, usar últimos 12 meses
                result = db.obtener_nuevos_usuarios_por_mes_ultimos_12()  # type: ignore
        else:
            result = db.obtener_nuevos_usuarios_por_mes_ultimos_12()  # type: ignore
        # Rellenar meses faltantes con 0 para evitar gráficos vacíos
        try:
            from datetime import date, timedelta
            base: Dict[str, int] = {}
            hoy = date.today().replace(day=1)
            for i in range(11, -1, -1):
                m = (hoy - timedelta(days=30 * i)).replace(day=1)
                clave = m.strftime("%Y-%m")
                base[clave] = 0
            base.update(result or {})
            result = dict(sorted(base.items()))
        except Exception:
            pass
        return {"nuevos": result}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/arpu12m")
async def api_arpu12m(request: Request, _=Depends(require_owner)):
    db = _get_db()
    result: Dict[str, float] = {}
    if db is None:
        return {"arpu": result}
    guard = _circuit_guard_json(db, "/api/arpu12m")
    if guard:
        return guard
    try:
        start = request.query_params.get("start")
        end = request.query_params.get("end")
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            if start and end:
                cur.execute(
                    """
                    WITH bounds AS (
                      SELECT date_trunc('month', %s::date) AS s, date_trunc('month', %s::date) AS e
                    )
                    SELECT to_char(date_trunc('month', COALESCE(p.fecha_pago::timestamp, make_timestamp(p.año, p.mes, 1, 0, 0, 0))), 'YYYY-MM') AS ym,
                           COALESCE(SUM(p.monto), 0) AS ingresos,
                           COUNT(DISTINCT p.usuario_id) AS pagadores
                    FROM pagos p, bounds b
                    WHERE date_trunc('month', COALESCE(p.fecha_pago::timestamp, make_timestamp(p.año, p.mes, 1, 0, 0, 0))) BETWEEN b.s AND b.e
                    GROUP BY 1
                    ORDER BY 1
                    """,
                    (start, end)
                )
            else:
                cur.execute(
                    """
                    SELECT to_char(date_trunc('month', COALESCE(p.fecha_pago::timestamp, make_timestamp(p.año, p.mes, 1, 0, 0, 0))), 'YYYY-MM') AS ym,
                           COALESCE(SUM(p.monto), 0) AS ingresos,
                           COUNT(DISTINCT p.usuario_id) AS pagadores
                    FROM pagos p
                    WHERE date_trunc('month', COALESCE(p.fecha_pago::timestamp, make_timestamp(p.año, p.mes, 1, 0, 0, 0))) 
                          >= date_trunc('month', CURRENT_DATE) - INTERVAL '11 months'
                    GROUP BY 1
                    ORDER BY 1
                    """
                )
            rows = cur.fetchall() or []

        # Construir base del rango solicitado o últimos 12 meses y completar faltantes
        try:
            from datetime import datetime as dt, date, timedelta
            base: Dict[str, float] = {}
            if start and end:
                s = dt.strptime(start, "%Y-%m-%d").date().replace(day=1)
                e = dt.strptime(end, "%Y-%m-%d").date().replace(day=1)
                curm = s
                while curm <= e:
                    base[curm.strftime("%Y-%m")] = 0.0
                    curm = (curm.replace(day=28) + timedelta(days=4)).replace(day=1)
            else:
                hoy = date.today().replace(day=1)
                for i in range(11, -1, -1):
                    m = (hoy - timedelta(days=30 * i)).replace(day=1)
                    base[m.strftime("%Y-%m")] = 0.0
            for r in rows:
                ym = str(r.get('ym'))
                ingresos = float(r.get('ingresos') or 0.0)
                pagadores = int(r.get('pagadores') or 0)
                base[ym] = (ingresos / pagadores) if pagadores > 0 else 0.0
            result = dict(sorted(base.items()))
        except Exception:
            # Si falla el relleno, calcular ARPU directo de filas
            result = {}
            for r in rows:
                ym = str(r.get('ym'))
                ingresos = float(r.get('ingresos') or 0.0)
                pagadores = int(r.get('pagadores') or 0)
                result[ym] = (ingresos / pagadores) if pagadores > 0 else 0.0
        return {"arpu": result}
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)


# --- Check-in inverso por QR (público para socios) ---
@app.get("/checkin")
async def checkin_page(request: Request):
    """Página pública de check-in para socios: autenticación por DNI+teléfono y lector de QR."""
    theme_vars = read_theme_vars(static_dir / "style.css")
    autenticado = bool(request.session.get("checkin_user_id"))
    socio_info = {}
    if autenticado:
        try:
            db = _get_db()
            if db:
                user_id = int(request.session.get("checkin_user_id"))
                with db.get_connection_context() as conn:  # type: ignore
                    cur = conn.cursor()
                    cur.execute(
                        """
                        SELECT id,
                               COALESCE(nombre,'') AS nombre,
                               COALESCE(dni,'') AS dni,
                               COALESCE(telefono,'') AS telefono,
                               COALESCE(tipo_cuota,'') AS tipo_cuota,
                               fecha_proximo_vencimiento,
                               COALESCE(cuotas_vencidas, 0) AS cuotas_vencidas,
                               ultimo_pago,
                               LOWER(COALESCE(rol, 'socio')) AS rol
                        FROM usuarios WHERE id = %s LIMIT 1
                        """,
                        (user_id,)
                    )
                    row = cur.fetchone()
                    if row:
                        import re as _re
                        user_id_db = int(row[0]) if row[0] is not None else user_id
                        nombre = str(row[1] or "")
                        dni = str(row[2] or "")
                        telefono = str(row[3] or "")
                        tipo_cuota = str(row[4] or "")
                        fpv = row[5]
                        cuotas_vencidas = int(row[6] or 0)
                        ultimo_pago = row[7]
                        rol = (row[8] or "socio").lower()
                        exento = rol in ("profesor","owner","dueño","dueno")
                        digits = _re.sub(r"\D+", "", telefono)
                        masked = ("*" * max(len(digits) - 4, 4)) + (digits[-4:] if len(digits) >= 4 else digits)
                        try:
                            from datetime import date as _date
                            fpv_date = fpv.date() if hasattr(fpv, 'date') else fpv
                            dias_restantes = (fpv_date - _date.today()).days if fpv_date else None
                        except Exception:
                            dias_restantes = None
                        socio_info = {
                            "id": user_id_db,
                            "nombre": nombre,
                            "dni": dni,
                            "telefono_mask": masked,
                            "tipo_cuota": tipo_cuota,
                            "fecha_proximo_vencimiento": (fpv.date().isoformat() if hasattr(fpv, 'date') else (fpv.isoformat() if fpv else None)),
                            "cuotas_vencidas": cuotas_vencidas,
                            "ultimo_pago": (ultimo_pago.date().isoformat() if hasattr(ultimo_pago, 'date') else (ultimo_pago.isoformat() if ultimo_pago else None)),
                            "dias_restantes": dias_restantes,
                            "exento": exento,
                        }
        except Exception:
            pass
    ctx = {
        "request": request,
        "theme": theme_vars,
        "gym_name": get_gym_name("Gimnasio"),
        "logo_url": _resolve_logo_url(),
        "autenticado": autenticado,
        "socio": socio_info,
    }
    return templates.TemplateResponse("checkin.html", ctx)

@app.get("/checkin/logout")
async def checkin_logout(request: Request):
    # Logout específico del flujo de check-in: limpia solo la sesión de socio y vuelve a /checkin
    try:
        if "checkin_user_id" in request.session:
            request.session.pop("checkin_user_id", None)
    except Exception:
        request.session.clear()
    return RedirectResponse(url="/checkin", status_code=303)


@app.post("/checkin/auth")
async def checkin_auth(request: Request):
    """Autentica al socio por DNI y teléfono (ambos numéricos) y guarda la sesión de check-in."""
    rid = getattr(getattr(request, 'state', object()), 'request_id', '-')
    db = _get_db()
    if db is None:
        try:
            logging.error(f"/checkin/auth: DB=None, intentando _force_db_init rid={rid}")
        except Exception:
            pass
        db = _force_db_init()
        if db is None:
            try:
                logging.error(f"/checkin/auth: _force_db_init falló rid={rid}")
            except Exception:
                pass
            return JSONResponse({"success": False, "message": "Base de datos no disponible"}, status_code=500)
    # Obtener datos del cuerpo (form o JSON)
    content_type = request.headers.get("content-type", "")
    try:
        logging.info(f"/checkin/auth: content_type={content_type or '-'} rid={rid}")
    except Exception:
        pass
    if content_type.startswith("application/x-www-form-urlencoded"):
        data = await request.form()
    else:
        try:
            data = await request.json()
        except Exception:
            data = {}
    dni = str(data.get("dni", "")).strip()
    telefono = str(data.get("telefono", "")).strip()
    # Normalizar: solo dígitos
    import re
    dni_num = re.sub(r"\D+", "", dni)
    tel_num = re.sub(r"\D+", "", telefono)
    if not dni_num or not tel_num:
        try:
            logging.info(f"/checkin/auth: input inválido rid={rid} dni_len={len(dni)} tel_len={len(telefono)}")
        except Exception:
            pass
        return JSONResponse({"success": False, "message": "Ingrese DNI y teléfono válidos"}, status_code=400)
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            # Preparar variantes del teléfono para permitir coincidencias flexibles
            tel_like = f"%{tel_num}"
            tel_last10 = tel_num[-10:] if len(tel_num) >= 10 else tel_num
            try:
                logging.debug(f"/checkin/auth: consultando usuario por dni={dni_num} tel={tel_num} tel_last10={tel_last10} rid={rid}")
            except Exception:
                pass
            # Comparar por equivalencia de dígitos para soportar formatos (+54, 0-prefijo, espacios, guiones)
            cur.execute(
                """
                SELECT id FROM usuarios
                WHERE activo = TRUE
                  AND regexp_replace(CAST(dni AS TEXT), '\\D+', '', 'g') = %s
                  AND (
                    regexp_replace(CAST(telefono AS TEXT), '\\D+', '', 'g') = %s
                    OR regexp_replace(CAST(telefono AS TEXT), '\\D+', '', 'g') = '54' || %s
                    OR regexp_replace(CAST(telefono AS TEXT), '\\D+', '', 'g') = '0' || %s
                    OR regexp_replace(CAST(telefono AS TEXT), '\\D+', '', 'g') LIKE %s
                    OR right(regexp_replace(CAST(telefono AS TEXT), '\\D+', '', 'g'), 10) = %s
                  )
                ORDER BY id ASC
                LIMIT 1
                """,
                (dni_num, tel_num, tel_num, tel_num, tel_like, tel_last10)
            )
            row = cur.fetchone()
            if not row:
                # Evitar 404 para no confundir con ruta inexistente
                try:
                    logging.info(f"/checkin/auth: credenciales inválidas rid={rid}")
                except Exception:
                    pass
                return JSONResponse({"success": False, "message": "Credenciales inválidas"}, status_code=200)
            user_id = int(row[0])
            request.session["checkin_user_id"] = user_id
            try:
                logging.info(f"/checkin/auth: autenticado usuario_id={user_id} rid={rid}")
            except Exception:
                pass
            # Obtener estado de cuotas para notificar en el cliente
            cuotas_vencidas = 0
            fecha_proximo_vencimiento_iso = None
            ultimo_pago_iso = None
            tipo_cuota = None
            exento = False
            dias_restantes = None
            try:
                cur.execute(
                    """
                    SELECT COALESCE(cuotas_vencidas, 0),
                           fecha_proximo_vencimiento,
                           ultimo_pago,
                           COALESCE(tipo_cuota, ''),
                           LOWER(COALESCE(rol, 'socio'))
                    FROM usuarios WHERE id = %s
                    """,
                    (user_id,)
                )
                r2 = cur.fetchone()
                if r2:
                    cuotas_vencidas = int(r2[0] or 0)
                    fpv = r2[1]
                    up = r2[2]
                    tipo_cuota = str(r2[3] or '')
                    rol = str(r2[4] or 'socio').lower()
                    exento = rol in ('profesor', 'owner', 'dueño', 'dueno')
                    try:
                        from datetime import date as _date
                        fpv_date = fpv.date() if hasattr(fpv, 'date') else fpv
                        if fpv_date:
                            dias_restantes = (fpv_date - _date.today()).days
                        fecha_proximo_vencimiento_iso = fpv_date.isoformat() if fpv_date else None
                    except Exception:
                        fecha_proximo_vencimiento_iso = fpv.isoformat() if fpv else None
                        dias_restantes = None
                    try:
                        ultimo_pago_iso = (up.date().isoformat() if hasattr(up, 'date') else (up.isoformat() if up else None))
                    except Exception:
                        ultimo_pago_iso = None
            except Exception:
                pass
            return JSONResponse({
                "success": True,
                "message": "Autenticado",
                "usuario_id": user_id,
                "cuotas_vencidas": cuotas_vencidas,
                "fecha_proximo_vencimiento": fecha_proximo_vencimiento_iso,
                "dias_restantes": dias_restantes,
                "ultimo_pago": ultimo_pago_iso,
                "tipo_cuota": tipo_cuota,
                "exento": exento
            })
    except Exception as e:
        try:
            logging.exception(f"Error en /checkin/auth rid={rid}")
        except Exception:
            pass
        return JSONResponse({"success": False, "message": str(e)}, status_code=500)


@app.post("/api/checkin/validate")
async def api_checkin_validate(request: Request):
    """Valida el token escaneado y registra asistencia si corresponde."""
    rid = getattr(getattr(request, 'state', object()), 'request_id', '-')
    db = _get_db()
    if db is None:
        try:
            logging.error(f"/api/checkin/validate: DB=None, intentando _force_db_init rid={rid}")
        except Exception:
            pass
        db = _force_db_init()
        if db is None:
            return JSONResponse({"success": False, "message": "Base de datos no disponible"}, status_code=500)
    guard = _circuit_guard_json(db, "/api/checkin/validate")
    if guard:
        return guard
    try:
        data = await request.json()
        token = str(data.get("token", "")).strip()
        socio_id = request.session.get("checkin_user_id")
        try:
            masked_token = ("***" + token[-4:]) if token else ""
            logging.info(f"/api/checkin/validate: recibido token={masked_token} socio_id={socio_id} rid={rid}")
        except Exception:
            pass
        if not socio_id:
            return JSONResponse({"success": False, "message": "Sesión de socio no encontrada"}, status_code=401)
        # Orden de parámetros: (token, socio_id)
        ok, msg = db.validar_token_y_registrar_asistencia(token, int(socio_id))  # type: ignore
        status = 200 if ok else 400
        # Señal explícita: marcar 'used' en checkin_pending para robustecer el polling del escritorio
        if ok:
            try:
                with db.get_connection_context() as conn:  # type: ignore
                    cur = conn.cursor()
                    cur.execute("UPDATE checkin_pending SET used = TRUE WHERE token = %s", (token,))
                    conn.commit()
            except Exception:
                pass
        try:
            logging.info(f"/api/checkin/validate: resultado ok={ok} msg='{msg}' rid={rid}")
        except Exception:
            pass
        return JSONResponse({"success": ok, "message": msg}, status_code=status)
    except Exception as e:
        try:
            logging.exception(f"Error en /api/checkin/validate rid={rid}")
        except Exception:
            pass
        return JSONResponse({"success": False, "message": str(e)}, status_code=500)


@app.get("/api/checkin/token_status")
async def api_checkin_token_status(request: Request):
    """Consulta el estado de un token: { exists, used, expired }.

    Criterio de 'used': se considera usado si el flag en checkin_pending es TRUE
    o si ya existe una asistencia para el usuario en la fecha actual.
    Esto hace el polling del escritorio más robusto ante posibles desincronizaciones.
    """
    rid = getattr(getattr(request, 'state', object()), 'request_id', '-')
    db = _get_db()
    if db is None:
        try:
            logging.error(f"/api/checkin/token_status: DB=None, intentando _force_db_init rid={rid}")
        except Exception:
            pass
        db = _force_db_init()
        if db is None:
            return JSONResponse({"exists": False, "used": False, "expired": True}, status_code=200)
    import datetime as _dt
    token = str(request.query_params.get("token", "")).strip()
    try:
        masked_token = ("***" + token[-4:]) if token else ""
        logging.info(f"/api/checkin/token_status: token={masked_token} rid={rid}")
    except Exception:
        pass
    if not token:
        return JSONResponse({"exists": False, "used": False, "expired": True}, status_code=200)
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            # Obtener también usuario_id para verificar asistencia del día
            cur.execute("SELECT usuario_id, used, expires_at FROM checkin_pending WHERE token = %s LIMIT 1", (token,))
            row = cur.fetchone()
            if not row:
                return JSONResponse({"exists": False, "used": False, "expired": True}, status_code=200)

            used_flag = bool(row.get("used") or False)
            expires_at = row.get("expires_at")
            now = _dt.datetime.utcnow()
            expired = bool(expires_at and expires_at < now)

            usuario_id = row.get("usuario_id")
            attended_today = False
            try:
                if usuario_id is not None:
                    cur2 = conn.cursor()
                    cur2.execute(
                        "SELECT 1 FROM asistencias WHERE usuario_id = %s AND fecha::date = CURRENT_DATE LIMIT 1",
                        (int(usuario_id),)
                    )
                    attended_today = cur2.fetchone() is not None
            except Exception:
                attended_today = False

            used = bool(used_flag or attended_today)

            # Log a nivel INFO para que aparezca en los logs por defecto
            try:
                logging.info(
                    f"/api/checkin/token_status: usuario_id={usuario_id} used_flag={used_flag} attended_today={attended_today} expired={expired} rid={rid}"
                )
            except Exception:
                pass

            return JSONResponse({"exists": True, "used": used, "expired": expired}, status_code=200)
    except Exception as e:
        try:
            logging.exception(f"Error en /api/checkin/token_status rid={rid}")
        except Exception:
            pass
        return JSONResponse({"exists": False, "used": False, "expired": True, "error": str(e)}, status_code=200)

@app.post("/api/checkin/create_token")
async def api_checkin_create_token(request: Request, _=Depends(require_gestion_access)):
    rid = getattr(getattr(request,'state',object()), 'request_id', '-')
    db = _get_db()
    if db is None:
        try:
            logging.error(f"/api/checkin/create_token: DB=None rid={rid}")
        except Exception:
            pass
        db = _force_db_init()
        if db is None:
            raise HTTPException(status_code=500, detail="DB no disponible")
    payload = await request.json()
    usuario_id = int(payload.get("usuario_id") or 0)
    expires_minutes = int(payload.get("expires_minutes") or 5)
    if not usuario_id:
        raise HTTPException(status_code=400, detail="usuario_id es requerido")
    token = secrets.token_urlsafe(12)
    try:
        db.crear_checkin_token(usuario_id, token, expires_minutes)  # type: ignore
        try:
            logging.info(f"/api/checkin/create_token: usuario_id={usuario_id} token=***{token[-4:]} expires={expires_minutes}m rid={rid}")
        except Exception:
            pass
        return JSONResponse({"success": True, "token": token, "expires_minutes": expires_minutes}, status_code=200)
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/asistencias/registrar")
async def api_asistencias_registrar(request: Request, _=Depends(require_gestion_access)):
    rid = getattr(getattr(request,'state',object()), 'request_id', '-')
    db = _get_db()
    if db is None:
        db = _force_db_init()
        if db is None:
            raise HTTPException(status_code=500, detail="DB no disponible")
    payload = await request.json()
    usuario_id = int(payload.get("usuario_id") or 0)
    fecha_str = str(payload.get("fecha") or "").strip()
    if not usuario_id:
        raise HTTPException(status_code=400, detail="usuario_id es requerido")
    from datetime import date
    fecha = None
    try:
        if fecha_str:
            parts = fecha_str.split("-")
            if len(parts) == 3:
                fecha = date(int(parts[0]), int(parts[1]), int(parts[2]))
    except Exception:
        fecha = None
    try:
        asistencia_id = db.registrar_asistencia(usuario_id, fecha)  # type: ignore
        try:
            logging.info(f"/api/asistencias/registrar: usuario_id={usuario_id} fecha={fecha} rid={rid}")
        except Exception:
            pass
        return JSONResponse({"success": True, "asistencia_id": asistencia_id}, status_code=200)
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except ValueError as e:
        try:
            logging.info(f"/api/asistencias/registrar: ya existía asistencia usuario_id={usuario_id} rid={rid}")
        except Exception:
            pass
        return JSONResponse({"success": True, "message": str(e)}, status_code=200)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.delete("/api/asistencias/eliminar")
async def api_asistencias_eliminar(request: Request, _=Depends(require_gestion_access)):
    rid = getattr(getattr(request,'state',object()), 'request_id', '-')
    db = _get_db()
    if db is None:
        db = _force_db_init()
        if db is None:
            raise HTTPException(status_code=500, detail="DB no disponible")
    payload = await request.json()
    usuario_id = int(payload.get("usuario_id") or 0)
    fecha_str = str(payload.get("fecha") or "").strip()
    if not usuario_id:
        raise HTTPException(status_code=400, detail="usuario_id es requerido")
    from datetime import date
    fecha = None
    try:
        if fecha_str:
            parts = fecha_str.split("-")
            if len(parts) == 3:
                fecha = date(int(parts[0]), int(parts[1]), int(parts[2]))
        else:
            fecha = date.today()
    except Exception:
        fecha = None
    try:
        db.eliminar_asistencia(usuario_id, fecha)  # type: ignore
        try:
            logging.info(f"/api/asistencias/eliminar: usuario_id={usuario_id} fecha={fecha} rid={rid}")
        except Exception:
            pass
        return JSONResponse({"success": True}, status_code=200)
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/asistencia_30d")
async def api_asistencia_30d(request: Request, _=Depends(require_owner)):
    db = _get_db()
    series: Dict[str, int] = {}
    if db is None:
        return series
    guard = _circuit_guard_json(db, "/api/asistencia_30d")
    if guard:
        return guard
    try:
        start = request.query_params.get("start")
        end = request.query_params.get("end")
        # Delegar a backend: nueva función agregada para consistencia de esquema
        if start and end:
            data = db.obtener_asistencias_por_rango_diario(start, end)  # type: ignore
        else:
            data = db.obtener_asistencias_por_dia(30)  # type: ignore
        for d, c in (data or []):
            series[str(d)] = int(c or 0)
        # Rellenar días faltantes con 0 para últimos 30 días
        try:
            from datetime import date, timedelta
            base: Dict[str, int] = {}
            hoy = date.today()
            for i in range(29, -1, -1):
                dia = hoy - timedelta(days=i)
                clave = dia.strftime("%Y-%m-%d")
                base[clave] = 0
            base.update(series or {})
            series = dict(sorted(base.items()))
        except Exception:
            pass
        return series
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/asistencia_por_hora_30d")
async def api_asistencia_por_hora_30d(request: Request, _=Depends(require_owner)):
    db = _get_db()
    series: Dict[str, int] = {}
    if db is None:
        return series
    try:
        start = request.query_params.get("start")
        end = request.query_params.get("end")
        # Delegar a backend: función agregada que agrupa por hora usando hora_registro
        if start and end:
            data = db.obtener_asistencias_por_hora_rango(start, end)  # type: ignore
        else:
            data = db.obtener_asistencias_por_hora(30)  # type: ignore
        for h, c in (data or []):
            label = f"{int(h):02d}:00" if isinstance(h, (int, float)) else str(h)
            series[label] = int(c or 0)
        # Completar horas 00..23 con 0 si faltan
        try:
            base: Dict[str, int] = {f"{i:02d}:00": 0 for i in range(0, 24)}
            base.update(series or {})
            series = dict(sorted(base.items()))
        except Exception:
            pass
        return series
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/activos_inactivos")
async def api_activos_inactivos(_=Depends(require_owner)):
    db = _get_db()
    if db is None:
        return {"Activos": 0, "Inactivos": 0}
    guard = _circuit_guard_json(db, "/api/activos_inactivos")
    if guard:
        return guard
    try:
        # Delegar al backend validado para conteo
        return db.obtener_conteo_activos_inactivos()  # type: ignore
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/tipos_cuota")
async def api_tipos_cuota(_=Depends(require_owner)):
    db = _get_db()
    dist: Dict[str, int] = {}
    if db is None:
        return dist
    try:
        # Contar usuarios activos por tipo de cuota (roles socio/miembro) de forma robusta:
        # - Soporta que usuarios.tipo_cuota sea nombre o id (distintos esquemas)
        # - Evita errores de tipo usando CAST a texto para comparar con id
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(
                """
                SELECT COALESCE(tc.nombre, CAST(u.tipo_cuota AS TEXT), 'Sin tipo') AS tipo,
                       COUNT(*) AS cantidad
                FROM usuarios u
                LEFT JOIN tipos_cuota tc 
                  ON (CAST(u.tipo_cuota AS TEXT) = tc.nombre) 
                     OR (CAST(u.tipo_cuota AS TEXT) = CAST(tc.id AS TEXT))
                WHERE u.activo = true
                  AND LOWER(COALESCE(u.rol,'')) IN ('socio','profesor')
                GROUP BY COALESCE(tc.nombre, CAST(u.tipo_cuota AS TEXT), 'Sin tipo')
                ORDER BY 1
                """
            )
            def normalize_label(label: str) -> str:
                s = (label or 'Sin tipo').strip().lower()
                replacements = {
                    'estandar': 'Estándar', 'estándar': 'Estándar', 'standard': 'Estándar',
                    'sin tipo': 'Sin tipo',
                    'mensualidad': 'Mensual', 'mensual': 'Mensual',
                }
                return replacements.get(s, s.title())
            for r in cur.fetchall() or []:
                raw = (r.get('tipo') or 'Sin tipo')
                nombre = normalize_label(str(raw))
                # Sumar en la etiqueta normalizada para unificar sin duplicados
                dist[nombre] = dist.get(nombre, 0) + int(r.get('cantidad') or 0)
        return dist
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/kpis_avanzados")
async def api_kpis_avanzados(_=Depends(require_owner)):
    db = _get_db()
    kpis: Dict[str, Any] = {
        'retention_rate': 0.0,
        'churn_rate': 0.0,
        'avg_attendance': 0.0,
        'peak_hour': 'N/A',
        'revenue_growth': 0.0,
        'weekly_active_users': 0,
        'payment_rate': 0.0,
        'ltv_12m': 0.0,
        'arpa': 0.0,
        'mrr': 0.0,
    }
    if db is None:
        return kpis
    guard = _circuit_guard_json(db, "/api/kpis_avanzados")
    if guard:
        return guard
    try:
        # Fechas clave y métricas utilizando métodos existentes del backend
        from datetime import timedelta
        hoy = datetime.now().date()
        primer_dia_mes = hoy.replace(day=1)
        fin_mes_anterior = primer_dia_mes - timedelta(days=1)
        primer_dia_mes_anterior = fin_mes_anterior.replace(day=1)

        # Ingresos del mes actual y anterior
        ing_act = float(db.calcular_ingresos_totales(primer_dia_mes, hoy) or 0.0)  # type: ignore
        ing_ant = float(db.calcular_ingresos_totales(primer_dia_mes_anterior, fin_mes_anterior) or 0.0)  # type: ignore
        kpis['revenue_growth'] = ((ing_act - ing_ant) / ing_ant * 100.0) if ing_ant > 0 else 0.0

        # Pagadores en el mes actual (roles socio/profesor, robusto a fecha/año-mes)
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            cur.execute(
                """
                SELECT COUNT(DISTINCT u.id) AS pagadores
                FROM usuarios u
                JOIN pagos p ON p.usuario_id = u.id
                WHERE u.activo = TRUE
                  AND LOWER(COALESCE(u.rol,'')) IN ('socio','profesor')
                  AND date_trunc('month', COALESCE(p.fecha_pago, make_date(p.año, p.mes, 1))) = date_trunc('month', CURRENT_DATE)
                """
            )
            row = cur.fetchone()
            pagadores_mes = int(row[0] if row and row[0] is not None else 0)

        # Lista de pagos del mes actual para métricas de retención
        pagos_mes = db.obtener_pagos_por_fecha(primer_dia_mes, hoy) or []  # type: ignore

        # Usuarios activos totales desde KPIs generales
        kpis_generales = db.obtener_kpis_generales() or {}
        activos_total = int(kpis_generales.get('total_activos', 0) or 0)
        kpis['payment_rate'] = (pagadores_mes / activos_total * 100.0) if activos_total > 0 else 0.0

        # ARPA y MRR
        kpis['mrr'] = ing_act
        kpis['arpa'] = (ing_act / pagadores_mes) if pagadores_mes > 0 else 0.0

        # LTV 12 meses
        inicio_12m = hoy - timedelta(days=365)
        pagos_12m = db.obtener_pagos_por_fecha(inicio_12m, hoy) or []  # type: ignore
        total_12m = sum(float(p.get('monto', 0) or 0.0) for p in pagos_12m if isinstance(p, dict))
        usuarios_12m = len({p.get('usuario_id') for p in pagos_12m if isinstance(p, dict)})
        kpis['ltv_12m'] = (total_12m / usuarios_12m) if usuarios_12m > 0 else 0.0

        # Retención vs mes anterior
        pagos_mes_ant = db.obtener_pagos_por_fecha(primer_dia_mes_anterior, fin_mes_anterior) or []  # type: ignore
        set_act = {p.get('usuario_id') for p in pagos_mes if isinstance(p, dict)}
        set_ant = {p.get('usuario_id') for p in pagos_mes_ant if isinstance(p, dict)}
        kpis['retention_rate'] = (len(set_act & set_ant) / len(set_ant) * 100.0) if len(set_ant) > 0 else 0.0
        kpis['churn_rate'] = 100.0 - kpis['retention_rate']

        # Asistencias últimos 30 días para promedio y hora pico
        asist_30 = db.obtener_asistencias_por_fecha_limite(hoy - timedelta(days=30)) or []  # type: ignore
        from collections import Counter
        dias_conteo = Counter()
        horas_conteo = Counter()
        for a in asist_30:
            f = a.get('fecha')
            h = a.get('hora_registro')
            if hasattr(f, 'date'):
                f = f.date()
            dias_conteo[f] += 1
            if h is not None:
                try:
                    hh = h.hour if hasattr(h, 'hour') else int(str(h).split(':')[0])
                    horas_conteo[hh] += 1
                except Exception:
                    pass
        kpis['avg_attendance'] = (sum(dias_conteo.values()) / len(dias_conteo)) if len(dias_conteo) > 0 else 0.0
        if len(horas_conteo) > 0:
            peak_h = max(horas_conteo.items(), key=lambda kv: kv[1])[0]
            kpis['peak_hour'] = f"{int(peak_h):02d}:00"
        else:
            kpis['peak_hour'] = 'N/A'

        # Usuarios activos semanales por asistencias últimos 7 días
        asist_7 = db.obtener_asistencias_por_fecha_limite(hoy - timedelta(days=7)) or []  # type: ignore
        kpis['weekly_active_users'] = len({a.get('usuario_id') for a in asist_7})

        return kpis
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/cohort_retencion_6m")
async def api_cohort_retencion_6m(_=Depends(require_owner)):
    """Retención por cohorte de registro (últimos 6 meses): % del cohorte que pagó este mes."""
    db = _get_db()
    result: Dict[str, float] = {}
    if db is None:
        return result
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            cur.execute(
                """
                SELECT to_char(date_trunc('month', u.fecha_registro), 'YYYY-MM') AS cohorte,
                       COUNT(*) AS tam,
                       SUM(CASE WHEN EXISTS (
                           SELECT 1 FROM pagos p
                           WHERE p.usuario_id = u.id
                             AND date_trunc('month', COALESCE(p.fecha_pago, make_date(p.año, p.mes, 1))) = date_trunc('month', CURRENT_DATE)
                       ) THEN 1 ELSE 0 END) AS retenidos
                FROM usuarios u
                WHERE u.fecha_registro IS NOT NULL
                  AND date_trunc('month', u.fecha_registro) >= date_trunc('month', CURRENT_DATE) - INTERVAL '5 months'
                  AND u.activo = true
                  AND LOWER(COALESCE(u.rol,'')) NOT IN ('dueño','dueno','owner','administrador','admin')
                GROUP BY 1
                ORDER BY 1
                """
            )
            for cohorte, tam, retenidos in cur.fetchall():
                tam = int(tam or 0); retenidos = int(retenidos or 0)
                result[str(cohorte)] = (retenidos / tam * 100.0) if tam > 0 else 0.0
        return result
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/cohort_retencion_heatmap")
async def api_cohort_retencion_heatmap(months: int = 6, _=Depends(require_owner)):
    """Matriz de retención por cohorte (registro) vs mes de pago.
    Devuelve { months: [...], cohorts: [...], matrix: [[%...], ...] } para últimos N meses.
    """
    db = _get_db()
    res: Dict[str, Any] = { 'months': [], 'cohorts': [], 'matrix': [] }
    if db is None:
        return res
    months = max(1, min(months, 24))
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            # Lista de meses (YYYY-MM) para los últimos N meses
            cur.execute("SELECT to_char(date_trunc('month', CURRENT_DATE) - s * INTERVAL '1 month', 'YYYY-MM') FROM generate_series(0,%s) s ORDER BY 1", (months-1,))
            months_list = [row[0] for row in cur.fetchall()]
            cohorts = months_list
            matrix: list[list[float]] = []
            for coh in cohorts:
                # Tamaño de cohorte
                cur.execute("""
                    SELECT COUNT(*) FROM usuarios u
                    WHERE u.activo = true AND u.fecha_registro IS NOT NULL
                      AND LOWER(COALESCE(u.rol,'')) NOT IN ('dueño','dueno','owner','administrador','admin')
                      AND to_char(date_trunc('month', u.fecha_registro), 'YYYY-MM') = %s
                """, (coh,))
                cohort_size = int(cur.fetchone()[0] or 0)
                row: list[float] = []
                for m in months_list:
                    # Usuarios del cohorte que pagaron en mes m
                    cur.execute("""
                        SELECT COUNT(*) FROM usuarios u
                        WHERE u.activo = true AND u.fecha_registro IS NOT NULL
                          AND LOWER(COALESCE(u.rol,'')) NOT IN ('dueño','dueno','owner','administrador','admin')
                          AND to_char(date_trunc('month', u.fecha_registro), 'YYYY-MM') = %s
                          AND EXISTS (
                            SELECT 1 FROM pagos p
                            WHERE p.usuario_id = u.id
                              AND to_char(date_trunc('month', COALESCE(p.fecha_pago, make_date(p.año, p.mes, 1))), 'YYYY-MM') = %s
                          )
                    """, (coh, m))
                    retained = int(cur.fetchone()[0] or 0)
                    pct = (retained / cohort_size * 100.0) if cohort_size > 0 else 0.0
                    row.append(pct)
                matrix.append(row)
            return {"months": months_list, "cohorts": cohorts, "matrix": matrix}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/arpa_por_tipo_cuota")
async def api_arpa_por_tipo_cuota(_=Depends(require_owner)):
    """ARPA por tipo de cuota para el mes actual (monto total / pagadores del tipo)."""
    db = _get_db()
    dist: Dict[str, float] = {}
    if db is None:
        return dist
    guard = _circuit_guard_json(db, "/api/arpa_por_tipo_cuota")
    if guard:
        return guard
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            def norm(label: str) -> str:
                s = (label or '—').strip().lower()
                replacements = {
                    'estandar': 'Estándar', 'estándar': 'Estándar', 'standard': 'Estándar',
                    'estudiante': 'Estudiante', 'student': 'Estudiante',
                    'funcional': 'Funcional', 'functional': 'Funcional',
                }
                return replacements.get(s, s.title())
            cur.execute(
                """
                SELECT COALESCE(tc.nombre, CAST(u.tipo_cuota AS TEXT), '—') AS tipo,
                       COALESCE(SUM(CASE WHEN date_trunc('month',
                           CASE
                               WHEN p.fecha_pago IS NOT NULL THEN p.fecha_pago
                               WHEN p.año IS NOT NULL AND p.mes IS NOT NULL THEN make_date(p.año, p.mes, 1)
                               ELSE NULL
                           END
                       ) = date_trunc('month', CURRENT_DATE) THEN p.monto END), 0) AS monto_mes,
                       COUNT(DISTINCT CASE WHEN date_trunc('month',
                           CASE
                               WHEN p.fecha_pago IS NOT NULL THEN p.fecha_pago
                               WHEN p.año IS NOT NULL AND p.mes IS NOT NULL THEN make_date(p.año, p.mes, 1)
                               ELSE NULL
                           END
                       ) = date_trunc('month', CURRENT_DATE) THEN u.id END) AS pagadores_mes
                FROM usuarios u
                LEFT JOIN pagos p ON p.usuario_id = u.id
                -- Emparejar robustamente por nombre o id como texto
                LEFT JOIN tipos_cuota tc 
                  ON (CAST(u.tipo_cuota AS TEXT) = tc.nombre)
                  OR (CAST(u.tipo_cuota AS TEXT) = CAST(tc.id AS TEXT))
                WHERE u.activo = true
                  AND LOWER(COALESCE(u.rol,'')) NOT IN ('dueño','dueno','owner','administrador','admin')
                GROUP BY 1
                """
            )
            for tipo, monto_mes, pagadores_mes in cur.fetchall():
                key = norm(str(tipo))
                # Excluir SOLO "estandar" (sin acento)
                if key.strip().lower() == 'estandar':
                    continue
                pagadores = int(pagadores_mes or 0)
                total = float(monto_mes or 0.0)
                dist[key] = (total / pagadores) if pagadores > 0 else 0.0
        return dist
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/payment_status_dist")
async def api_payment_status_dist(_=Depends(require_owner)):
    db = _get_db()
    dist = {"no_payments": 0, "up_to_date": 0, "pending": 0, "overdue": 0}
    if db is None:
        return dist
    guard = _circuit_guard_json(db, "/api/payment_status_dist")
    if guard:
        return guard
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(
                """
                WITH last_pay AS (
                  SELECT u.id AS usuario_id,
                         MAX(p.fecha_pago::timestamp) AS last_date
                  FROM usuarios u
                  LEFT JOIN pagos p ON p.usuario_id = u.id
                  WHERE u.activo = true
                    AND LOWER(COALESCE(u.rol,'')) IN ('socio','profesor')
                  GROUP BY u.id
                )
                SELECT
                  SUM(CASE WHEN last_date IS NULL THEN 1 ELSE 0 END) AS no_payments,
                  SUM(CASE WHEN last_date IS NOT NULL AND date_trunc('month', last_date) = date_trunc('month', CURRENT_DATE::timestamp) THEN 1 ELSE 0 END) AS up_to_date,
                  SUM(CASE WHEN last_date IS NOT NULL AND date_trunc('month', last_date) < date_trunc('month', CURRENT_DATE::timestamp) AND (CURRENT_DATE - last_date::date) <= 30 THEN 1 ELSE 0 END) AS pending,
                  SUM(CASE WHEN last_date IS NOT NULL AND (CURRENT_DATE - last_date::date) > 30 THEN 1 ELSE 0 END) AS overdue
                FROM last_pay
                """
            )
            row = cur.fetchone() or {}
            dist["no_payments"] = int(row.get("no_payments") or 0)
            dist["up_to_date"] = int(row.get("up_to_date") or 0)
            dist["pending"] = int(row.get("pending") or 0)
            dist["overdue"] = int(row.get("overdue") or 0)
        return dist
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)


# --- Tablas detalladas para subpestañas ---

@app.get("/api/pagos_detalle")
async def api_pagos_detalle(request: Request, _=Depends(require_gestion_access)):
    """Listado detallado de pagos con filtros por fecha, búsqueda y paginación.

    - Filtros: start (YYYY-MM-DD), end (YYYY-MM-DD), q (nombre/dni/metodo/concepto)
    - Paginación: limit, offset
    """
    db = _get_db()
    if db is None:
        return {"count": 0, "items": []}
    guard = _circuit_guard_json(db, "/api/pagos_detalle")
    if guard:
        return guard
    try:
        start = request.query_params.get("start")
        end = request.query_params.get("end")
        q = request.query_params.get("q")
        limit_q = request.query_params.get("limit")
        offset_q = request.query_params.get("offset")
        try:
            limit = int(limit_q) if (limit_q and str(limit_q).isdigit()) else 50
        except Exception:
            limit = 50
        try:
            offset = int(offset_q) if (offset_q and str(offset_q).isdigit()) else 0
        except Exception:
            offset = 0
        # Normalizar vacíos
        if start and isinstance(start, str) and start.strip() == "":
            start = None
        if end and isinstance(end, str) and end.strip() == "":
            end = None

        rows = db.obtener_pagos_por_fecha(start, end)  # type: ignore
        items = rows
        if q and isinstance(q, str) and q.strip() != "":
            ql = q.lower()
            def _match(r: Dict[str, Any]) -> bool:
                try:
                    nombre = str(r.get("usuario_nombre") or r.get("nombre") or "").lower()
                    dni = str(r.get("dni") or "").lower()
                    metodo = str(r.get("metodo_pago") or r.get("metodo") or "").lower()
                    concepto = str(r.get("concepto_pago") or r.get("concepto") or "").lower()
                    return (ql in nombre) or (ql in dni) or (ql in metodo) or (ql in concepto)
                except Exception:
                    return False
            items = [r for r in rows if _match(r)]
        total = len(items)
        sliced = items[offset:offset+limit]
        return {"count": total, "items": sliced}
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

# --- Detalle de pago por ID ---
@app.get("/api/pagos/{pago_id}")
async def api_pago_resumen(pago_id: int, _=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            # Consulta única con JOINs y agregación JSON para detalles
            cur.execute(
                """
                SELECT 
                    -- Pago (columnas explícitas)
                    p.id AS id,
                    p.usuario_id AS usuario_id,
                    p.monto AS monto,
                    p.mes AS mes,
                    p.año AS año,
                    p.fecha_pago AS fecha_pago,
                    p.metodo_pago_id AS metodo_pago_id,
                    -- Usuario
                    u.id AS usuario_id_ref,
                    u.nombre AS usuario_nombre,
                    u.dni AS dni,
                    -- Agregados de detalles
                    COALESCE(SUM(COALESCE(pd.cantidad,1) * COALESCE(pd.precio_unitario,0)), 0) AS total_detalles,
                    JSON_AGG(
                        JSON_BUILD_OBJECT(
                            'id', pd.id,
                            'pago_id', pd.pago_id,
                            'concepto_nombre', COALESCE(cp.nombre, pd.descripcion),
                            'cantidad', COALESCE(pd.cantidad, 1),
                            'precio_unitario', COALESCE(pd.precio_unitario, 0),
                            'subtotal', COALESCE(pd.subtotal, COALESCE(pd.cantidad,1) * COALESCE(pd.precio_unitario,0))
                        )
                    ) FILTER (WHERE pd.id IS NOT NULL) AS detalles
                FROM pagos p
                JOIN usuarios u ON u.id = p.usuario_id
                LEFT JOIN pago_detalles pd ON pd.pago_id = p.id
                LEFT JOIN conceptos_pago cp ON cp.id = pd.concepto_id
                WHERE p.id = %s
                GROUP BY p.id, u.id
                """,
                (pago_id,)
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Pago no encontrado")
        # Separar campos: mantener interfaz {pago, detalles, total_detalles}
        total_detalles = float(row.get("total_detalles") or 0)
        detalles = row.get("detalles") or []
        # Construir 'pago' sin las agregaciones JSON
        pago = {k: v for k, v in row.items() if k not in ("detalles", "total_detalles")}
        return {"pago": pago, "detalles": detalles, "total_detalles": total_detalles}
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

# --- Recibo PDF por pago ---
@app.get("/api/pagos/{pago_id}/recibo.pdf")
async def api_pago_recibo_pdf(pago_id: int, request: Request, _=Depends(require_gestion_access)):
    pm = _get_pm()
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    if pm is None:
        raise HTTPException(status_code=503, detail="PaymentManager no disponible")
    try:
        # Obtener pago y usuario
        pago = pm.obtener_pago(int(pago_id))
        if not pago:
            raise HTTPException(status_code=404, detail="Pago no encontrado")
        usuario = db.obtener_usuario_por_id(int(getattr(pago, 'usuario_id', 0)))  # type: ignore
        if not usuario:
            raise HTTPException(status_code=404, detail="Usuario del pago no encontrado")

        # Detalles del pago (si existen) y totales con comisión según método
        try:
            detalles = pm.obtener_detalles_pago(int(pago_id))
        except Exception:
            detalles = []
        subtotal = 0.0
        try:
            subtotal = sum(float(getattr(d, 'subtotal', 0.0) or 0.0) for d in (detalles or [])) if detalles else float(getattr(pago, 'monto', 0.0) or 0.0)
        except Exception:
            subtotal = float(getattr(pago, 'monto', 0.0) or 0.0)
        metodo_id = getattr(pago, 'metodo_pago_id', None)
        try:
            totales = pm.calcular_total_con_comision(subtotal, metodo_id)
        except Exception:
            totales = {"subtotal": subtotal, "comision": 0.0, "total": subtotal}

        # Determinar modo de operación y posible override de número
        qp = request.query_params
        preview_mode = False
        try:
            qpv = qp.get("preview")
            preview_mode = True if (qpv and str(qpv).lower() in ("1","true","yes")) else False
        except Exception:
            preview_mode = False
        numero_override = None
        try:
            nraw = qp.get("numero")
            numero_override = (str(nraw).strip() or None) if (nraw is not None) else None
        except Exception:
            numero_override = None

        # Campos opcionales para personalizar el recibo
        obs_text = None
        try:
            oraw = qp.get("observaciones")
            obs_text = (str(oraw).strip() or None) if (oraw is not None) else None
        except Exception:
            obs_text = None
        emitido_por = None
        try:
            eraw = qp.get("emitido_por")
            emitido_por = (str(eraw).strip() or None) if (eraw is not None) else None
        except Exception:
            emitido_por = None

        # Overrides avanzados: encabezado, gimnasio, fecha, método, destinatario, visibilidad, items y totales
        def _qp_bool(val):
            try:
                s = str(val).strip().lower()
            except Exception:
                return None
            if s in ("1","true","yes","on"): return True
            if s in ("0","false","no","off"): return False
            return None

        titulo = None
        try:
            titulo = (str(qp.get("titulo")).strip() or None) if (qp.get("titulo") is not None) else None
        except Exception:
            titulo = None
        gym_name_override = None
        gym_address_override = None
        try:
            gym_name_override = (str(qp.get("gym_name")).strip() or None) if (qp.get("gym_name") is not None) else None
        except Exception:
            gym_name_override = None
        try:
            gym_address_override = (str(qp.get("gym_address")).strip() or None) if (qp.get("gym_address") is not None) else None
        except Exception:
            gym_address_override = None

        fecha_emision_disp = None
        try:
            fraw = qp.get("fecha")
            if fraw is not None:
                s = str(fraw).strip()
                try:
                    if "/" in s:
                        dt = datetime.strptime(s, "%d/%m/%Y")
                    else:
                        dt = datetime.strptime(s, "%Y-%m-%d")
                    fecha_emision_disp = dt.strftime("%d/%m/%Y")
                except Exception:
                    # Si no parsea, usar tal cual
                    fecha_emision_disp = s or None
        except Exception:
            fecha_emision_disp = None

        metodo_override = None
        try:
            metodo_override = (str(qp.get("metodo")).strip() or None) if (qp.get("metodo") is not None) else None
        except Exception:
            metodo_override = None

        # Tipo de cuota y periodo (opcional)
        tipo_cuota_override = None
        try:
            tipo_cuota_override = (str(qp.get("tipo_cuota")).strip() or None) if (qp.get("tipo_cuota") is not None) else None
        except Exception:
            tipo_cuota_override = None
        periodo_override = None
        try:
            periodo_override = (str(qp.get("periodo")).strip() or None) if (qp.get("periodo") is not None) else None
        except Exception:
            periodo_override = None

        usuario_nombre_override = None
        usuario_dni_override = None
        try:
            usuario_nombre_override = (str(qp.get("usuario_nombre")).strip() or None) if (qp.get("usuario_nombre") is not None) else None
        except Exception:
            usuario_nombre_override = None
        try:
            usuario_dni_override = (str(qp.get("usuario_dni")).strip() or None) if (qp.get("usuario_dni") is not None) else None
        except Exception:
            usuario_dni_override = None

        mostrar_logo = _qp_bool(qp.get("mostrar_logo"))
        mostrar_metodo = _qp_bool(qp.get("mostrar_metodo"))
        mostrar_dni = _qp_bool(qp.get("mostrar_dni"))

        detalles_override = None
        try:
            iraw = qp.get("items")
            if iraw is not None:
                obj = json.loads(str(iraw))
                if isinstance(obj, list):
                    detalles_override = obj
        except Exception:
            detalles_override = None

        # Totales override si vienen (mantener existentes para valores faltantes)
        try:
            sub_o = qp.get("subtotal")
            com_o = qp.get("comision")
            tot_o = qp.get("total")
            if sub_o is not None or com_o is not None or tot_o is not None:
                s = float(sub_o) if (sub_o is not None and str(sub_o).strip() != "") else float(totales.get("subtotal", 0.0))
                c = float(com_o) if (com_o is not None and str(com_o).strip() != "") else float(totales.get("comision", 0.0))
                t = float(tot_o) if (tot_o is not None and str(tot_o).strip() != "") else float(totales.get("total", s + c))
                totales = {"subtotal": s, "comision": c, "total": t}
        except Exception:
            pass

        # Intentar reutilizar comprobante existente o crear uno nuevo para numeración
        numero_comprobante = None
        comprobante_id = None
        try:
            with db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur.execute(
                    """
                    SELECT id, numero_comprobante
                    FROM comprobantes_pago
                    WHERE pago_id = %s AND estado = 'emitido'
                    ORDER BY fecha_creacion DESC
                    LIMIT 1
                    """,
                    (int(pago_id),)
                )
                row = cur.fetchone()
                if row:
                    comprobante_id = int(row.get("id"))
                    numero_comprobante = row.get("numero_comprobante")
        except Exception:
            numero_comprobante = None

        # En modo preview no creamos ni reservamos número; usamos override si viene
        if preview_mode:
            if numero_override:
                numero_comprobante = numero_override
        else:
            try:
                if not numero_comprobante:
                    # Crear y reservar número de comprobante de forma atómica
                    comprobante_id = db.crear_comprobante(
                        tipo_comprobante='recibo',
                        pago_id=int(pago_id),
                        usuario_id=int(getattr(pago, 'usuario_id', 0)),
                        monto_total=float(getattr(pago, 'monto', 0.0) or 0.0),
                        plantilla_id=None,
                        datos_comprobante=None,
                        emitido_por=None
                    )
                    # Obtener número creado
                    comp = db.obtener_comprobante(int(comprobante_id))
                    if comp:
                        numero_comprobante = comp.get('numero_comprobante')
                # Si hay override en modo no preview, usarlo solo para el PDF (no persistimos aquí)
                if numero_override:
                    numero_comprobante = numero_override
            except Exception:
                # Si falla la creación, continuar sin numeración
                numero_comprobante = None

        # Generar PDF con el generador existente, incluyendo número si está disponible
        from pdf_generator import PDFGenerator
        pdfg = PDFGenerator()
        filepath = pdfg.generar_recibo(
            pago,
            usuario,
            numero_comprobante,
            detalles=detalles,
            totales=totales,
            observaciones=obs_text,
            emitido_por=emitido_por,
            titulo=titulo,
            gym_name=gym_name_override,
            gym_address=gym_address_override,
            fecha_emision=fecha_emision_disp,
            metodo_pago=metodo_override,
            usuario_nombre=usuario_nombre_override,
            usuario_dni=usuario_dni_override,
            detalles_override=detalles_override,
            mostrar_logo=mostrar_logo,
            mostrar_metodo=mostrar_metodo,
            mostrar_dni=mostrar_dni,
            tipo_cuota=tipo_cuota_override,
            periodo=periodo_override,
        )

        # Guardar ruta del PDF en el comprobante si existe
        try:
            if comprobante_id is not None and filepath:
                with db.get_connection_context() as conn:  # type: ignore
                    cur = conn.cursor()
                    cur.execute(
                        "UPDATE comprobantes_pago SET archivo_pdf = %s WHERE id = %s",
                        (str(filepath), int(comprobante_id))
                    )
                    conn.commit()
        except Exception:
            pass

        # Servir el archivo PDF
        from starlette.responses import FileResponse
        filename = os.path.basename(filepath)
        resp = FileResponse(filepath, media_type="application/pdf")
        try:
            resp.headers["Content-Disposition"] = f'inline; filename="{filename}"'
        except Exception:
            pass
        return resp
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

# --- Numeración de recibos: próximos números y configuración ---
@app.get("/api/recibos/numero-proximo")
async def api_recibos_numero_proximo(_=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    try:
        numero = db.get_next_receipt_number()
        return {"numero": str(numero)}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/recibos/config")
async def api_recibos_config_get(_=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    try:
        cfg = db.get_receipt_numbering_config()
        return cfg
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.put("/api/recibos/config")
async def api_recibos_config_put(request: Request, _=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    try:
        payload = await request.json()
        ok = db.save_receipt_numbering_config(payload)
        if ok:
            return {"ok": True}
        return JSONResponse({"error": "No se pudo guardar la configuración"}, status_code=400)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

# --- Crear/registrar pago (básico) ---
@app.post("/api/pagos")
async def api_pagos_create(request: Request, _=Depends(require_gestion_access)):
    pm = _get_pm()
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, "/api/pagos[POST]")
    if guard:
        return guard
    if pm is None:
        raise HTTPException(status_code=503, detail="PaymentManager no disponible")
    payload = await request.json()
    try:
        usuario_id_raw = payload.get("usuario_id")
        monto_raw = payload.get("monto")
        mes_raw = payload.get("mes")
        año_raw = payload.get("año")
        metodo_pago_id = payload.get("metodo_pago_id")
        conceptos_raw = payload.get("conceptos") or []
        fecha_pago_raw = payload.get("fecha_pago")

        # Validaciones comunes
        if usuario_id_raw is None:
            raise HTTPException(status_code=400, detail="'usuario_id' es obligatorio")
        try:
            usuario_id = int(usuario_id_raw)
            metodo_pago_id_int = int(metodo_pago_id) if metodo_pago_id is not None else None
        except Exception:
            raise HTTPException(status_code=400, detail="Tipos inválidos en payload")

        # Si vienen conceptos, usar flujo avanzado
        if isinstance(conceptos_raw, list) and len(conceptos_raw) > 0:
            conceptos: list[dict] = []
            for c in conceptos_raw:
                try:
                    cid = int(c.get("concepto_id"))
                    cantidad = int(c.get("cantidad") or 1)
                    precio_unitario = float(c.get("precio_unitario") or 0.0)
                except Exception:
                    raise HTTPException(status_code=400, detail="Conceptos inválidos en payload")
                if cantidad <= 0 or precio_unitario < 0:
                    raise HTTPException(status_code=400, detail="Cantidad/precio inválidos en conceptos")
                conceptos.append({"concepto_id": cid, "cantidad": cantidad, "precio_unitario": precio_unitario})

            # Resolver fecha desde fecha_pago o mes/año
            fecha_dt = None
            try:
                if fecha_pago_raw:
                    fecha_dt = datetime.fromisoformat(str(fecha_pago_raw))
                elif mes_raw is not None and año_raw is not None:
                    mes_i = int(mes_raw); año_i = int(año_raw)
                    fecha_dt = datetime(int(año_i), int(mes_i), 1)
            except Exception:
                raise HTTPException(status_code=400, detail="fecha_pago inválida")

            try:
                pago_id = pm.registrar_pago_avanzado(usuario_id, metodo_pago_id_int, conceptos, fecha_dt)
            except ValueError as ve:
                raise HTTPException(status_code=400, detail=str(ve))
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
            return {"ok": True, "id": int(pago_id)}

        # Flujo básico si no hay conceptos
        if monto_raw is None or mes_raw is None or año_raw is None:
            raise HTTPException(status_code=400, detail="'monto', 'mes' y 'año' son obligatorios cuando no hay 'conceptos'")
        try:
            monto = float(monto_raw)
            mes = int(mes_raw)
            año = int(año_raw)
        except Exception:
            raise HTTPException(status_code=400, detail="Tipos inválidos en payload")
        if not (1 <= mes <= 12):
            raise HTTPException(status_code=400, detail="'mes' debe estar entre 1 y 12")
        if monto <= 0:
            raise HTTPException(status_code=400, detail="'monto' debe ser mayor a 0")

        try:
            pago_id = pm.registrar_pago(usuario_id, monto, mes, año, metodo_pago_id_int)
        except ValueError as ve:
            raise HTTPException(status_code=400, detail=str(ve))
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

        return {"ok": True, "id": int(pago_id)}
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

# --- Modificar pago existente ---
@app.put("/api/pagos/{pago_id}")
async def api_pagos_update(pago_id: int, request: Request, _=Depends(require_gestion_access)):
    pm = _get_pm()
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, f"/api/pagos/{pago_id}[PUT]")
    if guard:
        return guard
    if pm is None or Pago is None:
        raise HTTPException(status_code=503, detail="PaymentManager o modelo Pago no disponible")
    payload = await request.json()
    try:
        usuario_id_raw = payload.get("usuario_id")
        monto_raw = payload.get("monto")
        fecha_raw = payload.get("fecha_pago")
        mes_raw = payload.get("mes")
        año_raw = payload.get("año")
        metodo_pago_id = payload.get("metodo_pago_id")

        if usuario_id_raw is None or monto_raw is None:
            raise HTTPException(status_code=400, detail="'usuario_id' y 'monto' son obligatorios")
        try:
            usuario_id = int(usuario_id_raw)
            monto = float(monto_raw)
            metodo_pago_id_int = int(metodo_pago_id) if metodo_pago_id is not None else None
        except Exception:
            raise HTTPException(status_code=400, detail="Tipos inválidos en payload")

        # Resolver fecha_pago
        fecha_dt = None
        if fecha_raw is not None:
            try:
                if isinstance(fecha_raw, str):
                    fecha_dt = datetime.fromisoformat(fecha_raw)
                else:
                    # Si llega ya como objeto serializado raro, intentar fallback
                    raise ValueError("fecha_pago debe ser string ISO")
            except Exception:
                raise HTTPException(status_code=400, detail="'fecha_pago' inválida, use ISO 8601 (YYYY-MM-DD)")
        else:
            if mes_raw is not None and año_raw is not None:
                try:
                    mes = int(mes_raw)
                    año = int(año_raw)
                    if not (1 <= mes <= 12):
                        raise HTTPException(status_code=400, detail="'mes' debe estar entre 1 y 12")
                    fecha_dt = datetime(año, mes, 1)
                except HTTPException:
                    raise
                except Exception:
                    raise HTTPException(status_code=400, detail="'mes'/'año' inválidos")
            else:
                # Intentar recuperar el pago para preservar su fecha
                try:
                    existing = pm.obtener_pago(pago_id)
                    if existing and getattr(existing, 'fecha_pago', None):
                        fecha_dt = existing.fecha_pago if not isinstance(existing.fecha_pago, str) else datetime.fromisoformat(existing.fecha_pago)
                    else:
                        fecha_dt = datetime.now()
                except Exception:
                    fecha_dt = datetime.now()

        # Resolver mes/año finales (usados en sync)
        if mes_raw is not None and año_raw is not None:
            try:
                mes = int(mes_raw)
                año = int(año_raw)
            except Exception:
                mes = fecha_dt.month
                año = fecha_dt.year
        else:
            mes = fecha_dt.month
            año = fecha_dt.year

        if monto <= 0:
            raise HTTPException(status_code=400, detail="'monto' debe ser mayor a 0")

        pago = Pago(id=int(pago_id), usuario_id=usuario_id, monto=monto, mes=mes, año=año, fecha_pago=fecha_dt, metodo_pago_id=metodo_pago_id_int)
        try:
            pm.modificar_pago(pago)
        except ValueError as ve:
            raise HTTPException(status_code=400, detail=str(ve))
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        return {"ok": True, "id": int(pago_id)}
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

# --- Eliminar pago ---
@app.delete("/api/pagos/{pago_id}")
async def api_pagos_delete(pago_id: int, _=Depends(require_gestion_access)):
    pm = _get_pm()
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, f"/api/pagos/{pago_id}[DELETE]")
    if guard:
        return guard
    if pm is None:
        raise HTTPException(status_code=503, detail="PaymentManager no disponible")
    try:
        pm.eliminar_pago(int(pago_id))
        return {"ok": True}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

# --- Historial de pagos por usuario (gestión) ---
@app.get("/api/usuarios/{usuario_id}/pagos")
async def api_usuario_historial(usuario_id: int, request: Request, _=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        return []
    try:
        limit_q = request.query_params.get("limit")
        offset_q = request.query_params.get("offset")
        lim = int(limit_q) if (limit_q and str(limit_q).isdigit()) else 50
        off = int(offset_q) if (offset_q and str(offset_q).isdigit()) else 0
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            try:
                cur.execute(
                    """
                    SELECT 
                        p.id, p.fecha_pago::date AS fecha, p.monto,
                        COALESCE(mp.nombre,'') AS metodo,
                        COALESCE(
                            (
                                SELECT COALESCE(cp.nombre, pd.descripcion)
                                FROM pago_detalles pd
                                LEFT JOIN conceptos_pago cp ON cp.id = pd.concepto_id
                                WHERE pd.pago_id = p.id
                                ORDER BY pd.id
                                LIMIT 1
                            ),
                            ''
                        ) AS concepto
                    FROM pagos p
                    LEFT JOIN metodos_pago mp ON mp.id = p.metodo_pago_id
                    WHERE p.usuario_id = %s
                    ORDER BY p.fecha_pago DESC
                    LIMIT %s OFFSET %s
                    """,
                    (usuario_id, lim, off)
                )
                rows = cur.fetchall() or []
            except Exception:
                cur.execute(
                    """
                    SELECT 
                        p.id, p.fecha_pago::date AS fecha, p.monto,
                        COALESCE(mp.nombre,'') AS metodo,
                        '' AS concepto
                    FROM pagos p
                    LEFT JOIN metodos_pago mp ON mp.id = p.metodo_pago_id
                    WHERE p.usuario_id = %s
                    ORDER BY p.fecha_pago DESC
                    LIMIT %s OFFSET %s
                    """,
                    (usuario_id, lim, off)
                )
                rows = cur.fetchall() or []
        return rows
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/usuarios_detalle")
async def api_usuarios_detalle(_=Depends(require_owner)):
    """Detalle de usuarios con métricas reales: pagos y asistencias."""
    db = _get_db()
    if db is None:
        return []
    guard = _circuit_guard_json(db, "/api/usuarios_detalle")
    if guard:
        return guard
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            cur.execute(
                """
                SELECT u.id,
                       COALESCE(u.nombre,'') AS nombre,
                       COALESCE(u.rol,'') AS rol,
                       COALESCE(u.dni,'') AS dni,
                       COALESCE(u.telefono,'') AS telefono,
                       COALESCE(u.tipo_cuota,'') AS tipo_cuota,
                       COALESCE(u.activo,false) AS activo,
                       COALESCE(u.fecha_registro::date, CURRENT_DATE) AS fecha_registro,
                       COALESCE(p.pagos_count,0) AS pagos_count,
                       p.last_pago_fecha::date AS last_pago_fecha,
                       COALESCE(a.asistencias_count,0) AS asistencias_count
                FROM usuarios u
                LEFT JOIN (
                    SELECT usuario_id, COUNT(*) AS pagos_count, MAX(fecha_pago) AS last_pago_fecha
                    FROM pagos
                    GROUP BY usuario_id
                ) p ON p.usuario_id = u.id
                LEFT JOIN (
                    SELECT usuario_id, COUNT(*) AS asistencias_count
                    FROM asistencias
                    GROUP BY usuario_id
                ) a ON a.usuario_id = u.id
                ORDER BY u.id
                """
            )
            rows = []
            for r in cur.fetchall():
                rows.append({
                    "id": r[0],
                    "nombre": r[1],
                    "rol": r[2],
                    "dni": r[3],
                    "telefono": r[4],
                    "tipo_cuota": r[5],
                    "activo": bool(r[6]),
                    "fecha_registro": str(r[7]) if r[7] is not None else None,
                    "pagos_count": int(r[8] or 0),
                    "last_pago_fecha": str(r[9]) if r[9] is not None else None,
                    "asistencias_count": int(r[10] or 0),
                })
        return rows
    except Exception as e:
        import traceback
        traceback.print_exc()
        print("Error in api_profesores_detalle:", repr(e))
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/profesores_detalle")
async def api_profesores_detalle(request: Request, _=Depends(require_owner)):
    """Detalle de profesores con contacto y horarios/sesiones reales.

    Devuelve por profesor:
    - Datos de contacto (nombre, email, teléfono)
    - Cantidad de horarios de disponibilidad (activos)
    - Resumen de horarios (día, hora_inicio, hora_fin)
    - Sesiones del mes actual y horas trabajadas reales
    """
    db = _get_db()
    if db is None:
        return []
    try:
        # Parsear parámetros de fecha (opcionales)
        start = request.query_params.get("start")
        end = request.query_params.get("end")
        if not start or (isinstance(start, str) and start.strip() == ""):
            start = None
        if not end or (isinstance(end, str) and end.strip() == ""):
            end = None

        from datetime import datetime as _dt
        from datetime import datetime
        start_date = None
        end_date = None
        try:
            if start:
                start_date = _dt.strptime(start, "%Y-%m-%d").date()
            if end:
                end_date = _dt.strptime(end, "%Y-%m-%d").date()
        except Exception:
            start_date = None
            end_date = None
        now = datetime.now()
        mes_actual = now.month
        anio_actual = now.year

        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            # Consulta única con CTEs que agrega horarios (JSON) y sesiones del rango o del mes actual
            # Índices recomendados (también se crean automáticamente en database.ensure_indexes):
            # - CREATE INDEX IF NOT EXISTS idx_profesores_usuario_id ON profesores(usuario_id);
            # - CREATE INDEX IF NOT EXISTS idx_horarios_profesores_profesor_id ON horarios_profesores(profesor_id);
            # - CREATE INDEX IF NOT EXISTS idx_horarios_profesores_dia_inicio ON horarios_profesores(dia_semana, hora_inicio);
            # - CREATE INDEX IF NOT EXISTS idx_profesor_horas_fecha ON profesor_horas_trabajadas(fecha);
            # - CREATE INDEX IF NOT EXISTS idx_profesor_horas_profesor_fecha ON profesor_horas_trabajadas(profesor_id, fecha);
            cur.execute(
                """
                WITH sesiones AS (
                    SELECT profesor_id,
                           COUNT(*) AS sesiones_mes,
                           COALESCE(SUM(minutos_totales) / 60.0, 0) AS horas_mes
                    FROM profesor_horas_trabajadas
                    WHERE hora_fin IS NOT NULL
                      AND (
                        ( %s IS NOT NULL AND %s IS NOT NULL AND fecha BETWEEN %s AND %s )
                        OR ( (%s IS NULL OR %s IS NULL) AND EXTRACT(MONTH FROM fecha) = %s AND EXTRACT(YEAR FROM fecha) = %s )
                      )
                    GROUP BY profesor_id
                ),
                horarios AS (
                    SELECT hp.profesor_id,
                           COUNT(hp.id) AS horarios_count,
                           JSON_AGG(
                               JSON_BUILD_OBJECT(
                                   'dia', hp.dia_semana,
                                   'inicio', hp.hora_inicio::text,
                                   'fin', hp.hora_fin::text
                               )
                               ORDER BY CASE hp.dia_semana 
                                   WHEN 'Lunes' THEN 1 
                                   WHEN 'Martes' THEN 2 
                                   WHEN 'Miércoles' THEN 3 
                                   WHEN 'Jueves' THEN 4 
                                   WHEN 'Viernes' THEN 5 
                                   WHEN 'Sábado' THEN 6 
                                   WHEN 'Domingo' THEN 7 
                               END, hp.hora_inicio
                           ) AS horarios
                    FROM horarios_profesores hp
                    GROUP BY hp.profesor_id
                )
                SELECT p.id AS id,
                       COALESCE(u.nombre,'') AS nombre,
                       ''::text AS email,
                       COALESCE(u.telefono,'') AS telefono,
                       COALESCE(h.horarios_count, 0) AS horarios_count,
                       COALESCE(h.horarios, '[]'::json) AS horarios,
                       COALESCE(s.sesiones_mes, 0) AS sesiones_mes,
                       COALESCE(s.horas_mes, 0) AS horas_mes
                FROM profesores p
                JOIN usuarios u ON u.id = p.usuario_id
                LEFT JOIN horarios h ON h.profesor_id = p.id
                LEFT JOIN sesiones s ON s.profesor_id = p.id
                ORDER BY p.id
                """,
                (
                    start_date, end_date, start_date, end_date,
                    start_date, end_date, mes_actual, anio_actual
                )
            )
            rows = cur.fetchall() or []
        return rows
    except Exception as e:
        logging.exception("Error final en /api/profesores_detalle")
        return []

# --- Endpoints de detalle para pagos y asistencias ---

@app.get("/api/profesor_sesiones")
async def api_profesor_sesiones(request: Request, _=Depends(require_owner)):
    """Lista de sesiones trabajadas por un profesor, opcionalmente por rango.

    Devuelve datos reales y completos por sesión obtenidos directamente del backend
    (database.py) sin cálculos en la web: fecha, inicio, fin, minutos, horas,
    clase (si existe) y tipo de actividad.
    """
    db = _get_db()
    if db is None:
        return []
    guard = _circuit_guard_json(db, "/api/profesor_sesiones")
    if guard:
        return guard
    try:
        pid = request.query_params.get("profesor_id")
        if not pid:
            return []
        try:
            profesor_id = int(pid)
        except Exception:
            return []
        start = request.query_params.get("start")
        end = request.query_params.get("end")
        # Delegar completamente al backend (database.py)
        from datetime import datetime
        fecha_inicio = None
        fecha_fin = None
        try:
            if start:
                fecha_inicio = datetime.strptime(start, "%Y-%m-%d").date()
            if end:
                fecha_fin = datetime.strptime(end, "%Y-%m-%d").date()
        except Exception:
            # Si las fechas no son válidas, se ignoran y se usa mes actual dentro del backend
            fecha_inicio = None
            fecha_fin = None

        try:
            sesiones = db.obtener_horas_trabajadas_profesor(profesor_id, fecha_inicio, fecha_fin)  # type: ignore
        except Exception:
            sesiones = []

        # Normalizar y filtrar: solo sesiones cerradas; permitimos minutos 0 para mostrar registro
        out = []
        for s in sesiones or []:
            minutos_val = s.get("minutos_totales")
            fin_val = s.get("hora_fin")
            # filtrar sin fin
            try:
                minutos_num = int(minutos_val) if minutos_val is not None else 0
            except Exception:
                minutos_num = 0
            if fin_val is None:
                continue
            # formato seguro para fecha y hora (HH:MM)
            def _fmt_date(d):
                try:
                    return str(d)[:10] if d is not None else ""
                except Exception:
                    return str(d) if d is not None else ""
            def _fmt_time(t):
                try:
                    if t is None:
                        return ""
                    # Manejar objetos datetime/time directamente
                    hh = getattr(t, "hour", None)
                    mm = getattr(t, "minute", None)
                    if hh is not None and mm is not None:
                        return f"{int(hh):02d}:{int(mm):02d}"
                    s = str(t)
                    # Intentar extraer HH:MM de cadenas comunes
                    for sep in ("T", " "):
                        if sep in s:
                            tail = s.split(sep, 1)[1]
                            if len(tail) >= 5 and tail[0:2].isdigit() and tail[2] == ":" and tail[3:5].isdigit():
                                return tail[:5]
                    if len(s) >= 5 and s[0:2].isdigit() and s[2] == ":" and s[3:5].isdigit():
                        return s[:5]
                    # Último intento: fromisoformat
                    try:
                        from datetime import datetime as _dt
                        dt = _dt.fromisoformat(s.replace('Z', '+00:00'))
                        return f"{dt.hour:02d}:{dt.minute:02d}"
                    except Exception:
                        pass
                    return ""
                except Exception:
                    return ""
            out.append({
                "id": s.get("id"),
                "fecha": _fmt_date(s.get("fecha")),
                "inicio": _fmt_time(s.get("hora_inicio")),
                "fin": _fmt_time(fin_val),
                "minutos": minutos_num,
                "horas": round(minutos_num / 60.0, 2),
                "tipo": s.get("tipo_actividad") or ""
            })
        return out
    except Exception as e:
        logging.exception("Error en /api/profesor_sesiones")
        return []

# Ruta duplicada /api/profesor_resumen eliminada para evitar respuestas inconsistentes

@app.put("/api/profesor_sesion/{sesion_id}")
async def api_profesor_sesion_update(sesion_id: int, request: Request, _=Depends(require_gestion_access)):
    """
    Edita una sesión de trabajo del profesor por ID.

    Body JSON admite: { fecha?, inicio?, fin?, tipo?, minutos? }
    """
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, "/api/profesor_sesion_update")
    if guard:
        return guard
    try:
        try:
            body = await request.json()
        except Exception:
            body = {}
        if not isinstance(body, dict):
            body = {}
        # Normalizar campos
        fecha = body.get("fecha")
        inicio = body.get("inicio")  # HH:MM
        fin = body.get("fin")        # HH:MM
        tipo = body.get("tipo")
        minutos_raw = body.get("minutos")
        minutos_int = None
        if minutos_raw is not None:
            try:
                minutos_int = int(minutos_raw)
                if minutos_int < 0:
                    minutos_int = 0
            except Exception:
                minutos_int = None

        # Validaciones mínimas
        if fecha is not None and not isinstance(fecha, str):
            fecha = None
        if inicio is not None and not isinstance(inicio, str):
            inicio = None
        if fin is not None and not isinstance(fin, str):
            fin = None
        if tipo is not None and not isinstance(tipo, str):
            tipo = None

        # Ejecutar actualización en DB
        try:
            result = db.actualizar_profesor_sesion(  # type: ignore
                sesion_id,
                fecha=fecha,
                hora_inicio=inicio,
                hora_fin=fin,
                tipo_actividad=tipo,
                minutos_totales=minutos_int,
            )
        except Exception as e:
            logging.exception("Error en actualizar_profesor_sesion")
            raise HTTPException(status_code=500, detail=str(e))

        if not result or not result.get("success"):
            msg = (result or {}).get("error") or "No se pudo actualizar la sesión"
            status = 400
            if msg == "Sesión no encontrada":
                status = 404
            elif msg == "ID de sesión inválido":
                status = 400
            elif msg == "Sin cambios para aplicar":
                status = 400
            return JSONResponse(status_code=status, content={"detail": msg})

        # Serializar campos de fecha/hora para una respuesta JSON segura
        updated_raw = result.get("updated")
        def _fmt_date(d):
            try:
                return str(d)[:10] if d is not None else None
            except Exception:
                return str(d) if d is not None else None
        def _fmt_time(t):
            try:
                sstr = str(t) if t is not None else None
                if sstr is None:
                    return None
                return sstr[:5] if len(sstr) >= 5 else sstr
            except Exception:
                return None
        # Convertir RealDictRow u otros mapeos a dict estándar
        raw_dict = None
        try:
            if updated_raw is not None:
                raw_dict = dict(updated_raw)
        except Exception:
            raw_dict = updated_raw if isinstance(updated_raw, dict) else None
        updated_safe = None
        if isinstance(raw_dict, dict):
            updated_safe = {
                "id": int(raw_dict.get("id")) if raw_dict.get("id") is not None else None,
                "profesor_id": int(raw_dict.get("profesor_id")) if raw_dict.get("profesor_id") is not None else None,
                "fecha": _fmt_date(raw_dict.get("fecha")),
                "hora_inicio": _fmt_time(raw_dict.get("hora_inicio")),
                "hora_fin": _fmt_time(raw_dict.get("hora_fin")),
                "minutos_totales": int(raw_dict.get("minutos_totales") or 0),
                "horas_totales": float(raw_dict.get("horas_totales") or 0.0),
                "tipo_actividad": raw_dict.get("tipo_actividad") or None,
            }

        return JSONResponse(status_code=200, content={"success": True, "updated": updated_safe})
    except HTTPException:
        raise
    except Exception as e:
        logging.exception("Error en /api/profesor_sesion/{sesion_id} PUT")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/profesor_sesion/{sesion_id}")
async def api_profesor_sesion_delete(sesion_id: int, _=Depends(require_gestion_access)):
    """
    Elimina una sesión de trabajo del profesor por ID.
    """
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, "/api/profesor_sesion_delete")
    if guard:
        return guard
    try:
        res = db.eliminar_profesor_sesion(sesion_id)  # type: ignore
        if not res or not res.get("success"):
            msg = (res or {}).get("error") or "No se pudo eliminar la sesión"
            status = 400
            if msg == "Sesión no encontrada":
                status = 404
            elif msg == "ID de sesión inválido":
                status = 400
            return JSONResponse(status_code=status, content={"detail": msg})
        return JSONResponse(status_code=200, content={"success": True, "deleted_id": res.get("deleted_id")})
    except Exception as e:
        logging.exception("Error en /api/profesor_sesion/{sesion_id} DELETE")
        raise HTTPException(status_code=500, detail=str(e))
        return {"success": False, "error": str(e)}

# --- CRUD Horarios de profesores ---
@app.get("/api/profesor_horarios")
async def api_profesor_horarios(request: Request, _=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        return []
    guard = _circuit_guard_json(db, "/api/profesor_horarios")
    if guard:
        return guard
    try:
        # Determinar permisos y el profesor_id efectivo
        pid = request.query_params.get("profesor_id")
        is_owner = bool(request.session.get("logged_in"))
        ses_prof_id = request.session.get("gestion_profesor_id")
        profesor_id = None
        if is_owner:
            # Dueño: usa el parámetro si es válido
            if not pid:
                return []
            try:
                profesor_id = int(pid)
            except Exception:
                return []
        else:
            # Profesor: fuerza su propio ID de sesión
            try:
                profesor_id = int(ses_prof_id) if ses_prof_id is not None else None
            except Exception:
                profesor_id = None
            if profesor_id is None:
                return []
        try:
            items = db.obtener_horarios_disponibilidad_profesor(profesor_id)  # type: ignore
        except Exception:
            items = []
        def _fmt_time(t):
            try:
                if t is None:
                    return ""
                hh = getattr(t, "hour", None)
                mm = getattr(t, "minute", None)
                if hh is not None and mm is not None:
                    return f"{int(hh):02d}:{int(mm):02d}"
                s = str(t)
                for sep in ("T", " "):
                    if sep in s:
                        tail = s.split(sep, 1)[1]
                        if len(tail) >= 5 and tail[0:2].isdigit() and tail[2] == ":" and tail[3:5].isdigit():
                            return tail[:5]
                if len(s) >= 5 and s[0:2].isdigit() and s[2] == ":" and s[3:5].isdigit():
                    return s[:5]
                try:
                    from datetime import datetime as _dt
                    dt = _dt.fromisoformat(s.replace('Z', '+00:00'))
                    return f"{dt.hour:02d}:{dt.minute:02d}"
                except Exception:
                    pass
                return ""
            except Exception:
                return ""
        out = []
        for h in (items or []):
            out.append({
                "id": h.get("id"),
                "dia": h.get("dia_semana"),
                "inicio": _fmt_time(h.get("hora_inicio")),
                "fin": _fmt_time(h.get("hora_fin")),
                "disponible": bool(h.get("disponible", True)),
            })
        return out
    except Exception as e:
        logging.exception("Error en /api/profesor_horarios [GET]")
        return []

@app.post("/api/profesor_horarios")
async def api_profesor_horarios_create(request: Request, _=Depends(require_owner)):
    db = _get_db()
    if db is None:
        return JSONResponse({"error": "no_db"}, status_code=500)
    guard = _circuit_guard_json(db, "/api/profesor_horarios[POST]")
    if guard:
        return guard
    try:
        data = await request.json()
        profesor_id = data.get("profesor_id")
        dia = data.get("dia")
        inicio = data.get("inicio")
        fin = data.get("fin")
        disponible = data.get("disponible")
        if profesor_id is None or dia is None or inicio is None or fin is None:
            return JSONResponse({"error": "missing_fields"}, status_code=400)
        try:
            profesor_id = int(profesor_id)
        except Exception:
            return JSONResponse({"error": "invalid_profesor_id"}, status_code=400)
        # Validación adicional de formato de hora para evitar cadenas vacías
        try:
            def _parse_time(s):
                if s is None:
                    raise ValueError("hora_requerida")
                ss = str(s).strip()
                if not ss:
                    raise ValueError("hora_requerida")
                from datetime import datetime as _dt
                for fmt in ("%H:%M:%S", "%H:%M"):
                    try:
                        return _dt.strptime(ss, fmt).time()
                    except Exception:
                        pass
                raise ValueError("formato_invalido")
            tinicio = _parse_time(inicio)
            tfin = _parse_time(fin)
            if not (tinicio < tfin):
                return JSONResponse({"error": "invalid_time_range"}, status_code=400)
        except ValueError as ve:
            msg = str(ve)
            if msg == "hora_requerida":
                return JSONResponse({"error": "times_required"}, status_code=400)
            return JSONResponse({"error": "invalid_time_format"}, status_code=400)
        disponible_val = bool(disponible) if disponible is not None else True
        try:
            created = db.crear_horario_profesor(profesor_id, str(dia), str(inicio), str(fin), disponible_val)  # type: ignore
        except Exception as e:
            logging.exception("Error crear horario profesor")
            msg = str(e)
            if "Profesor no existe" in msg:
                return JSONResponse({"error": "profesor_not_found"}, status_code=404)
            if "Día inválido" in msg:
                return JSONResponse({"error": "invalid_day"}, status_code=400)
            if "hora_inicio debe ser menor" in msg:
                return JSONResponse({"error": "invalid_time_range"}, status_code=400)
            if "horas_requeridas" in msg:
                return JSONResponse({"error": "times_required"}, status_code=400)
            if "violates foreign key constraint" in msg:
                return JSONResponse({"error": "profesor_not_found"}, status_code=404)
            return JSONResponse({"error": msg}, status_code=500)
        return {"ok": True, "horario": created}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.put("/api/profesor_horarios/{horario_id}")
async def api_profesor_horarios_update(horario_id: int, request: Request, _=Depends(require_owner)):
    db = _get_db()
    if db is None:
        return JSONResponse({"error": "no_db"}, status_code=500)
    guard = _circuit_guard_json(db, "/api/profesor_horarios[PUT]")
    if guard:
        return guard
    try:
        data = await request.json()
        dia = data.get("dia")
        inicio = data.get("inicio")
        fin = data.get("fin")
        disponible = data.get("disponible")
        if dia is None or inicio is None or fin is None:
            return JSONResponse({"error": "missing_fields"}, status_code=400)
        disponible_val = bool(disponible) if disponible is not None else True
        try:
            updated = db.actualizar_horario_profesor(horario_id, str(dia), str(inicio), str(fin), disponible_val)  # type: ignore
        except Exception as e:
            logging.exception("Error actualizar horario profesor")
            msg = str(e)
            if "Día inválido" in msg:
                return JSONResponse({"error": "invalid_day"}, status_code=400)
            if "hora_inicio debe ser menor" in msg:
                return JSONResponse({"error": "invalid_time_range"}, status_code=400)
            return JSONResponse({"error": msg}, status_code=500)
        return {"ok": bool(updated), "horario": updated}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.delete("/api/profesor_horarios/{horario_id}")
async def api_profesor_horarios_delete(horario_id: int, _=Depends(require_owner)):
    db = _get_db()
    if db is None:
        return JSONResponse({"error": "no_db"}, status_code=500)
    guard = _circuit_guard_json(db, "/api/profesor_horarios[DELETE]")
    if guard:
        return guard
    try:
        try:
            deleted = db.eliminar_horario_profesor(horario_id)  # type: ignore
        except Exception as e:
            logging.exception("Error eliminar horario profesor")
            return JSONResponse({"error": str(e)}, status_code=500)
        return {"ok": bool(deleted)}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

# --- Sesiones de trabajo de profesores ---
@app.post("/api/profesor_sesion_inicio")
async def api_profesor_sesion_inicio(request: Request, _=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        return JSONResponse({"error": "no_db"}, status_code=500)
    guard = _circuit_guard_json(db, "/api/profesor_sesion_inicio")
    if guard:
        return guard
    try:
        data = await request.json()
        profesor_id = data.get("profesor_id")
        tipo = data.get("tipo") or data.get("tipo_actividad") or "Trabajo"
        # Determinar permisos según rol: dueño puede iniciar para cualquiera; profesor sólo para sí mismo
        is_owner = bool(request.session.get("logged_in"))
        ses_prof_id = request.session.get("gestion_profesor_id")
        effective_prof_id = None
        if is_owner:
            if profesor_id is None:
                return JSONResponse({"error": "missing_fields"}, status_code=400)
            try:
                effective_prof_id = int(profesor_id)
            except Exception:
                return JSONResponse({"error": "invalid_profesor_id"}, status_code=400)
        else:
            try:
                effective_prof_id = int(ses_prof_id) if ses_prof_id is not None else None
            except Exception:
                effective_prof_id = None
            if effective_prof_id is None:
                return JSONResponse({"error": "invalid_profesor_session"}, status_code=403)
        try:
            res = db.iniciar_sesion_trabajo_profesor(effective_prof_id, str(tipo))  # type: ignore
        except Exception as e:
            logging.exception("Error iniciar sesión trabajo profesor")
            return JSONResponse({"error": str(e)}, status_code=500)
        # Serialización segura de la sesión y consistencia de campos
        def _fmt_date(d):
            try:
                return str(d)[:10] if d is not None else None
            except Exception:
                return None
        def _fmt_time(t):
            try:
                if t is None:
                    return None
                hh = getattr(t, "hour", None)
                mm = getattr(t, "minute", None)
                if hh is not None and mm is not None:
                    return f"{int(hh):02d}:{int(mm):02d}"
                s = str(t)
                for sep in ("T", " "):
                    if sep in s:
                        tail = s.split(sep, 1)[1]
                        if len(tail) >= 5 and tail[0:2].isdigit() and tail[2] == ":" and tail[3:5].isdigit():
                            return tail[:5]
                if len(s) >= 5 and s[0:2].isdigit() and s[2] == ":" and s[3:5].isdigit():
                    return s[:5]
                try:
                    from datetime import datetime as _dt
                    dt = _dt.fromisoformat(s.replace('Z', '+00:00'))
                    return f"{dt.hour:02d}:{dt.minute:02d}"
                except Exception:
                    pass
                return None
            except Exception:
                return None
        datos = None
        try:
            datos = res.get("datos") if isinstance(res, dict) else None
        except Exception:
            datos = None
        raw = None
        if datos is not None:
            try:
                raw = dict(datos)
            except Exception:
                raw = datos if isinstance(datos, dict) else None
        sesion_safe = None
        if isinstance(raw, dict):
            minutos = int(raw.get("minutos_totales") or 0)
            horas = round(minutos / 60.0, 2)
            sesion_safe = {
                "id": int(raw.get("id") or 0),
                "profesor_id": int(raw.get("profesor_id") or 0),
                "fecha": _fmt_date(raw.get("fecha")),
                "hora_inicio": _fmt_time(raw.get("hora_inicio")),
                "hora_fin": _fmt_time(raw.get("hora_fin")),
                "minutos_totales": minutos,
                "horas_totales": horas,
                "tipo_actividad": raw.get("tipo_actividad") or str(tipo),
            }
        success_val = bool((res or {}).get("success")) if isinstance(res, dict) else True
        mensaje = (res or {}).get("mensaje") if isinstance(res, dict) else None
        return JSONResponse(status_code=200, content={"success": success_val, "mensaje": mensaje, "sesion": sesion_safe})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.post("/api/profesor_sesion_fin")
async def api_profesor_sesion_fin(request: Request, _=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        return JSONResponse({"error": "no_db"}, status_code=500)
    guard = _circuit_guard_json(db, "/api/profesor_sesion_fin")
    if guard:
        return guard
    try:
        data = {}
        try:
            data = await request.json()
        except Exception:
            data = {}
        profesor_id = data.get("profesor_id") or request.session.get("gestion_profesor_id")
        if profesor_id is None:
            return JSONResponse({"error": "missing_fields"}, status_code=400)
        try:
            profesor_id = int(profesor_id)
        except Exception:
            return JSONResponse({"error": "invalid_profesor_id"}, status_code=400)
        try:
            res = db.finalizar_sesion_trabajo_profesor(profesor_id)  # type: ignore
        except Exception as e:
            logging.exception("Error finalizar sesión trabajo profesor")
            return JSONResponse({"error": str(e)}, status_code=500)
        # Serialización segura de la sesión cerrada
        def _fmt_date(d):
            try:
                return str(d)[:10] if d is not None else None
            except Exception:
                return None
        def _fmt_time(t):
            try:
                if t is None:
                    return None
                hh = getattr(t, "hour", None)
                mm = getattr(t, "minute", None)
                if hh is not None and mm is not None:
                    return f"{int(hh):02d}:{int(mm):02d}"
                s = str(t)
                for sep in ("T", " "):
                    if sep in s:
                        tail = s.split(sep, 1)[1]
                        if len(tail) >= 5 and tail[0:2].isdigit() and tail[2] == ":" and tail[3:5].isdigit():
                            return tail[:5]
                if len(s) >= 5 and s[0:2].isdigit() and s[2] == ":" and s[3:5].isdigit():
                    return s[:5]
                try:
                    from datetime import datetime as _dt
                    dt = _dt.fromisoformat(s.replace('Z', '+00:00'))
                    return f"{dt.hour:02d}:{dt.minute:02d}"
                except Exception:
                    pass
                return None
            except Exception:
                return None
        datos = None
        try:
            datos = res.get("datos") if isinstance(res, dict) else None
        except Exception:
            datos = None
        raw = None
        if datos is not None:
            try:
                raw = dict(datos)
            except Exception:
                raw = datos if isinstance(datos, dict) else None
        sesion_safe = None
        minutos = 0
        horas = 0.0
        if isinstance(raw, dict):
            minutos = int(raw.get("minutos_totales") or 0)
            horas = round(minutos / 60.0, 2)
            sesion_safe = {
                "id": int(raw.get("id") or 0),
                "profesor_id": int(raw.get("profesor_id") or 0),
                "fecha": _fmt_date(raw.get("fecha")),
                "hora_inicio": _fmt_time(raw.get("hora_inicio")),
                "hora_fin": _fmt_time(raw.get("hora_fin")),
                "minutos_totales": minutos,
                "horas_totales": horas,
                "tipo_actividad": raw.get("tipo_actividad") or "Trabajo",
            }
        success_val = bool((res or {}).get("success")) if isinstance(res, dict) else True
        mensaje = (res or {}).get("mensaje") if isinstance(res, dict) else None
        return JSONResponse(status_code=200, content={"success": success_val, "mensaje": mensaje, "sesion": sesion_safe, "minutos": minutos, "horas": horas})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/profesor_sesion_activa")
async def api_profesor_sesion_activa(request: Request, _=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        return {"activa": False}
    guard = _circuit_guard_json(db, "/api/profesor_sesion_activa")
    if guard:
        return guard
    try:
        # Resolver profesor_id según rol
        is_owner = bool(request.session.get("logged_in"))
        pid = request.query_params.get("profesor_id")
        profesor_id = None
        if is_owner:
            if not pid:
                return {"activa": False}
            try:
                profesor_id = int(pid)
            except Exception:
                return JSONResponse({"activa": False}, status_code=200)
        else:
            ses_prof_id = request.session.get("gestion_profesor_id")
            try:
                profesor_id = int(ses_prof_id) if ses_prof_id is not None else None
            except Exception:
                profesor_id = None
            if profesor_id is None:
                return {"activa": False}
        try:
            ses = db.obtener_sesion_activa_profesor(profesor_id)  # type: ignore
        except Exception:
            ses = None
        activa = False
        tipo = None
        sesion_raw = None
        if isinstance(ses, dict):
            try:
                activa = bool(ses.get("tiene_sesion_activa"))
            except Exception:
                activa = False
            sesion_raw = ses.get("sesion_activa") or None
        def _fmt_date(d):
            try:
                return str(d)[:10] if d is not None else None
            except Exception:
                return None
        def _fmt_time(t):
            try:
                if t is None:
                    return None
                hh = getattr(t, "hour", None)
                mm = getattr(t, "minute", None)
                if hh is not None and mm is not None:
                    return f"{int(hh):02d}:{int(mm):02d}"
                s = str(t)
                for sep in ("T", " "):
                    if sep in s:
                        tail = s.split(sep, 1)[1]
                        if len(tail) >= 5 and tail[0:2].isdigit() and tail[2] == ":" and tail[3:5].isdigit():
                            return tail[:5]
                if len(s) >= 5 and s[0:2].isdigit() and s[2] == ":" and s[3:5].isdigit():
                    return s[:5]
                return None
            except Exception:
                return None
        sesion_safe = None
        if isinstance(sesion_raw, dict):
            tipo = sesion_raw.get("tipo_actividad")
            minutos = int(sesion_raw.get("minutos_totales") or 0)
            horas = round(minutos / 60.0, 2)
            sesion_safe = {
                "id": int(sesion_raw.get("id") or 0),
                "profesor_id": int(sesion_raw.get("profesor_id") or 0),
                "fecha": _fmt_date(sesion_raw.get("fecha")),
                "hora_inicio": _fmt_time(sesion_raw.get("hora_inicio")),
                "hora_fin": _fmt_time(sesion_raw.get("hora_fin")),
                "minutos_totales": minutos,
                "horas_totales": horas,
                "tipo_actividad": tipo,
            }
        return JSONResponse({"activa": activa, "tipo_actividad": tipo, "sesion": sesion_safe}, status_code=200)
    except Exception as e:
        logging.exception("Error en /api/profesor_sesion_activa")
        return JSONResponse({"activa": False}, status_code=200)

@app.get("/api/profesor_sesion_duracion")
async def api_profesor_sesion_duracion(request: Request, _=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        return {"minutos": 0}
    guard = _circuit_guard_json(db, "/api/profesor_sesion_duracion")
    if guard:
        return guard
    try:
        # Resolver profesor_id según rol
        is_owner = bool(request.session.get("logged_in"))
        pid = request.query_params.get("profesor_id")
        profesor_id = None
        if is_owner:
            if not pid:
                return {"minutos": 0}
            try:
                profesor_id = int(pid)
            except Exception:
                return {"minutos": 0}
        else:
            ses_prof_id = request.session.get("gestion_profesor_id")
            try:
                profesor_id = int(ses_prof_id) if ses_prof_id is not None else None
            except Exception:
                profesor_id = None
            if profesor_id is None:
                return {"minutos": 0}
        try:
            dur = db.obtener_duracion_sesion_actual_profesor(profesor_id)  # type: ignore
        except Exception:
            dur = None
        minutos = 0
        if isinstance(dur, dict):
            try:
                minutos = int(dur.get("minutos_transcurridos") or 0)
            except Exception:
                minutos = 0
        elif isinstance(dur, (int, float)):
            minutos = int(dur)
        return JSONResponse({"minutos": minutos}, status_code=200)
    except Exception as e:
        logging.exception("Error en /api/profesor_sesion_duracion")
        return JSONResponse({"minutos": 0}, status_code=200)

@app.get("/api/usuario_pagos")
async def api_usuario_pagos(request: Request, _=Depends(require_owner)):
    """Lista de pagos reales de un usuario con soporte de búsqueda y paginación."""
    db = _get_db()
    if db is None:
        return []
    try:
        usuario_id = request.query_params.get("usuario_id")
        q = request.query_params.get("q")
        limit = request.query_params.get("limit")
        offset = request.query_params.get("offset")
        if not usuario_id:
            return []
        lim = int(limit) if limit and limit.isdigit() else 50
        off = int(offset) if offset and offset.isdigit() else 0
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            if q:
                cur.execute(
                    """
                    SELECT p.fecha_pago::date, p.monto, COALESCE(u.tipo_cuota,'')
                    FROM pagos p
                    JOIN usuarios u ON u.id = p.usuario_id
                    WHERE p.usuario_id = %s AND (u.tipo_cuota ILIKE %s)
                    ORDER BY p.fecha_pago DESC
                    LIMIT %s OFFSET %s
                    """,
                    (int(usuario_id), f"%{q}%", lim, off)
                )
            else:
                cur.execute(
                    """
                    SELECT p.fecha_pago::date, p.monto, COALESCE(u.tipo_cuota,'')
                    FROM pagos p
                    JOIN usuarios u ON u.id = p.usuario_id
                    WHERE p.usuario_id = %s
                    ORDER BY p.fecha_pago DESC
                    LIMIT %s OFFSET %s
                    """,
                    (int(usuario_id), lim, off)
                )
            rows = []
            for r in cur.fetchall():
                rows.append({
                    "fecha": str(r[0]) if r[0] is not None else None,
                    "monto": float(r[1] or 0),
                    "tipo_cuota": r[2],
                })
        return rows
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/usuario_asistencias")
async def api_usuario_asistencias(request: Request, _=Depends(require_owner)):
    """Lista de asistencias reales de un usuario con soporte de búsqueda y paginación."""
    db = _get_db()
    if db is None:
        return []
    guard = _circuit_guard_json(db, "/api/usuario_asistencias")
    if guard:
        return guard
    try:
        usuario_id = request.query_params.get("usuario_id")
        q = request.query_params.get("q")
        limit = request.query_params.get("limit")
        offset = request.query_params.get("offset")
        if not usuario_id:
            return []
        lim = int(limit) if limit and limit.isdigit() else 50
        off = int(offset) if offset and offset.isdigit() else 0
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            if q:
                cur.execute(
                    """
                    SELECT fecha::date, hora_registro
                    FROM asistencias
                    WHERE usuario_id = %s AND (CAST(hora_registro AS TEXT) ILIKE %s)
                    ORDER BY fecha DESC
                    LIMIT %s OFFSET %s
                    """,
                    (int(usuario_id), f"%{q}%", lim, off)
                )
            else:
                cur.execute(
                    """
                    SELECT fecha::date, hora_registro
                    FROM asistencias
                    WHERE usuario_id = %s
                    ORDER BY fecha DESC
                    LIMIT %s OFFSET %s
                    """,
                    (int(usuario_id), lim, off)
                )
            rows = []
            for r in cur.fetchall():
                rows.append({
                    "fecha": str(r[0]) if r[0] is not None else None,
                    "hora": str(r[1]) if r[1] is not None else None,
                })
        return rows
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/asistencias_hoy_ids")
async def api_asistencias_hoy_ids(_=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        return []
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            cur.execute("SELECT DISTINCT usuario_id FROM asistencias WHERE fecha::date = CURRENT_DATE")
            rows = cur.fetchall() or []
            out = []
            for r in rows:
                try:
                    out.append(int(r[0]))
                except Exception:
                    pass
            return out
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/waitlist_events")
async def api_waitlist_events(request: Request, _=Depends(require_gestion_access)):
    """Eventos recientes de autopromoción/declinación desde auditoría para toasts UI.
    Parámetros: since_id (opcional) para traer sólo eventos nuevos.
    """
    db = _get_db()
    if db is None:
        return {"items": []}
    guard = _circuit_guard_json(db, "/api/waitlist_events")
    if guard:
        return guard
    try:
        since_id_raw = request.query_params.get("since_id")
        since_id = int(since_id_raw) if since_id_raw and since_id_raw.isdigit() else 0
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            if since_id > 0:
                cur.execute(
                    """
                    SELECT 
                        l.id,
                        l.action,
                        l.timestamp,
                        TRIM(COALESCE(u.nombre,'')) AS nombre_full,
                        COALESCE(tc.nombre, c.nombre) AS tipo_txt,
                        ch.dia_semana,
                        ch.hora_inicio
                    FROM audit_logs l
                    LEFT JOIN usuarios u ON u.id = l.user_id
                    LEFT JOIN clases_horarios ch ON ch.id = l.record_id
                    LEFT JOIN clases c ON c.id = ch.clase_id
                    LEFT JOIN tipos_clases tc ON tc.id = c.tipo_clase_id
                    WHERE l.action IN ('auto_promote_waitlist','decline_waitlist_promotion')
                      AND l.id > %s
                    ORDER BY l.id DESC
                    LIMIT 100
                    """,
                    (since_id,)
                )
            else:
                cur.execute(
                    """
                    SELECT 
                        l.id,
                        l.action,
                        l.timestamp,
                        TRIM(COALESCE(u.nombre,'')) AS nombre_full,
                        COALESCE(tc.nombre, c.nombre) AS tipo_txt,
                        ch.dia_semana,
                        ch.hora_inicio
                    FROM audit_logs l
                    LEFT JOIN usuarios u ON u.id = l.user_id
                    LEFT JOIN clases_horarios ch ON ch.id = l.record_id
                    LEFT JOIN clases c ON c.id = ch.clase_id
                    LEFT JOIN tipos_clases tc ON tc.id = c.tipo_clase_id
                    WHERE l.action IN ('auto_promote_waitlist','decline_waitlist_promotion')
                    ORDER BY l.id DESC
                    LIMIT 50
                    """
                )
            rows = cur.fetchall() or []

        items = []
        for r in rows:
            try:
                action = r.get("action") or ""
                nombre = r.get("nombre_full") or None
                tipo_txt = r.get("tipo_txt") or None
                dia_txt = r.get("dia_semana") or None
                hora_raw = r.get("hora_inicio")
                hora_txt = str(hora_raw) if hora_raw is not None else ""

                if action == "auto_promote_waitlist":
                    msg = f"Autopromoción: {nombre or 'Usuario'} inscrito automáticamente en {tipo_txt or 'la clase'} {('el ' + str(dia_txt)) if dia_txt else ''} {('a las ' + str(hora_txt)) if hora_txt else ''}."
                    tipo = "success"
                elif action == "decline_waitlist_promotion":
                    msg = f"Declinación: {nombre or 'Usuario'} declinó promoción a {tipo_txt or 'la clase'} {('el ' + str(dia_txt)) if dia_txt else ''} {('a las ' + str(hora_txt)) if hora_txt else ''}."
                    tipo = "info"
                else:
                    msg = "Actualización de lista de espera registrada."
                    tipo = "info"
                items.append({
                    "id": r.get("id"),
                    "action": action,
                    "message": msg,
                    "type": tipo,
                    "timestamp": r.get("timestamp"),
                })
            except Exception:
                pass
        return {"items": items}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/delinquency_alerts_recent")
async def api_delinquency_alerts_recent(request: Request, _=Depends(require_gestion_access)):
    """Alertas recientes de desactivación por morosidad para toasts globales.
    Filtra alertas del día actual registradas en audit_logs con table_name='alerts' y action='ALERT'.
    """
    db = _get_db()
    if db is None:
        return {"items": []}
    guard = _circuit_guard_json(db, "/api/delinquency_alerts_recent")
    if guard:
        return guard
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            # Traer alertas del día para reducir ruido; se filtra luego por título/categoría
            cur.execute(
                """
                SELECT id, user_id, new_values, timestamp
                FROM audit_logs
                WHERE table_name = 'alerts' AND action = 'ALERT'
                  AND timestamp >= date_trunc('day', now())
                ORDER BY id DESC
                LIMIT 200
                """
            )
            rows = cur.fetchall() or []
        items = []
        for r in rows:
            try:
                nv = r.get("new_values")
                alert = {}
                try:
                    alert = json.loads(nv) if nv else {}
                except Exception:
                    alert = {}
                title = (alert.get("title") or "").strip()
                category = (alert.get("category") or "").strip()
                message = (alert.get("message") or "").strip()
                # Match específico por título, con fallback por categoría + palabra clave
                is_delinquency = (title.lower() == "usuario desactivado por morosidad") or (
                    category.upper() == "PAYMENT" and ("morosidad" in message.lower())
                )
                if not is_delinquency:
                    continue
                uid = r.get("user_id") or alert.get("user_id")
                nombre = None
                try:
                    u = db.obtener_usuario_por_id(int(uid)) if uid is not None else None  # type: ignore
                    if u:
                        # Soportar tanto objeto como dict
                        nombre = getattr(u, "nombre", None)
                        if nombre is None and isinstance(u, dict):
                            nombre = u.get("nombre")
                except Exception:
                    nombre = None
                items.append({
                    "id": r.get("id"),
                    "user_id": uid,
                    "usuario_nombre": nombre,
                    "title": title or "Usuario desactivado por morosidad",
                    "message": message,
                    "timestamp": r.get("timestamp")
                })
            except Exception:
                # Ignorar filas mal formateadas
                continue
        return {"items": items}
    except Exception as e:
        try:
            logging.exception("Error in /api/delinquency_alerts_recent")
        except Exception:
            pass
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/asistencias_detalle")
async def api_asistencias_detalle(request: Request, _=Depends(require_owner)):
    """Listado de asistencias con nombre del usuario para un rango de fechas (por defecto últimos 30 días), con búsqueda y paginación."""
    db = _get_db()
    if db is None:
        return []
    guard = _circuit_guard_json(db, "/api/asistencias_detalle")
    if guard:
        return guard
    try:
        start = request.query_params.get("start")
        end = request.query_params.get("end")
        q = request.query_params.get("q")
        limit = request.query_params.get("limit")
        offset = request.query_params.get("offset")
        lim = int(limit) if limit and limit.isdigit() else 500
        off = int(offset) if offset and offset.isdigit() else 0
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            if start and end:
                if q:
                    cur.execute(
                        """
                        SELECT a.fecha::date, a.hora_registro, u.nombre
                        FROM asistencias a
                        JOIN usuarios u ON u.id = a.usuario_id
                        WHERE a.fecha BETWEEN %s AND %s AND (u.nombre ILIKE %s)
                        ORDER BY a.fecha DESC, a.hora_registro DESC
                        LIMIT %s OFFSET %s
                        """,
                        (start, end, f"%{q}%", lim, off)
                    )
                else:
                    cur.execute(
                        """
                        SELECT a.fecha::date, a.hora_registro, u.nombre
                        FROM asistencias a
                        JOIN usuarios u ON u.id = a.usuario_id
                        WHERE a.fecha BETWEEN %s AND %s
                        ORDER BY a.fecha DESC, a.hora_registro DESC
                        LIMIT %s OFFSET %s
                        """,
                        (start, end, lim, off)
                    )
            else:
                if q:
                    cur.execute(
                        """
                        SELECT a.fecha::date, a.hora_registro, u.nombre
                        FROM asistencias a
                        JOIN usuarios u ON u.id = a.usuario_id
                        WHERE a.fecha >= CURRENT_DATE - INTERVAL '30 days' AND (u.nombre ILIKE %s)
                        ORDER BY a.fecha DESC, a.hora_registro DESC
                        LIMIT %s OFFSET %s
                        """,
                        (f"%{q}%", lim, off)
                    )
                else:
                    cur.execute(
                        """
                        SELECT a.fecha::date, a.hora_registro, u.nombre
                        FROM asistencias a
                        JOIN usuarios u ON u.id = a.usuario_id
                        WHERE a.fecha >= CURRENT_DATE - INTERVAL '30 days'
                        ORDER BY a.fecha DESC, a.hora_registro DESC
                        LIMIT %s OFFSET %s
                        """,
                        (lim, off)
                    )
        rows = []
        for r in cur.fetchall():
            rows.append({
                "fecha": str(r[0]) if r[0] is not None else None,
                "hora": str(r[1]) if r[1] is not None else None,
                "usuario": r[2] or ''
            })
        return rows
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/export")
async def api_export(_=Depends(require_owner)):
    # Exporta CSV de KPIs visibles en el dashboard
    rows: list[list[Any]] = [["Métrica", "Valor"]]
    try:
        kpis = await api_kpis()  # type: ignore
        if isinstance(kpis, dict) and 'kpis' in kpis:
            rows.append(["Socios activos", kpis['kpis'].get('total_activos', 0)])
            rows.append(["Ingresos del mes", kpis['kpis'].get('ingresos_mes_actual', 0.0)])
            rows.append(["Asistencias hoy", kpis['kpis'].get('asistencias_hoy', 0)])
            rows.append(["Nuevos (30d)", kpis['kpis'].get('nuevos_30_dias', 0)])
            rows.append(["ARPU (mes)", kpis.get('arpu', 0.0)])
            rows.append(["Morosos", kpis.get('morosos', 0)])
    except Exception:
        pass

    # Generar CSV en memoria y devolver como texto
    from io import StringIO
    buf = StringIO()
    w = csv.writer(buf)
    w.writerows(rows)
    buf.seek(0)
    return JSONResponse({"csv": buf.getvalue()})

# --- Exportación completa en ZIP con múltiples CSVs ---
@app.get("/api/export_csv")
async def api_export_csv(request: Request, _=Depends(require_owner)):
    """Genera un ZIP con múltiples CSVs del dashboard aplicando filtros actuales.

    Incluye:
    - kpis.csv: KPIs generales
    - usuarios.csv: detalle de usuarios con conteos
    - asistencias.csv: asistencias por rango (start/end)
    - pagos.csv: pagos por rango (start/end)
    - profesores.csv: detalle de profesores con sesiones/horas del rango
    - profesor_sesiones.csv: sesiones trabajadas por profesores en el rango
    """
    db = _get_db()
    if db is None:
        return JSONResponse({"error": "DB no disponible"}, status_code=500)

    start = request.query_params.get("start")
    end = request.query_params.get("end")
    prof_start = request.query_params.get("prof_start") or start
    prof_end = request.query_params.get("prof_end") or end

    from io import StringIO, BytesIO
    import zipfile

    def write_csv_to_zip(zf: zipfile.ZipFile, fname: str, headers: list[str], rows: list[list]):
        buf = StringIO()
        w = csv.writer(buf)
        if headers:
            w.writerow(headers)
        for r in rows:
            w.writerow(r)
        zf.writestr(fname, buf.getvalue())

    zip_buf = BytesIO()
    zf = zipfile.ZipFile(zip_buf, mode="w", compression=zipfile.ZIP_DEFLATED)

    # 1) KPIs
    try:
        k = await api_kpis()  # type: ignore
        kpis_rows = [["Métrica", "Valor"]]
        if isinstance(k, dict) and 'kpis' in k:
            kpis_rows += [
                ["Socios activos", k['kpis'].get('total_activos', 0)],
                ["Ingresos del mes", k['kpis'].get('ingresos_mes_actual', 0.0)],
                ["Asistencias hoy", k['kpis'].get('asistencias_hoy', 0)],
                ["Nuevos (30d)", k['kpis'].get('nuevos_30_dias', 0)],
                ["ARPU (mes)", k.get('arpu', 0.0)],
                ["Morosos", k.get('morosos', 0)],
            ]
        write_csv_to_zip(zf, "kpis.csv", [], kpis_rows)
    except Exception:
        write_csv_to_zip(zf, "kpis.csv", [], [["Error obteniendo KPIs"]])

    # 2) Usuarios
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            cur.execute(
                """
                SELECT u.id,
                       COALESCE(u.nombre,'') AS nombre,
                       COALESCE(u.rol,'') AS rol,
                       COALESCE(u.dni,'') AS dni,
                       COALESCE(u.telefono,'') AS telefono,
                       COALESCE(u.tipo_cuota,'') AS tipo_cuota,
                       COALESCE(u.activo,false) AS activo,
                       COALESCE(u.fecha_registro::date, CURRENT_DATE) AS fecha_registro,
                       COALESCE(p.pagos_count,0) AS pagos_count,
                       p.last_pago_fecha::date AS last_pago_fecha,
                       COALESCE(a.asistencias_count,0) AS asistencias_count
                FROM usuarios u
                LEFT JOIN (
                    SELECT usuario_id, COUNT(*) AS pagos_count, MAX(fecha_pago) AS last_pago_fecha
                    FROM pagos
                    GROUP BY usuario_id
                ) p ON p.usuario_id = u.id
                LEFT JOIN (
                    SELECT usuario_id, COUNT(*) AS asistencias_count
                    FROM asistencias
                    GROUP BY usuario_id
                ) a ON a.usuario_id = u.id
                ORDER BY u.id
                """
            )
            urows = []
            for r in cur.fetchall():
                urows.append([
                    r[0], r[1], r[2], r[3], r[4], r[5], bool(r[6]),
                    str(r[7]) if r[7] is not None else '',
                    int(r[8] or 0),
                    str(r[9]) if r[9] is not None else '',
                    int(r[10] or 0),
                ])
        write_csv_to_zip(zf, "usuarios.csv", [
            "id","nombre","rol","dni","telefono","tipo_cuota","activo","fecha_registro","pagos_count","last_pago_fecha","asistencias_count"
        ], urows)
    except Exception:
        write_csv_to_zip(zf, "usuarios.csv", [], [["Error exportando usuarios"]])

    # 3) Asistencias por rango
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            if start and end:
                cur.execute(
                    """
                    SELECT a.fecha::date, a.hora_registro, u.nombre
                    FROM asistencias a
                    JOIN usuarios u ON u.id = a.usuario_id
                    WHERE a.fecha BETWEEN %s AND %s
                    ORDER BY a.fecha DESC, a.hora_registro DESC
                    """,
                    (start, end)
                )
            else:
                # Últimos 30 días por defecto
                cur.execute(
                    """
                    SELECT a.fecha::date, a.hora_registro, u.nombre
                    FROM asistencias a
                    JOIN usuarios u ON u.id = a.usuario_id
                    WHERE a.fecha >= CURRENT_DATE - INTERVAL '30 day'
                    ORDER BY a.fecha DESC, a.hora_registro DESC
                    """
                )
            arows = [[str(r[0]) if r[0] is not None else '', str(r[1]) if r[1] is not None else '', r[2] or ''] for r in cur.fetchall()]
        write_csv_to_zip(zf, "asistencias.csv", ["fecha","hora","usuario"], arows)
    except Exception:
        write_csv_to_zip(zf, "asistencias.csv", [], [["Error exportando asistencias"]])

    # 4) Pagos por rango
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            if start and end:
                cur.execute(
                    """
                    SELECT p.usuario_id, u.nombre, p.fecha_pago::date, p.monto, COALESCE(u.tipo_cuota,'')
                    FROM pagos p
                    JOIN usuarios u ON u.id = p.usuario_id
                    WHERE p.fecha_pago BETWEEN %s AND %s
                    ORDER BY p.fecha_pago DESC
                    """,
                    (start, end)
                )
            else:
                cur.execute(
                    """
                    SELECT p.usuario_id, u.nombre, p.fecha_pago::date, p.monto, COALESCE(u.tipo_cuota,'')
                    FROM pagos p
                    JOIN usuarios u ON u.id = p.usuario_id
                    WHERE p.fecha_pago >= CURRENT_DATE - INTERVAL '30 day'
                    ORDER BY p.fecha_pago DESC
                    """
                )
            prows = []
            for r in cur.fetchall():
                prows.append([
                    int(r[0] or 0), r[1] or '', str(r[2]) if r[2] is not None else '', float(r[3] or 0.0), r[4] or ''
                ])
        write_csv_to_zip(zf, "pagos.csv", ["usuario_id","usuario","fecha_pago","monto","tipo_cuota"], prows)
    except Exception:
        write_csv_to_zip(zf, "pagos.csv", [], [["Error exportando pagos"]])

    # 5) Profesores detalle
    try:
        prof_rows = []
        detalle = await api_profesores_detalle(request)  # type: ignore
        if isinstance(detalle, list):
            for p in detalle:
                # Resumen de horarios como texto
                horarios = p.get("horarios") or []
                resumen = "; ".join([
                    f"{h.get('dia')}: {h.get('inicio')}-{h.get('fin')}" for h in horarios if isinstance(h, dict)
                ])
                prof_rows.append([
                    p.get("id"), p.get("nombre"), p.get("telefono"), int(p.get("horarios_count", 0)), resumen,
                    int(p.get("sesiones_mes", 0)), float(p.get("horas_mes", 0.0))
                ])
        write_csv_to_zip(zf, "profesores.csv", ["id","nombre","telefono","horarios_count","horarios_resumen","sesiones_rango","horas_rango"], prof_rows)
    except Exception:
        write_csv_to_zip(zf, "profesores.csv", [], [["Error exportando profesores"]])

    # 6) Sesiones de profesores por rango
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            if prof_start and prof_end:
                cur.execute(
                    """
                    SELECT profesor_id,
                           fecha::date,
                           hora_inicio,
                           hora_fin,
                           COALESCE(minutos_totales,0) AS minutos,
                           COALESCE(horas_totales,0) AS horas,
                           COALESCE(tipo_actividad,'') AS tipo
                    FROM profesor_horas_trabajadas
                    WHERE hora_fin IS NOT NULL AND fecha BETWEEN %s AND %s
                    ORDER BY fecha DESC, profesor_id
                    """,
                    (prof_start, prof_end)
                )
            else:
                # Mes actual por defecto
                from datetime import datetime, date, timedelta
                now = datetime.now()
                start_date = date(now.year, now.month, 1)
                if now.month == 12:
                    end_date = date(now.year, 12, 31)
                else:
                    end_date = date(now.year, now.month + 1, 1) - timedelta(days=1)
                cur.execute(
                    """
                    SELECT profesor_id,
                           fecha::date,
                           hora_inicio,
                           hora_fin,
                           COALESCE(minutos_totales,0) AS minutos,
                           COALESCE(horas_totales,0) AS horas,
                           COALESCE(tipo_actividad,'') AS tipo
                    FROM profesor_horas_trabajadas
                    WHERE hora_fin IS NOT NULL AND fecha BETWEEN %s AND %s
                    ORDER BY fecha DESC, profesor_id
                    """,
                    (start_date, end_date)
                )
            srows = []
            for r in cur.fetchall():
                srows.append([
                    int(r[0] or 0), str(r[1]) if r[1] is not None else '', str(r[2]) if r[2] is not None else '', str(r[3]) if r[3] is not None else '',
                    int(r[4] or 0), float(r[5] or 0.0), r[6] or ''
                ])
        write_csv_to_zip(zf, "profesor_sesiones.csv", ["profesor_id","fecha","hora_inicio","hora_fin","minutos","horas","tipo"], srows)
    except Exception:
        write_csv_to_zip(zf, "profesor_sesiones.csv", [], [["Error exportando sesiones de profesores"]])

    # Finalizar ZIP y devolver
    zf.close()
    zip_buf.seek(0)
    return Response(content=zip_buf.getvalue(), media_type="application/zip", headers={
        "Content-Disposition": "attachment; filename=dashboard_export.zip"
    })
@app.get("/api/profesor_resumen")
async def api_profesor_resumen(request: Request, _=Depends(require_gestion_access)):
    """Resumen de horas trabajadas, proyectadas y extras (fuera de horario) para un profesor.

    - Trabajadas: suma de `minutos_totales` de sesiones cerradas en el rango.
    - Proyectadas: suma de minutos desde `horarios_profesores` (disponibilidades activas)
      por cada día del rango, contando ocurrencias por día de semana. No depende de clases.
    - Extras: horas fuera del horario establecido (cálculo en database.py).
    - Balance: trabajadas - proyectadas (se devuelve como minutos_balance/horas_balance).
    """
    db = _get_db()
    if db is None:
        return {
            "total_sesiones": 0,
            "minutos_trabajados": 0,
            "horas_trabajadas": 0.0,
            "minutos_proyectados": 0,
            "horas_proyectadas": 0.0,
            "minutos_extras": 0,
            "horas_extras": 0.0,
        }
    try:
        pid = request.query_params.get("profesor_id")
        role = request.session.get("role")
        profesor_id: Optional[int] = None
        if pid:
            try:
                profesor_id = int(pid)
            except Exception:
                profesor_id = None

        if role == "profesor":
            sess_prof_id = request.session.get("gestion_profesor_id")
            if sess_prof_id is None and request.session.get("gestion_profesor_user_id"):
                try:
                    with db.get_connection_context() as conn:  # type: ignore
                        cur = conn.cursor()
                        cur.execute("SELECT profesor_id FROM profesores WHERE usuario_id = %s", (request.session.get("gestion_profesor_user_id"),))
                        row = cur.fetchone()
                        if row and row[0]:
                            sess_prof_id = int(row[0])
                except Exception:
                    sess_prof_id = None
            if profesor_id is None:
                if sess_prof_id is None:
                    raise HTTPException(status_code=403, detail="profesor_profile_required")
                profesor_id = int(sess_prof_id)
            else:
                if sess_prof_id is None or int(profesor_id) != int(sess_prof_id):
                    raise HTTPException(status_code=403, detail="forbidden")
        else:
            if profesor_id is None:
                return {
                    "total_sesiones": 0,
                    "minutos_trabajados": 0,
                    "horas_trabajadas": 0.0,
                    "minutos_proyectados": 0,
                    "horas_proyectadas": 0.0,
                    "minutos_extras": 0,
                    "horas_extras": 0.0,
                }

        start = request.query_params.get("start")
        end = request.query_params.get("end")
        from datetime import datetime, date, timedelta
        now = datetime.now()
        if start and end:
            try:
                start_date = datetime.strptime(start, "%Y-%m-%d").date()
                end_date = datetime.strptime(end, "%Y-%m-%d").date()
            except Exception:
                start_date = date(now.year, now.month, 1)
                # último día del mes
                if now.month == 12:
                    end_date = date(now.year, 12, 31)
                else:
                    end_date = date(now.year, now.month + 1, 1) - timedelta(days=1)
        else:
            start_date = date(now.year, now.month, 1)
            if now.month == 12:
                end_date = date(now.year, 12, 31)
            else:
                end_date = date(now.year, now.month + 1, 1) - timedelta(days=1)

        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()

            # 1) Minutos trabajados y cantidad de sesiones cerradas
            cur.execute(
                """
                SELECT COALESCE(SUM(pht.minutos_totales),0) AS min_trab,
                       COUNT(*) AS total_ses
                FROM profesor_horas_trabajadas pht
                WHERE pht.profesor_id = %s
                  AND pht.hora_fin IS NOT NULL
                  AND pht.fecha BETWEEN %s AND %s
                """,
                (profesor_id, start_date, end_date)
            )
            r = cur.fetchone() or (0, 0)
            min_trabajados = int(r[0] or 0)
            total_sesiones = int(r[1] or 0)

        # 2) Minutos proyectados: cálculo centralizado en database.py (no depende de clases)
        min_proyectados = 0
        try:
            if hasattr(db, 'obtener_minutos_proyectados_profesor_rango'):
                res = db.obtener_minutos_proyectados_profesor_rango(  # type: ignore
                    profesor_id,
                    start_date.isoformat(),
                    end_date.isoformat()
                )
                if isinstance(res, dict) and res.get('success'):
                    min_proyectados = int(res.get('minutos_proyectados', 0) or 0)
        except Exception:
            min_proyectados = 0

        # 3) Horas extras fuera de horario: usar cálculo existente en database.py
        horas_extras_fuera = 0.0
        try:
            if hasattr(db, 'obtener_horas_extras_profesor'):
                res_ex = db.obtener_horas_extras_profesor(  # type: ignore
                    profesor_id,
                    start_date,
                    end_date
                )
                if isinstance(res_ex, dict) and res_ex.get('success'):
                    horas_extras_fuera = float(res_ex.get('total_horas_extras', 0) or 0.0)
        except Exception:
            horas_extras_fuera = 0.0

        return {
            "total_sesiones": total_sesiones,
            "minutos_trabajados": min_trabajados,
            "horas_trabajadas": round(min_trabajados / 60.0, 2),
            "minutos_proyectados": int(min_proyectados),
            "horas_proyectadas": round(min_proyectados / 60.0, 2),
            "minutos_extras": int(round(horas_extras_fuera * 60)),  # extras fuera de horario (min)
            "horas_extras": round(horas_extras_fuera, 2),           # extras fuera de horario (h)
            "minutos_balance": int(min_trabajados - min_proyectados),
            "horas_balance": round((min_trabajados - min_proyectados) / 60.0, 2),
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {
            "total_sesiones": 0,
            "minutos_trabajados": 0,
            "horas_trabajadas": 0.0,
            "minutos_proyectados": 0,
            "horas_proyectadas": 0.0,
            "minutos_extras": 0,
            "horas_extras": 0.0,
        }

@app.get("/api/profesores/{profesor_id}")
async def api_profesor_get(profesor_id: int, _=Depends(require_owner)):
    db = _get_db()
    if db is None:
        return JSONResponse({"error": "DB no disponible"}, status_code=500)
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            # Descubrir columnas disponibles en la tabla 'profesores' de forma segura
            try:
                cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='profesores'")
                cols = {row.get("column_name") for row in (cur.fetchall() or [])}
            except Exception:
                cols = set()
            selects = [
                "p.id AS id",
                "p.usuario_id AS usuario_id",
                "COALESCE(u.nombre,'') AS usuario_nombre",
                "COALESCE(u.telefono,'') AS usuario_telefono",
                "COALESCE(u.notas,'') AS usuario_notas",
            ]
            if "sueldo" in cols:
                selects.append("p.sueldo AS sueldo")
            elif "salario" in cols:
                selects.append("p.salario AS salario")
            if "notas" in cols:
                selects.append("p.notas AS notas")
            if "tipo" in cols:
                selects.append("p.tipo AS tipo")
            if "especialidades" in cols:
                selects.append("p.especialidades AS especialidades")
            if "certificaciones" in cols:
                selects.append("p.certificaciones AS certificaciones")
            if "experiencia_años" in cols:
                selects.append("p.experiencia_años AS experiencia_años")
            if "tarifa_por_hora" in cols:
                selects.append("p.tarifa_por_hora AS tarifa_por_hora")
            if "fecha_contratacion" in cols:
                selects.append("p.fecha_contratacion AS fecha_contratacion")
            if "biografia" in cols:
                selects.append("p.biografia AS biografia")
            if "telefono_emergencia" in cols:
                selects.append("p.telefono_emergencia AS telefono_emergencia")
            sql = f"SELECT {', '.join(selects)} FROM profesores p LEFT JOIN usuarios u ON u.id = p.usuario_id WHERE p.id = %s"
            cur.execute(sql, (profesor_id,))
            row = cur.fetchone()
            if not row:
                return JSONResponse({"error": "not_found"}, status_code=404)
            sueldo_val = None
            try:
                if "sueldo" in row and row["sueldo"] is not None:
                    sueldo_val = float(row["sueldo"])  # type: ignore
                elif "salario" in row and row["salario"] is not None:
                    sueldo_val = float(row["salario"])  # type: ignore
            except Exception:
                sueldo_val = row.get("sueldo") or row.get("salario") or None
            tarifa_val = None
            try:
                if "tarifa_por_hora" in row and row["tarifa_por_hora"] is not None:
                    tarifa_val = float(row["tarifa_por_hora"])  # type: ignore
            except Exception:
                tarifa_val = row.get("tarifa_por_hora") or None
            experiencia_val = None
            try:
                if "experiencia_años" in row and row["experiencia_años"] is not None:
                    experiencia_val = int(row["experiencia_años"])  # type: ignore
            except Exception:
                experiencia_val = row.get("experiencia_años") or None
            return {
                "profesor_id": int(row.get("id") or profesor_id),
                "usuario_id": int(row.get("usuario_id") or 0),
                "usuario_nombre": row.get("usuario_nombre") or "",
                "usuario_telefono": row.get("usuario_telefono") or "",
                "usuario_notas": row.get("usuario_notas"),
                "sueldo": sueldo_val,
                "notas": row.get("notas"),
                "tipo": row.get("tipo"),
                "especialidades": row.get("especialidades"),
                "certificaciones": row.get("certificaciones"),
                "experiencia_años": experiencia_val,
                "tarifa_por_hora": tarifa_val,
                "fecha_contratacion": row.get("fecha_contratacion"),
                "biografia": row.get("biografia"),
                "telefono_emergencia": row.get("telefono_emergencia"),
            }
    except Exception as e:
        logging.exception("Error en /api/profesores/{id}")
        return JSONResponse({"error": str(e)}, status_code=500)

@app.put("/api/profesores/{profesor_id}")
async def api_profesor_update(profesor_id: int, request: Request, _=Depends(require_owner)):
    db = _get_db()
    if db is None:
        return JSONResponse({"error": "DB no disponible"}, status_code=500)
    try:
        try:
            payload = await request.json()
        except Exception:
            payload = {}
        usuario_id = payload.get("usuario_id")
        sueldo = payload.get("sueldo")
        salario = payload.get("salario")
        notas = payload.get("notas")
        tipo = payload.get("tipo") or payload.get("especialidad")
        especialidades = payload.get("especialidades")
        certificaciones = payload.get("certificaciones")
        experiencia = payload.get("experiencia_años", payload.get("experiencia"))
        tarifa = payload.get("tarifa_por_hora", payload.get("tarifa"))
        fecha_contratacion = payload.get("fecha_contratacion")
        biografia = payload.get("biografia")
        telefono_emergencia = payload.get("telefono_emergencia")
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            # Descubrir columnas disponibles en 'profesores'
            try:
                cur2 = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur2.execute("SELECT column_name FROM information_schema.columns WHERE table_name='profesores'")
                cols = {r.get("column_name") for r in (cur2.fetchall() or [])}
            except Exception:
                cols = set()
            sets = []
            params = []
            pass
            # Determinar columna de sueldo/salario según exista
            if (sueldo is not None or salario is not None) and ("sueldo" in cols or "salario" in cols):
                val = sueldo if sueldo is not None else salario
                try:
                    val_num = float(val) if val is not None else None
                except Exception:
                    val_num = None
                if "sueldo" in cols:
                    sets.append("sueldo = %s")
                    params.append(val_num)
                elif "salario" in cols:
                    sets.append("salario = %s")
                    params.append(val_num)
            if "notas" in cols and notas is not None:
                sets.append("notas = %s")
                params.append(str(notas))
            # Nuevos campos
            if "tipo" in cols and tipo is not None:
                sets.append("tipo = %s")
                params.append(str(tipo))
            if "especialidades" in cols and especialidades is not None:
                sets.append("especialidades = %s")
                params.append(str(especialidades))
            if "certificaciones" in cols and certificaciones is not None:
                sets.append("certificaciones = %s")
                params.append(str(certificaciones))
            if "experiencia_años" in cols and experiencia is not None:
                try:
                    exp_num = int(experiencia)
                except Exception:
                    exp_num = 0
                sets.append("experiencia_años = %s")
                params.append(exp_num)
            if "tarifa_por_hora" in cols and tarifa is not None:
                try:
                    tarifa_num = float(tarifa)
                except Exception:
                    tarifa_num = 0.0
                sets.append("tarifa_por_hora = %s")
                params.append(tarifa_num)
            if "fecha_contratacion" in cols and fecha_contratacion is not None:
                sets.append("fecha_contratacion = %s")
                params.append(str(fecha_contratacion))
            if "biografia" in cols and biografia is not None:
                sets.append("biografia = %s")
                params.append(str(biografia))
            if "telefono_emergencia" in cols and telefono_emergencia is not None:
                sets.append("telefono_emergencia = %s")
                params.append(str(telefono_emergencia))
            if not sets:
                # Si no hay columnas de 'profesores' para actualizar, intentar guardar 'notas' en 'usuarios'
                updated = False
                try:
                    if notas is not None and "notas" not in cols:
                        uid = None
                        try:
                            uid = int(usuario_id) if usuario_id is not None else None
                        except Exception:
                            uid = None
                        if uid is None:
                            try:
                                cur2 = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                                cur2.execute("SELECT usuario_id FROM profesores WHERE id = %s", (int(profesor_id),))
                                row2 = cur2.fetchone() or {}
                                uid = int(row2.get("usuario_id")) if row2.get("usuario_id") is not None else None
                            except Exception:
                                uid = None
                        if uid:
                            cur.execute("UPDATE usuarios SET notas = %s WHERE id = %s", (str(notas), int(uid)))
                            try:
                                conn.commit()
                            except Exception:
                                pass
                            updated = True
                except Exception:
                    updated = False
                return {"success": updated, "updated": 1 if updated else 0}
            sql = f"UPDATE profesores SET {', '.join(sets)} WHERE id = %s"
            params.append(int(profesor_id))
            cur.execute(sql, tuple(params))
            # Fallback: si 'notas' no pertenece a 'profesores', guardar en 'usuarios'
            try:
                if notas is not None and "notas" not in cols:
                    uid2 = None
                    try:
                        uid2 = int(usuario_id) if usuario_id is not None else None
                    except Exception:
                        uid2 = None
                    if uid2 is None:
                        try:
                            cur3 = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                            cur3.execute("SELECT usuario_id FROM profesores WHERE id = %s", (int(profesor_id),))
                            row3 = cur3.fetchone() or {}
                            uid2 = int(row3.get("usuario_id")) if row3.get("usuario_id") is not None else None
                        except Exception:
                            uid2 = None
                    if uid2:
                        cur.execute("UPDATE usuarios SET notas = %s WHERE id = %s", (str(notas), int(uid2)))
                conn.commit()
            except Exception:
                pass
        # Devolver registro actualizado
        try:
            return await api_profesor_get(profesor_id)  # type: ignore
        except Exception:
            return {"success": True}
    except Exception as e:
        logging.exception("Error en PUT /api/profesores/{id}")
        return JSONResponse({"error": str(e)}, status_code=500)

# --- Endpoints de administración de WhatsApp (estado, estadísticas, control de servidor, configuración) ---

@app.get("/api/whatsapp/state")
async def api_whatsapp_state(_=Depends(require_gestion_access)):
    pm = _get_pm()
    db = _get_db()
    if db is None:
        return {"disponible": False, "habilitado": False, "servidor_activo": False, "configuracion_valida": False}
    guard = _circuit_guard_json(db, "/api/whatsapp/state")
    if guard:
        return guard
    if pm is None:
        return {"disponible": False, "habilitado": False, "servidor_activo": False, "configuracion_valida": False}
    try:
        return pm.obtener_estado_whatsapp()
    except Exception as e:
        import traceback; traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/whatsapp/stats")
async def api_whatsapp_stats(_=Depends(require_gestion_access)):
    pm = _get_pm()
    db = _get_db()
    if db is None:
        return {"error": "DB no disponible"}
    guard = _circuit_guard_json(db, "/api/whatsapp/stats")
    if guard:
        return guard
    if pm is None:
        return {"error": "PaymentManager no disponible"}
    try:
        return pm.obtener_estadisticas_whatsapp()
    except Exception as e:
        import traceback; traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

# --- Pendings, retry y limpieza de fallidos ---

@app.get("/api/whatsapp/pendings")
async def api_whatsapp_pendings(request: Request, _=Depends(require_gestion_access)):
    pm = _get_pm()
    db = _get_db()
    if db is None:
        return JSONResponse({"items": []})
    guard = _circuit_guard_json(db, "/api/whatsapp/pendings")
    if guard:
        return guard
    try:
        dias_param = request.query_params.get("dias")
        try:
            dias = int(dias_param) if dias_param else 30
        except Exception:
            dias = 30
        limite_param = request.query_params.get("limit")
        try:
            limite = int(limite_param) if limite_param else 200
        except Exception:
            limite = 200
        interval_str = f"{dias} days"
        items: list[dict[str, Any]] = []
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(
                """
                SELECT DISTINCT ON (wm.phone_number)
                       wm.id,
                       wm.user_id,
                       COALESCE(u.nombre,'') AS usuario_nombre,
                       COALESCE(u.telefono,'') AS usuario_telefono,
                       wm.phone_number,
                       wm.message_type,
                       wm.template_name,
                       wm.message_content,
                       wm.status,
                       wm.message_id,
                       wm.sent_at AS fecha_envio
                FROM whatsapp_messages wm
                LEFT JOIN usuarios u ON u.id = wm.user_id
                WHERE wm.status = 'failed'
                  AND wm.sent_at >= CURRENT_TIMESTAMP - (%s::interval)
                ORDER BY wm.phone_number, wm.sent_at DESC
                LIMIT %s
                """,
                (interval_str, limite)
            )
            for row in cur.fetchall() or []:
                try:
                    r = dict(row)
                except Exception:
                    r = row  # type: ignore
                # Asegurar serialización de fecha
                if r.get("fecha_envio") is not None:
                    try:
                        r["fecha_envio"] = str(r["fecha_envio"])  # ISO-like
                    except Exception:
                        pass
                items.append(r)
        return {"items": items}
    except Exception as e:
        import traceback; traceback.print_exc()
        return JSONResponse({"error": str(e), "items": []}, status_code=500)

@app.post("/api/whatsapp/retry")
async def api_whatsapp_retry(request: Request, _=Depends(require_owner)):
    pm = _get_pm()
    db = _get_db()
    if db is None:
        return JSONResponse({"success": False, "message": "DB no disponible"}, status_code=503)
    guard = _circuit_guard_json(db, "/api/whatsapp/retry")
    if guard:
        return guard
    if pm is None:
        return JSONResponse({"success": False, "message": "PaymentManager no disponible"}, status_code=503)
    try:
        try:
            payload = await request.json()
        except Exception:
            payload = {}
        telefono = str(payload.get("telefono") or payload.get("phone") or "").strip()
        usuario_id = payload.get("usuario_id")

        uid = None
        if usuario_id is not None:
            try:
                uid = int(usuario_id)
            except Exception:
                uid = None
        if uid is None and telefono:
            try:
                uid = db._obtener_user_id_por_telefono_whatsapp(telefono)  # type: ignore
            except Exception:
                uid = None
        if uid is None:
            return JSONResponse({"success": False, "message": "usuario_id no encontrado"}, status_code=400)

        last_type = None
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            if telefono:
                cur.execute(
                    """
                    SELECT message_type, template_name, message_content
                    FROM whatsapp_messages
                    WHERE phone_number = %s AND status = 'failed'
                    ORDER BY sent_at DESC
                    LIMIT 1
                    """,
                    (telefono,)
                )
            else:
                cur.execute(
                    """
                    SELECT message_type, template_name, message_content
                    FROM whatsapp_messages
                    WHERE user_id = %s AND status = 'failed'
                    ORDER BY sent_at DESC
                    LIMIT 1
                    """,
                    (uid,)
                )
            row = cur.fetchone() or {}
            try:
                last_type = (row.get("message_type") or "").strip().lower()
            except Exception:
                last_type = None

        if last_type in ("welcome", "bienvenida"):
            ok = pm.enviar_mensaje_bienvenida_whatsapp(int(uid))
            return {"success": bool(ok), "tipo": last_type or "welcome"}
        elif last_type in ("overdue", "recordatorio_vencida", "payment_reminder", "pago_recordatorio"):
            if getattr(pm, 'whatsapp_manager', None):
                ok = pm.whatsapp_manager.enviar_recordatorio_cuota_vencida(int(uid))
                return {"success": bool(ok), "tipo": last_type or "recordatorio_vencida"}
            else:
                return JSONResponse({"success": False, "message": "Gestor WhatsApp no disponible"}, status_code=503)
        elif last_type in ("class_reminder", "recordatorio_clase"):
            return JSONResponse({"success": False, "message": "recordatorio_clase requiere datos de clase"}, status_code=400)
        else:
            ok = pm.enviar_mensaje_bienvenida_whatsapp(int(uid))
            return {"success": bool(ok), "tipo": last_type or "welcome"}
    except Exception as e:
        import traceback; traceback.print_exc()
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)

@app.post("/api/whatsapp/clear_failures")
async def api_whatsapp_clear_failures(request: Request, _=Depends(require_owner)):
    pm = _get_pm()
    db = _get_db()
    if db is None:
        return JSONResponse({"success": False, "message": "DB no disponible"}, status_code=503)
    guard = _circuit_guard_json(db, "/api/whatsapp/clear_failures")
    if guard:
        return guard
    try:
        try:
            payload = await request.json()
        except Exception:
            payload = {}
        telefono = str(payload.get("telefono") or payload.get("phone") or "").strip()
        dias_param = payload.get("desde_dias") or payload.get("days")
        try:
            dias = int(dias_param) if dias_param is not None else 30
        except Exception:
            dias = 30
        interval_str = f"{dias} days"

        total_deleted = 0
        phones: list[str] = []
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            if telefono:
                cur.execute(
                    """
                    DELETE FROM whatsapp_messages
                    WHERE phone_number = %s AND status = 'failed'
                      AND sent_at >= CURRENT_TIMESTAMP - (%s::interval)
                    """,
                    (telefono, interval_str)
                )
                try:
                    total_deleted = int(cur.rowcount or 0)
                except Exception:
                    total_deleted = 0
                try:
                    conn.commit()
                except Exception:
                    pass
                phones = [telefono] if telefono else []
            else:
                # Obtener teléfonos con fallidos recientes y limpiar todos
                cur.execute(
                    """
                    SELECT DISTINCT phone_number
                    FROM whatsapp_messages
                    WHERE status = 'failed'
                      AND sent_at >= CURRENT_TIMESTAMP - (%s::interval)
                    ORDER BY phone_number
                    """,
                    (interval_str,)
                )
                phones = [r.get("phone_number") for r in (cur.fetchall() or []) if r.get("phone_number")]
                for ph in phones:
                    try:
                        cur.execute(
                            """
                            DELETE FROM whatsapp_messages
                            WHERE phone_number = %s AND status = 'failed'
                              AND sent_at >= CURRENT_TIMESTAMP - (%s::interval)
                            """,
                            (ph, interval_str)
                        )
                        total_deleted += int(cur.rowcount or 0)
                    except Exception:
                        pass
                try:
                    conn.commit()
                except Exception:
                    pass
        return {"success": True, "deleted": int(total_deleted), "phones": phones}
    except Exception as e:
        import traceback; traceback.print_exc()
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)

@app.post("/api/whatsapp/server/start")
async def api_whatsapp_server_start(_=Depends(require_owner)):
    pm = _get_pm()
    db = _get_db()
    if db is None:
        return JSONResponse({"success": False, "message": "DB no disponible"}, status_code=503)
    guard = _circuit_guard_json(db, "/api/whatsapp/server/start")
    if guard:
        return guard
    if pm is None:
        return JSONResponse({"success": False, "message": "PaymentManager no disponible"}, status_code=503)
    try:
        ok = pm.iniciar_servidor_whatsapp()
        return {"success": bool(ok)}
    except Exception as e:
        import traceback; traceback.print_exc()
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)

@app.post("/api/whatsapp/server/stop")
async def api_whatsapp_server_stop(_=Depends(require_owner)):
    pm = _get_pm()
    db = _get_db()
    if db is None:
        return JSONResponse({"success": False, "message": "DB no disponible"}, status_code=503)
    guard = _circuit_guard_json(db, "/api/whatsapp/server/stop")
    if guard:
        return guard
    if pm is None:
        return JSONResponse({"success": False, "message": "PaymentManager no disponible"}, status_code=503)
    try:
        ok = pm.detener_servidor_whatsapp()
        return {"success": bool(ok)}
    except Exception as e:
        import traceback; traceback.print_exc()
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)

@app.post("/api/whatsapp/config")
async def api_whatsapp_config(request: Request, _=Depends(require_owner)):
    pm = _get_pm()
    db = _get_db()
    if db is None:
        return JSONResponse({"success": False, "message": "DB no disponible"}, status_code=503)
    guard = _circuit_guard_json(db, "/api/whatsapp/config")
    if guard:
        return guard
    if pm is None:
        return JSONResponse({"success": False, "message": "PaymentManager no disponible"}, status_code=503)
    try:
        content_type = request.headers.get("content-type", "")
        if content_type.startswith("application/json"):
            data = await request.json()
        else:
            data = await request.form()
    except Exception:
        data = {}
    # Filtrar claves permitidas (configuración conocida)
    allowed_keys = {
        "phone_number_id", "whatsapp_business_account_id", "access_token",
        "allowlist_numbers", "allowlist_enabled", "enable_webhook",
        "max_retries", "retry_delay_seconds"
    }
    try:
        cfg = {k: (data.get(k)) for k in allowed_keys if (k in data)}
        # Normalizar booleanos
        for bk in ("allowlist_enabled", "enable_webhook"):
            if bk in cfg and cfg[bk] is not None:
                val = cfg[bk]
                if isinstance(val, bool):
                    cfg[bk] = "true" if val else "false"
                else:
                    cfg[bk] = str(val).strip().lower()
        ok = pm.configurar_whatsapp(cfg)
        try:
            pm.start_whatsapp_initialization(background=True, delay_seconds=1.5)
        except Exception:
            pass
        return {"success": bool(ok), "applied_keys": list(cfg.keys())}
    except Exception as e:
        import traceback; traceback.print_exc()
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)

# --- Endpoints de WhatsApp por usuario (acciones puntuales desde Gestión) ---

@app.post("/api/usuarios/{usuario_id}/whatsapp/bienvenida")
async def api_usuario_whatsapp_bienvenida(usuario_id: int, _=Depends(require_gestion_access)):
    pm = _get_pm()
    db = _get_db()
    if db is None:
        return JSONResponse({"success": False, "message": "DB no disponible"}, status_code=503)
    guard = _circuit_guard_json(db, "/api/usuarios/{id}/whatsapp/bienvenida")
    if guard:
        return guard
    if pm is None:
        return JSONResponse({"success": False, "message": "PaymentManager no disponible"}, status_code=503)
    try:
        ok = pm.enviar_mensaje_bienvenida_whatsapp(int(usuario_id))
        return {"success": bool(ok)}
    except Exception as e:
        import traceback; traceback.print_exc()
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)

@app.post("/api/usuarios/{usuario_id}/whatsapp/confirmacion_pago")
async def api_usuario_whatsapp_confirmacion_pago(usuario_id: int, request: Request, _=Depends(require_gestion_access)):
    """Envía confirmación de pago por WhatsApp para un usuario.
    Si no se proporcionan datos en el payload, intenta usar el último pago del usuario.
    """
    pm = _get_pm()
    db = _get_db()
    if db is None:
        return JSONResponse({"success": False, "message": "DB no disponible"}, status_code=503)
    guard = _circuit_guard_json(db, "/api/usuarios/{id}/whatsapp/confirmacion_pago")
    if guard:
        return guard
    if pm is None or not getattr(pm, 'whatsapp_manager', None):
        return JSONResponse({"success": False, "message": "Gestor WhatsApp no disponible"}, status_code=503)
    try:
        try:
            payload = await request.json()
        except Exception:
            payload = {}
        monto = payload.get("monto")
        mes = payload.get("mes") or payload.get("month")
        anio = payload.get("año") or payload.get("anio") or payload.get("year")

        nombre = None
        telefono = None
        try:
            u = db.obtener_usuario_por_id(int(usuario_id))  # type: ignore
            if u:
                nombre = getattr(u, 'nombre', None)
                telefono = getattr(u, 'telefono', None)
        except Exception:
            pass

        # Si faltan datos, intentar obtener el pago más reciente
        if (monto is None or mes is None or anio is None):
            try:
                # Usa PaymentManager para obtener historial y tomar el más reciente
                pagos = []
                if pm and hasattr(pm, 'obtener_historial_pagos'):
                    pagos = pm.obtener_historial_pagos(int(usuario_id), limit=1)  # type: ignore
                if pagos:
                    p0 = pagos[0]
                    monto = getattr(p0, 'monto', None)
                    mes = getattr(p0, 'mes', None)
                    anio = getattr(p0, 'año', None)
            except Exception:
                pass

        # Validar datos mínimos
        if not telefono or monto is None or mes is None or anio is None:
            return JSONResponse({"success": False, "message": "Datos insuficientes para confirmación"}, status_code=400)

        payment_data = {
            'user_id': int(usuario_id),
            'phone': str(telefono),
            'name': str(nombre or ""),
            'amount': float(monto),
            'date': f"{int(mes):02d}/{int(anio)}"
        }

        ok = pm.whatsapp_manager.send_payment_confirmation(payment_data)
        return {"success": bool(ok)}
    except Exception as e:
        import traceback; traceback.print_exc()
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)

@app.post("/api/usuarios/{usuario_id}/whatsapp/desactivacion")
async def api_usuario_whatsapp_desactivacion(usuario_id: int, request: Request, _=Depends(require_gestion_access)):
    """Envía notificación de desactivación por cuotas vencidas u otro motivo."""
    pm = _get_pm()
    db = _get_db()
    if db is None:
        return JSONResponse({"success": False, "message": "DB no disponible"}, status_code=503)
    guard = _circuit_guard_json(db, "/api/usuarios/{id}/whatsapp/desactivacion")
    if guard:
        return guard
    if pm is None or not getattr(pm, 'whatsapp_manager', None):
        return JSONResponse({"success": False, "message": "Gestor WhatsApp no disponible"}, status_code=503)
    try:
        try:
            payload = await request.json()
        except Exception:
            payload = {}
        motivo = (payload.get("motivo") or "cuotas vencidas").strip()
        ok = pm.whatsapp_manager.enviar_notificacion_desactivacion(usuario_id=int(usuario_id), motivo=motivo, force_send=True)
        return {"success": bool(ok)}
    except Exception as e:
        import traceback; traceback.print_exc()
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)

@app.post("/api/usuarios/{usuario_id}/whatsapp/recordatorio_vencida")
async def api_usuario_whatsapp_recordatorio_vencida(usuario_id: int, _=Depends(require_gestion_access)):
    pm = _get_pm()
    db = _get_db()
    if db is None:
        return JSONResponse({"success": False, "message": "DB no disponible"}, status_code=503)
    guard = _circuit_guard_json(db, "/api/usuarios/{id}/whatsapp/recordatorio_vencida")
    if guard:
        return guard
    if pm is None or not getattr(pm, 'whatsapp_manager', None):
        return JSONResponse({"success": False, "message": "Gestor WhatsApp no disponible"}, status_code=503)
    try:
        ok = pm.whatsapp_manager.enviar_recordatorio_cuota_vencida(int(usuario_id))
        return {"success": bool(ok)}
    except Exception as e:
        import traceback; traceback.print_exc()
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)

@app.post("/api/usuarios/{usuario_id}/whatsapp/recordatorio_clase")
async def api_usuario_whatsapp_recordatorio_clase(usuario_id: int, request: Request, _=Depends(require_gestion_access)):
    pm = _get_pm()
    db = _get_db()
    if db is None:
        return JSONResponse({"success": False, "message": "DB no disponible"}, status_code=503)
    guard = _circuit_guard_json(db, "/api/usuarios/{id}/whatsapp/recordatorio_clase")
    if guard:
        return guard
    if pm is None or not getattr(pm, 'whatsapp_manager', None):
        return JSONResponse({"success": False, "message": "Gestor WhatsApp no disponible"}, status_code=503)
    try:
        try:
            payload = await request.json()
        except Exception:
            payload = {}
        clase_info = {
            'tipo_clase': (payload.get('tipo_clase') or payload.get('clase_nombre') or ''),
            'fecha': (payload.get('fecha') or ''),
            'hora': (payload.get('hora') or ''),
        }
        ok = pm.whatsapp_manager.enviar_recordatorio_horario_clase(int(usuario_id), clase_info)
        return {"success": bool(ok)}
    except Exception as e:
        import traceback; traceback.print_exc()
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)

# Último mensaje de WhatsApp del usuario
@app.get("/api/usuarios/{usuario_id}/whatsapp/ultimo")
async def api_usuario_whatsapp_ultimo(usuario_id: int, request: Request, _=Depends(require_owner)):
    db = _get_db()
    if db is None:
        return JSONResponse({"success": False, "message": "DB no disponible"}, status_code=503)
    guard = _circuit_guard_json(db, "/api/usuarios/{id}/whatsapp/ultimo")
    if guard:
        return guard
    try:
        direccion = request.query_params.get("direccion") or None
        tipo = request.query_params.get("tipo") or None
        if direccion not in (None, "enviado", "recibido"):
            direccion = None
        item = db.obtener_ultimo_mensaje_whatsapp(user_id=int(usuario_id), telefono=None, message_type=tipo, direccion=direccion)  # type: ignore
        if not item:
            return {"success": True, "item": None}
        return {"success": True, "item": item}
    except Exception as e:
        import traceback; traceback.print_exc()
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)

# Historial de WhatsApp por usuario
@app.get("/api/usuarios/{usuario_id}/whatsapp/historial")
async def api_usuario_whatsapp_historial(usuario_id: int, request: Request, _=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        return JSONResponse({"success": False, "message": "DB no disponible"}, status_code=503)
    guard = _circuit_guard_json(db, "/api/usuarios/{id}/whatsapp/historial")
    if guard:
        return guard
    try:
        tipo = request.query_params.get("tipo") or None
        limite_q = request.query_params.get("limit")
        limite = 50
        try:
            limite = int(limite_q) if (limite_q and str(limite_q).isdigit()) else 50
        except Exception:
            limite = 50
        items = db.obtener_historial_mensajes_whatsapp(user_id=int(usuario_id), message_type=(tipo or None), limit=int(limite))  # type: ignore
        # Normalizar fechas a string para evitar problemas de serialización
        for it in items or []:
            for k in ("sent_at", "created_at"):
                if it.get(k) is not None:
                    try:
                        it[k] = str(it[k])
                    except Exception:
                        pass
        return {"success": True, "items": items}
    except Exception as e:
        import traceback; traceback.print_exc()
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)

# Borrar mensaje individual de WhatsApp por ID interno
@app.delete("/api/usuarios/{usuario_id}/whatsapp/{message_pk}")
async def api_usuario_whatsapp_delete(usuario_id: int, message_pk: int, request: Request, _=Depends(require_owner)):
    db = _get_db()
    if db is None:
        return JSONResponse({"success": False, "message": "DB no disponible"}, status_code=503)
    guard = _circuit_guard_json(db, "/api/usuarios/{id}/whatsapp/delete")
    if guard:
        return guard
    try:
        # Obtener valores previos para auditoría desde repositorio
        try:
            old_item = db.obtener_mensaje_whatsapp_por_pk(int(usuario_id), int(message_pk))  # type: ignore
        except Exception:
            old_item = None

        ok = bool(db.eliminar_mensaje_whatsapp_por_pk(int(usuario_id), int(message_pk)))  # type: ignore
        if not ok:
            return JSONResponse({"success": False, "message": "Mensaje no encontrado"}, status_code=404)

        # Registrar auditoría de eliminación
        try:
            ip_addr = (getattr(request, 'client', None).host if getattr(request, 'client', None) else None)
            ua = request.headers.get('user-agent', '')
            sid = getattr(getattr(request, 'session', {}), 'get', lambda *a, **k: None)('session_id')
            db.registrar_audit_log(  # type: ignore
                user_id=int(usuario_id),
                action="DELETE",
                table_name="whatsapp_messages",
                record_id=int(message_pk),
                old_values=json.dumps(old_item, default=str) if old_item else None,
                new_values=None,
                ip_address=ip_addr,
                user_agent=ua,
                session_id=sid,
            )
        except Exception:
            # No bloquear por fallos en auditoría
            pass

        return {"success": True, "deleted": int(message_pk)}
    except Exception as e:
        import traceback; traceback.print_exc()
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)

# Borrar mensaje individual de WhatsApp por message_id
@app.delete("/api/usuarios/{usuario_id}/whatsapp/by-mid/{message_id}")
async def api_usuario_whatsapp_delete_by_mid(usuario_id: int, message_id: str, request: Request, _=Depends(require_owner)):
    db = _get_db()
    if db is None:
        return JSONResponse({"success": False, "message": "DB no disponible"}, status_code=503)
    guard = _circuit_guard_json(db, "/api/usuarios/{id}/whatsapp/delete_by_mid")
    if guard:
        return guard
    try:
        # Obtener item previo y pk utilizando repositorio
        pk_id = None
        try:
            old_item = db.obtener_mensaje_whatsapp_por_message_id(int(usuario_id), str(message_id))  # type: ignore
            if old_item:
                try:
                    pk_id = int(old_item.get("id"))
                except Exception:
                    pk_id = None
        except Exception:
            old_item = None

        # Realizar borrado por message_id usando método del repositorio
        try:
            deleted = bool(db.eliminar_mensaje_whatsapp_por_message_id(int(usuario_id), str(message_id)))  # type: ignore
        except Exception:
            deleted = False

        if not deleted:
            return JSONResponse({"success": False, "message": "Mensaje no encontrado"}, status_code=404)

        # Registrar auditoría
        try:
            ip_addr = (getattr(request, 'client', None).host if getattr(request, 'client', None) else None)
            ua = request.headers.get('user-agent', '')
            sid = getattr(getattr(request, 'session', {}), 'get', lambda *a, **k: None)('session_id')
            db.registrar_audit_log(  # type: ignore
                user_id=int(usuario_id),
                action="DELETE",
                table_name="whatsapp_messages",
                record_id=int(pk_id) if pk_id is not None else None,
                old_values=json.dumps(old_item, default=str) if old_item else None,
                new_values=None,
                ip_address=ip_addr,
                user_agent=ua,
                session_id=sid,
            )
        except Exception:
            pass

        return {"success": True, "deleted_mid": str(message_id), "deleted_pk": int(pk_id) if pk_id is not None else None}
    except Exception as e:
        import traceback; traceback.print_exc()
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)

# --- Endpoint público: escaneo de QR de rutina por UUID ---
@app.get("/api/rutinas/qr_scan/{uuid_rutina}")
async def api_rutina_qr_scan(uuid_rutina: str, request: Request):
    """Valida UUID y retorna JSON con la rutina completa y ejercicios.

    Estructura: {
      "ok": true,
      "rutina": { id, uuid_rutina, nombre_rutina, descripcion, dias_semana, categoria, activa, ejercicios: [...], dias: [...] }
    }
    """
    db = _get_db()
    if db is None:
        return JSONResponse({"ok": False, "error": "DB no disponible"}, status_code=500)
    uid = str(uuid_rutina or "").strip()
    if not uid or len(uid) < 8:
        return JSONResponse({"ok": False, "error": "UUID inválido"}, status_code=400)
    try:
        rutina = db.obtener_rutina_completa_por_uuid_dict(uid)  # type: ignore
    except Exception:
        rutina = None
    if not rutina:
        return JSONResponse({"ok": False, "error": "Rutina no encontrada"}, status_code=404)
    if not bool(rutina.get("activa", True)):
        return JSONResponse({"ok": False, "error": "Rutina inactiva"}, status_code=403)

    # Restringir el acceso: si hay usuario logueado, el QR debe corresponder a su rutina activa
    try:
        usuario_id = request.session.get("usuario_id")
    except Exception:
        usuario_id = None
    if usuario_id is not None:
        try:
            rid_user = int(rutina.get("usuario_id")) if rutina.get("usuario_id") is not None else None
        except Exception:
            rid_user = None
        if rid_user is None or int(usuario_id) != int(rid_user):
            return JSONResponse({"ok": False, "error": "QR no corresponde a tu rutina"}, status_code=403)

        # Conceder acceso temporal por 1 día a esta rutina en la sesión del usuario
        try:
            request.session["qr_access_rutina_id"] = int(rutina.get("id") or 0)
            request.session["qr_access_until"] = int(time.time()) + 24 * 60 * 60
        except Exception:
            pass

    ejercicios = rutina.get("ejercicios") or []
    dias_map: dict[int, list[dict]] = {}
    for it in ejercicios:
        try:
            d = int(it.get("dia_semana") or 1)
        except Exception:
            d = 1
        dias_map.setdefault(d, []).append(it)
    dias = []
    for d in sorted(dias_map.keys()):
        items = dias_map[d]
        items_sorted = sorted(items, key=lambda x: int(x.get("orden") or 0))
        dias.append({
            "numero": d,
            "nombre": f"Día {d}",
            "ejercicios": [
                {
                    "nombre": (e.get("ejercicio") or {}).get("nombre"),
                    "descripcion": (e.get("ejercicio") or {}).get("descripcion"),
                    "video_url": (e.get("ejercicio") or {}).get("video_url"),
                    "series": e.get("series") or "",
                    "repeticiones": e.get("repeticiones") or "",
                    "ejercicio_id": int(e.get("ejercicio_id") or 0)
                }
                for e in items_sorted
            ]
        })
    out = dict(rutina)
    out["dias"] = dias
    return JSONResponse({"ok": True, "rutina": out}, status_code=200)

# --- Endpoint público de previsualización: escaneo de QR efímero por UUID ---
@app.get("/api/rutinas/preview/qr_scan/{uuid_rutina}")
async def api_rutina_preview_qr_scan(uuid_rutina: str):
    """Retorna JSON con la rutina efímera construida para previsualización.

    No requiere autenticación y expira automáticamente a los 10 minutos.
    """
    uid = str(uuid_rutina or "").strip()
    if not uid or len(uid) < 8:
        return JSONResponse({"ok": False, "error": "UUID inválido"}, status_code=400)
    rutina = _get_excel_preview_routine(uid)
    if not rutina:
        return JSONResponse({"ok": False, "error": "Rutina no encontrada o expirada"}, status_code=404)
    ejercicios = rutina.get("ejercicios") or []
    dias_map: dict[int, list[dict]] = {}
    for it in ejercicios:
        try:
            d = int(it.get("dia_semana") or 1)
        except Exception:
            d = 1
        dias_map.setdefault(d, []).append(it)
    dias = []
    for d in sorted(dias_map.keys()):
        items = dias_map[d]
        items_sorted = sorted(items, key=lambda x: int(x.get("orden") or 0))
        dias.append({
            "numero": d,
            "nombre": f"Día {d}",
            "ejercicios": [
                {
                    "nombre": (e.get("ejercicio") or {}).get("nombre"),
                    "descripcion": (e.get("ejercicio") or {}).get("descripcion"),
                    "video_url": (e.get("ejercicio") or {}).get("video_url"),
                    "series": e.get("series") or "",
                    "repeticiones": e.get("repeticiones") or "",
                    "ejercicio_id": int(e.get("ejercicio_id") or 0)
                }
                for e in items_sorted
            ]
        })
    out = dict(rutina)
    out["dias"] = dias
    return JSONResponse({"ok": True, "rutina": out}, status_code=200)
