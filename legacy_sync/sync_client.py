import os
import threading
import logging
import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from device_id import get_device_id

try:
    import requests  # type: ignore
except Exception:  # pragma: no cover
    requests = None  # type: ignore


_warned_proxy_down = False


def _resolve_base_url() -> str:
    """Resuelve la URL base para sincronización.

    Prioriza SIEMPRE la WebApp upstream cuando esté saludable y recae al proxy
    local solo si es estrictamente necesario.

    Upstream (preferido):
    - ENV var UPSTREAM_WEBAPP_BASE_URL
    - config/config.json -> upstream_webapp_base_url | webapp_base_url
    - Default https://gym-ms-zrk.up.railway.app

    Proxy local (fallback):
    - ENV var LOCAL_PROXY_BASE_URL
    - config/config.json -> local_proxy_base_url | local_proxy_url
    - Default http://127.0.0.1:8080
    """
    global _warned_proxy_down
    # Resolver upstream preferido
    upstream = os.getenv("UPSTREAM_WEBAPP_BASE_URL", "").strip()
    if not upstream:
        try:
            base_dir = os.path.dirname(os.path.abspath(__file__))
            cfg_path = os.path.join(base_dir, "config", "config.json")
            if os.path.exists(cfg_path):
                with open(cfg_path, "r", encoding="utf-8") as f:
                    data = json.load(f) or {}
                up = data.get("upstream_webapp_base_url") or data.get("webapp_base_url")
                if isinstance(up, str) and up.strip():
                    upstream = up.strip()
        except Exception:
            upstream = ""
    if not upstream:
        upstream = "https://gym-ms-zrk.up.railway.app"
    upstream = upstream.rstrip("/")

    # Resolver proxy local como fallback
    proxy = os.getenv("LOCAL_PROXY_BASE_URL", "").strip()
    if not proxy:
        try:
            base_dir = os.path.dirname(os.path.abspath(__file__))
            cfg_path = os.path.join(base_dir, "config", "config.json")
            if os.path.exists(cfg_path):
                with open(cfg_path, "r", encoding="utf-8") as f:
                    data = json.load(f) or {}
                cfg_url = data.get("local_proxy_base_url") or data.get("local_proxy_url")
                if isinstance(cfg_url, str) and cfg_url.strip():
                    proxy = cfg_url.strip()
        except Exception:
            proxy = ""
    if not proxy:
        proxy = "http://127.0.0.1:8080"
    proxy = proxy.rstrip("/")

    # Si requests no está disponible, retornar upstream por defecto
    if requests is None:
        return upstream

    # Chequear salud de upstream primero
    try:
        ru = requests.get(upstream + "/healthz", timeout=1.5)
        if ru.status_code == 200:
            return upstream
    except Exception:
        pass

    # Upstream no responde: intentar proxy local
    try:
        rp = requests.get(proxy + "/healthz", timeout=1.5)
        if rp.status_code == 200:
            try:
                if not _warned_proxy_down:
                    logging.warning("sync_client: upstream no disponible; usando proxy local como fallback")
                    _warned_proxy_down = True
            except Exception:
                pass
            return proxy
    except Exception:
        pass

    # Último recurso: devolver proxy para encolar offline
    try:
        if not _warned_proxy_down:
            logging.warning(f"sync_client: upstream y proxy no respondieron salud; usando proxy {proxy}")
            _warned_proxy_down = True
    except Exception:
        pass
    return proxy


def resolve_auth_headers() -> Dict[str, str]:
    """Devuelve encabezados de autorización si se configura SYNC_API_TOKEN.

    Busca primero en env `SYNC_API_TOKEN` y luego en `config/config.json` clave
    `sync_api_token`. Si existe, retorna `{"Authorization": "Bearer <token>"}`.
    """
    token = os.getenv("SYNC_API_TOKEN", "").strip()
    if not token:
        try:
            base_dir = os.path.dirname(os.path.abspath(__file__))
            cfg_path = os.path.join(base_dir, "config", "config.json")
            if os.path.exists(cfg_path):
                with open(cfg_path, "r", encoding="utf-8") as f:
                    data = json.load(f) or {}
                t = data.get("sync_api_token")
                if isinstance(t, str) and t.strip():
                    token = t.strip()
        except Exception:
            token = ""
    if token:
        return {"Authorization": f"Bearer {token}"}
    return {}


def _post_async(url: str, payload: Dict[str, Any]) -> None:
    """Fire-and-forget POST to the local proxy in a daemon thread."""
    def _run():
        if requests is None:
            logging.debug("sync_client: requests no disponible; omitiendo POST")
            return
        try:
            headers = {"Content-Type": "application/json"}
            try:
                auth = resolve_auth_headers()
                if auth:
                    headers.update(auth)
            except Exception:
                pass
            # Usar json= para serialización segura
            resp = requests.post(url, json=payload, headers=headers, timeout=2.5)
            # Aceptamos 2xx y 202 (enqueue)
            if not (200 <= resp.status_code < 300):
                try:
                    logging.warning(f"sync_client: POST {url} status={resp.status_code} body={resp.text[:200]}")
                except Exception:
                    logging.warning(f"sync_client: POST {url} status={resp.status_code}")
        except Exception as e:  # pragma: no cover
            logging.debug(f"sync_client: fallo POST {url}: {e}")

    t = threading.Thread(target=_run, daemon=True)
    t.start()


_MAX_BATCH = 200  # tamaño máximo por lote para evitar payloads enormes


def _is_valid_op(op: Dict[str, Any]) -> bool:
    try:
        t = op.get("type") or op.get("name")
        p = op.get("payload") or op.get("data")
        return isinstance(t, str) and t.strip() != "" and isinstance(p, dict)
    except Exception:
        return False


def _enrich_ops(operations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    device_id = get_device_id()
    enriched: List[Dict[str, Any]] = []
    for op in operations:
        if not isinstance(op, dict):
            continue
        if not _is_valid_op(op):
            try:
                logging.warning(f"sync_client: operación inválida descartada: {op}")
            except Exception:
                pass
            continue
        op_copy = dict(op)
        # Normalizar campos
        if "type" not in op_copy and op_copy.get("name"):
            op_copy["type"] = op_copy.pop("name")
        if "payload" not in op_copy and op_copy.get("data"):
            op_copy["payload"] = op_copy.pop("data")
        if "ts" not in op_copy or not isinstance(op_copy["ts"], str):
            op_copy["ts"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        if "op_id" not in op_copy:
            op_copy["op_id"] = str(uuid.uuid4())
        src = op_copy.get("source") or {}
        if not isinstance(src, dict):
            src = {}
        if not src.get("device_id"):
            src["device_id"] = device_id
        op_copy["source"] = src
        enriched.append(op_copy)
    return enriched


def enqueue_operations(operations: List[Dict[str, Any]]) -> None:
    """Send a batch of operations to the local proxy upload queue.

    operations: list of { type: str, payload: dict, ts: iso8601, op_id?: str, source?: {device_id} }
    """
    if not operations:
        return
    # Validación y enriquecimiento: op_id, device_id, normalización
    enriched: List[Dict[str, Any]] = _enrich_ops(operations)
    if not enriched:
        return
    base = _resolve_base_url()
    url = f"{base}/api/sync/upload"
    # Enviar en lotes para evitar cuerpos gigantes
    i = 0
    while i < len(enriched):
        chunk = enriched[i:i + _MAX_BATCH]
        payload = {
            "operations": chunk,
            "client_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "device_id": get_device_id(),
            "sync_flags": {"batched": True, "chunk_size": len(chunk)},
        }
        _post_async(url, payload)
        i += _MAX_BATCH


def _op(op_type: str, payload: Dict[str, Any], ts: Optional[str] = None) -> Dict[str, Any]:
    return {
        "type": op_type,
        "payload": payload or {},
        "ts": ts or (datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")),
        "op_id": str(uuid.uuid4()),
        "source": {"device_id": get_device_id()},
    }


# Helper builders for common ops used by ActionHistoryManager
def op_user_add(data: Dict[str, Any]) -> Dict[str, Any]:
    return _op("user.add", data)


def op_user_update(data: Dict[str, Any]) -> Dict[str, Any]:
    return _op("user.update", data)


def op_user_delete(data: Dict[str, Any]) -> Dict[str, Any]:
    return _op("user.delete", data)


def op_routine_assign(data: Dict[str, Any]) -> Dict[str, Any]:
    return _op("routine.assign", data)


def op_routine_unassign(data: Dict[str, Any]) -> Dict[str, Any]:
    return _op("routine.unassign", data)


# ---- Routine helpers ----
def op_routine_add(data: Dict[str, Any]) -> Dict[str, Any]:
    return _op("routine.add", data)


def op_routine_update(data: Dict[str, Any]) -> Dict[str, Any]:
    return _op("routine.update", data)


def op_routine_delete(data: Dict[str, Any]) -> Dict[str, Any]:
    return _op("routine.delete", data)


# ---- Routine exercise helpers ----
def op_routine_exercise_add(data: Dict[str, Any]) -> Dict[str, Any]:
    return _op("routine_exercise.add", data)


def op_routine_exercise_update(data: Dict[str, Any]) -> Dict[str, Any]:
    return _op("routine_exercise.update", data)


def op_routine_exercise_delete(data: Dict[str, Any]) -> Dict[str, Any]:
    return _op("routine_exercise.delete", data)


# ---- Exercise catalog helpers ----
def op_exercise_add(data: Dict[str, Any]) -> Dict[str, Any]:
    return _op("exercise.add", data)


def op_exercise_update(data: Dict[str, Any]) -> Dict[str, Any]:
    return _op("exercise.update", data)


def op_exercise_delete(data: Dict[str, Any]) -> Dict[str, Any]:
    return _op("exercise.delete", data)


# ---- Payment helpers ----
def op_payment_add(data: Dict[str, Any]) -> Dict[str, Any]:
    return _op("payment.add", data)


def op_payment_update(data: Dict[str, Any]) -> Dict[str, Any]:
    return _op("payment.update", data)


def op_payment_delete(data: Dict[str, Any]) -> Dict[str, Any]:
    return _op("payment.delete", data)


# ---- Tag helpers ----
def op_tag_add(data: Dict[str, Any]) -> Dict[str, Any]:
    return _op("tag.add", data)


def op_tag_update(data: Dict[str, Any]) -> Dict[str, Any]:
    return _op("tag.update", data)


def op_tag_delete(data: Dict[str, Any]) -> Dict[str, Any]:
    return _op("tag.delete", data)


# ---- User-Tag helpers ----
def op_user_tag_add(data: Dict[str, Any]) -> Dict[str, Any]:
    return _op("user_tag.add", data)


def op_user_tag_update(data: Dict[str, Any]) -> Dict[str, Any]:
    return _op("user_tag.update", data)


def op_user_tag_delete(data: Dict[str, Any]) -> Dict[str, Any]:
    return _op("user_tag.delete", data)


# ---- Note helpers ----
def op_note_add(data: Dict[str, Any]) -> Dict[str, Any]:
    return _op("note.add", data)


def op_note_update(data: Dict[str, Any]) -> Dict[str, Any]:
    return _op("note.update", data)


def op_note_delete(data: Dict[str, Any]) -> Dict[str, Any]:
    return _op("note.delete", data)


# ---- Attendance helpers ----
def op_attendance_add(data: Dict[str, Any]) -> Dict[str, Any]:
    return _op("attendance.add", data)


def op_attendance_update(data: Dict[str, Any]) -> Dict[str, Any]:
    return _op("attendance.update", data)


def op_attendance_delete(data: Dict[str, Any]) -> Dict[str, Any]:
    return _op("attendance.delete", data)


# ---- Class attendance helpers ----
def op_class_attendance_add(data: Dict[str, Any]) -> Dict[str, Any]:
    return _op("class_attendance.add", data)


def op_class_attendance_update(data: Dict[str, Any]) -> Dict[str, Any]:
    return _op("class_attendance.update", data)


def op_class_attendance_delete(data: Dict[str, Any]) -> Dict[str, Any]:
    return _op("class_attendance.delete", data)


# ---- Professor schedule helpers ----
def op_professor_schedule_add(data: Dict[str, Any]) -> Dict[str, Any]:
    return _op("professor_schedule.add", data)


def op_professor_schedule_update(data: Dict[str, Any]) -> Dict[str, Any]:
    return _op("professor_schedule.update", data)


def op_professor_schedule_delete(data: Dict[str, Any]) -> Dict[str, Any]:
    return _op("professor_schedule.delete", data)


# ---- Professor substitution helpers ----
def op_professor_substitution_add(data: Dict[str, Any]) -> Dict[str, Any]:
    return _op("professor_substitution.add", data)


def op_professor_substitution_update(data: Dict[str, Any]) -> Dict[str, Any]:
    return _op("professor_substitution.update", data)


def op_professor_substitution_delete(data: Dict[str, Any]) -> Dict[str, Any]:
    return _op("professor_substitution.delete", data)


# ---- Class helpers ----
def op_class_add(data: Dict[str, Any]) -> Dict[str, Any]:
    return _op("class.add", data)


def op_class_update(data: Dict[str, Any]) -> Dict[str, Any]:
    return _op("class.update", data)


def op_class_delete(data: Dict[str, Any]) -> Dict[str, Any]:
    return _op("class.delete", data)


# ---- Class schedule helpers ----
def op_class_schedule_add(data: Dict[str, Any]) -> Dict[str, Any]:
    return _op("class_schedule.add", data)


def op_class_schedule_update(data: Dict[str, Any]) -> Dict[str, Any]:
    return _op("class_schedule.update", data)


def op_class_schedule_delete(data: Dict[str, Any]) -> Dict[str, Any]:
    return _op("class_schedule.delete", data)


# ---- Class membership helpers ----
def op_class_membership_add(data: Dict[str, Any]) -> Dict[str, Any]:
    return _op("class_membership.add", data)


def op_class_membership_update(data: Dict[str, Any]) -> Dict[str, Any]:
    return _op("class_membership.update", data)


def op_class_membership_delete(data: Dict[str, Any]) -> Dict[str, Any]:
    return _op("class_membership.delete", data)