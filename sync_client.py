# -*- coding: utf-8 -*-
"""
sync_client.py — Cliente de sincronización ligero

Objetivo: capturar operaciones locales relevantes y persistirlas en una cola
durable para observabilidad y posibles integraciones futuras. La replicación
de datos entre bases se realiza por PostgreSQL (publications/subscriptions);
este cliente NO envía datos, sólo registra operaciones.

Persistencia: archivo JSON en `config/sync_state.json` con estructura:
{
  "version": 1,
  "queue": [ { op } ],
  "last_flush_ts": <epoch_seconds>
}

Se limita el tamaño de la cola y se realiza deduplicación básica por clave
natural dependiendo del tipo de operación.
"""

import json
import os
import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_DIR = os.path.join(BASE_DIR, 'config')
STATE_PATH = os.path.join(CONFIG_DIR, 'sync_state.json')

_LOCK = threading.RLock()
_MAX_QUEUE = 1000


def _now_iso() -> str:
    try:
        return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    except Exception:
        return datetime.utcnow().isoformat() + "Z"


def _ensure_state() -> Dict[str, Any]:
    os.makedirs(CONFIG_DIR, exist_ok=True)
    state: Dict[str, Any] = {"version": 1, "queue": [], "last_flush_ts": None}
    if os.path.exists(STATE_PATH):
        try:
            with open(STATE_PATH, 'r', encoding='utf-8') as f:
                raw = f.read().strip()
                if raw:
                    cur = json.loads(raw)
                    if isinstance(cur, dict):
                        state.update(cur)
                        if 'queue' not in state or not isinstance(state['queue'], list):
                            state['queue'] = []
        except Exception:
            # Si el archivo está corrupto, se re-inicializa
            state = {"version": 1, "queue": [], "last_flush_ts": None}
    return state


def _save_state(state: Dict[str, Any]) -> None:
    try:
        # Escribir de forma atómica simple
        tmp_path = STATE_PATH + ".tmp"
        with open(tmp_path, 'w', encoding='utf-8') as f:
            json.dump(state, f, ensure_ascii=False)
        os.replace(tmp_path, STATE_PATH)
    except Exception:
        # Mejor esfuerzo: fallback a escritura directa
        try:
            with open(STATE_PATH, 'w', encoding='utf-8') as f:
                json.dump(state, f, ensure_ascii=False)
        except Exception:
            pass


def _dedup_key(op: Dict[str, Any]) -> Optional[str]:
    """Construye una clave de deduplicación por tipo."""
    t = str(op.get('type') or '')
    p = op.get('payload') or {}
    try:
        if t == 'payment.update':
            # Clave natural: usuario_id + mes + año
            return f"pay:{int(p.get('user_id'))}:{int(p.get('mes'))}:{int(p.get('año'))}"
        if t == 'payment.delete':
            return f"paydel:{int(p.get('user_id'))}:{int(p.get('mes'))}:{int(p.get('año'))}"
        if t == 'attendance.update':
            # usuario_id + fecha (YYYY-MM-DD)
            return f"att:{int(p.get('user_id'))}:{str(p.get('fecha') or '')}"
        if t == 'attendance.delete':
            return f"attdel:{int(p.get('user_id'))}:{str(p.get('fecha') or '')}"
        if t == 'user.add':
            # DNI si disponible, sino nombre+tel combinados
            dni = p.get('dni')
            if dni:
                return f"uadd:{str(dni)}"
            return f"uadd:{str(p.get('nombre') or '')}:{str(p.get('telefono') or '')}"
        if t == 'user.update':
            return f"uupd:{int(p.get('user_id'))}"
        if t == 'user.delete':
            return f"udel:{int(p.get('user_id'))}"
        if t == 'routine.assign':
            return f"rassign:{int(p.get('user_id'))}:{int(p.get('routine_id'))}"
        if t == 'routine.unassign':
            return f"runassign:{int(p.get('user_id'))}:{int(p.get('routine_id'))}"
    except Exception:
        pass
    return None


def enqueue_operations(ops: List[Dict[str, Any]]) -> bool:
    """Añade operaciones a la cola persistente.

    - Limita tamaño máximo
    - Deduplica por _dedup_key (reemplaza la anterior)
    - Enriquecer con timestamp si falta
    """
    if not ops:
        return False
    with _LOCK:
        state = _ensure_state()
        queue: List[Dict[str, Any]] = state.get('queue') or []
        # Índice para deduplicación por clave
        index: Dict[str, int] = {}
        for i, existing in enumerate(queue):
            k = existing.get('dedup_key')
            if k:
                index[k] = i
        # Incorporar nuevas operaciones
        for op in ops:
            if 'ts' not in op:
                op['ts'] = _now_iso()
            k = _dedup_key(op)
            if k:
                op['dedup_key'] = k
                if k in index:
                    # Reemplazar la entrada existente
                    queue[index[k]] = op
                else:
                    index[k] = len(queue)
                    queue.append(op)
            else:
                queue.append(op)
        # Aplicar límite de tamaño
        if len(queue) > _MAX_QUEUE:
            queue = queue[-_MAX_QUEUE:]
        state['queue'] = queue
        state['last_flush_ts'] = time.time()
        _save_state(state)
        return True


def get_pending_count() -> int:
    """Devuelve el número de operaciones pendientes en la cola."""
    try:
        state = _ensure_state()
        q = state.get('queue') or []
        return int(len(q))
    except Exception:
        return 0


# ===== Helpers de construcción de operaciones =====

def _op(t: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    return {"type": t, "payload": payload, "ts": _now_iso()}


def op_payment_update(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _op('payment.update', payload)


def op_payment_delete(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _op('payment.delete', payload)


def op_attendance_update(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _op('attendance.update', payload)


def op_attendance_delete(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _op('attendance.delete', payload)


def op_user_add(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _op('user.add', payload)


def op_user_update(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _op('user.update', payload)


def op_user_delete(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _op('user.delete', payload)


def op_routine_assign(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _op('routine.assign', payload)


def op_routine_unassign(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _op('routine.unassign', payload)


# Espacio reservado para futuras operaciones de etiquetas/notas si hiciera falta
def op_tag_add(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _op('tag.add', payload)


def op_tag_update(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _op('tag.update', payload)


def op_tag_delete(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _op('tag.delete', payload)


def op_note_add(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _op('note.add', payload)


def op_note_update(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _op('note.update', payload)


def op_note_delete(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _op('note.delete', payload)


def op_user_tag_add(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _op('user_tag.add', payload)


def op_user_tag_update(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _op('user_tag.update', payload)


def op_user_tag_delete(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _op('user_tag.delete', payload)