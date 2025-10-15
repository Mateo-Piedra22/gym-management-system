import os
import json
import uuid
from datetime import date
from typing import Dict, Any, List, Optional, Tuple

# Ensure project root on path to import sync helpers
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import logging

try:
    import requests  # type: ignore
except Exception:  # pragma: no cover
    requests = None  # type: ignore

from sync_client import (
    _resolve_base_url,
    op_user_add,
    op_user_update,
    op_user_delete,
    op_payment_add,
    op_payment_update,
    op_payment_delete,
    op_attendance_add,
    op_attendance_update,
    op_attendance_delete,
    op_note_add,
    op_note_update,
    op_note_delete,
    op_tag_add,
    op_tag_update,
    op_tag_delete,
    op_user_tag_add,
    op_user_tag_delete,
    op_exercise_add,
    op_exercise_update,
    op_exercise_delete,
    op_routine_add,
    op_routine_update,
    op_routine_delete,
    op_routine_exercise_add,
    op_routine_exercise_update,
    op_routine_exercise_delete,
    op_class_add,
    op_class_update,
    op_class_delete,
    op_class_schedule_add,
    op_class_schedule_update,
    op_class_schedule_delete,
    op_class_membership_add,
    op_class_membership_delete,
    op_class_attendance_add,
    op_class_attendance_update,
    op_class_attendance_delete,
)


def _http_get_json(url: str, params: Optional[Dict[str, Any]] = None, timeout: float = 10.0) -> Tuple[int, Any]:
    r = requests.get(url, params=params or {}, timeout=timeout)
    try:
        return r.status_code, r.json()
    except Exception:
        return r.status_code, r.text


def _http_post_json(url: str, body: Dict[str, Any], timeout: float = 15.0) -> Tuple[int, Any]:
    headers = {"Content-Type": "application/json"}
    r = requests.post(url, json=body, headers=headers, timeout=timeout)
    try:
        return r.status_code, r.json()
    except Exception:
        return r.status_code, r.text


def _ensure_device(op: Dict[str, Any], device_id: str) -> Dict[str, Any]:
    o = dict(op)
    src = dict(o.get("source") or {})
    src["device_id"] = device_id
    o["source"] = src
    if not o.get("op_id"):
        o["op_id"] = uuid.uuid4().hex
    return o


def build_phase_operations(token: str, dni: str, uid_hint: int) -> List[List[Dict[str, Any]]]:
    """Construye operaciones en fases para minimizar abortos de transacción."""
    today = date.today()
    try:
        seed = int(str(token), 16) % 10000
    except Exception:
        seed = uid_hint % 10000
    exercise_id = 800000 + seed
    routine_id = 810000 + seed
    routine_exercise_id = 815000 + seed
    note_id = 820000 + seed
    tag_id = 825000 + seed

    phase1: List[Dict[str, Any]] = [
        op_user_add({
            "dni": dni,
            "nombre": f"Usuario API {uid_hint} {token}",
            "telefono": "000-999",
            "tipo_cuota": f"cuota_{token}",
            "rol": "socio",
        }),
        op_user_update({
            "dni": dni,
            "telefono": "000-123",
            "tipo_cuota": f"cuota_{token}",
            "active": True,
        }),
        op_tag_add({
            "id": tag_id,
            "nombre": f"tag_{token}",
            "color": f"#%06x" % (seed % 0xFFFFFF),
            "descripcion": "Etiqueta de prueba",
        }),
        op_tag_update({
            "id": tag_id,
            "descripcion": "Etiqueta actualizada",
        }),
    ]

    phase2: List[Dict[str, Any]] = [
        op_user_tag_add({
            "dni": dni,
            "etiqueta_id": tag_id,
        }),
        op_user_tag_delete({
            "dni": dni,
            "etiqueta_id": tag_id,
        }),
    ]

    phase3: List[Dict[str, Any]] = [
        op_exercise_add({
            "id": exercise_id,
            "nombre": f"Press {token}",
            "grupo_muscular": "pecho",
            "descripcion": "Ejercicio de prueba",
        }),
        op_exercise_update({
            "id": exercise_id,
            "descripcion": "Ejercicio actualizado",
        }),
        op_exercise_delete({
            "id": exercise_id,
        }),
    ]

    phase4: List[Dict[str, Any]] = [
        op_routine_add({
            "id": routine_id,
            "dni": dni,
            "nombre_rutina": f"Rutina {token}",
            "descripcion": "Rutina de prueba",
            "dias_semana": "L,M,V",
            "categoria": "general",
            "activa": True,
        }),
        op_routine_exercise_add({
            "id": routine_exercise_id,
            "rutina_id": routine_id,
            "ejercicio_id": exercise_id,  # aunque borrado en fase3, servidor debe ignorar si no existe
            "dia_semana": "L",
            "series": 3,
            "repeticiones": "10",
            "orden": 1,
        }),
        op_routine_exercise_update({
            "id": routine_exercise_id,
            "series": 4,
        }),
        op_routine_exercise_delete({
            "id": routine_exercise_id,
        }),
        op_routine_update({
            "id": routine_id,
            "activa": False,
        }),
        op_routine_delete({
            "id": routine_id,
        }),
    ]

    phase5: List[Dict[str, Any]] = [
        op_payment_add({
            "dni": dni,
            "monto": 187.0,
            "mes": today.month,
            "año": today.year,
        }),
        op_payment_update({
            "dni": dni,
            "mes": today.month,
            "año": today.year,
            "monto": 200.0,
        }),
        op_payment_delete({
            "dni": dni,
            "mes": today.month,
            "año": today.year,
        }),
    ]

    phase6: List[Dict[str, Any]] = [
        op_attendance_add({
            "dni": dni,
            "fecha": today.isoformat(),
        }),
        op_attendance_update({
            "dni": dni,
            "fecha": today.isoformat(),
            "hora": "08:00:00",
        }),
        op_attendance_delete({
            "dni": dni,
            "fecha": today.isoformat(),
        }),
    ]

    phase7: List[Dict[str, Any]] = [
        op_note_add({
            "id": note_id,
            "dni": dni,
            "categoria": "general",
            "titulo": f"nota_{token}",
            "contenido": "Contenido de prueba",
            "importancia": 1,
            "activa": True,
        }),
        op_note_update({
            "id": note_id,
            "importancia": 2,
            "contenido": "Contenido actualizado",
        }),
        op_note_delete({
            "id": note_id,
        }),
    ]

    return [phase1, phase2, phase3, phase4, phase5, phase6, phase7]


def get_server_latest(base: str) -> str:
    code, data = _http_get_json(f"{base}/api/sync/download")
    if code == 200 and isinstance(data, dict):
        latest = data.get("latest")
        if isinstance(latest, str) and latest:
            return latest
    # Fallback: usar fecha local si el endpoint no responde como se espera
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def upload_ops(base: str, device_id: str, ops: List[Dict[str, Any]]) -> Dict[str, Any]:
    enriched = [_ensure_device(op, device_id) for op in ops]
    body = {
        "operations": enriched,
        "client_time": get_server_latest(base),
        "device_id": device_id,
        "sync_flags": {"probe": True},
    }
    code, data = _http_post_json(f"{base}/api/sync/upload", body)
    return {"status": code, "data": data}


def download_ops(base: str, since: str, device_id: str) -> Dict[str, Any]:
    code, data = _http_get_json(f"{base}/api/sync/download", params={"since": since, "device_id": device_id})
    return {"status": code, "data": data}


def contains_token(ops: List[Dict[str, Any]], token: str) -> bool:
    tok = str(token)
    for op in ops:
        try:
            payload = op.get("payload") or {}
            # Buscar el token en campos comunes
            for k in ("dni", "nombre", "titulo", "descripcion", "categoria"):
                v = payload.get(k)
                if isinstance(v, str) and tok in v:
                    return True
        except Exception:
            pass
    return False


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Probe de sincronización bidireccional vía API")
    parser.add_argument("--base", dest="base", default=None, help="Base URL del webapp (autodetección si no se provee)")
    args = parser.parse_args()

    if requests is None:
        print(json.dumps({"ok": False, "error": "requests no disponible"}, ensure_ascii=False))
        return

    base = (args.base or _resolve_base_url()).rstrip("/")

    tokenA = uuid.uuid4().hex[:8]
    tokenB = uuid.uuid4().hex[:8]
    dniA = f"DA{tokenA[:6]}"
    dniB = f"DB{tokenB[:6]}"
    devA = f"probe_dev_A_{tokenA}"
    devB = f"probe_dev_B_{tokenB}"

    # 1) Obtener marca de tiempo base del servidor
    since0 = get_server_latest(base)

    # 2) Ejecutar en fases por cada dispositivo
    phasesA = build_phase_operations(tokenA, dniA, uid_hint=87)
    phasesB = build_phase_operations(tokenB, dniB, uid_hint=188)
    phase_results: List[Dict[str, Any]] = []
    latest_marker = since0

    for idx, (opsA, opsB) in enumerate(zip(phasesA, phasesB), start=1):
        upA = upload_ops(base, devA, opsA)
        dlB = download_ops(base, latest_marker, devB)
        dlB_ok = False
        new_latest = latest_marker
        if dlB.get("status") == 200 and isinstance(dlB.get("data"), dict):
            ops = (dlB["data"] or {}).get("operations") or []
            dlB_ok = isinstance(ops, list) and contains_token(ops, tokenA)
            maybe_latest = (dlB["data"] or {}).get("latest")
            if isinstance(maybe_latest, str) and maybe_latest:
                new_latest = maybe_latest

        upB = upload_ops(base, devB, opsB)
        dlA = download_ops(base, new_latest, devA)
        dlA_ok = False
        final_latest = new_latest
        if dlA.get("status") == 200 and isinstance(dlA.get("data"), dict):
            ops = (dlA["data"] or {}).get("operations") or []
            dlA_ok = isinstance(ops, list) and contains_token(ops, tokenB)
            maybe_latest = (dlA["data"] or {}).get("latest")
            if isinstance(maybe_latest, str) and maybe_latest:
                final_latest = maybe_latest

        latest_marker = final_latest
        phase_results.append({
            "phase": idx,
            "upload": {"A": upA, "B": upB},
            "download": {"B": dlB, "A": dlA},
            "token_seen": {"B_sees_A": dlB_ok, "A_sees_B": dlA_ok},
        })

        # Si la fase tuvo fallos graves, detener para diagnóstico claro
        try:
            a_failed = int(((upA.get("data") or {}).get("failed") or 0))
            b_failed = int(((upB.get("data") or {}).get("failed") or 0))
            if a_failed > 0 or b_failed > 0:
                break
        except Exception:
            pass

    summary = {
        "ok": all(
            (pr.get("upload", {}).get("A", {}).get("status") in (200, 202)) and
            (pr.get("upload", {}).get("B", {}).get("status") in (200, 202))
            for pr in phase_results
        ),
        "base": base,
        "devices": {"A": devA, "B": devB},
        "tokens": {"A": tokenA, "B": tokenB},
        "dni": {"A": dniA, "B": dniB},
        "phases": phase_results,
    }

    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()