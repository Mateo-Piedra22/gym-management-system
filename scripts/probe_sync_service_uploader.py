# -*- coding: utf-8 -*-
"""
Probe SyncService auto-uploader: verifica disparo por cambio y periódico.
"""
import os
import sys
import time

# Asegurar import desde raíz del proyecto
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

from sync_client import (
    clear_queue,
    get_pending_count,
    enqueue_operations,
    op_user_update,
)
from utils_modules.sync_service import SyncService


def wait_until_queue_empty(timeout_s: float = 10.0) -> bool:
    """Espera hasta que la cola quede vacía, con timeout."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if get_pending_count() == 0:
            return True
        time.sleep(0.4)
    return get_pending_count() == 0


def main():
    print("== Probe: SyncService auto-uploader ==")
    # Limpia la cola para estado conocido
    try:
        removed = clear_queue()
        print(f"cola limpiada, removed={removed}")
    except Exception as e:
        print(f"warning: no se pudo limpiar la cola: {e}")

    # Fase 1: disparo inmediato por cambio
    op = op_user_update({"user_id": 123, "nombre": "Tester"})
    ok = enqueue_operations([op])
    print(f"enqueue cambio immediate_ok={ok}, pending={get_pending_count()}")

    svc = SyncService(auto_upload_on_change=True, periodic_upload_interval_ms=60000)
    svc.start()
    # Forzamos un tick para evaluar y lanzar subida
    svc._tick()
    # Espera envío
    if wait_until_queue_empty(12):
        print("fase 1: OK, flush por cambio")
    else:
        print("fase 1: FALLÓ, no se vació la cola")
    svc.stop()

    # Fase 2: disparo periódico sin cambio (auto_upload_on_change=False)
    ok = enqueue_operations([op_user_update({"user_id": 456, "nombre": "Periodic"})])
    print(f"enqueue periodic_ok={ok}, pending={get_pending_count()}")

    svc2 = SyncService(auto_upload_on_change=False, periodic_upload_interval_ms=2000)
    svc2.start()
    # Primer tick: no debe subir aún
    svc2._tick()
    time.sleep(2.5)
    # Segundo tick: ya debería disparar por periodo
    svc2._tick()
    if wait_until_queue_empty(8):
        print("fase 2: OK, flush periódico")
    else:
        print("fase 2: FALLÓ, la cola sigue con elementos")
    svc2.stop()

    print("== Fin probe SyncService auto-uploader ==")


if __name__ == "__main__":
    main()