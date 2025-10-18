# -*- coding: utf-8 -*-
"""
OutboxPoller — Envía cambios locales (capturados por triggers) al servidor remoto.

Características:
- Lee lotes desde public.sync_outbox (orden por id) con bloqueo SKIP LOCKED.
- Construye payload { changes: [...] } y lo envía a /api/sync/upload_outbox con Bearer token.
- Borra del outbox los cambios confirmados (por dedup_key).
- Backoff exponencial en errores de red, tolerante a offline.
- Hilo en background, no bloquea la UI.

No interfiere con la cola `sync_client`; es un mecanismo paralelo para cambios genéricos.
"""

import os
import json
import time
import threading
from typing import Optional, List, Dict, Any

try:
    import requests  # type: ignore
except Exception:
    requests = None  # type: ignore

try:
    import psycopg2
    import psycopg2.extras
except Exception:
    psycopg2 = None  # type: ignore


class OutboxPoller:
    def __init__(self, db_manager, batch_size: int = 50, interval_s: float = 5.0):
        self.db_manager = db_manager
        self.batch_size = int(batch_size)
        self.interval_s = float(interval_s)
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._running = False
        self.on_status: Optional[callable] = None  # callback opcional
        # Lock para evitar ejecuciones simultáneas de flush explícito
        self._flush_lock = threading.Lock()

    def start(self):
        if self._running:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name='OutboxPoller', daemon=True)
        self._thread.start()
        self._running = True

    def stop(self):
        self._stop.set()
        self._running = False

    def _emit(self, status: Dict[str, Any]):
        cb = self.on_status
        if cb:
            try:
                cb(status)
            except Exception:
                pass

    def _webapp_base_url(self) -> str:
        try:
            from utils import get_webapp_base_url  # type: ignore
            return get_webapp_base_url()
        except Exception:
            return os.getenv('WEBAPP_BASE_URL', 'https://gym-ms-zrk.up.railway.app')

    def _token(self) -> str:
        # Usar helper centralizado que además persiste en config si viene desde ENV
        try:
            from utils import get_sync_upload_token  # type: ignore
            token = get_sync_upload_token(persist_from_env=True)
            if isinstance(token, str):
                return token.strip()
        except Exception:
            pass
        # Fallback legacy (no debería llegar aquí si utils está disponible)
        t = os.getenv('SYNC_UPLOAD_TOKEN', '').strip()
        if t:
            return t
        try:
            from utils import resource_path  # type: ignore
            cfg_path = resource_path('config/config.json')
            if os.path.exists(cfg_path):
                with open(cfg_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                c = data.get('sync_upload_token')
                if isinstance(c, str) and c.strip():
                    return c.strip()
        except Exception:
            pass
        return ''

    def _loop(self):
        if requests is None or psycopg2 is None:
            return
        backoff = 1.0
        while not self._stop.is_set():
            try:
                to_send = self._read_batch()
                if not to_send:
                    self._emit({"pending": 0})
                    time.sleep(self.interval_s)
                    backoff = 1.0
                    continue
                url = self._webapp_base_url().rstrip('/') + '/api/sync/upload_outbox'
                token = self._token()
                if not token:
                    # Sin token configurado: no intentar enviar para evitar 401 spam
                    self._emit({"pending": len(to_send), "auth": "missing"})
                    time.sleep(max(self.interval_s, 5.0))
                    backoff = min(backoff * 2.0, 60.0)
                    continue
                payload = {"changes": [self._change_to_payload(ch) for ch in to_send]}
                headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}", "X-Upload-Token": token}
                resp = requests.post(url, json=payload, headers=headers, timeout=15)
                acked: List[str] = []
                if resp.status_code == 200:
                    body = resp.json() if resp.content else {}
                    acked = list(body.get('acked') or [])
                elif resp.status_code == 401:
                    # Token inválido o faltante en el servidor: emitir estado y aplicar cooldown fuerte
                    self._emit({"pending": len(to_send), "auth": "invalid"})
                    time.sleep(60.0)
                    backoff = min(max(backoff, 10.0) * 2.0, 120.0)
                    continue
                # Borrar acked
                if acked:
                    self._delete_acked_by_dedup(acked)
                self._emit({"sent": len(to_send), "acked": len(acked)})
                backoff = 1.0
            except Exception:
                # Offline o error: backoff exponencial limitado
                time.sleep(backoff)
                backoff = min(backoff * 2.0, 60.0)

    def _read_batch(self) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        try:
            with self.db_manager.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur.execute(
                    """
                    SELECT id, schema_name, table_name, op, pk, data, dedup_key, txid, created_at
                    FROM public.sync_outbox
                    ORDER BY id
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                    """,
                    (self.batch_size,)
                )
                rows = cur.fetchall() or []
                for r in rows:
                    out.append(dict(r))
        except Exception:
            pass
        return out

    def _change_to_payload(self, ch: Dict[str, Any]) -> Dict[str, Any]:
        try:
            return {
                "schema": str(ch.get("schema_name") or "public"),
                "table": str(ch.get("table_name") or ""),
                "op": str(ch.get("op") or "").upper(),
                "pk": ch.get("pk") or {},
                "data": ch.get("data"),
                "dedup_key": str(ch.get("dedup_key") or ""),
            }
        except Exception:
            return {"schema": "public", "table": "", "op": "", "pk": {}, "data": None, "dedup_key": ""}

    def _delete_acked_by_dedup(self, dedup_keys: List[str]) -> None:
        if not dedup_keys:
            return
        try:
            with self.db_manager.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor()
                cur.execute(
                    """
                    DELETE FROM public.sync_outbox
                    WHERE dedup_key = ANY(%s)
                    """,
                    (dedup_keys,)
                )
                conn.commit()
        except Exception:
            pass

    # Nuevo: ejecutar una iteración de envío inmediatamente bajo demanda
    def flush_once(self) -> Dict[str, Any]:
        """Envía un lote inmediatamente (si hay) y devuelve un resumen.
        No bloquea el hilo principal si se llama desde un hilo de background.
        """
        res: Dict[str, Any] = {}
        if requests is None or psycopg2 is None:
            res.update({"ok": False, "error": "deps_missing"})
            self._emit(res)
            return res
        if not self.db_manager:
            res.update({"ok": False, "error": "no_db"})
            self._emit(res)
            return res
        # Evitar solapamiento de flushes explícitos
        if not self._flush_lock.acquire(blocking=False):
            # Ya hay un flush en curso; emitir estado neutro
            res.update({"ok": False, "error": "flush_busy"})
            self._emit(res)
            return res
        try:
            to_send = self._read_batch()
            if not to_send:
                res.update({"pending": 0, "sent": 0, "acked": 0})
                self._emit(res)
                return res
            url = self._webapp_base_url().rstrip('/') + '/api/sync/upload_outbox'
            token = self._token()
            if not token:
                res.update({"pending": len(to_send), "auth": "missing"})
                self._emit(res)
                return res
            payload = {"changes": [self._change_to_payload(ch) for ch in to_send]}
            headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}", "X-Upload-Token": token}
            acked: List[str] = []
            try:
                resp = requests.post(url, json=payload, headers=headers, timeout=10)
                if resp.status_code == 200:
                    body = resp.json() if resp.content else {}
                    acked = list(body.get('acked') or [])
                elif resp.status_code == 401:
                    res.update({"pending": len(to_send), "auth": "invalid"})
                    self._emit(res)
                    return res
            except Exception:
                # Error de red; emitir sin bloquear
                res.update({"pending": len(to_send), "error": "network"})
                self._emit(res)
                return res
            if acked:
                self._delete_acked_by_dedup(acked)
            res.update({"sent": len(to_send), "acked": len(acked)})
            self._emit(res)
            return res
        finally:
            try:
                self._flush_lock.release()
            except Exception:
                pass