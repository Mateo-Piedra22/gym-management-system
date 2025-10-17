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
                payload = {"changes": [self._change_to_payload(ch) for ch in to_send]}
                url = self._webapp_base_url().rstrip('/') + '/api/sync/upload_outbox'
                token = self._token()
                headers = {}
                if token:
                    headers["Authorization"] = f"Bearer {token}"
                    headers["X-Upload-Token"] = token
                resp = requests.post(url, json=payload, headers=headers, timeout=15)
                acked: List[str] = []
                if resp.status_code == 200:
                    body = resp.json() if resp.content else {}
                    acked = list(body.get('acked') or [])
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
        return {
            "schema": ch.get("schema_name") or "public",
            "table": ch.get("table_name"),
            "op": ch.get("op"),
            "pk": ch.get("pk") or {},
            "data": ch.get("data"),
            "dedup_key": ch.get("dedup_key"),
        }

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