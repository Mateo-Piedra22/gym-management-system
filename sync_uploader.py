# -*- coding: utf-8 -*-
"""
sync_uploader.py — Uploader ligero de operaciones encoladas

Propósito:
- Leer la cola persistente del `sync_client`.
- Enviar las operaciones en lotes a un transporte (archivo o HTTP).
- Borrar de la cola las operaciones confirmadas (idempotente) por 'dedup_key'.

Notas:
- Por defecto usa `FileTransport`, que escribe a `config/sync_outbox.jsonl`.
- `HttpTransport` es opcional y se activa si se proporciona URL en ENV/archivo.
- Diseñado para integraciones futuras; la app principal usa replicación PostgreSQL.
"""

import json
import os
import time
import random
from typing import Any, Dict, List, Optional, Tuple

# Importación tardía para evitar romper entornos sin dependencias extra
try:
    import requests  # type: ignore
except Exception:
    requests = None  # HTTP opcional

from sync_client import read_queue, drop_by_dedup_keys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_DIR = os.path.join(BASE_DIR, 'config')
OUTBOX_PATH = os.path.join(CONFIG_DIR, 'sync_outbox.jsonl')
UPLOADER_CFG = os.path.join(CONFIG_DIR, 'sync_uploader.json')


class Transport:
    def send(self, ops: List[Dict[str, Any]]) -> Tuple[bool, List[str]]:
        """Envía un lote de operaciones.
        Devuelve (ok, acked_dedup_keys).
        """
        raise NotImplementedError


class FileTransport(Transport):
    def __init__(self, outbox_path: str = OUTBOX_PATH) -> None:
        self.outbox_path = outbox_path
        os.makedirs(os.path.dirname(self.outbox_path), exist_ok=True)

    def send(self, ops: List[Dict[str, Any]]) -> Tuple[bool, List[str]]:
        try:
            with open(self.outbox_path, 'a', encoding='utf-8') as f:
                for op in ops:
                    f.write(json.dumps(op, ensure_ascii=False) + "\n")
            # Confirmar por dedup_key conocidas
            acked = [str(op.get('dedup_key')) for op in ops if op.get('dedup_key')]
            return True, acked
        except Exception:
            return False, []


class HttpTransport(Transport):
    def __init__(self, url: str, auth_token: Optional[str] = None, timeout: float = 10.0) -> None:
        self.url = url
        self.auth_token = auth_token
        self.timeout = timeout

    def send(self, ops: List[Dict[str, Any]]) -> Tuple[bool, List[str]]:
        if requests is None:
            return False, []
        try:
            headers = {"Content-Type": "application/json"}
            if self.auth_token:
                headers["Authorization"] = f"Bearer {self.auth_token}"
                headers["X-Upload-Token"] = self.auth_token
            payload = {"ops": ops}
            resp = requests.post(self.url, json=payload, headers=headers, timeout=self.timeout)  # type: ignore
            if resp.status_code // 100 == 2:
                try:
                    data = resp.json()
                    acked = data.get('acked') or []
                    acked = [str(k) for k in acked]
                except Exception:
                    # Si no hay cuerpo JSON, asumir ack de todas las dedup_keys conocidas
                    acked = [str(op.get('dedup_key')) for op in ops if op.get('dedup_key')]
                return True, acked
            # 410/404: endpoint legacy/eliminado -> no es recuperable
            if resp.status_code in (404, 410):
                return False, []
            # Otros códigos: tratar como fallo temporal
            return False, []
        except Exception:
            return False, []


class SyncUploader:
    def __init__(
        self,
        transport: Optional[Transport] = None,
        batch_size: int = 50,
        max_attempts: int = 5,
        backoff_base: float = 0.8,
        backoff_factor: float = 1.7,
    ) -> None:
        self.transport = transport or self._default_transport()
        self.batch_size = max(1, int(batch_size))
        self.max_attempts = max(1, int(max_attempts))
        self.backoff_base = float(backoff_base)
        self.backoff_factor = float(backoff_factor)

    def _default_transport(self) -> Transport:
        # Cargar config si existe
        url = os.getenv("SYNC_UPLOAD_URL", "").strip()
        token = os.getenv("SYNC_UPLOAD_TOKEN", "").strip()
        try:
            if os.path.exists(UPLOADER_CFG):
                with open(UPLOADER_CFG, 'r', encoding='utf-8') as f:
                    cfg = json.load(f)
                    url = str(cfg.get('url') or url).strip()
                    token = str(cfg.get('auth_token') or token).strip()
        except Exception:
            pass
        # Fallback: leer token y URL desde config/config.json y utils.get_webapp_base_url
        try:
            from utils import resource_path, get_webapp_base_url, get_sync_upload_token  # type: ignore
            # Token centralizado (persistirá desde ENV si aplica)
            token2 = get_sync_upload_token(persist_from_env=True)
            if token2 and not token:
                token = token2
            cfg_path = resource_path('config/config.json')
            if os.path.exists(cfg_path):
                with open(cfg_path, 'r', encoding='utf-8') as f:
                    app_cfg = json.load(f)
                if not url:
                    base = app_cfg.get('webapp_base_url')
                    if isinstance(base, str) and base.strip():
                        base_url = base.strip()
                    else:
                        base_url = get_webapp_base_url()
                    url = base_url.rstrip('/') + '/api/sync/upload'
            if not url:
                # Último recurso: utils.get_webapp_base_url
                url = get_webapp_base_url().rstrip('/') + '/api/sync/upload'
        except Exception:
            pass
        # Si hay URL y requests disponible, usar HTTP; si no, fallback a archivo
        if url and requests is not None:
            return HttpTransport(url=url, auth_token=token)
        return FileTransport(OUTBOX_PATH)

    def flush_once(self) -> Tuple[int, int]:
        """Envía una pasada completa de la cola en lotes.
        Devuelve (enviadas, borradas)."""
        sent_total = 0
        deleted_total = 0
        # Leer cola completa y trabajar por ventanas
        queue = read_queue()
        n = len(queue)
        if n == 0:
            return 0, 0
        # Procesar en lotes contiguos desde el principio
        for start in range(0, n, self.batch_size):
            batch = queue[start:start + self.batch_size]
            if not batch:
                break
            sent = self._send_with_retry(batch)
            sent_total += int(len(batch) if sent else 0)
            if sent:
                # Borrar por dedup_key confirmadas
                dedup_keys = [str(op.get('dedup_key')) for op in batch if op.get('dedup_key')]
                if dedup_keys:
                    deleted_total += drop_by_dedup_keys(dedup_keys)
        return sent_total, deleted_total

    def _send_with_retry(self, batch: List[Dict[str, Any]]) -> bool:
        attempt = 0
        delay = self.backoff_base
        while attempt < self.max_attempts:
            ok, _acked = self.transport.send(batch)
            if ok:
                return True
            # backoff con jitter
            sleep_for = max(0.1, delay + random.uniform(-0.2, 0.3))
            time.sleep(sleep_for)
            delay *= self.backoff_factor
            attempt += 1
        return False


# Pequeño CLI para uso manual: `python -m sync_uploader`
if __name__ == '__main__':
    uploader = SyncUploader()
    sent, deleted = uploader.flush_once()
    print(json.dumps({"sent": sent, "deleted": deleted}, ensure_ascii=False))