import os
import sys
import json
import sqlite3
import threading
import time
import logging
import hashlib
import random
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from flask import Flask, request, jsonify

try:
    import requests  # type: ignore
except Exception:  # pragma: no cover
    requests = None  # type: ignore


DB_PATH = os.getenv("PROXY_QUEUE_DB", "proxy_queue.sqlite")
DEFAULT_UPSTREAM = "https://gym-ms-zrk.up.railway.app"


def _read_cfg_upstream() -> Optional[str]:
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        cfg_path = os.path.join(base_dir, "config", "config.json")
        if os.path.exists(cfg_path):
            with open(cfg_path, "r", encoding="utf-8") as f:
                data = json.load(f) or {}
            val = data.get("upstream_webapp_base_url") or data.get("webapp_base_url")
            if isinstance(val, str) and val.strip():
                return val.strip()
    except Exception:
        return None
    return None


def resolve_upstream_base_url() -> str:
    # No usar WEBAPP_BASE_URL para evitar bucles (proxy→proxy)
    val = os.getenv("UPSTREAM_WEBAPP_BASE_URL", "").strip()
    if val:
        return val
    cfg = _read_cfg_upstream()
    if cfg:
        return cfg
    return DEFAULT_UPSTREAM


def _resolve_sync_token() -> str:
    """Obtiene token de sync para propagar al upstream si está configurado.

    Busca `SYNC_API_TOKEN` en entorno y luego `sync_api_token` en config/config.json.
    """
    tok = os.getenv("SYNC_API_TOKEN", "").strip()
    if tok:
        return tok
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        cfg_path = os.path.join(base_dir, "config", "config.json")
        if os.path.exists(cfg_path):
            with open(cfg_path, "r", encoding="utf-8") as f:
                data = json.load(f) or {}
            val = data.get("sync_api_token")
            if isinstance(val, str) and val.strip():
                return val.strip()
    except Exception:
        return ""
    return ""


def ensure_schema(db_path: str = DB_PATH):
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS http_sync_ops (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                endpoint TEXT NOT NULL,   -- 'upload'
                payload_json TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending', -- 'pending'|'processing'|'done'|'failed'
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                created_at TEXT NOT NULL,
                next_attempt_at TEXT,
                op_hash TEXT
            )
            """
        )
        # Evolución de esquema
        try:
            conn.execute("ALTER TABLE http_sync_ops ADD COLUMN next_attempt_at TEXT")
        except Exception:
            pass
        try:
            conn.execute("ALTER TABLE http_sync_ops ADD COLUMN op_hash TEXT")
        except Exception:
            pass
        try:
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_http_sync_ops_op_hash ON http_sync_ops(op_hash)")
        except Exception:
            pass
        conn.commit()
    finally:
        conn.close()

def _payload_hash(payload: Dict[str, Any]) -> str:
    try:
        data = json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
        return hashlib.sha256(data).hexdigest()
    except Exception:
        return ""


def _queue_counts() -> Dict[str, int]:
    ensure_schema()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT status, COUNT(*) AS c
            FROM http_sync_ops
            GROUP BY status
            """
        )
        rows = cur.fetchall() or []
        out: Dict[str, int] = {"pending": 0, "processing": 0, "done": 0, "failed": 0}
        for r in rows:
            st = str(r["status"]) if r["status"] is not None else ""
            c = int(r["c"]) if r["c"] is not None else 0
            if st in out:
                out[st] = c
        return out
    finally:
        conn.close()


def enqueue_upload(payload: Dict[str, Any]) -> int:
    ensure_schema()
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        now_iso = datetime.now(timezone.utc).isoformat()
        op_hash = _payload_hash(payload)
        if op_hash:
            cur.execute(
                "SELECT id FROM http_sync_ops WHERE op_hash=? AND status IN ('pending','processing','done') LIMIT 1",
                (op_hash,),
            )
            row = cur.fetchone()
            if row:
                return int(row[0])
        cur.execute(
            "INSERT INTO http_sync_ops (endpoint, payload_json, created_at, next_attempt_at, op_hash) VALUES (?, ?, ?, ?, ?)",
            ("upload", json.dumps(payload, ensure_ascii=False), now_iso, now_iso, op_hash or None),
        )
        conn.commit()
        return int(cur.lastrowid)
    finally:
        conn.close()


def _dequeue_one() -> Optional[Dict[str, Any]]:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        now_iso = datetime.now(timezone.utc).isoformat()
        cur.execute(
            """
            SELECT id, endpoint, payload_json, attempts
            FROM http_sync_ops
            WHERE status='pending'
              AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
            ORDER BY id ASC
            LIMIT 1
            """,
            (now_iso,),
        )
        row = cur.fetchone()
        if not row:
            return None
        cur.execute("UPDATE http_sync_ops SET status='processing' WHERE id=?", (row["id"],))
        conn.commit()
        return {
            "id": int(row["id"]),
            "endpoint": str(row["endpoint"]),
            "payload": json.loads(row["payload_json"] or "{}"),
            "attempts": int(row["attempts"] or 0),
        }
    finally:
        conn.close()


def _mark_done(item_id: int):
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute("UPDATE http_sync_ops SET status='done' WHERE id=?", (item_id,))
        conn.commit()
    finally:
        conn.close()


def _mark_failed(item_id: int, error: str, attempts: int):
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        try:
            delay = min(1800, max(1, 2 ** max(0, int(attempts))) + random.random())
            next_when = (datetime.now(timezone.utc) + timedelta(seconds=delay)).isoformat().replace("+00:00", "Z")
        except Exception:
            next_when = None
        cur.execute(
            "UPDATE http_sync_ops SET status='pending', attempts=attempts+1, last_error=?, next_attempt_at=? WHERE id=?",
            (error[:500], next_when, item_id),
        )
        conn.commit()
    finally:
        conn.close()

# Circuit breaker simple
_CB_OPEN_UNTIL: Optional[float] = None


def _worker_loop(stop_event: threading.Event, interval_sec: float = 3.0):
    ensure_schema()
    prune_counter = 0
    while not stop_event.is_set():
        item = None
        try:
            item = _dequeue_one()
        except Exception as e:  # pragma: no cover
            try:
                logging.debug(f"Proxy worker dequeue error: {e}")
            except Exception:
                pass
            item = None

        if not item:
            time.sleep(interval_sec)
            continue

        global _CB_OPEN_UNTIL
        now_ts = time.time()
        if _CB_OPEN_UNTIL and now_ts < _CB_OPEN_UNTIL:
            _mark_failed(item["id"], "circuit_breaker_open", item.get("attempts", 0))
            time.sleep(interval_sec)
            continue

        upstream = resolve_upstream_base_url().rstrip("/")
        try:
            if item["endpoint"] == "upload":
                if requests is None:
                    raise RuntimeError("requests no disponible")
                url = upstream + "/api/sync/upload"
                headers = {"Content-Type": "application/json"}
                try:
                    tok = _resolve_sync_token()
                    if tok:
                        headers["Authorization"] = f"Bearer {tok}"
                except Exception:
                    pass
                r = requests.post(url, json=item["payload"], headers=headers, timeout=6)
                if r.status_code >= 200 and r.status_code < 300:
                    _mark_done(item["id"])
                else:
                    if 500 <= r.status_code < 600:
                        _CB_OPEN_UNTIL = time.time() + 15.0
                    try:
                        body_snip = (r.text or "")[:500]
                    except Exception:
                        body_snip = ""
                    _mark_failed(item["id"], f"HTTP {r.status_code}: {body_snip}", item.get("attempts", 0))
            else:
                _mark_failed(item["id"], f"endpoint desconocido: {item['endpoint']}", item.get("attempts", 0))
        except Exception as e:  # pragma: no cover
            _CB_OPEN_UNTIL = time.time() + 5.0
            _mark_failed(item["id"], str(e), item.get("attempts", 0))
            time.sleep(1.0)

        try:
            prune_counter += 1
            if prune_counter % 20 == 0:
                _prune_done()
        except Exception:
            pass


def create_app() -> Flask:
    app = Flask(__name__)

    stop_event = threading.Event()
    worker = threading.Thread(target=_worker_loop, args=(stop_event,), daemon=True)
    worker.start()

    @app.route("/healthz", methods=["GET"])
    def healthz():
        try:
            ensure_schema()
            conn = sqlite3.connect(DB_PATH)
            try:
                conn.execute("SELECT 1")
            finally:
                conn.close()
            return jsonify({
                "status": "ok",
                "upstream": resolve_upstream_base_url(),
                "db_path": DB_PATH,
            }), 200
        except Exception as e:  # pragma: no cover
            return jsonify({"status": "error", "error": str(e)}), 500

    @app.route("/api/sync/upload", methods=["POST"])
    def api_sync_upload():
        try:
            payload = request.get_json(silent=True) or {}
            if not isinstance(payload, dict):
                return jsonify({"success": False, "message": "JSON inválido"}), 400
            ops = payload.get("operations")
            if not isinstance(ops, list) or not ops:
                return jsonify({"success": False, "message": "operations vacío"}), 400
            item_id = enqueue_upload(payload)
            return jsonify({"success": True, "queued_id": item_id, "count": len(ops)}), 202
        except Exception as e:  # pragma: no cover
            return jsonify({"success": False, "message": str(e)}), 500

    @app.route("/api/sync/download", methods=["GET"])
    def api_sync_download():
        try:
            # Este endpoint no se encola: se reenvía directamente al upstream
            since = request.args.get("since", type=str)
            device_id = request.args.get("device_id", type=str)
            upstream = resolve_upstream_base_url().rstrip("/")
            url = upstream + "/api/sync/download"
            if requests is None:
                # Fallback seguro: no propagar error al Desktop
                nowz = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
                return jsonify({
                    "success": True,
                    "operations": [],
                    "latest": nowz,
                    "message": "proxy: requests no disponible",
                    "retry_after_sec": 10,
                }), 200
            params = {}
            if since:
                params["since"] = since
            if device_id:
                params["device_id"] = device_id
            if not params:
                params = None
            # Circuit breaker: si está abierto, responder sin golpear upstream
            global _CB_OPEN_UNTIL
            try:
                now_ts = time.time()
                if _CB_OPEN_UNTIL and now_ts < _CB_OPEN_UNTIL:
                    nowz = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
                    resp = jsonify({
                        "success": True,
                        "operations": [],
                        "latest": nowz,
                        "message": "proxy: circuito abierto (upstream en fallo reciente)",
                        "retry_after_sec": max(1, int(_CB_OPEN_UNTIL - now_ts)),
                    })
                    # Incluir cabecera Retry-After para clientes avanzados
                    return (resp.response[0], 200, {"Content-Type": "application/json", "Retry-After": str(max(1, int(_CB_OPEN_UNTIL - now_ts)))})
            except Exception:
                pass

            headers = {}
            try:
                tok = _resolve_sync_token()
                if tok:
                    headers = {"Authorization": f"Bearer {tok}"}
            except Exception:
                headers = {}
            r = requests.get(url, params=params, headers=headers, timeout=6)
            try:
                if r.status_code >= 400:
                    body = r.text
                    body_snip = body[:500] if body else ""
                    logging.warning(f"proxy download error {r.status_code} url={url} params={params} body_snip={body_snip}")
            except Exception:
                pass
            # Fallback suave en 5xx: no propagar al Desktop, abrir circuito
            if 500 <= r.status_code < 600:
                try:
                    _CB_OPEN_UNTIL = time.time() + 15.0
                except Exception:
                    pass
                nowz = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
                return (json.dumps({
                    "success": True,
                    "operations": [],
                    "latest": nowz,
                    "message": f"proxy: upstream devolvió {r.status_code}",
                    "retry_after_sec": 10,
                }).encode("utf-8"), 200, {"Content-Type": "application/json", "Retry-After": "10"})
            return (r.content, r.status_code, {"Content-Type": r.headers.get("Content-Type", "application/json")})
        except Exception as e:  # pragma: no cover
            # Fallback seguro en excepciones locales
            try:
                global _CB_OPEN_UNTIL
                _CB_OPEN_UNTIL = time.time() + 5.0
            except Exception:
                pass
            nowz = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            return jsonify({
                "success": True,
                "operations": [],
                "latest": nowz,
                "message": f"proxy: excepción local {str(e)}",
                "retry_after_sec": 8,
            }), 200

    @app.route("/")
    def index():
        return jsonify({
            "name": "GymMS Local Proxy",
            "version": 1,
            "health": "/healthz",
            "metrics": "/metrics",
            "endpoints": ["/api/sync/upload", "/api/sync/download"],
            "upstream_base_url": resolve_upstream_base_url(),
        })

    @app.route("/metrics", methods=["GET"])
    def metrics():
        try:
            counts = _queue_counts()
            db_size = None
            try:
                if os.path.exists(DB_PATH):
                    db_size = os.path.getsize(DB_PATH)
            except Exception:
                pass
            cb_open_for = None
            if _CB_OPEN_UNTIL:
                try:
                    cb_open_for = max(0.0, _CB_OPEN_UNTIL - time.time())
                except Exception:
                    cb_open_for = None
            return jsonify({
                "queue": counts,
                "upstream": resolve_upstream_base_url(),
                "db_path": DB_PATH,
                "db_size_bytes": db_size,
                "circuit_breaker_open_for_sec": cb_open_for,
            }), 200
        except Exception as e:  # pragma: no cover
            return jsonify({"error": str(e)}), 500

    # Registrar un hook de arranque compatible con Flask <3 y Flask 3+
    def _announce():  # pragma: no cover
        try:
            logging.info("Local Proxy iniciado en /api/sync/*")
        except Exception:
            pass

    registered = False
    try:
        # Flask 0.11+ (recomendado en Flask 3)
        app.before_serving(_announce)  # type: ignore[attr-defined]
        registered = True
    except Exception:
        try:
            # Flask <3 (deprecado, pero aún puede existir en instalaciones antiguas)
            app.before_first_request(_announce)  # type: ignore[attr-defined]
            registered = True
        except Exception:
            registered = False

    if not registered:
        # Último recurso: ejecutar una vez en el primer request
        _done_flag = {"done": False}

        @app.before_request  # type: ignore[attr-defined]
        def _announce_once():  # pragma: no cover
            if not _done_flag["done"]:
                _done_flag["done"] = True
                _announce()

    return app


def _prune_done(retain_done: int = 1000, older_than_days: int = 7):
    """Elimina entradas 'done' antiguas y limita cardinalidad para evitar crecimiento infinito."""
    ensure_schema()
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        try:
            cutoff = (datetime.now(timezone.utc) - timedelta(days=max(1, int(older_than_days)))).isoformat().replace("+00:00", "Z")
            cur.execute("DELETE FROM http_sync_ops WHERE status='done' AND created_at < ?", (cutoff,))
        except Exception:
            pass
        try:
            cur.execute(
                """
                DELETE FROM http_sync_ops
                WHERE id IN (
                  SELECT id FROM http_sync_ops WHERE status='done' ORDER BY id DESC LIMIT -1 OFFSET ?
                )
                """,
                (max(0, int(retain_done)),),
            )
        except Exception:
            pass
        conn.commit()
    finally:
        conn.close()


def _port_in_use(port: int, host: str = "127.0.0.1", timeout: float = 0.3) -> bool:
    try:
        import socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        ok = (s.connect_ex((host, int(port))) == 0)
        try:
            s.close()
        except Exception:
            pass
        return ok
    except Exception:
        return False


def _find_free_port(start_port: int, max_steps: int = 50) -> Optional[int]:
    try:
        import socket
        for p in range(int(start_port), int(start_port) + int(max_steps)):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(0.2)
                try:
                    if s.connect_ex(("127.0.0.1", p)) != 0:
                        return p
                except Exception:
                    return p
    except Exception:
        return None
    return None


def _update_local_proxy_base_url_config(new_port: int) -> None:
    """Intenta persistir la URL base del proxy en config/config.json."""
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        cfg_path = os.path.join(base_dir, "config", "config.json")
        data: Dict[str, Any] = {}
        if os.path.exists(cfg_path):
            try:
                with open(cfg_path, "r", encoding="utf-8") as f:
                    data = json.load(f) or {}
            except Exception:
                data = {}
        new_base = f"http://127.0.0.1:{int(new_port)}"
        data["local_proxy_base_url"] = new_base
        if "LOCAL_PROXY_BASE_URL" in data:
            data["LOCAL_PROXY_BASE_URL"] = new_base
        with open(cfg_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        # No bloquear si no se puede escribir
        pass


if __name__ == "__main__":  # pragma: no cover
    port = int(os.getenv("LOCAL_PROXY_PORT", "8080"))
    # Si el puerto está ocupado, decidir qué hacer
    if _port_in_use(port):
        # Si está ocupado y ya hay un proxy saludable, no iniciar otro
        healthy = False
        if requests is not None:
            try:
                r = requests.get(f"http://127.0.0.1:{port}/healthz", timeout=0.8)
                healthy = (r.status_code == 200)
            except Exception:
                healthy = False
        if healthy:
            try:
                logging.info(f"Otro proxy ya corre en 127.0.0.1:{port}; no se inicia un segundo.")
            except Exception:
                pass
            sys.exit(0)
        # Si está ocupado por otro servicio, elegir puerto libre
        try:
            new_port = _find_free_port(port + 1, max_steps=100) or (port + 1)
        except Exception:
            new_port = port + 1
        try:
            logging.warning(f"Puerto {port} ocupado por otro servicio; usando puerto alternativo {new_port}.")
        except Exception:
            pass
        port = int(new_port)
        # Intentar persistir para que el Desktop lo detecte en próximos arranques
        try:
            _update_local_proxy_base_url_config(port)
        except Exception:
            pass

    app = create_app()
    app.run(host="127.0.0.1", port=port)