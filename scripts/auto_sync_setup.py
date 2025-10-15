import os
import json
import pathlib
import sys
import traceback


def _proj_root() -> pathlib.Path:
    try:
        return pathlib.Path(__file__).resolve().parent.parent
    except Exception:
        return pathlib.Path.cwd()


def _ensure_base_urls(cfg: dict) -> dict:
    """Asegura server_base_url y client_base_url en config/config.json.

    - Prioriza variables de entorno: SERVER_BASE_URL y CLIENT_BASE_URL
    - Por defecto usa Railway público para servidor y localhost para cliente
    """
    server_env = os.getenv("SERVER_BASE_URL", "").strip()
    client_env = os.getenv("CLIENT_BASE_URL", "").strip()
    server_default = "https://gym-ms-zrk.up.railway.app"
    client_default = "http://127.0.0.1:31415"

    server_url = (server_env or cfg.get("server_base_url") or server_default).strip()
    client_url = (client_env or cfg.get("client_base_url") or client_default).strip()

    # Normalizar sin trailing slash
    server_url = server_url.rstrip("/")
    client_url = client_url.rstrip("/")

    cfg["server_base_url"] = server_url
    cfg["client_base_url"] = client_url
    return cfg


def _read_config() -> dict:
    base = _proj_root()
    cfg_path = base / "config" / "config.json"
    data = {}
    try:
        if cfg_path.exists():
            with open(cfg_path, "r", encoding="utf-8") as f:
                data = json.load(f) or {}
    except Exception:
        data = {}
    return data


def _write_config(cfg: dict):
    base = _proj_root()
    cfg_dir = base / "config"
    cfg_dir.mkdir(parents=True, exist_ok=True)
    cfg_path = cfg_dir / "config.json"
    with open(cfg_path, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)


def _import_setup(base_dir: pathlib.Path):
    # Asegurar que el proyecto esté en sys.path para importar symmetricds
    proj_root = str(base_dir)
    if proj_root not in sys.path:
        sys.path.insert(0, proj_root)
    from symmetricds import setup_symmetric as setup  # type: ignore
    return setup


def _regenerate_properties():
    base_dir = _proj_root()
    setup = _import_setup(base_dir)
    cfg = setup._load_config(base_dir)
    # Garantizar server/client base URLs
    full_cfg = _ensure_base_urls(dict(cfg))
    _write_config(full_cfg)
    paths = setup._write_properties(base_dir, full_cfg)
    print("[OK] Propiedades regeneradas:")
    for k, v in paths.items():
        print(f" - {k}: {v}")


def _register_client_and_enable_initial_load():
    base_dir = _proj_root()
    # Importar utilidades existentes
    # scripts/register_client_on_server.py
    from scripts.register_client_on_server import main as register_main  # type: ignore
    # scripts/request_initial_load.py
    from scripts.request_initial_load import main as initial_load_main  # type: ignore

    print("[STEP] Registrando cliente en el servidor…")
    try:
        register_main()
        print("[OK] Cliente registrado y registro habilitado en sym_node_security.")
    except Exception as e:
        print("[WARN] Fallo al registrar cliente automáticamente:", str(e))
        traceback.print_exc()

    print("[STEP] Habilitando carga inicial…")
    try:
        initial_load_main()
        print("[OK] Carga inicial habilitada (initial_load_enabled=1).")
    except Exception as e:
        print("[WARN] Fallo al habilitar carga inicial automáticamente:", str(e))
        traceback.print_exc()


def _verify_setup_summary():
    print("[STEP] Verificando configuración en el servidor…")
    try:
        from symmetricds.scripts.verify_setup import main as verify_main  # type: ignore
        verify_main()
    except Exception as e:
        print("[WARN] Verificación fallida:", str(e))
        traceback.print_exc()


def main():
    print("=== Auto Setup SymmetricDS ===")
    # 1) Regenerar propiedades con URLs base
    _regenerate_properties()
    # 2) Registrar cliente y habilitar carga inicial
    _register_client_and_enable_initial_load()
    # 3) Verificar configuración
    _verify_setup_summary()
    print("=== Listo. Revisa logs en symmetricds/logs y estado en config/status.json ===")


if __name__ == "__main__":
    main()