import json
import pathlib
import os
import sys

def main():
    base_dir = pathlib.Path(__file__).resolve().parent.parent
    cfg_path = base_dir / "config" / "config.json"
    try:
        with open(cfg_path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    except Exception as e:
        print(f"[Config] No pude leer config.json: {e}")
        return 1

    try:
        sys.path.append(str(base_dir))
        from symmetricds import setup_symmetric as setup
    except Exception as e:
        print(f"[Config] No pude importar setup_symmetric: {e}")
        return 1

    try:
        setup._configure_railway_server(cfg, print)
        print("[Config] Configuración de triggers/routers en Railway aplicada")
        return 0
    except Exception as e:
        print(f"[Config] Error configurando Railway: {e}")
        return 2

if __name__ == "__main__":
    os._exit(main())