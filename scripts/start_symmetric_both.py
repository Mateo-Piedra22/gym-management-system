import os
import sys
import time
from pathlib import Path


def main():
    base_dir = Path(__file__).resolve().parent.parent
    proj_root = str(base_dir)
    if proj_root not in sys.path:
        sys.path.insert(0, proj_root)
    try:
        from symmetricds import setup_symmetric as setup  # type: ignore
    except Exception as e:
        print(f"[Error] No se pudo importar setup_symmetric: {e}")
        sys.exit(1)

    # Apuntar SYMMETRICDS_HOME si no está definido
    env_home = os.getenv('SYMMETRICDS_HOME')
    if not env_home:
        local_home = base_dir / 'symmetricds' / 'symmetric-server-3.16.7'
        if local_home.exists():
            os.environ['SYMMETRICDS_HOME'] = str(local_home)
            print(f"[Setup] SYMMETRICDS_HOME -> {local_home}")
        else:
            print("[Error] No se encontró symmetric-server-3.16.7 en symmetricds/")
            sys.exit(1)

    # Lanzar ambos engines en background con health-check
    try:
        class DM:
            pass
        dm = DM()
        setup.start_symmetricds_background(dm, logger=print, check_interval_sec=30)
    except KeyboardInterrupt:
        print("[Stop] Interrumpido por usuario")
    except Exception as e:
        print(f"[Error] Falló el arranque de SymmetricDS: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()