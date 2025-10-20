import sys
import json
from pathlib import Path

# Asegurar imports del proyecto
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils_modules.replication_setup import (
    ensure_bidirectional_replication,
    ensure_logical_replication_from_config_path,
)


def main():
    cfg_path = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else (Path(__file__).resolve().parent.parent / 'config' / 'config.json')
    if not cfg_path.exists():
        print(f"Config no encontrada: {cfg_path}")
        sys.exit(2)
    try:
        with open(cfg_path, 'r', encoding='utf-8') as f:
            cfg = json.load(f)
        # Orquestar bidireccional; si falla capacidades remotas, cae a unidireccional
        res = ensure_bidirectional_replication(cfg)
        print(json.dumps(res, ensure_ascii=False, indent=2))
        sys.exit(0 if res.get('ok') else 1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()