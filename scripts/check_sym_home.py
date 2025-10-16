import os
import sys
from pathlib import Path


def main():
    base_dir = Path(__file__).resolve().parent.parent
    if str(base_dir) not in sys.path:
        sys.path.insert(0, str(base_dir))
    try:
        from symmetricds.setup_symmetric import _build_classpath, _find_sym_script  # type: ignore
    except Exception as e:
        print(f"[Error] import setup_symmetric: {e}")
        sys.exit(1)
    home = os.getenv('SYMMETRICDS_HOME') or str(base_dir / 'symmetricds' / 'symmetric-server-3.16.7')
    print(f"SYMMETRICDS_HOME={home}")
    script = _find_sym_script(Path(home))
    print(f"sym_script={script}")
    cp = _build_classpath(Path(home))
    print(f"classpath_len={len(cp)}")
    print(cp[:2000])


if __name__ == '__main__':
    main()