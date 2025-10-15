import pathlib
import sys


def _import_setup(base_dir: pathlib.Path):
    # Asegurar que el proyecto esté en sys.path para importar symmetricds
    proj_root = str(base_dir)
    if proj_root not in sys.path:
        sys.path.insert(0, proj_root)
    from symmetricds import setup_symmetric as setup  # type: ignore
    return setup


def main():
    base_dir = pathlib.Path(__file__).resolve().parent.parent
    setup = _import_setup(base_dir)
    cfg = setup._load_config(base_dir)
    paths = setup._write_properties(base_dir, cfg)
    print("Regenerado:")
    for k, v in paths.items():
        print(f" - {k}: {v}")


if __name__ == "__main__":
    main()