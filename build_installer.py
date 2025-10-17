#!/usr/bin/env python3
"""
Compila y empaqueta el Gym Management System usando cx_Freeze (alternativa madura y estable).
- Genera distribución tipo onedir en 'dist/' con ejecutable GUI y recursos completos.
- Soporte verificado para PyQt6 y librerías científicas/web del proyecto.
Requisitos:
- pip install cx_Freeze

Notas:
- En Windows (Python 3.9–3.13) se recomienda incluir MSVCR automáticamente.
"""

import os
import sys
import shutil
import platform
from pathlib import Path
import subprocess
import argparse

PROJECT_ROOT = Path(__file__).resolve().parent
ENTRY_SCRIPT = PROJECT_ROOT / "main.py"
GYMMSW_SCRIPT = PROJECT_ROOT / "GymMSW.py"
DB_CONFIG_SCRIPT = PROJECT_ROOT / "cdbconfig.py"
APP_NAME = "Gym Management System"
ICON_PATH = PROJECT_ROOT / "assets" / "gym_logo.ico"
DIST_DIR = PROJECT_ROOT / "dist"

EXCLUDE_DIRS = {"build", "dist", "__pycache__", ".git", ".hg", ".svn", ".idea", ".vscode", ".trae"}

# Extensiones de archivos de datos que deben copiarse junto al ejecutable
INCLUDE_FILE_EXTS = {
    ".xlsx", ".xls", ".csv", ".json", ".txt", ".md", ".ini", ".cfg",
    ".qss", ".ico", ".png", ".jpg", ".jpeg", ".svg", ".gif",
    ".sqlite", ".db", ".pdf", ".css", ".html", ".xml", ".yaml", ".yml",
}

# Paquetes que el proyecto usa y que pueden necesitar inclusión explícita
INCLUDE_PACKAGES = [
    "PyQt6",
    "matplotlib",
    "pandas",
    "numpy",
    "reportlab",
    "psutil",
    "psycopg2",
    "keyring",
    "openpyxl",
    "xlwt",
    "xlsxtpl",
    "fastapi",
    "uvicorn",
    "starlette",
    "jinja2",
    "h11",
    "httptools",
    "websockets",
    "pywa",
    "itsdangerous",
]


def ensure_cx_freeze_installed():
    try:
        import cx_Freeze  # noqa: F401
    except Exception:
        print("cx_Freeze no está instalado. Instalando...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "cx_Freeze"])
        print("cx_Freeze instalado correctamente.")


def _is_pure_code_dir(dir_path: Path) -> bool:
    """Devuelve True si el directorio contiene solo código/cache Python (sin datos)."""
    code_exts = {".py", ".pyc", ".pyo"}
    for root, dirs, files in os.walk(dir_path):
        dirs[:] = [d for d in dirs if d != "__pycache__"]
        for f in files:
            ext = Path(f).suffix.lower()
            if ext not in code_exts:
                return False
    return True


def discover_data_inclusions(project_root: Path):
    """Descubre archivos y carpetas de datos para incluir en la distribución.
    Retorna lista de pares (src, dest_rel).
    """
    inclusions: list[tuple[str, str]] = []
    for item in project_root.iterdir():
        if item.is_dir():
            name = item.name
            if name in EXCLUDE_DIRS:
                continue
            if _is_pure_code_dir(item):
                continue
            inclusions.append((str(item), name))
        elif item.is_file():
            if item.name == Path(__file__).name:
                continue
            if item.suffix.lower() in INCLUDE_FILE_EXTS:
                inclusions.append((str(item), item.name))
    return inclusions


def build_options(include_pairs: list[tuple[str, str]]):
    include_files: list[tuple[str, str]] = []

    for src, dest in include_pairs:
        include_files.append((src, dest))

    # Refuerzo explícito de assets clave
    assets_dir = PROJECT_ROOT / "assets"
    if assets_dir.exists():
        include_files.append((str(assets_dir), "assets"))

    styles_dir = PROJECT_ROOT / "styles"
    if styles_dir.exists():
        include_files.append((str(styles_dir), "styles"))

    webapp_dir = PROJECT_ROOT / "webapp"
    if webapp_dir.exists():
        include_files.append((str(webapp_dir), "webapp"))

    config_dir = PROJECT_ROOT / "config"
    if config_dir.exists():
        include_files.append((str(config_dir), "config"))

    # No incluir artefactos de replicación externos (jar/binarios de terceros)

    gym_data = PROJECT_ROOT / "gym_data.txt"
    if gym_data.exists():
        include_files.append((str(gym_data), "gym_data.txt"))

    offline_db = PROJECT_ROOT / "offline_queue.sqlite"
    if offline_db.exists():
        include_files.append((str(offline_db), "offline_queue.sqlite"))

    readme = PROJECT_ROOT / "README.md"
    if readme.exists():
        include_files.append((str(readme), "README.md"))

    reqs = PROJECT_ROOT / "requirements.txt"
    if reqs.exists():
        include_files.append((str(reqs), "requirements.txt"))

    whatsapp_txt = PROJECT_ROOT / "SISTEMA WHATSAPP.txt"
    if whatsapp_txt.exists():
        include_files.append((str(whatsapp_txt), "SISTEMA WHATSAPP.txt"))

    # Matplotlib mpl-data
    try:
        import matplotlib
        mpl_data_path = Path(matplotlib.get_data_path())
        include_files.append((str(mpl_data_path), os.path.join("matplotlib", "mpl-data")))
        print(f"- Detectado mpl-data de Matplotlib en: {mpl_data_path}")
    except Exception as e:
        print(f"ADVERTENCIA: No se pudo localizar mpl-data de Matplotlib automáticamente: {e}")

    return {
        "build_exe": str(DIST_DIR),
        "optimize": 0,
        "include_msvcr": True,
        "packages": INCLUDE_PACKAGES,
        "excludes": [],
        # Evitar problemas de import en paquetes de plantillas Excel
        "zip_exclude_packages": ["xlwt", "xltpl", "xlsxtpl"],
        # Asegurar inclusión explícita de submódulos requeridos
        "includes": [
            "xlwt.ExcelFormulaLexer",
            "xlwt.ExcelFormulaParser",
            "xltpl.basex",
            "xltpl.cellcontext",
            "xltpl.writerx",
            "xlsxtpl.writerx",
            # Fix: incluir módulo de zona horaria de Windows requerido por keyring/pywin32
            "win32timezone",
        ],
        "include_files": include_files,
    }


def parse_args():
    parser = argparse.ArgumentParser(description="Compila el sistema con cx_Freeze")
    parser.add_argument(
        "--msi",
        action="store_true",
        help="Además del build_exe, genera instalador MSI (Windows)"
    )
    return parser.parse_args()


def main():
    args_cli = parse_args()

    if not ENTRY_SCRIPT.exists():
        print(f"ERROR: No se encontró el archivo de entrada: {ENTRY_SCRIPT}")
        sys.exit(1)

    ensure_cx_freeze_installed()

    data_pairs = discover_data_inclusions(PROJECT_ROOT)

    templates_dir = PROJECT_ROOT / "assets" / "templates"
    if templates_dir.exists():
        data_pairs.append((str(templates_dir), os.path.join("assets", "templates")))

    # Nombre de salida sin espacios
    target_name = APP_NAME.replace(" ", "_") + ".exe"

    # Preparar opciones de build
    options = build_options(data_pairs)

    # Limpiar salida previa
    if DIST_DIR.exists():
        shutil.rmtree(DIST_DIR)
    DIST_DIR.mkdir(parents=True, exist_ok=True)

    # Ejecutar setup de cx_Freeze programáticamente
    from cx_Freeze import setup, Executable

    base = "Win32GUI" if platform.system().lower().startswith("win") else None
    icon_arg = str(ICON_PATH) if ICON_PATH.exists() else None
    if icon_arg is None:
        print("ADVERTENCIA: Icono no encontrado, se generará el ejecutable sin icono.")

    # Inyectar comando de construcción
    sys.argv = [sys.argv[0], "build_exe"] + ([])
    setup(
        name=APP_NAME,
        version="1.0.0",
        description="Distribución del Gym Management System",
        options={"build_exe": options},
        executables=[
            Executable(
                script=str(ENTRY_SCRIPT),
                base=base,
                target_name=target_name,
                icon=icon_arg,
            ),
            # Ejecutable ligero para solo web (GymMSW)
            Executable(
                script=str(GYMMSW_SCRIPT),
                base=base,
                target_name="GymMSW.exe",
                icon=icon_arg,
            ),
            # Ejecutable separado para el editor de configuración de base de datos
            Executable(
                script=str(DB_CONFIG_SCRIPT),
                base=base,
                target_name="cdbconfig.exe",
                icon=icon_arg,
            )
        ],
    )

    exe_path = DIST_DIR / target_name
    if exe_path.exists():
        size_mb = exe_path.stat().st_size / (1024 * 1024)
        print(f"\nListo: {exe_path} ({size_mb:.2f} MB)")
        print("\nDistribución 'onedir' completa en 'dist/' con datos garantizados.")
    else:
        print("\nCompilación terminada, pero no se encontró el ejecutable esperado en 'dist/'. Revise la salida anterior.")

    if args_cli.msi and platform.system().lower().startswith("win"):
        print("\nGenerando MSI...")
        # Ejecutar bdist_msi en una nueva invocación
        sys.argv = [sys.argv[0], "bdist_msi"]
        setup(
            name=APP_NAME,
            version="1.0.0",
            description="Instalador MSI del Gym Management System",
            options={"build_exe": options},
            executables=[
                Executable(
                    script=str(ENTRY_SCRIPT),
                    base=base,
                    target_name=target_name,
                    icon=icon_arg,
                ),
                # Incluir también GymMSW en el MSI
                Executable(
                    script=str(GYMMSW_SCRIPT),
                    base=base,
                    target_name="GymMSW.exe",
                    icon=icon_arg,
                ),
            ],
        )
        print("MSI generado.")


if __name__ == "__main__":
    main()