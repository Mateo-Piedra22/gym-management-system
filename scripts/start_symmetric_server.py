import os
import sys
import time
import json
import pathlib
import signal
import tarfile
import urllib.request
import platform


def _proj_root() -> pathlib.Path:
    try:
        return pathlib.Path(__file__).resolve().parent.parent
    except Exception:
        return pathlib.Path.cwd()


def _ensure_server_base_url_in_cfg(setup_module, cfg: dict) -> dict:
    """Asegura server_base_url en config.json y env para generación de properties.

    - Usa ENV `SERVER_BASE_URL` si está presente.
    - Si no, intenta mantener valor existente o default Railway.
    - Propaga también a ENV `SYM_SERVER_BASE_URL` para `_write_properties`.
    """
    base_dir = _proj_root()
    cfg_path = base_dir / "config" / "config.json"
    server_env = os.getenv("SERVER_BASE_URL", "").strip()
    server_default = "https://gymms-symmetricds-node-production.up.railway.app"
    server_url = (server_env or cfg.get("server_base_url") or server_default).strip()
    # Normalizar esquema: anteponer https:// si falta
    if server_url and not (server_url.startswith("http://") or server_url.startswith("https://")):
        server_url = "https://" + server_url
    server_url = server_url.rstrip('/')
    cfg["server_base_url"] = server_url
    try:
        cfg_path.parent.mkdir(parents=True, exist_ok=True)
        with open(cfg_path, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
    except Exception:
        pass
    # Propagar para el generador
    os.environ["SYM_SERVER_BASE_URL"] = server_url
    return cfg


def main():
    base_dir = _proj_root()
    # Asegurar que el proyecto esté en sys.path para importar symmetricds
    proj_root = str(base_dir)
    if proj_root not in sys.path:
        sys.path.insert(0, proj_root)
    from symmetricds import setup_symmetric as setup  # type: ignore

    # Sincronizar el puerto HTTP del engine con `PORT` (Railway)
    port_env = os.getenv("PORT") or os.getenv("RAILWAY_PORT") or "31417"
    try:
        port_val = str(int(port_env))
    except Exception:
        port_val = "31417"
    os.environ["SYM_RAILWAY_HTTP_PORT"] = port_val

    # Cargar config y asegurar server_base_url
    cfg = setup._load_config(base_dir)
    cfg = dict(cfg)
    cfg = _ensure_server_base_url_in_cfg(setup, cfg)

    # Inyectar credenciales DB remota desde ENV (Railway)
    # Soporta `DATABASE_URL` (postgres://user:pass@host:port/db) y `SYM_DB_*`
    db_url = os.getenv("DATABASE_URL") or os.getenv("RAILWAY_DATABASE_URL") or ""
    if db_url:
        try:
            from urllib.parse import urlparse
            parsed = urlparse(db_url)
            if parsed.scheme.startswith("postgres"):
                cfg.setdefault("db_remote", {})
                cfg["db_remote"]["db_type"] = "postgresql"
                if parsed.hostname:
                    cfg["db_remote"]["host"] = parsed.hostname
                if parsed.port:
                    cfg["db_remote"]["port"] = parsed.port
                if parsed.path:
                    cfg["db_remote"]["db_name"] = parsed.path.lstrip("/")
                if parsed.username:
                    cfg["db_remote"]["username"] = parsed.username
                if parsed.password:
                    cfg["db_remote"]["password"] = parsed.password
        except Exception:
            pass
    # Variables especificas si no hay DATABASE_URL
    host = os.getenv("SYM_DB_HOST") or os.getenv("POSTGRES_HOST")
    port = os.getenv("SYM_DB_PORT") or os.getenv("POSTGRES_PORT")
    name = os.getenv("SYM_DB_NAME") or os.getenv("POSTGRES_DB")
    user = os.getenv("SYM_DB_USER") or os.getenv("POSTGRES_USER")
    pwd = os.getenv("SYM_DB_PASSWORD") or os.getenv("POSTGRES_PASSWORD")
    if any([host, port, name, user, pwd]):
        cfg.setdefault("db_remote", {})
        cfg["db_remote"]["db_type"] = cfg["db_remote"].get("db_type", "postgresql")
        if host:
            cfg["db_remote"]["host"] = host
        if port:
            try:
                cfg["db_remote"]["port"] = int(port)
            except Exception:
                cfg["db_remote"]["port"] = port
        if name:
            cfg["db_remote"]["db_name"] = name
        if user:
            cfg["db_remote"]["username"] = user
        if pwd:
            cfg["db_remote"]["password"] = pwd

    # Persistir config actualizada para transparencia
    try:
        cfg_path = base_dir / "config" / "config.json"
        cfg_path.parent.mkdir(parents=True, exist_ok=True)
        with open(cfg_path, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

    # Generar properties
    paths = setup._write_properties(base_dir, cfg)
    print(f"[Setup] railway.properties -> {paths.get('railway')}")
    # Mostrar sync.url para verificación rápida en logs
    try:
        with open(paths.get('railway'), 'r', encoding='utf-8') as pf:
            for line in pf:
                if line.strip().startswith('sync.url='):
                    print(f"[Setup] sync.url -> {line.strip().split('=',1)[1]}")
                    break
    except Exception as e:
        print(f"[Warn] No se pudo leer sync.url de properties: {e}")

    # Resolver SYMMETRICDS_HOME
    env_home = os.getenv('SYMMETRICDS_HOME')
    if env_home:
        sym_home = pathlib.Path(env_home)
    else:
        base_sym = base_dir / 'symmetricds'
        candidates = []
        try:
            candidates = sorted([p for p in base_sym.glob('symmetric-server-*') if p.is_dir()])
        except Exception:
            candidates = []
        sym_home = candidates[-1] if candidates else base_sym

    # Copiar engines al HOME
    try:
        dest_engines = sym_home / 'engines'
        dest_engines.mkdir(exist_ok=True)
        src_railway = pathlib.Path(paths['railway'])
        (dest_engines / 'railway.properties').write_text(src_railway.read_text(encoding='utf-8'), encoding='utf-8')
        print(f"[Setup] Copiado railway.properties a {dest_engines}")
    except Exception as e:
        print(f"[Error] No se pudo copiar properties al SYMMETRICDS_HOME: {e}")
        sys.exit(1)

    # Localizar Java; si no hay 17+, descargar uno ligero para Linux
    java_bin, java_version, java_major = setup._find_java()
    if not java_bin or java_major < 17:
        if os.getenv("SETUP_SKIP_JRE_DOWNLOAD") == "1":
            print(f"[Error] Java 17+ no encontrado (detectado: {java_version}).")
            sys.exit(1)
        print("[Info] Java 17+ no disponible. Descargando JRE ligero…")
        cache_dir = base_dir / ".cache" / "jre17"
        cache_dir.mkdir(parents=True, exist_ok=True)
        # Determinar arquitectura
        machine = platform.machine().lower()
        arch = "x64" if machine in ("x86_64", "amd64") else ("aarch64" if machine in ("aarch64", "arm64") else "x64")
        # Candidatos de descarga (API estable de Adoptium, luego GitHub latest)
        candidates = [
            f"https://api.adoptium.net/v3/binary/latest/17/ga/linux/{arch}/jre/hotspot/normal/adoptium",
            f"https://github.com/adoptium/temurin17-binaries/releases/latest/download/OpenJDK17U-jre_{arch}_linux_hotspot.tar.gz",
            f"https://github.com/adoptium/temurin17-binaries/releases/latest/download/OpenJDK17U-jdk_{arch}_linux_hotspot.tar.gz",
        ]
        # Si el usuario define JAVA_DOWNLOAD_URL, probarlo primero
        override = os.getenv("JAVA_DOWNLOAD_URL")
        if override:
            candidates.insert(0, override)

        tar_path = cache_dir / "jre17.tar.gz"

        # Descargar con fallback y mejor manejo de errores
        last_err = None
        for idx, url in enumerate(candidates, start=1):
            try:
                print(f"[Info] Intento {idx}: Descargando JRE desde {url}")
                req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
                with urllib.request.urlopen(req, timeout=60) as resp:
                    # Guardar streaming para evitar timeouts largos
                    with open(tar_path, "wb") as out:
                        while True:
                            chunk = resp.read(1024 * 64)
                            if not chunk:
                                break
                            out.write(chunk)
                print("[Info] Descarga completada")
                last_err = None
                break
            except Exception as e:
                last_err = e
                print(f"[Warn] No se pudo descargar desde {url}: {e}")
                # continuar con el siguiente candidato

        if last_err is not None:
            print(f"[Error] Falló la descarga de JRE tras {len(candidates)} intentos: {last_err}")
            sys.exit(1)
        # Extraer
        try:
            with tarfile.open(tar_path, "r:gz") as t:
                t.extractall(cache_dir)
        except Exception as e:
            print(f"[Error] Falló la extracción de JRE: {e}")
            sys.exit(1)
        # Detectar carpeta extraída (única subcarpeta)
        extracted = None
        for p in cache_dir.iterdir():
            if p.is_dir() and (p / "bin" / "java").exists():
                extracted = p
                break
        if not extracted:
            print("[Error] No se encontró bin/java en JRE descargado.")
            sys.exit(1)
        os.environ["JAVA_HOME"] = str(extracted)
        os.environ["PATH"] = str(extracted / "bin") + os.pathsep + os.environ.get("PATH", "")
        # Reintentar detección
        java_bin, java_version, java_major = setup._find_java()
        if not java_bin or java_major < 17:
            print(f"[Error] Java 17+ aún no disponible (detectado: {java_version}).")
            sys.exit(1)
    print(f"[Info] Usando Java: {java_bin} ({java_version})")

    # Arrancar Symmetric WebServer para el engine 'railway'
    proc = setup._start_engine(java_bin, sym_home, pathlib.Path(paths['railway']), print)
    if not proc:
        print("[Error] No se pudo iniciar SymmetricDS WebServer.")
        sys.exit(1)
    print(f"[Run] SymmetricDS 'railway' escuchando en puerto {port_val}…")
    # Imprimir sync.url y server_base_url al final para que aparezca cerca del mensaje de 'Run'
    try:
        with open(paths.get('railway'), 'r', encoding='utf-8') as pf:
            for line in pf:
                if line.strip().startswith('sync.url='):
                    print(f"[Run] sync.url -> {line.strip().split('=',1)[1]}")
                    break
    except Exception as e:
        print(f"[Warn] No se pudo leer sync.url al final del arranque: {e}")
    try:
        print(f"[Run] server_base_url -> {os.environ.get('SYM_SERVER_BASE_URL','(no definido)')}")
    except Exception:
        pass

    # Mantener proceso en foreground para plataformas tipo Procfile
    def _handle_sigterm(_sig, _frm):
        try:
            proc.terminate()
        except Exception:
            pass
        sys.exit(0)

    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)
    try:
        proc.wait()
    except Exception:
        while True:
            time.sleep(5)


if __name__ == '__main__':
    main()