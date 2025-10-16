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
    # Default oficial: 31415
    port_env = os.getenv("PORT") or os.getenv("RAILWAY_PORT") or "31415"
    try:
        port_val = str(int(port_env))
    except Exception:
        port_val = "31415"
    os.environ["SYM_RAILWAY_HTTP_PORT"] = port_val
    # Alinear el puerto del SymmetricWebServer (Spring Boot Jetty) con Railway
    # Spring Boot respeta SERVER_PORT env y -Dserver.port
    os.environ["SERVER_PORT"] = port_val
    # Log explícito para diagnóstico en Railway
    try:
        print(f"[Boot] ENV PORT={os.getenv('PORT')} RAILWAY_PORT={os.getenv('RAILWAY_PORT')} SERVER_PORT={os.getenv('SERVER_PORT')}")
        print(f"[Boot] Puerto web efectivo: {port_val}")
    except Exception:
        pass

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
                    cfg["db_remote"]["database"] = parsed.path.lstrip("/")
                if parsed.username:
                    cfg["db_remote"]["user"] = parsed.username
                if parsed.password:
                    cfg["db_remote"]["password"] = parsed.password
        except Exception:
            pass
    # Variables especificas si no hay DATABASE_URL
    # Aceptar también variables estándar de PostgreSQL: PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD
    host = os.getenv("SYM_DB_HOST") or os.getenv("POSTGRES_HOST") or os.getenv("PGHOST")
    port = os.getenv("SYM_DB_PORT") or os.getenv("POSTGRES_PORT") or os.getenv("PGPORT")
    name = os.getenv("SYM_DB_NAME") or os.getenv("POSTGRES_DB") or os.getenv("PGDATABASE")
    user = os.getenv("SYM_DB_USER") or os.getenv("POSTGRES_USER") or os.getenv("PGUSER")
    pwd = os.getenv("SYM_DB_PASSWORD") or os.getenv("POSTGRES_PASSWORD") or os.getenv("PGPASSWORD")
    # Solo usar variables sueltas si NO hay DATABASE_URL definido
    if (not db_url) and any([host, port, name, user, pwd]):
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
            cfg["db_remote"]["database"] = name
        if user:
            cfg["db_remote"]["user"] = user
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

    # Generar properties comunes (railway/local) pero usaremos corp-000 en Railway
    paths = setup._write_properties(base_dir, cfg)
    corp_src_path = base_dir / 'symmetricds' / 'engines' / 'corp-000.properties'
    print(f"[Setup] corp-000.properties -> {corp_src_path}")
    # Mostrar sync.url de corp-000 para verificación rápida en logs
    try:
        with open(corp_src_path, 'r', encoding='utf-8') as pf:
            for line in pf:
                if line.strip().startswith('sync.url='):
                    print(f"[Setup] sync.url -> {line.strip().split('=',1)[1]}")
                    break
    except Exception as e:
        print(f"[Warn] No se pudo leer sync.url de corp-000.properties: {e}")

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

    # Copiar corp-000.properties al HOME y reescribir credenciales/URL/puerto
    try:
        dest_engines = sym_home / 'engines'
        dest_engines.mkdir(exist_ok=True)
        src_corp = corp_src_path
        if not src_corp.exists():
            print(f"[Error] No se encontró {src_corp}")
            sys.exit(1)
        corp_path = dest_engines / 'corp-000.properties'
        corp_path.write_text(src_corp.read_text(encoding='utf-8'), encoding='utf-8')
        print(f"[Setup] Copiado corp-000.properties a {dest_engines}")
        # Reescribir credenciales y URL de DB en corp-000.properties desde ENV/CFG actual
        try:
            rp = corp_path
            txt = rp.read_text(encoding='utf-8')
            remote = dict(cfg.get('db_remote') or {})
            host = str(remote.get('host', '')).strip()
            rport = str(remote.get('port', '5432')).strip()
            dbname = str(remote.get('database', 'railway')).strip()
            user_r = str(remote.get('user', 'postgres')).strip()
            pwd_r = str(remote.get('password', '')).strip()
            sslmode = str(os.getenv('PGSSLMODE') or remote.get('sslmode', 'require')).strip() or 'require'
            app_name = 'gym_management_system'
            # Construir jdbc url
            jdbc_url = ''
            if host and rport and dbname:
                jdbc_url = (
                    f"jdbc:postgresql://{host}:{rport}/{dbname}?sslmode={sslmode}"
                    f"&ApplicationName={app_name}&connectTimeout=10"
                )
                import re as _re
                txt = _re.sub(r'^\s*db\.url\s*=.*$', f'db.url={jdbc_url}', txt, flags=_re.MULTILINE)
            # Reemplazar usuario y password si presentes
            if user_r:
                import re as _re
                txt = _re.sub(r'^\s*db\.user\s*=.*$', f'db.user={user_r}', txt, flags=_re.MULTILINE)
            if pwd_r:
                import re as _re
                txt = _re.sub(r'^\s*db\.password\s*=.*$', f'db.password={pwd_r}', txt, flags=_re.MULTILINE)
            # Alinear http.port y sync.url
            import re as _re
            if 'http.port' in txt:
                txt = _re.sub(r'^\s*http\.port\s*=.*$', f'http.port={port_val}', txt, flags=_re.MULTILINE)
            server_base = os.environ.get('SYM_SERVER_BASE_URL', '').strip() or str(cfg.get('server_base_url', '')).strip()
            if server_base:
                server_base = server_base.rstrip('/')
                txt = _re.sub(r'^\s*sync\.url\s*=.*$', f'sync.url={server_base}/sync/corp-000', txt, flags=_re.MULTILINE)
            # Desactivar push en el servidor central
            txt = _re.sub(r'^\s*start\.push\.job\s*=.*$', 'start.push.job=false', txt, flags=_re.MULTILINE)
            if 'job.push.period.time.ms' in txt:
                txt = _re.sub(r'^\s*job\.push\.period\.time\.ms\s*=.*$', 'job.push.period.time.ms=-1', txt, flags=_re.MULTILINE)
            else:
                txt += ('' if txt.endswith('\n') else '\n') + 'job.push.period.time.ms=-1\n'
            rp.write_text(txt, encoding='utf-8')
            # Log sin exponer la password
            safe_url = jdbc_url.replace(pwd_r, '***') if (jdbc_url and pwd_r) else (jdbc_url or '(sin cambios)')
            print(f"[Setup] corp-000.properties actualizado: db.user={user_r} db.url={safe_url}")
        except Exception as e:
            print(f"[Warn] No se pudo reescribir DB/sync en corp-000.properties: {e}")
        # Limpiar otros engines para evitar que se cargue el cliente local en Railway
        removed = []
        for p in dest_engines.glob('*.properties'):
            try:
                if p.name != 'corp-000.properties':
                    removed.append(p.name)
                    p.unlink(missing_ok=True)
            except Exception:
                pass
        if removed:
            print(f"[Setup] Limpiado engines: removidos {', '.join(removed)}")
    except Exception as e:
        print(f"[Error] No se pudo copiar corp-000.properties al SYMMETRICDS_HOME: {e}")
        sys.exit(1)

    # Ajustar conf/symmetric-server.properties para respetar el puerto de Railway (http.port y server.port)
    try:
        conf_path = sym_home / 'conf' / 'symmetric-server.properties'
        conf_path.parent.mkdir(parents=True, exist_ok=True)
        txt = conf_path.read_text(encoding='utf-8') if conf_path.exists() else ''
        import re as _re
        if 'http.port' in txt:
            txt = _re.sub(r'^\s*http\.port\s*=\s*.*$', f'http.port={port_val}', txt, flags=_re.MULTILINE)
        else:
            txt += ('' if txt.endswith('\n') else '\n') + f'http.port={port_val}\n'
        if 'server.port' in txt:
            txt = _re.sub(r'^\s*server\.port\s*=\s*.*$', f'server.port={port_val}', txt, flags=_re.MULTILINE)
        else:
            txt += f'server.port={port_val}\n'
        if 'host.bind.name' in txt:
            txt = _re.sub(r'^\s*host\.bind\.name\s*=\s*.*$', 'host.bind.name=0.0.0.0', txt, flags=_re.MULTILINE)
        else:
            txt += f'host.bind.name=0.0.0.0\n'
        # Asegurar http.host explícito para conectores Jetty
        if 'http.host' in txt:
            txt = _re.sub(r'^\s*http\.host\s*=\s*.*$', 'http.host=0.0.0.0', txt, flags=_re.MULTILINE)
        else:
            txt += f'http.host=0.0.0.0\n'
        # Asegurar bind.address por compatibilidad
        if 'bind.address' in txt:
            txt = _re.sub(r'^\s*bind\.address\s*=\s*.*$', 'bind.address=0.0.0.0', txt, flags=_re.MULTILINE)
        else:
            txt += f'bind.address=0.0.0.0\n'
        conf_path.write_text(txt, encoding='utf-8')
        print(f"[Setup] Actualizado conf/symmetric-server.properties: http.port/server.port={port_val}")
    except Exception as e:
        print(f"[Warn] No se pudo ajustar puerto en symmetric-server.properties: {e}")

    # Forzar puerto de Spring Boot mediante application.properties y SPRING_CONFIG_LOCATION
    try:
        app_props = sym_home / 'conf' / 'application.properties'
        app_props.parent.mkdir(parents=True, exist_ok=True)
        app_txt = '\n'.join([
            f'server.port={port_val}',
            'server.address=0.0.0.0',
        ])
        app_props.write_text(app_txt, encoding='utf-8')
        os.environ['SPRING_CONFIG_LOCATION'] = str(app_props)
        print(f"[Setup] application.properties creado y SPRING_CONFIG_LOCATION definido: server.port={port_val}")
    except Exception as e:
        print(f"[Warn] No se pudo crear/usar application.properties: {e}")

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

    # Arrancar Symmetric WebServer para el engine 'corp-000'
    corp_engine_props = sym_home / 'engines' / 'corp-000.properties'
    proc = setup._start_engine(java_bin, sym_home, corp_engine_props, print)
    if not proc:
        print("[Error] No se pudo iniciar SymmetricDS WebServer.")
        sys.exit(1)
    print(f"[Run] SymmetricDS 'corp-000' escuchando en puerto {port_val}…")
    # Imprimir sync.url y server_base_url al final para que aparezca cerca del mensaje de 'Run'
    try:
        with open(corp_engine_props, 'r', encoding='utf-8') as pf:
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