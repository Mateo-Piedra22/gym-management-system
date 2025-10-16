import os
import sys
import json
import threading
import time
import subprocess
import socket
import re
from pathlib import Path
from typing import Optional, Tuple

# Estado global de ciclo de vida para apagado gracioso
RUNNER_THREAD = None
PROCESS_HANDLES = {}
STOP_EVENT = threading.Event()

try:
    import psycopg2
    import psycopg2.extras
except Exception:
    psycopg2 = None  # type: ignore

try:
    import keyring  # Almacén seguro de credenciales (Windows Credential Manager)
except Exception:
    keyring = None

from config import KEYRING_SERVICE_NAME
from device_id import get_device_id


def _load_config(base_dir: Path) -> dict:
    """Lee config/config.json y retorna su dict (o {})."""
    cfg_path = base_dir / 'config' / 'config.json'
    try:
        with open(cfg_path, 'r', encoding='utf-8') as f:
            return json.load(f) or {}
    except Exception:
        return {}


def _resolve_password(user: str, host: str, port: int, fallback: str = "") -> str:
    """Resuelve contraseña desde keyring usando cuentas compuestas.

    Intenta en orden: user@host:port, user@host, user. Si falla, usa fallback.
    """
    if keyring is not None:
        candidates = [
            f"{user}@{host}:{port}",
            f"{user}@{host}",
            f"{user}",
        ]
        for account in candidates:
            try:
                pwd = keyring.get_password(KEYRING_SERVICE_NAME, account)
            except Exception:
                pwd = None
            if pwd:
                return pwd
    # Fallback a entorno o parámetro (Railway y variantes comunes)
    for env_key in ('PGPASSWORD','POSTGRES_PASSWORD','DB_PASSWORD','PG_PASS','DATABASE_PASSWORD'):
        val = os.getenv(env_key)
        if val:
            return val
    # Intentar parsear DATABASE_URL si está presente (formato postgres://user:pass@host:port/db)
    try:
        db_url = os.getenv('DATABASE_URL', '')
        if db_url.startswith('postgres://') or db_url.startswith('postgresql://'):
            at_idx = db_url.find('@')
            if at_idx > 0:
                auth = db_url[:at_idx]
                colon_idx = auth.rfind(':')
                if colon_idx > 0:
                    return auth[colon_idx+1:]
    except Exception:
        pass
    return fallback


def _jdbc_url(h: str, p: int, d: str, ssl: str, app_name: str, timeout: int) -> str:
    """Construye URL JDBC de PostgreSQL con parámetros útiles."""
    # application_name en PG JDBC se mapea como ApplicationName
    params = [
        f"sslmode={ssl}",
        f"ApplicationName={app_name}",
        f"connectTimeout={timeout}",
        # Forzar timezone válido para PostgreSQL (evita FATAL: invalid value for parameter "TimeZone")
        # Codificado para URL: "-c TimeZone=America/Argentina/Buenos_Aires"
        "options=-c%20TimeZone%3DAmerica/Argentina/Buenos_Aires",
    ]
    return f"jdbc:postgresql://{h}:{p}/{d}?" + "&".join(params)


def _ensure_dirs(base_dir: Path):
    (base_dir / 'symmetricds').mkdir(exist_ok=True)
    (base_dir / 'symmetricds' / 'engines').mkdir(exist_ok=True)
    (base_dir / 'symmetricds' / 'logs').mkdir(exist_ok=True)


def _write_properties(base_dir: Path, cfg: dict) -> dict:
    """Genera archivos .properties para local y railway y retorna rutas."""
    _ensure_dirs(base_dir)

    # Resolver perfiles local y remoto (Railway)
    local = cfg.get('db_local') or {
        'host': cfg.get('host', 'localhost'),
        'port': cfg.get('port', 5432),
        'database': cfg.get('database', 'gimnasio'),
        'user': cfg.get('user', 'postgres'),
        'sslmode': cfg.get('sslmode', 'prefer'),
        'connect_timeout': cfg.get('connect_timeout', 10),
        'application_name': cfg.get('application_name', 'gym_management_system'),
    }
    remote = cfg.get('db_remote') or {
        'host': 'shuttle.proxy.rlwy.net',
        'port': 5432,
        'database': 'railway',
        'user': 'postgres',
        'sslmode': 'require',
        'connect_timeout': 10,
        'application_name': 'gym_management_system',
    }

    # Contraseñas desde keyring (o fallback específico del perfil si presente)
    local_pwd = _resolve_password(
        str(local.get('user', 'postgres')),
        str(local.get('host', 'localhost')),
        int(local.get('port', 5432)),
        str(local.get('password', '')),
    )
    remote_pwd = _resolve_password(
        str(remote.get('user', 'postgres')),
        str(remote.get('host', '')),
        int(remote.get('port', 5432)),
        str(remote.get('password', '')),
    )

    # Construir URLs JDBC
    local_jdbc = _jdbc_url(
        str(local.get('host', 'localhost')),
        int(local.get('port', 5432)),
        str(local.get('database', 'gimnasio')),
        str(local.get('sslmode', 'prefer')),
        str(local.get('application_name', 'gym_management_system')),
        int(local.get('connect_timeout', 10)),
    )
    remote_jdbc = _jdbc_url(
        str(remote.get('host', '')),
        int(remote.get('port', 5432)),
        str(remote.get('database', 'railway')),
        str(remote.get('sslmode', 'require')),
        str(remote.get('application_name', 'gym_management_system')),
        int(remote.get('connect_timeout', 10)),
    )

    # Puertos HTTP locales para engines
    local_http_port = int(os.getenv('SYM_LOCAL_HTTP_PORT', 31416))
    railway_http_port = int(os.getenv('SYM_RAILWAY_HTTP_PORT', 31417))

    # Base URLs para sincronización y registro
    # Permite desplegar el servidor en cloud sin interferir con el cliente local
    server_base_url = str(cfg.get('server_base_url', os.getenv('SYM_SERVER_BASE_URL', 'http://127.0.0.1:31415')))
    client_base_url = str(cfg.get('client_base_url', os.getenv('SYM_CLIENT_BASE_URL', 'http://127.0.0.1:31415')))

    # Fijar puertos estables: usa env o defaults (sin elegir aleatorios)
    # Evita alertas de puertos cambiantes en el IDE y mantiene consistencia.
    # Si están ocupados, el servidor web central (31415) es el que realmente importa.
    # Los http.port de cada engine se quedan en 31416/31417 (o lo que se haya definido en env).

    # Archivo común symmetric-ds.properties (se asegura en disco si no existe)
    common_path = base_dir / 'symmetricds' / 'symmetric-ds.properties'
    if not common_path.exists():
        try:
            common_path.write_text(
                "\n".join([
                    "# Configuración común de SymmetricDS",
                    "java.heap.max.size=256m",
                    "rest.api.enabled=true",
                    "http.timeout.ms=30000",
                    "# Logs en el directorio del proyecto",
                    "log.dir=logs",
                ]),
                encoding='utf-8'
            )
        except Exception:
            pass

    # railway.properties (servidor/master)
    railway_props = "\n".join([
        "engine.name=railway",
        "group.id=server",
        "external.id=railway",
        f"db.driver=org.postgresql.Driver",
        f"db.url={remote_jdbc}",
        f"db.user={remote.get('user', 'postgres')}",
        f"db.password={remote_pwd}",
        f"auto.create=true",
        f"auto.sync=true",
        # Solicitar automáticamente reload inicial cuando corresponde
        "auto.reload=true",
        # Explicitar que este engine es raíz (no cliente)
        # Evita mensajes que pidan registration.url y fuerzas a no auto-registrar
        "auto.registration=false",
        f"http.port={railway_http_port}",
        f"sync.url={server_base_url.rstrip('/')}/sync/railway",
        # Para nodo raíz, dejar registration.url vacío explícitamente
        "registration.url=",
        "registration.open=true",
        "route.simple=true",
        "channel.default=true",
        "data.create_time.timezone=America/Argentina/Buenos_Aires",
        "# Conflictos: master (Railway) gana siempre",
        "conflict.resolve.default=master_wins",
        "# Seguridad básica",
        "rest.api.user=sym_user",
        "rest.api.password=sym_password",
    ])

    # local.properties (cliente)
    # El cliente se registra contra el engine 'railway' en el WebServer (Jetty 31415)
    local_props = "\n".join([
        "engine.name=local",
        "group.id=client",
        f"external.id=local-{get_device_id()}",
        f"db.driver=org.postgresql.Driver",
        f"db.url={local_jdbc}",
        f"db.user={local.get('user', 'postgres')}",
        f"db.password={local_pwd}",
        f"auto.create=true",
        f"auto.sync=true",
        # Permite que el cliente también solicite reload inicial si es necesario
        "auto.reload=true",
        # Registrar automáticamente el cliente contra el servidor al inicio
        "auto.registration=true",
        # Forzar instalación de triggers al iniciar el engine cliente
        "auto.sync.triggers.at.startup=true",
        f"http.port={local_http_port}",
        # El cliente publica su propio endpoint; debe terminar con el nombre del engine
        f"sync.url={client_base_url.rstrip('/')}/sync/local",
        f"registration.url={server_base_url.rstrip('/')}/sync/railway",
        "data.create_time.timezone=America/Argentina/Buenos_Aires",
        "channel.default=true",
        "# Conflictos: master (Railway) gana siempre",
        "conflict.resolve.default=master_wins",
    ])

    eng_dir = base_dir / 'symmetricds' / 'engines'
    railway_path = eng_dir / 'railway.properties'
    local_path = eng_dir / 'local.properties'

    railway_path.write_text(railway_props, encoding='utf-8')
    local_path.write_text(local_props, encoding='utf-8')

    return {
        'common': str(common_path),
        'railway': str(railway_path),
        'local': str(local_path),
        'railway_port': railway_http_port,
        'local_port': local_http_port,
        'server_base_url': server_base_url,
        'client_base_url': client_base_url,
    }


def _parse_java_major(version_output: str) -> int:
    """Extrae la versión mayor de Java desde la salida de `java -version`.

    Soporta formatos típicos:
    - 'java version "1.8.0_xxx"' -> 8
    - 'openjdk version "17.0.7"' -> 17
    - 'java version "21"' -> 21
    """
    try:
        text = version_output.lower()
        # Buscar el número dentro de comillas
        m = re.search(r'version\s+"([0-9][0-9]?\.[0-9].*?|[0-9]{1,2})"', text)
        if m:
            v = m.group(1)
            # Manejar formato 1.x (Java 8)
            if v.startswith('1.'):
                parts = v.split('.')
                if len(parts) >= 2 and parts[1].isdigit():
                    return int(parts[1])
                return 8
            # Manejar formato moderno (17, 21, 17.0.7, etc.)
            main = re.match(r'^(\d{1,2})', v)
            if main:
                return int(main.group(1))
        # Algunas distribuciones imprimen en stderr sin comillas
        m2 = re.search(r'openjdk\s+version\s+(\d{1,2})', text)
        if m2:
            return int(m2.group(1))
    except Exception:
        pass
    return 0


def _find_java() -> tuple[str, str, int]:
    """Encuentra binario de Java elegible (>=17) y retorna (ruta, salida_version, mayor).

    Prioriza versiones modernas instaladas en rutas conocidas y un JRE embebido
    en symmetricds/jre si existe. Evita seleccionar Java 8 para compatibilidad
    con SymmetricDS 3.16+ (classfile 61 = Java 17).
    """
    # Intentar resolver base_dir para chequear jre embebido
    try:
        base_dir = Path(sys.executable).resolve().parent if getattr(sys, 'frozen', False) else Path(__file__).resolve().parent.parent
    except Exception:
        base_dir = Path.cwd()

    embedded_jre = base_dir / 'symmetricds' / 'jre' / 'bin' / ('java.exe' if os.name == 'nt' else 'java')
    # Intentar detectar el JRE embebido sin ejecutarlo, leyendo el archivo 'release'
    try:
        if embedded_jre.exists():
            release_file = embedded_jre.parent.parent / 'release'
            if release_file.exists():
                txt = release_file.read_text(encoding='utf-8', errors='ignore')
                m = re.search(r'JAVA_VERSION\s*=\s*"([^"]+)"', txt)
                if m:
                    java_ver = m.group(1)
                    try:
                        major = int(java_ver.split('.')[0])
                    except Exception:
                        major = _parse_java_major(f'openjdk version "{java_ver}"')
                    if major >= 17:
                        return (str(embedded_jre), f'openjdk version "{java_ver}" (embedded)', major)
    except Exception:
        pass
    candidates = [
        str(embedded_jre),
        r"C:\\Program Files\\Java\\jdk-21\\bin\\java.exe",
        r"C:\\Program Files\\Java\\jdk-17\\bin\\java.exe",
        r"C:\\Program Files\\Amazon Corretto\\jdk21\\bin\\java.exe",
        r"C:\\Program Files\\Amazon Corretto\\jdk17\\bin\\java.exe",
        'java',
    ]
    best: tuple[str, str, int] | None = None
    for c in candidates:
        try:
            if not c:
                continue
            proc = subprocess.run([c, '-version'], capture_output=True, text=True)
            if proc.returncode == 0:
                ver_out = (proc.stderr or proc.stdout or '').strip()
                major = _parse_java_major(ver_out)
                if major >= 17:
                    # Preferir la mayor versión hallada
                    if best is None or major > best[2]:
                        best = (c, ver_out, major)
        except Exception:
            continue
    return best if best else ('', '', 0)


def _find_sym_jar(sym_home: Path) -> Path:
    """Busca el JAR de SymmetricDS dentro de SYMMETRICDS_HOME/lib.

    IMPORTANTE: si no se encuentra, retornar una ruta no válida a un archivo
    (no un directorio vacío), para que las comprobaciones de is_file() fallen
    correctamente y no se lance "java -jar .".
    """
    lib = sym_home / 'lib'
    try:
        if lib.exists():
            for f in lib.iterdir():
                if f.name.startswith('symmetric-server') and f.suffix == '.jar' and f.is_file():
                    return f
    except Exception:
        pass
    # Retornar una ruta imposible dentro de lib para que is_file() sea False
    return lib / 'symmetric-server-NOTFOUND.jar'


def _build_classpath(sym_home: Path) -> str:
    """Construye el classpath a partir de todos los JARs en sym_home/lib."""
    lib_dir = sym_home / 'lib'
    jars = []
    try:
        for p in lib_dir.glob('*.jar'):
            jars.append(str(p))
    except Exception:
        pass
    # En Windows el separador es ';'
    return ';'.join(jars)


def _find_sym_script(sym_home: Path) -> Path | None:
    """Detecta el script de arranque 'sym' si existe (Windows o Unix)."""
    candidates = [
        sym_home / 'bin' / 'sym.bat',
        sym_home / 'bin' / 'sym.cmd',
        sym_home / 'bin' / 'sym',
    ]
    for c in candidates:
        try:
            if c.exists():
                return c
        except Exception:
            continue
    return None


def _start_engine(java_bin: str, sym_home: Path, props_path: Path, logger) -> subprocess.Popen | None:
    """Inicia un engine de SymmetricDS siempre vía classpath con SymmetricWebServer y JRE embebido.

    - Evita usar 'sym.bat' para impedir que tome un Java del sistema (Java 8)
      que provoque UnsupportedClassVersionError.
    - Construye classpath con lib/, web/WEB-INF/lib y web/WEB-INF/classes.
    - Deja que SymmetricWebServer escanee engines/ dentro de SYMMETRICDS_HOME.
    """
    try:
        # Redirigir stdout/stderr del engine Java a archivos en symmetricds/logs
        sym_root = props_path.parent.parent
        logs_dir = sym_root / 'logs'
        try:
            logs_dir.mkdir(exist_ok=True)
        except Exception:
            pass
        base_name = f"symmetricds-{props_path.stem}"
        out_path = logs_dir / f"{base_name}.out.log"
        err_path = logs_dir / f"{base_name}.err.log"
        out_f = open(out_path, 'a', encoding='utf-8', errors='replace')
        err_f = open(err_path, 'a', encoding='utf-8', errors='replace')

        # Lanzar vía classpath usando SymmetricWebServer
        # Construir classpath incluyendo lib, web/WEB-INF/lib y web/WEB-INF/classes
        cp_lib = _build_classpath(sym_home)
        web_lib_dir = sym_home / 'web' / 'WEB-INF' / 'lib'
        cp_web = ''
        try:
            jars = [str(p) for p in web_lib_dir.glob('*.jar')]
            cp_web = ';'.join(jars)
        except Exception:
            pass
        # Incluir también clases compiladas del webapp
        web_classes_dir = sym_home / 'web' / 'WEB-INF' / 'classes'
        cp_parts = [cp_lib]
        if cp_web:
            cp_parts.append(cp_web)
        try:
            if web_classes_dir.exists():
                cp_parts.append(str(web_classes_dir))
        except Exception:
            pass
        cp = ';'.join([p for p in cp_parts if p])
        if not cp:
            raise RuntimeError(f"Classpath vacío. Verifica instalación completa en {sym_home / 'lib'}")
        # Dejar que SymmetricWebServer escanee engines/ y arranque ambos engines
        # No pasamos '-p' para evitar diferencias de argumentos entre versiones.
        # Forzar timezone JVM a un identificador válido para PostgreSQL
        # Evita que el driver pgjdbc envíe 'America/Buenos_Aires' (no soportado en PG 17)
        # Determinar puerto web para Railway/Spring Boot Jetty
        web_port = None
        try:
            web_port = os.getenv('PORT') or os.getenv('RAILWAY_PORT') or os.getenv('SERVER_PORT')
        except Exception:
            web_port = None
        if not web_port:
            web_port = '31415'
        # Construir comando Java, forzando server.port y bind address
        cmd = [
            java_bin,
            '-Duser.timezone=America/Argentina/Buenos_Aires',
            f'-Dserver.port={web_port}',
            '-Dserver.address=0.0.0.0',
            '-cp', cp,
            'org.jumpmind.symmetric.SymmetricWebServer'
        ]
        logger(f"[SymmetricDS] Lanzando engine vía classpath (escaneo de engines/): {' '.join(cmd)}")
        # Al lanzar directo garantizamos uso del JRE embebido (java_bin)
        proc = subprocess.Popen(cmd, stdout=out_f, stderr=err_f, cwd=str(sym_home))
        return proc
    except Exception as e:
        logger(f"[SymmetricDS] Error lanzando engine con {props_path}: {e}")
        return None


def _connect_pg(host: str, port: int, db: str, user: str, password: str, sslmode: str = "prefer"):
    """Crea una conexión psycopg2 con parámetros básicos.

    Retorna None si psycopg2 no está disponible o falla la conexión.
    """
    if psycopg2 is None:
        return None
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=db,
            user=user,
            password=password,
            sslmode=sslmode,
            connect_timeout=10,
        )
        conn.autocommit = True
        return conn
    except Exception:
        return None


def _ensure_server_channel_router(conn, log):
    """Asegura canal 'default' y router simple hacia grupo 'client' en Railway."""
    try:
        with conn.cursor() as cur:
            # Verificar que existan las tablas clave antes de intentar insertar
            try:
                cur.execute(
                    "SELECT to_regclass('public.sym_channel'), to_regclass('public.sym_router'), to_regclass('public.sym_node_group'), to_regclass('public.sym_node_group_link')"
                )
                reg = cur.fetchone()
                if (not reg) or any(x is None for x in reg):
                    log("[SymmetricDS] Esquema incompleto en Railway (falta sym_channel/sym_router). Omito asegurar canal/router por ahora.")
                    return
            except Exception:
                log("[SymmetricDS] Error verificando esquema Railway al asegurar canal/router; omito por ahora.")
                return
            # Asegurar node groups (server y client) y links bidireccionales
            try:
                cur.execute(
                    """
                    DO $$
                    BEGIN
                        IF NOT EXISTS (SELECT 1 FROM sym_node_group WHERE node_group_id = 'server') THEN
                            INSERT INTO sym_node_group (node_group_id) VALUES ('server');
                        END IF;
                        IF NOT EXISTS (SELECT 1 FROM sym_node_group WHERE node_group_id = 'client') THEN
                            INSERT INTO sym_node_group (node_group_id) VALUES ('client');
                        END IF;
                        -- Enlace server->client: servidor envía cuando el cliente hace pull (W)
                        IF NOT EXISTS (
                            SELECT 1 FROM sym_node_group_link WHERE source_node_group_id = 'server' AND target_node_group_id = 'client'
                        ) THEN
                            INSERT INTO sym_node_group_link (source_node_group_id, target_node_group_id, data_event_action)
                            VALUES ('server', 'client', 'W');
                        ELSE
                            UPDATE sym_node_group_link
                               SET data_event_action = 'W'
                             WHERE source_node_group_id = 'server' AND target_node_group_id = 'client'
                               AND (data_event_action IS NULL OR data_event_action NOT IN ('W','P'));
                        END IF;
                        -- Enlace client->server: cliente empuja (P)
                        IF NOT EXISTS (
                            SELECT 1 FROM sym_node_group_link WHERE source_node_group_id = 'client' AND target_node_group_id = 'server'
                        ) THEN
                            INSERT INTO sym_node_group_link (source_node_group_id, target_node_group_id, data_event_action)
                            VALUES ('client', 'server', 'P');
                        ELSE
                            UPDATE sym_node_group_link
                               SET data_event_action = 'P'
                             WHERE source_node_group_id = 'client' AND target_node_group_id = 'server'
                               AND (data_event_action IS NULL OR data_event_action NOT IN ('W','P'));
                        END IF;
                    END$$;
                    """
                )
            except Exception:
                pass
            # Canal por defecto si no existe
            cur.execute(
                """
                DO $$
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM sym_channel WHERE channel_id = 'default') THEN
                        INSERT INTO sym_channel (channel_id, processing_order, queue, enabled, max_batch_size)
                        VALUES ('default', 1, 'default', 1, 1000);
                    END IF;
                END$$;
                """
            )
            # Router hacia clientes (grupo 'client')
            cur.execute(
                """
                DO $$
                DECLARE
                    has_sync boolean := EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_schema='public' AND table_name='sym_router' AND column_name='sync_config'
                    );
                    has_enabled boolean := EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_schema='public' AND table_name='sym_router' AND column_name='enabled'
                    );
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM sym_router WHERE router_id = 'toClients') THEN
                        IF has_sync THEN
                            INSERT INTO sym_router (
                                router_id, source_node_group_id, target_node_group_id,
                                router_type, router_expression, sync_config,
                                create_time, last_update_time
                            ) VALUES (
                                'toClients', 'server', 'client', 'default', NULL, 1,
                                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                            );
                        ELSIF has_enabled THEN
                            INSERT INTO sym_router (
                                router_id, source_node_group_id, target_node_group_id,
                                router_type, router_expression, enabled,
                                create_time, last_update_time
                            ) VALUES (
                                'toClients', 'server', 'client', 'default', NULL, 1,
                                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                            );
                        ELSE
                            INSERT INTO sym_router (
                                router_id, source_node_group_id, target_node_group_id,
                                router_type, router_expression,
                                create_time, last_update_time
                            ) VALUES (
                                'toClients', 'server', 'client', 'default', NULL,
                                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                            );
                        END IF;
                    END IF;
                END$$;
                """
            )
    except Exception as e:
        try:
            log(f"[SymmetricDS] No se pudo asegurar canal/router: {e}")
        except Exception:
            pass


def _ensure_client_channel_router(conn, log):
    """Asegura canal 'default' y router simple hacia grupo 'server' en Local."""
    try:
        with conn.cursor() as cur:
            # Verificar tablas clave antes de intentar insertar
            try:
                cur.execute(
                    "SELECT to_regclass('public.sym_channel'), to_regclass('public.sym_router'), to_regclass('public.sym_node_group'), to_regclass('public.sym_node_group_link')"
                )
                reg = cur.fetchone()
                if (not reg) or any(x is None for x in reg):
                    log("[SymmetricDS] Esquema incompleto en Local (falta sym_channel/sym_router). Omito asegurar canal/router por ahora.")
                    return
            except Exception:
                log("[SymmetricDS] Error verificando esquema Local al asegurar canal/router; omito por ahora.")
                return
            # Asegurar node groups y links básicos también en cliente (por si el engine replica config más tarde)
            try:
                cur.execute(
                    """
                    DO $$
                    BEGIN
                        IF NOT EXISTS (SELECT 1 FROM sym_node_group WHERE node_group_id = 'server') THEN
                            INSERT INTO sym_node_group (node_group_id) VALUES ('server');
                        END IF;
                        IF NOT EXISTS (SELECT 1 FROM sym_node_group WHERE node_group_id = 'client') THEN
                            INSERT INTO sym_node_group (node_group_id) VALUES ('client');
                        END IF;
                        -- Enlace server->client: servidor envía cuando el cliente hace pull (W)
                        IF NOT EXISTS (
                            SELECT 1 FROM sym_node_group_link WHERE source_node_group_id = 'server' AND target_node_group_id = 'client'
                        ) THEN
                            INSERT INTO sym_node_group_link (source_node_group_id, target_node_group_id, data_event_action)
                            VALUES ('server', 'client', 'W');
                        ELSE
                            UPDATE sym_node_group_link
                               SET data_event_action = 'W'
                             WHERE source_node_group_id = 'server' AND target_node_group_id = 'client'
                               AND (data_event_action IS NULL OR data_event_action NOT IN ('W','P'));
                        END IF;
                        -- Enlace client->server: cliente empuja (P)
                        IF NOT EXISTS (
                            SELECT 1 FROM sym_node_group_link WHERE source_node_group_id = 'client' AND target_node_group_id = 'server'
                        ) THEN
                            INSERT INTO sym_node_group_link (source_node_group_id, target_node_group_id, data_event_action)
                            VALUES ('client', 'server', 'P');
                        ELSE
                            UPDATE sym_node_group_link
                               SET data_event_action = 'P'
                             WHERE source_node_group_id = 'client' AND target_node_group_id = 'server'
                               AND (data_event_action IS NULL OR data_event_action NOT IN ('W','P'));
                        END IF;
                    END$$;
                    """
                )
            except Exception:
                pass
            # Canal por defecto si no existe
            cur.execute(
                """
                DO $$
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM sym_channel WHERE channel_id = 'default') THEN
                        INSERT INTO sym_channel (channel_id, processing_order, queue, enabled, max_batch_size)
                        VALUES ('default', 1, 'default', 1, 1000);
                    END IF;
                END$$;
                """
            )
            # Router hacia servidor (grupo 'server')
            cur.execute(
                """
                DO $$
                DECLARE
                    has_sync boolean := EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_schema='public' AND table_name='sym_router' AND column_name='sync_config'
                    );
                    has_enabled boolean := EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_schema='public' AND table_name='sym_router' AND column_name='enabled'
                    );
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM sym_router WHERE router_id = 'toServer') THEN
                        IF has_sync THEN
                            INSERT INTO sym_router (
                                router_id, source_node_group_id, target_node_group_id,
                                router_type, router_expression, sync_config,
                                create_time, last_update_time
                            ) VALUES (
                                'toServer', 'client', 'server', 'default', NULL, 1,
                                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                            );
                        ELSIF has_enabled THEN
                            INSERT INTO sym_router (
                                router_id, source_node_group_id, target_node_group_id,
                                router_type, router_expression, enabled,
                                create_time, last_update_time
                            ) VALUES (
                                'toServer', 'client', 'server', 'default', NULL, 1,
                                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                            );
                        ELSE
                            INSERT INTO sym_router (
                                router_id, source_node_group_id, target_node_group_id,
                                router_type, router_expression,
                                create_time, last_update_time
                            ) VALUES (
                                'toServer', 'client', 'server', 'default', NULL,
                                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                            );
                        END IF;
                    END IF;
                END$$;
                """
            )
    except Exception as e:
        try:
            log(f"[SymmetricDS] No se pudo asegurar canal/router en cliente: {e}")
        except Exception:
            pass


def _list_public_tables(conn) -> list:
    """Lista tablas BASE del esquema public, excluyendo tablas sym_* y pg_*.
    Retorna lista de nombres simples.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_type = 'BASE TABLE'
                  AND table_name NOT LIKE 'sym_%'
                  AND table_name NOT LIKE 'pg_%'
                ORDER BY table_name
                """
            )
            rows = cur.fetchall()
            return [r[0] for r in rows]
    except Exception:
        return []


def _ensure_triggers_for_all_tables(conn, log, tables: list):
    """Crea entradas en sym_trigger y sym_trigger_router para cada tabla.

    - Canal: 'default'
    - Sincroniza INSERT/UPDATE/DELETE
    - Router: 'toClients'
    """
    try:
        with conn.cursor() as cur:
            # Verificar existencia de tablas de triggers antes de operar
            try:
                cur.execute("SELECT to_regclass('public.sym_trigger'), to_regclass('public.sym_trigger_router')")
                reg = cur.fetchone()
                if (not reg) or any(x is None for x in reg):
                    log("[SymmetricDS] Esquema de triggers incompleto (falta sym_trigger/sym_trigger_router). Omito creación de triggers por ahora.")
                    return
            except Exception:
                log("[SymmetricDS] Error verificando esquema de triggers; omito creación por ahora.")
                return
            for tbl in tables:
                trig_id = f"trg_{tbl}"
                try:
                    # sym_trigger
                    cur.execute(
                        """
                        DO $$
                        DECLARE
                            has_use_pk_data boolean := EXISTS (
                                SELECT 1 FROM information_schema.columns
                                WHERE table_schema='public' AND table_name='sym_trigger' AND column_name='use_pk_data'
                            );
                            has_use_stream_lobs boolean := EXISTS (
                                SELECT 1 FROM information_schema.columns
                                WHERE table_schema='public' AND table_name='sym_trigger' AND column_name='use_stream_lobs'
                            );
                            trig TEXT := %s;
                            src_tbl TEXT := %s;
                        BEGIN
                            IF NOT EXISTS (SELECT 1 FROM sym_trigger WHERE trigger_id = trig) THEN
                                IF has_use_stream_lobs AND has_use_pk_data THEN
                                    INSERT INTO sym_trigger (
                                        trigger_id, source_table_name, channel_id,
                                        sync_on_insert, sync_on_update, sync_on_delete,
                                        use_stream_lobs, use_pk_data,
                                        create_time, last_update_time
                                    ) VALUES (trig, src_tbl, 'default', 1, 1, 1, 0, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
                                ELSIF has_use_stream_lobs AND NOT has_use_pk_data THEN
                                    INSERT INTO sym_trigger (
                                        trigger_id, source_table_name, channel_id,
                                        sync_on_insert, sync_on_update, sync_on_delete,
                                        use_stream_lobs,
                                        create_time, last_update_time
                                    ) VALUES (trig, src_tbl, 'default', 1, 1, 1, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
                                ELSIF NOT has_use_stream_lobs AND has_use_pk_data THEN
                                    INSERT INTO sym_trigger (
                                        trigger_id, source_table_name, channel_id,
                                        sync_on_insert, sync_on_update, sync_on_delete,
                                        use_pk_data,
                                        create_time, last_update_time
                                    ) VALUES (trig, src_tbl, 'default', 1, 1, 1, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
                                ELSE
                                    INSERT INTO sym_trigger (
                                        trigger_id, source_table_name, channel_id,
                                        sync_on_insert, sync_on_update, sync_on_delete,
                                        create_time, last_update_time
                                    ) VALUES (trig, src_tbl, 'default', 1, 1, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
                                END IF;
                            END IF;
                        END$$;
                        """,
                        (trig_id, tbl)
                    )
                except Exception:
                    pass
                try:
                    # sym_trigger_router
                    cur.execute(
                        """
                        DO $$
                        DECLARE
                            has_tr_create_time boolean := EXISTS (
                                SELECT 1 FROM information_schema.columns
                                WHERE table_schema='public' AND table_name='sym_trigger_router' AND column_name='create_time'
                            );
                            has_tr_last_update_time boolean := EXISTS (
                                SELECT 1 FROM information_schema.columns
                                WHERE table_schema='public' AND table_name='sym_trigger_router' AND column_name='last_update_time'
                            );
                        BEGIN
                            IF NOT EXISTS (
                                SELECT 1 FROM sym_trigger_router WHERE trigger_id = %s AND router_id = 'toClients'
                            ) THEN
                                IF has_tr_create_time AND has_tr_last_update_time THEN
                                    INSERT INTO sym_trigger_router (
                                        trigger_id, router_id, initial_load_order,
                                        create_time, last_update_time
                                    ) VALUES (%s, 'toClients', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
                                ELSIF has_tr_create_time AND NOT has_tr_last_update_time THEN
                                    INSERT INTO sym_trigger_router (
                                        trigger_id, router_id, initial_load_order,
                                        create_time
                                    ) VALUES (%s, 'toClients', 1, CURRENT_TIMESTAMP);
                                ELSIF NOT has_tr_create_time AND has_tr_last_update_time THEN
                                    INSERT INTO sym_trigger_router (
                                        trigger_id, router_id, initial_load_order,
                                        last_update_time
                                    ) VALUES (%s, 'toClients', 1, CURRENT_TIMESTAMP);
                                ELSE
                                    INSERT INTO sym_trigger_router (trigger_id, router_id, initial_load_order)
                                    VALUES (%s, 'toClients', 1);
                                END IF;
                            END IF;
                        END$$;
                        """,
                        (trig_id, trig_id, trig_id, trig_id, trig_id)
                    )
                except Exception:
                    pass
    except Exception as e:
        try:
            log(f"[SymmetricDS] No se pudieron asegurar triggers/routers: {e}")
        except Exception:
            pass


def _configure_railway_server(cfg: dict, log) -> None:
    """Configura canal, router y triggers en la base Railway (servidor).

    Asume que las tablas sym_* ya existen (creadas por el engine) y que el registro
    de nodos está abierto. No modifica la lógica de la app.
    """
    try:
        remote = cfg.get('db_remote') or {}
        host = str(remote.get('host', ''))
        port = int(remote.get('port', 5432))
        db = str(remote.get('database', 'railway'))
        user = str(remote.get('user', 'postgres'))
        sslmode = str(remote.get('sslmode', 'require'))
        # Resolver password desde keyring
        pwd = _resolve_password(user, host, port, '')
        conn = _connect_pg(host, port, db, user, pwd, sslmode=sslmode)
        if conn is None:
            log("[SymmetricDS] psycopg2 no disponible o conexión a Railway fallida; skip configuración de triggers")
            return
        _ensure_server_channel_router(conn, log)
        tables = _list_public_tables(conn)
        # Filtrar tablas de auditoría internas si existiera alguna específica (opcional)
        tables = [t for t in tables if not t.startswith('sync_')]
        _ensure_triggers_for_all_tables(conn, log, tables)
        try:
            conn.close()
        except Exception:
            pass
        log(f"[SymmetricDS] Configuración de triggers/routers aplicada en Railway para {len(tables)} tablas")
    except Exception as e:
        try:
            log(f"[SymmetricDS] Error configurando servidor Railway: {e}")
        except Exception:
            pass


def _configure_local_client(cfg: dict, log) -> None:
    """Configura canal, router y triggers en la base Local (cliente)."""
    try:
        local = cfg.get('db_local') or {}
        host = str(local.get('host', 'localhost'))
        port = int(local.get('port', 5432))
        db = str(local.get('database', 'gimnasio'))
        user = str(local.get('user', 'postgres'))
        sslmode = str(local.get('sslmode', 'prefer'))
        pwd = _resolve_password(user, host, port, '')
        conn = _connect_pg(host, port, db, user, pwd, sslmode=sslmode)
        if conn is None:
            log("[SymmetricDS] Conexión a DB local fallida; skip configuración de triggers cliente")
            return
        _ensure_client_channel_router(conn, log)
        tables = _list_public_tables(conn)
        # Reusar la misma creación de triggers, pero apuntar al router 'toServer'
        try:
            with conn.cursor() as cur:
                for tbl in tables:
                    trig_id = f"trg_{tbl}"
                    # Asegurar sym_trigger
                    try:
                        cur.execute(
                            """
                            DO $$
                            DECLARE
                                has_use_pk_data boolean := EXISTS (
                                    SELECT 1 FROM information_schema.columns
                                    WHERE table_schema='public' AND table_name='sym_trigger' AND column_name='use_pk_data'
                                );
                                has_use_stream_lobs boolean := EXISTS (
                                    SELECT 1 FROM information_schema.columns
                                    WHERE table_schema='public' AND table_name='sym_trigger' AND column_name='use_stream_lobs'
                                );
                                trig TEXT := %s;
                                src_tbl TEXT := %s;
                            BEGIN
                                IF NOT EXISTS (SELECT 1 FROM sym_trigger WHERE trigger_id = trig) THEN
                                    IF has_use_stream_lobs AND has_use_pk_data THEN
                                        INSERT INTO sym_trigger (
                                            trigger_id, source_table_name, channel_id,
                                            sync_on_insert, sync_on_update, sync_on_delete,
                                            use_stream_lobs, use_pk_data,
                                            create_time, last_update_time
                                        ) VALUES (trig, src_tbl, 'default', 1, 1, 1, 0, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
                                    ELSIF has_use_stream_lobs AND NOT has_use_pk_data THEN
                                        INSERT INTO sym_trigger (
                                            trigger_id, source_table_name, channel_id,
                                            sync_on_insert, sync_on_update, sync_on_delete,
                                            use_stream_lobs,
                                            create_time, last_update_time
                                        ) VALUES (trig, src_tbl, 'default', 1, 1, 1, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
                                    ELSIF NOT has_use_stream_lobs AND has_use_pk_data THEN
                                        INSERT INTO sym_trigger (
                                            trigger_id, source_table_name, channel_id,
                                            sync_on_insert, sync_on_update, sync_on_delete,
                                            use_pk_data,
                                            create_time, last_update_time
                                        ) VALUES (trig, src_tbl, 'default', 1, 1, 1, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
                                    ELSE
                                        INSERT INTO sym_trigger (
                                            trigger_id, source_table_name, channel_id,
                                            sync_on_insert, sync_on_update, sync_on_delete,
                                            create_time, last_update_time
                                        ) VALUES (trig, src_tbl, 'default', 1, 1, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
                                    END IF;
                                END IF;
                            END$$;
                            """,
                            (trig_id, tbl)
                        )
                    except Exception:
                        pass
                    # Asegurar trigger_router hacia servidor
                    try:
                        cur.execute(
                            """
                            DO $$
                            DECLARE
                                has_tr_create_time boolean := EXISTS (
                                    SELECT 1 FROM information_schema.columns
                                    WHERE table_schema='public' AND table_name='sym_trigger_router' AND column_name='create_time'
                                );
                                has_tr_last_update_time boolean := EXISTS (
                                    SELECT 1 FROM information_schema.columns
                                    WHERE table_schema='public' AND table_name='sym_trigger_router' AND column_name='last_update_time'
                                );
                            BEGIN
                                IF NOT EXISTS (
                                    SELECT 1 FROM sym_trigger_router WHERE trigger_id = %s AND router_id = 'toServer'
                                ) THEN
                                    IF has_tr_create_time AND has_tr_last_update_time THEN
                                        INSERT INTO sym_trigger_router (
                                            trigger_id, router_id, initial_load_order,
                                            create_time, last_update_time
                                        ) VALUES (%s, 'toServer', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
                                    ELSIF has_tr_create_time AND NOT has_tr_last_update_time THEN
                                        INSERT INTO sym_trigger_router (
                                            trigger_id, router_id, initial_load_order,
                                            create_time
                                        ) VALUES (%s, 'toServer', 1, CURRENT_TIMESTAMP);
                                    ELSIF NOT has_tr_create_time AND has_tr_last_update_time THEN
                                        INSERT INTO sym_trigger_router (
                                            trigger_id, router_id, initial_load_order,
                                            last_update_time
                                        ) VALUES (%s, 'toServer', 1, CURRENT_TIMESTAMP);
                                    ELSE
                                        INSERT INTO sym_trigger_router (trigger_id, router_id, initial_load_order)
                                        VALUES (%s, 'toServer', 1);
                                    END IF;
                                END IF;
                            END$$;
                            """,
                            (trig_id, trig_id, trig_id, trig_id, trig_id)
                        )
                    except Exception:
                        pass
        except Exception as e:
            log(f"[SymmetricDS] No se pudieron asegurar triggers/routers en cliente: {e}")
        try:
            conn.close()
        except Exception:
            pass
        log(f"[SymmetricDS] Configuración de triggers/routers aplicada en Local para {len(tables)} tablas")
    except Exception as e:
        try:
            log(f"[SymmetricDS] Error configurando cliente local: {e}")
        except Exception:
            pass


def start_symmetricds_background(db_manager, logger=None, check_interval_sec: int = 60):
    """Genera configuración y arranca SymmetricDS en segundo plano.

    - Genera symmetricds/engines/local.properties y railway.properties
    - Asegura symmetricds/symmetric-ds.properties
    - Busca Java y JAR de SymmetricDS (SYMMETRICDS_HOME)
    - Lanza ambos engines como procesos daemon y mantiene health-check
    """

    # Normalizar logger: aceptar módulo logging, función callable o fallback a print
    def _mk_log(lg):
        try:
            # Si es módulo logging, usar .info
            if hasattr(lg, 'info') and callable(getattr(lg, 'info')):
                return getattr(lg, 'info')
            # Si es callable, usarlo directamente
            if callable(lg):
                return lg
        except Exception:
            pass
        return print
    log = _mk_log(logger)

    # Evitar arranques duplicados: verificar hilo activo y status.json
    try:
        for t in threading.enumerate():
            if t.name == "SymmetricDSRunner":
                log("[SymmetricDS] Runner ya activo; omito arranque duplicado.")
                return True
    except Exception:
        pass
    # Si hay runner global ya vivo, evitar doble arranque
    try:
        if RUNNER_THREAD and RUNNER_THREAD.is_alive():
            log("[SymmetricDS] Runner global ya vivo; omito arranque.")
            return True
    except Exception:
        pass

    base_dir = Path(sys.executable).resolve().parent if getattr(sys, 'frozen', False) else Path(__file__).resolve().parent.parent
    try:
        status_path_guard = base_dir / 'symmetricds' / 'status.json'
        if status_path_guard.exists():
            st = json.loads(status_path_guard.read_text(encoding='utf-8') or '{}')
            if bool(st.get('running')):
                log("[SymmetricDS] status.json indica 'running'; omito arranque duplicado.")
                return True
    except Exception:
        pass

    def _runner():
        try:
            log("[SymmetricDS] Preparando configuración…")
            cfg = _load_config(base_dir)
            # Generar properties y asegurar directorios antes de cualquier chequeo
            paths = _write_properties(base_dir, cfg)

            # Localizar Java elegible (>=17) y JAR
            java_bin, java_version, java_major = _find_java()
            status_path = base_dir / 'symmetricds' / 'status.json'
            if not java_bin:
                msg = "Java 17+ no encontrado. Instala JDK/JRE 17 o superior."
                log(f"[SymmetricDS] {msg}")
                try:
                    status_path.write_text(json.dumps({
                        'running': False,
                        'message': msg,
                        'railway_port': paths.get('railway_port'),
                        'local_port': paths.get('local_port'),
                        'java_version': java_version,
                        'external_id': f"local-{get_device_id()}",
                        'last_check_ts': int(time.time()),
                    }), encoding='utf-8')
                except Exception:
                    pass
                # Sin Java no continuamos
                return
            # Si versión insuficiente, informar y no lanzar
            if java_major < 17:
                msg = f"Java detectado (<17): {java_version}. Requiere Java 17 o superior."
                log(f"[SymmetricDS] {msg}")
                try:
                    status_path.write_text(json.dumps({
                        'running': False,
                        'message': msg,
                        'railway_port': paths.get('railway_port'),
                        'local_port': paths.get('local_port'),
                        'java_version': java_version,
                        'external_id': f"local-{get_device_id()}",
                        'last_check_ts': int(time.time()),
                    }), encoding='utf-8')
                except Exception:
                    pass
                return
            # Detectar SYMMETRICDS_HOME: env var, o carpeta versionada dentro de symmetricds/
            env_home = os.getenv('SYMMETRICDS_HOME')
            if env_home:
                sym_home = Path(env_home)
            else:
                base_sym = base_dir / 'symmetricds'
                # Buscar subcarpeta symmetric-server-*
                candidates = []
                try:
                    candidates = sorted([p for p in base_sym.glob('symmetric-server-*') if p.is_dir()])
                except Exception:
                    candidates = []
                sym_home = candidates[-1] if candidates else base_sym
            # Log diagnóstico de Java
            try:
                log(f"[SymmetricDS] Usando Java en: {java_bin} ({java_version})")
            except Exception:
                pass
            # Validar instalación: script o librerías
            sym_script = _find_sym_script(sym_home)
            cp = _build_classpath(sym_home)
            if (not sym_script) and (not cp or cp.strip() == ''):
                msg = (
                    "Instalación de SymmetricDS incompleta: no se encontró 'bin/sym(.bat)' ni JARs en 'lib'. "
                    "Extrae el ZIP oficial y apunta SYMMETRICDS_HOME a esa carpeta."
                )
                log(f"[SymmetricDS] {msg}")
                try:
                    status_path.write_text(json.dumps({
                        'running': False,
                        'message': msg,
                        'railway_port': paths.get('railway_port'),
                        'local_port': paths.get('local_port'),
                        'java_version': java_version,
                        'external_id': f"local-{get_device_id()}",
                        'last_check_ts': int(time.time()),
                    }), encoding='utf-8')
                except Exception:
                    pass
                return

            # Copiar engines/*.properties generados al engines/ dentro de SYMMETRICDS_HOME
            try:
                dest_engines = sym_home / 'engines'
                dest_engines.mkdir(exist_ok=True)
                src_railway = Path(paths['railway'])
                src_local = Path(paths['local'])
                (dest_engines / 'railway.properties').write_text(src_railway.read_text(encoding='utf-8'), encoding='utf-8')
                (dest_engines / 'local.properties').write_text(src_local.read_text(encoding='utf-8'), encoding='utf-8')
                log(f"[SymmetricDS] Engines copiados a {dest_engines} para escaneo automático")
            except Exception as e:
                log(f"[SymmetricDS] No se pudieron copiar engines a SYMMETRICDS_HOME: {e}")

            # Lanzar UN solo servidor web (SymmetricWebServer) que escanea ambos engines
            # Evita conflicto del puerto 31415 por procesos duplicados
            web_proc = _start_engine(java_bin, sym_home, Path(paths['railway']), log)
            if not web_proc:
                log("[SymmetricDS] No se pudo iniciar el servidor SymmetricWebServer.")
                return
            # Guardar handle único bajo claves conocidas para compatibilidad de apagado
            try:
                PROCESS_HANDLES['web'] = web_proc
                PROCESS_HANDLES['railway'] = web_proc
                PROCESS_HANDLES['local'] = web_proc
            except Exception:
                pass
            # Compatibilidad: mantener variables locales para lógica de apagado posterior
            railway_proc = web_proc
            local_proc = web_proc
            log("[SymmetricDS] Servidor web lanzado; engines serán cargados por auto-descubrimiento.")
            try:
                status_path.write_text(json.dumps({
                    'running': True,
                    'message': 'Engines lanzados',
                    'railway_port': paths.get('railway_port'),
                    'local_port': paths.get('local_port'),
                    'java_version': java_version,
                    'external_id': f"local-{get_device_id()}",
                    'last_check_ts': int(time.time()),
                }), encoding='utf-8')
            except Exception:
                pass

            # Esperar a que los engines creen el esquema SymmetricDS (tablas sym_*)
            def _sym_schema_ready(conn) -> bool:
                try:
                    with conn.cursor() as cur:
                        # Verificar múltiples tablas clave para considerar esquema listo
                        checks = [
                            'public.sym_channel',
                            'public.sym_context',
                            'public.sym_node',
                            'public.sym_router',
                            'public.sym_trigger',
                            'public.sym_trigger_router',
                            # Asegurar que existe la tabla usada para solicitar cargas iniciales
                            'public.sym_table_reload_request'
                        ]
                        for t in checks:
                            cur.execute("SELECT to_regclass(%s)", (t,))
                            r = cur.fetchone()
                            if not (r and r[0]):
                                return False
                        return True
                except Exception:
                    return False

            def _wait_schema_ready(cfg: dict, log_func) -> Tuple[bool, bool]:
                # Conexiones a remoto (Railway) y local
                remote = cfg.get('db_remote') or {}
                local = cfg.get('db_local') or {}
                r_host = str(remote.get('host', ''))
                r_port = int(remote.get('port', 5432))
                r_db = str(remote.get('database', 'railway'))
                r_user = str(remote.get('user', 'postgres'))
                r_ssl = str(remote.get('sslmode', 'require'))
                r_pwd = _resolve_password(r_user, r_host, r_port, str(remote.get('password', '')))

                l_host = str(local.get('host', 'localhost'))
                l_port = int(local.get('port', 5432))
                l_db = str(local.get('database', 'gymdb'))
                l_user = str(local.get('user', 'postgres'))
                l_ssl = str(local.get('sslmode', 'prefer'))
                l_pwd = _resolve_password(l_user, l_host, l_port, str(local.get('password', '')))

                r_conn = _connect_pg(r_host, r_port, r_db, r_user, r_pwd, sslmode=r_ssl)
                l_conn = _connect_pg(l_host, l_port, l_db, l_user, l_pwd, sslmode=l_ssl)
                r_ok = False
                l_ok = False
                # Ampliar espera a 5 minutos para permitir creación completa del esquema
                deadline = time.time() + 300
                while time.time() < deadline:
                    try:
                        if r_conn and not r_ok:
                            r_ok = _sym_schema_ready(r_conn)
                        if l_conn and not l_ok:
                            l_ok = _sym_schema_ready(l_conn)
                        if r_ok and l_ok:
                            break
                        time.sleep(3)
                    except Exception:
                        try:
                            time.sleep(3)
                        except Exception:
                            pass
                try:
                    if r_conn:
                        r_conn.close()
                except Exception:
                    pass
                try:
                    if l_conn:
                        l_conn.close()
                except Exception:
                    pass
                log_func(f"[SymmetricDS] Esquema listo: railway={r_ok}, local={l_ok}")
                return r_ok, l_ok

            _wait_schema_ready(cfg, log)

            # Configurar canal/router/triggers en Railway
            try:
                _configure_railway_server(cfg, log)
            except Exception as e:
                log(f"[SymmetricDS] Configuración de servidor falló: {e}")

            # Configurar canal/router/triggers en Local (cliente)
            try:
                _configure_local_client(cfg, log)
            except Exception as e:
                log(f"[SymmetricDS] Configuración de cliente falló: {e}")

            # Intentar carga inicial automática una vez que el cliente esté registrado
            def _attempt_initial_load():
                try:
                    remote = cfg.get('db_remote') or {}
                    host = str(remote.get('host', ''))
                    port = int(remote.get('port', 5432))
                    db = str(remote.get('database', 'railway'))
                    user = str(remote.get('user', 'postgres'))
                    sslmode = str(remote.get('sslmode', 'require'))
                    pwd = _resolve_password(user, host, port, str(remote.get('password', '')))
                    conn = _connect_pg(host, port, db, user, pwd, sslmode=sslmode)
                    if conn is None:
                        return False
                    node_ext_id = f"local-{get_device_id()}"
                    with conn.cursor() as cur:
                        cur.execute("SELECT node_id FROM sym_node WHERE external_id = %s", (node_ext_id,))
                        row = cur.fetchone()
                        if not row:
                            return False
                        node_id = row[0]
                        # Verificar que la tabla de reload exista
                        try:
                            cur.execute("SELECT to_regclass('public.sym_table_reload_request')")
                            rr = cur.fetchone()
                            if not rr or not rr[0]:
                                return False
                        except Exception:
                            return False
                        tables = _list_public_tables(conn)
                        # Solicitar reload para todas las tablas si no existe petición previa
                        for tbl in tables:
                            try:
                                cur.execute(
                                    """
                                    INSERT INTO sym_table_reload_request (
                                        target_node_id, source_node_id, router_id,
                                        channel_id, table_name, create_table,
                                        delete_before_reload, reload_select,
                                        initial_load_id
                                    )
                                    SELECT %s, node_id, 'toClients', 'default', %s, 1, 1, NULL, nextval('sym_sequence')
                                    FROM sym_node WHERE node_id = 'railway'
                                    ON CONFLICT DO NOTHING
                                    """,
                                    (node_id, tbl)
                                )
                            except Exception:
                                pass
                    try:
                        conn.close()
                    except Exception:
                        pass
                    log(f"[SymmetricDS] Carga inicial solicitada para {len(tables)} tablas al nodo {node_ext_id}")
                    return True
                except Exception:
                    return False

            # Intentar de inmediato; si el cliente aún no está registrado, no hace nada
            try:
                ok_il = _attempt_initial_load()
                if ok_il:
                    log("[SymmetricDS] Carga inicial disparada exitosamente")
                else:
                    log("[SymmetricDS] Carga inicial no aplicada (cliente aún no registrado o tabla inexistente)")
            except Exception as e:
                log(f"[SymmetricDS] Error al intentar carga inicial: {e}")

            # Health check loop
            while True:
                time.sleep(check_interval_sec)
                try:
                    # Si se solicitó stop, terminar y salir
                    if STOP_EVENT.is_set():
                        try:
                            if railway_proc and railway_proc.poll() is None:
                                railway_proc.terminate()
                        except Exception:
                            pass
                        try:
                            if local_proc and local_proc.poll() is None:
                                local_proc.terminate()
                        except Exception:
                            pass
                        try:
                            status_path.write_text(json.dumps({
                                'running': False,
                                'message': 'Stop solicitado',
                                'railway_port': paths.get('railway_port'),
                                'local_port': paths.get('local_port'),
                                'java_version': java_version,
                                'external_id': f"local-{get_device_id()}",
                                'last_check_ts': int(time.time()),
                            }), encoding='utf-8')
                        except Exception:
                            pass
                        break
                    # Tocar la DB local para asegurar conectividad
                    getattr(db_manager, 'ensure_indexes', lambda: None)()
                    # Verificar procesos vivos
                    live = (railway_proc.poll() is None) and (local_proc.poll() is None)
                    status = {
                        'running': live,
                        'message': 'OK' if live else 'Reinicio de engines',
                        'railway_port': paths.get('railway_port'),
                        'local_port': paths.get('local_port'),
                        'java_version': java_version,
                        'external_id': f"local-{get_device_id()}",
                        'last_check_ts': int(time.time()),
                    }
                    if not live:
                        log("[SymmetricDS] Un engine se detuvo; intentando reiniciar (web único)…")
                        new_proc = _start_engine(java_bin, sym_home, Path(paths['railway']), log)
                        if new_proc:
                            railway_proc = new_proc
                            local_proc = new_proc
                            try:
                                PROCESS_HANDLES['web'] = new_proc
                                PROCESS_HANDLES['railway'] = new_proc
                                PROCESS_HANDLES['local'] = new_proc
                            except Exception:
                                pass
                        else:
                            log("[SymmetricDS] Reinicio falló: proceso no lanzado")
                    else:
                        # Ejecutar carga inicial una vez y luego no repetir
                        try:
                            if not getattr(_runner, '_initial_load_done', False):
                                if _attempt_initial_load():
                                    setattr(_runner, '_initial_load_done', True)
                                    try:
                                        log("[SymmetricDS] Carga inicial disparada exitosamente (health-check)")
                                    except Exception:
                                        pass
                        except Exception:
                            pass
                        log("[SymmetricDS] Health check OK (engines vivos)")
                    try:
                        status_path.write_text(json.dumps(status), encoding='utf-8')
                    except Exception:
                        pass
                except Exception as e:
                    log(f"[SymmetricDS] Health check falló: {e}")
                    try:
                        status_path.write_text(json.dumps({
                            'running': False,
                            'message': f'Health check falló: {e}',
                            'railway_port': paths.get('railway_port'),
                            'local_port': paths.get('local_port'),
                            'external_id': f"local-{get_device_id()}",
                            'last_check_ts': int(time.time()),
                        }), encoding='utf-8')
                    except Exception:
                        pass
        except Exception as e:
            try:
                log(f"[SymmetricDS] Error en hilo de arranque: {e}")
            except Exception:
                pass

    thr = threading.Thread(target=_runner, name="SymmetricDSRunner", daemon=True)
    thr.start()
    try:
        globals()['RUNNER_THREAD'] = thr
    except Exception:
        pass
    return True


def stop_symmetricds(logger=None, timeout_sec: int = 10) -> bool:
    """Solicita apagado gracioso de los engines de SymmetricDS.

    Señala STOP_EVENT para que el bucle de salud cierre, intenta terminar los
    procesos y actualiza status.json.
    """
    log = logger or print
    try:
        STOP_EVENT.set()
    except Exception:
        pass
    # Intentar terminar inmediatamente si siguen vivos
    try:
        for name, proc in list(PROCESS_HANDLES.items()):
            try:
                if proc.poll() is None:
                    proc.terminate()
            except Exception:
                pass
    except Exception:
        pass
    # Espera breve y kill si necesario
    try:
        time.sleep(2)
        for name, proc in list(PROCESS_HANDLES.items()):
            try:
                if proc.poll() is None:
                    proc.kill()
            except Exception:
                pass
    except Exception:
        pass
    # Limpiar handles
    try:
        PROCESS_HANDLES.clear()
    except Exception:
        pass
    # Escribir status detenido
    try:
        base_dir = Path(sys.executable).resolve().parent if getattr(sys, 'frozen', False) else Path(__file__).resolve().parent.parent
        status_path = base_dir / 'symmetricds' / 'status.json'
        status_path.write_text(json.dumps({
            'running': False,
            'message': 'Engines detenidos',
            'railway_port': None,
            'local_port': None,
            'last_check_ts': int(time.time()),
        }), encoding='utf-8')
    except Exception:
        pass
    try:
        log("[SymmetricDS] Stop solicitado")
    except Exception:
        pass
    return True