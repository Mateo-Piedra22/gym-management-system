import logging
import psycopg2
import psycopg2.extras
from contextlib import contextmanager
from typing import Dict, Any, Generator

logger = logging.getLogger(__name__)

class RawPostgresManager:
    """
    Gestor de conexiones 'crudas' (psycopg2) para tareas administrativas.
    Se utiliza exclusivamente en el Admin Panel para operaciones de infraestructura
    (crear DBs, gestionar inquilinos) donde el ORM no es adecuado.
    """
    def __init__(self, connection_params: Dict[str, Any]):
        self.params = connection_params
        self.logger = logger

    @contextmanager
    def get_connection_context(self) -> Generator[Any, None, None]:
        """
        Provee un contexto de conexión psycopg2 que se cierra automáticamente.
        """
        conn = None
        try:
            # Extraer parámetros asegurando compatibilidad con psycopg2
            pg_params = {
                "host": self.params.get("host"),
                "port": self.params.get("port"),
                "dbname": self.params.get("database"),
                "user": self.params.get("user"),
                "password": self.params.get("password"),
                "sslmode": self.params.get("sslmode", "require"),
                "connect_timeout": self.params.get("connect_timeout", 10),
                "application_name": self.params.get("application_name", "gym_admin_raw")
            }
            
            conn = psycopg2.connect(**pg_params)
            # Por defecto autocommit=False, el servicio debe hacer commit explícito
            # o podemos habilitarlo si preferimos. AdminService hace commits explícitos.
            yield conn
        except Exception as e:
            self.logger.error(f"Error en conexión RawPostgresManager: {e}")
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            raise e
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    def inicializar_base_datos(self):
        """
        Método de compatibilidad/placeholder si se requiere inicialización específica.
        En este contexto raw, la inicialización suele ser manual o vía scripts.
        """
        pass
