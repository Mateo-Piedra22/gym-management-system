import os
from typing import Dict, List, Tuple


REQUIRED_ENV: Dict[str, List[str]] = {
    'local': [
        'DB_LOCAL_HOST', 'DB_LOCAL_PORT', 'DB_LOCAL_DATABASE',
        'DB_LOCAL_USER', 'DB_LOCAL_PASSWORD'
    ],
    'remote': [
        'DB_REMOTE_HOST', 'DB_REMOTE_PORT', 'DB_REMOTE_DATABASE',
        'DB_REMOTE_USER', 'DB_REMOTE_PASSWORD'
    ],
}

OPTIONAL_ENV: List[str] = [
    'DB_LOCAL_SSLMODE', 'DB_REMOTE_SSLMODE',
    'DB_LOCAL_CONNECT_TIMEOUT', 'DB_REMOTE_CONNECT_TIMEOUT',
    'REPLICATION_SUBSCRIPTION_NAME', 'REPLICATION_PUBLICATION_NAME',
    'REMOTE_CAN_REACH_LOCAL',
]


def validate_environment() -> Dict[str, object]:
    """Valida presencia de variables de entorno críticas para PostgreSQL.

    Devuelve un dict con `ok`, `missing`, `present` y `profile`.
    """
    profile = str(os.getenv('DB_PROFILE', 'local')).lower()
    required = REQUIRED_ENV.get(profile, REQUIRED_ENV['local'])
    missing: List[str] = []
    present: List[Tuple[str, str]] = []

    for key in required:
        val = os.getenv(key)
        if val is None or str(val).strip() == '':
            missing.append(key)
        else:
            present.append((key, '***' if 'PASSWORD' in key else str(val)))

    # También aceptar el fallback genérico DB_* si faltan claves específicas
    if missing:
        fallbacks = ['DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASSWORD']
        fb_missing = [k for k in fallbacks if not os.getenv(k)]
        if len(fb_missing) < len(fallbacks):
            # Si al menos hay algunas de las genéricas, considerar válido para correr
            for k in fallbacks:
                v = os.getenv(k)
                if v:
                    present.append((k, '***' if 'PASSWORD' in k else str(v)))
            missing = []

    return {
        'ok': len(missing) == 0,
        'missing': missing,
        'present': present,
        'profile': profile,
    }

