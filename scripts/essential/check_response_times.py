import os
import json
import time
from pathlib import Path

import psycopg2


def _load_db_env_from_config():
    """Set DB_* env vars from config/config.json if not already present."""
    cfg_path = Path(__file__).resolve().parents[2] / 'config' / 'config.json'
    if not cfg_path.exists():
        return False
    try:
        with open(cfg_path, 'r', encoding='utf-8') as f:
            cfg = json.load(f) or {}
        node = cfg.get('db_connection') or cfg
        # Only set if missing to avoid overriding explicit env
        os.environ.setdefault('DB_HOST', str(node.get('host') or 'localhost'))
        os.environ.setdefault('DB_PORT', str(node.get('port') or 5432))
        os.environ.setdefault('DB_NAME', str(node.get('database') or 'gimnasio'))
        os.environ.setdefault('DB_USER', str(node.get('user') or 'postgres'))
        os.environ.setdefault('DB_PASSWORD', str(node.get('password') or ''))
        os.environ.setdefault('DB_SSLMODE', str(node.get('sslmode') or 'prefer'))
        os.environ.setdefault('DB_CONNECT_TIMEOUT', str(node.get('connect_timeout') or 10))
        os.environ.setdefault('DB_APPLICATION_NAME', str(node.get('application_name') or 'gym_management_system'))
        return True
    except Exception:
        return False


def _connect():
    host = os.getenv('DB_HOST', 'localhost')
    port = int(os.getenv('DB_PORT', '5432'))
    dbname = os.getenv('DB_NAME', 'gimnasio')
    user = os.getenv('DB_USER', 'postgres')
    password = os.getenv('DB_PASSWORD', '')
    sslmode = os.getenv('DB_SSLMODE', 'prefer')
    app_name = os.getenv('DB_APPLICATION_NAME', 'gym_management_system')
    timeout = int(os.getenv('DB_CONNECT_TIMEOUT', '10'))
    return psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
        sslmode=sslmode,
        application_name=app_name,
        connect_timeout=timeout,
    )


def _measure(cursor, name, sql, params=None):
    start = time.perf_counter()
    cursor.execute(sql, params or ())
    elapsed = (time.perf_counter() - start) * 1000.0
    try:
        row = cursor.fetchone()
        value = row[0] if row is not None and len(row) > 0 else None
    except Exception:
        value = None
    return {
        'query': name,
        'time_ms': round(elapsed, 2),
        'result': value,
    }


def main():
    # Ensure env is set; prefer existing env, fallback to config.json
    _load_db_env_from_config()

    results = {
        'ok': False,
        'env_profile': os.getenv('DB_PROFILE', 'env/default'),
        'db_host': os.getenv('DB_HOST', ''),
        'db_name': os.getenv('DB_NAME', ''),
        'metrics': [],
        'notes': [],
    }

    conn = None
    try:
        conn = _connect()
        cursor = conn.cursor()

        # Core counts
        results['metrics'].append(_measure(cursor, 'usuarios_activos_count', "SELECT COUNT(*) FROM usuarios WHERE activo = TRUE"))

        results['metrics'].append(_measure(cursor, 'asistencias_hoy_count', "SELECT COUNT(*) FROM asistencias WHERE fecha = CURRENT_DATE"))

        results['metrics'].append(_measure(
            cursor,
            'pagos_mes_actual_sum',
            "SELECT COALESCE(SUM(monto), 0) FROM pagos WHERE fecha_pago >= date_trunc('month', CURRENT_DATE) AND fecha_pago < date_trunc('month', CURRENT_DATE) + INTERVAL '1 month'",
        ))

        # WhatsApp-related
        try:
            results['metrics'].append(_measure(cursor, 'whatsapp_templates_activos', "SELECT COUNT(*) FROM whatsapp_templates WHERE active = TRUE"))
        except Exception as e:
            try:
                cursor.connection.rollback()
            except Exception:
                pass
            results['notes'].append(f"whatsapp_templates not available: {e}")

        try:
            results['metrics'].append(_measure(cursor, 'whatsapp_config_activa', "SELECT COUNT(*) FROM whatsapp_config WHERE active = TRUE"))
        except Exception as e:
            try:
                cursor.connection.rollback()
            except Exception:
                pass
            results['notes'].append(f"whatsapp_config not available: {e}")

        try:
            results['metrics'].append(_measure(cursor, 'whatsapp_outbound_last30_nofailed', "SELECT COUNT(*) FROM whatsapp_messages WHERE user_id IS NOT NULL AND message_type = 'outbound' AND sent_at >= NOW() - INTERVAL '30 days' AND status <> 'failed'"))
        except Exception as e:
            try:
                cursor.connection.rollback()
            except Exception:
                pass
            results['notes'].append(f"whatsapp_messages not available: {e}")

        # Profesor horas: sesiones activas
        try:
            results['metrics'].append(_measure(cursor, 'profesor_sesion_activa_count', "SELECT COUNT(*) FROM profesor_horas_trabajadas WHERE hora_fin IS NULL"))
        except Exception as e:
            try:
                cursor.connection.rollback()
            except Exception:
                pass
            results['notes'].append(f"profesor_horas_trabajadas not available: {e}")

        # Profesor horas: cerradas últimos 30 días para un profesor
        try:
            cursor.execute("SELECT id FROM profesores ORDER BY id ASC LIMIT 1")
            row = cursor.fetchone()
            if row and row[0]:
                profesor_id = int(row[0])
                results['metrics'].append(_measure(
                    cursor,
                    'profesor_sesiones_cerradas_last30',
                    "SELECT COUNT(*) FROM profesor_horas_trabajadas WHERE profesor_id = %s AND hora_fin IS NOT NULL AND fecha_inicio >= CURRENT_DATE - INTERVAL '30 days'",
                    (profesor_id,),
                ))
            else:
                results['notes'].append('No hay profesores para medir sesiones cerradas.')
        except Exception as e:
            try:
                cursor.connection.rollback()
            except Exception:
                pass
            results['notes'].append(f"profesores lookup failed: {e}")

        results['ok'] = True

    except Exception as e:
        results['error'] = str(e)
        results['ok'] = False
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass

    try:
        print(json.dumps(results, ensure_ascii=False))
    except Exception:
        print(str(results))


if __name__ == '__main__':
    main()