import os
import time
import logging
from contextlib import contextmanager


_LOG_DIR = 'logs'
_LOG_FILE = os.path.join(_LOG_DIR, 'ui_profile.log')


def _ensure_log_dir():
    try:
        os.makedirs(_LOG_DIR, exist_ok=True)
    except Exception:
        pass


@contextmanager
def profile(section: str, extra: str | None = None):
    """Context manager para perfilar secciones que corren en el hilo de UI.

    Registra en logging y en `logs/ui_profile.log` el tiempo transcurrido.
    """
    start = time.perf_counter()
    try:
        yield
    finally:
        duration = time.perf_counter() - start
        msg = f"UI profile: {section} took {duration:.3f}s"
        if extra:
            msg += f" | {extra}"
        try:
            logging.info(msg)
        except Exception:
            pass
        try:
            _ensure_log_dir()
            with open(_LOG_FILE, 'a', encoding='utf-8') as f:
                f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} {msg}\n")
        except Exception:
            pass