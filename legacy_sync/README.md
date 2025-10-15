Este directorio archiva el sistema de sincronización legacy basado en HTTP/proxy.

Contenido movido desde la raíz del proyecto y utils_modules/:
- sync_client.py
- local_proxy.py
- download_sync_worker.py
- offline_sync_manager.py
- utils_modules/proxy_manager.py
- utils_modules/proxy_watchdog.py
- Bases de cola: proxy_queue.sqlite, offline_queue.sqlite

Motivo: Migración completa a SymmetricDS para sincronización bidireccional.