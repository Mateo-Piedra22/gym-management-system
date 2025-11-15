"""
Vercel Python Serverless entrypoint for the FastAPI app.

This exposes the FastAPI instance as `app`, which Vercel can serve
via its Python runtime. No local server is started here.
"""

from apps.webapp.server import app
try:
    from apps.admin.main import admin_app
    app.mount("/admin", admin_app)
except Exception:
    import logging
    logging.exception("Admin mount failed")

__all__ = ["app"]