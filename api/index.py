"""
Vercel Python Serverless entrypoint for the FastAPI app.

This exposes the FastAPI instance as `app`, which Vercel can serve
via its Python runtime. No local server is started here.
"""

from fastapi import FastAPI

app = FastAPI()
try:
    from apps.admin.main import admin_app
    app.mount("/admin", admin_app)
except Exception:
    pass
try:
    from apps.webapp.server import app as webapp
    app.mount("/", webapp)
except Exception:
    pass

__all__ = ["app"]