"""
Vercel Python Serverless entrypoint for the FastAPI app.

This exposes the FastAPI instance as `app`, which Vercel can serve
via its Python runtime. No local server is started here.
"""

from apps.webapp.server import app

__all__ = ["app"]