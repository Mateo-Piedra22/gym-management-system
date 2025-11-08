"""
Vercel Python Serverless entrypoint for the FastAPI app.

This exposes the FastAPI instance as `app`, which Vercel can serve
via its Python runtime. No local server is started here.
"""

from webapp.server import app  # FastAPI instance

# Optional: make `__all__` explicit
__all__ = ["app"]