import os
import logging
from dotenv import load_dotenv

# Cargar variables de entorno antes de importar otras cosas
load_dotenv()

from apps.webapp.main import app as tenant_app
from apps.admin.main import admin_app

logger = logging.getLogger(__name__)

class DomainDispatcher:
    def __init__(self, app_default, app_admin):
        self.app_default = app_default
        self.app_admin = app_admin
        self.base_domain = (os.getenv("TENANT_BASE_DOMAIN") or "").strip().lower()

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" and scope["type"] != "websocket":
             await self.app_default(scope, receive, send)
             return

        host = ""
        for k, v in scope.get("headers", []):
            if k == b"host":
                host = v.decode("utf-8").split(":")[0].lower()
                break
        
        is_admin = False
        
        if self.base_domain:
            if host == self.base_domain:
                is_admin = True
            elif host == f"www.{self.base_domain}":
                is_admin = True
            elif host == f"admin.{self.base_domain}":
                is_admin = True
        
        # Optional: Localhost override for testing admin on a specific port? 
        # Or if we access via IP?
        # For now, we strictly follow domain logic for isolation.
        # If no match, it goes to tenant_app, which handles "unknown tenant" or displays a landing if configured.
        
        if is_admin:
            await self.app_admin(scope, receive, send)
        else:
            await self.app_default(scope, receive, send)

app = DomainDispatcher(tenant_app, admin_app)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
