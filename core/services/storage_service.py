import logging
import os
import time
import hashlib
import requests
from typing import Optional, Dict, Any, Tuple

logger = logging.getLogger(__name__)

class StorageService:
    """
    Servicio unificado para almacenamiento de archivos en Backblaze B2 
    y distribución a través de Cloudflare CDN.
    """
    def __init__(self):
        self.key_id = os.getenv("B2_KEY_ID") or os.getenv("B2_MASTER_KEY_ID")
        self.app_key = os.getenv("B2_APP_KEY") or os.getenv("B2_MASTER_APPLICATION_KEY")
        self.bucket_name = os.getenv("B2_BUCKET_NAME")
        self.bucket_id = os.getenv("B2_BUCKET_ID")
        self.cdn_domain = os.getenv("CLOUDFLARE_CDN_URL") or os.getenv("CDN_URL")
        
        self._auth_token = None
        self._api_url = None
        self._download_url = None
        self._auth_timestamp = 0

    def _authorize(self) -> bool:
        """Autentica con la API de B2 y cachea el token."""
        if not self.key_id or not self.app_key:
            logger.warning("Faltan credenciales de B2 (B2_KEY_ID, B2_APP_KEY). Almacenamiento en la nube deshabilitado.")
            return False

        # Re-autenticar si el token tiene más de 23 horas (duran 24h)
        if self._auth_token and (time.time() - self._auth_timestamp) < 82000:
            return True

        try:
            auth_url = "https://api.backblazeb2.com/b2api/v2/b2_authorize_account"
            resp = requests.get(auth_url, auth=(self.key_id, self.app_key), timeout=10)
            if resp.status_code != 200:
                logger.error(f"Error autorizando B2: {resp.text}")
                return False
            
            data = resp.json()
            self._auth_token = data.get("authorizationToken")
            self._api_url = data.get("apiUrl")
            self._download_url = data.get("downloadUrl")
            self._auth_timestamp = time.time()
            
            # Auto-detect bucket ID if name is provided but ID is missing
            if self.bucket_name and not self.bucket_id:
                allowed = data.get("allowed", {}).get("bucketName")
                if allowed and allowed == self.bucket_name:
                     self.bucket_id = data.get("allowed", {}).get("bucketId")
            
            return True
        except Exception as e:
            logger.error(f"Excepción autorizando B2: {e}")
            return False

    def _get_upload_url(self) -> Optional[Dict[str, str]]:
        """Obtiene una URL temporal para subir un archivo."""
        if not self._authorize():
            return None
        if not self.bucket_id:
            logger.error("Falta B2_BUCKET_ID. No se puede obtener URL de subida.")
            return None
            
        try:
            url = f"{self._api_url}/b2api/v2/b2_get_upload_url"
            headers = {"Authorization": self._auth_token}
            data = {"bucketId": self.bucket_id}
            resp = requests.post(url, headers=headers, json=data, timeout=10)
            if resp.status_code != 200:
                logger.error(f"Error obteniendo URL de subida: {resp.text}")
                return None
            return resp.json()
        except Exception as e:
            logger.error(f"Excepción obteniendo upload URL: {e}")
            return None

    def upload_file(self, file_data: bytes, file_name: str, content_type: str, subfolder: str = "") -> Optional[str]:
        """
        Sube un archivo a B2 y devuelve la URL pública (CDN o directa).
        
        Args:
            file_data: Contenido del archivo en bytes.
            file_name: Nombre del archivo.
            content_type: MIME type (ej. image/png, video/mp4).
            subfolder: Carpeta virtual (ej. 'logos', 'videos').
            
        Returns:
            str: URL pública del archivo o None si falla.
        """
        if not self.key_id or not self.app_key:
            return None

        upload_info = self._get_upload_url()
        if not upload_info:
            return None
            
        upload_url = upload_info.get("uploadUrl")
        upload_token = upload_info.get("authorizationToken")
        
        # Sanitizar y construir path
        safe_name = "".join(c for c in file_name if c.isalnum() or c in "._-").strip()
        if subfolder:
            safe_sub = "".join(c for c in subfolder if c.isalnum() or c in "._-").strip()
            final_name = f"{safe_sub}/{safe_name}"
        else:
            final_name = safe_name
            
        # B2 requiere SHA1 del contenido en header
        sha1_of_file_data = hashlib.sha1(file_data).hexdigest()
        
        headers = {
            "Authorization": upload_token,
            "X-Bz-File-Name": final_name, 
            "Content-Type": content_type,
            "X-Bz-Content-Sha1": sha1_of_file_data,
            "X-Bz-Info-Author": "gym-management-system"
        }
        
        try:
            resp = requests.post(upload_url, headers=headers, data=file_data, timeout=60)
            if resp.status_code != 200:
                logger.error(f"Error subiendo archivo a B2: {resp.text}")
                return None
                
            # Construir URL pública
            return self.get_file_url(final_name)
            
        except Exception as e:
            logger.error(f"Excepción subiendo archivo: {e}")
            return None

    def get_file_url(self, file_path: str) -> str:
        """Construye la URL pública para un archivo existente."""
        if not file_path: return ""
        
        # Si ya es una URL completa, devolverla
        if file_path.startswith("http") or file_path.startswith("/"):
            return file_path
            
        path = file_path.lstrip("/")
        
        if self.cdn_domain and self.bucket_name:
            base = self.cdn_domain.rstrip("/")
            if not base.startswith("http"):
                base = f"https://{base}"
            # Cloudflare suele mapear root -> bucket root o /file/bucket_name/ -> root
            # Asumimos estructura estándar: /file/<bucket>/<path>
            return f"{base}/file/{self.bucket_name}/{path}"
            
        if self._download_url and self.bucket_name:
             return f"{self._download_url}/file/{self.bucket_name}/{path}"
             
        # Fallback si no se ha inicializado (intenta inicializar)
        if self._authorize() and self._download_url:
            return f"{self._download_url}/file/{self.bucket_name}/{path}"
            
        return file_path
