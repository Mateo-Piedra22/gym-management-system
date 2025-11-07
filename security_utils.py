"""
Utilidades de seguridad para el Sistema de Gestión de Gimnasio.

Este módulo proporciona funciones para el hash seguro de contraseñas,
generación de tokens y otras utilidades de seguridad.
"""

try:
    import bcrypt  # type: ignore
except Exception:
    bcrypt = None  # type: ignore
import secrets
import string
from typing import Optional

class SecurityUtils:
    """Clase de utilidades de seguridad."""
    
    @staticmethod
    def hash_password(password: str) -> str:
        """
        Genera un hash seguro de contraseña usando bcrypt.
        
        Args:
            password: Contraseña en texto plano
            
        Returns:
            Hash de contraseña como string
        """
        if not password:
            return ""
        if bcrypt:
            salt = bcrypt.gensalt()
            hashed = bcrypt.hashpw(password.encode('utf-8'), salt)
            return hashed.decode('utf-8')
        return password
    
    @staticmethod
    def verify_password(password: str, hashed: str) -> bool:
        """
        Verifica una contraseña contra su hash.
        
        Args:
            password: Contraseña en texto plano
            hashed: Hash almacenado
            
        Returns:
            True si la contraseña coincide, False en caso contrario
        """
        if not hashed:
            return False
        hs = str(hashed)
        if bcrypt and hs.startswith("$2"):
            try:
                return bcrypt.checkpw(password.encode('utf-8'), hs.encode('utf-8'))
            except Exception:
                return False
        return password == hs
    
    @staticmethod
    def generate_secure_token(length: int = 32) -> str:
        """
        Genera un token seguro aleatorio.
        
        Args:
            length: Longitud del token
            
        Returns:
            Token seguro como string
        """
        return secrets.token_urlsafe(length)
    
    @staticmethod
    def generate_secure_password(length: int = 16) -> str:
        """
        Genera una contraseña segura aleatoria.
        
        Args:
            length: Longitud de la contraseña
            
        Returns:
            Contraseña segura como string
        """
        alphabet = string.ascii_letters + string.digits + "!@#$%^&*"
        return ''.join(secrets.choice(alphabet) for _ in range(length))
    
    @staticmethod
    def generate_api_key(prefix: str = "gymms") -> str:
        """
        Genera una clave API segura con prefijo.
        
        Args:
            prefix: Prefijo para la clave API
            
        Returns:
            Clave API segura
        """
        token = secrets.token_urlsafe(32)
        return f"{prefix}_{token}"
    
    @staticmethod
    def sanitize_input(input_string: str, max_length: int = 255) -> str:
        """
        Sanitiza entrada de usuario para prevenir inyección.
        
        Args:
            input_string: String a sanitizar
            max_length: Longitud máxima permitida
            
        Returns:
            String sanitizado
        """
        if not input_string:
            return ""
        
        # Limitar longitud
        input_string = input_string[:max_length]
        
        # Eliminar caracteres peligrosos para SQL/HTML
        dangerous_chars = ['<', '>', '"', "'", '&', ';', '--', '/*', '*/', 'xp_']
        for char in dangerous_chars:
            input_string = input_string.replace(char, '')
        
        return input_string.strip()
    
    @staticmethod
    def validate_password_strength(password: str) -> dict:
        """
        Valida la fortaleza de una contraseña.
        
        Args:
            password: Contraseña a validar
            
        Returns:
            Diccionario con resultado de validación
        """
        result = {
            'valid': False,
            'length_ok': False,
            'has_upper': False,
            'has_lower': False,
            'has_digit': False,
            'has_special': False,
            'score': 0,
            'message': ''
        }
        
        # Verificar longitud
        if len(password) >= 8:
            result['length_ok'] = True
            result['score'] += 1
        
        # Verificar mayúsculas
        if any(c.isupper() for c in password):
            result['has_upper'] = True
            result['score'] += 1
        
        # Verificar minúsculas
        if any(c.islower() for c in password):
            result['has_lower'] = True
            result['score'] += 1
        
        # Verificar dígitos
        if any(c.isdigit() for c in password):
            result['has_digit'] = True
            result['score'] += 1
        
        # Verificar caracteres especiales
        if any(c in string.punctuation for c in password):
            result['has_special'] = True
            result['score'] += 1
        
        # Determinar validez
        if result['score'] >= 4:
            result['valid'] = True
            result['message'] = 'Contraseña fuerte'
        elif result['score'] >= 3:
            result['message'] = 'Contraseña media'
        else:
            result['message'] = 'Contraseña débil'
        
        return result