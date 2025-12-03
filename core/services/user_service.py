from typing import List, Optional, Dict, Any
from datetime import date, datetime, timedelta
from sqlalchemy.orm import Session
from core.services.base import BaseService
from core.database.repositories.user_repository import UserRepository
from core.database.repositories.payment_repository import PaymentRepository
from core.database.repositories.gym_repository import GymRepository
from core.database.repositories.teacher_repository import TeacherRepository
from core.database.orm_models import Usuario

class UserService(BaseService):
    def __init__(self, db: Session = None):
        super().__init__(db)
        self.user_repo = UserRepository(self.db, None, None) # Logger and Cache can be None for now or injected
        self.payment_repo = PaymentRepository(self.db, None, None)
        self.gym_repo = GymRepository(self.db, None, None)

    def get_user(self, user_id: int) -> Optional[Usuario]:
        return self.user_repo.obtener_usuario(user_id)

    def get_user_by_dni(self, dni: str) -> Optional[Usuario]:
        return self.user_repo.obtener_usuario_por_dni(dni)

    def list_users(self, q: Optional[str] = None, limit: int = 50, offset: int = 0) -> List[Dict]:
        # Moving logic from router's api_usuarios_list
        # The repository has buscar_usuarios but it returns a specific dict format and doesn't support offset/limit fully in the same way.
        # We should probably enhance the repository or just implement the specific query here if it's complex filtering.
        # Ideally, we ask the repository to do it.
        
        # Let's use the repository's finding capabilities.
        # If the repo method isn't enough, we should add a method to the repo, not write SQL here.
        # But for now, to strictly follow the plan of moving logic out of Routers:
        
        return self.user_repo.listar_usuarios_paginados(q, limit, offset)

    def create_user(self, data: Dict[str, Any]) -> int:
        # Validation logic moved from router
        dni = data.get("dni")
        if self.user_repo.obtener_usuario_por_dni(dni):
            raise ValueError("DNI ya existe")
        
        # Create object
        usuario = Usuario(**data)
        return self.user_repo.crear_usuario(usuario)

    def update_user(self, user_id: int, data: Dict[str, Any], modifier_id: Optional[int] = None, is_owner: bool = False) -> bool:
        # Logic for PIN update and ID change
        current_user = self.user_repo.obtener_usuario(user_id)
        if not current_user:
            raise ValueError("Usuario no encontrado")
            
        # Handle PIN logic (preserve if not provided)
        if "pin" not in data or data["pin"] is None:
             data["pin"] = current_user.pin
             
        # Update fields
        for k, v in data.items():
            if k != "new_id" and hasattr(current_user, k):
                setattr(current_user, k, v)
                
        self.user_repo.actualizar_usuario(current_user)
        
        # Handle ID change
        new_id = data.get("new_id")
        if new_id and int(new_id) != user_id:
            if not is_owner:
                raise PermissionError("Solo el dueño puede cambiar el ID de usuario")
            self.user_repo.cambiar_usuario_id(user_id, int(new_id))
            
        return True

    def delete_user(self, user_id: int):
        self.user_repo.eliminar_usuario(user_id)

    def get_user_panel_data(self, user_id: int) -> Dict[str, Any]:
        u = self.user_repo.obtener_usuario(user_id)
        if not u:
            return None
            
        # Calculate days remaining
        dias_restantes = None
        fpv = u.fecha_proximo_vencimiento
        if fpv:
             delta = (fpv - date.today()).days
             dias_restantes = delta

        # Get last payments
        pagos = self.payment_repo.obtener_ultimos_pagos(user_id, limit=10)
        
        # Get routines (GymRepo might handle this or we need a RoutineRepository)
        # Assuming we can access routines via relationship or simple query
        # For now, let's assume GymRepository has something or we add it.
        rutinas = [r for r in u.rutinas if r.activa] # SQLAlchemy relationship if available
        
        return {
            "usuario": u,
            "dias_restantes": dias_restantes,
            "pagos": pagos,
            "rutinas": rutinas
        }

    def get_user_tags(self, user_id: int):
        return self.user_repo.obtener_etiquetas_usuario(user_id)

    def add_user_tag(self, user_id: int, tag_data: Dict[str, Any], assigned_by: Optional[int]):
        etiqueta_id = tag_data.get("etiqueta_id")
        nombre = tag_data.get("nombre")
        
        if not etiqueta_id and nombre:
            # Create tag if not exists
            # This logic was in the router
            # We need a method in repo to get_or_create tag
            et = self.user_repo.obtener_o_crear_etiqueta(nombre) # We need to ensure this exists
            etiqueta_id = et.id
            
        if etiqueta_id:
            self.user_repo.asignar_etiqueta(user_id, etiqueta_id, assigned_by)
            return True
        return False

    def remove_user_tag(self, user_id: int, tag_id: int):
        self.user_repo.remover_etiqueta(user_id, tag_id)
        return True
