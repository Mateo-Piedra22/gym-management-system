from typing import List, Optional, Dict, Any
from datetime import date, datetime
from sqlalchemy.orm import Session
from core.services.base import BaseService
from core.database.repositories.teacher_repository import TeacherRepository

class TeacherService(BaseService):
    def __init__(self, db: Session = None):
        super().__init__(db)
        self.teacher_repo = TeacherRepository(self.db, None, None)

    def list_teachers_basic(self) -> List[Dict]:
        return self.teacher_repo.obtener_profesores_basico_con_ids()

    def get_teacher_details_list(self, start_date: Optional[date] = None, end_date: Optional[date] = None) -> List[Dict]:
        return self.teacher_repo.obtener_detalle_profesores(start_date, end_date)

    def get_teacher(self, teacher_id: int) -> Optional[Dict]:
        return self.teacher_repo.obtener_profesor_por_id(teacher_id)

    def update_teacher(self, teacher_id: int, data: Dict[str, Any]) -> bool:
        return self.teacher_repo.actualizar_profesor(teacher_id, **data)
    
    def get_teacher_sessions(self, teacher_id: int, start_date: Optional[date] = None, end_date: Optional[date] = None):
        return self.teacher_repo.obtener_horas_trabajadas_profesor(teacher_id, start_date, end_date)
