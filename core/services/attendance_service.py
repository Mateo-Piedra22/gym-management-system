from typing import List, Optional, Dict, Any
from datetime import date, datetime
from sqlalchemy.orm import Session
from core.services.base import BaseService
from core.database.repositories.attendance_repository import AttendanceRepository

class AttendanceService(BaseService):
    def __init__(self, db: Session = None):
        super().__init__(db)
        self.attendance_repo = AttendanceRepository(self.db, None, None)

    def register_attendance(self, user_id: int, attendance_date: Optional[date] = None) -> int:
        return self.attendance_repo.registrar_asistencia(user_id, attendance_date)

    def register_attendance_batch(self, items: List[Dict[str, Any]]) -> Dict[str, Any]:
        return self.attendance_repo.registrar_asistencias_batch(items)

    def get_attendances_by_date(self, query_date: date) -> List[Dict]:
        return self.attendance_repo.obtener_asistencias_por_fecha(query_date)

    def delete_attendance(self, attendance_id: int):
        self.attendance_repo.eliminar_asistencia(attendance_id)

    def create_checkin_token(self, user_id: int, token: str) -> int:
        return self.attendance_repo.crear_checkin_token(user_id, token)

    def validate_checkin_token(self, token: str, user_id: int):
        return self.attendance_repo.validar_token_y_registrar_asistencia(token, user_id)
