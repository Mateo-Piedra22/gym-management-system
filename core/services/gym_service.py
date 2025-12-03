from typing import List, Optional, Dict, Any
from datetime import time
from sqlalchemy.orm import Session
from core.services.base import BaseService
from core.database.repositories.gym_repository import GymRepository

class GymService(BaseService):
    def __init__(self, db: Session = None):
        super().__init__(db)
        self.gym_repo = GymRepository(self.db, None, None)

    def get_config(self) -> Dict[str, str]:
        return self.gym_repo.obtener_configuracion_gimnasio()

    def update_config(self, data: Dict[str, Any]) -> bool:
        return self.gym_repo.actualizar_configuracion_gimnasio(data)

    def list_exercises(self) -> List[Dict]:
        return self.gym_repo.obtener_todos_ejercicios()

    def create_exercise(self, data: Dict[str, Any]) -> int:
        return self.gym_repo.crear_ejercicio(**data)
        
    def update_exercise(self, exercise_id: int, data: Dict[str, Any]) -> bool:
        return self.gym_repo.actualizar_ejercicio(exercise_id, **data)

    def delete_exercise(self, exercise_id: int) -> bool:
        return self.gym_repo.eliminar_ejercicio(exercise_id)

    def list_routines(self) -> List[Dict]:
        return self.gym_repo.obtener_todas_rutinas()

    def get_routine_details(self, routine_id: int) -> Dict:
        return self.gym_repo.obtener_detalles_rutina(routine_id)
        
    def create_routine(self, data: Dict[str, Any]) -> int:
        # data should contain 'nombre', 'usuario_id', etc.
        # and optionally 'ejercicios' list
        ejercicios = data.pop('ejercicios', [])
        rutina_id = self.gym_repo.crear_rutina(**data)
        
        for idx, ej in enumerate(ejercicios):
            self.gym_repo.agregar_ejercicio_rutina(
                rutina_id, 
                ej.get('ejercicio_id'), 
                series=ej.get('series', 3), 
                repeticiones=ej.get('repeticiones', '10'), 
                dia_semana=ej.get('dia_semana'), 
                orden=idx
            )
        return rutina_id

    def delete_routine(self, routine_id: int) -> bool:
        return self.gym_repo.eliminar_rutina(routine_id)

    # Classes Management
    def list_classes(self) -> List[Dict]:
        return self.gym_repo.obtener_todas_clases()

    def create_class(self, name: str, description: str = None, type_id: int = None) -> int:
        return self.gym_repo.crear_clase(name, description, type_id)

    def schedule_class(self, class_id: int, day: str, start: time, end: time, quota: int = 20) -> int:
        return self.gym_repo.programar_horario_clase(class_id, day, start, end, quota)

    def get_class_schedules(self, class_id: int) -> List[Dict]:
        return self.gym_repo.obtener_horarios_clase(class_id)

    def enroll_user_in_class(self, user_id: int, schedule_id: int) -> bool:
        return self.gym_repo.inscribir_usuario_clase(user_id, schedule_id)

    def cancel_class_enrollment(self, user_id: int, schedule_id: int) -> bool:
        return self.gym_repo.cancelar_inscripcion_clase(user_id, schedule_id)
