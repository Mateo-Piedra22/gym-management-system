from typing import List, Optional, Dict, Any
from datetime import date, datetime
from sqlalchemy.orm import Session
from core.services.base import BaseService
from core.database.repositories.payment_repository import PaymentRepository

class PaymentService(BaseService):
    def __init__(self, db: Session = None):
        super().__init__(db)
        self.payment_repo = PaymentRepository(self.db, None, None)

    def get_payment_methods(self, only_active: bool = True) -> List[Dict]:
        return self.payment_repo.obtener_metodos_pago(only_active)

    def register_payments_batch(self, items: List[Dict[str, Any]], skip_duplicates: bool = False) -> Dict[str, Any]:
        return self.payment_repo.registrar_pagos_batch(items, skip_duplicates=skip_duplicates)
