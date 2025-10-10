from PyQt6.QtWidgets import QDialog, QFormLayout, QSpinBox, QDoubleSpinBox, QDialogButtonBox, QLabel
from models import Pago

class PaymentDialog(QDialog):
    def __init__(self, parent=None, pago: Pago = None):
        super().__init__(parent)
        
        self.pago = pago
        self.setWindowTitle("Modificar Pago")

        if not self.pago:
            # Si no hay pago, este diálogo no debería usarse.
            self.setWindowTitle("Error")
            return

        # --- Layout y Widgets ---
        layout = QFormLayout(self)
        
        self.month_spinbox = QSpinBox()
        self.month_spinbox.setRange(1, 12)
        
        self.year_spinbox = QSpinBox()
        self.year_spinbox.setRange(2020, 2050)
        
        self.amount_spinbox = QDoubleSpinBox()
        self.amount_spinbox.setRange(0, 999999)
        self.amount_spinbox.setSingleStep(500)
        self.amount_spinbox.setPrefix("$ ")

        layout.addRow(QLabel("Mes:"), self.month_spinbox)
        layout.addRow(QLabel("Año:"), self.year_spinbox)
        layout.addRow(QLabel("Monto (ARS):"), self.amount_spinbox)

        self.buttonBox = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        layout.addRow(self.buttonBox)
        
        self.buttonBox.accepted.connect(self.accept)
        self.buttonBox.rejected.connect(self.reject)

        self.load_payment_data()

    def load_payment_data(self):
        """Carga los datos del pago en los campos del formulario."""
        self.month_spinbox.setValue(self.pago.mes)
        self.year_spinbox.setValue(self.pago.año)
        self.amount_spinbox.setValue(self.pago.monto)

    def get_payment_data(self) -> Pago:
        """Recoge los datos del formulario y actualiza el objeto Pago."""
        if self.pago:
            self.pago.mes = self.month_spinbox.value()
            self.pago.año = self.year_spinbox.value()
            self.pago.monto = self.amount_spinbox.value()
        return self.pago

