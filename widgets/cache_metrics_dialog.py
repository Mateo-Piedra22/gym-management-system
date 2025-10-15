import logging
from PyQt6.QtWidgets import QDialog, QVBoxLayout, QLabel, QPushButton, QHBoxLayout, QTableWidget, QTableWidgetItem, QHeaderView
from PyQt6.QtCore import Qt


class CacheMetricsDialog(QDialog):
    """Diálogo para visualizar métricas actuales de la caché."""

    def __init__(self, db_manager, parent=None):
        super().__init__(parent)
        self.db_manager = db_manager
        self.setWindowTitle("Métricas de Caché")
        self.resize(500, 320)
        self._setup_ui()
        self._load_metrics()

    def _setup_ui(self):
        layout = QVBoxLayout(self)
        self.info_label = QLabel("Estado actual de CacheManager")
        self.info_label.setAlignment(Qt.AlignmentFlag.AlignLeft)
        layout.addWidget(self.info_label)

        self.table = QTableWidget()
        self.table.setColumnCount(2)
        self.table.setHorizontalHeaderLabels(["Métrica", "Valor"])
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        layout.addWidget(self.table)

        buttons = QHBoxLayout()
        self.refresh_button = QPushButton("Actualizar")
        self.refresh_button.clicked.connect(self._load_metrics)
        self.close_button = QPushButton("Cerrar")
        self.close_button.clicked.connect(self.accept)
        buttons.addStretch()
        buttons.addWidget(self.refresh_button)
        buttons.addWidget(self.close_button)
        layout.addLayout(buttons)

    def _load_metrics(self):
        try:
            stats = {}
            try:
                cm = getattr(self.db_manager, 'cache_manager', None)
                if cm is not None and hasattr(cm, 'get_stats'):
                    stats = cm.get_stats() or {}
            except Exception as e:
                logging.error(f"Error obteniendo métricas de caché: {e}")
                stats = {}

            # Métricas de timeouts de base de datos
            timeouts = {}
            try:
                if hasattr(self.db_manager, 'get_timeout_metrics'):
                    timeouts = self.db_manager.get_timeout_metrics() or {}
            except Exception:
                timeouts = {}

            items = [
                ("hits", stats.get("hits", 0)),
                ("misses", stats.get("misses", 0)),
                ("entries", stats.get("entries", 0)),
                ("capacity", stats.get("capacity", 0)),
                ("evicted", stats.get("evicted", 0)),
                ("expired", stats.get("expired", 0)),
                ("ttl_default_ms", stats.get("ttl_default_ms", 0)),
                ("statement_timeouts", timeouts.get("statement_timeouts", 0)),
                ("lock_timeouts", timeouts.get("lock_timeouts", 0)),
                ("idle_timeouts", timeouts.get("idle_timeouts", 0)),
            ]
            self.table.setRowCount(len(items))
            for i, (k, v) in enumerate(items):
                self.table.setItem(i, 0, QTableWidgetItem(str(k)))
                self.table.setItem(i, 1, QTableWidgetItem(str(v)))
        except Exception:
            # Evitar que el diálogo crashee
            self.table.setRowCount(0)