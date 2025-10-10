from PyQt6.QtWidgets import QProxyStyle, QStyleOption, QWidget, QStyle
from PyQt6.QtGui import QPainter, QPolygon, QColor, QPen
from PyQt6.QtCore import QPoint, Qt

class CustomProxyStyle(QProxyStyle):
    """
    Un estilo personalizado para asegurar que las flechas en los ComboBox y SpinBox
    se dibujen correctamente mediante código, evitando problemas de renderizado.
    """
    def drawPrimitive(self, element: QStyle.PrimitiveElement, option: QStyleOption, painter: QPainter, widget: QWidget | None = ...):
        if element in [QStyle.PrimitiveElement.PE_IndicatorSpinUp,
                       QStyle.PrimitiveElement.PE_IndicatorSpinDown,
                       QStyle.PrimitiveElement.PE_IndicatorArrowDown]:

            # Color fijo para las flechas
            arrow_color = QColor("#D8DEE9")  # Color por defecto 

            rect = option.rect
            center_x = rect.center().x()
            center_y = rect.center().y()

            painter.save()
            painter.setRenderHint(QPainter.RenderHint.Antialiasing)
            pen = QPen(arrow_color)
            pen.setWidth(1)
            painter.setPen(pen)
            painter.setBrush(arrow_color)

            if element == QStyle.PrimitiveElement.PE_IndicatorSpinUp:
                points = [QPoint(center_x, center_y - 2), QPoint(center_x - 4, center_y + 2), QPoint(center_x + 4, center_y + 2)]
                polygon = QPolygon(points)
                painter.drawPolygon(polygon)
            elif element == QStyle.PrimitiveElement.PE_IndicatorSpinDown or element == QStyle.PrimitiveElement.PE_IndicatorArrowDown:
                points = [QPoint(center_x, center_y + 2), QPoint(center_x - 4, center_y - 2), QPoint(center_x + 4, center_y - 2)]
                polygon = QPolygon(points)
                painter.drawPolygon(polygon)

            painter.restore()
            return

        super().drawPrimitive(element, option, painter, widget)

