from PyQt6.QtWidgets import QStyledItemDelegate, QStyle, QApplication
from PyQt6.QtGui import QTextDocument, QPalette
from PyQt6.QtCore import Qt, QRect

class RichTextDelegate(QStyledItemDelegate):
    """
    Un delegado personalizado que renderiza HTML en celdas de tabla, manejando
    correctamente el color del texto tanto en estado normal como seleccionado.
    """
    def paint(self, painter, option, index):
        painter.save()

        doc = QTextDocument()

        # Obtenemos el texto del modelo
        text = index.model().data(index, Qt.ItemDataRole.DisplayRole)

        # Si la celda está seleccionada, usamos el color de texto resaltado
        if option.state & QStyle.StateFlag.State_Selected:
            text_color = option.palette.color(QPalette.ColorRole.HighlightedText).name()
        else:
            text_color = option.palette.color(QPalette.ColorRole.Text).name()

        # Aplicamos el estilo al documento completo y limitamos el ancho al rectángulo
        style_sheet = f"div {{ color: {text_color}; }}"
        doc.setDefaultStyleSheet(style_sheet)
        doc.setHtml(text)
        doc.setTextWidth(option.rect.width())

        # Pintamos el fondo de la celda (especialmente para el estado seleccionado)
        option.widget.style().drawControl(QStyle.ControlElement.CE_ItemViewItem, option, painter)

        # Recortar para evitar que el HTML se desborde a otras columnas
        painter.setClipRect(option.rect)

        # Centrado vertical del contenido
        text_rect = QRect(option.rect)
        y_offset = (option.rect.height() - int(doc.size().height())) // 2
        text_rect.setY(option.rect.y() + max(0, y_offset))

        painter.translate(text_rect.topLeft())
        doc.drawContents(painter)

        painter.restore()

    def sizeHint(self, option, index):
        # Calculamos el tamaño necesario para el contenido HTML
        doc = QTextDocument()
        doc.setHtml(index.model().data(index, Qt.ItemDataRole.DisplayRole))
        return doc.size().toSize()

