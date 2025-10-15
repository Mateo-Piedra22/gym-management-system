import os
import shutil
from typing import Dict, List, Optional
from PyQt6.QtGui import QPixmap, QIcon
from PyQt6.QtCore import QSize

class IconManager:
    """Gestor de iconos para tipos de cuota"""
    
    def __init__(self):
        self.icons_dir = "assets/quota_icons"
        self.ensure_icons_directory()
        self.predefined_icons = self._get_predefined_icons()
    
    def ensure_icons_directory(self):
        """Asegura que el directorio de iconos existe"""
        if not os.path.exists(self.icons_dir):
            os.makedirs(self.icons_dir, exist_ok=True)
    
    def _get_predefined_icons(self) -> Dict[str, str]:
        """Retorna la biblioteca de iconos predefinidos"""
        return {
            # Deportes y Fitness
            "🏃": "fitness",
            "💪": "muscle",
            "🏋️": "weightlifting",
            "🤸": "gymnastics",
            "🧘": "yoga",
            "🏊": "swimming",
            "🚴": "cycling",
            "🥊": "boxing",
            "🏀": "basketball",
            "⚽": "soccer",
            "🎾": "tennis",
            "🏐": "volleyball",
            "🏓": "pingpong",
            "🥋": "martial_arts",
            "🤾": "handball",
            "🏸": "badminton",
            
            # Premios y Logros
            "🏆": "trophy",
            "🥇": "gold",
            "🥈": "silver",
            "🥉": "bronze",
            "🎖️": "medal",
            "👑": "crown",
            "💎": "diamond",
            "⭐": "premium",
            "🌟": "star",
            "✨": "sparkles",
            "🎯": "target",
            "🔥": "fire",
            "⚡": "lightning",
            "💫": "dizzy",
            
            # Dinero y Precios
            "💰": "money",
            "💵": "dollar",
            "💳": "credit_card",
            "💸": "money_wings",
            "🪙": "coin",
            "💲": "dollar_sign",
            "🏦": "bank",
            "📊": "chart",
            "📈": "trending_up",
            "📉": "trending_down",
            
            # Educación y Estudiantes
            "🎓": "student",
            "📚": "books",
            "📖": "book",
            "✏️": "pencil",
            "📝": "memo",
            "🎒": "backpack",
            "📐": "ruler",
            "🔬": "microscope",
            "🧮": "abacus",
            "📊": "chart_bars",
            
            # Profesiones y Roles
            "👨‍💼": "businessman",
            "👩‍💼": "businesswoman",
            "👨‍⚕️": "doctor_man",
            "👩‍⚕️": "doctor_woman",
            "👨‍🏫": "teacher_man",
            "👩‍🏫": "teacher_woman",
            "👨‍💻": "programmer_man",
            "👩‍💻": "programmer_woman",
            "👮": "police",
            "🧑‍🚒": "firefighter",
            
            # Familia y Grupos
            "👨‍👩‍👧‍👦": "family",
            "👥": "group",
            "👫": "couple",
            "👶": "baby",
            "🧒": "child",
            "👦": "boy",
            "👧": "girl",
            "👨": "man",
            "👩": "woman",
            "👴": "old_man",
            "👵": "old_woman",
            "🧓": "senior",
            
            # Tiempo y Horarios
            "⏰": "alarm_clock",
            "⏱️": "stopwatch",
            "⏳": "hourglass",
            "🕐": "clock_1",
            "🕕": "clock_6",
            "🕘": "clock_9",
            "📅": "calendar",
            "📆": "calendar_spiral",
            "🗓️": "calendar_pad",
            "⌚": "watch",
            
            # Entretenimiento
            "🎵": "music",
            "🎶": "musical_notes",
            "🎤": "microphone",
            "🎧": "headphones",
            "🎮": "gaming",
            "🎲": "dice",
            "🎪": "circus",
            "🎨": "art",
            "🎭": "theater",
            "🎬": "cinema",
            "📺": "tv",
            "📱": "phone",
            
            # Salud y Bienestar
            "❤️": "heart",
            "💚": "green_heart",
            "💙": "blue_heart",
            "💜": "purple_heart",
            "🧠": "brain",
            "🫀": "anatomical_heart",
            "🩺": "stethoscope",
            "💊": "pill",
            "🏥": "hospital",
            "🚑": "ambulance",
            "🧘‍♀️": "woman_lotus",
            "🧘‍♂️": "man_lotus",
            
            # Naturaleza y Elementos
            "🌱": "seedling",
            "🌿": "herb",
            "🍃": "leaves",
            "🌳": "tree",
            "🌲": "evergreen",
            "🌺": "hibiscus",
            "🌸": "cherry_blossom",
            "🌼": "daisy",
            "🌻": "sunflower",
            "🌹": "rose",
            "☀️": "sun",
            "🌙": "moon",
            "⭐": "star_outline",
            "🌈": "rainbow",
            
            # Tecnología
            "💻": "laptop",
            "🖥️": "desktop",
            "⌨️": "keyboard",
            "🖱️": "mouse",
            "📱": "mobile",
            "💾": "floppy_disk",
            "💿": "cd",
            "📀": "dvd",
            "🔌": "plug",
            "🔋": "battery",
            "📡": "satellite",
            "🛰️": "satellite_orbital",
            
            # Transporte
            "🚗": "car",
            "🚕": "taxi",
            "🚙": "suv",
            "🚌": "bus",
            "🚎": "trolleybus",
            "🏎️": "race_car",
            "🚓": "police_car",
            "🚑": "ambulance_transport",
            "🚒": "fire_truck",
            "🚐": "minibus",
            "🛻": "pickup_truck",
            "🚚": "delivery_truck",
            
            # Comida y Bebida
            "🍎": "apple",
            "🍌": "banana",
            "🍊": "orange",
            "🍇": "grapes",
            "🥗": "salad",
            "🥑": "avocado",
            "🥕": "carrot",
            "🍞": "bread",
            "🥛": "milk",
            "☕": "coffee",
            "🧃": "juice_box",
            "💧": "water_drop"
        }
    
    def get_predefined_icons_list(self) -> List[str]:
        """Retorna lista de iconos predefinidos disponibles"""
        return list(self.predefined_icons.keys())
    
    def get_icon_name(self, icon_identifier: str) -> str:
        """Obtiene el nombre descriptivo de un icono"""
        if icon_identifier in self.predefined_icons:
            return self.predefined_icons[icon_identifier]
        return "custom_icon"
    
    def search_icons_by_name(self, search_term: str) -> List[str]:
        """Busca íconos por nombre descriptivo"""
        if not search_term:
            return self.get_predefined_icons_list()
        
        search_term = search_term.lower()
        matching_icons = []
        
        for emoji, name in self.predefined_icons.items():
            if search_term in name.lower() or search_term in emoji:
                matching_icons.append(emoji)
        
        return matching_icons
    
    def get_icon_path(self, icon_identifier: str) -> Optional[str]:
        """Obtiene la ruta del icono basado en el identificador"""
        # Si es un emoji (icono predefinido)
        if icon_identifier in self.predefined_icons:
            return icon_identifier  # Retorna el emoji directamente
        
        # Si es una ruta de archivo personalizado
        if icon_identifier.startswith(self.icons_dir):
            if os.path.exists(icon_identifier):
                return icon_identifier
        
        # Buscar en el directorio de iconos
        full_path = os.path.join(self.icons_dir, icon_identifier)
        if os.path.exists(full_path):
            return full_path
        
        return None
    
    def create_qicon(self, icon_identifier: str, size: QSize = QSize(32, 32)) -> QIcon:
        """Crea un QIcon desde un identificador de icono"""
        # Si es un emoji, crear un icono de texto
        if icon_identifier in self.predefined_icons:
            return self._create_emoji_icon(icon_identifier, size)
        
        # Si es una ruta de archivo
        icon_path = self.get_icon_path(icon_identifier)
        if icon_path and os.path.exists(icon_path):
            pixmap = QPixmap(icon_path)
            if not pixmap.isNull():
                return QIcon(pixmap.scaled(size))
        
        # Icono por defecto
        return self._create_default_icon(size)
    
    def _create_emoji_icon(self, emoji: str, size: QSize) -> QIcon:
        """Crea un QIcon desde un emoji"""
        from PyQt6.QtGui import QPainter, QFont, QColor
        from PyQt6.QtCore import Qt
        
        pixmap = QPixmap(size)
        pixmap.fill(QColor(0, 0, 0, 0))  # Transparente
        
        painter = QPainter(pixmap)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        
        font = QFont()
        font.setPixelSize(int(size.width() * 0.8))
        painter.setFont(font)
        
        painter.setPen(QColor(0, 0, 0))
        painter.drawText(pixmap.rect(), Qt.AlignmentFlag.AlignCenter, emoji)
        painter.end()
        
        return QIcon(pixmap)
    
    def _create_default_icon(self, size: QSize) -> QIcon:
        """Crea un icono por defecto"""
        return self._create_emoji_icon("💰", size)
    
    def save_custom_icon(self, source_path: str, icon_name: str) -> Optional[str]:
        """Guarda un icono personalizado en el directorio de iconos"""
        try:
            if not os.path.exists(source_path):
                return None
            
            # Obtener extensión del archivo
            _, ext = os.path.splitext(source_path)
            if ext.lower() not in ['.png', '.jpg', '.jpeg', '.gif', '.bmp', '.svg']:
                return None
            
            # Crear nombre único si es necesario
            base_name = icon_name if icon_name.endswith(ext) else f"{icon_name}{ext}"
            destination_path = os.path.join(self.icons_dir, base_name)
            
            counter = 1
            while os.path.exists(destination_path):
                name_without_ext = icon_name.replace(ext, '')
                base_name = f"{name_without_ext}_{counter}{ext}"
                destination_path = os.path.join(self.icons_dir, base_name)
                counter += 1
            
            # Copiar archivo
            shutil.copy2(source_path, destination_path)
            return destination_path
            
        except Exception as e:
            print(f"Error al guardar icono personalizado: {e}")
            return None
    
    def get_custom_icons(self) -> List[str]:
        """Obtiene lista de iconos personalizados disponibles"""
        custom_icons = []
        if os.path.exists(self.icons_dir):
            for file in os.listdir(self.icons_dir):
                if file.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.bmp', '.svg')):
                    custom_icons.append(os.path.join(self.icons_dir, file))
        return custom_icons
    
    def delete_custom_icon(self, icon_path: str) -> bool:
        """Elimina un icono personalizado"""
        try:
            if os.path.exists(icon_path) and icon_path.startswith(self.icons_dir):
                os.remove(icon_path)
                return True
            return False
        except Exception as e:
            print(f"Error al eliminar icono: {e}")
            return False
    
    def validate_icon(self, icon_path: str) -> bool:
        """Valida si un archivo es un icono válido"""
        try:
            if not os.path.exists(icon_path):
                return False
            
            # Verificar extensión
            _, ext = os.path.splitext(icon_path)
            if ext.lower() not in ['.png', '.jpg', '.jpeg', '.gif', '.bmp', '.svg']:
                return False
            
            # Verificar que se puede cargar como imagen
            pixmap = QPixmap(icon_path)
            return not pixmap.isNull()
            
        except Exception:
            return False