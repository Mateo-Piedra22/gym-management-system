# -*- coding: utf-8 -*-
"""
Accessibility Manager - Sistema de accesibilidad y mejoras de contraste
Maneja configuraciones de accesibilidad, alto contraste, y legibilidad
"""

import logging
from typing import Dict, Any, Optional
from PyQt6.QtCore import QObject, pyqtSignal, QSettings
from PyQt6.QtWidgets import QApplication, QWidget
from PyQt6.QtGui import QPalette, QColor, QFont

class AccessibilityManager(QObject):
    """Gestor de configuraciones de accesibilidad"""
    
    # Señales para notificar cambios
    contrast_changed = pyqtSignal(str)  # modo de contraste
    font_size_changed = pyqtSignal(int)  # tamaño de fuente
    accessibility_enabled = pyqtSignal(bool)  # estado de accesibilidad
    
    def __init__(self, main_window):
        super().__init__()
        self.main_window = main_window
        self.settings = QSettings('GymManagement', 'Accessibility')
        
        # Configuraciones de accesibilidad
        self.high_contrast_enabled = False
        self.large_fonts_enabled = False
        self.screen_reader_support = False
        self.keyboard_navigation_enhanced = True
        
        # Paletas de colores
        self.color_schemes = {
            'normal': self._create_normal_palette(),
            'high_contrast': self._create_high_contrast_palette(),
            'dark_high_contrast': self._create_dark_high_contrast_palette(),
            'low_vision': self._create_low_vision_palette()
        }
        
        # Configuraciones de fuente
        self.font_sizes = {
            'small': 9,
            'normal': 11,
            'large': 14,
            'extra_large': 18,
            'accessibility': 22
        }
        
        self.current_scheme = 'normal'
        self.current_font_size = 'normal'
        
        # Cargar configuraciones guardadas
        self.load_settings()
        
        logging.info("AccessibilityManager inicializado")
    
    def _create_normal_palette(self) -> QPalette:
        """Crea la paleta de colores normal"""
        palette = QPalette()
        
        # Colores base
        palette.setColor(QPalette.ColorRole.Window, QColor(240, 240, 240))
        palette.setColor(QPalette.ColorRole.WindowText, QColor(0, 0, 0))
        palette.setColor(QPalette.ColorRole.Base, QColor(255, 255, 255))
        palette.setColor(QPalette.ColorRole.AlternateBase, QColor(245, 245, 245))
        palette.setColor(QPalette.ColorRole.Text, QColor(0, 0, 0))
        palette.setColor(QPalette.ColorRole.Button, QColor(225, 225, 225))
        palette.setColor(QPalette.ColorRole.ButtonText, QColor(0, 0, 0))
        palette.setColor(QPalette.ColorRole.Highlight, QColor(0, 120, 215))
        palette.setColor(QPalette.ColorRole.HighlightedText, QColor(255, 255, 255))
        
        return palette
    
    def _create_high_contrast_palette(self) -> QPalette:
        """Crea la paleta de alto contraste (claro)"""
        palette = QPalette()
        
        # Alto contraste: negro sobre blanco
        palette.setColor(QPalette.ColorRole.Window, QColor(255, 255, 255))
        palette.setColor(QPalette.ColorRole.WindowText, QColor(0, 0, 0))
        palette.setColor(QPalette.ColorRole.Base, QColor(255, 255, 255))
        palette.setColor(QPalette.ColorRole.AlternateBase, QColor(240, 240, 240))
        palette.setColor(QPalette.ColorRole.Text, QColor(0, 0, 0))
        palette.setColor(QPalette.ColorRole.Button, QColor(255, 255, 255))
        palette.setColor(QPalette.ColorRole.ButtonText, QColor(0, 0, 0))
        palette.setColor(QPalette.ColorRole.Highlight, QColor(0, 0, 255))
        palette.setColor(QPalette.ColorRole.HighlightedText, QColor(255, 255, 255))
        
        # Bordes más marcados
        palette.setColor(QPalette.ColorRole.Dark, QColor(0, 0, 0))
        palette.setColor(QPalette.ColorRole.Shadow, QColor(0, 0, 0))
        
        return palette
    
    def _create_dark_high_contrast_palette(self) -> QPalette:
        """Crea la paleta de alto contraste oscuro"""
        palette = QPalette()
        
        # Alto contraste: blanco sobre negro
        palette.setColor(QPalette.ColorRole.Window, QColor(0, 0, 0))
        palette.setColor(QPalette.ColorRole.WindowText, QColor(255, 255, 255))
        palette.setColor(QPalette.ColorRole.Base, QColor(0, 0, 0))
        palette.setColor(QPalette.ColorRole.AlternateBase, QColor(32, 32, 32))
        palette.setColor(QPalette.ColorRole.Text, QColor(255, 255, 255))
        palette.setColor(QPalette.ColorRole.Button, QColor(0, 0, 0))
        palette.setColor(QPalette.ColorRole.ButtonText, QColor(255, 255, 255))
        palette.setColor(QPalette.ColorRole.Highlight, QColor(255, 255, 0))
        palette.setColor(QPalette.ColorRole.HighlightedText, QColor(0, 0, 0))
        
        # Bordes más marcados
        palette.setColor(QPalette.ColorRole.Light, QColor(255, 255, 255))
        palette.setColor(QPalette.ColorRole.Dark, QColor(255, 255, 255))
        
        return palette
    
    def _create_low_vision_palette(self) -> QPalette:
        """Crea la paleta optimizada para baja visión"""
        palette = QPalette()
        
        # Colores con alto contraste y tonos cálidos
        palette.setColor(QPalette.ColorRole.Window, QColor(255, 255, 240))  # Crema suave
        palette.setColor(QPalette.ColorRole.WindowText, QColor(0, 0, 0))
        palette.setColor(QPalette.ColorRole.Base, QColor(255, 255, 255))
        palette.setColor(QPalette.ColorRole.AlternateBase, QColor(250, 250, 235))
        palette.setColor(QPalette.ColorRole.Text, QColor(0, 0, 0))
        palette.setColor(QPalette.ColorRole.Button, QColor(240, 240, 220))
        palette.setColor(QPalette.ColorRole.ButtonText, QColor(0, 0, 0))
        palette.setColor(QPalette.ColorRole.Highlight, QColor(255, 165, 0))  # Naranja
        palette.setColor(QPalette.ColorRole.HighlightedText, QColor(0, 0, 0))
        
        return palette
    
    def set_contrast_mode(self, mode: str):
        """Establece el modo de contraste"""
        try:
            if mode not in self.color_schemes:
                logging.warning(f"Modo de contraste no válido: {mode}")
                return
            
            self.current_scheme = mode
            palette = self.color_schemes[mode]
            
            # Aplicar paleta a la aplicación
            QApplication.setPalette(palette)
            
            # Aplicar estilos adicionales si es necesario
            self._apply_contrast_styles(mode)
            
            # Guardar configuración
            self.settings.setValue('contrast_mode', mode)
            
            # Emitir señal
            self.contrast_changed.emit(mode)
            
            logging.info(f"Modo de contraste cambiado a: {mode}")
            
        except Exception as e:
            logging.error(f"Error estableciendo modo de contraste: {e}")
    
    def _apply_contrast_styles(self, mode: str):
        """Aplica estilos adicionales según el modo de contraste"""
        try:
            if mode == 'high_contrast':
                style = """
                QWidget {
                    border: 2px solid black;
                }
                QPushButton {
                    border: 3px solid black;
                    padding: 8px;
                    font-weight: bold;
                }
                QPushButton:hover {
                    background-color: #E0E0E0;
                }
                QLineEdit, QComboBox {
                    border: 2px solid black;
                    padding: 4px;
                }
                QTableWidget {
                    gridline-color: black;
                    border: 2px solid black;
                }
                QTableWidget::item {
                    border: 1px solid black;
                }
                QTabWidget::pane {
                    border: 3px solid black;
                }
                QTabBar::tab {
                    border: 2px solid black;
                    padding: 8px;
                    font-weight: bold;
                }
                """
            elif mode == 'dark_high_contrast':
                style = """
                QWidget {
                    border: 2px solid white;
                }
                QPushButton {
                    border: 3px solid white;
                    padding: 8px;
                    font-weight: bold;
                }
                QPushButton:hover {
                    background-color: #404040;
                }
                QLineEdit, QComboBox {
                    border: 2px solid white;
                    padding: 4px;
                }
                QTableWidget {
                    gridline-color: white;
                    border: 2px solid white;
                }
                QTableWidget::item {
                    border: 1px solid white;
                }
                QTabWidget::pane {
                    border: 3px solid white;
                }
                QTabBar::tab {
                    border: 2px solid white;
                    padding: 8px;
                    font-weight: bold;
                }
                """
            elif mode == 'low_vision':
                style = """
                QWidget {
                    font-size: 14px;
                }
                QPushButton {
                    border: 2px solid #8B4513;
                    padding: 10px;
                    font-weight: bold;
                    font-size: 16px;
                }
                QLineEdit, QComboBox {
                    border: 2px solid #8B4513;
                    padding: 6px;
                    font-size: 14px;
                }
                QTableWidget {
                    font-size: 14px;
                    gridline-color: #8B4513;
                }
                QTabBar::tab {
                    padding: 10px;
                    font-size: 16px;
                    font-weight: bold;
                }
                """
            else:
                style = ""  # Estilo normal
            
            if hasattr(self.main_window, 'setStyleSheet'):
                current_style = self.main_window.styleSheet()
                # Combinar con estilos existentes
                self.main_window.setStyleSheet(current_style + "\n" + style)
                
        except Exception as e:
            logging.error(f"Error aplicando estilos de contraste: {e}")
    
    def set_font_size(self, size_name: str):
        """Establece el tamaño de fuente"""
        try:
            if size_name not in self.font_sizes:
                logging.warning(f"Tamaño de fuente no válido: {size_name}")
                return
            
            self.current_font_size = size_name
            font_size = self.font_sizes[size_name]
            
            # Crear fuente base
            font = QFont()
            font.setPointSize(font_size)
            
            # Aplicar a la aplicación
            QApplication.setFont(font)
            
            # Aplicar estilos específicos para tamaños grandes
            if size_name in ['large', 'extra_large', 'accessibility']:
                self._apply_large_font_styles(font_size)
            
            # Guardar configuración
            self.settings.setValue('font_size', size_name)
            
            # Emitir señal
            self.font_size_changed.emit(font_size)
            
            logging.info(f"Tamaño de fuente cambiado a: {size_name} ({font_size}px)")
            
        except Exception as e:
            logging.error(f"Error estableciendo tamaño de fuente: {e}")
    
    def _apply_large_font_styles(self, font_size: int):
        """Aplica estilos adicionales para fuentes grandes"""
        try:
            style = f"""
            QWidget {{
                font-size: {font_size}px;
            }}
            QPushButton {{
                padding: {max(8, font_size // 2)}px;
                font-size: {font_size}px;
            }}
            QLineEdit, QComboBox {{
                padding: {max(4, font_size // 3)}px;
                font-size: {font_size}px;
            }}
            QTableWidget {{
                font-size: {font_size}px;
            }}
            QTabBar::tab {{
                padding: {max(8, font_size // 2)}px;
                font-size: {font_size}px;
            }}
            QLabel {{
                font-size: {font_size}px;
            }}
            """
            
            if hasattr(self.main_window, 'setStyleSheet'):
                current_style = self.main_window.styleSheet()
                self.main_window.setStyleSheet(current_style + "\n" + style)
                
        except Exception as e:
            logging.error(f"Error aplicando estilos de fuente grande: {e}")
    
    def toggle_high_contrast(self):
        """Alterna el modo de alto contraste"""
        if self.current_scheme == 'normal':
            self.set_contrast_mode('high_contrast')
        else:
            self.set_contrast_mode('normal')
    
    def enable_accessibility_mode(self):
        """Habilita el modo de accesibilidad completo"""
        try:
            # Aplicar configuraciones de accesibilidad
            self.set_contrast_mode('low_vision')
            self.set_font_size('accessibility')
            
            # Habilitar características adicionales
            self.large_fonts_enabled = True
            self.keyboard_navigation_enhanced = True
            
            # Configurar tooltips más largos
            self._configure_accessibility_tooltips()
            
            # Guardar estado
            self.settings.setValue('accessibility_mode', True)
            
            self.accessibility_enabled.emit(True)
            logging.info("Modo de accesibilidad habilitado")
            
        except Exception as e:
            logging.error(f"Error habilitando modo de accesibilidad: {e}")
    
    def disable_accessibility_mode(self):
        """Deshabilita el modo de accesibilidad"""
        try:
            # Restaurar configuraciones normales
            self.set_contrast_mode('normal')
            self.set_font_size('normal')
            
            # Deshabilitar características adicionales
            self.large_fonts_enabled = False
            
            # Guardar estado
            self.settings.setValue('accessibility_mode', False)
            
            self.accessibility_enabled.emit(False)
            logging.info("Modo de accesibilidad deshabilitado")
            
        except Exception as e:
            logging.error(f"Error deshabilitando modo de accesibilidad: {e}")
    
    def _configure_accessibility_tooltips(self):
        """Configura tooltips para accesibilidad"""
        try:
            # Aumentar duración de tooltips
            QApplication.instance().setAttribute(
                QApplication.ApplicationAttribute.AA_DisableWindowContextHelpButton, False
            )
            
            # Configurar tooltips más descriptivos
            # Esto se puede integrar con el TooltipManager existente
            
        except Exception as e:
            logging.error(f"Error configurando tooltips de accesibilidad: {e}")
    
    def get_contrast_ratio(self, color1: QColor, color2: QColor) -> float:
        """Calcula la relación de contraste entre dos colores"""
        try:
            # Calcular luminancia relativa
            def get_luminance(color):
                r, g, b = color.red() / 255.0, color.green() / 255.0, color.blue() / 255.0
                
                # Aplicar corrección gamma
                def gamma_correct(c):
                    return c / 12.92 if c <= 0.03928 else pow((c + 0.055) / 1.055, 2.4)
                
                r, g, b = gamma_correct(r), gamma_correct(g), gamma_correct(b)
                return 0.2126 * r + 0.7152 * g + 0.0722 * b
            
            lum1 = get_luminance(color1)
            lum2 = get_luminance(color2)
            
            # Calcular relación de contraste
            lighter = max(lum1, lum2)
            darker = min(lum1, lum2)
            
            return (lighter + 0.05) / (darker + 0.05)
            
        except Exception as e:
            logging.error(f"Error calculando relación de contraste: {e}")
            return 1.0
    
    def validate_contrast(self, foreground: QColor, background: QColor) -> Dict[str, Any]:
        """Valida si los colores cumplen con estándares de accesibilidad"""
        try:
            ratio = self.get_contrast_ratio(foreground, background)
            
            return {
                'ratio': ratio,
                'aa_normal': ratio >= 4.5,  # WCAG AA para texto normal
                'aa_large': ratio >= 3.0,   # WCAG AA para texto grande
                'aaa_normal': ratio >= 7.0, # WCAG AAA para texto normal
                'aaa_large': ratio >= 4.5,  # WCAG AAA para texto grande
                'level': 'AAA' if ratio >= 7.0 else 'AA' if ratio >= 4.5 else 'Fail'
            }
            
        except Exception as e:
            logging.error(f"Error validando contraste: {e}")
            return {'ratio': 1.0, 'level': 'Fail'}
    
    def load_settings(self):
        """Carga configuraciones guardadas"""
        try:
            # Cargar modo de contraste
            contrast_mode = self.settings.value('contrast_mode', 'normal')
            if contrast_mode in self.color_schemes:
                self.current_scheme = contrast_mode
            
            # Cargar tamaño de fuente
            font_size = self.settings.value('font_size', 'normal')
            if font_size in self.font_sizes:
                self.current_font_size = font_size
            
            # Cargar modo de accesibilidad
            accessibility_mode = self.settings.value('accessibility_mode', False, type=bool)
            if accessibility_mode:
                self.enable_accessibility_mode()
            else:
                # Aplicar configuraciones individuales
                self.set_contrast_mode(self.current_scheme)
                self.set_font_size(self.current_font_size)
            
            logging.info("Configuraciones de accesibilidad cargadas")
            
        except Exception as e:
            logging.error(f"Error cargando configuraciones de accesibilidad: {e}")
    
    def save_settings(self):
        """Guarda configuraciones actuales"""
        try:
            self.settings.setValue('contrast_mode', self.current_scheme)
            self.settings.setValue('font_size', self.current_font_size)
            self.settings.sync()
            
            logging.info("Configuraciones de accesibilidad guardadas")
            
        except Exception as e:
            logging.error(f"Error guardando configuraciones de accesibilidad: {e}")
    
    def get_accessibility_status(self) -> Dict[str, Any]:
        """Obtiene el estado actual de accesibilidad"""
        return {
            'contrast_mode': self.current_scheme,
            'font_size': self.current_font_size,
            'high_contrast_enabled': self.current_scheme != 'normal',
            'large_fonts_enabled': self.large_fonts_enabled,
            'keyboard_navigation_enhanced': self.keyboard_navigation_enhanced,
            'screen_reader_support': self.screen_reader_support
        }

# Instancia global del gestor de accesibilidad
accessibility_manager = None

def initialize_accessibility_manager(main_window):
    """Inicializa el gestor de accesibilidad global"""
    global accessibility_manager
    accessibility_manager = AccessibilityManager(main_window)
    return accessibility_manager

def get_accessibility_manager():
    """Obtiene la instancia global del gestor de accesibilidad"""
    return accessibility_manager