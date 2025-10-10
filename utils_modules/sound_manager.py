# -*- coding: utf-8 -*-
"""
Sound Manager - Sistema de indicadores sonoros para acciones importantes
Proporciona retroalimentación auditiva para mejorar la accesibilidad
"""

import logging
import os
from typing import Dict, Optional
from PyQt6.QtCore import QObject, pyqtSignal, QSettings, QUrl, QTimer
from PyQt6.QtMultimedia import QSoundEffect, QMediaPlayer, QAudioOutput
from PyQt6.QtWidgets import QApplication

class SoundManager(QObject):
    """Gestor de sonidos e indicadores auditivos"""
    
    # Señales para notificar eventos de sonido
    sound_played = pyqtSignal(str)  # tipo de sonido reproducido
    sound_error = pyqtSignal(str)   # error en reproducción
    
    def __init__(self):
        super().__init__()
        self.settings = QSettings('GymManagement', 'Sound')
        
        # Estado del gestor de sonidos
        self.enabled = True
        self.volume = 0.7
        self.sound_effects_enabled = True
        self.notification_sounds_enabled = True
        
        # Efectos de sonido
        self.sound_effects = {}
        self.media_player = None
        self.audio_output = None
        
        # Definir sonidos del sistema
        self.system_sounds = {
            # Acciones exitosas
            'success': {
                'frequency': 800,
                'duration': 200,
                'description': 'Acción completada exitosamente'
            },
            'save': {
                'frequency': 600,
                'duration': 150,
                'description': 'Datos guardados'
            },
            'delete': {
                'frequency': 400,
                'duration': 300,
                'description': 'Elemento eliminado'
            },
            
            # Errores y advertencias
            'error': {
                'frequency': 200,
                'duration': 500,
                'description': 'Error en la operación'
            },
            'warning': {
                'frequency': 300,
                'duration': 300,
                'description': 'Advertencia'
            },
            'validation_error': {
                'frequency': 250,
                'duration': 200,
                'description': 'Error de validación'
            },
            
            # Navegación
            'navigation': {
                'frequency': 500,
                'duration': 100,
                'description': 'Cambio de navegación'
            },
            'focus_change': {
                'frequency': 450,
                'duration': 80,
                'description': 'Cambio de foco'
            },
            'tab_change': {
                'frequency': 550,
                'duration': 120,
                'description': 'Cambio de pestaña'
            },
            
            # Notificaciones
            'notification': {
                'frequency': 700,
                'duration': 250,
                'description': 'Notificación general'
            },
            'payment_processed': {
                'frequency': 900,
                'duration': 300,
                'description': 'Pago procesado'
            },
            'user_registered': {
                'frequency': 750,
                'duration': 200,
                'description': 'Usuario registrado'
            },
            
            # Sistema
            'startup': {
                'frequency': 600,
                'duration': 400,
                'description': 'Sistema iniciado'
            },
            'shutdown': {
                'frequency': 400,
                'duration': 600,
                'description': 'Sistema cerrando'
            },
            'backup_complete': {
                'frequency': 800,
                'duration': 350,
                'description': 'Respaldo completado'
            }
        }
        
        # Inicializar sistema de audio
        self.initialize_audio_system()
        
        # Cargar configuraciones
        self.load_settings()
        
        logging.info("SoundManager inicializado")
    
    def initialize_audio_system(self):
        """Inicializa el sistema de audio"""
        try:
            # Crear reproductor de medios
            self.media_player = QMediaPlayer()
            self.audio_output = QAudioOutput()
            self.media_player.setAudioOutput(self.audio_output)
            
            # Configurar volumen inicial
            self.audio_output.setVolume(self.volume)
            
            # Crear efectos de sonido para cada tipo
            for sound_type in self.system_sounds.keys():
                effect = QSoundEffect()
                effect.setVolume(self.volume)
                self.sound_effects[sound_type] = effect
            
            logging.info("Sistema de audio inicializado correctamente")
            
        except Exception as e:
            logging.error(f"Error inicializando sistema de audio: {e}")
            self.enabled = False
    
    def generate_tone(self, frequency: int, duration: int, volume: float = None) -> bytes:
        """Genera un tono sintético"""
        try:
            import math
            import struct
            
            if volume is None:
                volume = self.volume
            
            sample_rate = 44100
            samples = int(sample_rate * duration / 1000)
            
            # Generar onda senoidal
            audio_data = []
            for i in range(samples):
                t = i / sample_rate
                # Aplicar envolvente para evitar clics
                envelope = 1.0
                if i < samples * 0.1:  # Fade in
                    envelope = i / (samples * 0.1)
                elif i > samples * 0.9:  # Fade out
                    envelope = (samples - i) / (samples * 0.1)
                
                sample = math.sin(2 * math.pi * frequency * t) * volume * envelope
                audio_data.append(struct.pack('<h', int(sample * 32767)))
            
            return b''.join(audio_data)
            
        except Exception as e:
            logging.error(f"Error generando tono: {e}")
            return b''
    
    def play_system_sound(self, sound_type: str):
        """Reproduce un sonido del sistema"""
        try:
            if not self.enabled or not self.sound_effects_enabled:
                return
            
            if sound_type not in self.system_sounds:
                logging.warning(f"Tipo de sonido no encontrado: {sound_type}")
                return
            
            sound_config = self.system_sounds[sound_type]
            
            # Usar sonido del sistema de Windows si está disponible
            if self._try_system_sound(sound_type):
                self.sound_played.emit(sound_type)
                return
            
            # Generar tono sintético como respaldo
            self._play_synthetic_tone(
                sound_config['frequency'],
                sound_config['duration']
            )
            
            self.sound_played.emit(sound_type)
            logging.debug(f"Sonido reproducido: {sound_type}")
            
        except Exception as e:
            logging.error(f"Error reproduciendo sonido {sound_type}: {e}")
            self.sound_error.emit(str(e))
    
    def _try_system_sound(self, sound_type: str) -> bool:
        """Intenta reproducir sonido del sistema operativo"""
        try:
            import winsound
            
            # Mapear tipos de sonido a sonidos de Windows
            windows_sounds = {
                'success': winsound.MB_OK,
                'error': winsound.MB_ICONHAND,
                'warning': winsound.MB_ICONEXCLAMATION,
                'notification': winsound.MB_ICONASTERISK,
                'startup': winsound.MB_OK,
                'shutdown': winsound.MB_OK
            }
            
            if sound_type in windows_sounds:
                winsound.MessageBeep(windows_sounds[sound_type])
                return True
                
        except ImportError:
            # winsound no disponible (no Windows)
            pass
        except Exception as e:
            logging.debug(f"Error con sonido del sistema: {e}")
        
        return False
    
    def _play_synthetic_tone(self, frequency: int, duration: int):
        """Reproduce un tono sintético"""
        try:
            # Crear archivo temporal de audio
            import tempfile
            import wave
            
            audio_data = self.generate_tone(frequency, duration)
            
            if not audio_data:
                return
            
            # Crear archivo WAV temporal
            with tempfile.NamedTemporaryFile(suffix='.wav', delete=False) as temp_file:
                with wave.open(temp_file.name, 'wb') as wav_file:
                    wav_file.setnchannels(1)  # Mono
                    wav_file.setsampwidth(2)  # 16 bits
                    wav_file.setframerate(44100)  # 44.1 kHz
                    wav_file.writeframes(audio_data)
                
                # Reproducir usando QMediaPlayer
                if self.media_player:
                    self.media_player.setSource(QUrl.fromLocalFile(temp_file.name))
                    self.media_player.play()
                    
                    # Programar eliminación del archivo temporal
                    QTimer.singleShot(duration + 1000, lambda: self._cleanup_temp_file(temp_file.name))
                    
        except Exception as e:
            logging.error(f"Error reproduciendo tono sintético: {e}")
    
    def _cleanup_temp_file(self, file_path: str):
        """Limpia archivos temporales"""
        try:
            if os.path.exists(file_path):
                os.unlink(file_path)
        except Exception as e:
            logging.debug(f"Error limpiando archivo temporal: {e}")
    
    def play_notification(self, message: str, sound_type: str = 'notification'):
        """Reproduce notificación con sonido"""
        try:
            if not self.notification_sounds_enabled:
                return
            
            # Reproducir sonido
            self.play_system_sound(sound_type)
            
            # Mostrar notificación visual si es necesario
            self._show_visual_notification(message)
            
        except Exception as e:
            logging.error(f"Error reproduciendo notificación: {e}")
    
    def _show_visual_notification(self, message: str):
        """Muestra notificación visual para usuarios con discapacidad auditiva"""
        try:
            # Implementar notificación visual (tooltip, flash, etc.)
            app = QApplication.instance()
            if app:
                # Flash de la ventana principal
                main_window = app.activeWindow()
                if main_window:
                    # Implementar flash visual
                    pass
                    
        except Exception as e:
            logging.debug(f"Error mostrando notificación visual: {e}")
    
    def set_volume(self, volume: float):
        """Establece el volumen (0.0 a 1.0)"""
        try:
            self.volume = max(0.0, min(1.0, volume))
            
            # Actualizar volumen en todos los efectos
            if self.audio_output:
                self.audio_output.setVolume(self.volume)
            
            for effect in self.sound_effects.values():
                effect.setVolume(self.volume)
            
            # Guardar configuración
            self.settings.setValue('volume', self.volume)
            
            logging.info(f"Volumen establecido a: {self.volume}")
            
        except Exception as e:
            logging.error(f"Error estableciendo volumen: {e}")
    
    def enable_sounds(self):
        """Habilita todos los sonidos"""
        self.enabled = True
        self.sound_effects_enabled = True
        self.notification_sounds_enabled = True
        self.settings.setValue('enabled', True)
        logging.info("Sonidos habilitados")
    
    def disable_sounds(self):
        """Deshabilita todos los sonidos"""
        self.enabled = False
        self.sound_effects_enabled = False
        self.notification_sounds_enabled = False
        self.settings.setValue('enabled', False)
        logging.info("Sonidos deshabilitados")
    
    def enable_sound_effects(self, enabled: bool = True):
        """Habilita/deshabilita efectos de sonido"""
        self.sound_effects_enabled = enabled
        self.settings.setValue('sound_effects_enabled', enabled)
        logging.info(f"Efectos de sonido {'habilitados' if enabled else 'deshabilitados'}")
    
    def enable_notifications(self, enabled: bool = True):
        """Habilita/deshabilita sonidos de notificación"""
        self.notification_sounds_enabled = enabled
        self.settings.setValue('notification_sounds_enabled', enabled)
        logging.info(f"Sonidos de notificación {'habilitados' if enabled else 'deshabilitados'}")
    
    def test_sound(self, sound_type: str = 'notification'):
        """Reproduce un sonido de prueba"""
        self.play_system_sound(sound_type)
    
    def load_settings(self):
        """Carga configuraciones guardadas"""
        try:
            self.enabled = self.settings.value('enabled', True, type=bool)
            self.volume = self.settings.value('volume', 0.7, type=float)
            self.sound_effects_enabled = self.settings.value('sound_effects_enabled', True, type=bool)
            self.notification_sounds_enabled = self.settings.value('notification_sounds_enabled', True, type=bool)
            
            # Aplicar volumen
            self.set_volume(self.volume)
            
            logging.info("Configuraciones de sonido cargadas")
            
        except Exception as e:
            logging.error(f"Error cargando configuraciones de sonido: {e}")
    
    def save_settings(self):
        """Guarda configuraciones actuales"""
        try:
            self.settings.setValue('enabled', self.enabled)
            self.settings.setValue('volume', self.volume)
            self.settings.setValue('sound_effects_enabled', self.sound_effects_enabled)
            self.settings.setValue('notification_sounds_enabled', self.notification_sounds_enabled)
            self.settings.sync()
            
            logging.info("Configuraciones de sonido guardadas")
            
        except Exception as e:
            logging.error(f"Error guardando configuraciones de sonido: {e}")
    
    def get_sound_status(self) -> Dict[str, any]:
        """Obtiene el estado actual del sistema de sonidos"""
        return {
            'enabled': self.enabled,
            'volume': self.volume,
            'sound_effects_enabled': self.sound_effects_enabled,
            'notification_sounds_enabled': self.notification_sounds_enabled,
            'available_sounds': list(self.system_sounds.keys())
        }
    
    def get_sound_descriptions(self) -> Dict[str, str]:
        """Obtiene descripciones de todos los sonidos disponibles"""
        return {sound_type: config['description'] 
                for sound_type, config in self.system_sounds.items()}

# Instancia global del gestor de sonidos
sound_manager = None

def initialize_sound_manager():
    """Inicializa el gestor de sonidos global"""
    global sound_manager
    sound_manager = SoundManager()
    return sound_manager

def get_sound_manager():
    """Obtiene la instancia global del gestor de sonidos"""
    return sound_manager

def play_sound(sound_type: str):
    """Función de conveniencia para reproducir sonidos"""
    if sound_manager:
        sound_manager.play_system_sound(sound_type)

def play_notification_sound(message: str, sound_type: str = 'notification'):
    """Función de conveniencia para reproducir notificaciones"""
    if sound_manager:
        sound_manager.play_notification(message, sound_type)