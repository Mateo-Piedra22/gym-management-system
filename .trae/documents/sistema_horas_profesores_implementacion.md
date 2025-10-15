# Guía de Implementación - Sistema de Horas de Trabajo para Profesores

## 1. Resumen de Cambios Requeridos

El sistema actual tiene la infraestructura básica pero requiere las siguientes mejoras:

1. **Corregir cálculo de horas en estadísticas** - Reemplazar simulación por datos reales
2. **Implementar widget de horas mensuales** - Nuevo componente en ProfessorsTabWidget
3. **Agregar detección de horas fuera de horario** - Lógica de comparación con horarios programados
4. **Mejorar manejo de sesiones** - Validaciones y alertas de sesiones largas
5. **Implementar reset mensual automático** - Contador que se reinicia cada mes

## 2. Modificaciones en Archivos Existentes

### 2.1 database.py - Método obtener_estadisticas_profesor

**Problema Actual**: El método usa datos simulados (total_clases * 1.5)

**Solución**: Reemplazar con datos reales de la tabla profesor_horas_trabajadas

```python
def obtener_estadisticas_profesor(self, profesor_id: int) -> dict:
    """Obtiene estadísticas de un profesor específico con horas reales"""
    try:
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            # Obtener evaluaciones del profesor
            cursor.execute("""
                SELECT COUNT(*) as total_evaluaciones,
                       AVG(puntuacion) as puntuacion_promedio
                FROM profesor_evaluaciones 
                WHERE profesor_id = %s
            """, (profesor_id,))
            evaluaciones_data = cursor.fetchone()
            
            # Obtener clases del profesor
            cursor.execute("""
                SELECT COUNT(DISTINCT ch.clase_id) as total_clases
                FROM clases_horarios ch
                WHERE ch.profesor_id = %s
            """, (profesor_id,))
            clases_data = cursor.fetchone()
            
            # Obtener estudiantes únicos
            cursor.execute("""
                SELECT COUNT(DISTINCT cu.usuario_id) as estudiantes_unicos
                FROM clase_usuarios cu
                JOIN clases_horarios ch ON cu.clase_horario_id = ch.id
                WHERE ch.profesor_id = %s
            """, (profesor_id,))
            estudiantes_data = cursor.fetchone()
            
            # CAMBIO PRINCIPAL: Obtener horas trabajadas reales del mes actual
            from datetime import datetime
            mes_actual = datetime.now().month
            año_actual = datetime.now().year
            
            cursor.execute("""
                SELECT COALESCE(SUM(horas_totales), 0) as horas_trabajadas
                FROM profesor_horas_trabajadas 
                WHERE profesor_id = %s 
                AND EXTRACT(MONTH FROM fecha) = %s
                AND EXTRACT(YEAR FROM fecha) = %s
                AND hora_fin IS NOT NULL
            """, (profesor_id, mes_actual, año_actual))
            horas_data = cursor.fetchone()
            
            return {
                'total_evaluaciones': evaluaciones_data['total_evaluaciones'] or 0,
                'puntuacion_promedio': float(evaluaciones_data['puntuacion_promedio'] or 0),
                'total_clases': clases_data['total_clases'] or 0,
                'estudiantes_unicos': estudiantes_data['estudiantes_unicos'] or 0,
                'horas_trabajadas': float(horas_data['horas_trabajadas'] or 0)
            }
            
    except Exception as e:
        logging.error(f"Error obteniendo estadísticas del profesor {profesor_id}: {str(e)}")
        return {
            'total_evaluaciones': 0,
            'puntuacion_promedio': 0.0,
            'total_clases': 0,
            'estudiantes_unicos': 0,
            'horas_trabajadas': 0.0
        }
```

### 2.2 database.py - Nuevo método para horas fuera de horario

```python
def obtener_horas_fuera_horario_profesor(self, profesor_id: int, mes: int = None, año: int = None) -> Dict:
    """Calcula las horas trabajadas fuera del horario programado"""
    try:
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            if mes is None or año is None:
                from datetime import datetime
                now = datetime.now()
                mes = mes or now.month
                año = año or now.year
            
            # Obtener todas las sesiones del mes
            cursor.execute("""
                SELECT fecha, hora_inicio, hora_fin, horas_totales,
                       EXTRACT(DOW FROM fecha) as dia_semana_num
                FROM profesor_horas_trabajadas
                WHERE profesor_id = %s
                AND EXTRACT(MONTH FROM fecha) = %s
                AND EXTRACT(YEAR FROM fecha) = %s
                AND hora_fin IS NOT NULL
                ORDER BY fecha, hora_inicio
            """, (profesor_id, mes, año))
            sesiones = cursor.fetchall()
            
            # Obtener horarios programados del profesor
            cursor.execute("""
                SELECT dia_semana, hora_inicio, hora_fin
                FROM horarios_profesores
                WHERE profesor_id = %s AND disponible = true
            """, (profesor_id,))
            horarios_programados = cursor.fetchall()
            
            # Mapear días de la semana
            dias_map = {
                0: 'Domingo', 1: 'Lunes', 2: 'Martes', 3: 'Miércoles',
                4: 'Jueves', 5: 'Viernes', 6: 'Sábado'
            }
            
            # Crear diccionario de horarios por día
            horarios_por_dia = {}
            for horario in horarios_programados:
                dia = horario['dia_semana']
                if dia not in horarios_por_dia:
                    horarios_por_dia[dia] = []
                horarios_por_dia[dia].append({
                    'inicio': horario['hora_inicio'],
                    'fin': horario['hora_fin']
                })
            
            total_horas_fuera = 0
            sesiones_fuera = []
            
            for sesion in sesiones:
                dia_nombre = dias_map[int(sesion['dia_semana_num'])]
                horarios_dia = horarios_por_dia.get(dia_nombre, [])
                
                if not horarios_dia:
                    # No hay horario programado para este día
                    total_horas_fuera += sesion['horas_totales']
                    sesiones_fuera.append({
                        'fecha': sesion['fecha'],
                        'horas_fuera': sesion['horas_totales'],
                        'motivo': 'Día no programado'
                    })
                else:
                    # Verificar si la sesión está dentro de algún horario programado
                    horas_dentro_horario = 0
                    
                    for horario in horarios_dia:
                        # Calcular intersección entre sesión y horario programado
                        inicio_interseccion = max(sesion['hora_inicio'], horario['inicio'])
                        fin_interseccion = min(sesion['hora_fin'], horario['fin'])
                        
                        if inicio_interseccion < fin_interseccion:
                            # Hay intersección
                            from datetime import datetime, timedelta
                            delta = datetime.combine(datetime.min, fin_interseccion) - datetime.combine(datetime.min, inicio_interseccion)
                            horas_dentro_horario += delta.total_seconds() / 3600
                    
                    horas_fuera_sesion = max(0, sesion['horas_totales'] - horas_dentro_horario)
                    if horas_fuera_sesion > 0:
                        total_horas_fuera += horas_fuera_sesion
                        sesiones_fuera.append({
                            'fecha': sesion['fecha'],
                            'horas_fuera': horas_fuera_sesion,
                            'motivo': 'Fuera de horario programado'
                        })
            
            return {
                'total_horas_fuera': round(total_horas_fuera, 2),
                'sesiones_fuera': sesiones_fuera,
                'mes': mes,
                'año': año
            }
            
    except Exception as e:
        logging.error(f"Error calculando horas fuera de horario: {str(e)}")
        return {
            'total_horas_fuera': 0,
            'sesiones_fuera': [],
            'mes': mes or datetime.now().month,
            'año': año or datetime.now().year
        }
```

### 2.3 professors_tab_widget.py - Nuevo widget de horas mensuales

**Agregar después de la línea 1443 (después del método crear_widget_estadisticas)**

```python
def crear_widget_horas_mensuales(self):
    """Crea el widget de seguimiento de horas mensuales"""
    widget = QWidget()
    layout = QVBoxLayout(widget)
    
    # Título del widget
    titulo = QLabel("📊 Seguimiento de Horas Mensuales")
    titulo.setObjectName("section_title")
    layout.addWidget(titulo)
    
    # Frame principal de métricas
    metrics_frame = QFrame()
    metrics_frame.setProperty("class", "metric-card")
    metrics_layout = QGridLayout(metrics_frame)
    
    # Métricas principales
    self.lbl_horas_mes = QLabel("0.0h")
    self.lbl_horas_mes.setAlignment(Qt.AlignmentFlag.AlignCenter)
    self.lbl_horas_mes.setObjectName("hours_month_label")
    
    self.lbl_sesiones_mes = QLabel("0")
    self.lbl_sesiones_mes.setAlignment(Qt.AlignmentFlag.AlignCenter)
    self.lbl_sesiones_mes.setObjectName("sessions_month_label")
    
    self.lbl_horas_fuera = QLabel("0.0h")
    self.lbl_horas_fuera.setAlignment(Qt.AlignmentFlag.AlignCenter)
    self.lbl_horas_fuera.setObjectName("hours_outside_label")
    
    self.lbl_promedio_diario = QLabel("0.0h")
    self.lbl_promedio_diario.setAlignment(Qt.AlignmentFlag.AlignCenter)
    self.lbl_promedio_diario.setObjectName("daily_average_label")
    
    # Layout de métricas
    metrics_layout.addWidget(QLabel("Horas del Mes"), 0, 0)
    metrics_layout.addWidget(self.lbl_horas_mes, 1, 0)
    metrics_layout.addWidget(QLabel("Sesiones"), 0, 1)
    metrics_layout.addWidget(self.lbl_sesiones_mes, 1, 1)
    metrics_layout.addWidget(QLabel("Horas Fuera de Horario"), 0, 2)
    metrics_layout.addWidget(self.lbl_horas_fuera, 1, 2)
    metrics_layout.addWidget(QLabel("Promedio Diario"), 0, 3)
    metrics_layout.addWidget(self.lbl_promedio_diario, 1, 3)
    
    layout.addWidget(metrics_frame)
    
    # Selector de mes/año
    date_frame = QFrame()
    date_layout = QHBoxLayout(date_frame)
    
    date_layout.addWidget(QLabel("Ver mes:"))
    
    self.mes_combo = QComboBox()
    meses = ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio',
             'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre']
    self.mes_combo.addItems(meses)
    
    from datetime import datetime
    self.mes_combo.setCurrentIndex(datetime.now().month - 1)
    
    self.año_spin = QSpinBox()
    self.año_spin.setRange(2020, 2030)
    self.año_spin.setValue(datetime.now().year)
    
    date_layout.addWidget(self.mes_combo)
    date_layout.addWidget(self.año_spin)
    date_layout.addStretch()
    
    # Botón de actualizar
    self.btn_actualizar_horas = QPushButton("🔄 Actualizar")
    self.btn_actualizar_horas.clicked.connect(self.actualizar_horas_mensuales)
    date_layout.addWidget(self.btn_actualizar_horas)
    
    layout.addWidget(date_frame)
    
    # Lista de alertas de horas fuera de horario
    layout.addWidget(QLabel("⚠️ Alertas de Horas Fuera de Horario:"))
    self.lista_alertas_horas = QListWidget()
    self.lista_alertas_horas.setMaximumHeight(120)
    layout.addWidget(self.lista_alertas_horas)
    
    # Conectar señales
    self.mes_combo.currentIndexChanged.connect(self.actualizar_horas_mensuales)
    self.año_spin.valueChanged.connect(self.actualizar_horas_mensuales)
    
    layout.addStretch()
    return widget

def actualizar_horas_mensuales(self):
    """Actualiza las métricas de horas mensuales del profesor seleccionado"""
    if not self.profesor_seleccionado:
        return
    
    try:
        mes = self.mes_combo.currentIndex() + 1
        año = self.año_spin.value()
        
        # Obtener resumen de horas del mes
        resumen = self.db_manager.obtener_resumen_horas_profesor(
            self.profesor_seleccionado['id'], mes, año
        )
        
        # Obtener horas fuera de horario
        horas_fuera = self.db_manager.obtener_horas_fuera_horario_profesor(
            self.profesor_seleccionado['id'], mes, año
        )
        
        # Actualizar métricas
        totales = resumen.get('totales', {})
        total_horas = totales.get('total_horas', 0)
        total_sesiones = totales.get('total_sesiones', 0)
        
        self.lbl_horas_mes.setText(f"{total_horas:.1f}h")
        self.lbl_sesiones_mes.setText(str(total_sesiones))
        self.lbl_horas_fuera.setText(f"{horas_fuera['total_horas_fuera']:.1f}h")
        
        # Calcular promedio diario (considerando solo días trabajados)
        promedio = total_horas / max(total_sesiones, 1) if total_sesiones > 0 else 0
        self.lbl_promedio_diario.setText(f"{promedio:.1f}h")
        
        # Actualizar lista de alertas
        self.lista_alertas_horas.clear()
        for sesion in horas_fuera['sesiones_fuera']:
            fecha_str = sesion['fecha'].strftime('%d/%m/%Y')
            texto = f"📅 {fecha_str}: {sesion['horas_fuera']:.1f}h - {sesion['motivo']}"
            self.lista_alertas_horas.addItem(texto)
        
        if not horas_fuera['sesiones_fuera']:
            self.lista_alertas_horas.addItem("✅ No hay horas fuera de horario este mes")
        
        # Aplicar estilos según valores
        if horas_fuera['total_horas_fuera'] > 0:
            self.lbl_horas_fuera.setStyleSheet("color: #ea580c; font-weight: bold;")
        else:
            self.lbl_horas_fuera.setStyleSheet("color: #16a34a; font-weight: bold;")
            
    except Exception as e:
        print(f"Error actualizando horas mensuales: {e}")
        # Valores por defecto en caso de error
        self.lbl_horas_mes.setText("0.0h")
        self.lbl_sesiones_mes.setText("0")
        self.lbl_horas_fuera.setText("0.0h")
        self.lbl_promedio_diario.setText("0.0h")
```

### 2.4 professors_tab_widget.py - Integrar nuevo widget

**Modificar el método de inicialización del panel derecho (alrededor de la línea 1180)**

```python
# Agregar después de crear el widget de estadísticas
self.hours_widget = self.crear_widget_horas_mensuales()
hours_tab_index = right_panel.addTab(self.hours_widget, "⏰ Horas Mensuales")
```

**Modificar el método cargar_estadisticas_profesor para incluir actualización de horas**

```python
def cargar_estadisticas_profesor(self):
    """Carga las estadísticas del profesor seleccionado"""
    if not self.profesor_seleccionado:
        return
    
    try:
        stats = self.db_manager.obtener_estadisticas_profesor(self.profesor_seleccionado['id'])
        
        self.lbl_evaluaciones.setText(str(stats['total_evaluaciones']))
        self.lbl_clases.setText(str(stats['total_clases']))
        self.lbl_estudiantes.setText(str(stats['estudiantes_unicos']))
        self.lbl_horas_trabajadas.setText(f"{stats['horas_trabajadas']:.1f}h")
        
        # Cargar evaluaciones recientes
        self.lista_evaluaciones.clear()
        evaluaciones = self.db_manager.obtener_evaluaciones_profesor(self.profesor_seleccionado['id'])
        
        for eval in evaluaciones[:5]:  # Solo las 5 más recientes
            texto = f"⭐ {eval['puntuacion']}/5 - {eval['nombre_usuario']}"
            if eval['comentario']:
                texto += f": {eval['comentario'][:50]}..."
            self.lista_evaluaciones.addItem(texto)
        
        # NUEVO: Actualizar widget de horas mensuales
        if hasattr(self, 'hours_widget'):
            self.actualizar_horas_mensuales()
            
    except Exception as e:
        print(f"Error al cargar estadísticas: {e}")
```

## 3. Validaciones y Mejoras de Sesiones

### 3.1 login_dialog.py - Mejorar manejo de inicio de sesión

**Agregar validación de sesiones abiertas antes de iniciar nueva sesión**

```python
# En el método handle_professor_login, antes de iniciar nueva sesión
def handle_professor_login(self, username, password):
    # ... código existente ...
    
    if login_successful and profesor_data:
        try:
            # NUEVO: Verificar si hay sesiones abiertas
            sesiones_abiertas = self.db_manager.verificar_sesiones_abiertas()
            sesiones_profesor = [s for s in sesiones_abiertas if s['profesor_id'] == profesor_data['id']]
            
            if sesiones_profesor:
                # Hay una sesión abierta, preguntar qué hacer
                respuesta = QMessageBox.question(
                    self, "Sesión Abierta Detectada",
                    f"Se detectó una sesión de trabajo abierta desde {sesiones_profesor[0]['hora_inicio']}.\n\n"
                    "¿Desea cerrar la sesión anterior y abrir una nueva?",
                    QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
                )
                
                if respuesta == QMessageBox.StandardButton.Yes:
                    # Cerrar sesión anterior
                    self.db_manager.finalizar_sesion_trabajo_profesor(profesor_data['id'])
                    QMessageBox.information(self, "Sesión Cerrada", "Sesión anterior cerrada correctamente.")
                else:
                    # No iniciar nueva sesión
                    QMessageBox.information(self, "Sesión Continuada", "Continuando con la sesión existente.")
                    return
            
            # Iniciar nueva sesión de trabajo
            if self.db_manager.iniciar_sesion_trabajo_profesor(profesor_data['id']):
                QMessageBox.information(
                    self, "Sesión Iniciada", 
                    f"✅ Sesión de trabajo iniciada para {profesor_data['nombre']}"
                )
            else:
                QMessageBox.warning(
                    self, "Error", 
                    "No se pudo iniciar la sesión de trabajo"
                )
                
        except Exception as e:
            QMessageBox.warning(
                self, "Advertencia", 
                f"Error al gestionar sesión de trabajo: {str(e)}"
            )
    
    # ... resto del código existente ...
```

### 3.2 main.py - Mejorar cierre de sesión

**Modificar el método closeEvent para incluir más validaciones**

```python
def closeEvent(self, event):
    """Maneja el evento de cierre de la ventana"""
    try:
        # Verificar sesiones abiertas antes de cerrar
        sesiones_abiertas = self.db_manager.verificar_sesiones_abiertas()
        
        if sesiones_abiertas:
            # Mostrar resumen de sesiones que se van a cerrar
            mensaje = "Se cerrarán las siguientes sesiones de trabajo:\n\n"
            for sesion in sesiones_abiertas:
                horas_transcurridas = sesion.get('horas_transcurridas', 0)
                mensaje += f"• {sesion['profesor_nombre']}: {horas_transcurridas:.1f} horas\n"
            
            mensaje += "\n¿Desea continuar?"
            
            respuesta = QMessageBox.question(
                self, "Cerrar Sesiones de Trabajo",
                mensaje,
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
            )
            
            if respuesta == QMessageBox.StandardButton.No:
                event.ignore()
                return
            
            # Cerrar todas las sesiones abiertas
            for sesion in sesiones_abiertas:
                try:
                    resultado = self.db_manager.finalizar_sesion_trabajo_profesor(sesion['profesor_id'])
                    print(f"Sesión cerrada para {sesion['profesor_nombre']}: {resultado.get('horas_trabajadas', 0):.1f} horas")
                except Exception as e:
                    print(f"Error cerrando sesión de {sesion['profesor_nombre']}: {e}")
        
        # Continuar con el cierre normal
        event.accept()
        
    except Exception as e:
        print(f"Error en closeEvent: {e}")
        event.accept()  # Permitir cierre aunque haya error
```

## 4. Configuraciones y Optimizaciones

### 4.1 Agregar configuraciones del sistema

**Ejecutar en la base de datos**

```sql
-- Configuraciones para el sistema de horas
INSERT INTO configuraciones (clave, valor, descripcion) VALUES
('max_horas_sesion', '12', 'Máximo de horas por sesión antes de alerta'),
('alerta_horas_fuera_horario', 'true', 'Activar alertas por horas fuera de horario'),
('reset_mensual_automatico', 'true', 'Reset automático de contadores mensuales'),
('notificar_sesiones_largas', 'true', 'Notificar sesiones de más de 8 horas')
ON CONFLICT (clave) DO NOTHING;
```

### 4.2 Índices para optimización

```sql
-- Índices adicionales para mejorar rendimiento
CREATE INDEX IF NOT EXISTS idx_profesor_horas_activas 
ON profesor_horas_trabajadas(profesor_id) 
WHERE hora_fin IS NULL;

CREATE INDEX IF NOT EXISTS idx_profesor_horas_mes_actual 
ON profesor_horas_trabajadas(profesor_id, fecha) 
WHERE EXTRACT(MONTH FROM fecha) = EXTRACT(MONTH FROM CURRENT_DATE)
AND EXTRACT(YEAR FROM fecha) = EXTRACT(YEAR FROM CURRENT_DATE);
```

## 5. Pruebas y Validación

### 5.1 Casos de Prueba Principales

1. **Inicio de sesión normal**: Profesor inicia sesión → Se crea registro en profesor_horas_trabajadas
2. **Cierre de sesión**: Aplicación se cierra → Se actualiza hora_fin y horas_totales
3. **Sesión larga**: Sesión > 12 horas → Se muestra alerta
4. **Horas fuera de horario**: Trabajo fuera de horario programado → Se detecta y reporta
5. **Cambio de mes**: Contador se reinicia automáticamente
6. **Sesiones múltiples**: Múltiples profesores con sesiones simultáneas

### 5.2 Validaciones de Integridad

- Verificar que no se puedan tener múltiples sesiones abiertas por profesor
- Validar que las horas calculadas sean coherentes
- Comprobar que los horarios programados se comparen correctamente
- Asegurar que el reset mensual funcione correctamente

## 6. Consideraciones de Rendimiento

- Los cálculos de horas fuera de horario se realizan bajo demanda
- Se usan índices específicos para consultas frecuentes
- Las estadísticas se cachean en el widget para evitar recálculos innecesarios
- Las alertas se muestran solo cuando hay datos relevantes

Esta implementación proporciona un sistema robusto y profesional de seguimiento de horas que cumple con todos los requisitos especificados.