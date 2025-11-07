# Paridad de funcionalidades: Programa Desktop vs WebApp

Este documento compara las funcionalidades del programa Desktop y la WebApp, destacando qué está presente en ambos y qué falta para alcanzar paridad completa.

## Funciones del Programa Desktop → Presencia en WebApp

- ✅ Gestión de usuarios (altas/bajas/edición, notas, etiquetas, estados)
- ✅ Gestión de pagos (crear/editar/eliminar) con filtros avanzados
- ✅ Numeración de recibos configurable (panel de configuración)
- ✅ Generación de recibo PDF desde pagos
- ✅ Configuración de métodos de pago (CRUD)
- ✅ Configuración de conceptos de pago (CRUD)
- ✅ Configuración de tipos de cuota (CRUD)
- ✅ Banco de ejercicios (CRUD)
 - ✅ Constructor de rutinas, asignación por usuario y exportación PDF/Excel
- ❌ Gestión de clases grupales (programación, cupos, inscripciones, asistencia por clase)
- ✅ Registro de asistencias y check-in con QR/token (incluye portal de check-in)
- ❌ WhatsApp: envío de mensajes salientes (la WebApp recibe webhooks y registra mensajes, sin envío desde la web)
 - ✅ WhatsApp: envío de mensajes salientes desde Gestión (bienvenida, confirmación de pago, desactivación, recordatorios)
- ✅ Gestión de profesores: datos, horarios, sesiones y métricas (horas trabajadas/proyectadas/extras)
- ✅ KPIs y dashboard (ingresos, nuevos usuarios, ARPU/retención/cohortes)
- ✅ Exportación CSV de listados (usuarios, pagos)
 - ❌ Exportaciones ejecutivas en PDF/Excel para reportes (más allá de recibos)
- ✅ Administración avanzada: renumerar IDs y asegurar Dueño (RLS/triggers)
- ❌ Auditoría detallada de seguridad y cambios críticos (panel dedicado)
- ❌ Backups y mantenimiento automatizado de BD (respaldos, optimización, integridad)
- ❌ Personalización & accesibilidad avanzada (alto contraste, lectores de pantalla, atajos)
- ❌ Diagnósticos del sistema (CPU/memoria/disco, salud BD, rendimiento)
- ✅ Emisión de QR desde Gestión
- ✅ Observaciones/notas en recibos y visualización coherente

## Funciones de la WebApp → Presencia en Programa Desktop

- ✅ Login del dueño y portal web con sesiones (roles dueño/profesor)
- ✅ Dashboard web con KPIs y reportes interactivos
- ✅ Gestión de usuarios (notas, etiquetas, estados) y pagos con filtros
- ✅ Numeración de recibos configurable y generación de PDF
- ✅ Banco de ejercicios (CRUD)
 - ✅ Gestión de rutinas desde la web (constructor, asignación, exportación)
- ❌ Gestión de clases grupales desde la web (programación, inscripciones, asistencia por clase)
- ✅ Check-in web para socios (QR/token) y registro de asistencia
- ✅ Gestión de profesores: horarios, sesiones y métricas (incluye horas extra)
- ✅ Exportación CSV
- ✅ Acciones avanzadas: renumerar IDs y asegurar Dueño
- ✅ Integración WhatsApp: recepción de webhooks y registro de mensajes
- ❌ WhatsApp: envío saliente y configuración guiada en UI
- ❌ Auditoría de seguridad con panel dedicado
- ❌ Diagnósticos del sistema y mantenimiento (backups, optimización BD) en la web
- ❌ Personalización/accesibilidad avanzada en UI (alto contraste, atajos, lectores)
- ✅ Portal de acceso por rol (dueño/profesor/socio) y flujo de check-in

## Notas y criterios

- La WebApp ya cubre el núcleo operativo: usuarios, pagos, recibos, profesores, asistencias, KPIs y CSV. Las brechas principales están en rutinas/clases, auditoría/diagnósticos y funciones avanzadas de WhatsApp.
 - “Rutinas” está implementado en BD, Desktop y WebApp (editor, asignación y exportación PDF/Excel).
- En WhatsApp, la WebApp recibe y registra webhooks; el envío saliente y la configuración guiada permanecen en Desktop.
 - En WhatsApp, la WebApp ahora envía mensajes desde Gestión y registra webhooks; la configuración guiada desde UI sigue pendiente y se realiza en Desktop.
 - Accesibilidad en la WebApp: se incorporaron roles ARIA reforzados, navegación por teclado (tabindex, atajos con `aria-keyshortcuts`), estados `aria-live`/`aria-busy` y enfoque al abrir modales; el modo de alto contraste y opciones de personalización aún están pendientes.
- Auditoría, backups y diagnósticos están implementados en Desktop (widgets dedicados) y no tienen contraparte en la WebApp.

## Recomendaciones para paridad completa

- Añadir módulo de Clases grupales en la WebApp: horarios, cupos, inscripciones y asistencia por clase.
- Exponer paneles web para Diagnósticos del sistema, Auditoría y Backups (estado, acciones y reportes).
- Completar configuración WhatsApp en WebApp desde UI.
- Incorporar accesibilidad avanzada (alto contraste, navegación por teclado, roles ARIA reforzados) y opciones de personalización.