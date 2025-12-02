import logging
from typing import Optional, Dict, Any
from fastapi import APIRouter, Request, Depends
from fastapi.responses import JSONResponse

from apps.webapp.dependencies import get_db, require_gestion_access
from apps.webapp.utils import _circuit_guard_json

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/api/kpis")
async def api_kpis(_=Depends(require_gestion_access)):
    db = get_db()
    if db is None:
        return {}
    guard = _circuit_guard_json(db, "/api/kpis")
    if guard:
        return guard
    try:
        if hasattr(db, 'obtener_kpis_principales'):
            return db.obtener_kpis_principales()
        # Fallback: use repository if available via db property or method
        if hasattr(db, 'gym') and hasattr(db.gym, 'obtener_kpis_principales'):
             return db.gym.obtener_kpis_principales()
        return {}
    except Exception as e:
        logger.error(f"Error /api/kpis: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)

@router.get("/api/ingresos12m")
async def api_ingresos12m(_=Depends(require_gestion_access)):
    db = get_db()
    if db is None:
        return {}
    guard = _circuit_guard_json(db, "/api/ingresos12m")
    if guard:
        return guard
    try:
        # Logic from legacy server or PaymentManager
        # Assuming logic was moved to PaymentManager or Repository
        if hasattr(db, 'obtener_ingresos_ultimos_12_meses'):
            return db.obtener_ingresos_ultimos_12_meses()
        # Try payment repository
        if hasattr(db, 'pagos') and hasattr(db.pagos, 'obtener_ingresos_ultimos_12_meses'):
             return db.pagos.obtener_ingresos_ultimos_12_meses()
        # Try PaymentManager if available in dependencies?
        # For now, return empty dict or implement query directly if repos don't have it
        # Implementing direct query as fallback based on standard logic
        with db.get_connection_context() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT 
                    TO_CHAR(fecha_pago, 'YYYY-MM') as mes, 
                    SUM(monto) as total 
                FROM pagos 
                WHERE fecha_pago >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '11 months')
                GROUP BY TO_CHAR(fecha_pago, 'YYYY-MM') 
                ORDER BY mes
            """)
            rows = cur.fetchall()
            return {row[0]: float(row[1]) for row in rows}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@router.get("/api/nuevos12m")
async def api_nuevos12m(_=Depends(require_gestion_access)):
    db = get_db()
    if db is None:
        return {}
    guard = _circuit_guard_json(db, "/api/nuevos12m")
    if guard:
        return guard
    try:
        with db.get_connection_context() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT 
                    TO_CHAR(fecha_registro, 'YYYY-MM') as mes, 
                    COUNT(*) as total 
                FROM usuarios 
                WHERE fecha_registro >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '11 months')
                GROUP BY TO_CHAR(fecha_registro, 'YYYY-MM') 
                ORDER BY mes
            """)
            rows = cur.fetchall()
            return {row[0]: int(row[1]) for row in rows}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@router.get("/api/arpu12m")
async def api_arpu12m(_=Depends(require_gestion_access)):
    # Placeholder for ARPU logic
    return {}

@router.get("/api/activos_inactivos")
async def api_activos_inactivos(_=Depends(require_gestion_access)):
    db = get_db()
    if db is None:
        return {}
    try:
        with db.get_connection_context() as conn:
            cur = conn.cursor()
            cur.execute("SELECT activo, COUNT(*) FROM usuarios GROUP BY activo")
            rows = cur.fetchall()
            return {("Activos" if r[0] else "Inactivos"): r[1] for r in rows}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@router.get("/api/kpis_avanzados")
async def api_kpis_avanzados(_=Depends(require_gestion_access)):
    # Placeholder
    return {}

@router.get("/api/cohort_retencion_6m")
async def api_cohort_retencion_6m(_=Depends(require_gestion_access)):
    # Placeholder
    return {}

@router.get("/api/cohort_retencion_heatmap")
async def api_cohort_retencion_heatmap(_=Depends(require_gestion_access)):
    # Placeholder
    return {}

@router.get("/api/arpa_por_tipo_cuota")
async def api_arpa_por_tipo_cuota(_=Depends(require_gestion_access)):
    # Placeholder
    return {}

@router.get("/api/payment_status_dist")
async def api_payment_status_dist(_=Depends(require_gestion_access)):
    # Placeholder
    return {}

@router.get("/api/waitlist_events")
async def api_waitlist_events(_=Depends(require_gestion_access)):
    # Placeholder
    return []

@router.get("/api/delinquency_alerts_recent")
async def api_delinquency_alerts_recent(_=Depends(require_gestion_access)):
    # Placeholder
    return []

@router.get("/api/profesor_resumen")
async def api_profesor_resumen(request: Request, _=Depends(require_gestion_access)):
    # Placeholder
    return {}

# Exports
@router.get("/api/export")
async def api_export(request: Request, _=Depends(require_gestion_access)):
    # Placeholder for export logic
    return JSONResponse({"error": "Not implemented"}, status_code=501)

@router.get("/api/export_csv")
async def api_export_csv(request: Request, _=Depends(require_gestion_access)):
    # Placeholder for CSV export logic
    return JSONResponse({"error": "Not implemented"}, status_code=501)
