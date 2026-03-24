from __future__ import annotations

import os
from datetime import datetime

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

try:
    from app.logging_config import get_application_logger
except ModuleNotFoundError:
    from logging_config import get_application_logger

try:
    from app.colsanitas_service import (
        PortalAutomationError,
        PortalConfigError,
        ReporteSinRegistrosError,
        consultar_reporte_institucion,
    )
except ModuleNotFoundError:
    # Support direct execution: python app/main.py
    from colsanitas_service import (
        PortalAutomationError,
        PortalConfigError,
        ReporteSinRegistrosError,
        consultar_reporte_institucion,
    )

APP_NAME = "Sanitas Reporte Institucion"
APP_VERSION = "2.0.0"
DEFAULT_HOST = os.getenv("APP_HOST", "127.0.0.1")
DEFAULT_PORT = int(os.getenv("APP_PORT", "8000"))
logger = get_application_logger("api")


class ConsultaReporteRequest(BaseModel):
    fechaDesde: str = Field(..., examples=["01/02/2026"])
    fechaHasta: str = Field(..., examples=["01/03/2026"])
    documento: str = Field(..., examples=["1072645338"])


app = FastAPI(
    title=APP_NAME,
    version=APP_VERSION,
    description=(
        "Servicio FastAPI que usa Selenium en headless para ingresar al portal de "
        "Colsanitas, descargar el Reporte de Institucion y devolver el resultado filtrado por documento."
    ),
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health", tags=["health"])
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/reportes/institucion", tags=["reportes"])
def obtener_reporte_institucion(
    fecha_desde: str = Query(..., alias="fechaDesde", description="Fecha inicial en formato dd/mm/yyyy."),
    fecha_hasta: str = Query(..., alias="fechaHasta", description="Fecha final en formato dd/mm/yyyy."),
    documento: str = Query(..., description="Numero de documento a filtrar."),
) -> dict[str, object]:
    return ejecutar_consulta(fecha_desde, fecha_hasta, documento)


@app.post("/api/reportes/institucion", tags=["reportes"])
def obtener_reporte_institucion_post(payload: ConsultaReporteRequest) -> dict[str, object]:
    return ejecutar_consulta(payload.fechaDesde, payload.fechaHasta, payload.documento)


def ejecutar_consulta(fecha_desde: str, fecha_hasta: str, documento: str) -> dict[str, object]:
    logger.info(
        f"Solicitud recibida: fechaDesde={fecha_desde}, fechaHasta={fecha_hasta}, documento={documento.strip()}"
    )
    fecha_desde_dt = validar_fecha(fecha_desde)
    fecha_hasta_dt = validar_fecha(fecha_hasta)

    if fecha_desde_dt > fecha_hasta_dt:
        raise HTTPException(
            status_code=422,
            detail="fechaDesde no puede ser mayor que fechaHasta.",
        )

    try:
        resultado = consultar_reporte_institucion(fecha_desde, fecha_hasta, documento.strip())
        logger.info(
            f"Solicitud completada correctamente. Registros encontrados: {resultado.get('total_registros', 0)}"
        )
        return resultado
    except PortalConfigError as exc:
        logger.error(f"Error de configuracion del portal: {exc}")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except ReporteSinRegistrosError as exc:
        logger.warning(f"Sin registros para el filtro solicitado: {exc}")
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except PortalAutomationError as exc:
        logger.error(f"Error de automatizacion del portal: {exc}")
        raise HTTPException(status_code=502, detail=str(exc)) from exc


def validar_fecha(fecha: str) -> datetime:
    try:
        return datetime.strptime(fecha, "%d/%m/%Y")
    except ValueError as exc:
        raise HTTPException(
            status_code=422,
            detail="Las fechas deben tener el formato dd/mm/yyyy.",
        ) from exc


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=DEFAULT_HOST, port=DEFAULT_PORT)
