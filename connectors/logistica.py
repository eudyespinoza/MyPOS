"""Utilities for logistics operations such as geocoding and freight cost
estimation.

This module centralises calls to external mapping services.  It provides
helpers to transform free-form addresses into geographic coordinates and to
estimate shipping costs and delivery dates between two points using public
routing services.

The functions are intentionally small and synchronous to simplify their use in
the rest of the codebase.  They rely on free services (OpenStreetMap's
Nominatim and OSRM) so that the module works out of the box in development
environments without requiring API keys.  In production these calls can be
replaced or extended with more robust providers.
"""

from __future__ import annotations

import datetime as _dt
import logging
import math
from typing import Tuple

import httpx


logger = logging.getLogger(__name__)


async def geocodificar_direccion(direccion: str) -> Tuple[float, float]:
    """Return ``(lat, lng)`` for ``direccion`` using the Nominatim service.

    Parameters
    ----------
    direccion:
        Free form address to geocode.

    Returns
    -------
    tuple
        A tuple with latitude and longitude as floats.  In case of error the
        tuple ``(0.0, 0.0)`` is returned.
    """

    url = "https://nominatim.openstreetmap.org/search"
    params = {"format": "json", "q": direccion}
    headers = {"User-Agent": "MyPOS/1.0"}
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(url, params=params, headers=headers)
            response.raise_for_status()
            data = response.json()
            if not data:
                logger.warning("Nominatim no devolvió resultados para %s", direccion)
                return 0.0, 0.0
            return float(data[0]["lat"]), float(data[0]["lon"])
    except Exception as exc:  # pragma: no cover - manejo de errores externo
        logger.error("Error al geocodificar dirección %s: %s", direccion, exc)
        return 0.0, 0.0


async def calcular_costo_flete(
    origen: Tuple[float, float],
    destino: Tuple[float, float],
    costo_por_km: float = 1.0,
) -> Tuple[float, float, str]:
    """Estimate shipping cost and delivery date between two coordinates.

    This function uses the public OSRM API to calculate the driving distance
    between ``origen`` and ``destino``.  The cost is a simple multiplication of
    the distance in kilometres by ``costo_por_km``.  A naïve delivery date
    estimation is also returned assuming a daily travel distance of 500 km.

    Parameters
    ----------
    origen, destino:
        Tuples in the form ``(lat, lng)``.
    costo_por_km:
        Monetary cost per kilometre used for the estimation.

    Returns
    -------
    tuple
        ``(costo, distancia_km, fecha_estimada)`` where ``fecha_estimada`` is a
        string in ISO format (YYYY-MM-DD).
    """

    # OSRM expects lon,lat order
    url = (
        "https://router.project-osrm.org/route/v1/driving/"
        f"{origen[1]},{origen[0]};{destino[1]},{destino[0]}?overview=false"
    )
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()
            distance_m = data["routes"][0]["distance"]
    except Exception as exc:  # pragma: no cover - manejo de errores externo
        logger.error("Error al calcular costo de flete: %s", exc)
        return 0.0, 0.0, _dt.date.today().isoformat()

    distancia_km = distance_m / 1000.0
    costo = round(distancia_km * costo_por_km, 2)

    # Estimar fecha de entrega muy básica: 500 km por día.
    dias = max(1, math.ceil(distancia_km / 500.0))
    fecha_estimada = (_dt.date.today() + _dt.timedelta(days=dias)).isoformat()

    return costo, distancia_km, fecha_estimada


__all__ = ["geocodificar_direccion", "calcular_costo_flete"]

