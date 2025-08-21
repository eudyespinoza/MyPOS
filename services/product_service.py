import os
import logging
from typing import Any, Dict, Optional

import pyarrow.parquet as pq
import pyarrow.compute as pc

from config import CACHE_FILE_PRODUCTOS

logger = logging.getLogger(__name__)


def obtener_producto_por_id(product_id: str | int) -> Optional[Dict[str, Any]]:
    """Obtiene los datos de un producto por su ID desde el Parquet de productos.

    Args:
        product_id: Identificador del producto (código).

    Returns:
        Un diccionario con la información del producto o ``None`` si no se encuentra.
    """
    product_id = str(product_id)

    if not os.path.exists(CACHE_FILE_PRODUCTOS):
        logger.error("Archivo de productos no encontrado: %s", CACHE_FILE_PRODUCTOS)
        return None

    try:
        table = pq.read_table(CACHE_FILE_PRODUCTOS)
        column_mapping = {
            "Número de Producto": "numero_producto",
            "Nombre del Producto": "nombre_producto",
        }
        table = table.rename_columns([column_mapping.get(col, col) for col in table.column_names])
        filtro = pc.equal(pc.field("numero_producto"), product_id)
        resultado = table.filter(filtro)
        if resultado.num_rows == 0:
            return None
        # Convertimos la primera fila a diccionario
        record = resultado.slice(0, 1).to_pylist()[0]
        return record
    except Exception as e:  # pragma: no cover - logging
        logger.error("Error al obtener producto por ID %s: %s", product_id, e, exc_info=True)
        return None
