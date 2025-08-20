import os
import logging
from typing import List, Dict

# La dependencia pymongo puede no estar disponible en todos los entornos.
# Se realiza la importación de forma opcional para evitar errores en tiempo de ejecución.
try:
    from pymongo import MongoClient, ASCENDING, TEXT
except Exception:  # pragma: no cover - fallback en caso de ausencia
    MongoClient = None
    ASCENDING = TEXT = None

logger = logging.getLogger(__name__)

MONGO_URL = os.getenv("MONGO_URL", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "mypos")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "productos")

if MongoClient:
    _client = MongoClient(MONGO_URL)
    _db = _client[MONGO_DB]
    _collection = _db[MONGO_COLLECTION]
    # Ensure indexes for fast lookup
    _collection.create_index([("sku", ASCENDING)], unique=True)
    _collection.create_index([("descripcion", TEXT)])
else:  # pragma: no cover - entorno sin MongoDB
    _collection = None


def indexar_productos(productos: List[Dict[str, str]]) -> int:
    """Indexa una lista de productos en MongoDB.

    Args:
        productos: Lista de diccionarios con claves ``sku`` y ``descripcion``.
    Returns:
        Número de documentos indexados.
    """
    if not productos or _collection is None:
        logger.warning("Colección de MongoDB no disponible o lista vacía")
        return 0
    operaciones = []
    for prod in productos:
        operaciones.append(
            {
                "update_one": {
                    "filter": {"sku": prod.get("sku")},
                    "update": {"$set": prod},
                    "upsert": True,
                }
            }
        )
    if operaciones:
        result = _collection.bulk_write(operaciones)
        return result.upserted_count + result.modified_count
    return 0


def buscar_productos(termino: str) -> List[Dict[str, str]]:
    """Busca productos por SKU exacto o por coincidencia en la descripción."""
    if not termino or _collection is None:
        return []
    query = {"$or": [{"sku": termino}, {"$text": {"$search": termino}}]}
    cursor = _collection.find(query, {"_id": 0})
    return list(cursor)

