import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(BASE_DIR, "cache")
os.makedirs(CACHE_DIR, exist_ok=True)

CACHE_FILE_PRODUCTOS = os.path.join(CACHE_DIR, 'productos_cache.parquet')
CACHE_FILE_STOCK = os.path.join(CACHE_DIR, 'stock_cache.parquet')
CACHE_FILE_CLIENTES = os.path.join(CACHE_DIR, 'clientes_cache.parquet')
CACHE_FILE_EMPLEADOS = os.path.join(CACHE_DIR, 'empleados_cache.parquet')
CACHE_FILE_ATRIBUTOS = os.path.join(CACHE_DIR, 'atributos_cache.parquet')