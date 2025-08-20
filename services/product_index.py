_index = []

def index_products(products):
    """Reemplaza el índice de productos en memoria."""
    global _index
    _index = products or []
    return len(_index)

def search_products(query):
    """Busca productos por coincidencia parcial de SKU o descripción."""
    if not query:
        return []
    q = query.lower()
    return [p for p in _index if q in str(p.get('sku', '')).lower() or q in str(p.get('description', '')).lower()]
