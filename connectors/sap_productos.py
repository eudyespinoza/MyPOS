import os
import requests
import configparser
import logging

# Ruta de configuración
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(os.path.dirname(ROOT_DIR), 'config.ini')

config = configparser.ConfigParser()
config.read(CONFIG_PATH)

logger = logging.getLogger(__name__)


def load_sap_config():
    """Carga la configuración necesaria para conectarse a SAP.

    Retorna un diccionario con la URL base y el endpoint de productos.
    """
    if 'sap' not in config:
        raise KeyError("La sección 'sap' no se encuentra en config.ini")
    return {
        'base_url': config['sap'].get('base_url', ''),
        'productos_endpoint': config['sap'].get('productos_endpoint', '')
    }


def obtener_productos_sap():
    """Obtiene el surtido de productos desde SAP.

    La función realiza una solicitud HTTP al endpoint configurado y
    normaliza la respuesta para devolver una lista de diccionarios con
    los campos: codigo, surtido, iva y unidad_medida.
    """
    sap_config = load_sap_config()
    url = f"{sap_config['base_url'].rstrip('/')}/{sap_config['productos_endpoint'].lstrip('/')}"

    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        data = response.json()
        productos = []
        for item in data.get('value', data):
            productos.append({
                'codigo': item.get('codigo') or item.get('Codigo'),
                'surtido': item.get('surtido') or item.get('Surtido'),
                'iva': item.get('iva') or item.get('IVA'),
                'unidad_medida': item.get('unidad_medida') or item.get('UnidadMedida')
            })
        return productos
    except Exception as exc:  # pragma: no cover - manejo de red
        logger.error(f"Error al obtener productos de SAP: {exc}")
        return []
