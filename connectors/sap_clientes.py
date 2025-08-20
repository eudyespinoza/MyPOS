import requests
import configparser
import os
import logging

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(os.path.dirname(ROOT_DIR), 'config.ini')

config = configparser.ConfigParser()
config.read(CONFIG_PATH)

LOG_DIR = os.path.join(ROOT_DIR, 'logs')
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, 'sap_clientes.log')

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


def _load_sap_config():
    if 'sap' not in config:
        raise KeyError("La sección 'sap' no se encuentra en config.ini")
    return {
        'base_url': config['sap'].get('base_url', ''),
        'token': config['sap'].get('token', '')
    }


def consultar_datos_impositivos(cuit):
    cfg = _load_sap_config()
    url = f"{cfg['base_url'].rstrip('/')}/clientes/{cuit}"
    headers = {'Authorization': f"Bearer {cfg['token']}"}
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        logging.info('Consulta de datos impositivos SAP exitosa para %s', cuit)
        return response.json()
    except Exception as exc:
        logging.error('Error consultando SAP para %s: %s', cuit, exc)
        return None


def actualizar_datos_impositivos(cuit, datos):
    cfg = _load_sap_config()
    url = f"{cfg['base_url'].rstrip('/')}/clientes/{cuit}"
    headers = {
        'Authorization': f"Bearer {cfg['token']}",
        'Content-Type': 'application/json'
    }
    try:
        response = requests.post(url, json=datos, headers=headers, timeout=30)
        response.raise_for_status()
        logging.info('Actualización de datos impositivos SAP exitosa para %s', cuit)
        return True
    except Exception as exc:
        logging.error('Error actualizando SAP para %s: %s', cuit, exc)
        return False
