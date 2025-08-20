import requests
import configparser
import os
import logging

# Obtén la ruta absoluta a la raíz del proyecto
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(os.path.dirname(ROOT_DIR), 'config.ini')  # Config.ini en la raíz del proyecto

# Cargar la configuración
config = configparser.ConfigParser()
config.read(CONFIG_PATH)

LOG_DIR = os.path.join(ROOT_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "d365_interface.log")

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

def load_d365_config():

    if 'd365' not in config:
        raise KeyError("La sección 'd365' no se encuentra en config.ini")

    return {
        "resource": config['d365'].get('resource', ''),
        "token_client": config['d365'].get('token_client', ''),
        "client_prod": config['d365'].get('client_prod', ''),
        "client_qa": config['d365'].get('client_qa', ''),
        "client_id_prod": config['d365'].get('client_id_prod', ''),
        "client_id_qa": config['d365'].get('client_id_qa', ''),
        "client_secret_prod": config['d365'].get('client_secret_prod', ''),
        "client_secret_qa": config['d365'].get('client_secret_qa', ''),
    }


def get_access_token_d365():

    d365_config = load_d365_config()
    client_id_prod = d365_config["client_id_prod"]
    client_secret_prod = d365_config["client_secret_prod"]
    token_url = d365_config["token_client"]
    resource = d365_config["resource"]
    token_params = {
        "grant_type": "client_credentials",
        "client_id": client_id_prod,
        "client_secret": client_secret_prod,
        "resource": resource
    }

    try:
        response = requests.post(token_url, data=token_params, timeout=60)
        response.raise_for_status()  # Verificar si hay errores en la respuesta

        token_data = response.json()
        access_token = token_data['access_token']
        logging.info(f"Consulta token a D365 OK")
        return access_token

    except requests.exceptions.RequestException as e:
        logging.info(f"Consulta token a D365 FALLO. {e}")
        print("Error al obtener el token de acceso:", e)
        return None


def get_access_token_d365_qa():

    d365_config = load_d365_config()
    client_id_qa = d365_config["client_id_qa"]
    client_secret_qa = d365_config["client_secret_qa"]
    token_url = d365_config["token_client"]
    resource = d365_config["resource"]
    token_params = {
        "grant_type": "client_credentials",
        "client_id": client_id_qa,
        "client_secret": client_secret_qa,
        "resource": resource
    }

    try:
        response = requests.post(token_url, data=token_params, timeout=60)
        response.raise_for_status()  # Verificar si hay errores en la respuesta

        token_data = response.json()
        access_token = token_data['access_token']
        logging.info(f"Consulta token a D365 OK")
        return access_token

    except requests.exceptions.RequestException as e:
        logging.info(f"Consulta token a D365 FALLO. {e}")
        return None