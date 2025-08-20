import requests
import configparser
import os
import logging
import sqlite3

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

# Ruta del archivo de base de datos para configuraciones POS
DB_PATH = os.path.join(ROOT_DIR, 'pos_config.db')

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


# ---------------------------------------------------------------------------
# Funciones de base de datos para configuración de Puntos de Venta
# ---------------------------------------------------------------------------

def get_connection():
    """Devuelve una conexión a la base de datos de configuración."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Crea las tablas necesarias si no existen."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS config_pos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tienda_id TEXT NOT NULL,
            pto_venta_id TEXT NOT NULL,
            centro_costo TEXT NOT NULL
        )
        """
    )
    conn.commit()
    conn.close()


def add_config_pos(tienda_id: str, pto_venta_id: str, centro_costo: str) -> int:
    """Inserta una nueva configuración de punto de venta."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO config_pos (tienda_id, pto_venta_id, centro_costo) VALUES (?, ?, ?)",
        (tienda_id, pto_venta_id, centro_costo),
    )
    conn.commit()
    config_id = cur.lastrowid
    conn.close()
    return config_id


def get_all_config_pos():
    """Obtiene todas las configuraciones de puntos de venta."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, tienda_id, pto_venta_id, centro_costo FROM config_pos")
    rows = [dict(row) for row in cur.fetchall()]
    conn.close()
    return rows


def get_config_pos_by_ids(tienda_id: str, pto_venta_id: str):
    """Obtiene una configuración por tienda y punto de venta."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, tienda_id, pto_venta_id, centro_costo FROM config_pos WHERE tienda_id = ? AND pto_venta_id = ?",
        (tienda_id, pto_venta_id),
    )
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def update_config_pos(config_id: int, tienda_id: str, pto_venta_id: str, centro_costo: str) -> bool:
    """Actualiza una configuración existente."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "UPDATE config_pos SET tienda_id = ?, pto_venta_id = ?, centro_costo = ? WHERE id = ?",
        (tienda_id, pto_venta_id, centro_costo, config_id),
    )
    conn.commit()
    updated = cur.rowcount > 0
    conn.close()
    return updated


def delete_config_pos(config_id: int) -> bool:
    """Elimina una configuración por su ID."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM config_pos WHERE id = ?", (config_id,))
    conn.commit()
    deleted = cur.rowcount > 0
    conn.close()
    return deleted
