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

DB_FILE = os.path.join(ROOT_DIR, "mypos.db")


def init_db():
    """Inicializa la base de datos local si no existe."""
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sap_productos (
            codigo TEXT PRIMARY KEY,
            surtido TEXT,
            iva REAL,
            unidad_medida TEXT
        )
        """
    )
    conn.commit()
    conn.close()

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


def agregar_surtido_masivo(productos):
    """Inserta o actualiza un listado de productos SAP."""
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.executemany(
        """
        INSERT OR REPLACE INTO sap_productos (codigo, surtido, iva, unidad_medida)
        VALUES (?, ?, ?, ?)
        """,
        [(
            p.get('codigo'),
            p.get('surtido'),
            p.get('iva'),
            p.get('unidad_medida'),
        ) for p in productos],
    )
    conn.commit()
    total = cur.rowcount
    conn.close()
    return total


def buscar_productos_sap(query):
    """Busca productos SAP que coincidan con el texto indicado."""
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    like = f"%{query}%"
    cur.execute(
        "SELECT codigo, surtido, iva, unidad_medida FROM sap_productos WHERE codigo LIKE ? OR surtido LIKE ?",
        (like, like),
    )
    rows = cur.fetchall()
    conn.close()
    return [
        {
            'codigo': r[0],
            'surtido': r[1],
            'iva': r[2],
            'unidad_medida': r[3],
        }
        for r in rows
    ]


def obtener_producto_sap(codigo):
    """Obtiene un único producto SAP por código."""
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute(
        "SELECT codigo, surtido, iva, unidad_medida FROM sap_productos WHERE codigo = ?",
        (codigo,),
    )
    row = cur.fetchone()
    conn.close()
    if row:
        return {
            'codigo': row[0],
            'surtido': row[1],
            'iva': row[2],
            'unidad_medida': row[3],
        }
    return None
