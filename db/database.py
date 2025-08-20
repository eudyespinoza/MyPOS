"""Database utility functions for MyPOS.

This module previously contained only helpers to request tokens from the D365
service.  It now also provides lightweight persistence for shopping carts so
that the application can store cart state between sessions without relying on a
full database backend.  Data is stored in a JSON file located next to this
module.  The design favours simplicity and is intended mainly for development
or small deployments.
"""

from __future__ import annotations

import json
import logging
import json
import os
import configparser
from typing import Dict

import requests


# ---------------------------------------------------------------------------
# Configuración y logging
import sqlite3

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(os.path.dirname(ROOT_DIR), "config.ini")

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
# ---------------------------------------------------------------------------
# D365 helpers

def load_d365_config() -> Dict[str, str]:
    if "d365" not in config:
        raise KeyError("La sección 'd365' no se encuentra en config.ini")
    return {
        "resource": config["d365"].get("resource", ""),
        "token_client": config["d365"].get("token_client", ""),
        "client_prod": config["d365"].get("client_prod", ""),
        "client_qa": config["d365"].get("client_qa", ""),
        "client_id_prod": config["d365"].get("client_id_prod", ""),
        "client_id_qa": config["d365"].get("client_id_qa", ""),
        "client_secret_prod": config["d365"].get("client_secret_prod", ""),
        "client_secret_qa": config["d365"].get("client_secret_qa", ""),
    }


def get_access_token_d365():
def get_access_token_d365() -> str | None:
    d365_config = load_d365_config()
    token_params = {
        "grant_type": "client_credentials",
        "client_id": d365_config["client_id_prod"],
        "client_secret": d365_config["client_secret_prod"],
        "resource": d365_config["resource"],
    }
    try:
        response = requests.post(token_url, data=token_params, timeout=60)
        response.raise_for_status()
        token_data = response.json()
        access_token = token_data['access_token']
        logging.info("Consulta token a D365 OK")
        return access_token
    except requests.exceptions.RequestException as e:
        logging.info(f"Consulta token a D365 FALLO. {e}")
        print("Error al obtener el token de acceso:", e)
        return None


def get_access_token_d365_qa():
        response = requests.post(d365_config["token_client"], data=token_params, timeout=60)
        response.raise_for_status()
        token_data = response.json()
        access_token = token_data["access_token"]
        logging.info("Consulta token a D365 OK")
        return access_token
    except requests.exceptions.RequestException as e:  # pragma: no cover - logging
        logging.info("Consulta token a D365 FALLO. %s", e)
        return None


def get_access_token_d365_qa() -> str | None:
    d365_config = load_d365_config()
    token_params = {
        "grant_type": "client_credentials",
        "client_id": d365_config["client_id_qa"],
        "client_secret": d365_config["client_secret_qa"],
        "resource": d365_config["resource"],
    }
    try:
        response = requests.post(token_url, data=token_params, timeout=60)
        response.raise_for_status()
        token_data = response.json()
        access_token = token_data['access_token']
        logging.info("Consulta token a D365 OK")
        return access_token
        response = requests.post(d365_config["token_client"], data=token_params, timeout=60)
        response.raise_for_status()
        token_data = response.json()
        access_token = token_data["access_token"]
        logging.info("Consulta token a D365 OK")
        return access_token
    except requests.exceptions.RequestException as e:  # pragma: no cover - logging
        logging.info("Consulta token a D365 FALLO. %s", e)
        return None


# ---------------------------------------------------------------------------
# Persistencia de carrito

CARTS_FILE = os.path.join(ROOT_DIR, "carts.json")


def save_cart(user_id: str, cart: dict, timestamp: str) -> bool:
    """Persist the shopping cart for ``user_id`` in a JSON file."""

    data = {}
    if os.path.exists(CARTS_FILE):
        try:
            with open(CARTS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            data = {}

    data[user_id] = {"cart": cart, "timestamp": timestamp}
    try:
        with open(CARTS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return True
    except Exception as exc:  # pragma: no cover - logging
        logging.error("Error al guardar carrito de %s: %s", user_id, exc)
        return False


def get_cart(user_id: str) -> dict:
    """Retrieve the stored cart for ``user_id`` from the JSON file."""

    if not os.path.exists(CARTS_FILE):
        return {"items": []}
    try:
        with open(CARTS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get(user_id, {"items": []})
    except Exception as exc:  # pragma: no cover - logging
        logging.error("Error al obtener carrito de %s: %s", user_id, exc)
        return {"items": []}


__all__ = [
    "load_d365_config",
    "get_access_token_d365",
    "get_access_token_d365_qa",
    "save_cart",
    "get_cart",
]

    except requests.exceptions.RequestException as e:
        logging.info(f"Consulta token a D365 FALLO. {e}")
        return None


# ---- Gestión de pagos ----
PAGOS_FILE = os.path.join(ROOT_DIR, "pagos.json")
OPERACIONES_FILE = os.path.join(ROOT_DIR, "operaciones.json")


def _read_json(path, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default


def guardar_pago(operacion_id, pagos):
    """Guarda el detalle de un pago en un archivo JSON."""
    data = _read_json(PAGOS_FILE, [])
    data.append({"operacion_id": operacion_id, **pagos})
    with open(PAGOS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def actualizar_estado_operacion(operacion_id, estado):
    """Actualiza el estado de una operación en un archivo JSON."""
    data = _read_json(OPERACIONES_FILE, {})
    data[str(operacion_id)] = estado
    with open(OPERACIONES_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
def obtener_facturas_emitidas(fecha_inicio, fecha_fin):
    """Devuelve una lista simulada de facturas emitidas entre fechas."""
    return [
        {
            "fecha": fecha_inicio,
            "numero": "F0001",
            "vendedor": "Juan",
            "monto": 1000.0,
        },
        {
            "fecha": fecha_fin,
            "numero": "F0002",
            "vendedor": "Ana",
            "monto": 2000.0,
        },
    ]


def obtener_saldos_por_vendedor(fecha_inicio, fecha_fin):
    """Devuelve saldos simulados por vendedor en el rango de fechas."""
    return [
        {"vendedor": "Juan", "saldo": 500.0},
        {"vendedor": "Ana", "saldo": 1500.0},
    ]

DB_PATH = os.path.join(ROOT_DIR, 'clientes.db')


def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        '''CREATE TABLE IF NOT EXISTS clientes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nombre TEXT NOT NULL,
            dni TEXT NOT NULL,
            cuit TEXT UNIQUE NOT NULL,
            direccion TEXT
        )'''
    )
    conn.commit()
    conn.close()


def guardar_cliente(datos):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        'INSERT INTO clientes (nombre, dni, cuit, direccion) VALUES (?,?,?,?)',
        (datos['nombre'], datos['dni'], datos['cuit'], datos.get('direccion'))
    )
    conn.commit()
    conn.close()


def actualizar_cliente(cuit, datos):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        'UPDATE clientes SET nombre=?, dni=?, cuit=?, direccion=? WHERE cuit=?',
        (
            datos['nombre'],
            datos['dni'],
            datos['cuit'],
            datos.get('direccion'),
            cuit,
        )
    )
    conn.commit()
    conn.close()


def buscar_cliente_por_cuit(cuit):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('SELECT nombre, dni, cuit, direccion FROM clientes WHERE cuit=?', (cuit,))
    row = cursor.fetchone()
    conn.close()
    if row:
        return {
            'nombre': row[0],
            'dni': row[1],
            'cuit': row[2],
            'direccion': row[3]
        }
    return None
