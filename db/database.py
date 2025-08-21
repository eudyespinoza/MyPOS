"""Database helpers and local persistence utilities for MyPOS.

This module centralises all lightweight data storage used by the
application.  Data is stored in several small SQLite databases located in
the ``db`` folder.  The helper functions here provide both bulk loading
routines (used by ``db.fabric`` to refresh the caches) and query helpers
used by the web application.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import configparser
from typing import Dict, Iterable, List, Any

import pyarrow.compute as pc
import pyarrow.parquet as pq
import requests

from config import CACHE_FILE_PRODUCTOS

# ---------------------------------------------------------------------------
# Configuración y logging
# ---------------------------------------------------------------------------

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

# ---------------------------------------------------------------------------
# Rutas de bases de datos y helper de conexión
# ---------------------------------------------------------------------------

DB_PATHS = {
    "atributos": os.path.join(ROOT_DIR, "atributos.db"),
    "stock": os.path.join(ROOT_DIR, "stock.db"),
    "grupos": os.path.join(ROOT_DIR, "grupos_cumplimiento.db"),
    "empleados": os.path.join(ROOT_DIR, "empleados.db"),
    "stores": os.path.join(ROOT_DIR, "stores.db"),
    "misc": os.path.join(ROOT_DIR, "misc.db"),
    "sap": os.path.join(ROOT_DIR, "mypos.db"),
    "pos_config": os.path.join(ROOT_DIR, "pos_config.db"),
    "clientes": os.path.join(ROOT_DIR, "clientes.db"),
    "pagos": os.path.join(ROOT_DIR, "pagos.db"),
}

PRODUCT_COLUMN_MAPPING = {
    "Número de Producto": "numero_producto",
    "Nombre de Categoría de Producto": "categoria_producto",
    "Nombre del Producto": "nombre_producto",
    "Grupo de Cobertura": "grupo_cobertura",
    "Unidad de Medida": "unidad_medida",
    "PrecioFinalConIVA": "precio_final_con_iva",
    "PrecioFinalConDescE": "precio_final_con_descuento",
    "StoreNumber": "store_number",
    "TotalDisponibleVenta": "total_disponible_venta",
    "Signo": "signo",
    "Multiplo": "multiplo",
}


def conectar_db(nombre: str) -> sqlite3.Connection:
    """Retorna una conexión a la base de datos indicada."""
    path = DB_PATHS[nombre]
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn

# ---------------------------------------------------------------------------
# Configuración D365
# ---------------------------------------------------------------------------

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

def get_access_token_d365() -> str | None:
    d365_config = load_d365_config()
    token_params = {
        "grant_type": "client_credentials",
        "client_id": d365_config["client_id_prod"],
        "client_secret": d365_config["client_secret_prod"],
        "resource": d365_config["resource"],
    }
    token_url = d365_config["token_client"]
    try:
        response = requests.post(token_url, data=token_params, timeout=60)
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
    token_url = d365_config["token_client"]
    try:
        response = requests.post(token_url, data=token_params, timeout=60)
        response.raise_for_status()
        token_data = response.json()
        access_token = token_data["access_token"]
        logging.info("Consulta token a D365 OK")
        return access_token
    except requests.exceptions.RequestException as e:  # pragma: no cover - logging
        logging.info("Consulta token a D365 FALLO. %s", e)
        return None

# ---------------------------------------------------------------------------
# Inicialización de bases
# ---------------------------------------------------------------------------

def init_db() -> None:
    """Crea todas las tablas necesarias si aún no existen."""
    # Productos SAP
    with conectar_db("sap") as conn:
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

    # Atributos
    with conectar_db("atributos") as conn:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS atributos (
                product_number TEXT,
                attribute_name TEXT,
                attribute_value TEXT
            )
            """
        )
        conn.commit()

    # Stock
    with conectar_db("stock") as conn:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS stock (
                codigo TEXT,
                almacen_365 TEXT,
                stock_fisico REAL,
                disponible_venta REAL,
                disponible_entrega REAL,
                comprometido REAL
            )
            """
        )
        conn.commit()

    # Grupos de cumplimiento
    with conectar_db("grupos") as conn:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS grupos_cumplimiento (
                store_locator_group_name TEXT,
                invent_location_id TEXT
            )
            """
        )
        conn.commit()

    # Empleados
    with conectar_db("empleados") as conn:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS empleados (
                email TEXT PRIMARY KEY,
                nombre_completo TEXT,
                id_puesto TEXT,
                empleado_d365 TEXT,
                numero_sap TEXT,
                last_store TEXT
            )
            """
        )
        conn.commit()

    # Tiendas
    with conectar_db("stores") as conn:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS stores (
                almacen_retiro TEXT,
                sitio_almacen_retiro TEXT,
                id_tienda TEXT PRIMARY KEY,
                id_unidad_operativa TEXT,
                nombre_tienda TEXT,
                almacen_envio TEXT,
                sitio_almacen_envio TEXT,
                direccion_unidad_operativa TEXT,
                direccion_completa_unidad_operativa TEXT
            )
            """
        )
        conn.commit()

    # Config POS
    with conectar_db("pos_config") as conn:
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

    # Clientes
    with conectar_db("clientes") as conn:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS clientes (
                cuit TEXT PRIMARY KEY,
                nombre TEXT,
                dni TEXT,
                direccion TEXT
            )
            """
        )
        conn.commit()

    # Pagos y facturación
    with conectar_db("pagos") as conn:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS operaciones (
                operacion_id TEXT PRIMARY KEY,
                estado TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS pagos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                operacion_id TEXT,
                efectivo REAL,
                transferencia REAL,
                tarjeta REAL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (operacion_id) REFERENCES operaciones (operacion_id)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS facturas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fecha TEXT,
                vendedor TEXT,
                total REAL
            )
            """
        )
        conn.commit()

    # Misceláneos (token, contador)
    with conectar_db("misc") as conn:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS tokens (
                token TEXT,
                created_at TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS contador_pdf (
                id INTEGER PRIMARY KEY CHECK (id=1),
                valor INTEGER
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS contador_presupuesto (
                id INTEGER PRIMARY KEY CHECK (id=1),
                valor INTEGER
            )
            """
        )
        conn.commit()

# ---------------------------------------------------------------------------
# Funciones de carga masiva
# ---------------------------------------------------------------------------

def _get_attr(row: Any, attr: str, default: Any = None) -> Any:
    return getattr(row, attr, row[attr] if isinstance(row, dict) and attr in row else default)

def agregar_atributos_masivo(atributos: Iterable[Any]) -> int:
    """Inserta atributos en la base local."""
    with conectar_db("atributos") as conn:
        cur = conn.cursor()
        cur.executemany(
            "INSERT OR REPLACE INTO atributos (product_number, attribute_name, attribute_value) VALUES (?, ?, ?)",
            [
                (
                    _get_attr(a, "ProductNumber"),
                    _get_attr(a, "AttributeName"),
                    _get_attr(a, "AttributeValue"),
                )
                for a in atributos
            ],
        )
        conn.commit()
        return cur.rowcount

def agregar_stock_masivo(stock_data: Iterable[Any]) -> int:
    with conectar_db("stock") as conn:
        cur = conn.cursor()
        cur.executemany(
            """
            INSERT OR REPLACE INTO stock
            (codigo, almacen_365, stock_fisico, disponible_venta, disponible_entrega, comprometido)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    _get_attr(s, "Codigo"),
                    _get_attr(s, "Almacen_365"),
                    _get_attr(s, "StockFisico"),
                    _get_attr(s, "DisponibleVenta"),
                    _get_attr(s, "DisponibleEntrega"),
                    _get_attr(s, "Comprometido"),
                )
                for s in stock_data
            ],
        )
        conn.commit()
        return cur.rowcount

def agregar_grupos_cumplimiento_masivo(grupos_data: Iterable[Any]) -> int:
    with conectar_db("grupos") as conn:
        cur = conn.cursor()
        cur.executemany(
            "INSERT OR REPLACE INTO grupos_cumplimiento (store_locator_group_name, invent_location_id) VALUES (?, ?)",
            [
                (
                    _get_attr(g, "StoreLocatorGroupName"),
                    _get_attr(g, "InventLocationId"),
                )
                for g in grupos_data
            ],
        )
        conn.commit()
        return cur.rowcount

def agregar_empleados_masivo(empleados: Iterable[Any]) -> int:
    with conectar_db("empleados") as conn:
        cur = conn.cursor()
        cur.executemany(
            """
            INSERT OR REPLACE INTO empleados
            (empleado_d365, id_puesto, email, nombre_completo, numero_sap, last_store)
            VALUES (?, ?, ?, ?, ?, COALESCE((SELECT last_store FROM empleados WHERE email = ?), ''))
            """,
            [
                (
                    _get_attr(e, "Id_Empleado_365"),
                    _get_attr(e, "Id_Puesto"),
                    _get_attr(e, "Email"),
                    _get_attr(e, "Nombre_Completo"),
                    _get_attr(e, "Numero_SAP"),
                    _get_attr(e, "Email"),
                )
                for e in empleados
            ],
        )
        conn.commit()
        return cur.rowcount

def agregar_datos_tienda_masivo(stores: Iterable[Any]) -> int:
    with conectar_db("stores") as conn:
        cur = conn.cursor()
        cur.executemany(
            """
            INSERT OR REPLACE INTO stores
            (almacen_retiro, sitio_almacen_retiro, id_tienda, id_unidad_operativa, nombre_tienda,
             almacen_envio, sitio_almacen_envio, direccion_unidad_operativa, direccion_completa_unidad_operativa)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    _get_attr(s, "Almacen_Retiro"),
                    _get_attr(s, "Sitio_Almacen_Retiro"),
                    _get_attr(s, "Id_Tienda"),
                    _get_attr(s, "Id_Unidad_Operativa"),
                    _get_attr(s, "Nombre_Tienda"),
                    _get_attr(s, "Almacen_Envio"),
                    _get_attr(s, "Sitio_Almacen_Envio"),
                    _get_attr(s, "Direccion_Unidad_Operativa"),
                    _get_attr(s, "Direccion_Completa_Unidad_Operativa"),
                )
                for s in stores
            ],
        )
        conn.commit()
        return cur.rowcount

def agregar_surtido_masivo(productos: Iterable[Dict[str, Any]]) -> int:
    with conectar_db("sap") as conn:
        cur = conn.cursor()
        cur.executemany(
            """
            INSERT OR REPLACE INTO sap_productos (codigo, surtido, iva, unidad_medida)
            VALUES (?, ?, ?, ?)
            """,
            [
                (
                    p.get("codigo"),
                    p.get("surtido"),
                    p.get("iva"),
                    p.get("unidad_medida"),
                )
                for p in productos
            ],
        )
        conn.commit()
        return cur.rowcount

# ---------------------------------------------------------------------------
# Funciones de consulta
# ---------------------------------------------------------------------------

def obtener_atributos(product_id: int | str) -> List[Dict[str, Any]]:
    with conectar_db("atributos") as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT product_number, attribute_name, attribute_value FROM atributos WHERE product_number = ?",
            (str(product_id),),
        )
        return [
            {
                "ProductNumber": r[0],
                "AttributeName": r[1],
                "AttributeValue": r[2],
            }
            for r in cur.fetchall()
        ]

def obtener_todos_atributos() -> List[Dict[str, Any]]:
    with conectar_db("atributos") as conn:
        cur = conn.cursor()
        cur.execute("SELECT product_number, attribute_name, attribute_value FROM atributos")
        return [
            {
                "ProductNumber": r[0],
                "AttributeName": r[1],
                "AttributeValue": r[2],
            }
            for r in cur.fetchall()
        ]

def obtener_stock() -> List[Dict[str, Any]]:
    with conectar_db("stock") as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT codigo, almacen_365, stock_fisico, disponible_venta, disponible_entrega, comprometido FROM stock"
        )
        return [
            {
                "codigo": r[0],
                "almacen_365": r[1],
                "stock_fisico": r[2],
                "disponible_venta": r[3],
                "disponible_entrega": r[4],
                "comprometido": r[5],
            }
            for r in cur.fetchall()
        ]

def obtener_grupos_cumplimiento(store_group: str) -> List[str]:
    with conectar_db("grupos") as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT invent_location_id FROM grupos_cumplimiento WHERE store_locator_group_name = ?",
            (store_group,),
        )
        return [r[0] for r in cur.fetchall()]

def obtener_empleados() -> List[Dict[str, Any]]:
    with conectar_db("empleados") as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT nombre_completo, email, id_puesto, empleado_d365, numero_sap, last_store FROM empleados"
        )
        return [
            {
                "nombre_completo": r[0],
                "email": r[1],
                "id_puesto": r[2],
                "empleado_d365": r[3],
                "numero_sap": r[4],
                "last_store": r[5],
            }
            for r in cur.fetchall()
        ]

def obtener_empleados_by_email(email: str) -> Dict[str, Any] | None:
    with conectar_db("empleados") as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT nombre_completo, email, id_puesto, empleado_d365, numero_sap, last_store
            FROM empleados WHERE email = ?
            """,
            (email.lower(),),
        )
        row = cur.fetchone()
        if row:
            return {
                "nombre_completo": row[0],
                "email": row[1],
                "id_puesto": row[2],
                "empleado_d365": row[3],
                "numero_sap": row[4],
                "last_store": row[5],
            }
        return None

def actualizar_last_store(email: str, store_id: str) -> None:
    with conectar_db("empleados") as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE empleados SET last_store = ? WHERE email = ?",
            (store_id, email.lower()),
        )
        conn.commit()


def obtener_stores_from_parquet() -> List[str]:
    """Devuelve la lista de IDs de tiendas disponibles desde el Parquet de productos."""
    if not os.path.exists(CACHE_FILE_PRODUCTOS):
        return []

    table = pq.read_table(CACHE_FILE_PRODUCTOS)
    renamed_table = table.rename_columns(
        [PRODUCT_COLUMN_MAPPING.get(col, col) for col in table.column_names]
    )
    stores = pc.unique(renamed_table["store_number"]).to_pylist()
    return [s for s in stores if s]

def obtener_datos_tienda_por_id(store_id: str) -> Dict[str, Any] | None:
    with conectar_db("stores") as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT almacen_retiro, sitio_almacen_retiro, id_tienda, id_unidad_operativa, nombre_tienda,
                   almacen_envio, sitio_almacen_envio, direccion_unidad_operativa, direccion_completa_unidad_operativa
            FROM stores WHERE id_tienda = ?
            """,
            (store_id,),
        )
        row = cur.fetchone()
        if row:
            return {
                "almacen_retiro": row[0],
                "sitio_almacen_retiro": row[1],
                "id_tienda": row[2],
                "id_unidad_operativa": row[3],
                "nombre_tienda": row[4],
                "almacen_envio": row[5],
                "sitio_almacen_envio": row[6],
                "direccion_unidad_operativa": row[7],
                "direccion_completa_unidad_operativa": row[8],
            }
        return None


def obtener_equivalencia() -> List[Dict[str, Any]]:
    """Devuelve lista de productos con su multiplo desde el Parquet."""
    if not os.path.exists(CACHE_FILE_PRODUCTOS):
        return []

    table = pq.read_table(CACHE_FILE_PRODUCTOS)
    renamed_table = table.rename_columns(
        [PRODUCT_COLUMN_MAPPING.get(col, col) for col in table.column_names]
    )
    eq_table = renamed_table.select(["numero_producto", "multiplo"])
    data = eq_table.to_pydict()
    return [
        {"numero_producto": num, "multiplo": mult}
        for num, mult in zip(data["numero_producto"], data["multiplo"])
    ]

# ---------------------------------------------------------------------------
# Tokens y utilidades varias
# ---------------------------------------------------------------------------

def guardar_token_d365(token: str) -> None:
    with conectar_db("misc") as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM tokens")
        cur.execute("INSERT INTO tokens (token, created_at) VALUES (?, datetime('now'))", (token,))
        conn.commit()

def obtener_token_d365() -> str | None:
    with conectar_db("misc") as conn:
        cur = conn.cursor()
        cur.execute("SELECT token FROM tokens ORDER BY rowid DESC LIMIT 1")
        row = cur.fetchone()
        return row[0] if row else None

def obtener_contador_pdf() -> int:
    with conectar_db("misc") as conn:
        cur = conn.cursor()
        cur.execute("SELECT valor FROM contador_pdf WHERE id = 1")
        row = cur.fetchone()
        valor = (row[0] if row else 0) + 1
        cur.execute("INSERT OR REPLACE INTO contador_pdf (id, valor) VALUES (1, ?)", (valor,))
        conn.commit()
        return valor

def obtener_contador_presupuesto() -> int:
    with conectar_db("misc") as conn:
        cur = conn.cursor()
        cur.execute("SELECT valor FROM contador_presupuesto WHERE id = 1")
        row = cur.fetchone()
        valor = (row[0] if row else 0) + 1
        cur.execute(
            "INSERT OR REPLACE INTO contador_presupuesto (id, valor) VALUES (1, ?)",
            (valor,),
        )
        conn.commit()
        return valor

# ---------------------------------------------------------------------------
# Gestión de clientes
# ---------------------------------------------------------------------------

def guardar_cliente(cliente: Dict[str, Any]) -> None:
    with conectar_db("clientes") as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT OR REPLACE INTO clientes (cuit, nombre, dni, direccion) VALUES (?, ?, ?, ?)",
            (
                cliente.get("cuit"),
                cliente.get("nombre"),
                cliente.get("dni"),
                cliente.get("direccion"),
            ),
        )
        conn.commit()

def actualizar_cliente(cuit: str, datos: Dict[str, Any]) -> bool:
    with conectar_db("clientes") as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE clientes SET cuit = ?, nombre = ?, dni = ?, direccion = ? WHERE cuit = ?",
            (
                datos.get("cuit"),
                datos.get("nombre"),
                datos.get("dni"),
                datos.get("direccion"),
                cuit,
            ),
        )
        conn.commit()
        return cur.rowcount > 0

def buscar_cliente_por_cuit(cuit: str) -> Dict[str, Any] | None:
    with conectar_db("clientes") as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT cuit, nombre, dni, direccion FROM clientes WHERE cuit = ?",
            (cuit,),
        )
        row = cur.fetchone()
        if row:
            return {
                "cuit": row[0],
                "nombre": row[1],
                "dni": row[2],
                "direccion": row[3],
            }
        return None

# ---------------------------------------------------------------------------
# Pagos y facturación
# ---------------------------------------------------------------------------

def guardar_pago(operacion_id: str, pagos: Dict[str, float]) -> None:
    with conectar_db("pagos") as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT OR IGNORE INTO operaciones (operacion_id, estado) VALUES (?, ?)",
            (operacion_id, "pendiente"),
        )
        cur.execute(
            "INSERT INTO pagos (operacion_id, efectivo, transferencia, tarjeta) VALUES (?, ?, ?, ?)",
            (
                operacion_id,
                pagos.get("efectivo", 0.0),
                pagos.get("transferencia", 0.0),
                pagos.get("tarjeta", 0.0),
            ),
        )
        conn.commit()

def actualizar_estado_operacion(operacion_id: str, estado: str) -> None:
    with conectar_db("pagos") as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT OR IGNORE INTO operaciones (operacion_id, estado) VALUES (?, ?)",
            (operacion_id, estado),
        )
        cur.execute(
            "UPDATE operaciones SET estado = ? WHERE operacion_id = ?",
            (estado, operacion_id),
        )
        conn.commit()

def obtener_facturas_emitidas(start_date: str, end_date: str) -> List[Dict[str, Any]]:
    with conectar_db("pagos") as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, fecha, vendedor, total FROM facturas WHERE fecha BETWEEN ? AND ?",
            (start_date, end_date),
        )
        return [
            {"id": r[0], "fecha": r[1], "vendedor": r[2], "total": r[3]}
            for r in cur.fetchall()
        ]

def obtener_saldos_por_vendedor(start_date: str, end_date: str) -> List[Dict[str, Any]]:
    with conectar_db("pagos") as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT vendedor, SUM(total) FROM facturas WHERE fecha BETWEEN ? AND ? GROUP BY vendedor",
            (start_date, end_date),
        )
        return [
            {"vendedor": r[0], "total": r[1]}
            for r in cur.fetchall()
        ]

# ---------------------------------------------------------------------------
# Gestión de carrito de compras
# ---------------------------------------------------------------------------

CARTS_FILE = os.path.join(ROOT_DIR, "carts.json")

def save_cart(user_id: str, cart: dict, timestamp: str) -> bool:
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
    if not os.path.exists(CARTS_FILE):
        return {"items": []}
    try:
        with open(CARTS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get(user_id, {"items": []})
    except Exception as exc:  # pragma: no cover - logging
        logging.error("Error al obtener carrito de %s: %s", user_id, exc)
        return {"items": []}

# ---------------------------------------------------------------------------
# Config POS helpers
# ---------------------------------------------------------------------------

def add_config_pos(tienda_id: str, pto_venta_id: str, centro_costo: str) -> int:
    with conectar_db("pos_config") as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO config_pos (tienda_id, pto_venta_id, centro_costo) VALUES (?, ?, ?)",
            (tienda_id, pto_venta_id, centro_costo),
        )
        conn.commit()
        return cur.lastrowid

def get_all_config_pos() -> List[Dict[str, Any]]:
    with conectar_db("pos_config") as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, tienda_id, pto_venta_id, centro_costo FROM config_pos")
        return [dict(row) for row in cur.fetchall()]

def get_config_pos_by_ids(tienda_id: str, pto_venta_id: str) -> Dict[str, Any] | None:
    with conectar_db("pos_config") as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, tienda_id, pto_venta_id, centro_costo FROM config_pos WHERE tienda_id = ? AND pto_venta_id = ?",
            (tienda_id, pto_venta_id),
        )
        row = cur.fetchone()
        return dict(row) if row else None

def update_config_pos(config_id: int, tienda_id: str, pto_venta_id: str, centro_costo: str) -> bool:
    with conectar_db("pos_config") as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE config_pos SET tienda_id = ?, pto_venta_id = ?, centro_costo = ? WHERE id = ?",
            (tienda_id, pto_venta_id, centro_costo, config_id),
        )
        conn.commit()
        return cur.rowcount > 0

def delete_config_pos(config_id: int) -> bool:
    with conectar_db("pos_config") as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM config_pos WHERE id = ?", (config_id,))
        conn.commit()
        return cur.rowcount > 0

# ---------------------------------------------------------------------------
# Búsqueda de productos SAP local
# ---------------------------------------------------------------------------

def buscar_productos_sap(query: str) -> List[Dict[str, Any]]:
    with conectar_db("sap") as conn:
        cur = conn.cursor()
        like = f"%{query}%"
        cur.execute(
            "SELECT codigo, surtido, iva, unidad_medida FROM sap_productos WHERE codigo LIKE ? OR surtido LIKE ?",
            (like, like),
        )
        rows = cur.fetchall()
        return [
            {
                "codigo": r[0],
                "surtido": r[1],
                "iva": r[2],
                "unidad_medida": r[3],
            }
            for r in rows
        ]

def obtener_producto_sap(codigo: str) -> Dict[str, Any] | None:
    with conectar_db("sap") as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT codigo, surtido, iva, unidad_medida FROM sap_productos WHERE codigo = ?",
            (codigo,),
        )
        row = cur.fetchone()
        if row:
            return {
                "codigo": row[0],
                "surtido": row[1],
                "iva": row[2],
                "unidad_medida": row[3],
            }
        return None


def obtener_producto_por_id(product_id: str) -> Dict[str, Any] | None:
    """Alias para obtener un producto por su código."""
    return obtener_producto_sap(product_id)

# ---------------------------------------------------------------------------
# Exported names
# ---------------------------------------------------------------------------

__all__ = [
    # Configuración D365
    "load_d365_config",
    "get_access_token_d365",
    "get_access_token_d365_qa",
    # Inicialización
    "init_db",
    # Carga masiva
    "agregar_atributos_masivo",
    "agregar_stock_masivo",
    "agregar_grupos_cumplimiento_masivo",
    "agregar_empleados_masivo",
    "agregar_datos_tienda_masivo",
    "agregar_surtido_masivo",
    # Consultas
    "obtener_atributos",
    "obtener_todos_atributos",
    "obtener_stock",
    "obtener_grupos_cumplimiento",
    "obtener_empleados",
    "obtener_empleados_by_email",
    "actualizar_last_store",
    "obtener_stores_from_parquet",
    "obtener_datos_tienda_por_id",
    "obtener_equivalencia",
    # Misceláneos
    "guardar_token_d365",
    "obtener_token_d365",
    "obtener_contador_pdf",
    "obtener_contador_presupuesto",
    "save_cart",
    "get_cart",
    # Clientes
    "guardar_cliente",
    "actualizar_cliente",
    "buscar_cliente_por_cuit",
    # Pagos y facturación
    "guardar_pago",
    "actualizar_estado_operacion",
    "obtener_facturas_emitidas",
    "obtener_saldos_por_vendedor",
    # Config POS
    "add_config_pos",
    "get_all_config_pos",
    "get_config_pos_by_ids",
    "update_config_pos",
    "delete_config_pos",
    # Productos SAP
    "buscar_productos_sap",
    "obtener_producto_sap",
    "obtener_producto_por_id",
]
