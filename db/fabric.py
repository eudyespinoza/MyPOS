import os
import traceback
import pyodbc
import logging
import asyncio
import configparser
import queue
import threading
from contextlib import contextmanager
from db.database import agregar_atributos_masivo, agregar_stock_masivo, \
    agregar_grupos_cumplimiento_masivo, agregar_empleados_masivo, agregar_datos_tienda_masivo
import requests

# Obtén la ruta absoluta a la raíz del proyecto
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))  # Carpeta donde está fabric.py
CONFIG_PATH = os.path.join(os.path.dirname(ROOT_DIR), 'config.ini')  # Config.ini en la raíz del proyecto

# Cargar la configuración
config = configparser.ConfigParser()
config.read(CONFIG_PATH)

_pool_lock = threading.Lock()
_connection_pool = queue.Queue(maxsize=10)

def load_db_config():
    """
    Loads the database configuration settings from the application's configuration file.
    This function retrieves database-related configuration values such as server, database
    name, username, and password from the parsed configuration. If the required 'database'
    section is not present in the configuration, it raises a KeyError.

    :raises KeyError: If the 'database' section is missing in the configuration file.

    :return: A dictionary containing the database configuration values with the following
    keys: 'server_fabric', 'database_fabric', 'username_fabric', 'password_fabric'.
    :rtype: dict
    """
    if 'database' not in config:
        raise KeyError("La sección 'database' no se encuentra en config.ini")

    return {
        "server_fabric": config['database'].get('server_fabric', ''),
        "database_fabric": config['database'].get('database_fabric', ''),
        "username_fabric": config['database'].get('username_fabric', ''),
        "password_fabric": config['database'].get('password_fabric', ''),
    }

def _create_connection():
    db_config = load_db_config()
    server = db_config["server_fabric"]
    database = db_config["database_fabric"]
    username = db_config["username_fabric"]
    password = db_config["password_fabric"]
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        "Authentication=ActiveDirectoryPassword;"
    )
    return pyodbc.connect(conn_str)


def _get_connection():
    with _pool_lock:
        if not _connection_pool.empty():
            return _connection_pool.get()
    return _create_connection()


def _release_connection(conn):
    with _pool_lock:
        try:
            _connection_pool.put(conn, block=False)
            return
        except queue.Full:
            pass
    conn.close()


@contextmanager
def conectar_fabric_db():
    try:
        conn = _get_connection()
        yield conn
    except pyodbc.Error as e:
        logging.error(f"Error al conectar con Fabric: {e}")
        raise
    finally:
        _release_connection(conn)

def obtener_parquet_productos():
    url = 'https://fabricstorageeastus.blob.core.windows.net/fabric/Respondio/Productos_Buscador?sp=re&st=2025-03-31T17:42:39Z&se=2025-04-01T01:42:39Z&spr=https&sv=2024-11-04&sr=b&sig=Eewxv5qqA76g%2BSBvmoHQQJaYfuTLSW7KlMmSJsd0xVU%3D'
    parquet_productos = requests.get(url)
    return parquet_productos

def obtener_atributos_fabric():
    """
    Obtains attributes from the Fabric database and inserts them into another system in bulk.
    """
    query = "SELECT * FROM Atributos;"
    try:
        with conectar_fabric_db() as conexion_fabric:
            cursor_fabric = conexion_fabric.cursor()
            cursor_fabric.execute(query)
            atributos = cursor_fabric.fetchall()

            if not atributos:
                logging.info("No se encontraron atributos en Fabric.")
                return 0

            total_insertados = agregar_atributos_masivo(atributos)
            return total_insertados
    except Exception as e:
        logging.error(f"Error al obtener atributos de Fabric: {e}")
        return 0

def obtener_stock_fabric():
    """
    Obtiene el stock desde Fabric y lo almacena en la base de datos local.
    """
    query = """
    SELECT Codigo, Almacen_365, StockFisico, DisponibleVenta, DisponibleEntrega, Comprometido 
    FROM DataStagingWarehouse.dbo.Stock_Buscador
    """

    try:
        with conectar_fabric_db() as conexion_fabric:
            cursor = conexion_fabric.cursor()
            cursor.execute(query)
            stock_data = cursor.fetchall()

            if not stock_data:
                logging.info("No se encontraron datos de stock en Fabric.")
                return 0

            total_insertados = agregar_stock_masivo(stock_data)
            return total_insertados
    except Exception as e:
        logging.error(f"Error al obtener stock de Fabric: {e}")
        return 0

    finally:
        conexion_fabric.close()
        logging.info("Conexión con Fabric cerrada.")

def obtener_stock_categoria(categoria_id):
    """
    Obtiene el stock para una categoría específica desde Fabric.

    :param categoria_id: Identificador de la categoría.
    :return: Lista de diccionarios con la información de stock por categoría.
    """
    query = """
    SELECT Codigo, Almacen_365, StockFisico, DisponibleVenta, DisponibleEntrega, Comprometido
    FROM DataStagingWarehouse.dbo.Stock_Buscador
    WHERE CategoriaId = ?
    """

    conexion_fabric = conectar_fabric_db()
    if not conexion_fabric:
        logging.error("No se pudo conectar a la base de datos de Fabric.")
        return []

    try:
        cursor = conexion_fabric.cursor()
        cursor.execute(query, (categoria_id,))
        registros = cursor.fetchall()

        if not registros:
            logging.info(f"No se encontraron datos de stock para la categoría {categoria_id}.")
            return []

        resultado = [
            {
                "codigo": row.Codigo,
                "almacen_365": row.Almacen_365,
                "stock_fisico": row.StockFisico,
                "disponible_venta": row.DisponibleVenta,
                "disponible_entrega": row.DisponibleEntrega,
                "comprometido": row.Comprometido,
            }
            for row in registros
        ]
        return resultado

    except Exception as e:
        logging.error(f"Error al obtener stock por categoría desde Fabric: {e}")
        return []

    finally:
        conexion_fabric.close()
        logging.info("Conexión con Fabric cerrada.")

def obtener_lista_precios_sucursal(sucursal_id):
    """
    Obtiene la lista de precios para una sucursal específica desde Fabric.

    :param sucursal_id: Identificador de la sucursal.
    :return: Lista de diccionarios con precios regular, applog y outlet.
    """
    query = """
    SELECT Codigo, PrecioRegular, PrecioApplog, PrecioOutlet
    FROM DataStagingWarehouse.dbo.Lista_Precios_Buscador
    WHERE SucursalId = ?
    """

    conexion_fabric = conectar_fabric_db()
    if not conexion_fabric:
        logging.error("No se pudo conectar a la base de datos de Fabric.")
        return []

    try:
        cursor = conexion_fabric.cursor()
        cursor.execute(query, (sucursal_id,))
        registros = cursor.fetchall()

        if not registros:
            logging.info(f"No se encontraron precios para la sucursal {sucursal_id}.")
            return []

        resultado = [
            {
                "codigo": row.Codigo,
                "precio_regular": row.PrecioRegular,
                "precio_applog": row.PrecioApplog,
                "precio_outlet": row.PrecioOutlet,
            }
            for row in registros
        ]
        return resultado

    except Exception as e:
        logging.error(f"Error al obtener lista de precios desde Fabric: {e}")
        return []

    finally:
        conexion_fabric.close()
        logging.info("Conexión con Fabric cerrada.")

def obtener_grupos_cumplimiento_fabric():
    """
    Obtiene los grupos de cumplimiento desde Fabric y los almacena en la base de datos local.
    """
    query = """
    SELECT StoreLocatorGroupName, InventLocationId
    FROM DataStagingWarehouse.dbo.Grupos_Cumplimiento_Buscador
    """

    try:
        with conectar_fabric_db() as conexion_fabric:
            cursor = conexion_fabric.cursor()
            cursor.execute(query)
            grupos_data = cursor.fetchall()

            if not grupos_data:
                logging.info("No se encontraron datos de grupos de cumplimiento en Fabric.")
                return 0

            total_insertados = agregar_grupos_cumplimiento_masivo(grupos_data)
            return total_insertados
    except Exception as e:
        logging.error(f"Error al obtener grupos de cumplimiento de Fabric: {e}")
        return 0

def obtener_empleados_fabric():
    """
    Obtains employees from the Fabric database and inserts them into another system in bulk.
    """
    query = ("""
    SELECT
        [value.PersonnelNumber] as Id_Empleado_365,
        [value.EmploymentId] as Id_Puesto,
        LOWER([value.PrimaryContactEmail]) as Email,  -- Convertir email a minúsculas
        [value.Name] as Nombre_Completo,    
        CASE
            WHEN [value.PhoneticFirstName] IS NOT NULL AND [value.PhoneticFirstName] <> '' THEN [value.PhoneticFirstName]
            ELSE [value.KnownAs]
        END as Numero_SAP
    FROM [Entidades_Dynamics_365_Prod].[dbo].[Employees] Empleados
    """)
    try:
        with conectar_fabric_db() as conexion_fabric:
            cursor_fabric = conexion_fabric.cursor()
            cursor_fabric.execute(query)
            empleados = cursor_fabric.fetchall()

            if not empleados:
                logging.info("No se encontraron empleados en Fabric.")
                return 0

            total_insertados = agregar_empleados_masivo(empleados)
            return total_insertados
    except Exception as e:
        logging.error(f"Error al obtener empleados de Fabric: {e}")
        return 0

def obtener_datos_tiendas():
    """
    Obtiene los datos de productos desde la base de datos Fabric y los almacena localmente.
    Usa fetchall() para recuperar todos los datos en una sola llamada.
    """
    query = """
    SELECT
        RTC.[value.InventLocation] AS Almacen_Retiro,
        IND1.inventsiteid AS Sitio_Almacen_Retiro,
        RTC.[value.StoreNumber] AS Id_Tienda,
        RTC.[value.OperatingUnitNumber] AS Id_Unidad_Operativa,
        RTC.[value.Name] AS Nombre_Tienda,
        RTC.[value.InventLocationIdForCustomerOrder] AS Almacen_Envio,
        IND2.inventsiteid AS Sitio_Almacen_Envio,
        OPM.[value.AddressLocationId] AS Direccion_Unidad_Operativa,
        OPM.[value.FullPrimaryAddress] AS Direccion_Completa_Unidad_Operativa
    FROM [DataStagingWarehouse].[dbo].[RetailChannels] RTC
    INNER JOIN [dataverse_fbmprod].[dbo].[inventdim] IND1
        ON RTC.[value.InventLocation] = IND1.inventlocationid
    INNER JOIN [dataverse_fbmprod].[dbo].[inventdim] IND2
        ON RTC.[value.InventLocationIdForCustomerOrder] = IND2.inventlocationid
    LEFT JOIN [DataStagingWarehouse].[dbo].[OperatingUnits] OPM ON RTC.[value.OperatingUnitNumber] = OPM.[value.OperatingUnitNumber]
    WHERE
    IND1.inventsiteid IS NOT NULL
    AND IND2.inventsiteid IS NOT NULL;
    """

    try:
        with conectar_fabric_db() as conexion_fabric:
            cursor_fabric = conexion_fabric.cursor()
            logging.info("Ejecutando consulta SQL en Fabric...")
            cursor_fabric.execute(query)
            logging.info("Consulta ejecutada con éxito.")

            logging.info("Recuperando los datos de tiendas con fetchall()...")
            productos = cursor_fabric.fetchall()
            logging.info(f"Número de registros recuperados: {len(productos)}")

            if not productos:
                logging.info("No se encontraron datos en Fabric.")
                return 0

            total_insertados = agregar_datos_tienda_masivo(productos)
            logging.info(f"Total de datos de tiendas insertados: {total_insertados}")

            return total_insertados
    except Exception as e:
        logging.error(f"Error al obtener datos de tienda de Fabric: {e}\n{traceback.format_exc()}")
        return 0

async def obtener_datos_codigo_postal(codigo_postal):
    """Consulta Fabric para obtener datos de dirección basados en el código postal."""
    logging.info(f"Consultando datos para código postal: {codigo_postal}")
    query = """
    SELECT
        AD.[value.ZipCode] AS AddressZipCode,
        AD.[value.CountryRegionId] AS AddressCountryRegionId,
        AD.[value.StateId] AS AddressState,
        AD.[value.CountyId] AS AddressCounty,
        AD.[value.CityAlias] AS AddressCity,
        AC.[value.Description] AS CountyName
    FROM [DataStagingWarehouse].[dbo].[AddressPostalCodesV3] AD
    INNER JOIN [DataStagingWarehouse].[dbo].[AddressCounties] AC
    ON AD.[value.CountyId]=AC.[value.CountyId]
    WHERE
        [value.ZipCode] <> ''
        AND AD.[value.ZipCode] LIKE '%[0-9]%'
        AND AD.[value.ZipCode] NOT LIKE '%[^0-9]%'
        AND AD.[value.StateId] <> ''
        AND AD.[value.CountyId] <> ''
        AND AD.[value.CountryRegionId] = 'ARG'
        AND AD.[value.ZipCode] = ?
    """

    try:
        with conectar_fabric_db() as conexion_fabric:
            cursor = conexion_fabric.cursor()
            cursor.execute(query, (codigo_postal,))
            resultados = cursor.fetchall()
            if not resultados:
                logging.info(f"No se encontraron datos para el código postal {codigo_postal}")
                return [], None
            datos = [
                {
                    "AddressZipCode": row.AddressZipCode,
                    "AddressCountryRegionId": row.AddressCountryRegionId,
                    "AddressState": row.AddressState,
                    "AddressCounty": row.AddressCounty,
                    "AddressCity": row.AddressCity,
                    "CountyName": row.CountyName
                } for row in resultados
            ]
            logging.info(f"Datos encontrados para código postal {codigo_postal}: {len(datos)} registros")
            return datos, None
    except Exception as e:
        error = f"Error al consultar código postal en Fabric: {str(e)}"
        logging.error(error)
        return None, error

def run_obtener_datos_codigo_postal(codigo_postal):
    return asyncio.run(obtener_datos_codigo_postal(codigo_postal))
