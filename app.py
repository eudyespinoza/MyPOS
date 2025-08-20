from flask import Flask, render_template, redirect, url_for, session, jsonify, request, send_from_directory, flash, send_file
from db.database import init_db, obtener_atributos, obtener_stores_from_parquet, obtener_stock, \
    obtener_grupos_cumplimiento, obtener_empleados, obtener_todos_atributos, guardar_token_d365, obtener_token_d365, \
    obtener_producto_por_id, get_config_pos_by_ids
    obtener_producto_por_id, buscar_productos_sap, obtener_producto_sap
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
from db.fabric import obtener_atributos_fabric, obtener_stock_fabric, obtener_grupos_cumplimiento_fabric, \
    obtener_empleados_fabric, obtener_datos_tiendas, run_obtener_datos_codigo_postal, \
    obtener_stock_categoria, obtener_lista_precios_sucursal
from auth import auth_bp, login_required, logout
from blueprints.autenticacion_avanzada import autenticacion_avanzada_bp
from blueprints.facturacion_arca import facturacion_arca_bp
from blueprints.secuencia_numerica import secuencia_bp
from blueprints.config_pos import config_pos_bp
from blueprints.pagos import pagos_bp
from connectors.d365_interface import (
    run_crear_presupuesto_batch,
    run_obtener_presupuesto_d365,
    run_actualizar_presupuesto_d365,
    run_validar_cliente_existente,
    run_alta_cliente_d365,
    guardar_numero_presupuesto,
    obtener_numeros_presupuesto,
)
    guardar_presupuesto_local,
    obtener_presupuestos_locales,
)
from blueprints.caja import caja_bp
from blueprints.pagos import pagos_bp
from blueprints.clientes import clientes_bp
from connectors.d365_interface import run_crear_presupuesto_batch, run_obtener_presupuesto_d365, run_actualizar_presupuesto_d365, run_validar_cliente_existente, run_alta_cliente_d365
from connectors.get_token import get_access_token_d365, get_access_token_d365_qa
from db.database import obtener_datos_tienda_por_id, obtener_empleados_by_email, actualizar_last_store, obtener_contador_pdf, save_cart, get_cart
from werkzeug.utils import secure_filename
from db.database import (
    obtener_datos_tienda_por_id,
    obtener_empleados_by_email,
    actualizar_last_store,
    obtener_contador_pdf,
    save_cart,
    get_cart,
)
from functools import lru_cache
from services.email_service import enviar_correo_fallo
from services.search_service import indexar_productos, buscar_productos
from services.product_index import index_products, search_products
import os
import io
import datetime
import pyarrow.parquet as pq
import pyarrow as pa
import pyarrow.compute as pc
import logging
import threading
from datetime import timedelta, timezone
import json
import requests
import io
import redis
from config import CACHE_FILE_PRODUCTOS, CACHE_FILE_STOCK, CACHE_FILE_CLIENTES, CACHE_FILE_EMPLEADOS, CACHE_FILE_ATRIBUTOS

clientes_lock = threading.Lock()

app = Flask(__name__, static_folder='static')

# 🔹 Configuración de Redis para caché
redis_client = redis.Redis.from_url(os.getenv('REDIS_URL', 'redis://localhost:6379/0'))


def cache_get_json(key):
    data = redis_client.get(key)
    if data:
        return json.loads(data)
    return None


def cache_set_json(key, value, ex=3600):
    redis_client.set(key, json.dumps(value), ex=ex)


def cache_get_table(key):
    data = redis_client.get(key)
    if data:
        buffer = io.BytesIO(data)
        return pq.read_table(buffer)
    return None


def cache_set_table(key, table, ex=3600):
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    redis_client.set(key, buffer.getvalue(), ex=ex)

# 🔹 Inicializar la base de datos
init_db()

# 🔹 Establecer la clave secreta para las sesiones
app.secret_key = 'sfrhdzfhsthes5ghe5hsths'
app.permanent_session_lifetime = timedelta(hours=4)

# 🔹 Definir rutas base
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# 🔹 Ruta del flag de inicialización
FLAG_FILE = os.path.join(BASE_DIR, 'db_initialized.flag')
FLAG_FILE_START = os.path.join(BASE_DIR, 'first_load_initialized.flag')

# 🔹 Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(BASE_DIR, "app.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

PRODUCTOS_PARQUET_URL = "https://fabricstorageeastus.blob.core.windows.net/fabric/Buscador/Productos_Buscador.parquet?sp=re&st=2025-04-09T18:46:04Z&se=2030-04-10T02:46:04Z&spr=https&sv=2024-11-04&sr=b&sig=4keHTQiesvWQlHhHfEi7mftZHq7yTJvsLdkdZ9oGWK8%3D"
CLIENTES_PARQUET_URL = "https://fabricstorageeastus.blob.core.windows.net/fabric/Buscador/Clientes_Base_Buscador.parquet?sp=re&st=2025-04-10T12:52:43Z&se=2030-04-10T20:52:43Z&spr=https&sv=2024-11-04&sr=b&sig=ELgolJCh%2BqJVNigrcw5hPpgDQblWuTQ378gIBUaW9Fo%3D"

def descargar_parquet_productos():
    """Descarga el archivo Parquet de productos desde la URL y lo guarda en CACHE_FILE_PRODUCTOS."""
    try:
        logger.info("Descargando archivo Parquet de productos desde la URL...")
        response = requests.get(PRODUCTOS_PARQUET_URL)
        response.raise_for_status()
        with open(CACHE_FILE_PRODUCTOS, 'wb') as f:
            f.write(response.content)
        logger.info(f"Archivo Parquet descargado y guardado como {CACHE_FILE_PRODUCTOS}")
    except Exception as e:
        logger.error(f"Error al descargar el archivo Parquet de productos: {e}", exc_info=True)
        enviar_correo_fallo("descargar_parquet_productos", str(e))
        raise

def actualizar_cache_productos():
    """Actualiza el archivo productos_cache.parquet descargándolo directamente."""
    try:
        descargar_parquet_productos()
        load_products_to_memory.cache_clear()
        logger.info("Caché de productos actualizada y memoria invalidada.")
    except Exception as e:
        logger.error(f"Error al actualizar caché de productos: {e}", exc_info=True)
        raise

def descargar_parquet_clientes():
    """Descarga el archivo Parquet de clientes desde la URL y lo guarda en CACHE_FILE_CLIENTES."""
    try:
        logger.info("Descargando archivo Parquet de clientes desde la URL...")
        response = requests.get(CLIENTES_PARQUET_URL)
        response.raise_for_status()
        with open(CACHE_FILE_CLIENTES, 'wb') as f:
            f.write(response.content)
        logger.info(f"Archivo Parquet descargado y guardado como {CACHE_FILE_CLIENTES}")
    except Exception as e:
        logger.error(f"Error al descargar el archivo Parquet de clientes: {e}", exc_info=True)
        enviar_correo_fallo("descargar_parquet_clientes", str(e))
        raise

def actualizar_cache_clientes():
    """Actualiza el archivo clientes_cache.parquet descargándolo directamente."""
    try:
        descargar_parquet_clientes()
        load_parquet_to_memory.cache_clear()
        logger.info("Caché de clientes actualizada y memoria invalidada.")
    except Exception as e:
        logger.error(f"Error al actualizar caché de clientes: {e}", exc_info=True)
        raise

def obtener_clientes_cache():
    """Obtiene los clientes desde el archivo Parquet en memoria."""
    cached_clients = cache_get_json('clientes_cache')
    if cached_clients:
        return cached_clients

    if os.path.exists(CACHE_FILE_CLIENTES):
        mod_time = datetime.date.fromtimestamp(os.path.getmtime(CACHE_FILE_CLIENTES))
        if mod_time != datetime.date.today():
            logger.info("El archivo de clientes está desactualizado, actualizando...")
            actualizar_cache_clientes()
    else:
        logger.info("No se encontró clientes_cache.parquet, descargándolo...")
        actualizar_cache_clientes()

    try:
        table = load_parquet_to_memory()
        if table is None:
            logger.error("No se pudo cargar la tabla de clientes desde el Parquet.")
            return []
        clients = [{col: table[col][i].as_py() for col in table.column_names} for i in range(len(table))]
        cache_set_json('clientes_cache', clients, ex=86400)
        return clients
    except Exception as e:
        logger.error(f"Error al leer clientes desde Parquet después de actualización: {e}", exc_info=True)
        return []

def obtener_productos_cache():
    """Obtiene los productos desde el archivo Parquet en memoria."""
    cached_table = cache_get_table('productos_cache')
    if cached_table is not None:
        return cached_table

    if os.path.exists(CACHE_FILE_PRODUCTOS):
        mod_time = datetime.date.fromtimestamp(os.path.getmtime(CACHE_FILE_PRODUCTOS))
        if mod_time != datetime.date.today():
            logger.info("El archivo de productos está desactualizado, actualizando...")
            actualizar_cache_productos()
    else:
        logger.info("No se encontró productos_cache.parquet, descargándolo...")
        actualizar_cache_productos()

    try:
        table = load_products_to_memory()
        if table is None:
            logger.error("No se pudo cargar la tabla de productos desde el Parquet.")
            return None
        cache_set_table('productos_cache', table, ex=86400)
        return table
    except Exception as e:
        logger.error(f"Error al leer productos desde Parquet: {e}", exc_info=True)
        return None

def actualizar_cache_stock():
    try:
        logger.info("Obteniendo stock para actualizar caché...")
        stock_data = obtener_stock()
        if not stock_data:
            logger.warning("No se encontraron datos de stock para actualizar el caché.")
            return
        data = {key: [item[key] for item in stock_data] for key in stock_data[0].keys()}
        table = pa.Table.from_pydict(data)
        pq.write_table(table, CACHE_FILE_STOCK)
        logger.info("Cache de stock actualizada en formato Parquet.")
        load_stock_to_memory.cache_clear()
        logger.info("Caché de load_stock_to_memory invalidado.")
    except Exception as e:
        logger.error(f"Error al actualizar caché de stock: {e}", exc_info=True)
        enviar_correo_fallo("actualizar_cache_stock", str(e))
        raise

def obtener_stock_cache():
    if os.path.exists(CACHE_FILE_STOCK):
        mod_time = datetime.date.fromtimestamp(os.path.getmtime(CACHE_FILE_STOCK))
        if mod_time == datetime.date.today():
            try:
                table = load_stock_to_memory()
                stock = [{col: table[col][i].as_py() for col in table.column_names} for i in range(len(table))]
                return stock
            except Exception as e:
                logger.error(f"Error al leer caché de stock desde Parquet: {e}", exc_info=True)
    actualizar_cache_stock()
    try:
        table = load_stock_to_memory()
        stock = [{col: table[col][i].as_py() for col in table.column_names} for i in range(len(table))]
        return stock
    except Exception as e:
        logger.error(f"Error al leer caché de stock desde Parquet después de actualización: {e}", exc_info=True)
        return []

def actualizar_cache_empleados():
    try:
        logger.info("Obteniendo empleados para actualizar caché...")
        empleados = obtener_empleados()
        if not empleados:
            logger.warning("No se encontraron empleados para actualizar el caché.")
            return
        data = {key: [item[key] for item in empleados] for key in empleados[0].keys()}
        table = pa.Table.from_pydict(data)
        pq.write_table(table, CACHE_FILE_EMPLEADOS)
        logger.info("Cache de empleados actualizada en formato Parquet.")
    except Exception as e:
        logger.error(f"Error al actualizar caché de empleados: {e}", exc_info=True)
        enviar_correo_fallo("actualizar_cache_empleados", str(e))
        raise

def obtener_empleados_cache():
    if os.path.exists(CACHE_FILE_EMPLEADOS):
        mod_time = datetime.date.fromtimestamp(os.path.getmtime(CACHE_FILE_EMPLEADOS))
        if mod_time == datetime.date.today():
            try:
                table = pq.read_table(CACHE_FILE_EMPLEADOS)
                employees = [{col: table[col][i].as_py() for col in table.column_names} for i in range(len(table))]
                return employees
            except Exception as e:
                logger.error(f"Error al leer caché de empleados desde Parquet: {e}", exc_info=True)
    actualizar_cache_empleados()
    try:
        table = pq.read_table(CACHE_FILE_EMPLEADOS)
        employees = [{col: table[col][i].as_py() for col in table.column_names} for i in range(len(table))]
        return employees
    except Exception as e:
        logger.error(f"Error al leer caché de empleados desde Parquet después de actualización: {e}", exc_info=True)
        return []

def actualizar_cache_atributos():
    try:
        logger.info("Obteniendo todos los atributos para actualizar caché...")
        atributos = obtener_todos_atributos()
        if not atributos:
            logger.warning("No se encontraron atributos para actualizar el caché.")
            return
        data = {key: [item[key] for item in atributos] for key in atributos[0].keys()}
        table = pa.Table.from_pydict(data)
        pq.write_table(table, CACHE_FILE_ATRIBUTOS)
        logger.info("Cache de atributos actualizada en formato Parquet único.")
        load_atributos_to_memory.cache_clear()
        logger.info("Caché de load_atributos_to_memory invalidado.")
    except Exception as e:
        logger.error(f"Error al actualizar caché de atributos: {e}", exc_info=True)
        enviar_correo_fallo("actualizar_cache_atributos", str(e))
        raise

def obtener_atributos_cache(product_id=None):
    if os.path.exists(CACHE_FILE_ATRIBUTOS):
        mod_time = datetime.date.fromtimestamp(os.path.getmtime(CACHE_FILE_ATRIBUTOS))
        if mod_time == datetime.date.today():
            try:
                table = load_atributos_to_memory()
                if product_id:
                    filter_condition = pc.equal(pc.field('ProductNumber'), str(product_id))
                    return table.filter(filter_condition)
                return table
            except Exception as e:
                logger.error(f"Error al leer caché de atributos desde Parquet: {e}", exc_info=True)
    actualizar_cache_atributos()
    try:
        table = load_atributos_to_memory()
        if product_id:
            filter_condition = pc.equal(pc.field('ProductNumber'), str(product_id))
            return table.filter(filter_condition)
        return table
    except Exception as e:
        logger.error(f"Error al leer caché de atributos desde Parquet después de actualización: {e}", exc_info=True)
        return None

# 🔹 Métodos load_*_to_memory()
@lru_cache(maxsize=1)
def load_parquet_to_memory():
    """Carga el archivo clientes_cache.parquet en memoria."""
    try:
        if not os.path.exists(CACHE_FILE_CLIENTES):
            logger.warning("Archivo clientes_cache.parquet no existe, descargándolo...")
            descargar_parquet_clientes()
        return pq.read_table(CACHE_FILE_CLIENTES)
    except Exception as e:
        logger.error(f"Error al cargar clientes_cache.parquet en memoria: {e}", exc_info=True)
        return None

@lru_cache(maxsize=1)
def load_products_to_memory():
    """Carga el archivo productos_cache.parquet en memoria."""
    try:
        if not os.path.exists(CACHE_FILE_PRODUCTOS):
            logger.warning("Archivo productos_cache.parquet no existe, descargándolo...")
            descargar_parquet_productos()
        return pq.read_table(CACHE_FILE_PRODUCTOS)
    except Exception as e:
        logger.error(f"Error al cargar productos_cache.parquet en memoria: {e}", exc_info=True)
        return None

@lru_cache(maxsize=1)
def load_stock_to_memory():
    return pq.read_table(CACHE_FILE_STOCK)

@lru_cache(maxsize=1)
def load_atributos_to_memory():
    return pq.read_table(CACHE_FILE_ATRIBUTOS)

# 🔹 Función para actualizar el token D365
def actualizar_token_d365():
    try:
        token = get_access_token_d365()
        if token:
            guardar_token_d365(token)
            logger.info("Token D365 actualizado exitosamente mediante cron.")
        else:
            raise Exception("No se pudo obtener el token D365.")
    except Exception as e:
        logger.error(f"Error al actualizar token D365 mediante cron: {e}", exc_info=True)
        enviar_correo_fallo("actualizar_token_d365", str(e))

# 🔹 Configuración del Scheduler
scheduler = BackgroundScheduler(job_defaults={'max_instances': 1})

def job_listener(event):
    if event.exception:
        logger.error(f"Error en tarea {event.job_id}: {event.exception}", exc_info=True)
        enviar_correo_fallo(event.job_id, str(event.exception))
    else:
        logger.info(f"Tarea {event.job_id} ejecutada correctamente")

scheduler.add_listener(job_listener, EVENT_JOB_ERROR | EVENT_JOB_EXECUTED)

def log_scheduler_alive():
    logger.info("Scheduler está vivo")

def safe_obtener_datos_clientes():
    with clientes_lock:
        try:
            actualizar_cache_clientes()
        except Exception as e:
            logger.error(f"Error en obtener_datos_clientes: {e}", exc_info=True)
            enviar_correo_fallo("safe_obtener_datos_clientes", str(e))
            raise

def safe_actualizar_cache_clientes():
    try:
        actualizar_cache_clientes()
    except Exception as e:
        logger.error(f"Error en actualizar_cache_clientes: {e}", exc_info=True)
        enviar_correo_fallo("safe_actualizar_cache_clientes", str(e))
        raise

def safe_obtener_atributos_fabric():
    try:
        obtener_atributos_fabric()
        safe_actualizar_cache_atributos()
    except Exception as e:
        logger.error(f"Error en obtener_atributos_fabric: {e}", exc_info=True)
        enviar_correo_fallo("safe_obtener_atributos_fabric", str(e))
        raise

def safe_obtener_empleados_fabric():
    try:
        obtener_empleados_fabric()
    except Exception as e:
        logger.error(f"Error en obtener_empleados_fabric: {e}", exc_info=True)
        enviar_correo_fallo("safe_obtener_empleados_fabric", str(e))
        raise

def safe_actualizar_cache_productos():
    try:
        actualizar_cache_productos()
    except Exception as e:
        logger.error(f"Error en actualizar_cache_productos: {e}", exc_info=True)
        enviar_correo_fallo("safe_actualizar_cache_productos", str(e))
        raise

def safe_obtener_stock_fabric():
    try:
        obtener_stock_fabric()
        safe_actualizar_cache_stock()
    except Exception as e:
        logger.error(f"Error en obtener_stock_fabric: {e}", exc_info=True)
        enviar_correo_fallo("safe_obtener_stock_fabric", str(e))
        raise

def safe_obtener_grupos_cumplimiento_fabric():
    try:
        obtener_grupos_cumplimiento_fabric()
    except Exception as e:
        logger.error(f"Error en obtener_grupos_cumplimiento_fabric: {e}", exc_info=True)
        enviar_correo_fallo("safe_obtener_grupos_cumplimiento_fabric", str(e))
        raise

def safe_obtener_datos_tiendas():
    try:
        obtener_datos_tiendas()
    except Exception as e:
        logger.error(f"Error en obtener_datos_tiendas: {e}", exc_info=True)
        enviar_correo_fallo("safe_obtener_datos_tiendas", str(e))
        raise

def safe_actualizar_cache_stock():
    try:
        actualizar_cache_stock()
    except Exception as e:
        logger.error(f"Error en actualizar_cache_stock: {e}", exc_info=True)
        enviar_correo_fallo("safe_actualizar_cache_stock", str(e))
        raise

def safe_actualizar_cache_atributos():
    try:
        actualizar_cache_atributos()
    except Exception as e:
        logger.error(f"Error en actualizar_cache_atributos: {e}", exc_info=True)
        enviar_correo_fallo("safe_actualizar_cache_atributos", str(e))
        raise

# Agregar tareas al scheduler (sin iniciar aún)
scheduler.add_job(log_scheduler_alive, CronTrigger(minute='*/5'))
scheduler.add_job(safe_obtener_datos_clientes, CronTrigger(minute='*/14'))
scheduler.add_job(safe_actualizar_cache_productos, CronTrigger(minute='*/20'))
scheduler.add_job(safe_obtener_atributos_fabric, CronTrigger(minute='*/30'))
scheduler.add_job(safe_obtener_empleados_fabric, CronTrigger(hour=7))
scheduler.add_job(safe_obtener_stock_fabric, CronTrigger(minute='*/20'))
scheduler.add_job(safe_obtener_grupos_cumplimiento_fabric, CronTrigger(day_of_week="sat", hour=22, minute=0))
scheduler.add_job(safe_obtener_datos_tiendas, CronTrigger(day_of_week="sat", hour=22, minute=30))
scheduler.add_job(actualizar_token_d365, CronTrigger(minute='*/10'))
scheduler.add_job(lambda: facturacion_arca_bp.solicitar_caea(), CronTrigger(day='1-15', hour=0, minute=0))  # Día 1 y 15

# 🔹 Función de configuración inicial
def run_first_time_setup():
    if not os.path.exists(FLAG_FILE) and not os.path.exists(FLAG_FILE_START):
        logger.info("⚡ Primera ejecución detectada. Cargando datos iniciales...")
        try:
            with open(FLAG_FILE_START, 'w') as f:
                f.write('proceso de carga inicializado correctamente.')
            logger.info("✅ Se inicio la primera carga correctamente.")
            actualizar_cache_productos()
            actualizar_cache_clientes()
            obtener_atributos_fabric()
            obtener_stock_fabric()
            obtener_grupos_cumplimiento_fabric()
            obtener_empleados_fabric()
            obtener_datos_tiendas()
            actualizar_cache_stock()
            actualizar_cache_empleados()
            actualizar_cache_atributos()
            actualizar_token_d365()
            with open(FLAG_FILE, 'w') as f:
                f.write('DB inicializada correctamente.')
            logger.info("✅ Datos iniciales y caché actualizados correctamente.")
            scheduler.start()
            logger.info("Scheduler iniciado tras completar la carga inicial.")
            for job in scheduler.get_jobs():
                logger.info(f"Job programado: {job.id}, Trigger: {job.trigger}, Next run: {job.next_run_time}")
        except Exception as e:
            logger.error(f"❌ Error durante la carga inicial: {e}", exc_info=True)
            enviar_correo_fallo("run_first_time_setup", str(e))
    elif os.path.exists(FLAG_FILE):
        scheduler.start()
        logger.info("Scheduler iniciado (carga inicial ya completada previamente).")
        for job in scheduler.get_jobs():
            logger.info(f"Job programado: {job.id}, Trigger: {job.trigger}, Next run: {job.next_run_time}")

# 🔹 Llamar a la configuración inicial
threading.Thread(target=run_first_time_setup, daemon=True).start()

# 🔹 Definir rutas
@app.route('/')
def root():
    if not os.path.exists(FLAG_FILE):
        return "La aplicación se está inicializando, por favor espera unos momentos y recarga la página.", 200
    if 'usuario' in session:
        session.permanent = True
        last_store = session.get('last_store')
        if last_store:
            return redirect(url_for('productos', store_id=last_store))
        return redirect(url_for('productos'))
    return redirect(url_for('autenticacion_avanzada.login_avanzado'))

@app.route('/productos')
@login_required
def productos():
    if not session.get('empleado_d365') or session.get('empleado_d365') == "":
        logger.warning("ID empleado no está presente")
        logout()  # Cierra la sesión
        flash("Inicia sesión nuevamente. Los datos de ID empleado no están presentes", "error")
        return redirect(url_for('autenticacion_avanzada.login_avanzado'))
    stores = obtener_stores_from_parquet()
    last_store = session.get('last_store', 'BA001GC')
    return render_template('index.html', stores=stores, last_store=last_store)


@app.route('/presupuestos')
@login_required
def presupuestos_page():
    """Página para gestionar presupuestos y carritos."""
    return render_template('presupuestos.html')

@app.route('/config/secuencias')
@login_required
def config_secuencias():
    if session.get('role') != 'admin':
        flash("Acceso denegado: Solo administradores pueden configurar secuencias.", "danger")
        logger.warning(f"Acceso denegado a /config/secuencias: {session.get('email')}")
        return redirect(url_for('productos'))
    return render_template('config_secuencias.html')


@app.route('/config/pos')
@login_required
def config_pos():
    if session.get('role') != 'admin':
        flash("Acceso denegado: Solo administradores pueden configurar POS.", "danger")
        logger.warning(f"Acceso denegado a /config/pos: {session.get('email')}")
        return redirect(url_for('productos'))
    return render_template('config_pos.html')


@app.route('/api/config_pos/<tienda_id>/<pto_venta_id>')
def obtener_config_pos(tienda_id, pto_venta_id):
    config = get_config_pos_by_ids(tienda_id, pto_venta_id)
    if not config:
        return jsonify({'error': 'Configuración no encontrada'}), 404
    return jsonify(config)
  

@app.route('/presupuestos')
@login_required
def presupuestos():
    """Página simple para gestionar búsqueda y recuperación de presupuestos."""
    return render_template('presupuestos.html')


@app.route('/api/stock/<codigo>/<store>')
def api_stock_codigo_store(codigo, store):
    try:
        if not codigo or not store:
            return jsonify({"mensaje": "Código de producto y tienda son requeridos."}), 400
        if not os.path.exists(CACHE_FILE_STOCK):
            logger.error(f"El archivo Parquet no existe en la ruta: {CACHE_FILE_STOCK}")
            return jsonify({"error": f"Archivo de caché no encontrado: {CACHE_FILE_STOCK}"}), 500
        logger.info(f"Intentando leer el archivo Parquet de stock desde: {CACHE_FILE_STOCK}")
        almacenes_asignados = obtener_grupos_cumplimiento(store)
        almacenes_asignados = [almacen.strip().upper() for almacen in almacenes_asignados]
        if not almacenes_asignados:
            return jsonify({"mensaje": f"No hay almacenes asignados a la tienda {store}."}), 404
        codigo_normalizado = str(codigo).strip().upper()
        table = load_stock_to_memory()
        codigo_filter = pc.match_substring(pc.field('codigo'), codigo_normalizado)
        almacenes_filter = pc.field('almacen_365').isin(almacenes_asignados)
        combined_filter = pc.and_kleene(codigo_filter, almacenes_filter)
        filtered_table = table.filter(combined_filter)
        import pandas as pd
        df = filtered_table.to_pandas()
        stock_filtrado = df.to_dict('records')
        almacenes_con_stock = {s["almacen_365"].strip().upper() for s in stock_filtrado}
        for almacen in almacenes_asignados:
            if almacen not in almacenes_con_stock:
                stock_filtrado.append({
                    "codigo": codigo_normalizado,
                    "almacen_365": almacen,
                    "stock_fisico": 0.00,
                    "disponible_venta": 0.00,
                    "disponible_entrega": 0.00,
                    "comprometido": 0.00
                })
        logger.info(f"Se encontraron {len(stock_filtrado)} registros de stock para el código '{codigo_normalizado}' y store '{store}'.")
        return jsonify(stock_filtrado)
    except Exception as e:
        logger.error(f"Error en búsqueda de stock desde Parquet: {e}", exc_info=True)
        return jsonify({"error": f"Error interno: {str(e)}"}), 500


@app.route('/api/stock_categoria/<categoria_id>')
def api_stock_categoria(categoria_id):
    """Endpoint que retorna el stock de una categoría."""
    try:
        datos = obtener_stock_categoria(categoria_id)
        return jsonify(datos)
    except Exception as e:
        logger.error(f"Error al obtener stock por categoría: {e}", exc_info=True)
        return jsonify({"error": f"Error interno: {str(e)}"}), 500


@app.route('/api/lista_precios/<sucursal_id>')
def api_lista_precios_sucursal(sucursal_id):
    """Endpoint que retorna la lista de precios de una sucursal."""
    try:
        precios = obtener_lista_precios_sucursal(sucursal_id)
        return jsonify(precios)
    except Exception as e:
        logger.error(f"Error al obtener lista de precios: {e}", exc_info=True)
        return jsonify({"error": f"Error interno: {str(e)}"}), 500


@app.route('/stock/<sucursal_id>')
def stock_view(sucursal_id):
    """Renderiza la página de stock con las listas de precios por sucursal."""
    precios = obtener_lista_precios_sucursal(sucursal_id)
    return render_template('stock.html', precios=precios, sucursal_id=sucursal_id)

@app.route('/api/update_last_store', methods=['POST'])
@login_required
def update_last_store():
    try:
        data = request.get_json()
        store_id = data.get('store_id')
        if not store_id:
            return jsonify({"error": "store_id es requerido"}), 400

        email = session.get('email')
        if not email:
            return jsonify({"error": "No se encontró email en la sesión"}), 401

        actualizar_last_store(email, store_id)
        session['last_store'] = store_id
        logger.info(f"Last_store actualizado a {store_id} para {email}")
        return jsonify({"message": "Last_store actualizado correctamente"}), 200
    except Exception as e:
        logger.error(f"Error al actualizar last_store: {e}", exc_info=True)
        enviar_correo_fallo("update_last_store", str(e))
        return jsonify({"error": str(e)}), 500

import time
@app.route('/producto/atributos/<int:product_id>')
@login_required
def obtener_atributos_producto(product_id):
    logger.info(f"Obteniendo atributos para producto {product_id}")
    FLAG_FILE = os.path.join(os.path.dirname(__file__), 'flag_file.txt')
    try:
        if os.path.exists(FLAG_FILE):
            table = obtener_atributos_cache(product_id=product_id)
            if table is None or table.num_rows == 0:
                logger.error(f"No se encontraron atributos para el producto {product_id} en la caché.")
                atributos = []
            else:
                import pandas as pd
                df = table.to_pandas()
                atributos = df.to_dict('records')
        else:
            atributos = obtener_atributos(product_id)

        # Obtener nombre del producto desde el caché si no hay atributos
        if not atributos:
            producto = obtener_producto_por_id(product_id)
            product_name = producto.get('nombre_producto', 'Producto desconocido') if producto else 'Producto desconocido'
        else:
            product_name = atributos[0].get('ProductName', atributos[0].get('product_name', 'Producto desconocido'))

        response = {
            "product_name": product_name,
            "product_number": str(product_id),
            "attributes": atributos
        }
        logger.info(f"Atributos obtenidos para producto {product_id}: {len(atributos)} atributos")
        print(response)
        return jsonify(response)
    except Exception as e:
        logger.error(f"Error al obtener atributos para producto {product_id}: {str(e)}", exc_info=True)
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({'error': f'Error al cargar atributos: {str(e)}'}), 500
        raise

@app.route('/api/productos')
@login_required
def api_productos():
    try:
        store = request.args.get('store', 'BA001GC').strip()
        page = int(request.args.get('page', 1))
        items_per_page = int(request.args.get('items_per_page', 200000))
        offset = (page - 1) * items_per_page

        if not os.path.exists(CACHE_FILE_PRODUCTOS):
            logger.error(f"El archivo Parquet no existe en la ruta: {CACHE_FILE_PRODUCTOS}")
            return jsonify({"error": f"Archivo de caché no encontrado: {CACHE_FILE_PRODUCTOS}"}), 500

        logger.info(f"Intentando leer el archivo Parquet de productos desde: {CACHE_FILE_PRODUCTOS}")
        table = obtener_productos_cache()
        if table is None:
            return jsonify({"error": "No se pudo cargar los productos desde el archivo Parquet"}), 500

        store_filter = pc.match_substring(pc.field('store_number'), store)
        filtered_table = table.filter(store_filter)

        column_mapping = {
            'Número de Producto': 'numero_producto',
            'Nombre de Categoría de Producto': 'categoria_producto',
            'Nombre del Producto': 'nombre_producto',
            'Grupo de Cobertura': 'grupo_cobertura',
            'Unidad de Medida': 'unidad_medida',
            'PrecioFinalConIVA': 'precio_final_con_iva',
            'PrecioFinalConDescE': 'precio_final_con_descuento',
            'StoreNumber': 'store_number',
            'TotalDisponibleVenta': 'total_disponible_venta',
            'Signo': 'signo',
            'Multiplo': 'multiplo'
        }
        renamed_table = filtered_table.rename_columns([column_mapping.get(col, col) for col in filtered_table.column_names])

        import pandas as pd
        df = renamed_table.to_pandas()
        df['precio_final_con_iva'] = df['precio_final_con_iva'].apply(lambda x: f"{x:,.2f}".replace(".", "X").replace(",", ".").replace("X", ","))
        df['precio_final_con_descuento'] = df['precio_final_con_descuento'].apply(lambda x: f"{x:,.2f}".replace(".", "X").replace(",", ".").replace("X", ","))
        df['total_disponible_venta'] = df['total_disponible_venta'].apply(lambda x: f"{x:,.2f}".replace(".", "X").replace(",", ".").replace("X", ","))

        # Generar URL de imagen protegida para cada producto
        df['imagen_url'] = df['numero_producto'].apply(lambda x: url_for('serve_image', filename=f"{x}.jpg"))

        paginated_products = df[offset:offset + items_per_page].to_dict('records')
        logger.info(f"Se encontraron {len(df)} productos para el store '{store}', paginados {len(paginated_products)}.")
        return jsonify(paginated_products)
    except Exception as e:
        logger.error(f"Error en búsqueda de productos desde Parquet: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route('/api/create_quotation', methods=['POST'])
@login_required
def create_quotation():
    try:
        data = request.get_json()
        logger.info(f"Datos recibidos: {data}")
        cart = data.get('cart', {})
        store_id = data.get('store_id', '')
        tipo_presupuesto = data.get('tipo_presupuesto', 'Caja')
        observaciones = cart.get('observations', '')

        if not session.get('empleado_d365') or session.get('empleado_d365') == "":
            logger.warning("ID empleado no está presente")
            logout()  # Cierra la sesión
            flash("Inicia sesión nuevamente. Los datos de ID empleado no están presentes", "error")
            return redirect(url_for('autenticacion_avanzada.login_avanzado'))

        if not cart.get('client') or not cart['client'].get('numero_cliente'):
            logger.warning("Cliente no seleccionado")
            return jsonify({"error": "Debe seleccionar un cliente para generar el presupuesto"}), 400

        items = [item for item in cart.get('items', []) if item.get('productId')]
        if not items:
            logger.warning("Carrito vacío")
            return jsonify({"error": "El carrito está vacío"}), 400

        access_token = obtener_token_d365()
        if not access_token:
            logger.error("No se pudo obtener token D365 desde la base de datos")
            enviar_correo_fallo("create_quotation", "No se pudo obtener token D365 desde la base de datos")
            return jsonify({"error": "No se pudo obtener token"}), 500

        tienda = obtener_datos_tienda_por_id(store_id)
        logger.info(f"Datos de tienda para {store_id}: {tienda}")
        if not tienda:
            logger.error(f"Tienda {store_id} no encontrada")
            return jsonify({"error": f"Tienda {store_id} no encontrada"}), 404

        datos_cabecera = {
            "tipo_presupuesto": tipo_presupuesto,
            "sitio": tienda.get('sitio_almacen_retiro', ''),
            "almacen_retiro": tienda.get('almacen_retiro', ''),
            "id_cliente": cart['client']['numero_cliente'],
            "id_empleado": session.get('empleado_d365', ''),
            "store_id": store_id,
            "id_direccion": tienda.get('direccion_unidad_operativa', ''),
            "observaciones": observaciones
        }
        logger.info(f"Datos cabecera preparados: {datos_cabecera}")

        lineas = []
        for item in items:
            precio_con_iva = float(item['precioLista'])
            precio_con_descuento = float(item['price'])
            cantidad = float(item['quantity'])
            descuento = ((precio_con_iva - precio_con_descuento) / precio_con_iva) * 100 if precio_con_iva != 0 else 0
            descuento_positivo = round(abs(descuento), 2)
            precio_sin_iva = round(precio_con_descuento / 1.21, 2)
            cantidad_redondeada = int(cantidad) if cantidad.is_integer() else round(cantidad, 2)

            lineas.append({
                "articulo": item['productId'],
                "cantidad": cantidad_redondeada,
                "precio": precio_sin_iva,
                "descuento": descuento_positivo,
                "unidad_medida": item.get('unidadMedida', 'Un'),
                "sitio": tienda.get('sitio_almacen_retiro', ''),
                "almacen_entrega": tienda.get('almacen_retiro', '')
            })
        logger.info(f"Líneas preparadas: {lineas}")

        quotation_number, error = run_crear_presupuesto_batch(datos_cabecera, lineas, access_token)
        if not quotation_number:
            logger.error(f"Error al crear el presupuesto: {error}")
            return jsonify({"error": error}), 500

        logger.info(f"Presupuesto creado: {quotation_number}")
        guardar_numero_presupuesto(quotation_number)
        guardar_presupuesto_local(quotation_number)
        return jsonify({"quotation_number": quotation_number}), 201

    except Exception as e:
        logger.error(f"Error creando presupuesto: {str(e)}", exc_info=True)
        enviar_correo_fallo("create_quotation", str(e))
        return jsonify({"error": str(e)}), 500

@app.route('/api/update_quotation/<quotation_id>', methods=['PUT'])
@login_required
def update_quotation(quotation_id):
    try:
        if not quotation_id.startswith('VENT1-'):
            return jsonify({"error": "ID de presupuesto D365 inválido"}), 400

        data = request.get_json()
        logger.info(f"Datos recibidos para actualizar presupuesto {quotation_id}: {data}")
        cart = data.get('cart', {})
        store_id = data.get('store_id', '')
        tipo_presupuesto = data.get('tipo_presupuesto', 'Caja')
        observaciones = cart.get('observations', '')

        if not session.get('empleado_d365') or session.get('empleado_d365') == "":
            logger.warning("ID empleado no está presente")
            logout()
            flash("Inicia sesión nuevamente. Los datos de ID empleado no están presentes", "error")
            return redirect(url_for('autenticacion_avanzada.login_avanzado'))

        if not cart.get('client') or not cart['client'].get('numero_cliente'):
            logger.warning("Cliente no seleccionado")
            return jsonify({"error": "Debe seleccionar un cliente para actualizar el presupuesto"}), 400

        items = [item for item in cart.get('items', []) if item.get('productId')]
        if not items:
            logger.warning("Carrito vacío")
            return jsonify({"error": "El carrito está vacío"}), 400

        access_token = obtener_token_d365()
        if not access_token:
            logger.error("No se pudo obtener token D365 desde la base de datos")
            enviar_correo_fallo("update_quotation", "No se pudo obtener token D365 desde la base de datos")
            return jsonify({"error": "No se pudo obtener token"}), 500

        logger.info(f"Obteniendo presupuesto D365 con ID: {quotation_id} para obtener líneas existentes")
        presupuesto_data, error = run_obtener_presupuesto_d365(quotation_id, access_token)
        if error:
            logger.error(f"Error al recuperar presupuesto D365 {quotation_id}: {error}")
            if "no encontrado" in error.lower():
                return jsonify({"error": error}), 404
            return jsonify({"error": error}), 500

        lineas_existentes = presupuesto_data["lines"]
        logger.info(f"Líneas existentes para {quotation_id}: {lineas_existentes}")

        tienda = obtener_datos_tienda_por_id(store_id)
        logger.info(f"Datos de tienda para {store_id}: {tienda}")
        if not tienda:
            logger.error(f"Tienda {store_id} no encontrada")
            return jsonify({"error": f"Tienda {store_id} no encontrada"}), 404

        fecha_actual = datetime.datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        fecha_expiracion = (datetime.datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        datos_cabecera = {
            "tipo_presupuesto": tipo_presupuesto,
            "sitio": tienda.get('sitio_almacen_retiro', ''),
            "almacen_retiro": tienda.get('almacen_retiro', ''),
            "id_cliente": cart['client']['numero_cliente'],
            "id_empleado": session.get('empleado_d365', ''),
            "store_id": store_id,
            "id_direccion": tienda.get('direccion_unidad_operativa', ''),
            "observaciones": observaciones,
            "ReceiptDateRequested": fecha_actual,
            "RequestedShippingDate": fecha_actual,
            "SalesQuotationExpiryDate": fecha_expiracion
        }
        logger.info(f"Datos cabecera preparados: {datos_cabecera}")

        lineas_nuevas = []
        for item in items:
            precio_con_iva = float(item['precioLista'])
            precio_con_descuento = float(item['price'])
            cantidad = float(item['quantity'])
            descuento = ((precio_con_iva - precio_con_descuento) / precio_con_iva) * 100 if precio_con_iva != 0 else 0
            descuento_positivo = round(abs(descuento), 2)
            precio_sin_iva = round(precio_con_descuento / 1.21, 2)
            cantidad_redondeada = int(cantidad) if cantidad.is_integer() else round(cantidad, 2)

            lineas_nuevas.append({
                "articulo": item['productId'],
                "cantidad": cantidad_redondeada,
                "precio": precio_sin_iva,
                "descuento": descuento_positivo,
                "unidad_medida": item.get('unidadMedida', 'Un'),
                "sitio": tienda.get('sitio_almacen_retiro', ''),
                "almacen_entrega": tienda.get('almacen_retiro', '')
            })
        logger.info(f"Líneas nuevas preparadas: {lineas_nuevas}")

        quotation_number, error = run_actualizar_presupuesto_d365(quotation_id, datos_cabecera, lineas_nuevas, lineas_existentes, access_token)
        if not quotation_number:
            logger.error(f"Error al actualizar el presupuesto {quotation_id}: {error}")
            return jsonify({"error": error}), 500

        logger.info(f"Presupuesto {quotation_id} actualizado exitosamente")
        guardar_numero_presupuesto(quotation_number)
        return jsonify({"quotation_number": quotation_number}), 200

    except Exception as e:
        logger.error(f"Error al actualizar presupuesto {quotation_id}: {str(e)}", exc_info=True)
        enviar_correo_fallo("update_quotation", str(e))
        return jsonify({"error": str(e)}), 500

@app.route('/api/datos_tienda/<store_id>')
@login_required
def get_store_data(store_id):
    try:
        tienda = obtener_datos_tienda_por_id(store_id)
        if not tienda:
            return jsonify({"error": "Tienda no encontrada"}), 404
        return jsonify(tienda), 200
    except Exception as e:
        logger.error(f"Error al obtener datos de tienda {store_id}: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/user_info')
@login_required
def get_user_info():
    try:
        email = session.get('email')
        if not email:
            logger.error("No se encontró email en la sesión")
            return jsonify({"error": "Usuario no autenticado"}), 401
        logger.info(f"Buscando empleado para email: {email}")
        empleado = obtener_empleados_by_email(email)
        logger.info(f"Empleado encontrado: {empleado}")
        response_data = {
            "nombre_completo": empleado.get('nombre_completo', 'Usuario desconocido') if empleado else 'Usuario desconocido',
            "email": email
        }
        return jsonify(response_data), 200
    except Exception as e:
        logger.error(f"Error al obtener info del usuario: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/check_products_update')
@login_required
def check_products_update():
    try:
        if os.path.exists(CACHE_FILE_PRODUCTOS):
            last_modified = os.path.getmtime(CACHE_FILE_PRODUCTOS)
            return jsonify({"last_modified": last_modified}), 200
        else:
            logger.warning("El archivo productos_cache.parquet no existe aún.")
            return jsonify({"last_modified": 0}), 200
    except Exception as e:
        logger.error(f"Error al verificar actualización de productos: {e}", exc_info=True)
        enviar_correo_fallo("check_products_update", str(e))
        return jsonify({"error": str(e)}), 500

@app.route('/api/generate_pdf_quotation_id', methods=['GET'])
@login_required
def generate_pdf_quotation_id():
    try:
        contador = obtener_contador_pdf()
        quotation_id = f"P-{str(contador).zfill(9)}"
        logger.info(f"ID de presupuesto PDF generado: {quotation_id}")
        return jsonify({"quotation_id": quotation_id}), 200
    except Exception as e:
        logger.error(f"Error al generar ID de presupuesto PDF: {e}", exc_info=True)
        enviar_correo_fallo("generate_pdf_quotation_id", str(e))
        return jsonify({"error": str(e)}), 500

@app.route('/api/clientes/create', methods=['POST'])
@login_required
def create_client():
    try:
        data = request.get_json()
        required_fields = ['nombre', 'apellido', 'dni', 'email', 'telefono', 'codigo_postal', 'ciudad', 'estado',
                           'condado', 'calle', 'altura']
        for field in required_fields:
            if not data.get(field):
                return jsonify({"error": f"El campo {field} es requerido"}), 400

        access_token = obtener_token_d365()
        if not access_token:
            return jsonify({"error": "No se pudo obtener token D365"}), 500

        customer_id, error = run_alta_cliente_d365(data, access_token)
        if error:
            return jsonify({"error": error}), 500

        actualizar_cache_clientes()
        return jsonify({"customer_id": customer_id, "message": "Cliente creado exitosamente"}), 201
    except Exception as e:
        logger.error(f"Error al crear cliente: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route('/api/productos/by_code')
@login_required
def api_productos_by_code():
    try:
        code = request.args.get('code', '').strip()
        store = request.args.get('store', '').strip()
        if not code:
            return jsonify({"error": "Código es requerido"}), 400

        if not os.path.exists(CACHE_FILE_PRODUCTOS):
            logger.error(f"El archivo Parquet no existe en la ruta: {CACHE_FILE_PRODUCTOS}")
            return jsonify({"error": f"Archivo de caché no encontrado: {CACHE_FILE_PRODUCTOS}"}), 500

        logger.info(f"Buscando producto exacto con código: {code}" + (f" y store: {store}" if store else ""))
        table = obtener_productos_cache()
        if table is None:
            return jsonify({"error": "No se pudo cargar los productos desde el archivo Parquet"}), 500

        column_mapping = {
            'Número de Producto': 'numero_producto',
            'Nombre de Categoría de Producto': 'categoria_producto',
            'Nombre del Producto': 'nombre_producto',
            'Grupo de Cobertura': 'grupo_cobertura',
            'Unidad de Medida': 'unidad_medida',
            'PrecioFinalConIVA': 'precio_final_con_iva',
            'PrecioFinalConDescE': 'precio_final_con_descuento',
            'StoreNumber': 'store_number',
            'TotalDisponibleVenta': 'total_disponible_venta',
            'Signo': 'signo',
            'Multiplo': 'multiplo'
        }
        renamed_table = table.rename_columns([column_mapping.get(col, col) for col in table.column_names])

        code_filter = pc.equal(pc.field('numero_producto'), code)
        filtered_table = renamed_table.filter(code_filter)
        if store:
            store_filter = pc.equal(pc.field('store_number'), store)
            filtered_table = filtered_table.filter(store_filter)

        import pandas as pd
        df = filtered_table.to_pandas()
        df['precio_final_con_iva'] = df['precio_final_con_iva'].apply(lambda x: f"{x:,.2f}".replace(".", "X").replace(",", ".").replace("X", ","))
        df['precio_final_con_descuento'] = df['precio_final_con_descuento'].apply(lambda x: f"{x:,.2f}".replace(".", "X").replace(",", ".").replace("X", ","))
        df['total_disponible_venta'] = df['total_disponible_venta'].apply(lambda x: f"{x:,.2f}".replace(".", "X").replace(",", ".").replace("X", ","))

        products = df.to_dict('records')

        if not products:
            return jsonify({"message": f"No se encontró producto con código {code}" + (f" en store {store}" if store else "")}), 404

        logger.info(f"Producto encontrado: {len(products)} resultados para el código {code}")
        return jsonify(products), 200
    except Exception as e:
        logger.error(f"Error en búsqueda de producto por código: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route('/api/sap/productos/search')
@login_required
def api_sap_productos_search():
    """Endpoint para buscar productos almacenados desde SAP."""
    try:
        query = request.args.get('query', '').strip()
        productos = buscar_productos_sap(query) if query else []
        return jsonify(productos), 200
    except Exception as e:
        logger.error(f"Error en búsqueda de productos SAP: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route('/api/sap/productos/<codigo>')
@login_required
def api_sap_producto_by_code(codigo):
    """Obtiene un producto específico desde la base SAP persistida."""
    try:
        producto = obtener_producto_sap(codigo)
        if producto:
            return jsonify(producto), 200
        return jsonify({"error": "Producto no encontrado"}), 404
    except Exception as e:
        logger.error(f"Error obteniendo producto SAP por código: {e}", exc_info=True)
        
        
@app.route('/api/index_products', methods=['POST'])
@login_required
def api_index_products():
    """Indexa productos en MongoDB para búsquedas rápidas."""
    try:
        table = obtener_productos_cache()
        if table is None:
            return jsonify({"error": "No se pudo cargar los productos"}), 500
        import pandas as pd
        df = table.select(['Número de Producto', 'Nombre del Producto']).to_pandas()
        productos = (
            {"sku": row['Número de Producto'], "descripcion": row['Nombre del Producto']}
            for _, row in df.iterrows()
        )
        count = indexar_productos(list(productos))
        return jsonify({"indexed": count}), 200
    except Exception as e:
        logger.error(f"Error al indexar productos: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route('/api/search_products_index')
@login_required
def api_search_products_index():
    """Busca productos en el índice de Mongo por SKU o descripción."""
    term = request.args.get('q', '').strip()
    try:
        results = buscar_productos(term)
        return jsonify(results), 200
    except Exception as e:
        logger.error(f"Error al buscar productos en índice: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route('/api/save_local_quotation', methods=['POST'])
@login_required
def save_local_quotation():
    try:
        data = request.get_json()
        quotation_id = data.get('quotation_id')
        if not quotation_id or not quotation_id.startswith('P-'):
            logger.error(f"ID de presupuesto local inválido: {quotation_id}")
            return jsonify({"error": "ID de presupuesto local inválido"}), 400

        if 'items' not in data or not isinstance(data['items'], list):
            logger.warning(f"Campo 'items' inválido o ausente en el presupuesto {quotation_id}. Inicializando como lista vacía.")
            data['items'] = []

        if 'timestamp' not in data:
            data['timestamp'] = datetime.datetime.now(timezone.utc).isoformat()
        if 'type' not in data:
            data['type'] = 'local'
        if 'store_id' not in data:
            data['store_id'] = session.get('last_store', 'BA001GC')
        if 'client' not in data:
            data['client'] = None
        if 'observations' not in data:
            data['observations'] = ''

        local_quotations_dir = os.path.join(BASE_DIR, 'quotations/local')
        os.makedirs(local_quotations_dir, exist_ok=True)

        file_path = os.path.join(local_quotations_dir, f"{quotation_id}.json")
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        logger.info(f"Presupuesto local guardado: {file_path}")
        return jsonify({"message": f"Presupuesto {quotation_id} guardado correctamente"}), 200
    except Exception as e:
        logger.error(f"Error al guardar presupuesto local: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route('/api/local_quotations', methods=['GET'])
@login_required
def get_local_quotations():
    try:
        local_quotations_dir = os.path.join(BASE_DIR, 'quotations/local')
        if not os.path.exists(local_quotations_dir):
            return jsonify([]), 200

        quotations = []
        for filename in os.listdir(local_quotations_dir):
            if filename.endswith('.json'):
                with open(os.path.join(local_quotations_dir, filename), 'r', encoding='utf-8') as f:
                    quotation = json.load(f)
                    quotations.append({
                        "quotation_id": quotation["quotation_id"],
                        "timestamp": quotation["timestamp"],
                        "client_name": quotation["client"]["nombre_cliente"] if quotation["client"] else "Sin cliente"
                    })
        return jsonify(quotations), 200
    except Exception as e:
        logger.error(f"Error al listar presupuestos locales: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route('/api/local_quotation/<quotation_id>', methods=['GET'])
@login_required
def get_local_quotation(quotation_id):
    try:
        file_path = os.path.join(BASE_DIR, 'quotations/local', f"{quotation_id}.json")
        if not os.path.exists(file_path):
            return jsonify({"error": f"Presupuesto {quotation_id} no encontrado"}), 404

        with open(file_path, 'r', encoding='utf-8') as f:
            quotation = json.load(f)
        print(jsonify(quotation).json)
        return jsonify(quotation), 200
    except Exception as e:
        logger.error(f"Error al recuperar presupuesto local {quotation_id}: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route('/api/d365_quotation/<quotation_id>', methods=['GET'])
@login_required
def get_d365_quotation(quotation_id):
    try:
        if not quotation_id.startswith('VENT1-'):
            return jsonify({"error": "ID de presupuesto D365 inválido"}), 400

        access_token = obtener_token_d365()
        if not access_token:
            logger.error("No se pudo obtener token D365")
            return jsonify({"error": "No se pudo obtener token D365"}), 500

        logger.info(f"Obteniendo presupuesto D365 con ID: {quotation_id}")
        presupuesto_data, error = run_obtener_presupuesto_d365(quotation_id, access_token)
        if error:
            logger.error(f"Error al recuperar presupuesto D365 {quotation_id}: {error}")
            if "no encontrado" in error.lower():
                return jsonify({"error": error}), 404
            return jsonify({"error": error}), 500

        header_data = presupuesto_data["header"]
        lines_data = presupuesto_data["lines"]
        logger.info(f"Presupuesto D365 {quotation_id} obtenido: header_data={header_data}, lines_data={lines_data}")

        if not header_data:
            return jsonify({"error": f"No se encontró cabecera para el presupuesto {quotation_id}"}), 404

        numero_cliente = header_data.get("InvoiceCustomerAccountNumber", "N/A")
        client_info = None
        try:
            logger.info(f"Buscando cliente con numero_cliente={numero_cliente}")
            if not os.path.exists(CACHE_FILE_CLIENTES):
                logger.error(f"El archivo Parquet no existe en la ruta: {CACHE_FILE_CLIENTES}")
            else:
                table = load_parquet_to_memory()
                column_mapping = {
                    'Bloqueado': 'bloqueado',
                    'Tipo_Contribuyente': 'tipo_contribuyente',
                    'Numero_Cliente': 'numero_cliente',
                    'Nombre_Cliente': 'nombre_cliente',
                    'Limite_Credito': 'limite_credito',
                    'Grupo_Impuestos': 'grupo_impuestos',
                    'NIF': 'nif',
                    'TIF': 'tif',
                    'Direccion_Completa': 'direccion_completa',
                    'Fecha_Modificacion': 'fecha_modificacion',
                    'Fecha_Creacion': 'fecha_creacion',
                    'EmailContacto': 'email_contacto',
                    'TelefonoContacto': 'telefono_contacto'
                }
                renamed_table = table.rename_columns([column_mapping.get(col, col) for col in table.column_names])
                numero_cliente_filter = pc.equal(pc.field('numero_cliente'), numero_cliente)
                filtered_table = renamed_table.filter(numero_cliente_filter)
                import pandas as pd
                df = filtered_table.to_pandas()
                filtered_clients = df.to_dict('records')
                client_info = filtered_clients[0] if filtered_clients else None
                if client_info:
                    logger.info(f"Cliente encontrado: {client_info}")
                else:
                    logger.info(f"Cliente con numero_cliente={numero_cliente} no encontrado")
        except Exception as e:
            logger.error(f"Error al buscar cliente con numero_cliente={numero_cliente}: {e}", exc_info=True)

        sales_order_origin = header_data.get("SalesOrderOriginCode")
        selected_store = request.args.get('store', sales_order_origin if sales_order_origin else "BA001GC")
        logger.info(f"Usando store_number={selected_store} para buscar productos")

        quotation_data = {
            "quotation_id": quotation_id,
            "type": "d365",
            "store_id": selected_store,
            "client": {
                "numero_cliente": numero_cliente,
                "nombre_cliente": client_info["nombre_cliente"] if client_info else "Cliente D365",
                "nif": client_info["nif"] if client_info else "N/A",
                "direccion_completa": client_info["direccion_completa"] if client_info else "N/A",
                "bloqueado": client_info["bloqueado"] if client_info else "N/A",
                "tipo_contribuyente": client_info["tipo_contribuyente"] if client_info else "N/A",
                "limite_credito": client_info["limite_credito"] if client_info else None,
                "grupo_impuestos": client_info["grupo_impuestos"] if client_info else "N/A",
                "tif": client_info["tif"] if client_info else "N/A",
                "email_contacto": client_info["email_contacto"] if client_info else "N/A",
                "telefono_contacto": client_info["telefono_contacto"] if client_info else "N/A",
                "fecha_creacion": client_info["fecha_creacion"] if client_info else "N/A",
                "fecha_modificacion": client_info["fecha_modificacion"] if client_info else "N/A"
            },
            "items": [],
            "observations": header_data.get("CustomersReference", ""),
            "timestamp": header_data.get("ReceiptDateRequested", datetime.datetime.now(timezone.utc).isoformat()),
            "has_flete": False,
            "header": {
                "SalesQuotationStatus": header_data.get("SalesQuotationStatus", ""),
                "GeneratedSalesOrderNumber": header_data.get("GeneratedSalesOrderNumber", "")
            }
        }

        has_flete = False
        unique_items = {}
        for line in lines_data:
            logger.info(f"Procesando línea: {line}")
            product = None
            try:
                logger.info(f"Buscando producto con numero_producto={line['ItemNumber']} en store {selected_store}")
                if not os.path.exists(CACHE_FILE_PRODUCTOS):
                    logger.error(f"El archivo Parquet no existe en la ruta: {CACHE_FILE_PRODUCTOS}")
                else:
                    table = load_products_to_memory()
                    column_mapping = {
                        'Número de Producto': 'numero_producto',
                        'Nombre de Categoría de Producto': 'categoria_producto',
                        'Nombre del Producto': 'nombre_producto',
                        'Grupo de Cobertura': 'grupo_cobertura',
                        'Unidad de Medida': 'unidad_medida',
                        'PrecioFinalConIVA': 'precio_final_con_iva',
                        'PrecioFinalConDescE': 'precio_final_con_descuento',
                        'StoreNumber': 'store_number',
                        'TotalDisponibleVenta': 'total_disponible_venta',
                        'Signo': 'signo',
                        'Multiplo': 'multiplo'
                    }
                    renamed_table = table.rename_columns([column_mapping.get(col, col) for col in table.column_names])
                    code_filter = pc.equal(pc.field('numero_producto'), line["ItemNumber"])
                    filtered_table = filtered_table.filter(code_filter)
                    store_filter = pc.equal(pc.field('store_number'), selected_store)
                    filtered_table = filtered_table.filter(store_filter)
                    import pandas as pd
                    df = filtered_table.to_pandas()
                    products = df.to_dict('records')
                    product = products[0] if products else None
                    if product:
                        logger.info(f"Producto encontrado: {product}")
                    else:
                        logger.info(f"Producto con numero_producto={line['ItemNumber']} no encontrado en store {selected_store}")
            except Exception as e:
                logger.error(f"Error al buscar producto con numero_producto={line['ItemNumber']}: {e}", exc_info=True)

            price = 0.0
            precio_lista = 0.0
            if product:
                price = float(product["precio_final_con_descuento"])
                precio_lista = float(product["precio_final_con_iva"])
            else:
                price = float(line.get("SalesPrice", 0))
                precio_lista = price

            item = {
                "productId": line["ItemNumber"],
                "productName": product["nombre_producto"] if product else line["ItemNumber"],
                "price": f"{price:,.2f}".replace(".", "X").replace(",", ".").replace("X", ","),
                "precioLista": f"{precio_lista:,.2f}".replace(".", "X").replace(",", ".").replace("X", ","),
                "quantity": float(line.get("RequestedSalesQuantity", 0)),
                "multiplo": float(product["multiplo"]) if product else 1,
                "unidadMedida": line.get("SalesUnitSymbol", product["unidad_medida"] if product else "Un")
            }

            product_name = item["productName"].lower()
            product_id = item["productId"]
            if "flete" in product_name or product_id == "350320":
                has_flete = True
                logger.info(f"Producto excluido por ser flete: {item['productName']} (ID: {item['productId']})")
                continue

            if item["productId"] in unique_items:
                unique_items[item["productId"]]["quantity"] += item["quantity"]
            else:
                unique_items[item["productId"]] = item

        quotation_data["items"] = list(unique_items.values())
        quotation_data["has_flete"] = has_flete
        logger.info(f"Presupuesto D365 {quotation_id} recuperado y enriquecido con datos de productos y clientes. has_flete={has_flete}")
        return jsonify(quotation_data), 200

    except Exception as e:
        logger.error(f"Error al procesar presupuesto D365 {quotation_id}: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route('/api/clientes/validate', methods=['POST'])
@login_required
def validate_client():
    try:
        data = request.get_json()
        dni = data.get('dni')
        if not dni:
            return jsonify({"error": "DNI es requerido"}), 400

        access_token = obtener_token_d365()
        if not access_token:
            return jsonify({"error": "No se pudo obtener token D365"}), 500

        existe, resultado = run_validar_cliente_existente(dni, access_token)
        if existe is None:
            return jsonify({"error": resultado}), 500
        if existe:
            return jsonify({"exists": True, "client": resultado}), 200
        return jsonify({"exists": False}), 200
    except Exception as e:
        logger.error(f"Error al validar cliente: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route('/api/direcciones/codigo_postal', methods=['POST'])
@login_required
def get_postal_code_data():
    try:
        data = request.get_json()
        codigo_postal = data.get('codigo_postal')
        if not codigo_postal:
            return jsonify({"error": "Código postal es requerido"}), 400

        datos, error = run_obtener_datos_codigo_postal(codigo_postal)
        if error:
            return jsonify({"error": error}), 500
        return jsonify(datos), 200
    except Exception as e:
        logger.error(f"Error al obtener datos de código postal: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route('/api/save_user_cart', methods=['POST'])
@login_required
def save_user_cart():
    try:
        data = request.get_json()
        user_id = data.get('userId')
        cart = data.get('cart')
        timestamp = data.get('timestamp')

        logger.info(f"Intento de guardar carrito para user_id: {user_id}, timestamp: {timestamp}, cart: {cart}")

        if not user_id or not cart or not timestamp:
            logger.error("Faltan parámetros: userId, cart, o timestamp")
            return jsonify({"error": "userId, cart, y timestamp son requeridos"}), 400

        # Validar que user_id coincide con la sesión
        if user_id != session.get('email'):
            logger.error(f"No autorizado: userId {user_id} no coincide con sesión {session.get('email')}")
            return jsonify({"error": "No autorizado: userId no coincide con la sesión"}), 403

        # Validar estructura del carrito (permitir items vacíos)
        if not isinstance(cart, dict):
            logger.error(f"El carrito debe ser un objeto, recibido: {type(cart)}")
            return jsonify({"error": "El carrito debe ser un objeto"}), 400

        # Asegurar que 'items' exista, incluso si está vacío
        if 'items' not in cart:
            cart['items'] = []
            logger.info("Campo 'items' no presente en el carrito, inicializado como []")

        # Validar que los ítems sean una lista
        if not isinstance(cart['items'], list):
            logger.error(f"El campo 'items' debe ser una lista, recibido: {type(cart['items'])}")
            return jsonify({"error": "El campo 'items' debe ser una lista"}), 400

        if save_cart(user_id, cart, timestamp):
            logger.info(f"Carrito guardado exitosamente para {user_id} con timestamp {timestamp}")
            return jsonify({"message": f"Carrito guardado correctamente para {user_id}", "timestamp": timestamp}), 200
        else:
            logger.error("Error al guardar el carrito en la base de datos")
            return jsonify({"error": "Error al guardar el carrito en la base de datos"}), 500
    except Exception as e:
        logger.error(f"Error al guardar carrito: {str(e)}", exc_info=True)
        return jsonify({"error": f"Error interno: {str(e)}"}), 500

@app.route('/api/get_user_cart', methods=['GET'])
@login_required
def get_user_cart():
    try:
        user_id = session.get('email')
        cart_data = get_cart(user_id)
        logger.info(f"Carrito recuperado para {user_id}")
        return jsonify(cart_data), 200
    except Exception as e:
        logger.error(f"Error al recuperar carrito: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route('/api/quotation_numbers', methods=['GET'])
@login_required
def api_quotation_numbers():
    """Devuelve los números de presupuesto almacenados localmente."""
    try:
        numeros = obtener_numeros_presupuesto()
        return jsonify(numeros), 200
    except Exception as e:
        logger.error(f"Error al obtener números de presupuesto: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500
@app.route('/api/quotations', methods=['GET'])
@login_required
def list_saved_quotations():
    """Retorna los números de presupuestos almacenados localmente."""
    return jsonify(obtener_presupuestos_locales())


@app.route('/api/products/index', methods=['POST'])
@login_required
def api_index_productos():
    """Indexa productos en una colección Mongo en memoria."""
    table = obtener_productos_cache()
    if table is None:
        return jsonify({"error": "No hay productos para indexar"}), 500
    try:
        df = table.to_pandas()
    except Exception as e:
        logger.error(f"Error convirtiendo productos a DataFrame: {e}")
        return jsonify({"error": "Error procesando productos"}), 500
    productos = []
    for _, row in df.iterrows():
        sku = row.get('ProductNumber') or row.get('productId') or row.get('sku')
        descripcion = row.get('ProductName') or row.get('productName') or row.get('description')
        productos.append({"sku": str(sku), "description": descripcion})
    index_products(productos)
    return jsonify({"indexed": len(productos)})


@app.route('/api/products/search')
@login_required
def api_search_productos():
    """Busca productos indexados por SKU o descripción."""
    query = request.args.get('query', '')
    resultados = search_products(query)
    if not resultados:
        # Intentar indexar productos si el índice está vacío
        table = obtener_productos_cache()
        if table is not None:
            try:
                df = table.to_pandas()
                productos = []
                for _, row in df.iterrows():
                    sku = row.get('ProductNumber') or row.get('productId') or row.get('sku')
                    descripcion = row.get('ProductName') or row.get('productName') or row.get('description')
                    productos.append({"sku": str(sku), "description": descripcion})
                index_products(productos)
                resultados = search_products(query)
            except Exception as e:
                logger.error(f"Error indexando productos durante la búsqueda: {e}")
    return jsonify(resultados)

@app.route('/api/clientes/search')
@login_required
def api_clientes_search():
    try:
        query = request.args.get('query', '').strip().lower()
        if not query:
            return jsonify([])

        if not os.path.exists(CACHE_FILE_CLIENTES):
            logger.error(f"El archivo Parquet no existe en la ruta: {CACHE_FILE_CLIENTES}")
            return jsonify({"error": f"Archivo de caché no encontrado: {CACHE_FILE_CLIENTES}"}), 500

        logger.info(f"Intentando leer el archivo Parquet desde: {CACHE_FILE_CLIENTES}")
        table = load_parquet_to_memory()
        nif_filter = pc.match_substring(pc.field('nif'), query)
        numero_cliente_filter = pc.match_substring(pc.field('numero_cliente'), query)
        combined_filter = pc.or_kleene(nif_filter, numero_cliente_filter)
        filtered_table = table.filter(combined_filter)
        import pandas as pd
        df = filtered_table.to_pandas()
        filtered_clientes = df.head(10).to_dict('records')
        logger.info(f"Se encontraron {len(filtered_clientes)} clientes para la consulta '{query}'.")
        return jsonify(filtered_clientes)
    except Exception as e:
        logger.error(f"Error en búsqueda de clientes desde Parquet: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

def upload_image_to_azure(file_stream, filename):
    """Sube un archivo de imagen a Azure Blob Storage."""
    from azure.storage.blob import BlobServiceClient
    connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING', '')
    container_name = os.getenv('AZURE_CONTAINER', 'product-images')
    service = BlobServiceClient.from_connection_string(connection_string)
    container = service.get_container_client(container_name)
    container.upload_blob(name=filename, data=file_stream, overwrite=True)
    return filename

@app.route('/images/<path:filename>')
@login_required
def serve_image(filename):
    """Recupera una imagen desde Azure Blob Storage y la sirve al cliente."""
    try:
        from azure.storage.blob import BlobServiceClient
        connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING', '')
        container_name = os.getenv('AZURE_CONTAINER', 'product-images')
        service = BlobServiceClient.from_connection_string(connection_string)
        blob_client = service.get_blob_client(container=container_name, blob=filename)
        stream = io.BytesIO()
        download_stream = blob_client.download_blob()
        download_stream.readinto(stream)
        stream.seek(0)
        content_type = download_stream.properties.content_settings.content_type
        return send_file(stream, mimetype=content_type)
    except Exception as e:
        logger.error(f"Error al servir imagen {filename}: {e}", exc_info=True)
        return send_file(io.BytesIO(), mimetype='application/octet-stream'), 404

@app.route('/upload_image', methods=['POST'])
@login_required
def upload_image():
    """Endpoint para subir una imagen a Azure Blob Storage."""
    file = request.files.get('file')
    if not file:
        return jsonify({'error': 'No file provided'}), 400
    filename = secure_filename(file.filename)
    upload_image_to_azure(file.stream, filename)
    return jsonify({'filename': filename, 'url': url_for('serve_image', filename=filename)}), 201

@app.route('/static/<path:filename>')
@login_required
def serve_static(filename):
    return send_from_directory('static', filename)

# 🔹 Registro de Blueprints
app.register_blueprint(auth_bp, url_prefix='/auth')
app.register_blueprint(autenticacion_avanzada_bp, url_prefix='/autenticacion_avanzada')
app.register_blueprint(facturacion_arca_bp, url_prefix='/modulo_facturacion_arca')
app.register_blueprint(secuencia_bp, url_prefix='/api/secuencias')
app.register_blueprint(config_pos_bp, url_prefix='/api/config_pos')
app.register_blueprint(pagos_bp, url_prefix='/pagos')
app.register_blueprint(caja_bp, url_prefix='/caja')
app.register_blueprint(pagos_bp, url_prefix='/pagos')
app.register_blueprint(clientes_bp, url_prefix='/clientes')

# 🔹 Ejecutar la aplicación
if __name__ == "__main__":
    app.run(debug=True)
