import logging
import os
import configparser
from ldap3 import Connection
from ldap3.core.exceptions import LDAPException, LDAPBindError
import requests
from functools import wraps
from flask import session, flash, redirect, url_for, jsonify, request

# Crear carpeta "logs/" si no existe
log_dir = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(log_dir, exist_ok=True)

# Crear un logger específico para este módulo
logger = logging.getLogger("auth_ldap_graph")
logger.setLevel(logging.DEBUG)

# Crear manejador para escribir en archivo
log_file = os.path.join(log_dir, "auth_ldap_graph.log")
file_handler = logging.FileHandler(log_file)
file_handler.setLevel(logging.DEBUG)

# Crear formato del log
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)

# Agregar manejador al logger (evita duplicados si ya existe)
if not logger.handlers:
    logger.addHandler(file_handler)

# (Opcional) También imprimir logs en consola
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

def login_required(f):
    """Decorador para verificar si el usuario ha iniciado sesión."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'usuario' not in session:
            logger.warning(f"Usuario no autenticado intentando acceder a {request.path} (AJAX: {request.headers.get('X-Requested-With') == 'XMLHttpRequest'})")
            if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                return jsonify({'error': 'Usuario no autenticado. Por favor, inicia sesión.', 'redirect': url_for('auth.login')}), 401
            flash("Por favor, inicia sesión primero", "warning")
            return redirect(url_for('auth.login'))
        return f(*args, **kwargs)
    return decorated_function

def load_config():
    """Carga la configuración de LDAP y Graph desde config.ini."""
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
    config.read(config_path)

    if 'ldap' not in config:
        logger.error("La sección 'ldap' no se encontró en config.ini")
        raise KeyError("La sección 'ldap' no se encontró en config.ini")

    if 'graph' not in config:
        logger.error("La sección 'graph' no se encontró en config.ini")
        raise KeyError("La sección 'graph' no se encontró en config.ini")

    return config['ldap'], config['graph']

ldap, graph = load_config()

def ldap_authenticate(username, password):
    """Autenticación contra LDAP en dos dominios."""
    server = ldap['ldap_server']
    familia = ldap['ldap_domain']
    todogriferia = ldap['ldap_domain_tg']
    # Intentar autenticación en el primer dominio
    try:
        logger.info(f"Intentando autenticación en {familia} para {username}")
        conn = Connection(server, user=f"{username}@{familia}", password=password, auto_bind=True)
        email = f"{username}@{familia}"
        return True, None, email
    except LDAPBindError:
        logger.warning(f"Credenciales inválidas en {familia} para {username}")
    except LDAPException as error:
        return False, handle_ldap_error(error), None

    # Si la autenticación en domain_1 falla, intentar con el segundo dominio
    try:
        logger.info(f"Intentando autenticación en {todogriferia} para {username}")
        conn = Connection(server, user=f"{username}@{todogriferia}", password=password, auto_bind=True)
        email = f"{username}@{todogriferia}"
        return True, None, email
    except LDAPBindError:
        logger.warning(f"Credenciales inválidas en {todogriferia} para {username}")
        return False, f"Credenciales inválidas", None
    except LDAPException as error:
        return False, handle_ldap_error(error), None

    mensaje = f"Usuario o contraseña incorrectos"
    logger.error(mensaje)
    return False, mensaje, None

def handle_ldap_error(error):
    """Manejo de errores LDAP y problemas de conexión."""
    mensaje = "No se pudo verificar las credenciales. Parece que no estás conectado a la VPN." if "WinError 10060" in str(error) else str(error)
    logger.error(f"Falló la conexión LDAP: {mensaje}")
    flash(mensaje, "danger")
    return mensaje

def get_access_token_graph():
    """Obtiene el token de autenticación para Microsoft Graph."""
    token_url = graph['token_client']
    client_id = graph['client_id']
    client_secret = graph['client_secret']
    token_params = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "resource": "https://graph.microsoft.com"
    }

    try:
        response = requests.post(token_url, data=token_params, timeout=120)
        response.raise_for_status()
        token_data_graph = response.json()
        access_token_graph = token_data_graph['access_token']
        logger.info("Consulta token a Graph OK")
        return access_token_graph
    except requests.exceptions.RequestException as e:
        logger.error(f"Consulta token a Graph FALLO: {e}")
        return None

def get_authorization(username):
    """Valida si el usuario tiene permisos en Microsoft Graph."""
    token = get_access_token_graph()
    if not token:
        return False, "No se pudo obtener el token de Graph"

    url = graph['client']
    headers = {"Authorization": f"Bearer {token}"}
    params = {"$select": "mail"}

    logger.info(f"Validando permisos del usuario {username} en Graph")
    response = requests.get(url, headers=headers, params=params, timeout=60)

    if response.status_code == 200:
        data = response.json()
        filtered_users = [user for user in data["value"] if username in user["mail"]]
        if not filtered_users:
            error = f"El usuario {username} NO tiene permisos de gestión"
            logger.warning(error)
            return False, error
        return True, None
    error = "El servidor de Microsoft Graph no responde"
    logger.error(error)
    return False, error
