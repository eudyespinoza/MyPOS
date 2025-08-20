import logging
import os
from functools import wraps
from flask import Blueprint, request, jsonify, session, redirect, url_for, flash, render_template
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from auth_module import ldap_authenticate  # Reusa LDAP existente
from db.database import obtener_empleados_by_email
from auth import login_required

# Log dedicado
logger = logging.getLogger('autenticacion_avanzada')
logger.setLevel(logging.DEBUG)
handler = logging.FileHandler('logs/autenticacion_avanzada.log')
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

autenticacion_avanzada_bp = Blueprint('autenticacion_avanzada', __name__)

# MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['pos_db']
usuarios_roles = db['usuarios_roles']  # Colección: {'email': str, 'role': str, 'permissions': [str]}

# Clave superior para acciones sensibles
CLAVE_SUPERIOR = os.getenv('CLAVE_SUPERIOR', 'supersecret')


def requiere_permiso_clave(permission):
    """Valida rol, permiso y clave superior para acciones sensibles."""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if session.get('role') != 'admin':
                return jsonify({'error': 'No autorizado'}), 403
            if permission not in session.get('permissions', []):
                return jsonify({'error': 'Permiso insuficiente'}), 403
            clave = None
            if request.is_json:
                clave = (request.json or {}).get('clave_superior')
            if clave is None:
                clave = request.form.get('clave_superior')
            if clave != CLAVE_SUPERIOR:
                return jsonify({'error': 'Clave superior incorrecta'}), 403
            return func(*args, **kwargs)

        return wrapper

    return decorator


@autenticacion_avanzada_bp.route('/login', methods=['GET', 'POST'])
def login_avanzado():
    if request.method == 'POST':
        username = request.form.get('username').lower()
        password = request.form.get('password')
        print(username, password)

        # Validar que los campos no estén vacíos
        if not username or not password:
            flash("Debes ingresar usuario y contraseña.", "danger")
            return redirect(url_for('autenticacion_avanzada.login_avanzado'))

        # Autenticación LDAP
        success, error, mail = ldap_authenticate(username, password)
        if not success:
            flash(f"Credenciales incorrectas: {error}", "danger")
            return render_template('autenticacion_avanzada/login.html')

        # Obtener datos del empleado
        datos = obtener_empleados_by_email(mail)
        if not datos:
            flash("No se encontraron datos del empleado en la base de datos.", "danger")
            return render_template('autenticacion_avanzada/login.html')

        # Extraer datos del empleado
        nombre_completo = datos['nombre_completo']
        email = datos['email']
        id_puesto = datos['id_puesto']
        empleado_d365 = datos['empleado_d365']
        numero_sap = datos['numero_sap']
        last_store = datos['last_store']

        # Obtener rol y permisos desde MongoDB (defaults)
        user_role = usuarios_roles.find_one({'email': email})
        role = user_role['role'] if user_role else 'user'
        permissions = user_role.get('permissions', []) if user_role else []

        # Guardar en sesión (compatible con app.py)
        session['usuario'] = nombre_completo
        session['id_puesto'] = id_puesto
        session['empleado_d365'] = empleado_d365
        session['numero_sap'] = numero_sap
        session['email'] = email
        session['last_store'] = last_store
        session['role'] = role  # Añadir rol para navbar dinámica
        session['permissions'] = permissions

        print(nombre_completo, email, id_puesto, empleado_d365, numero_sap, role)
        flash("Iniciaste sesión con éxito.", "success")
        logger.info(f"Login exitoso: {email}, rol: {role}")
        return redirect(url_for('productos'))

    return render_template('autenticacion_avanzada/login.html')


@autenticacion_avanzada_bp.route('/logout')
@login_required
def logout():
    session.clear()
    flash("Cerraste sesión con éxito.", "success")
    return redirect(url_for('autenticacion_avanzada.login_avanzado'))


@autenticacion_avanzada_bp.route('/configs_menu')
@login_required
def configs_menu():
    if session.get('role') != 'admin':
        flash("Acceso denegado: Solo admins", "danger")
        logger.warning(f"Acceso denegado a configs: {session['email']}")
        return redirect(url_for('productos'))

    modulos = [
        {'nombre': 'Facturación ARCA', 'url': url_for('modulo_facturacion_arca.config_facturacion')}
    ]
    return render_template('autenticacion_avanzada/configs_menu.html', modulos=modulos)


@autenticacion_avanzada_bp.route('/set_role', methods=['POST'])
@login_required
@requiere_permiso_clave('manage_roles')
def set_role():
    data = request.json
    email = data['email']
    role = data['role']
    permissions = data.get('permissions', [])
    usuarios_roles.update_one(
        {'email': email},
        {'$set': {'role': role, 'permissions': permissions}},
        upsert=True,
    )
    logger.info(f"Rol actualizado: {email} -> {role} | permisos: {permissions}")
    return jsonify({'message': 'Rol y permisos actualizados'})
