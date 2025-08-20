from flask import Blueprint, request, session, redirect, url_for, flash, render_template
from auth_module import ldap_authenticate, login_required
from db.database import obtener_empleados_by_email


auth_bp = Blueprint('auth', __name__)

@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username').lower()
        password = request.form.get('password')
        print(username, password)

        # Validar que los campos no estén vacíos
        if not username or not password:
            flash("Debes ingresar usuario y contraseña.", "danger")
            return redirect(url_for('auth.login'))

        # Autenticación LDAP
        success, error, mail = ldap_authenticate(username, password)
        if not success:
            flash(f"Credenciales incorrectas: {error}", "danger")
            return render_template('login.html')

        # Obtener datos del empleado
        datos = obtener_empleados_by_email(mail)

        # Validar si se encontraron datos
        if not datos:
            flash("No se encontraron datos del empleado en la base de datos.", "danger")
            return render_template('login.html')

        # Acceder directamente a los valores del diccionario
        nombre_completo = datos['nombre_completo']
        email = datos['email']
        id_puesto = datos['id_puesto']
        empleado_d365 = datos['empleado_d365']
        numero_sap = datos['numero_sap']
        last_store = datos['last_store']

        # Guardar usuario autenticado en sesión
        session['usuario'] = nombre_completo
        session['id_puesto'] = id_puesto
        session['empleado_d365'] = empleado_d365
        session['numero_sap'] = numero_sap
        session['email'] = email
        session['last_store'] = last_store
        print(nombre_completo, email, id_puesto, empleado_d365, numero_sap)
        flash("Iniciaste sesión con éxito.", "success")

        # Redirigir al inicio
        return redirect(url_for('productos'))

    # Renderizar el formulario de inicio de sesión
    return render_template('login.html')


@auth_bp.route('/logout')
@login_required
def logout():
    session.clear()
    flash("Cerraste sesión con éxito.", "success")
    return redirect(url_for('auth.login'))
