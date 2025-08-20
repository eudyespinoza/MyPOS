import re
import logging
from flask import Blueprint, request, render_template, redirect, url_for, flash
from db.database import guardar_cliente, actualizar_cliente, buscar_cliente_por_cuit
from connectors.sap_clientes import consultar_datos_impositivos, actualizar_datos_impositivos

logger = logging.getLogger('clientes')
clientes_bp = Blueprint('clientes', __name__)


def validar_dni(dni: str) -> bool:
    return bool(re.fullmatch(r"\d{7,8}", dni or ""))


def validar_cuit(cuit: str) -> bool:
    return bool(re.fullmatch(r"\d{11}", cuit or ""))


@clientes_bp.route('/nuevo', methods=['GET', 'POST'])
def nuevo_cliente():
    if request.method == 'POST':
        datos = {
            'nombre': request.form.get('nombre'),
            'dni': request.form.get('dni'),
            'cuit': request.form.get('cuit'),
            'direccion': request.form.get('direccion')
        }

        if not validar_dni(datos['dni']):
            flash('DNI inválido', 'danger')
            return render_template('clientes/form.html', cliente=datos)

        if not validar_cuit(datos['cuit']):
            flash('CUIT inválido', 'danger')
            return render_template('clientes/form.html', cliente=datos)

        if buscar_cliente_por_cuit(datos['cuit']):
            flash('El cliente ya existe', 'warning')
            return render_template('clientes/form.html', cliente=datos)

        guardar_cliente(datos)
        try:
            actualizar_datos_impositivos(datos['cuit'], datos)
        except Exception as exc:  # pragma: no cover - fallo externo
            logger.warning('No se pudo sincronizar con SAP: %s', exc)
        flash('Cliente guardado', 'success')
        return redirect(url_for('clientes.nuevo_cliente'))

    return render_template('clientes/form.html', cliente=None)


@clientes_bp.route('/<cuit>/editar', methods=['GET', 'POST'])
def editar_cliente(cuit):
    cliente = buscar_cliente_por_cuit(cuit)
    if not cliente:
        flash('Cliente no encontrado', 'danger')
        return redirect(url_for('clientes.nuevo_cliente'))

    if request.method == 'POST':
        datos = {
            'nombre': request.form.get('nombre'),
            'dni': request.form.get('dni'),
            'cuit': request.form.get('cuit'),
            'direccion': request.form.get('direccion')
        }

        if not validar_dni(datos['dni']):
            flash('DNI inválido', 'danger')
            return render_template('clientes/form.html', cliente=datos)

        if not validar_cuit(datos['cuit']):
            flash('CUIT inválido', 'danger')
            return render_template('clientes/form.html', cliente=datos)

        actualizar_cliente(cuit, datos)
        try:
            actualizar_datos_impositivos(datos['cuit'], datos)
        except Exception as exc:  # pragma: no cover
            logger.warning('No se pudo sincronizar con SAP: %s', exc)
        flash('Cliente actualizado', 'success')
        return redirect(url_for('clientes.editar_cliente', cuit=datos['cuit']))

    try:
        sap_info = consultar_datos_impositivos(cuit)
        if sap_info:
            cliente.update(sap_info)
    except Exception as exc:  # pragma: no cover
        logger.warning('No se pudo consultar SAP: %s', exc)

    return render_template('clientes/form.html', cliente=cliente)
