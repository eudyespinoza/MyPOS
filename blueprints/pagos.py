import os
import csv
import logging
import requests
from flask import Blueprint, jsonify, render_template, request
from auth_module import login_required
from connectors.payway import PaywayClient
from db.database import guardar_pago, actualizar_estado_operacion

logger = logging.getLogger('pagos')
pagos_bp = Blueprint('pagos', __name__)
_client = PaywayClient()

BANK_API_URL = os.getenv('BANK_API_URL')


def validar_transferencia(referencia: str, monto: float) -> bool:
    """Valida una transferencia contra una API bancaria o archivo de conciliación.

    Primero intenta consultar una API bancaria definida por la variable
    de entorno ``BANK_API_URL``. Si no está configurada o la validación
    falla, se consulta un archivo ``conciliaciones.csv`` almacenado en
    la raíz del proyecto.
    """
    if monto <= 0:
        return True

    if BANK_API_URL and referencia:
        try:
            response = requests.get(f"{BANK_API_URL}/{referencia}", timeout=10)
            if response.ok:
                data = response.json()
                return float(data.get('monto', 0)) == float(monto)
        except requests.RequestException:
            pass

    conciliacion_path = os.path.join(
        os.path.dirname(__file__),
        '../conciliaciones.csv',
    )
    if os.path.exists(conciliacion_path) and referencia:
        with open(conciliacion_path, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                try:
                    if (
                        row.get('referencia') == referencia
                        and float(row.get('monto', 0)) == float(monto)
                    ):
                        return True
                except (TypeError, ValueError):
                    continue
    return False


@pagos_bp.route('/registrar', methods=['POST'])
def registrar_pago():
    """Registra un pago mixto para una operación."""
    data = request.get_json(silent=True) or request.form
    operacion_id = data.get('operacion_id')
    pagos = {
        'efectivo': float(data.get('efectivo', 0) or 0),
        'transferencia': float(data.get('transferencia', 0) or 0),
        'tarjeta': float(data.get('tarjeta', 0) or 0),
    }
    referencia = data.get('referencia')

    if pagos['transferencia'] and not validar_transferencia(
        referencia, pagos['transferencia']
    ):
        return jsonify({'error': 'Transferencia no válida'}), 400

    guardar_pago(operacion_id, pagos)
    actualizar_estado_operacion(operacion_id, 'pagado')
    return jsonify({'status': 'ok'})


@pagos_bp.route('/', methods=['GET'])
def formulario_pagos():
    """Muestra el formulario de pagos."""
    return render_template('pagos.html')


@pagos_bp.route('/pago', methods=['POST'])
@login_required
def crear_pago():
    """Crea un pago utilizando Payway."""
    data = request.get_json() or {}
    try:
        result = _client.create_payment(data)
        return jsonify(result), 201
    except Exception as exc:  # pragma: no cover - depende de servicio externo
        logger.error(f"Error al crear pago: {exc}")
        return jsonify({'error': str(exc)}), 400


@pagos_bp.route('/pago/<payment_id>', methods=['GET'])
@login_required
def obtener_pago(payment_id: str):
    """Obtiene el estado de un pago por ID."""
    try:
        result = _client.get_payment(payment_id)
        return jsonify(result)
    except Exception as exc:  # pragma: no cover
        logger.error(f"Error al obtener pago {payment_id}: {exc}")
        return jsonify({'error': str(exc)}), 400
