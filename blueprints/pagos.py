import logging
from flask import Blueprint, jsonify, request
from auth_module import login_required
from connectors.payway import PaywayClient

logger = logging.getLogger('pagos')
pagos_bp = Blueprint('pagos', __name__)
_client = PaywayClient()


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
