import logging
import os
from flask import Blueprint, request, jsonify
from auth_module import login_required
from db.database import (
    add_config_pos,
    get_all_config_pos,
    update_config_pos,
    delete_config_pos,
    get_config_pos_by_ids,
)

# Configuración de logging
logger = logging.getLogger('config_pos')
logger.setLevel(logging.DEBUG)
log_dir = os.path.join(os.path.dirname(__file__), '../logs')
os.makedirs(log_dir, exist_ok=True)
handler = logging.FileHandler(os.path.join(log_dir, 'config_pos.log'))
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
if not logger.handlers:
    logger.addHandler(handler)

config_pos_bp = Blueprint('config_pos', __name__)

@config_pos_bp.route('/', methods=['GET'])
@login_required
def listar_config_pos():
    """Lista todas las configuraciones de puntos de venta."""
    configs = get_all_config_pos()
    logger.debug(f"Listando {len(configs)} configuraciones POS")
    return jsonify(configs), 200

@config_pos_bp.route('/', methods=['POST'])
@login_required
def crear_config_pos():
    """Crea una nueva configuración de POS."""
    data = request.get_json() or {}
    required = ['tienda_id', 'pto_venta_id', 'centro_costo']
    if not all(data.get(f) for f in required):
        missing = [f for f in required if not data.get(f)]
        logger.warning(f"Faltan campos requeridos: {missing}")
        return jsonify({'error': f'Faltan campos requeridos: {", ".join(missing)}'}), 400
    config_id = add_config_pos(data['tienda_id'].strip(), data['pto_venta_id'].strip(), data['centro_costo'].strip())
    logger.info(f"Configuración POS creada con ID {config_id}")
    return jsonify({'id': config_id}), 201

@config_pos_bp.route('/<int:config_id>', methods=['PUT'])
@login_required
def actualizar_config_pos(config_id):
    """Actualiza una configuración existente."""
    data = request.get_json() or {}
    required = ['tienda_id', 'pto_venta_id', 'centro_costo']
    if not all(data.get(f) for f in required):
        missing = [f for f in required if not data.get(f)]
        logger.warning(f"Faltan campos requeridos: {missing}")
        return jsonify({'error': f'Faltan campos requeridos: {", ".join(missing)}'}), 400
    updated = update_config_pos(config_id, data['tienda_id'].strip(), data['pto_venta_id'].strip(), data['centro_costo'].strip())
    if not updated:
        logger.warning(f"Configuración POS {config_id} no encontrada para actualización")
        return jsonify({'error': 'Configuración no encontrada'}), 404
    logger.info(f"Configuración POS {config_id} actualizada")
    return jsonify({'message': 'Actualizado correctamente'}), 200

@config_pos_bp.route('/<int:config_id>', methods=['DELETE'])
@login_required
def eliminar_config_pos(config_id):
    """Elimina una configuración por ID."""
    deleted = delete_config_pos(config_id)
    if not deleted:
        logger.warning(f"Configuración POS {config_id} no encontrada para eliminación")
        return jsonify({'error': 'Configuración no encontrada'}), 404
    logger.info(f"Configuración POS {config_id} eliminada")
    return jsonify({'message': 'Eliminado correctamente'}), 200

@config_pos_bp.route('/buscar', methods=['GET'])
@login_required
def buscar_config_pos():
    """Obtiene una configuración específica por tienda y punto de venta."""
    tienda_id = request.args.get('tienda_id')
    pto_venta_id = request.args.get('pto_venta_id')
    if not tienda_id or not pto_venta_id:
        logger.warning("tienda_id y pto_venta_id son requeridos para la búsqueda")
        return jsonify({'error': 'tienda_id y pto_venta_id son requeridos'}), 400
    config = get_config_pos_by_ids(tienda_id.strip(), pto_venta_id.strip())
    if not config:
        logger.info(f"No se encontró configuración para {tienda_id}/{pto_venta_id}")
        return jsonify({'error': 'Configuración no encontrada'}), 404
    return jsonify(config), 200
