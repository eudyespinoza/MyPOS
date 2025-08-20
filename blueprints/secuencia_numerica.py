import logging
import os
from flask import Blueprint, request, jsonify, session
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from auth_module import login_required

# Configuración de logging
logger = logging.getLogger('secuencias_numericas')
logger.setLevel(logging.DEBUG)
log_dir = os.path.join(os.path.dirname(__file__), '../logs')
os.makedirs(log_dir, exist_ok=True)
handler = logging.FileHandler(os.path.join(log_dir, 'secuencias_numericas.log'))
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
if not logger.handlers:  # Evitar duplicar manejadores
    logger.addHandler(handler)

secuencia_bp = Blueprint('secuencia_numerica', __name__)

# Conexión MongoDB
try:
    client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=5000)
    client.server_info()  # Validar conexión
    db = client['pos_db']
    secuencias_collection = db['secuencias_numericas']
    logger.info("Conexión a MongoDB establecida correctamente")
except PyMongoError as e:
    logger.error(f"Error en conexión MongoDB: {str(e)}", exc_info=True)
    raise

# Crear índice único para evitar duplicados
try:
    secuencias_collection.create_index(
        [('tienda_id', 1), ('pto_venta_id', 1), ('tipo_secuencia', 1)],
        unique=True
    )
    logger.info("Índice único creado para secuencias_numericas")
except PyMongoError as e:
    logger.error(f"Error al crear índice: {str(e)}", exc_info=True)

@secuencia_bp.route('/configurar', methods=['POST'])
@login_required
def configurar_secuencia():
    """
    Configura una secuencia numérica para una tienda y punto de venta.
    """
    try:
        data = request.get_json()
        logger.debug(f"Datos recibidos en /configurar: {data}")

        # Validar campos requeridos
        required_fields = ['tienda_id', 'pto_venta_id', 'tipo_secuencia', 'secuencia_inicial', 'longitud']
        if not data or not all(field in data for field in required_fields):
            missing = [field for field in required_fields if field not in data or not data[field]]
            logger.warning(f"Faltan campos requeridos: {missing}")
            return jsonify({'error': f'Faltan campos requeridos: {", ".join(missing)}'}), 400

        tienda_id = data['tienda_id'].strip()
        pto_venta_id = data['pto_venta_id'].strip()
        tipo_secuencia = data['tipo_secuencia']
        secuencia_inicial = int(data['secuencia_inicial'])
        longitud = int(data['longitud'])
        prefijo = data.get('prefijo', '').strip()
        sufijo = data.get('sufijo', '').strip()
        activo = data.get('activo', True)

        # Validar tipo_secuencia
        valid_types = ['Factura_A', 'Factura_B', 'Nota_Credito_A', 'Nota_Credito_B']
        if tipo_secuencia not in valid_types:
            logger.warning(f"Tipo de secuencia inválido: {tipo_secuencia}")
            return jsonify({'error': f'Tipo de secuencia inválido. Opciones válidas: {", ".join(valid_types)}'}), 400

        # Validar longitud y secuencia inicial
        if longitud < 1:
            logger.warning(f"Longitud inválida: {longitud}")
            return jsonify({'error': 'La longitud debe ser mayor a 0'}), 400
        if secuencia_inicial < 1:
            logger.warning(f"Secuencia inicial inválida: {secuencia_inicial}")
            return jsonify({'error': 'La secuencia inicial debe ser mayor a 0'}), 400

        # Preparar documento
        documento = {
            'tienda_id': tienda_id,
            'pto_venta_id': pto_venta_id,
            'tipo_secuencia': tipo_secuencia,
            'secuencia_actual': secuencia_inicial,
            'prefijo': prefijo,
            'sufijo': sufijo,
            'longitud': longitud,
            'activo': activo
        }

        # Actualizar o insertar
        result = secuencias_collection.update_one(
            {'tienda_id': tienda_id, 'pto_venta_id': pto_venta_id, 'tipo_secuencia': tipo_secuencia},
            {'$set': documento},
            upsert=True
        )
        logger.info(f"Secuencia configurada para {tienda_id}/{pto_venta_id}/{tipo_secuencia} por {session.get('email')}. Upserted: {result.upserted_id is not None}")

        return jsonify({
            'message': 'Secuencia configurada exitosamente',
            'tienda_id': tienda_id,
            'pto_venta_id': pto_venta_id,
            'tipo_secuencia': tipo_secuencia
        }), 200

    except PyMongoError as e:
        logger.error(f"Error en MongoDB al configurar secuencia: {str(e)}", exc_info=True)
        return jsonify({'error': 'Error interno al guardar la secuencia'}), 500
    except ValueError as e:
        logger.warning(f"Error de validación: {str(e)}")
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        logger.error(f"Error inesperado al configurar secuencia: {str(e)}", exc_info=True)
        return jsonify({'error': 'Error inesperado al configurar la secuencia'}), 500

@secuencia_bp.route('/obtener_siguiente', methods=['POST'])
@login_required
def obtener_siguiente_secuencia():
    """
    Obtiene el siguiente número de secuencia para una tienda y punto de venta, incrementándolo atómicamente.
    """
    try:
        data = request.get_json()
        logger.debug(f"Datos recibidos en /obtener_siguiente: {data}")

        required_fields = ['tienda_id', 'pto_venta_id', 'tipo_secuencia']
        if not data or not all(field in data for field in required_fields):
            missing = [field for field in required_fields if field not in data or not data[field]]
            logger.warning(f"Faltan campos requeridos: {missing}")
            return jsonify({'error': f'Faltan campos requeridos: {", ".join(missing)}'}), 400

        tienda_id = data['tienda_id'].strip()
        pto_venta_id = data['pto_venta_id']
        tipo_secuencia = data['tipo_secuencia']

        # Buscar e incrementar secuencia atómicamente
        result = secuencias_collection.find_one_and_update(
            {
                'tienda_id': tienda_id,
                'pto_venta_id': str(pto_venta_id),
                'tipo_secuencia': tipo_secuencia,
                'activo': True
            },
            {'$inc': {'secuencia_actual': 1}},
            return_document=True
        )

        if not result:
            logger.warning(f"No está configurada la secuencia {tipo_secuencia} para {tienda_id}/{pto_venta_id}")
            return jsonify({'error': f'No está configurada la secuencia {tipo_secuencia} para {tienda_id}/{pto_venta_id}'}), 404

        secuencia_actual = result['secuencia_actual']
        prefijo = result.get('prefijo', '')
        sufijo = result.get('sufijo', '')
        longitud = result['longitud']
        numero_formateado = f"{prefijo}{str(secuencia_actual).zfill(longitud)}{sufijo}"

        logger.info(f"Secuencia obtenida: {numero_formateado} ({secuencia_actual}) para {tienda_id}/{pto_venta_id}/{tipo_secuencia}")
        return jsonify({
            'secuencia_actual': secuencia_actual,
            'numero': numero_formateado
        }), 200

    except PyMongoError as e:
        logger.error(f"Error en MongoDB al obtener secuencia: {str(e)}", exc_info=True)
        return jsonify({'error': 'Error interno al obtener la secuencia'}), 500
    except Exception as e:
        logger.error(f"Error inesperado al obtener secuencia: {str(e)}", exc_info=True)
        return jsonify({'error': 'Error inesperado al obtener la secuencia'}), 500

@secuencia_bp.route('/listar', methods=['GET'])
@login_required
def listar_secuencias():
    """
    Lista todas las secuencias configuradas, opcionalmente filtradas por tienda_id, pto_venta_id y tipo_secuencia.
    """
    try:
        query = {'activo': True}
        if 'tienda_id' in request.args:
            query['tienda_id'] = request.args.get('tienda_id').strip()
        if 'pto_venta_id' in request.args:
            query['pto_venta_id'] = request.args.get('pto_venta_id').strip()
        if 'tipo_secuencia' in request.args:
            query['tipo_secuencia'] = request.args.get('tipo_secuencia')

        secuencias = list(secuencias_collection.find(query, {'_id': 0}))
        logger.info(f"Listadas {len(secuencias)} secuencias para consulta: {query}")
        return jsonify(secuencias), 200

    except PyMongoError as e:
        logger.error(f"Error en MongoDB al listar secuencias: {str(e)}", exc_info=True)
        return jsonify({'error': 'Error interno al listar secuencias'}), 500
    except Exception as e:
        logger.error(f"Error inesperado al listar secuencias: {str(e)}", exc_info=True)
        return jsonify({'error': 'Error inesperado al listar secuencias'}), 500
