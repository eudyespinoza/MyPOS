import logging
import os
import base64
import subprocess
import requests
import xml.etree.ElementTree as ET
import html
from flask import Blueprint, request, jsonify, session, render_template, flash, redirect, url_for
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from datetime import datetime, timedelta, timezone
from zeep import Client, Settings
from zeep.plugins import HistoryPlugin
from auth_module import login_required
from dateutil.parser import isoparse
from db.database import obtener_stores_from_parquet

# Configuración de logging
logger = logging.getLogger('modulo_facturacion_arca')
logger.setLevel(logging.DEBUG)
log_dir = os.path.join(os.path.dirname(__file__), '../logs')
os.makedirs(log_dir, exist_ok=True)
handler = logging.FileHandler(os.path.join(log_dir, 'modulo_facturacion_arca.log'))
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
if not logger.handlers:  # Evitar duplicar manejadores
    logger.addHandler(handler)

facturacion_arca_bp = Blueprint('modulo_facturacion_arca', __name__)

# Conexión MongoDB
try:
    client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=5000)
    client.server_info()  # Validar conexión
    db = client['pos_db']
    config_collection = db['config_facturacion_arca']
    logger.info("Conexión a MongoDB establecida correctamente")
except PyMongoError as e:
    logger.error(f"Error en conexión MongoDB: {str(e)}", exc_info=True)
    raise

def get_certificado_data(config):
    """
    Obtiene los datos del certificado desde la configuración o un archivo estático.
    """
    cert_path = os.path.join(os.path.dirname(__file__), '../cert/certificado.pfx')
    if not config.get('certificado_data') and os.path.exists(cert_path):
        logger.info(f"Usando certificado estático desde {cert_path}")
        with open(cert_path, 'rb') as f:
            return base64.b64encode(f.read()).decode('utf-8')
    return config.get('certificado_data')

def ta_valido(config):
    """
    Verifica si el Ticket de Acceso (TA) es válido basado en su fecha de expiración.
    """
    ta_path = os.path.join(os.path.dirname(__file__), '../ta.xml')
    if not os.path.exists(ta_path):
        return False
    try:
        tree = ET.parse(ta_path)
        root = tree.getroot()
        inner = root.find(".//{http://wsaa.view.sua.dvadac.desein.afip.gov.ar/}loginCmsReturn")
        if inner is None:
            return False
        unescaped = html.unescape(inner.text)
        inner_root = ET.fromstring(unescaped)
        exp = inner_root.find(".//expirationTime").text
        exp_dt = isoparse(exp)
        now = datetime.now(timezone(timedelta(hours=-3)))
        return now < exp_dt
    except Exception as e:
        logger.error(f"Error validando TA existente: {str(e)}", exc_info=True)
        return False

def obtener_ta(config):
    """
    Obtiene un nuevo Ticket de Acceso (TA) de AFIP o reutiliza uno válido.
    - Timestamps en UTC con sufijo 'Z' para evitar xml.generationTime.invalid.
    - Ventana -5 minutos / +12 horas para absorber latencias/drift.
    """
    import tempfile

    base_dir = os.path.dirname(__file__)
    ta_path = os.path.join(base_dir, '../ta.xml')
    cert_dir = os.path.join(base_dir, '../cert')
    os.makedirs(cert_dir, exist_ok=True)

    pem_cert_path = os.path.join(cert_dir, 'temp_cert.pem')
    pem_key_path  = os.path.join(cert_dir, 'temp_key.pem')
    pfx_password  = config['clave_privada']

    # Ruta de OpenSSL (ajusta si corresponde)
    openssl_path = r"C:\Program Files\OpenSSL-Win64\bin\openssl.exe"

    # Si ya hay TA válido, lo reutilizamos
    if ta_valido(config):
        logger.info("Reutilizando TA válido.")
        try:
            tree = ET.parse(ta_path)
            root = tree.getroot()
            login_cms_return = root.find(".//{http://wsaa.view.sua.dvadac.desein.afip.gov.ar/}loginCmsReturn")
            inner_xml = html.unescape(login_cms_return.text)
            inner_root = ET.fromstring(inner_xml)
            token = inner_root.find(".//token").text
            sign = inner_root.find(".//sign").text
            return token, sign
        except Exception as e:
            logger.error(f"Error al reutilizar TA: {str(e)}", exc_info=True)
            raise

    logger.info("Generando nuevo TA...")

    # Certificado PFX (de Mongo o archivo estático)
    certificado_data = get_certificado_data(config)
    if not certificado_data:
        logger.error("No se encontró certificado válido para generar TA")
        raise ValueError("No se encontró certificado válido")

    # Escribimos PFX a un archivo temporal de manera segura
    try:
        decoded_data = base64.b64decode(certificado_data)
        with tempfile.NamedTemporaryFile(prefix="cert_", suffix=".pfx", delete=False) as tmp_pfx:
            tmp_pfx_path = tmp_pfx.name
            tmp_pfx.write(decoded_data)
        logger.info(f"Archivo temporal PFX creado: {tmp_pfx_path} ({len(decoded_data)} bytes)")
    except Exception as e:
        logger.error(f"Error al decodificar/escribir PFX: {str(e)}", exc_info=True)
        raise ValueError(f"Error al procesar el certificado: {str(e)}")

    if not os.path.exists(openssl_path):
        logger.error(f"OpenSSL no encontrado en {openssl_path}")
        # Limpieza del PFX temporal
        try:
            os.remove(tmp_pfx_path)
        except Exception:
            pass
        raise ValueError(f"OpenSSL no está instalado o la ruta es incorrecta: {openssl_path}")

    # Extraer cert y key en PEM
    try:
        subprocess.run([
            openssl_path, "pkcs12",
            "-in", tmp_pfx_path,
            "-clcerts", "-nokeys",
            "-out", pem_cert_path,
            "-passin", f"pass:{pfx_password}"
        ], check=True)

        subprocess.run([
            openssl_path, "pkcs12",
            "-in", tmp_pfx_path,
            "-nocerts", "-nodes",
            "-out", pem_key_path,
            "-passin", f"pass:{pfx_password}"
        ], check=True)
    except subprocess.CalledProcessError as e:
        logger.error(f"Error al ejecutar OpenSSL: {str(e)}", exc_info=True)
        raise ValueError(f"Error al extraer certificados con OpenSSL: {str(e)}")
    finally:
        # Borramos el PFX temporal
        try:
            os.remove(tmp_pfx_path)
        except Exception:
            pass

    # --- FIX DE TIEMPOS: usar UTC con 'Z' y ventana amplia ---
    now_utc = datetime.now(timezone.utc)
    gen_time = (now_utc - timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ")
    exp_time = (now_utc + timedelta(hours=12)).strftime("%Y-%m-%dT%H:%M:%SZ")
    unique_id = int(now_utc.timestamp())

    # Log de diagnóstico para correlacionar reloj host vs UTC
    logger.info(f"host_now_local={datetime.now().astimezone().isoformat()}")
    logger.info(f"host_now_utc={now_utc.isoformat()}")
    logger.info(f"gen_time={gen_time} exp_time={exp_time} unique_id={unique_id}")

    login_xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<loginTicketRequest version="1.0">
  <header>
    <uniqueId>{unique_id}</uniqueId>
    <generationTime>{gen_time}</generationTime>
    <expirationTime>{exp_time}</expirationTime>
  </header>
  <service>wsfe</service>
</loginTicketRequest>"""

    # Guardamos el LTR para auditoría
    ltr_path = os.path.join(base_dir, '../loginTicketRequest.xml')
    with open(ltr_path, "w", encoding="utf-8") as f:
        f.write(login_xml)
    logger.info(f"LoginTicketRequest generado en {ltr_path}")

    # Firmar CMS con OpenSSL (SMIME)
    cms_path = os.path.join(base_dir, '../cms.tmp')
    try:
        subprocess.run([
            openssl_path, "smime", "-sign",
            "-signer", pem_cert_path,
            "-inkey", pem_key_path,
            "-in", ltr_path,
            "-out", cms_path,
            "-outform", "PEM",
            "-nodetach"
        ], check=True)
    except subprocess.CalledProcessError as e:
        logger.error(f"Error al firmar CMS con OpenSSL: {str(e)}", exc_info=True)
        raise ValueError(f"Error al firmar CMS: {str(e)}")
    logger.info(f"CMS firmado guardado en {cms_path}")

    # Leer CMS y extraer base64
    with open(cms_path, "r", encoding="utf-8") as f:
        lines = f.readlines()
        b64_lines = [l for l in lines if not l.startswith("-----")]
        cms_b64 = "".join(b64_lines).replace("\n", "").strip()

    soap = f"""<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                  xmlns:ser="http://wsaa.view.sua.dvadac.desein.afip.gov.ar/">
   <soapenv:Header/>
   <soapenv:Body>
      <ser:loginCms>
         <in0>{cms_b64}</in0>
      </ser:loginCms>
   </soapenv:Body>
</soapenv:Envelope>"""

    headers = {
        "Content-Type": "text/xml; charset=utf-8",
        "SOAPAction": "http://wsaa.view.sua.dvadac.desein.afip.gov.ar/ws/services/LoginCms/loginCms"
    }

    logger.info("Enviando request al WSAA (HOMO)...")
    res = requests.post(
        "https://wsaahomo.afip.gov.ar/ws/services/LoginCms",
        data=soap.encode("utf-8"),
        headers=headers,
        timeout=30
    )

    # Limpieza de archivos temporales (PEMs y CMS). Conservamos el LTR por auditoría.
    try:
        if os.path.exists(pem_cert_path):
            os.remove(pem_cert_path)
        if os.path.exists(pem_key_path):
            os.remove(pem_key_path)
        if os.path.exists(cms_path):
            os.remove(cms_path)
    except Exception:
        pass

    if res.status_code == 200:
        logger.info("WSAA respondió 200. Guardando TA...")
        with open(ta_path, "w", encoding="utf-8") as f:
            f.write(res.text)
        try:
            root = ET.fromstring(res.content)
            login_cms_return = root.find(".//{http://wsaa.view.sua.dvadac.desein.afip.gov.ar/}loginCmsReturn")
            if login_cms_return is None:
                logger.error("No se encontró loginCmsReturn en la respuesta del WSAA")
                raise ValueError("Respuesta inválida del WSAA (sin loginCmsReturn)")

            inner_xml = html.unescape(login_cms_return.text)
            inner_root = ET.fromstring(inner_xml)
            token = inner_root.find(".//token").text
            sign = inner_root.find(".//sign").text
            if not token or not sign:
                raise ValueError("WSAA no devolvió token/sign")
            return token, sign
        except Exception as e:
            logger.error(f"No se pudo parsear TA: {str(e)}", exc_info=True)
            raise
    else:
        logger.error(f"Error WSAA: Código {res.status_code}, Respuesta: {res.text[:500]}")
        raise ValueError(f"Error en WSAA: {res.status_code} - {res.text[:500]}")


@facturacion_arca_bp.route('/config', methods=['GET', 'POST'])
@login_required
def config_facturacion():
    """
    Maneja la configuración de facturación ARCA. Solo accesible para admins.
    """
    if session.get('role') != 'admin':
        flash("Acceso denegado: Solo administradores pueden configurar facturación.", "danger")
        logger.warning(f"Acceso denegado a /modulo_facturacion_arca/config: {session.get('email')}")
        return redirect(url_for('productos'))

    if request.method == 'POST':
        try:
            if 'certificado' in request.files:
                certificado_file = request.files['certificado']
                if certificado_file.filename == '':
                    logger.warning("No se seleccionó un archivo de certificado")
                    return jsonify({'error': 'No se seleccionó un archivo de certificado'}), 400
                certificado_data = base64.b64encode(certificado_file.read()).decode('utf-8')
            else:
                logger.warning("No se subió el archivo de certificado")
                return jsonify({'error': 'No se subió el archivo de certificado'}), 400

            data = {
                'store_id': request.form.get('store_id'),
                'cuit': request.form.get('cuit'),
                'certificado_data': certificado_data,
                'clave_privada': request.form.get('clave_privada'),
                'punto_venta': request.form.get('punto_vta'),
                'ambiente': request.form.get('ambiente'),
                'modo_autorizacion': request.form.get('modo_autorizacion')
            }

            required = ['store_id', 'cuit', 'certificado_data', 'clave_privada', 'punto_vta', 'ambiente', 'modo_autorizacion']
            for k in required:
                if not data.get(k):
                    logger.warning(f"Campo requerido faltante: {k}")
                    return jsonify({'error': f'Campo requerido faltante: {k}'}), 400

            if data['modo_autorizacion'] not in ['CAE', 'CAEA']:
                logger.warning("Modo de autorización inválido")
                return jsonify({'error': 'Modo de autorización inválido: debe ser CAE o CAEA'}), 400

            if not data['cuit'].isdigit() or len(data['cuit']) != 11:
                logger.warning("CUIT inválido")
                return jsonify({'error': 'CUIT inválido'}), 400

            stores = obtener_stores_from_parquet()
            if data['store_id'] not in stores:
                logger.warning(f"store_id inválido: {data['store_id']}")
                return jsonify({'error': f'Sucursal inválida: {data["store_id"]}'}), 400

            config_collection.update_one({'_id': 'config_arca'}, {'$set': data}, upsert=True)
            logger.info(f'Configuración ARCA actualizada por usuario {session.get("email")} (rol: {session.get("role")})')
            return jsonify({'message': 'Configuración guardada exitosamente'})
        except Exception as e:
            logger.error(f'Error inesperado al guardar config: {str(e)}', exc_info=True)
            return jsonify({'error': f'Error interno al guardar configuración: {str(e)}'}), 500

    try:
        config = config_collection.find_one({'_id': 'config_arca'}) or {}
        using_static_cert = not config.get('certificado_data') and os.path.exists(os.path.join(os.path.dirname(__file__), '../cert/certificado.pfx'))
        stores = obtener_stores_from_parquet()
        last_store = session.get('last_store', 'BA001GC')
        logger.info(f"Configuración ARCA cargada para {session.get('email')}. Usando certificado estático: {using_static_cert}, stores: {stores}, last_store: {last_store}")
        return render_template('facturacion_arca/config_arca.html', config=config, using_static_cert=using_static_cert, stores=stores, last_store=last_store)
    except Exception as e:
        logger.error(f'Error al obtener config: {str(e)}', exc_info=True)
        return jsonify({'error': f'Error interno al cargar configuración: {str(e)}'}), 500

@facturacion_arca_bp.route('/facturar', methods=['POST'])
@login_required
def facturar():
    """
    Genera una factura usando WSFEv1 de AFIP, obteniendo el número de comprobante desde el módulo de secuencias.
    """
    logger.debug(f"Iniciando facturación para usuario {session.get('email')}")
    data = request.get_json()
    if not data:
        logger.error("No se recibió datos JSON válidos")
        return jsonify({'error': 'No se recibieron datos válidos'}), 400

    try:
        logger.debug(f"Datos recibidos: {data}")
        config = config_collection.find_one({'_id': 'config_arca'})
        if not config:
            logger.warning("Configuración ARCA no encontrada en MongoDB")
            return jsonify({'error': 'Configuración ARCA no encontrada'}), 400

        store_id = data.get('store_id', session.get('last_store', 'BA001HE'))
        pto_vta = data.get('punto_venta', 587)
        tipo_cbte = data.get('tipo_cbte', '6')  # Factura B por defecto
        tipo_map = {
            '1': 'Factura_A',
            '6': 'Factura_B',
            '2': 'Nota_Credito_A',
            '7': 'Nota_Credito_B'
        }
        tipo_secuencia = tipo_map.get(str(tipo_cbte), 'Factura_B')

        # Obtener número de comprobante
        seq_response = requests.post(
            'http://localhost:5000/api/secuencias/obtener_siguiente',
            json={
                'tienda_id': store_id,
                'pto_venta_id': pto_vta,
                'tipo_secuencia': tipo_secuencia
            },
            headers={'X-Requested-With': 'XMLHttpRequest'},
            cookies={'session': request.cookies.get('session')}
        )
        if seq_response.status_code != 200:
            error = seq_response.json().get('error', 'Error al obtener número de secuencia')
            logger.error(f"Error al obtener secuencia para {store_id}/{pto_vta}/{tipo_secuencia}: {error}")
            return jsonify({'error': error}), seq_response.status_code

        seq_data = seq_response.json()
        nro_cbte = seq_data['secuencia_actual']
        logger.info(f"Número de comprobante obtenido: {seq_data['numero']} ({nro_cbte})")

        # Obtener TA
        token, sign = obtener_ta(config)
        history = HistoryPlugin()
        settings = Settings(strict=False, xml_huge_tree=True)
        wsdl = "https://wswhomo.afip.gov.ar/wsfev1/service.asmx?WSDL" if config.get('ambiente') == 'homologacion' else "https://servicios1.afip.gov.ar/wsfev1/service.asmx?WSDL"
        client = Client(wsdl=wsdl, settings=settings, plugins=[history])
        logger.info(f"Usando WSDL: {wsdl}")

        cuit_emisor = int(config['cuit'])
        auth = {
            "Token": token,
            "Sign": sign,
            "Cuit": cuit_emisor
        }

        # Modo Prueba
        if 'cart' not in data:
            logger.info("Procesando factura en modo prueba")
            cuit_receptor = int(data.get('doc_nro', 0))
            pto_vta = int(data.get('punto_venta', config['punto_venta']))
            neto = float(data.get('imp_neto', 0))
            iva_pct = float(data.get('iva_pct', 21.0)) / 100
            imp_iva = float(data.get('imp_iva', round(neto * iva_pct, 2)))
            imp_total = float(data.get('imp_total', round(neto + imp_iva, 2)))
            concepto = int(data.get('concepto', 1))
            fecha_cbte = data.get('fecha_cbte', datetime.now().strftime('%Y%m%d'))

            if neto > 9999999999999.99 or imp_iva > 9999999999999.99 or imp_total > 9999999999999.99:
                logger.warning("Importe excede el límite de AFIP")
                return jsonify({'error': 'El importe excede el límite permitido por AFIP'}), 400

            req = {
                "FeCabReq": {
                    "CantReg": 1,
                    "PtoVta": pto_vta,
                    "CbteTipo": int(tipo_cbte)
                },
                "FeDetReq": {
                    "FECAEDetRequest": [{
                        "Concepto": concepto,
                        "DocTipo": 96,  # CUIT
                        "DocNro": cuit_receptor,
                        "CbteDesde": nro_cbte,
                        "CbteHasta": nro_cbte,
                        "CbteFch": fecha_cbte,
                        "ImpTotal": round(imp_total, 2),
                        "ImpTotConc": 0.00,
                        "ImpNeto": round(neto, 2),
                        "ImpOpEx": 0.00,
                        "ImpIVA": round(imp_iva, 2),
                        "ImpTrib": 0.00,
                        "MonId": "PES",
                        "MonCotiz": 1.0,
                        "CanMisMonExt": "N",
                        "CondicionIVAReceptorId": 5,
                        "Iva": {
                            "AlicIva": [{
                                "Id": 5 if imp_iva > 0 else 3,
                                "BaseImp": round(neto, 2),
                                "Importe": round(imp_iva, 2)
                            }]
                        }
                    }]
                }
            }

        # Modo Carrito
        else:
            logger.info("Procesando factura desde carrito")
            if not data['cart'].get('client') or not data['cart']['client'].get('nif'):
                logger.warning("Falta información del cliente")
                return jsonify({'error': 'Debe seleccionar un cliente para facturar'}), 400

            cuit_receptor = int(data['cart']['client']['nif'] or data['cart']['client']['numero_cliente'] or '0')
            pto_vta = int(data.get('punto_venta', config['punto_venta']))
            items = data['cart']['items']

            neto = float(data.get('imp_neto', sum(float(item.get('price', 0)) * float(item.get('quantity', 0)) for item in items)))
            imp_iva = float(data.get('imp_iva', sum(
                (convertir_moneda_a_numero(item.get('precioLista', item.get('price', 0))) - float(item.get('price', 0))) *
                float(item.get('quantity', 0)) * 0.21 for item in items
            )))
            imp_total = float(data.get('imp_total', round(neto + imp_iva, 2)))

            if neto > 9999999999999.99 or imp_iva > 9999999999999.99 or imp_total > 9999999999999.99:
                logger.warning("Importe excede el límite de AFIP")
                return jsonify({'error': 'El importe excede el límite permitido por AFIP'}), 400

            neto = round(neto, 2)
            imp_iva = round(imp_iva, 2)
            imp_total = round(imp_total, 2)

            req = {
                "FeCabReq": {
                    "CantReg": 1,
                    "PtoVta": pto_vta,
                    "CbteTipo": int(tipo_cbte)
                },
                "FeDetReq": {
                    "FECAEDetRequest": [{
                        "Concepto": int(data.get('concepto', 1)),
                        "DocTipo": 96,
                        "DocNro": cuit_receptor,
                        "CbteDesde": nro_cbte,
                        "CbteHasta": nro_cbte,
                        "CbteFch": data.get('fecha_cbte', datetime.now().strftime('%Y%m%d')),
                        "ImpTotal": imp_total,
                        "ImpTotConc": 0.00,
                        "ImpNeto": neto,
                        "ImpOpEx": 0.00,
                        "ImpIVA": imp_iva,
                        "ImpTrib": 0.00,
                        "MonId": "PES",
                        "MonCotiz": 1.0,
                        "CanMisMonExt": "N",
                        "CondicionIVAReceptorId": 5,
                        "Iva": {
                            "AlicIva": [{
                                "Id": 5,
                                "BaseImp": neto,
                                "Importe": imp_iva
                            }]
                        }
                    }]
                }
            }

        logger.debug(f"Factura a enviar: {req}")
        result = client.service.FECAESolicitar(auth, req)
        logger.debug(f"Respuesta cruda de AFIP: {result}")

        if 'FeDetResp' in result:
            fe_det_resp = result['FeDetResp']
            if fe_det_resp.FECAEDetResponse and len(fe_det_resp.FECAEDetResponse) > 0:
                det_response = fe_det_resp.FECAEDetResponse[0]
                cae = det_response.CAE
                caefchvto = det_response.CAEFchVto
                if cae and caefchvto:
                    logger.info(f"Factura generada: CAE {cae}, Vencimiento {caefchvto}, Comprobante {nro_cbte}")
                    return jsonify({
                        'autorizacion': cae,
                        'vencimiento': caefchvto,
                        'nro_cbte': nro_cbte,
                        'message': 'Factura emitida exitosamente'
                    })
                else:
                    errors = getattr(result, 'Errors', None)
                    if errors and errors.Err and len(errors.Err) > 0:
                        error_msg = errors.Err[0].Msg
                        if 'no se corresponde con el proximo a autorizar' in error_msg.lower():
                            logger.warning(f"Error 10016: {error_msg}")
                            return jsonify({'error': f"El número de comprobante {nro_cbte} ya fue usado. Consulta FECompUltimoAutorizado."}), 400
                        logger.error(f"Error de AFIP: {error_msg}")
                        return jsonify({'error': error_msg}), 400
                    obs = getattr(det_response, 'Observaciones', None)
                    if obs and obs.Obs and len(obs.Obs) > 0:
                        obs_msg = obs.Obs[0].Msg
                        logger.warning(f"Observación de AFIP: {obs_msg}")
                        return jsonify({'error': obs_msg}), 400
            else:
                errors = getattr(result, 'Errors', None)
                if errors and errors.Err and len(errors.Err) > 0:
                    error_msg = errors.Err[0].Msg
                    logger.error(f"Error de AFIP: {error_msg}")
                    return jsonify({'error': error_msg}), 400
                return jsonify({'error': 'Respuesta de AFIP no contiene CAE ni errores claros'}), 400
        else:
            logger.error(f"Respuesta inesperada de AFIP: {result}")
            return jsonify({'error': 'Respuesta de AFIP no contiene FeDetResp'}), 400

    except ValueError as ve:
        logger.warning(f'Validación fallida en facturación: {str(ve)}')
        return jsonify({'error': str(ve)}), 400
    except Exception as e:
        logger.error(f'Error en facturación ARCA: {str(e)}', exc_info=True)
        return jsonify({'error': str(e)}), 500

def convertir_moneda_a_numero(valor):
    """
    Convierte un valor de moneda a número.
    """
    if not valor:
        return 0
    try:
        return float(str(valor).replace('.', '').replace(',', '.'))
    except ValueError:
        return 0

@facturacion_arca_bp.route('/solicitar_caea', methods=['GET'])
@login_required
def solicitar_caea():
    """
    Solicita un CAEA a AFIP.
    """
    try:
        config = config_collection.find_one({'_id': 'config_arca'})
        if not config:
            logger.warning("Configuración ARCA no encontrada")
            return jsonify({'error': 'Configuración ARCA no encontrada'}), 400

        token, sign = obtener_ta(config)
        history = HistoryPlugin()
        settings = Settings(strict=False, xml_huge_tree=True)
        wsdl = "https://wswhomo.afip.gov.ar/wsfev1/service.asmx?WSDL" if config.get('ambiente') == 'homologacion' else "https://servicios1.afip.gov.ar/wsfev1/service.asmx?WSDL"
        client = Client(wsdl=wsdl, settings=settings, plugins=[history])

        cuit_emisor = int(config['cuit'])
        auth = {
            "Token": token,
            "Sign": sign,
            "Cuit": cuit_emisor
        }

        periodo = datetime.now().strftime('%Y%m')
        orden = 1 if datetime.now().day <= 15 else 2
        req = {
            "Periodo": periodo,
            "Orden": orden
        }

        result = client.service.FECAEASolicitar(auth, req)
        logger.debug(f"Respuesta CAEA: {result}")

        if hasattr(result, 'ResultGet') and result.ResultGet:
            caea = result.ResultGet.CAEA
            caea_fch_vto = result.ResultGet.FchVigHasta
            logger.info(f"CAEA solicitado: {caea}, Vencimiento: {caea_fch_vto}")
            return jsonify({
                'caea': caea,
                'vencimiento': caea_fch_vto,
                'message': 'CAEA solicitado exitosamente'
            })
        else:
            errors = getattr(result, 'Errors', None)
            if errors and errors.Err:
                error_msg = errors.Err[0].Msg
                logger.error(f"Error al solicitar CAEA: {error_msg}")
                return jsonify({'error': error_msg}), 400
            return jsonify({'error': 'Respuesta de AFIP no contiene CAEA ni errores claros'}), 400

    except Exception as e:
        logger.error(f"Error al solicitar CAEA: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500
