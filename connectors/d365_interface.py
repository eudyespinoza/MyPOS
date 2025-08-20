import os
import httpx
from flask import session
import configparser
import logging
from datetime import datetime, timedelta
from services.email_service import enviar_correo_fallo
from db.database import obtener_contador_presupuesto
import uuid
import json
import asyncio

# Obtén la ruta absoluta a la raíz del proyecto
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(os.path.dirname(ROOT_DIR), 'config.ini')  # Config.ini en la raíz del proyecto

# Cargar la configuración
config = configparser.ConfigParser()
config.read(CONFIG_PATH)

LOG_DIR = os.path.join(ROOT_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "d365_interface.log")

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Archivo local para persistir números de presupuesto
PRESUPUESTOS_FILE = os.path.join(LOG_DIR, "presupuestos.json")


def guardar_numero_presupuesto(numero):
    """Guarda un número de presupuesto en un archivo JSON."""
    try:
        data = []
        if os.path.exists(PRESUPUESTOS_FILE):
            with open(PRESUPUESTOS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
        data.append({"numero": numero, "timestamp": datetime.utcnow().isoformat()})
        with open(PRESUPUESTOS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logging.info(f"Número de presupuesto {numero} guardado correctamente")
    except Exception as e:
        logging.error(f"Error al guardar número de presupuesto {numero}: {e}")


def obtener_numeros_presupuesto():
    """Devuelve la lista de números de presupuesto guardados."""
    try:
        if not os.path.exists(PRESUPUESTOS_FILE):
            return []
        with open(PRESUPUESTOS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"Error al obtener números de presupuesto: {e}")
        return []

def load_d365_config():
    if 'd365' not in config:
        raise KeyError("La sección 'd365' no se encuentra en config.ini")
    return {
        "client_prod": config['d365'].get('resource', ''),
        "client_qa": config['d365'].get('client_qa', ''),
        "client_id_prod": config['d365'].get('client_id_prod', ''),
        "client_id_qa": config['d365'].get('client_id_qa', ''),
        "client_secret_prod": config['d365'].get('client_secret_prod', ''),
        "client_secret_qa": config['d365'].get('client_secret_qa', ''),
    }

def generar_referencia_presupuesto():
    """Genera un número de referencia único en formato BUSCADOR-XXXXXXXXX usando el contador de la tabla misc."""
    try:
        contador = obtener_contador_presupuesto()
        referencia = f"BUSCADOR-{str(contador).zfill(9)}"
        logging.info(f"Referencia de presupuesto generada: {referencia}")
        return referencia
    except Exception as e:
        error = f"Error al generar referencia de presupuesto: {e}"
        enviar_correo_fallo("generar_referencia_presupuesto", error)
        logging.error(error)
        return f"BUSCADOR-ERROR-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"  # Fallback en caso de fallo

async def crear_presupuesto_batch(datos_cabecera, lineas, access_token):
    logging.info(f"Datos recibidos para crear presupuesto: cabecera={datos_cabecera}, lineas={lineas}")
    if not datos_cabecera or not lineas or not access_token:
        error = "Datos o token inválidos."
        enviar_correo_fallo("crear_presupuesto_batch", error)
        return None, error

    d365_config = load_d365_config()
    async with httpx.AsyncClient() as client:
        # Paso 1: Crear la cabecera individualmente
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {access_token}'
        }
        fecha_actual = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        fecha_expiracion = (datetime.utcnow() + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        referencia = generar_referencia_presupuesto()

        cabecera_payload = {
            "dataAreaId": "uni",
            "CashDiscountPercentage": 0,
            "CurrencyCode": "ARS",
            "SalesQuotationTypeId": datos_cabecera.get("tipo_presupuesto", "Caja"),
            "DefaultShippingSiteId": datos_cabecera.get("sitio", ""),
            "DefaultShippingWarehouseId": datos_cabecera.get("almacen_retiro", ""),
            "FixedExchangeRate": 0,
            "InvoiceCustomerAccountNumber": datos_cabecera.get("id_cliente", ""),
            "QuotationResponsiblePersonnelNumber": datos_cabecera.get("id_empleado", ""),
            "QuotationTakerPersonnelNumber": datos_cabecera.get("id_empleado", ""),
            "ReportingCurrencyFixedExchangeRate": 0,
            "RequestingCustomerAccountNumber": datos_cabecera.get("id_cliente", ""),
            "TotalDiscountPercentage": 0,
            "DeliveryModeCode": "Ret Suc",
            "CustomersReference": datos_cabecera.get("observaciones", ""),
            "SalesOrderOriginCode": datos_cabecera.get("store_id", ""),
            "DeliveryAddressLocationId": datos_cabecera.get("id_direccion", ""),
            "ReceiptDateRequested": fecha_actual,
            "RequestedShippingDate": fecha_actual,
            "SalesQuotationExpiryDate": fecha_expiracion,
            "CustomerRequisitionNumber": referencia,
            "SkipOpportunityCreationPrompt": "Yes"
        }
        logging.info(f"Enviando cabecera a {d365_config['client_prod']}/data/SalesQuotationHeadersV2: {json.dumps(cabecera_payload)}")

        try:
            response = await client.post(
                f"{d365_config['client_prod']}/data/SalesQuotationHeadersV2",
                headers=headers,
                json=cabecera_payload,
                timeout=httpx.Timeout(60)
            )
            response.raise_for_status()
            data = response.json()
            logging.info(f"Respuesta de cabecera completa: status={response.status_code}, body={response.text}")
            sales_quotation_number = data.get("SalesQuotationNumber")
            logging.info(f"Intento de extraer SalesQuotationNumber: {sales_quotation_number}")
            if not sales_quotation_number:
                error = "No se obtuvo SalesQuotationNumber en la respuesta de la cabecera."
                logging.error(f"{error} Respuesta completa: {response.text}")
                enviar_correo_fallo("crear_presupuesto_batch", f"{error} Respuesta: {response.text}")
                return None, error
            logging.info(f"Cabecera creada exitosamente: {sales_quotation_number}")
        except httpx.HTTPStatusError as e:
            error = f"Error HTTP al crear cabecera: {e}, Respuesta: {e.response.text}"
            logging.error(error)
            enviar_correo_fallo("crear_presupuesto_batch", error)
            return None, error
        except Exception as e:
            error = f"Error al crear cabecera: {str(e)}, Respuesta: {response.text if 'response' in locals() else 'N/A'}"
            logging.error(error)
            enviar_correo_fallo("crear_presupuesto_batch", error)
            return None, error

        # Paso 2: Crear las líneas en un lote
        batch_url = f"{d365_config['client_prod']}/data/$batch"
        batch_boundary = f"batch_{uuid.uuid4()}"
        changeset_boundary = f"changeset_{uuid.uuid4()}"

        batch_headers = {
            'Content-Type': f'multipart/mixed; boundary={batch_boundary}',
            'Authorization': f'Bearer {access_token}'
        }

        batch_body = [
            f"--{batch_boundary}",
            f"Content-Type: multipart/mixed; boundary={changeset_boundary}",
            ""
        ]

        for i, linea in enumerate(lineas):
            content_id_linea = str(uuid.uuid4())
            linea_payload = {
                "dataAreaId": "uni",
                "ItemNumber": linea.get('articulo', ''),
                "LineDiscountPercentage": 0,
                "RequestedSalesQuantity": linea.get('cantidad', 0),
                "SalesPrice": linea.get('precio', 0),
                "SalesQuotationNumber": sales_quotation_number,
                "ShippingSiteId": linea.get('sitio', ''),
                "ShippingWarehouseId": linea.get('almacen_entrega', '')
            }
            batch_body.extend([
                f"--{changeset_boundary}",
                "Content-Type: application/http",
                "Content-Transfer-Encoding: binary",
                f"Content-ID: {content_id_linea}",
                "",
                f"POST {d365_config['client_prod']}/data/SalesQuotationLines HTTP/1.1",
                "Content-Type: application/json",
                "",
                json.dumps(linea_payload)
            ])

        batch_body.extend([f"--{changeset_boundary}--", f"--{batch_boundary}--"])
        batch_body_str = "\r\n".join(batch_body)
        logging.info(f"Cuerpo del lote OData para líneas: {batch_body_str}")

        try:
            response = await client.post(
                batch_url,
                headers=batch_headers,
                data=batch_body_str,
                timeout=httpx.Timeout(120)
            )
            logging.info(f"Respuesta del servidor al lote: status={response.status_code}, headers={response.headers}")
            response.raise_for_status()

            response_lines = response.text.splitlines()
            logging.info(f"Respuesta del lote de líneas completa: {response.text}")
            errores = []
            for i, line in enumerate(response_lines):
                if line.startswith("HTTP/1.1"):
                    status_code = int(line.split(" ")[1])
                    if status_code >= 400:  # Solo errores 400 o superiores
                        errores.append(f"Error en operación {i}: {line}")
                        logging.warning(f"Detectado error real en operación {i}: {line}")

            if errores:
                error = f"Errores al crear líneas: {errores}. Respuesta completa: {response.text}"
                logging.error(error)
                enviar_correo_fallo("crear_presupuesto_batch", error)
                return sales_quotation_number, error
            logging.info(f"Presupuesto completo creado exitosamente: {sales_quotation_number}")
            session['new_quotation'] = sales_quotation_number
            return sales_quotation_number, None

        except httpx.HTTPStatusError as e:
            error = f"Error HTTP al crear líneas: {e}, Respuesta: {e.response.text}"
            logging.error(error)
            enviar_correo_fallo("crear_presupuesto_batch", error)
            return sales_quotation_number, error
        except Exception as e:
            error = f"Error al crear líneas en batch: {str(e)}, Respuesta: {response.text if 'response' in locals() else 'N/A'}"
            logging.error(error)
            enviar_correo_fallo("crear_presupuesto_batch", error)
            return sales_quotation_number, error

async def obtener_presupuesto_d365(quotation_id, access_token):
    """Recupera los datos de un presupuesto existente desde D365."""
    logging.info(f"Recuperando presupuesto D365: {quotation_id}")
    if not quotation_id or not access_token:
        error = "ID de presupuesto o token inválidos."
        logging.error(error)
        enviar_correo_fallo("obtener_presupuesto_d365", error)
        return None, error

    d365_config = load_d365_config()
    async with httpx.AsyncClient() as client:
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {access_token}'
        }

        # Obtener líneas del presupuesto
        lines_url = f"{d365_config['client_prod']}/data/SalesQuotationLines?$filter=SalesQuotationNumber eq '{quotation_id}'&$select=InventoryLotId,ItemNumber,RequestingCustomerAccountNumber,SalesQuotationNumber,SalesPrice,RequestedSalesQuantity,SalesUnitSymbol,ShippingSiteId,ShippingWarehouseId"
        try:
            lines_response = await client.get(lines_url, headers=headers, timeout=httpx.Timeout(30))
            lines_response.raise_for_status()
            lines_data = lines_response.json().get("value", [])
            logging.info(f"Líneas obtenidas para {quotation_id}: {len(lines_data)}")
        except httpx.HTTPStatusError as e:
            error = f"Error HTTP al obtener líneas: {e}, Respuesta: {e.response.text}"
            logging.error(error)
            enviar_correo_fallo("obtener_presupuesto_d365", error)
            return None, error
        except Exception as e:
            error = f"Error al obtener líneas: {str(e)}"
            logging.error(error)
            enviar_correo_fallo("obtener_presupuesto_d365", error)
            return None, error

        if not lines_data:
            error = f"No se encontraron líneas para el presupuesto {quotation_id}"
            logging.info(error)
            return None, error

        # Obtener cabecera del presupuesto con campos adicionales
        header_url = f"{d365_config['client_prod']}/data/SalesQuotationHeadersV2?$filter=SalesQuotationNumber eq '{quotation_id}'&$select=SalesQuotationNumber,InvoiceCustomerAccountNumber,CustomersReference,SalesOrderOriginCode,ReceiptDateRequested,SalesQuotationStatus,GeneratedSalesOrderNumber"
        try:
            header_response = await client.get(header_url, headers=headers, timeout=httpx.Timeout(30))
            header_response.raise_for_status()
            header_data = header_response.json().get("value", [])[0] if header_response.json().get("value") else {}
            logging.info(f"Cabecera obtenida para {quotation_id}")
        except httpx.HTTPStatusError as e:
            error = f"Error HTTP al obtener cabecera: {e}, Respuesta: {e.response.text}"
            logging.error(error)
            enviar_correo_fallo("obtener_presupuesto_d365", error)
            return None, error
        except Exception as e:
            error = f"Error al obtener cabecera: {str(e)}"
            logging.error(error)
            enviar_correo_fallo("obtener_presupuesto_d365", error)
            return None, error

        # Combinar datos en una respuesta
        presupuesto_data = {
            "header": header_data,
            "lines": lines_data
        }
        logging.info(f"Presupuesto D365 {quotation_id} recuperado exitosamente")
        return presupuesto_data, None

async def actualizar_presupuesto_d365(quotation_id, datos_cabecera, lineas_nuevas, lineas_existentes, access_token):
    """Actualiza un presupuesto existente en D365: elimina todas las líneas, agrega las nuevas y actualiza la cabecera."""
    logging.info(f"Actualizando presupuesto D365: {quotation_id}")
    if not quotation_id or not datos_cabecera or not lineas_nuevas or not access_token:
        error = "ID de presupuesto, datos, líneas nuevas o token inválidos."
        logging.error(error)
        enviar_correo_fallo("actualizar_presupuesto_d365", error)
        return None, error

    d365_config = load_d365_config()
    async with httpx.AsyncClient() as client:
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {access_token}'
        }

        # Primer Batch: Eliminar todas las líneas existentes
        if lineas_existentes:
            batch_url = f"{d365_config['client_prod']}/data/$batch"
            batch_boundary = f"batch_{uuid.uuid4()}"
            changeset_boundary = f"changeset_{uuid.uuid4()}"

            batch_headers = {
                'Content-Type': f'multipart/mixed; boundary={batch_boundary}',
                'Authorization': f'Bearer {access_token}'
            }

            batch_body = [
                f"--{batch_boundary}",
                f"Content-Type: multipart/mixed; boundary={changeset_boundary}",
                ""
            ]

            for i, line in enumerate(lineas_existentes):
                inventory_lot_id = line.get("InventoryLotId")
                if not inventory_lot_id:
                    logging.warning(f"Línea {i} no tiene InventoryLotId, omitiendo eliminación: {line}")
                    continue
                batch_body.extend([
                    f"--{changeset_boundary}",
                    "Content-Type: application/http",
                    "Content-Transfer-Encoding: binary",
                    "",
                    f"DELETE {d365_config['client_prod']}/data/SalesQuotationLines(dataAreaId='UNI',InventoryLotId='{inventory_lot_id}')?cross-company=true HTTP/1.1",
                    f"Content-ID: {i+1}",
                    f"From: SalesQuotationLines(dataAreaId='UNI',InventoryLotId='{inventory_lot_id}')?cross-company=true",
                    ""
                ])

            batch_body.extend([f"--{changeset_boundary}--", f"--{batch_boundary}--"])
            batch_body_str = "\r\n".join(batch_body)
            logging.info(f"Cuerpo del lote OData para eliminación de líneas: {batch_body_str}")

            try:
                response = await client.post(
                    batch_url,
                    headers=batch_headers,
                    data=batch_body_str,
                    timeout=httpx.Timeout(120)
                )
                logging.info(f"Respuesta del servidor al lote de eliminación: status={response.status_code}, headers={response.headers}")
                response.raise_for_status()

                response_lines = response.text.splitlines()
                logging.info(f"Respuesta del lote de eliminación completa: {response.text}")
                errores = []
                for i, line in enumerate(response_lines):
                    if line.startswith("HTTP/1.1"):
                        status_code = int(line.split(" ")[1])
                        if status_code >= 400:
                            errores.append(f"Error en operación {i}: {line}")
                            logging.warning(f"Detectado error en operación de eliminación {i}: {line}")

                if errores:
                    error = f"Errores al eliminar líneas: {errores}. Respuesta completa: {response.text}"
                    logging.error(error)
                    enviar_correo_fallo("actualizar_presupuesto_d365", error)
                    return None, error
                logging.info(f"Todas las líneas existentes eliminadas para {quotation_id}")
            except httpx.HTTPStatusError as e:
                error = f"Error HTTP al eliminar líneas: {e}, Respuesta: {e.response.text}"
                logging.error(error)
                enviar_correo_fallo("actualizar_presupuesto_d365", error)
                return None, error
            except Exception as e:
                error = f"Error al eliminar líneas en batch: {str(e)}, Respuesta: {response.text if 'response' in locals() else 'N/A'}"
                logging.error(error)
                enviar_correo_fallo("actualizar_presupuesto_d365", error)
                return None, error

        # Segundo Batch: Crear las nuevas líneas y actualizar la cabecera
        batch_url = f"{d365_config['client_prod']}/data/$batch"
        batch_boundary = f"batch_{uuid.uuid4()}"
        changeset_boundary = f"changeset_{uuid.uuid4()}"

        batch_headers = {
            'Content-Type': f'multipart/mixed; boundary={batch_boundary}',
            'Authorization': f'Bearer {access_token}'
        }

        batch_body = [
            f"--{batch_boundary}",
            f"Content-Type: multipart/mixed; boundary={changeset_boundary}",
            ""
        ]

        # Agregar la solicitud PATCH para actualizar la cabecera con fechas
        cabecera_payload = {
            "DeliveryModeCode": "Ret Suc",
            "CustomersReference": datos_cabecera.get("observaciones", ""),
            "ReceiptDateRequested": datos_cabecera.get("ReceiptDateRequested", ""),
            "RequestedShippingDate": datos_cabecera.get("RequestedShippingDate", ""),
            "SalesQuotationExpiryDate": datos_cabecera.get("SalesQuotationExpiryDate", "")
        }
        batch_body.extend([
            f"--{changeset_boundary}",
            "Content-Type: application/http",
            "Content-Transfer-Encoding: binary",
            "",
            f"PATCH {d365_config['client_prod']}/data/SalesQuotationHeadersV2(dataAreaId='UNI',SalesQuotationNumber='{quotation_id}')?cross-company=true HTTP/1.1",
            "Content-ID: 1",
            "Accept: application/json;q=0.9, */*;q=0.1",
            "OData-Version: 4.0",
            "Content-Type: application/json",
            "OData-MaxVersion: 4.0",
            "",
            json.dumps(cabecera_payload)
        ])

        # Agregar las solicitudes POST para las nuevas líneas
        for i, linea in enumerate(lineas_nuevas):
            content_id_linea = str(i + 2)  # Comenzar desde 2 porque 1 es la cabecera
            linea_payload = {
                "dataAreaId": "uni",
                "ItemNumber": linea.get('articulo', ''),
                "LineDiscountPercentage": 0,
                "RequestedSalesQuantity": linea.get('cantidad', 0),
                "SalesPrice": linea.get('precio', 0),
                "SalesQuotationNumber": quotation_id,
                "ShippingSiteId": linea.get('sitio', ''),
                "ShippingWarehouseId": linea.get('almacen_entrega', '')
            }
            batch_body.extend([
                f"--{changeset_boundary}",
                "Content-Type: application/http",
                "Content-Transfer-Encoding: binary",
                f"Content-ID: {content_id_linea}",
                "",
                f"POST {d365_config['client_prod']}/data/SalesQuotationLines HTTP/1.1",
                "Content-Type: application/json",
                "",
                json.dumps(linea_payload)
            ])

        batch_body.extend([f"--{changeset_boundary}--", f"--{batch_boundary}--"])
        batch_body_str = "\r\n".join(batch_body)
        logging.info(f"Cuerpo del lote OData para nuevas líneas y cabecera: {batch_body_str}")

        try:
            response = await client.post(
                batch_url,
                headers=batch_headers,
                data=batch_body_str,
                timeout=httpx.Timeout(120)
            )
            logging.info(f"Respuesta del servidor al lote de creación: status={response.status_code}, headers={response.headers}")
            response.raise_for_status()

            response_lines = response.text.splitlines()
            logging.info(f"Respuesta del lote de creación completa: {response.text}")
            errores = []
            for i, line in enumerate(response_lines):
                if line.startswith("HTTP/1.1"):
                    status_code = int(line.split(" ")[1])
                    if status_code >= 400:
                        errores.append(f"Error en operación {i}: {line}")
                        logging.warning(f"Detectado error en operación de creación {i}: {line}")

            if errores:
                error = f"Errores al crear líneas o actualizar cabecera: {errores}. Respuesta completa: {response.text}"
                logging.error(error)
                enviar_correo_fallo("actualizar_presupuesto_d365", error)
                return quotation_id, error
            logging.info(f"Presupuesto {quotation_id} actualizado exitosamente: nuevas líneas creadas y cabecera actualizada")
            session['updated_quotation'] = quotation_id
            return quotation_id, None

        except httpx.HTTPStatusError as e:
            error = f"Error HTTP al crear líneas o actualizar cabecera: {e}, Respuesta: {e.response.text}"
            logging.error(error)
            enviar_correo_fallo("actualizar_presupuesto_d365", error)
            return quotation_id, error
        except Exception as e:
            error = f"Error al crear líneas o actualizar cabecera en batch: {str(e)}, Respuesta: {response.text if 'response' in locals() else 'N/A'}"
            logging.error(error)
            enviar_correo_fallo("actualizar_presupuesto_d365", error)
            return quotation_id, error

async def validar_cliente_existente(dni, access_token):
    """Valida si un cliente ya existe en D365 basado en el DNI (TaxExemptNumber)."""
    logging.info(f"Validando cliente existente con DNI: {dni}")
    d365_config = load_d365_config()
    url = f"{d365_config['client_prod']}/data/CustomersV3?$filter=TaxExemptNumber eq '{dni}'"
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }

    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url, headers=headers, timeout=httpx.Timeout(30))
            response.raise_for_status()
            data = response.json()
            clientes = data.get("value", [])
            if clientes:
                logging.info(f"Cliente encontrado con DNI {dni}: {clientes[0]}")
                return True, clientes[0]  # Cliente existe, devolvemos datos
            logging.info(f"No se encontró cliente con DNI {dni}")
            return False, None  # Cliente no existe
        except httpx.HTTPStatusError as e:
            error = f"Error HTTP al validar cliente: {e}, Respuesta: {e.response.text}"
            logging.error(error)
            enviar_correo_fallo("validar_cliente_existente", error)
            return None, error
        except Exception as e:
            error = f"Error al validar cliente: {str(e)}"
            logging.error(error)
            enviar_correo_fallo("validar_cliente_existente", error)
            return None, error

async def alta_cliente_d365(datos_cliente, access_token):
    """Crea un nuevo cliente en Dynamics 365, registrando primero el DNI en VATNumTables."""
    logging.info(f"Creando cliente en D365 con datos: {datos_cliente}")
    d365_config = load_d365_config()
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {access_token}'
    }

    # Paso 1: Registrar el DNI en VATNumTables
    vat_url = f"{d365_config['client_prod']}/data/VATNumTables"
    vat_payload = {
        "dataAreaId": "uni",
        "VATNum": datos_cliente['dni'],
        "CountryRegionId": "ARG",
        "Name": f"{datos_cliente['nombre']} {datos_cliente['apellido']}",
        "AxxTaxFiscalIdentificationType_TaxFiscalIdentificationId": "DNI"
    }
    async with httpx.AsyncClient() as client:
        try:
            vat_response = await client.post(vat_url, headers=headers, json=vat_payload, timeout=httpx.Timeout(60))
            vat_response.raise_for_status()
            logging.info(f"DNI {datos_cliente['dni']} registrado exitosamente en VATNumTables")
        except httpx.HTTPStatusError as e:
            error = f"Error HTTP al registrar DNI en VATNumTables: {e}, Respuesta: {e.response.text}"
            logging.error(error)
            enviar_correo_fallo("alta_cliente_d365 - VATNumTables", error)
            return None, error
        except Exception as e:
            error = f"Error al registrar DNI en VATNumTables: {str(e)}"
            logging.error(error)
            enviar_correo_fallo("alta_cliente_d365 - VATNumTables", error)
            return None, error

    # Paso 2: Crear el cliente en CustomersV3
    customer_url = f"{d365_config['client_prod']}/data/CustomersV3"
    fecha_actual = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    customer_payload = {
        "AccountStatement": "Always",
        "AddressBooks": "FBM-CLN",
        "dataAreaId": "uni",
        "AxxTaxFiscalIdentificationType_TaxFiscalIdentificationId": "DNI",
        "CreditLimit": 5,
        "CreditLimitIsMandatory": "Yes",
        "CustomerGroupId": "C-CF",
        "LanguageId": "es",
        "NameAlias": f"{datos_cliente['nombre']} {datos_cliente['apellido']}",
        "OnHoldStatus": "No",
        "PersonFirstName": datos_cliente['nombre'],
        "PersonLastName": datos_cliente['apellido'],
        "PartyType": "Person",
        "PrimaryContactEmail": datos_cliente['email'],
        "PrimaryContactPhone": datos_cliente['telefono'],
        "SalesCurrencyCode": "ARS",
        "SalesTaxGroup": "C-CF",
        "TaxExemptNumber": datos_cliente['dni'],
        "CustomerType": "None",
        "CompanyType": "Blank",
        "AxxTaxVATConditionId": "CF",
        "AxxTaxPersonType": "Physic",
        "AddressCity": datos_cliente['ciudad'],
        "AddressStreet": datos_cliente['calle'],
        "AddressStreetNumber": datos_cliente['altura'],
        "AddressBuildingComplement": datos_cliente.get('referencia', ''),
        "AddressZipCode": datos_cliente['codigo_postal'],
        "AddressCountryRegionId": "ARG",
        "AddressState": datos_cliente['estado'],
        "AddressCounty": datos_cliente['condado'],
        "InvoiceAddressLongitude": datos_cliente.get('longitud', 0),
        "InvoiceAddressLatitude": datos_cliente.get('latitud', 0),
        "AddressDescription": "Casa",
        "AddressValidFrom": fecha_actual,
        "AddressValidTo": "2154-12-31T23:59:59Z",
        "AxxTaxPCGrossIncAgreeType": "NotInscript"
    }

    async with httpx.AsyncClient() as client:
        try:
            logging.info(customer_payload)
            response = await client.post(customer_url, headers=headers, json=customer_payload, timeout=httpx.Timeout(60))
            response.raise_for_status()
            data = response.json()
            customer_id = data.get("CustomerAccount", None)
            logging.info(response.text)
            if not customer_id:
                error = "No se obtuvo CustomerAccount en la respuesta."
                logging.error(f"{error} Respuesta: {response.text}")
                enviar_correo_fallo("alta_cliente_d365", error)
                return None, error
            logging.info(f"Cliente creado exitosamente: {customer_id}")
            return customer_id, None
        except httpx.HTTPStatusError as e:
            error = f"Error HTTP al crear cliente: {e}, Respuesta: {e.response.text}"
            logging.error(error)
            enviar_correo_fallo("alta_cliente_d365", error)
            return None, error
        except Exception as e:
            error = f"Error al crear cliente: {str(e)}"
            logging.error(error)
            enviar_correo_fallo("alta_cliente_d365", error)
            return None, error


# Función síncrona para integrar con Flask
def run_validar_cliente_existente(dni, access_token):
    return asyncio.run(validar_cliente_existente(dni, access_token))

def run_alta_cliente_d365(datos_cliente, access_token):
    return asyncio.run(alta_cliente_d365(datos_cliente, access_token))

def run_crear_presupuesto_batch(datos_cabecera, lineas, access_token):
    return asyncio.run(crear_presupuesto_batch(datos_cabecera, lineas, access_token))

def run_obtener_presupuesto_d365(quotation_id, access_token):
    return asyncio.run(obtener_presupuesto_d365(quotation_id, access_token))

def run_actualizar_presupuesto_d365(quotation_id, datos_cabecera, lineas_nuevas, lineas_existentes, access_token):
    return asyncio.run(actualizar_presupuesto_d365(quotation_id, datos_cabecera, lineas_nuevas, lineas_existentes, access_token))

