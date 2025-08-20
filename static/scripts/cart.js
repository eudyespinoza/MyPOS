/***************************************
 * Archivo: cart.js
 * Descripción: Lógica del carrito de compras, incluyendo validación de cantidad, actualización de display, facturación condicional, y gestión de clientes.
 ***************************************/

// Variables globales (deben ser accesibles desde scripts.js)
let cart = {
    items: [],
    client: null,
    quotation_id: null,
    type: 'new',
    observations: ''
}; // Array para almacenar los ítems del carrito
let currentProductToAdd = null; // Producto seleccionado para agregar al carrito
let selectedClient = null;
let cartObservations = "";
let lastQuotationNumber = null;
let db;

const DB_NAME = 'CartDB';
const DB_VERSION = 1;
const CART_STORE = 'carts';

/***************************************
 * Funciones del carrito
 ***************************************/

function validarCantidad(multiplo, cantidad) {
    try {
        // Asegurar que multiplo sea un número válido
        const multiploValido = (multiplo === null || multiplo === undefined || isNaN(multiplo) || multiplo <= 0) ? 1 : parseFloat(multiplo.toFixed(2));
        // Asegurar que cantidad sea un número válido
        const cantidadNum = isNaN(cantidad) ? 1 : parseFloat(cantidad);
        const tolerance = 0.0001;
        const cantidadRedondeada = Number(cantidadNum.toFixed(2));
        if (Math.abs(cantidadRedondeada % multiploValido) < tolerance || multiploValido === 1) {
            return cantidadRedondeada;
        } else {
            const cantidadAjustada = Math.ceil(cantidadRedondeada / multiploValido) * multiploValido;
            return Number(cantidadAjustada.toFixed(2));
        }
    } catch (error) {
        console.error('DEBUG: Error en validarCantidad:', error);
        return 1; // Valor por defecto en caso de error
    }
}

function calcularCajas(cantidad, multiplo, unidadMedida) {
    if (!["m2", "M2"].includes(unidadMedida)) return ""; // Solo para m2 o M2
    const multiploValido = (multiplo === null || multiplo === undefined || multiplo <= 0) ? 1 : parseFloat(multiplo);
    const multiploRedondeado = Number(multiploValido.toFixed(2));
    const cantidadRedondeada = Number(cantidad.toFixed(2));
    const cajas = cantidadRedondeada / multiploRedondeado;
    return `Equivalente a ${cajas.toFixed(0)} caja${cajas === 1 ? "" : "s"}`;
}

function showQuantityModal(event, productId, productName, price) {
    event.stopPropagation();
    const parsedPrice = convertirMonedaANumero(String(price));
    const product = products.find(p => p.numero_producto === productId) || {
        numero_producto: productId,
        nombre_producto: productName,
        precio_final_con_descuento: price,
        precio_final_con_iva: price, // Valor por defecto si no se encuentra el producto
        multiplo: 1,
        unidad_medida: "Un"
    };
    const multiplo = product ? Number(product.multiplo.toFixed(2)) : 1;
    const unidadMedida = product ? product.unidad_medida : "Un";
    const precioLista = convertirMonedaANumero(product.precio_final_con_iva);
    currentProductToAdd = {
        productId,
        productName,
        price: parsedPrice,
        multiplo,
        unidadMedida,
        precioLista // Nuevo campo para precio sin descuento
    };
    document.getElementById("quantityModalProductName").textContent = productName;
    document.getElementById("quantityModalProductPrice").textContent = `$${formatearMoneda(parsedPrice)}`;
    const input = document.getElementById("quantityInput");
    input.value = multiplo;
    input.min = multiplo;
    document.getElementById("quantityModalUnitMeasure").textContent = unidadMedida;
    const cantidadInicial = validarCantidad(multiplo, multiplo);
    input.value = cantidadInicial;
    const cajasElement = document.getElementById("quantityModalCajas");
    cajasElement.textContent = calcularCajas(cantidadInicial, multiplo, unidadMedida);
    updateTotal();
    const modalElement = document.getElementById("quantityModal");
    const modal = new bootstrap.Modal(modalElement);
    modal.show();
    modalElement.addEventListener('shown.bs.modal', function () {
        input.focus();
    }, { once: true });
    input.addEventListener('keydown', function (e) {
        if (e.key === 'Enter') {
            e.preventDefault();
            const addButton = document.querySelector('#quantityModal .modal-footer .btn-primary');
            if (addButton) {
                addButton.focus();
            }
        }
    });
}

function adjustQuantity(delta) {
    const input = document.getElementById("quantityInput");
    let quantity = parseFloat(input.value) || currentProductToAdd.multiplo; // Usar multiplo como valor inicial si está vacío
    const step = delta * currentProductToAdd.multiplo; // Calcular el paso basado en el multiplo
    // Ajustar la cantidad sumando el paso
    quantity += step;
    // Asegurar que no baje del mínimo (multiplo)
    if (quantity < currentProductToAdd.multiplo) {
        quantity = currentProductToAdd.multiplo;
    }
    // Validar y ajustar al múltiplo correcto
    const cantidadValidada = validarCantidad(currentProductToAdd.multiplo, quantity);
    input.value = cantidadValidada;
    // Actualizar equivalencia y total
    const cajasElement = document.getElementById("quantityModalCajas");
    cajasElement.textContent = calcularCajas(cantidadValidada, currentProductToAdd.multiplo, currentProductToAdd.unidadMedida);
    updateTotal();
}

function updateTotal() {
    const quantity = parseFloat(document.getElementById("quantityInput").value) || currentProductToAdd.multiplo;
    const total = currentProductToAdd.price * Number(quantity.toFixed(2)); // Redondear cantidad para cálculo
    document.getElementById("quantityModalTotal").textContent = `$${formatearMoneda(total)}`;
    // Actualizar equivalencia al cambiar la cantidad
    const cajasElement = document.getElementById("quantityModalCajas");
    cajasElement.textContent = calcularCajas(quantity, currentProductToAdd.multiplo, currentProductToAdd.unidadMedida);
}

function addToCartConfirmed() {
    const quantityInput = document.getElementById("quantityInput");
    const quantity = parseFloat(quantityInput.value);
    const modal = bootstrap.Modal.getInstance(document.getElementById("quantityModal"));
    const addButton = document.querySelector('#quantityModal .modal-footer .btn-primary');
    if (!currentProductToAdd || isNaN(quantity) || quantity <= 0) {
        showToast('danger', 'Cantidad inválida');
        return;
    }
    addButton.disabled = true;
    const existingItemIndex = cart.items.findIndex(item => item.productId === currentProductToAdd.productId);
    if (existingItemIndex !== -1) {
        cart.items[existingItemIndex].quantity += quantity;
    } else {
        cart.items.push({
            productId: currentProductToAdd.productId,
            productName: currentProductToAdd.productName,
            price: currentProductToAdd.price,
            precioLista: currentProductToAdd.precioLista,
            quantity: quantity,
            multiplo: currentProductToAdd.multiplo,
            unidadMedida: currentProductToAdd.unidadMedida,
            available: true // Asumimos disponible inicialmente
        });
    }
    updateCartDisplay();
    modal.hide();
    addButton.disabled = false;
    showToast('success', `Producto "${currentProductToAdd.productName}" agregado al carrito`);
    currentProductToAdd = null;
    saveCartToIndexedDB();
}

function removeFromCart(productId) {
    const itemIndex = cart.items.findIndex(item => item.productId === productId);
    if (itemIndex !== -1) {
        cart.items.splice(itemIndex, 1);
        updateCartDisplay();
        saveCartToIndexedDB();
        showToast('info', 'Producto eliminado del carrito');
    }
}

function updateCartDisplay() {
    const cartTable = document.getElementById('cartTable');
    const cartTotal = document.getElementById('cartTotal');
    const clientInfo = document.getElementById('cartClientInfo');
    const cartObservationsInput = document.getElementById('cartObservations');

    if (!cartTable || !cartTotal || !clientInfo) {
        console.error('Elementos del DOM no encontrados para actualizar el carrito');
        return;
    }

    cartTable.innerHTML = '';
    let total = 0;

    if (cart.items.length === 0) {
        cartTable.innerHTML = '<tr><td colspan="5" class="text-center text-muted">El carrito está vacío</td></tr>';
    } else {
        cart.items.forEach(item => {
            const itemTotal = item.price * item.quantity;
            total += itemTotal;
            const row = document.createElement('tr');
            row.innerHTML = `
                <td>${item.productId}</td>
                <td>${item.productName}</td>
                <td>
                    <input type="number" class="form-control form-control-sm quantity-input" value="${item.quantity}" min="${item.multiplo}" step="${item.multiplo}" data-product-id="${item.productId}">
                </td>
                <td>
                    <input type="number" class="form-control form-control-sm price-input" value="${item.price}" min="0" step="0.01" data-product-id="${item.productId}">
                </td>
                <td class="line-total">$${formatearMoneda(itemTotal)}</td>
                <td><button class="btn btn-danger btn-sm" onclick="removeFromCart('${item.productId}')">Eliminar</button></td>
            `;
            cartTable.appendChild(row);

            // Agregar evento para actualizar cantidad
            const quantityInput = row.querySelector('.quantity-input');
            quantityInput.addEventListener('change', (e) => {
                const newQuantity = validarCantidad(item.multiplo, parseFloat(e.target.value));
                if (newQuantity !== item.quantity) {
                    item.quantity = newQuantity;
                    updateCartDisplay();
                    saveCartToIndexedDB();
                }
            });

            // Agregar evento para actualizar precio
            const priceInput = row.querySelector('.price-input');
            priceInput.addEventListener('change', (e) => {
                const newPrice = parseFloat(e.target.value);
                if (!isNaN(newPrice) && newPrice !== item.price) {
                    item.price = newPrice;
                    updateCartDisplay();
                    saveCartToIndexedDB();
                }
            });
        });
    }

    cartTotal.textContent = `$${formatearMoneda(total)}`;
    if (cart.client) {
        clientInfo.innerHTML = `
            Nombre: ${cart.client.nombre_cliente}<br>
            DNI: ${cart.client.nif}<br>
            Número Cliente: ${cart.client.numero_cliente}<br>
            Dirección: ${cart.client.direccion_completa}<br>
            <button class="btn btn-link" onclick="showClientDetails()">Detalles</button>
            <button class="btn btn-danger btn-sm" onclick="removeClientFromCart()">Eliminar</button>
        `;
    } else {
        clientInfo.innerHTML = 'Sin cliente';
    }
    if (cartObservationsInput) {
        cartObservationsInput.value = cart.observations || '';
    }
}

function showClientDetails() {
    if (!cart.client) {
        showToast('warning', 'No hay cliente seleccionado');
        return;
    }
    const modal = document.getElementById('clientDetailsModal');
    const modalBody = modal.querySelector('.modal-body');
    modalBody.innerHTML = `
        <div class="row">
            <div class="col-12">
                <h6>Información Personal</h6>
                <p>Nombre: ${cart.client.nombre_cliente}</p>
                <p>Número Cliente: ${cart.client.numero_cliente}</p>
                <p>Bloqueado: ${cart.client.bloqueado}</p>
                <p>Tipo Contribuyente: ${cart.client.tipo_contribuyente}</p>
            </div>
            <div class="col-12">
                <h6>Datos Fiscales</h6>
                <p>Límite Crédito: ${cart.client.limite_credito || 'N/A'}</p>
                <p>Grupo Impuestos: ${cart.client.grupo_impuestos}</p>
                <p>DNI (NIF): ${cart.client.nif}</p>
                <p>NIF (TIF): ${cart.client.tif}</p>
            </div>
            <div class="col-12">
                <h6>Contacto</h6>
                <p>Dirección: ${cart.client.direccion_completa}</p>
                <p>Email: ${cart.client.email_contacto || 'N/A'}</p>
                <p>Teléfono: ${cart.client.telefono_contacto || 'N/A'}</p>
            </div>
            <div class="col-12">
                <h6>Fechas</h6>
                <p>Fecha Creación: ${cart.client.fecha_creacion || 'N/A'}</p>
                <p>Fecha Modificación: ${cart.client.fecha_modificacion || 'N/A'}</p>
            </div>
        </div>
    `;
    new bootstrap.Modal(modal).show();
}

function removeClientFromCart() {
    if (!cart.client) {
        showToast('warning', 'No hay cliente para eliminar');
        return;
    }
    const modal = document.getElementById('removeClientWarningModal');
    const modalBody = modal.querySelector('.modal-body');
    modalBody.innerHTML = `
        <p>Eliminar el cliente asociado al presupuesto ${cart.quotation_id || 'N/A'} generará un nuevo presupuesto. ¿Estás seguro de continuar?</p>
    `;
    new bootstrap.Modal(modal).show();

    const confirmButton = modal.querySelector('#confirmRemoveClient');
    const cancelButton = modal.querySelector('#cancelRemoveClient');
    confirmButton.addEventListener('click', () => {
        cart.client = null;
        cart.quotation_id = null;
        cart.type = 'new';
        updateCartDisplay();
        saveCartToIndexedDB();
        showToast('success', 'Cliente eliminado y presupuesto reiniciado');
        bootstrap.Modal.getInstance(modal).hide();
    }, { once: true });
    cancelButton.addEventListener('click', () => {
        bootstrap.Modal.getInstance(modal).hide();
    }, { once: true });
}

function selectClient(client) {
    selectedClient = client;
    cart.client = {
        numero_cliente: client.numero_cliente,
        nombre_cliente: client.nombre_cliente,
        nif: client.nif,
        direccion_completa: client.direccion_completa,
        email_contacto: client.email_contacto,
        telefono_contacto: client.telefono_contacto,
        bloqueado: client.bloqueado,
        tipo_contribuyente: client.tipo_contribuyente,
        limite_credito: client.limite_credito,
        grupo_impuestos: client.grupo_impuestos,
        tif: client.tif,
        fecha_creacion: client.fecha_creacion,
        fecha_modificacion: client.fecha_modificacion
    };
    updateCartDisplay();
    saveCartToIndexedDB();
    showToast('success', `Cliente ${client.nombre_cliente} seleccionado`);
}

function openClientSearchModal() {
    const modal = new bootstrap.Modal(document.getElementById('clientSearchModal'));
    modal.show();
    const searchInput = document.getElementById('clientSearchInput');
    const clientList = document.getElementById('clientList');
    searchInput.value = '';
    clientList.innerHTML = '';

    searchInput.addEventListener('input', () => {
        const query = searchInput.value.trim();
        if (query.length >= 3) {
            fetch(`/api/clientes/search?query=${encodeURIComponent(query)}`)
                .then(response => response.json())
                .then(clients => {
                    clientList.innerHTML = '';
                    clients.forEach(client => {
                        const item = document.createElement('a');
                        item.href = '#';
                        item.className = 'list-group-item list-group-item-action';
                        item.innerHTML = `${client.nombre_cliente} (NIF: ${client.nif}, Cliente: ${client.numero_cliente})`;
                        item.addEventListener('click', () => {
                            selectClient(client);
                            modal.hide();
                        });
                        clientList.appendChild(item);
                    });
                })
                .catch(error => {
                    console.error('Error al buscar clientes:', error);
                    showToast('danger', 'Error al buscar clientes');
                });
        }
    });
}

function initIndexedDB() {
    return new Promise((resolve, reject) => {
        const request = indexedDB.open(DB_NAME, DB_VERSION);

        request.onupgradeneeded = (event) => {
            db = event.target.result;
            if (!db.objectStoreNames.contains(CART_STORE)) {
                db.createObjectStore(CART_STORE, { keyPath: 'userId' });
            }
        };

        request.onsuccess = (event) => {
            db = event.target.result;
            resolve(db);
        };

        request.onerror = (event) => {
            console.error('Error al inicializar IndexedDB:', event.target.error);
            reject(event.target.error);
        };
    });
}

function saveCartToIndexedDB() {
    if (!db) {
        console.warn('IndexedDB no inicializado');
        return Promise.resolve();
    }
    return new Promise((resolve, reject) => {
        const transaction = db.transaction([CART_STORE], 'readwrite');
        const store = transaction.objectStore(CART_STORE);
        const userId = sessionStorage.getItem('email') || 'anonymous';
        const cartEntry = {
            userId: userId,
            cart: cart,
            timestamp: new Date().toISOString()
        };
        const request = store.put(cartEntry);

        request.onsuccess = () => {
            resolve();
        };

        request.onerror = (event) => {
            console.error('Error al guardar carrito en IndexedDB:', event.target.error);
            reject(event.target.error);
        };
    });
}

function loadCartFromIndexedDB() {
    if (!db) {
        console.warn('IndexedDB no inicializado');
        return Promise.resolve(null);
    }
    return new Promise((resolve, reject) => {
        const transaction = db.transaction([CART_STORE], 'readonly');
        const store = transaction.objectStore(CART_STORE);
        const userId = sessionStorage.getItem('email') || 'anonymous';
        const request = store.get(userId);

        request.onsuccess = (event) => {
            const result = event.target.result;
            resolve(result ? result.cart : null);
        };

        request.onerror = (event) => {
            console.error('Error al cargar carrito desde IndexedDB:', event.target.error);
            reject(event.target.error);
        };
    });
}

function syncCartWithBackend() {
    const userId = sessionStorage.getItem('email');
    if (!userId) {
        console.warn('Usuario no autenticado, no se sincroniza con backend');
        return Promise.resolve();
    }
    return fetch('/api/save_user_cart', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ userId, cart, timestamp: new Date().toISOString() })
    })
    .then(response => {
        if (!response.ok) throw new Error('Error al sincronizar con backend');
        return response.json();
    })
    .catch(error => {
        console.error('Error al sincronizar carrito:', error);
        showToast('danger', 'Error al sincronizar el carrito con el servidor');
    });
}

function conditionalBilling() {
    if (!cart.client) {
        showToast('warning', 'Debe seleccionar un cliente para facturar');
        return;
    }
    if (cart.items.length === 0) {
        showToast('warning', 'El carrito está vacío');
        return;
    }
    const modal = new bootstrap.Modal(document.getElementById('quotationTypeModal'));
    modal.show();

    const sendToCashButton = document.getElementById('sendToCash');
    const saveButton = document.getElementById('saveQuotation');
    sendToCashButton.addEventListener('click', () => {
        generateQuotation('Caja');
        modal.hide();
    }, { once: true });
    saveButton.addEventListener('click', () => {
        generateQuotation('Guardar');
        modal.hide();
    }, { once: true });
}

function generateQuotation(type) {
    const storeId = document.getElementById('storeFilter').value || 'BA001GC';
    fetch('/api/generate_pdf_quotation_id')
        .then(response => response.json())
        .then(data => {
            lastQuotationNumber = data.quotation_id;
            const quotationData = {
                cart: cart,
                store_id: storeId,
                tipo_presupuesto: type
            };
            return fetch('/api/create_quotation', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(quotationData)
            });
        })
        .then(response => {
            if (!response.ok) throw new Error('Error al generar el presupuesto');
            return response.json();
        })
        .then(data => {
            cart.quotation_id = data.quotation_number;
            cart.type = type === 'Caja' ? 'processed' : 'saved';
            updateCartDisplay();
            saveCartToIndexedDB();
            syncCartWithBackend();
            showToast('success', `Presupuesto ${data.quotation_number} generado como ${type}`);
            if (type === 'Caja') {
                printConfirmation();
            }
        })
        .catch(error => {
            console.error('Error al generar el presupuesto:', error);
            showToast('danger', 'Error al generar el presupuesto: ' + error.message);
        });
}

function printConfirmation() {
    const modal = new bootstrap.Modal(document.getElementById('printConfirmationModal'));
    modal.show();

    const noButton = document.getElementById('printNo');
    const yesButton = document.getElementById('printYes');
    noButton.addEventListener('click', () => {
        modal.hide();
    }, { once: true });
    yesButton.addEventListener('click', () => {
        window.print(); // Implementar lógica real de impresión si es necesario
        modal.hide();
        showToast('info', 'Impresión iniciada');
    }, { once: true });
}

function openRecoverQuotationModal() {
    const modal = new bootstrap.Modal(document.getElementById('recoverQuotationModal'));
    modal.show();
    const searchInput = document.getElementById('quotationSearchInput');
    const clientList = document.getElementById('quotationList');
    searchInput.value = '';
    clientList.innerHTML = '';

    searchInput.addEventListener('keypress', (e) => {
        if (e.key === 'Enter') {
            const query = searchInput.value.trim();
            if (query.length >= 3) {
                fetch(`/api/local_quotations?query=${encodeURIComponent(query)}`)
                    .then(response => response.json())
                    .then(quotations => {
                        clientList.innerHTML = '';
                        quotations.forEach(quotation => {
                            const item = document.createElement('a');
                            item.href = '#';
                            item.className = 'list-group-item list-group-item-action';
                            item.innerHTML = `${quotation.quotation_id} | ${quotation.client_name || 'Sin cliente'} | ${new Date(quotation.timestamp).toLocaleString()}`;
                            item.addEventListener('click', () => {
                                loadQuotation(quotation.quotation_id, 'local');
                                modal.hide();
                            });
                            clientList.appendChild(item);
                        });
                    })
                    .catch(error => {
                        console.error('Error al recuperar presupuestos:', error);
                        showToast('danger', 'Error al recuperar presupuestos');
                    });
            }
        }
    });
}

function loadQuotation(quotationId, type) {
    fetch(`/api/${type}_quotation/${quotationId}`)
        .then(response => {
            if (!response.ok) throw new Error('Presupuesto no encontrado');
            return response.json();
        })
        .then(quotation => {
            cart = quotation;
            cart.type = type;
            updateCartDisplay();
            saveCartToIndexedDB();
            syncCartWithBackend();
            showToast('success', `Presupuesto ${quotationId} cargado`);
        })
        .catch(error => {
            console.error('Error al cargar presupuesto:', error);
            showToast('danger', 'Error al cargar el presupuesto: ' + error.message);
        });
}

/***************************************
 * Inicialización
 ***************************************/
document.addEventListener('DOMContentLoaded', () => {
    initIndexedDB().then(() => {
        loadCartFromIndexedDB().then(loadedCart => {
            if (loadedCart) {
                cart = loadedCart;
                updateCartDisplay();
            }
        });
    });

    // Evento para observaciones
    const observationsInput = document.getElementById('cartObservations');
    if (observationsInput) {
        observationsInput.addEventListener('input', (e) => {
            cartObservations = e.target.value;
            cart.observations = cartObservations;
            saveCartToIndexedDB();
            syncCartWithBackend();
        });
    }

    // Botón de facturación condicional
    const billingButton = document.getElementById('billingButton');
    if (billingButton) {
        billingButton.addEventListener('click', conditionalBilling);
    }

    // Botón para buscar cliente
    const clientSearchButton = document.getElementById('clientSearchButton');
    if (clientSearchButton) {
        clientSearchButton.addEventListener('click', openClientSearchModal);
    }

    // Botón para recuperar presupuesto
    const recoverButton = document.getElementById('recoverQuotationButton');
    if (recoverButton) {
        recoverButton.addEventListener('click', openRecoverQuotationModal);
    }
});