/***************************************
 * Archivo: app.js
 * Descripción: Gestión de filtros, paginación,
 * visualización de imágenes y carga dinámica de productos
 * filtrados por StoreID.
 ***************************************/

// Función auxiliar para realizar solicitudes fetch con autenticación
async function fetchWithAuth(url, options = {}) {
  console.log('fetchWithAuth called:', url, options);

  const defaultOptions = {
    credentials: 'include',
    headers: {
      'X-Requested-With': 'XMLHttpRequest',
      ...(options.headers || {})
    }
  };

  const mergedOptions = {
    ...options,
    credentials: defaultOptions.credentials,
    headers: defaultOptions.headers
  };

  try {
    const response = await fetch(url, mergedOptions);

    if (!response.ok) {
      if (response.status === 401) {
        showToast('warning', 'Usuario no autenticado. Por favor, inicia sesión.');
        setTimeout(() => { window.location.href = '/autenticacion_avanzada/login'; }, 2000);
        throw new Error('Usuario no autenticado');
      }
      // Intentamos parsear el error si viene en JSON
      let errorMsg = `Error: ${response.status} ${response.statusText}`;
      try {
        const ct = response.headers.get('content-type') || '';
        if (ct.includes('application/json')) {
          const errorData = await response.json();
          errorMsg = errorData?.error || errorMsg;
        } else {
          const txt = await response.text();
          if (txt) errorMsg = txt;
        }
      } catch (_) { /* no-op */ }
      throw new Error(errorMsg);
    }

    return response; // 🔹 DEVOLVEMOS SIEMPRE EL Response crudo
  } catch (error) {
    console.error(`Error en fetchWithAuth (${url}):`, error);
    throw error;
  }
}

document.addEventListener('DOMContentLoaded', () => {
  const themeToggleBtn = document.getElementById('themeToggleBtn');
  const body = document.body;

  if (!themeToggleBtn) {
    console.error('Error: No se encontró el elemento themeToggleBtn');
    return;
  }

  // Cargar tema guardado o predeterminado
  const savedTheme = localStorage.getItem('theme') || 'light';
  body.classList.add(savedTheme === 'dark' ? 'dark-mode' : 'light-mode');
  updateThemeIcon(savedTheme);

  // Evento de clic para cambiar tema
  themeToggleBtn.addEventListener('click', () => {
    const currentTheme = body.classList.contains('dark-mode') ? 'dark' : 'light';
    const newTheme = currentTheme === 'light' ? 'dark' : 'light';

    body.classList.remove(`${currentTheme}-mode`);
    body.classList.add(`${newTheme}-mode`);
    localStorage.setItem('theme', newTheme);
    updateThemeIcon(newTheme);
  });

  function updateThemeIcon(theme) {
    themeToggleBtn.innerHTML = theme === 'light' ? '<i class="bi bi-moon-stars"></i>' : '<i class="bi bi-sun"></i>';
  }
});

/* Variables globales */
let products = [];         // Productos obtenidos desde el backend
let filteredProducts = []; // Productos filtrados (usados para paginación)
let currentPage = 1;
const ITEMS_PER_PAGE = 20;
let currentModalImageIndex = 0;
let currentModalProduct = null;
let hoverTimeout;          // Temporizador para evitar activaciones rápidas
let overlayVisible = false; // Indicador para el estado del overlay
let isMouseOverImage = false; // Para controlar el overlay al pasar el mouse sobre la imagen
let cart = {
    items: [],
    client: null,
    quotation_id: null,
    type: 'new',
    observations: ''
}; // Array para almacenar los ítems del carrito
let currentProductToAdd = null; // Producto seleccionado para agregar al carrito
let selectedClient = null;
let lastProductsUpdate = 0;
let cartObservations = "";
let db;
const DB_NAME = 'CartDB';
const DB_VERSION = 1;
const CART_STORE = 'carts';

async function checkProductsUpdate() {
    try {
        const data = await fetchWithAuth('/api/check_products_update');
        const newLastModified = data.last_modified;

        if (newLastModified > lastProductsUpdate) {
            lastProductsUpdate = newLastModified;
            const storeId = document.getElementById("storeFilter").value || lastStore || 'BA001GC';
            await loadProducts(storeId, 1, 20000, false, false);
        }
    } catch (error) {
        console.error("Error al verificar actualización de productos:", error);
        showToast('danger', 'Error al verificar actualización de productos.');
    }
}

/***************************************
 * Cambio de Vista: Tabla ↔ Cards
 ***************************************/
function cambiarVista(vista) {
    const tableView = document.getElementById("tableView");
    const cardView = document.getElementById("cardView");
    const btnTableView = document.getElementById("btnTableView");
    const btnCardView = document.getElementById("btnCardView");

    if (!tableView || !cardView || !btnTableView || !btnCardView) {
        console.error("❌ Error: Elementos del DOM no encontrados.");
        return;
    }

    if (vista === "tabla") {
        tableView.classList.remove("d-none");
        cardView.classList.add("d-none");
        btnTableView.classList.add("active");
        btnCardView.classList.remove("active");
    } else {
        tableView.classList.add("d-none");
        cardView.classList.remove("d-none");
        btnCardView.classList.add("active");
        btnTableView.classList.remove("active");
    }

    displayProducts(filteredProducts);
}

/***************************************
 * Generar Vista en Cards
 ***************************************/
function cargarVistaCards(productos) {
  const cardView = document.getElementById("cardView");

  if (!cardView) {
    console.error("❌ Error: No se encontró el contenedor de cards (`cardView`).");
    return;
  }

  cardView.innerHTML = "";

  if (!productos.length) {
    cardView.innerHTML = `<p class="text-center text-danger">No se encontraron productos.</p>`;
    return;
  }

  const fragment = document.createDocumentFragment();

  for (const product of productos) {
    const cardWrapper = document.createElement("div");
    cardWrapper.className = "col-lg-2 col-md-3 col-sm-6 col-12 mb-3";

    const card = document.createElement("div");
    card.className = "card small-card shadow-sm";
    card.setAttribute("data-product-id", product.numero_producto);

    card.innerHTML = `
      <div class="image-container">
        <div class="spinner-border spinner-border-sm text-primary image-spinner" role="status">
          <span class="visually-hidden">Cargando...</span>
        </div>
        <img data-src="${product.imagen_url || '/images/default.jpg'}"
             alt="${product.nombre_producto}"
             class="card-img-top product-image lazyload"
             onclick="event.stopPropagation(); showModal(${product.numero_producto}, 1)">
      </div>
      <div class="card-body text-center p-2"> 
        <h6 class="card-title">${product.numero_producto} | ${product.nombre_producto}</h6>
        <p class="card-text"><strong>$${product.precio_final_con_iva}</strong></p>
        <p class="card-text"><strong><span class="text-success">$${product.precio_final_con_descuento}</span></strong></p>
        <p class="card-text coverage-group">${product.grupo_cobertura}</p>
        <button class="btn btn-sm btn-outline-primary btn-stock mt-1"
                onclick="event.stopPropagation(); buscarStock(event, '${product.nombre_producto}', '${product.numero_producto}', '${product.unidad_medida}')">
          <i class="bi bi-box-seam"></i> Stock
        </button>
        <button class="btn btn-sm btn-success btn-cart mt-1"
                onclick="showQuantityModal(event, '${product.numero_producto}', '${product.nombre_producto}', '${product.precio_final_con_descuento}')">
          <i class="bi bi-cart-plus"></i>
        </button>
      </div>
    `;

    cardWrapper.appendChild(card);
    fragment.appendChild(cardWrapper);
  }

  cardView.appendChild(fragment);
  setupCardListeners();
  initLazyLoading();
}

// Obtiene la URL de imagen desde el listado de productos
async function obtenerImagenProducto(productId) {
    const product = products.find(p => p.numero_producto === productId);
    return product && product.imagen_url ? product.imagen_url : '/images/default.jpg';
}

/***************************************
 * Funciones Utilitarias y de Imagen
 ***************************************/
// Las imágenes se obtienen desde el backend, por lo que no se requiere lógica adicional aquí.

/***************************************
 * Funciones para Modal y Overlay
 ***************************************/
async function showModal(productId) {
    currentModalProduct = productId;

    const modal = document.getElementById('imageModal');
    const modalImg = document.getElementById('modalImage');
    const modalContent = document.querySelector('.image-modal-content');

    const imageUrl = await obtenerImagenProducto(productId);
    const availableImages = [imageUrl];

    currentModalImageIndex = 0;
    modalImg.src = imageUrl;
    ajustarImagenModal();
    modal.style.display = 'flex';

    // Guardamos las imágenes disponibles en un dataset para navegación
    modal.dataset.availableImages = JSON.stringify(availableImages);

    // Elimina botones previos para evitar duplicados
    document.getElementById('modalPrevButton')?.remove();
    document.getElementById('modalNextButton')?.remove();
}

function closeModal() {
    document.getElementById('imageModal').style.display = 'none';
}

function navigateModalImage(direction) {
    const modal = document.getElementById('imageModal');
    const modalImg = document.getElementById('modalImage');
    let availableImages = JSON.parse(modal.dataset.availableImages); // Recuperamos imágenes disponibles

    currentModalImageIndex += direction;

    // 🔹 Evitar desbordamiento de índice
    if (currentModalImageIndex < 0) {
        currentModalImageIndex = availableImages.length - 1;
    } else if (currentModalImageIndex >= availableImages.length) {
        currentModalImageIndex = 0;
    }

    modalImg.src = availableImages[currentModalImageIndex];
}

function updateModalNavButtons() {
  // Aquí podrías actualizar el estado de los botones según el índice actual
  // Por ejemplo, deshabilitar "prev" si currentModalImageIndex === 0, etc.
}

function closeOverlay() {
  const overlay = document.getElementById('productOverlay');
  const overlayContent = document.getElementById('overlayContent');

  if (overlay && overlayContent) {
    overlayContent.innerHTML = '';
    overlay.classList.add('d-none');
    overlayVisible = false;
  }
}

function ajustarAnchoModal() {
    const tabla = document.querySelector("#tableView");
    const cards = document.querySelector("#cardView");
    const overlayContent = document.getElementById("overlayContent");

    if (!overlayContent) return;

    let nuevoAncho = "90vw"; // 🔹 Valor por defecto

    // 🔹 Si la tabla está visible, tomamos su ancho
    if (!tabla.classList.contains("d-none")) {
        nuevoAncho = `${tabla.offsetWidth}px`;
    }

    // 🔹 Si las cards están activas, ajustamos a su contenedor
    if (!cards.classList.contains("d-none")) {
        nuevoAncho = `${cards.offsetWidth}px`; // Ajustamos un poco más pequeño si es necesario
    }

    // Aplicamos el ancho calculado
    overlayContent.style.width = nuevoAncho;
}

/***************************************
 * Funciones para imágenes de producto
 ***************************************/
async function generateImageHtml(productId) {
    const imageUrl = await obtenerImagenProducto(productId);
    return `
            <div class="product-image-container" data-current-index="1">
                <img
                    src="${imageUrl}"
                    class="product-image img-thumbnail"
                    onclick="event.stopPropagation(); showModal(${productId}, 1)"
                    alt="Producto ${productId}"
                >
            </div>
        `;
}

async function navigateImage(productId, container, direction) {
    // Navegación deshabilitada
}

/***************************************
 * Funciones de Paginación y Visualización
 ***************************************/
async function displayProducts(products) {
    const productList = document.getElementById("productList");
    const cardView = document.getElementById("cardView");

    productList.innerHTML = "";
    cardView.innerHTML = "";

    if (!products || !Array.isArray(products) || products.length === 0) {
        console.log("No hay productos para mostrar.");
        mostrarMensaje("No hay productos disponibles.");
        productList.innerHTML = `<tr><td colspan="11" class="text-center text-danger">No se encontraron productos.</td></tr>`;
        cardView.innerHTML = `<p class="text-center text-danger">No se encontraron productos.</p>`;
        updatePagination(0);
        return;
    }

    const totalPages = Math.ceil(products.length / ITEMS_PER_PAGE);
    if (currentPage > totalPages) {
        currentPage = totalPages || 1;
    }

    const startIndex = (currentPage - 1) * ITEMS_PER_PAGE;
    const endIndex = startIndex + ITEMS_PER_PAGE;
    const pageProducts = products.slice(startIndex, endIndex);

    const isTableView = !document.getElementById("tableView").classList.contains("d-none");

    if (isTableView) {
        const fragment = document.createDocumentFragment();
        for (const product of pageProducts) {
            const row = document.createElement("tr");
            row.dataset.productId = product["numero_producto"];
            row.innerHTML = `
                <td><strong>${product["numero_producto"]}</strong></td>
                <td>${product["categoria_producto"]}</td>
                <td>${product["nombre_producto"]}</td>
                <td>${product["grupo_cobertura"]}</td>
                <td>${product["unidad_medida"]}</td>
                <td>${product["precio_final_con_iva"]}</td>
                <td><strong><span class="text-success">$${product["precio_final_con_descuento"]}</span></strong></td>
                <td>${product["store_number"]}</td>
                <td id="images-${product["numero_producto"]}">
                    <div class="image-container">
                        <div class="spinner-border spinner-border-sm text-primary image-spinner" role="status">
                            <span class="visualmente-hidden">Cargando...</span>
                        </div>
                        <img data-src="${product["imagen_url"] || '/images/default.jpg'}" class="product-image img-thumbnail lazyload" alt="Cargando...">
                    </div>
                </td>
                <td>
                    <button class="btn btn-primary btn-sm btn-stock" 
                            onclick="buscarStock(event, '${product["nombre_producto"]}', '${product["numero_producto"]}', '${product["unidad_medida"]}')">
                        <i class="bi bi-box-seam"></i>
                    </button>
                </td>
                <td>
                    <button class="btn btn-success btn-cart btn-md" 
                            onclick="showQuantityModal(event, '${product["numero_producto"]}', '${product["nombre_producto"]}', '${product["precio_final_con_descuento"]}')">
                        <i class="bi bi-cart-plus"></i>
                    </button>
                </td>
            `;
            fragment.appendChild(row);
        }
        productList.appendChild(fragment);
        setupRowListeners();
        initLazyLoading();
    } else {
        cargarVistaCards(pageProducts);
    }

    updatePagination(products.length);
}

// Función para mostrar un mensaje en la interfaz de usuario
function mostrarMensaje(mensaje) {
    const mensajeElement = document.getElementById("mensaje");
    if (mensajeElement) {
        mensajeElement.textContent = mensaje;
        mensajeElement.classList.remove("d-none"); // Asegúrate de que el mensaje sea visible
    }
}

function changePage(page) {
    if (page >= 1 && page <= Math.ceil(filteredProducts.length / ITEMS_PER_PAGE)) {
        currentPage = page;
        displayProducts(filteredProducts); // Actualizar la vista con los productos filtrados
    }
}

async function actualizarTabla(productos) {
    const productList = document.getElementById("productList");
    productList.innerHTML = ""; // 🔹 Limpiar tabla antes de insertar

    if (!productos.length) {
        productList.innerHTML = `<tr>
            <td colspan="10" class="text-center text-danger">No se encontraron productos.</td>
        </tr>`;
        return;
    }

    for (const product of productos) {
        const row = document.createElement("tr");
        row.dataset.productId = product.numero_producto; // 🔹 Guardar ID del producto en la fila

        // Obtener imagen
        const imageUrl = await obtenerImagenProducto(product.numero_producto);

        row.innerHTML = `
            <td><strong>${product.numero_producto}</strong></td>
            <td>${product.categoria_producto}</td>
            <td>${product.nombre_producto}</td>
            <td>${product.grupo_cobertura}</td>
            <td>${product.unidad_medida}</td>
            <td>${product.precio_final_con_iva}</td>
            <td><strong><span class="text-success">$${product.precio_final_con_descuento}</span></strong></td>
            <td>${product.store_number}</td>
            <td>
                <img data-src="${imageUrl}" class="product-image img-thumbnail lazyload"
                     alt="Producto ${product.numero_producto}" 
                     onclick="showModal(${product.numero_producto}, 1)">
            </td>
            <td>
                <button class="btn btn-primary btn-sm btn-stock" 
                        onclick="buscarStock(event, '${product.nombre_producto}', '${product.numero_producto}', '${product.unidad_medida}')">
                    <i class="bi bi-box-seam"></i> Stock
                </button>
            </td>
        `;

        productList.appendChild(row);
    }

    // 🔹 Llamar a setupRowListeners() después de agregar las filas
    setupRowListeners();
    setTimeout(initLazyLoading, 100);
}

function updatePagination(totalItems) {
  const totalPages = Math.ceil(totalItems / ITEMS_PER_PAGE);
  const pagination = document.getElementById('pagination');
  if (!pagination) return;

  pagination.innerHTML = "";
  const ul = document.createElement("ul");
  ul.classList.add("pagination", "justify-content-center");

  // Botón "Anterior"
  const prevItem = document.createElement("li");
  prevItem.classList.add("page-item");
  if (currentPage === 1) prevItem.classList.add("disabled");
  const prevLink = document.createElement("a");
  prevLink.classList.add("page-link");
  prevLink.href = "#";
  prevLink.innerHTML = `<i class="bi bi-chevron-left"></i>`;
  prevLink.onclick = (e) => {
    e.preventDefault();
    changePage(currentPage - 1);
  };
  prevItem.appendChild(prevLink);
  ul.appendChild(prevItem);

  // Botones numerados
  const maxPagesToShow = 7;
  const halfRange = Math.floor(maxPagesToShow / 2);
  let start = Math.max(1, currentPage - halfRange);
  let end = Math.min(totalPages, currentPage + halfRange);
  if (start > 1) {
    ul.appendChild(createPageItem(1));
    if (start > 2) ul.appendChild(createDotsItem());
  }
  for (let i = start; i <= end; i++) {
    ul.appendChild(createPageItem(i));
  }
  if (end < totalPages) {
    if (end < totalPages - 1) ul.appendChild(createDotsItem());
    ul.appendChild(createPageItem(totalPages));
  }

  // Botón "Siguiente"
  const nextItem = document.createElement("li");
  nextItem.classList.add("page-item");
  if (currentPage === totalPages) nextItem.classList.add("disabled");
  const nextLink = document.createElement("a");
  nextLink.classList.add("page-link");
  nextLink.href = "#";
  nextLink.innerHTML = `<i class="bi bi-chevron-right"></i>`;
  nextLink.onclick = (e) => {
    e.preventDefault();
    changePage(currentPage + 1);
  };
  nextItem.appendChild(nextLink);
  ul.appendChild(nextItem);

  pagination.appendChild(ul);
}

function createPageItem(page) {
  const li = document.createElement("li");
  li.classList.add("page-item");
  if (page === currentPage) li.classList.add("active");
  const link = document.createElement("a");
  link.classList.add("page-link");
  link.href = "#";
  link.textContent = page;
  link.onclick = (e) => {
    e.preventDefault();
    changePage(page);
  };
  li.appendChild(link);
  return li;
}

function createDotsItem() {
  const li = document.createElement("li");
  li.classList.add("page-item", "disabled");
  const span = document.createElement("span");
  span.classList.add("page-link");
  span.textContent = "...";
  li.appendChild(span);
  return li;
}

function normalizeText(text = "") {
    return text.normalize("NFD").replace(/[\u0300-\u036f]/g, "").trim().toLowerCase();
}

function filterProducts() {
    const searchTerms = document.getElementById('search').value
        .trim()
        .toLowerCase()
        .split(' ')
        .filter(term => term.length > 0);

    const category = document.getElementById("categoryFilter").value;
    const coverage = document.getElementById("coverageGroupFilter").value;
    const store = document.getElementById("storeFilter").value;
    const excludeSpecial = document.getElementById("excludeSpecialCategories").checked;
    const minPrice = parseFloat(document.getElementById("minPrice").value) || 0;
    const maxPrice = parseFloat(document.getElementById("maxPrice").value) || Infinity;

    // Nuevos filtros de stock
    const signoMas = document.getElementById("signoMas").checked;
    const signoMenos = document.getElementById("signoMenos").checked;
    const signoCero = document.getElementById("cero").checked;

    const excludeWords = ["outlet", "outle", "2da", "saldo", "lote", "@", "//", "pedido", "outl"];

    if (!products || !Array.isArray(products)) {
        throw new Error("La lista de productos no está definida o no es válida.");
    }

    let filtered = products.filter(product => {
        const productName = product["nombre_producto_normalizado"] || "";
        const productNumber = product["numero_producto"] ? String(product["numero_producto"]) : "";
        const productSigno = product["signo"] || "0"; // Asumimos "0" si no existe el campo

        const matchesSearch = searchTerms.length === 0 || searchTerms.every(term =>
            productName.includes(term) || productNumber.includes(term)
        );

        const matchesCategory = !category || product["categoria_producto"] === category;
        const matchesCoverage = !coverage || product["grupo_cobertura"] === coverage;
        const matchesStore = !store || product["store_number"] === store;

        const productPrice = parseFloat(product["precio_final_con_descuento"]?.replace(/\./g, "").replace(",", ".")) || 0;
        const matchesPrice = productPrice >= minPrice && productPrice <= maxPrice;

        const excludeSpecialFilter = excludeSpecial
            ? !excludeWords.some(word =>
                productName.includes(word) ||
                productName.startsWith(word) ||
                productName.endsWith(word)
            )
            : true;

        // Filtro por signo
        const matchesSigno =
            (signoMas && productSigno === "+") ||
            (signoMenos && productSigno === "-") ||
            (signoCero && productSigno === "0");

        return matchesSearch && matchesCategory && matchesCoverage && matchesStore && matchesPrice && excludeSpecialFilter && matchesSigno;
    });

    // Ordenar alfabéticamente por nombre_producto
    filtered.sort((a, b) => {
        const nameA = a.nombre_producto?.toLowerCase() || "";
        const nameB = b.nombre_producto?.toLowerCase() || "";
        return nameA.localeCompare(nameB);
    });

    return filtered;
}

function filterAndPaginate(withSpinner = false, resetPage = true) {
    if (withSpinner) showSpinner();
    if (resetPage) {
        currentPage = 1;
    }
    document.getElementById("productList").innerHTML = "";
    document.getElementById("cardView").innerHTML = "";
    filteredProducts = [];
    try {
        filteredProducts = filterProducts();
        displayProducts(filteredProducts);
    } catch (error) {
        console.error("Error en filterAndPaginate:", error);
        filteredProducts = [];
        document.getElementById("productList").innerHTML = `<tr><td colspan="10" class="text-center text-danger">Error al filtrar productos. Intenta de nuevo.</td></tr>`;
        document.getElementById("cardView").innerHTML = `<p class="text-center text-danger">Error al filtrar productos. Intenta de nuevo.</p>`;
        updatePagination(0);
    }
    if (withSpinner) hideSpinner();
}

function clearFilters() {
    document.getElementById('search').value = '';
    document.getElementById('categoryFilter').value = '';
    document.getElementById('coverageGroupFilter').value = '';
    document.getElementById('excludeSpecialCategories').checked = true;
    document.getElementById("priceRangeFilter").value = "";
    document.getElementById("minPrice").value = "";
    document.getElementById("maxPrice").value = "";
    // Reiniciar filtros de stock
    document.getElementById("signoMas").checked = true;
    document.getElementById("signoMenos").checked = true;
    document.getElementById("cero").checked = true;

    currentPage = 1;
    filterAndPaginate();
}

function sortTable(columnIndex) {
  const table = document.getElementById('productTable');
  const rows = Array.from(table.querySelectorAll('tbody tr'));
  const headers = table.querySelectorAll('th');
  if (!rows.length) {
    console.warn("No hay productos para ordenar.");
    return;
  }
  const currentDirection = headers[columnIndex].getAttribute('data-sort') === 'asc' ? 'desc' : 'asc';
  headers.forEach(header => header.removeAttribute('data-sort'));
  headers[columnIndex].setAttribute('data-sort', currentDirection);

  rows.sort((a, b) => {
    const aValue = a.cells[columnIndex].textContent.trim();
    const bValue = b.cells[columnIndex].textContent.trim();
    if (!isNaN(aValue) && !isNaN(bValue)) {
      return currentDirection === 'asc'
        ? parseFloat(aValue) - parseFloat(bValue)
        : parseFloat(bValue) - parseFloat(aValue);
    } else {
      return currentDirection === 'asc'
        ? aValue.localeCompare(bValue)
        : bValue.localeCompare(aValue);
    }
  });
  const tbody = table.querySelector('tbody');
  tbody.innerHTML = '';
  rows.forEach(row => tbody.appendChild(row));
}

/***************************************
 * Funciones para inicializar filtros
 ***************************************/
function initializeFilters(previousCategory = "", previousCoverage = "") {
  // Recolectar categorías y grupos de cobertura a partir de los productos actuales
  const categories = new Set();
  const coverageGroups = new Set();

  products.forEach(product => {
    categories.add(product["categoria_producto"]);
    coverageGroups.add(product["grupo_cobertura"]);
  });

  // Repoblar los filtros, conservando el valor previamente seleccionado si existe
  populateFilter("categoryFilter", categories, false, previousCategory);
  populateFilter("coverageGroupFilter", coverageGroups, false, previousCoverage);

  // Para el filtro de tiendas, se carga sólo en la carga inicial (usando la variable global backendStores)
  const storeFilterElement = document.getElementById("storeFilter");
  if (storeFilterElement && storeFilterElement.options.length === 0) {
    const storeSet = new Set(backendStores);
    populateFilter("storeFilter", storeSet, true);
  }
}

function populateFilter(filterId, values, isStoreFilter = false, selectedValue = "") {
  const filterElement = document.getElementById(filterId);
  if (!filterElement) {
    console.error(`No se encontró el filtro con ID: ${filterId}`);
    return;
  }

  // Para filtros que no sean de tienda, reiniciamos las opciones
  if (!isStoreFilter) {
    filterElement.innerHTML = '';
    const defaultOption = document.createElement("option");
    defaultOption.value = "";
    defaultOption.textContent = "Todos";
    filterElement.appendChild(defaultOption);
  }

  const sortedValues = Array.from(values).sort((a, b) => a.localeCompare(b));
  sortedValues.forEach(value => {
    if (value) {
      const option = document.createElement("option");
      option.value = value;
      option.textContent = value;
      filterElement.appendChild(option);
    }
  });

  // Si se proporcionó un valor seleccionado, se intenta restablecer
  if (selectedValue) {
    for (let i = 0; i < filterElement.options.length; i++) {
      if (filterElement.options[i].value === selectedValue) {
        filterElement.value = selectedValue;
        break;
      }
    }
  } else if (isStoreFilter && filterElement.options.length > 0 && !filterElement.value) {
    filterElement.value = sortedValues[0];
  }
}

/***************************************
 * Función para cargar productos desde el backend
 ***************************************/
async function loadProducts(storeId, page = 1, itemsPerPage = 20000, showSpinnerParam = true, resetPage = true) {
    let spinnerShown = false;
    try {
        if (showSpinnerParam) {
            showSpinner();
            spinnerShown = true;
        }
        const response = await fetchWithAuth(`/api/productos?store=${storeId}&page=${page}&items_per_page=${itemsPerPage}`);
        if (!response.ok) {
            throw new Error("Error al cargar productos");
        }
        const rawProducts = await response.json();
        products = rawProducts.map(product => ({
            ...product,
            nombre_producto_normalizado: normalizeText(product["nombre_producto"] || "")
        }));
        // Actualizar filtro de tienda...
        const storeFilterElement = document.getElementById("storeFilter");
        if (storeFilterElement && storeFilterElement.value !== storeId) {
            storeFilterElement.value = storeId;
        }
        // Conservar valores previos de filtros...
        const catEl = document.getElementById("categoryFilter");
        const covEl = document.getElementById("coverageGroupFilter");
        const previousCategory = catEl ? catEl.value : "";
        const previousCoverage = covEl ? covEl.value : "";
        // Reiniciar filtros...
        initializeFilters(previousCategory, previousCoverage);
        if (resetPage) {
            currentPage = 1;
        }
        filterAndPaginate(false, resetPage);
        // Actualizar precios en el carrito...
        updateCartPrices(storeId);
        // Actualizar lastProductsUpdate...
        const updateResponse = await fetchWithAuth('/api/check_products_update');
        if (updateResponse.ok) {
            const updateData = await updateResponse.json();
            lastProductsUpdate = updateData.last_modified;
        }
    } catch (error) {
        console.error("Error al cargar productos:", error);
        showToast('danger', 'Error al cargar productos');
    } finally {
        if (spinnerShown) {
            hideSpinner();
        }
    }
}

/***************************************
 * Event Listeners
 ***************************************/
document.addEventListener("DOMContentLoaded", function () {
    showSpinner();
    const initialStore = lastStore || 'BA001GC';
    loadProducts(initialStore);

    const storeFilterElement = document.getElementById('storeFilter');
    if (storeFilterElement) {
        storeFilterElement.value = initialStore;
        storeFilterElement.addEventListener('change', function() {
            const selectedStore = this.value;
            loadProducts(selectedStore);
            updateLastStore(selectedStore);
        });
    }

    // Configurar polling cada 5 minutos (300,000 ms)
    setInterval(checkProductsUpdate, 300000);

    let debounceTimeout;
    document.getElementById('search').addEventListener('input', () => {
        const busqueda = document.getElementById('search').value;
        clearTimeout(debounceTimeout);
        debounceTimeout = setTimeout(() => {
            if (busqueda.length >= 3 || busqueda.length === 0) {
                filterAndPaginate(false);
            }
        }, 300);
    });

    document.getElementById('categoryFilter').addEventListener('change', filterAndPaginate);
    document.getElementById('coverageGroupFilter').addEventListener('change', filterAndPaginate);
    document.getElementById('excludeSpecialCategories').addEventListener('change', filterAndPaginate);
});

// Inicialización de toasts de Bootstrap (si aplica)
document.addEventListener('DOMContentLoaded', function () {
  const toastElements = document.querySelectorAll('.toast');
  toastElements.forEach(function (toastElement) {
    const toast = new bootstrap.Toast(toastElement);
    toast.show();
  });
});

// Función opcional para limpiar caché de imágenes
function clearImageCache() {
  const keys = Object.keys(localStorage);
  keys.forEach(key => {
    if (key.startsWith('img_cache_')) {
      localStorage.removeItem(key);
    }
  });
}

async function updateLastStore(storeId) {
    try {
        const response = await fetch('/api/update_last_store', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ store_id: storeId })
        });
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.error || 'Error al actualizar last_store');
        }
    } catch (error) {
        console.error("Error al actualizar last_store:", error);
        showToast('danger', `Error al actualizar la última tienda: ${error.message}`);
    }
}

let spinnerActive = false; // bandera global

function showSpinner() {
  if (spinnerActive) return;  // Si ya está activo, no hace nada
  spinnerActive = true;
  const spinner = document.getElementById('spinner');
  if (spinner) {
    spinner.style.display = 'flex'; // Usamos 'flex' para centrar (con align-items/justify-content)
  }
}

function hideSpinner() {
  if (!spinnerActive) return; // Evitamos ocultarlo si no está activo
  spinnerActive = false;
  const spinner = document.getElementById('spinner');
  if (spinner) {
    spinner.style.display = 'none';
  }
}

async function buscarStock(event, productoNombre, productoId, unidadMedida, storeId) {
    try {
        event.stopPropagation();
        showSpinner();

        if (!productoId) {
            console.error("⚠️ Error: productoId no válido en buscarStock()");
            alert("Error: Producto no válido.");
            return;
        }

        const effectiveStoreId = storeId || document.getElementById("storeFilter").value || "BA001GC";
        const url = `/api/stock/${encodeURIComponent(productoId)}/${encodeURIComponent(effectiveStoreId)}`;

        const stockResp = await fetch(url);

        if (!stockResp.ok) {
            if (stockResp.status === 404) {
                abrirOverlayStock(productoNombre, productoId, unidadMedida, null, "No hay stock disponible para este producto en la tienda seleccionada.");
                return;
            }
            throw new Error("Error al obtener stock del producto");
        }

        const stockData = await stockResp.json();
        abrirOverlayStock(productoNombre, productoId, unidadMedida, stockData);
    } catch (error) {
        console.error("❌ Error al buscar stock:", error);
        abrirOverlayStock(productoNombre, productoId, unidadMedida, null, "Ocurrió un error al buscar el stock.");
    } finally {
        hideSpinner();
    }
}

function convertirMonedaANumero(valor) {
    if (valor === null || valor === undefined) {
        console.warn(`Valor no válido para convertirMonedaANumero: ${valor}`);
        return 0;
    }
    if (typeof valor === 'number') {
        return parseFloat(valor.toFixed(2));
    }
    try {
        let numeroStr = valor.toString().trim();
        numeroStr = numeroStr.replace(/[^0-9,.]/g, '');
        numeroStr = numeroStr.replace(/\./g, "").replace(",", ".");
        const numero = parseFloat(numeroStr);
        if (isNaN(numero)) {
            console.warn(`No se pudo convertir a número: ${valor} -> ${numeroStr}`);
            return 0;
        }
        return parseFloat(numero.toFixed(2));
    } catch (error) {
        console.warn(`Error al convertir moneda: ${valor}`, error);
        return 0;
    }
}

function formatearMoneda(valor) {
    if (typeof valor !== 'number' || isNaN(valor)) {
        console.warn(`Valor no válido para formatearMoneda: ${valor}`);
        return '0,00';
    }
    return valor.toLocaleString("es-AR", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function abrirOverlayStock(nombre, codigo, unidad, stockData) {
  const overlay = document.getElementById("stockOverlay");
  if (!overlay) {
    console.error("Error: No se encontró el elemento #stockOverlay");
    return;
  }
  const nombreElem = document.getElementById("stockProductoNombre");
  const codigoElem = document.getElementById("stockProductoCodigo");
  const unidadElem = document.getElementById("stockProductoUnidad");
  const tabla = document.getElementById("stockTabla");
  const totalVenta = document.getElementById("totalVenta");
  const totalEntrega = document.getElementById("totalEntrega");
  const totalComprometido = document.getElementById("totalComprometido");

  if (!nombreElem || !codigoElem || !unidadElem || !tabla || !totalVenta || !totalEntrega || !totalComprometido) {
    console.error("Error: Faltan elementos dentro de #stockOverlay");
    return;
  }

  nombreElem.textContent = nombre;
  codigoElem.textContent = codigo;
  unidadElem.textContent = unidad;

  tabla.innerHTML = "";
  totalVenta.textContent = "0,00";
  totalEntrega.textContent = "0,00";
  totalComprometido.textContent = "0,00";

  if (!stockData || stockData.length === 0) {
    tabla.innerHTML = `<tr><td colspan="4" class="text-center text-danger">No hay stock disponible para este producto en la tienda seleccionada.</td></tr>`;
  } else {
    let sumaVenta = 0, sumaEntrega = 0, sumaComprometido = 0;
    stockData.forEach((stock) => {
      const row = document.createElement("tr");
      let disponibleVenta = convertirMonedaANumero(stock.disponible_venta);
      let disponibleEntrega = convertirMonedaANumero(stock.disponible_entrega);
      let comprometido = convertirMonedaANumero(stock.comprometido);
      sumaVenta += disponibleVenta;
      sumaEntrega += disponibleEntrega;
      sumaComprometido += comprometido;
      row.innerHTML = `
        <td>${stock.almacen_365}</td>
        <td>${formatearMoneda(disponibleVenta)}</td>
        <td>${formatearMoneda(disponibleEntrega)}</td>
        <td>${formatearMoneda(comprometido)}</td>
      `;
      tabla.appendChild(row);
    });
    totalVenta.textContent = formatearMoneda(sumaVenta);
    totalEntrega.textContent = formatearMoneda(sumaEntrega);
    totalComprometido.textContent = formatearMoneda(sumaComprometido);
  }

  // Mostrar overlay con animación
  overlay.classList.remove("d-none");
  setTimeout(() => {
    overlay.classList.add("active");
  }, 10); // Pequeño retraso para activar la transición
}

function cerrarOverlaystock() {
  const overlay = document.getElementById("stockOverlay");
  if (overlay) {
    overlay.classList.remove("active");
    setTimeout(() => {
      overlay.classList.add("d-none");
    }, 300); // Esperar a que termine la animación
  }
}

async function mostrarAtributos(productId) {
  const overlay = document.getElementById("productOverlay");
  const overlayContent = document.getElementById("overlayContent");

  if (!overlay || !overlayContent) {
    console.error("No se encontraron elementos del overlay.");
    return;
  }

  try {
    showSpinner();

    const response = await fetchWithAuth(`/producto/atributos/${productId}`);
    const data = await response.json();   // 🔹 ahora sí, parseamos aquí

    const productName =
      data.product_name || data.productName || data.name || "Producto sin nombre";

    const rawAttributes = data.attributes || data.atributos || [];
    const attributes = Array.isArray(rawAttributes)
      ? rawAttributes.map(a => ({
          name: a.AttributeName || a.attribute_name || a.name || 'Atributo',
          value: a.AttributeValue || a.attribute_value || a.value || ''
        }))
      : [];

    const imageUrl = await obtenerImagenProducto(productId);
    const defaultImage = '/images/default.jpg';
    const validImages = imageUrl ? [imageUrl] : [defaultImage];

    overlayContent.innerHTML = `
      <button class="overlay-close-btn" onclick="cerrarOverlay()">×</button>
      <div class="header text-center mb-4">
        <img src="https://productimages.familiabercomat.com/small/logo_0.png" alt="Logo de Familia Bercomat" class="logo mb-3">
        <h1 class="product-title">${productName}</h1>
      </div>
      <div class="product-container d-flex flex-column flex-md-row gap-4">
        <div class="product-images flex-1">
          <img id="mainImage" src="${validImages[0]}" alt="Producto" class="main-image">
          <div class="thumbnail-container mt-3" id="thumbnail-container">
            ${validImages.map((url, index) => `
              <img src="${url}" alt="Imagen ${index + 1}" class="thumbnail" data-index="${index}">
            `).join("")}
          </div>
        </div>
        <div class="product-details flex-1">
          <div class="specs">
            <h2>Especificaciones</h2>
            <ul id="specifications-list">
              ${
                attributes.length > 0
                  ? attributes.map(item => `
                      <li class="section-attribute">
                        <span class="label">${item.name}:</span>
                        <span class="value">${item.value}</span>
                      </li>
                    `).join("")
                  : `<li style="color: var(--icon-color); font-style: italic;">Este producto no tiene atributos disponibles.</li>`
              }
            </ul>
          </div>
        </div>
      </div>
    `;

    const mainImage = overlayContent.querySelector("#mainImage");
    mainImage.addEventListener("click", () => {
      mainImage.classList.toggle("zoomed");
    });

    const thumbnails = overlayContent.querySelectorAll(".thumbnail");
    thumbnails.forEach(thumbnail => {
      thumbnail.addEventListener("mouseover", () => {
        mainImage.src = thumbnail.src;
      });
    });

    overlay.classList.remove("d-none");
    setTimeout(() => overlay.classList.add("active"), 10);

    overlay.addEventListener("click", function handleOverlayClick(event) {
      if (event.target === overlay) {
        cerrarOverlay();
        overlay.removeEventListener("click", handleOverlayClick);
      }
    });

    overlayContent.addEventListener("mouseleave", cerrarOverlay);

  } catch (error) {
    console.error("Error al mostrar atributos:", error);
    overlayContent.innerHTML = `<p class="text-danger text-center">Error al cargar los atributos del producto: ${error.message}</p>`;
    overlay.classList.remove("d-none");
    setTimeout(() => overlay.classList.add("active"), 10);
  } finally {
    hideSpinner();
  }
}

function sanitizeText(text) {
    // Permitir letras, números, espacios y algunos caracteres básicos, eliminando otros especiales
    return text.replace(/[^a-zA-Z0-9áéíóúÁÉÍÓÚñÑ\s.,-]/g, "").substring(0, 180);
}

document.addEventListener("DOMContentLoaded", function () {


    const observationsInput = document.getElementById("cartObservations");
    if (observationsInput) {
        observationsInput.addEventListener("input", function () {
            cartObservations = sanitizeText(this.value); // Actualizar la variable global
            this.value = cartObservations; // Reflejar el valor sanitizado en el campo
        });
    }
});

document.getElementById('cartObservations').addEventListener('input', (e) => {
    cartObservations = e.target.value;
    cart.observations = cartObservations;
    const timestamp = new Date().toISOString();
    getUserId().then(userId => {
        Promise.all([
            saveCartToIndexedDB(userId, cart, timestamp),
            syncCartWithBackend(userId, cart, timestamp)
        ]).catch(error => {
            console.error('Error al guardar observaciones:', error);
            showToast('danger', 'Error al guardar observaciones');
        });
    });
});

async function updateCartPrices(storeId) {

    // Asegurarse de que cart.items sea un array
    if (!cart.items || !Array.isArray(cart.items)) {
        console.warn('cart.items no es un array, inicializando como []:', cart.items);
        cart.items = [];
    }

    if (!cart.items.length) {
        return;
    }

    cart.items.forEach(item => {
        const updatedProduct = products.find(p => p.numero_producto === item.productId);
        if (updatedProduct) {
            item.price = convertirMonedaANumero(updatedProduct.precio_final_con_descuento);
            item.precioLista = convertirMonedaANumero(updatedProduct.precio_final_con_iva);
            item.unidadMedida = updatedProduct.unidad_medida;
            item.multiplo = Number(updatedProduct.multiplo.toFixed(2));
            item.available = true; // Marcar como disponible

        } else {
            console.warn(`Producto ${item.productId} no encontrado en la tienda ${storeId}. Marcando como no disponible.`);
            item.available = false; // Marcar como no disponible
        }
    });

    const timestamp = new Date().toISOString();
    const userId = await getUserId();
    if (!userId) {
        console.warn('Usuario no autenticado, guardando solo en IndexedDB');
        await saveCartToIndexedDB('anonymous', cart, timestamp);
        return;
    }

    await Promise.all([
        saveCartToIndexedDB(userId, cart, timestamp),
        syncCartWithBackend(userId, cart, timestamp)
    ]).then(() => {
        if (cart.items.length > 0) {
        }
    }).catch(error => {
        console.error('Error al guardar carrito después de actualizar precios:', error);
        showToast('danger', 'Error al guardar los cambios del carrito.');
    });
}

function cerrarOverlay() {
    const overlay = document.getElementById("productOverlay");
    const overlayContent = document.getElementById("overlayContent");

    if (overlay && overlayContent) {
        overlay.classList.remove("active");
        setTimeout(() => {
            overlay.classList.add("d-none");
            overlayContent.innerHTML = ''; // Limpiar contenido
        }, 300); // Esperar a que termine la animación
    }
}

function setupRowListeners() {
    const rows = document.querySelectorAll("#productList tr");
    if (!rows) {
        console.warn("No se encontraron filas para configurar listeners.");
        return;
    }

    rows.forEach(row => {
        row.addEventListener("mousedown", function (event) {
            this.dataset.isSelecting = "false";
        });

        row.addEventListener("mousemove", function () {
            this.dataset.isSelecting = "true";
        });

        row.addEventListener("mouseup", function (event) {
            // Excluir clics en .product-image, .btn-stock y .btn-cart
            if (this.dataset.isSelecting === "false") {
                if (!event.target.closest(".product-image, .btn-stock, .btn-cart, input, textarea")) {
                    mostrarAtributos(this.dataset.productId);
                }
            }
        });
    });
}

function setupCardListeners() {
    const cards = document.querySelectorAll(".card");
    if (!cards.length) {
        console.warn("No se encontraron cards para configurar listeners.");
        return;
    }

    cards.forEach(card => {
        card.addEventListener("mousedown", function (event) {
            this.dataset.isSelecting = "false";
        });

        card.addEventListener("mousemove", function () {
            this.dataset.isSelecting = "true";
        });

        card.addEventListener("mouseup", function (event) {
            // Excluir clics en .product-image, .btn-stock y .btn-cart
            if (this.dataset.isSelecting === "false") {
                if (!event.target.closest(".product-image, .btn-stock, .btn-cart, input, textarea")) {
                    const productId = card.getAttribute("data-product-id");
                    if (productId) {
                        mostrarAtributos(productId);
                    }
                }
            }
        });
    });
}

document.addEventListener("DOMContentLoaded", function () {
    setTimeout(() => {
        const btnTableView = document.getElementById("btnTableView");
        const btnCardView = document.getElementById("btnCardView");

        if (!btnTableView || !btnCardView) {
            console.error("Error: No se encontraron los botones `btnTableView` o `btnCardView` en el DOM.");
            return;
        }

        btnTableView.addEventListener("click", () => cambiarVista("tabla"));
        btnCardView.addEventListener("click", () => cambiarVista("cards"));
    }, 200); //
});

function actualizarFiltrosPrecio(origen) {
    const priceRangeSelect = document.getElementById("priceRangeFilter");
    const minPriceInput = document.getElementById("minPrice");
    const maxPriceInput = document.getElementById("maxPrice");

    if (origen === "select") {
        // 🔹 Si el usuario selecciona un rango predefinido
        const priceRange = priceRangeSelect.value;
        if (!priceRange) {
            minPriceInput.value = "";
            maxPriceInput.value = "";
        } else {
            const [min, max] = priceRange.split("-").map(Number);
            minPriceInput.value = min;
            maxPriceInput.value = max === 999999999 ? "" : max;
        }
    } else if (origen === "manual") {
        // 🔹 Si el usuario edita manualmente, reseteamos el select
        priceRangeSelect.value = "";
    }

    // 🔹 Aplicar el filtro con los valores actuales
    filterAndPaginate(false);
}

// ✅ Agregar eventos separados para cada cambio
document.getElementById("priceRangeFilter").addEventListener("change", () => actualizarFiltrosPrecio("select"));
document.getElementById("minPrice").addEventListener("input", () => actualizarFiltrosPrecio("manual"));
document.getElementById("maxPrice").addEventListener("input", () => actualizarFiltrosPrecio("manual"));

function initLazyLoading() {
    const lazyImages = document.querySelectorAll("img.lazyload");

    if ("IntersectionObserver" in window) {
        const observer = new IntersectionObserver((entries, observer) => {
            entries.forEach(entry => {
                if (entry.isIntersecting) {
                    const img = entry.target;
                    const dataSrc = img.getAttribute("data-src");
                    const container = img.closest(".image-container");
                    const spinner = container ? container.querySelector(".image-spinner") : null;

                    if (dataSrc) {
                        img.src = dataSrc;
                        img.onload = () => {
                            if (spinner) spinner.style.display = "none"; // Ocultar spinner cuando la imagen carga
                            img.classList.add("loaded");
                            img.classList.remove("lazyload");
                            observer.unobserve(img);
                        };
                            img.onerror = () => {
                            if (spinner) spinner.style.display = "none"; // Ocultar spinner si falla
                            img.src = "/images/default.jpg"; // Imagen por defecto en caso de error
                        };
                        img.removeAttribute("data-src");
                    }
                }
            });
        }, {
            rootMargin: "100px",
            threshold: 0.1
        });

        lazyImages.forEach(img => {
            if (img.getAttribute("data-src")) {
                observer.observe(img);
            }
        });
    } else {
        // Fallback para navegadores sin IntersectionObserver
        lazyImages.forEach(img => {
            const dataSrc = img.getAttribute("data-src");
            if (dataSrc) {
                img.src = dataSrc;
                img.onload = () => {
                    const container = img.closest(".image-container");
                    const spinner = container ? container.querySelector(".image-spinner") : null;
                    if (spinner) spinner.style.display = "none";
                };
                img.removeAttribute("data-src");
            }
        });
    }
}

// Ejecutar al cargar la página
document.addEventListener("DOMContentLoaded", initLazyLoading);

function ajustarImagenModal() {
    const modalImg = document.getElementById("modalImage");
    if (!modalImg) return;

    if (modalImg.src.includes("default.jpg")) {
        modalImg.style.width = "400px";  // 🔹 Ajusta a un tamaño fijo si es default
        modalImg.style.height = "auto";
    } else {
        modalImg.style.width = "";
        modalImg.style.height = "";
    }
}

function limpiarCacheImagenes() {
    const keys = Object.keys(localStorage);
    keys.forEach(key => {
        if (key.startsWith("img_cache_")) {
            localStorage.removeItem(key);
        }
    });
}

function limpiarCacheCada24Horas() {
    const cacheKey = "ultimaLimpiezaCache";
    const ultimaLimpieza = localStorage.getItem(cacheKey);
    const ahora = Date.now();
    const unDiaEnMilisegundos = 24 * 60 * 60 * 1000;

    if (!ultimaLimpieza || (ahora - parseInt(ultimaLimpieza, 10)) > unDiaEnMilisegundos) {
        limpiarCacheImagenes();
        localStorage.setItem(cacheKey, ahora.toString());
    }
}

function detectarHardRefresh() {
    const navigationEntries = performance.getEntriesByType("navigation");

    if (navigationEntries.length > 0 && navigationEntries[0].type === "reload") {

        if (sessionStorage.getItem("ultimaRecarga") === "soft") {
            limpiarCacheImagenes();
        }

        sessionStorage.setItem("ultimaRecarga", "hard");
    } else {
        sessionStorage.setItem("ultimaRecarga", "soft");
    }
}


document.addEventListener("DOMContentLoaded", () => {
    limpiarCacheCada24Horas();
    detectarHardRefresh();
});

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
            quantity: quantity,
            unidadMedida: currentProductToAdd.unidadMedida,
            multiplo: currentProductToAdd.multiplo,
            precioLista: currentProductToAdd.precioLista
        });
    }


    const timestamp = new Date().toISOString();
    getUserId().then(userId => {
        Promise.all([
            saveCartToIndexedDB(userId, cart, timestamp),
            syncCartWithBackend(userId, cart, timestamp)
        ]).then(() => {
            updateCartDisplay();
            modal.hide();
            addButton.disabled = false;
        }).catch(error => {
            console.error('Error al guardar el carrito:', error);
            showToast('danger', 'Error al guardar el carrito');
        });
    });
}

function updateCartDisplay() {
    const cartItemsDesktop = document.getElementById("cartItemsDesktop");
    const cartItemsMobile = document.getElementById("cartItemsMobile");
    const cartItemCount = document.getElementById("cartItemCount");
    const cartTotalFloat = document.getElementById("cartTotalFloat");
    const cartTotalFixed = document.getElementById("cartTotalFixed");
    const cartClientInfo = document.getElementById("cartClientInfo");
    let cartClientFloat = document.getElementById("cartClientFloat");
    const observationsInput = document.getElementById("cartObservations");

    if (!cartItemCount || !cartTotalFloat || !cartTotalFixed) {
        console.error("Elementos críticos 'cartItemCount', 'cartTotalFloat' o 'cartTotalFixed' no encontrados en el DOM");
        return;
    }


    if (observationsInput) {
        cartObservations = sanitizeText(observationsInput.value);
    }

    if (!cartClientFloat) {
        console.warn("Elemento 'cartClientFloat' no encontrado; creándolo dinámicamente");
        const floatButton = document.querySelector(".cart-float-btn");
        if (floatButton) {
            cartClientFloat = document.createElement("span");
            cartClientFloat.id = "cartClientFloat";
            cartClientFloat.textContent = "Sin cliente";
            floatButton.appendChild(cartClientFloat);
        }
    }

    if (cartClientFloat) {
        cartClientFloat.textContent = cart.client && cart.client.nombre_cliente ? cart.client.nombre_cliente : "Sin cliente";
    }

    let quotationIdDisplay = document.getElementById("quotationIdDisplay");
    if (!quotationIdDisplay) {
        quotationIdDisplay = document.createElement("p");
        quotationIdDisplay.id = "quotationIdDisplay";
        quotationIdDisplay.className = "text-muted mb-2";
        if (cartClientInfo && cartClientInfo.parentElement) {
            cartClientInfo.parentElement.insertBefore(quotationIdDisplay, cartClientInfo.nextSibling);
        } else {
            console.warn("No se encontró cartClientInfo o su padre para insertar quotationIdDisplay");
        }
    }
    if (quotationIdDisplay) {
        if (cart.quotation_id && cart.type !== 'new') {
            quotationIdDisplay.textContent = `Presupuesto: ${cart.quotation_id} (${cart.type === 'd365' ? 'D365' : 'Local'})`;
        } else {
            quotationIdDisplay.textContent = "Nuevo Presupuesto";
        }
    }

    if (cart.client && cartClientInfo) {
        document.getElementById("cartClientName").textContent = cart.client.nombre_cliente || "N/A";
        document.getElementById("cartClientNumber").textContent = cart.client.numero_cliente || "N/A";
        document.getElementById("cartClientNif").textContent = cart.client.nif || "N/A";
        document.getElementById("cartClientAddress").textContent = cart.client.direccion_completa || "N/A";
        cartClientInfo.style.display = "block";
    } else if (cartClientInfo) {
        cartClientInfo.style.display = "none";
    }

    if (cartItemsDesktop && cartItemsMobile) {
        cartItemsDesktop.innerHTML = "";
        cartItemsMobile.innerHTML = "";

        const isMobile = window.innerWidth <= 768;
        if (isMobile) {
            cartItemsDesktop.parentElement.style.display = "none";
            cartItemsMobile.parentElement.style.display = "block";
        } else {
            cartItemsDesktop.parentElement.style.display = "table";
            cartItemsMobile.parentElement.style.display = "none";
        }

        let total = 0;
        const storeId = document.getElementById("storeFilter").value || "BA001GC";

        if (!cart.items || !Array.isArray(cart.items)) {
            console.error('cart.items no está definido o no es un array:', cart.items);
            cartItemsDesktop.innerHTML = '<tr><td colspan="6" class="text-muted">El carrito está vacío o no se pudo cargar.</td></tr>';
            cartItemsMobile.innerHTML = '<p class="text-muted">El carrito está vacío o no se pudo cargar.</p>';
            cartTotalFixed.textContent = '$0,00';
            cartTotalFloat.textContent = '0,00';
            cartItemCount.textContent = '0';
            if (observationsInput) {
                observationsInput.value = cartObservations;
            }
            return;
        }


        cart.items.forEach((item, index) => {
            if (item.productId) {
                const itemTotal = item.available !== false ? item.price * item.quantity : 0;
                if (item.available !== false) {
                    total += itemTotal;
                }
                const cajasText = calcularCajas(item.quantity, item.multiplo, item.unidadMedida);
                const availabilityText = item.available === false ? '<span class="text-danger">No disponible en esta tienda</span>' : '';
                const disabledAttr = item.available === false ? 'disabled' : '';

                if (isMobile) {
                    const row = document.createElement("div");
                    row.className = "cart-row-mobile mb-3 p-2 border-bottom";
                    row.innerHTML = `
                        <div class="cart-row-item d-flex justify-content-between" style="width: 100%; padding: 5px 0;">
                            <span style="font-weight: 600; min-width: 100px;"><strong>SKU:</strong></span>
                            <span style="flex: 1; text-align: right;">${item.productId}</span>
                        </div>
                        <div class="cart-row-item d-flex justify-content-between" style="width: 100%; padding: 5px 0;">
                            <span style="font-weight: 600; min-width: 100px;"><strong>Producto:</strong></span>
                            <span style="flex: 1; text-align: right;">${item.productName} ${availabilityText}</span>
                        </div>
                        <div class="cart-row-item d-flex justify-content-between" style="width: 100%; padding: 5px 0;">
                            <span style="font-weight: 600; min-width: 100px;"><strong>Precio:</strong></span>
                            <span style="flex: 1; text-align: right;">${item.available !== false ? '$' + formatearMoneda(item.price) : 'N/A'}</span>
                        </div>
                        <div class="cart-row-item d-flex justify-content-between" style="width: 100%; padding: 5px 0;">
                            <span style="font-weight: 600; min-width: 100px;"><strong>Total:</strong></span>
                            <span style="flex: 1; text-align: right;">${item.available !== false ? '$' + formatearMoneda(itemTotal) : 'N/A'}</span>
                        </div>
                        <div class="cart-row-item d-flex justify-content-end" style="width: 100%; padding: 5px 0;">
                            <button class="btn btn-outline-primary btn-sm me-2" onclick="buscarStock(event, '${item.productName}', '${item.productId}', '${item.unidadMedida || ''}', '${storeId}')" title="Ver Stock" ${disabledAttr}>
                                <i class="bi bi-box-seam"></i>
                            </button>
                            <button class="btn btn-danger btn-sm" onclick="removeFromCart(${index})" title="Eliminar">
                                <i class="bi bi-trash"></i>
                            </button>
                        </div>
                        <div class="cart-row-item cart-quantity d-flex justify-content-between align-items-start" style="width: 100%; padding-top: 10px;">
                            <span style="font-weight: 600; min-width: 100px;"><strong>Cant.:</strong></span>
                            <div style="display: flex; flex-direction: column; align-items: flex-end;">
                                <div class="input-group input-group-sm" style="width: 150px;">
                                    <button class="btn btn-outline-secondary" onclick="adjustCartQuantity(${index}, -1)" ${disabledAttr}>
                                        <i class="bi bi-dash"></i>
                                    </button>
                                    <input type="number" class="form-control text-center cart-quantity-input" value="${item.quantity.toFixed(2)}" min="${item.multiplo}" step="${item.multiplo}" onchange="updateCartQuantity(${index}, this.value)" ${disabledAttr}>
                                    <span class="input-group-text">${item.unidadMedida || ''}</span>
                                    <button class="btn btn-outline-secondary" onclick="adjustCartQuantity(${index}, 1)" ${disabledAttr}>
                                        <i class="bi bi-plus"></i>
                                    </button>
                                </div>
                                ${cajasText ? `<small class="text-muted d-block mt-1 equivalencia-texto">${cajasText}</small>` : ""}
                            </div>
                        </div>
                    `;
                    cartItemsMobile.appendChild(row);
                } else {
                    const row = document.createElement("tr");
                    row.className = "cart-row";
                    row.innerHTML = `
                        <td>${item.productId}</td>
                        <td>${item.productName} ${availabilityText}</td>
                        <td class="cart-quantity">
                            <div class="quantity-wrapper">
                                <div class="input-group input-group-sm">
                                    <button class="btn btn-outline-secondary" onclick="adjustCartQuantity(${index}, -1)" ${disabledAttr}>
                                        <i class="bi bi-dash"></i>
                                    </button>
                                    <input type="number" class="form-control text-center cart-quantity-input" value="${item.quantity.toFixed(2)}" min="${item.multiplo}" step="${item.multiplo}" onchange="updateCartQuantity(${index}, this.value)" ${disabledAttr}>
                                    <span class="input-group-text">${item.unidadMedida || ''}</span>
                                    <button class="btn btn-outline-secondary" onclick="adjustCartQuantity(${index}, 1)" ${disabledAttr}>
                                        <i class="bi bi-plus"></i>
                                    </button>
                                </div>
                                ${cajasText ? `<small class="text-muted equivalencia-texto">${cajasText}</small>` : ""}
                            </div>
                        </td>
                        <td>${item.available !== false ? '$' + formatearMoneda(item.price) : 'N/A'}</td>
                        <td>${item.available !== false ? '$' + formatearMoneda(itemTotal) : 'N/A'}</td>
                        <td class="cart-actions">
                            <div class="action-buttons d-flex gap-2">
                                <button class="btn btn-outline-primary btn-sm" onclick="buscarStock(event, '${item.productName}', '${item.productId}', '${item.unidadMedida || ''}', '${storeId}')" title="Ver Stock" ${disabledAttr}>
                                    <i class="bi bi-box-seam"></i>
                                </button>
                                <button class="btn btn-danger btn-sm" onclick="removeFromCart(${index})" title="Eliminar">
                                    <i class="bi bi-trash"></i>
                                </button>
                            </div>
                        </td>
                    `;
                    cartItemsDesktop.appendChild(row);
                }
            } else {
                console.warn(`Ítem inválido en el carrito (índice ${index}):`, item);
            }
        });

        cartTotalFixed.textContent = `$${formatearMoneda(total)}`;
        cartTotalFloat.textContent = formatearMoneda(total);
        cartItemCount.textContent = cart.items.filter(item => item.productId).length;

        if (observationsInput) {
            observationsInput.value = cartObservations;
        }

    }
}

window.addEventListener("resize", () => {
  const isMobileResize = window.innerWidth <= 768;
  if (isMobileResize) {
    cartItemsDesktop.parentElement.style.display = "none";
    cartItemsMobile.parentElement.style.display = "block";
  } else {
    cartItemsDesktop.parentElement.style.display = "table";
    cartItemsMobile.parentElement.style.display = "none";
  }
  updateCartDisplay();
});

function updateCartQuantity(index, newQuantity) {
    const quantity = parseFloat(newQuantity);
    const cantidadValidada = validarCantidad(cart[index].multiplo, quantity);
    cart[index].quantity = cantidadValidada;
    updateCartDisplay();
}

function adjustCartQuantity(index, delta) {
    try {
        // Validar que el ítem existe y tiene las propiedades necesarias
        if (!cart.items[index]) {
            throw new Error(`Ítem en índice ${index} no existe en cart.items`);
        }
        if (typeof cart.items[index].quantity !== 'number' || isNaN(cart.items[index].quantity)) {
            console.warn(`Cantidad inválida para ítem ${index}, inicializando a 1:`, cart.items[index].quantity);
            cart.items[index].quantity = 1;
        }
        if (typeof cart.items[index].multiplo !== 'number' || isNaN(cart.items[index].multiplo)) {
            console.warn(`Múltiplo inválido para ítem ${index}, inicializando a 1:`, cart.items[index].multiplo);
            cart.items[index].multiplo = 1;
        }

        let quantity = cart.items[index].quantity + (delta * cart.items[index].multiplo);
        if (quantity < cart.items[index].multiplo) quantity = cart.items[index].multiplo;
        quantity = Number(quantity.toFixed(2));
        const cantidadValidada = validarCantidad(cart.items[index].multiplo, quantity);
        cart.items[index].quantity = cantidadValidada;

        // Sincronizar con IndexedDB y backend
        const timestamp = new Date().toISOString();
        getUserId().then(userId => {
            if (!userId) {
                console.warn('Usuario no autenticado, guardando solo en IndexedDB');
                saveCartToIndexedDB('anonymous', cart, timestamp).then(() => {
                    updateCartDisplay();
                });
                return;
            }
            Promise.all([
                saveCartToIndexedDB(userId, cart, timestamp),
                syncCartWithBackend(userId, cart, timestamp)
            ]).then(() => {
                updateCartDisplay();
            }).catch(error => {
                console.error('Error al sincronizar cantidad:', error);
                showToast('danger', 'Error al guardar la cantidad');
            });
        });
    } catch (error) {
        console.error('DEBUG: Error en adjustCartQuantity:', error);
        showToast('danger', `Error al ajustar cantidad: ${error.message}`);
    }
}

function clearCart() {
    cart = {
        items: [],
        client: null,
        quotation_id: null,
        type: 'new',
        observations: ''
    };
    cartObservations = '';
    document.getElementById("cartObservations").value = '';
    const timestamp = new Date().toISOString();
    getUserId().then(userId => {
        Promise.all([
            saveCartToIndexedDB(userId, cart, timestamp),
            syncCartWithBackend(userId, cart, timestamp)
        ]).then(() => {
            updateCartDisplay();
        }).catch(error => {
            console.error('Error al vaciar el carrito:', error);
            showToast('danger', 'Error al vaciar el carrito');
        });
    });
}

function removeFromCart(index) {
    cart.items.splice(index, 1);
    const timestamp = new Date().toISOString();
    getUserId().then(userId => {
        if (!userId) {
            console.warn('Usuario no autenticado, guardando solo en IndexedDB');
            saveCartToIndexedDB('anonymous', cart, timestamp).then(() => {
                updateCartDisplay();
            });
            return;
        }
        Promise.all([
            saveCartToIndexedDB(userId, cart, timestamp),
            syncCartWithBackend(userId, cart, timestamp)
        ]).then(() => {
            updateCartDisplay();
        }).catch(error => {
            console.error('Error al eliminar ítem del carrito:', error);
            showToast('danger', 'Error al eliminar el producto');
        });
    });
}

function toggleCart() {
    const cartOverlay = document.getElementById("cartOverlay");
    cartOverlay.classList.toggle("d-none");
    updateCartDisplay();
}

document.addEventListener("DOMContentLoaded", function () {
    const quantityInput = document.getElementById("quantityInput");
    quantityInput.addEventListener("change", function () {
        const quantity = parseFloat(this.value);
        const cantidadValidada = validarCantidad(currentProductToAdd.multiplo, quantity);
        this.value = cantidadValidada;
        const cajasElement = document.getElementById("quantityModalCajas");
        cajasElement.textContent = calcularCajas(cantidadValidada, currentProductToAdd.multiplo, currentProductToAdd.unidadMedida);
        updateTotal();
    });
});

function showClientDetailsModal() {
    const modalElement = document.getElementById("clientDetailsModal");
    if (!modalElement) {
        console.error("Error: El elemento con ID 'clientDetailsModal' no se encuentra en el DOM.");
        showToast("danger", "No se pudo abrir el modal de detalles del cliente.");
        return;
    }
    if (!cart.client) {
        console.error("Error: No hay cliente seleccionado en el carrito.");
        showToast("warning", "No hay cliente seleccionado para mostrar detalles.");
        return;
    }
    if (typeof bootstrap === "undefined" || !bootstrap.Modal) {
        console.error("Error: Bootstrap no está cargado o la clase Modal no está definida.");
        showToast("danger", "Error de configuración: Bootstrap no está disponible.");
        return;
    }

    // Llenar los datos del cliente (ya se hace en selectClient, pero usamos los mismos datos aquí)
    document.getElementById("modalClientName").textContent = cart.client.nombre_cliente || "N/A";
    document.getElementById("modalClientNumber").textContent = cart.client.numero_cliente || "N/A";
    document.getElementById("modalClientBlocked").textContent = cart.client.bloqueado || "N/A";
    document.getElementById("modalClientType").textContent = cart.client.tipo_contribuyente || "N/A";
    document.getElementById("modalClientCreditLimit").textContent = cart.client.limite_credito ? `$${cart.client.limite_credito.toFixed(2)}` : "N/A";
    document.getElementById("modalClientTaxGroup").textContent = cart.client.grupo_impuestos || "N/A";
    document.getElementById("modalClientNif").textContent = cart.client.nif || "N/A";
    document.getElementById("modalClientTif").textContent = cart.client.tif || "N/A";
    document.getElementById("modalClientAddress").textContent = cart.client.direccion_completa || "N/A";
    document.getElementById("modalClientEmail").textContent = cart.client.email_contacto || "N/A";
    document.getElementById("modalClientPhone").textContent = cart.client.telefono_contacto || "N/A";
    document.getElementById("modalClientCreateDate").textContent = cart.client.fecha_creacion || "N/A";
    document.getElementById("modalClientModDate").textContent = cart.client.fecha_modificacion || "N/A";

    const modal = new bootstrap.Modal(modalElement);
    modal.show();
}

// Variable global para almacenar el número del presupuesto
let lastQuotationNumber = null;

// Mostrar modal de selección de tipo
function showQuotationTypeModal() {
  if (!cart.items.length) {
    showToast('danger', 'El carrito está vacío.');
    return;
  }
  if (!cart.client || !cart.client.numero_cliente) {
    showToast('danger', 'Debe seleccionar un cliente antes de generar el presupuesto.');
    return;
  }
  const modal = new bootstrap.Modal(document.getElementById('quotationTypeModal'));
  modal.show();
}

// Crear presupuesto con el tipo seleccionado
async function createInD365WithType(tipo_presupuesto) {
    const typeModal = bootstrap.Modal.getInstance(document.getElementById('quotationTypeModal'));
    typeModal.hide();

    if (!cart.items.length) {
        showToast('danger', 'El carrito está vacío.');
        return;
    }

    if (!cart.client || !cart.client.numero_cliente) {
        showToast('danger', 'Debe seleccionar un cliente antes de generar el presupuesto.');
        return;
    }

    showSpinner();
    try {
        const storeId = document.getElementById("storeFilter").value || "BA001GC";
        const payload = {
            cart: {
                items: cart.items.filter(item => item.productId),
                client: cart.client || {},
                observations: cartObservations
            },
            store_id: storeId,
            tipo_presupuesto: tipo_presupuesto
        };

        let response;
        if (cart.type === "d365" && cart.quotation_id) {
            // Actualizar presupuesto existente en D365
            response = await fetch(`/api/update_quotation/${cart.quotation_id}`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
        } else {
            // Crear nuevo presupuesto en D365
            response = await fetch('/api/create_quotation', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
        }

        const result = await response.json();
        if (!response.ok) throw new Error(result.error || "Error al procesar el presupuesto");

        lastQuotationNumber = result.quotation_number;
        toggleCart();

        // Mostrar modal de confirmación para imprimir
        await showPrintConfirmationModal(lastQuotationNumber, tipo_presupuesto);

        // No limpiar aquí, se manejará en closePrintModal
    } catch (error) {
        console.error("Error al procesar en D365:", error);
        showToast('danger', `Error: ${error.message}`);
    } finally {
        hideSpinner();
    }
}

// Mostrar modal de confirmación de impresión
function showPrintConfirmationModal(quotationNumber, tipo) {
    const modal = new bootstrap.Modal(document.getElementById('printConfirmationModal'));
    document.getElementById('printConfirmationMessage').textContent =
        `Se generó correctamente el presupuesto ${quotationNumber} (${tipo}).`;
    modal.show();
}

// Cerrar modal de confirmación y manejar la acción
function closePrintModal(print) {
    const modal = bootstrap.Modal.getInstance(document.getElementById('printConfirmationModal'));
    modal.hide();

    if (print) {
        generatePDF().then(() => {
            // Limpiar carrito y observaciones después de generar el PDF
            cartObservations = "";
            document.getElementById("cartObservations").value = "";
            delete cart.quotation_id;
            delete cart.type;
            clearCart();

            // Sincronizar el carrito después de limpiar
            const timestamp = new Date().toISOString();
            getUserId().then(userId => {
                if (!userId) {
                    console.warn('Usuario no autenticado, guardando solo en IndexedDB');
                    saveCartToIndexedDB('anonymous', cart, timestamp).then(() => {
                        updateCartDisplay();
                    });
                    return;
                }
                Promise.all([
                    saveCartToIndexedDB(userId, cart, timestamp),
                    syncCartWithBackend(userId, cart, timestamp)
                ]).then(() => {
                    updateCartDisplay();
                }).catch(error => {
                    console.error('Error al sincronizar el carrito después de limpiar:', error);
                });
            });
        }).catch(error => {
            console.error("Error al generar PDF:", error);
            showToast('danger', 'Error al generar el PDF.');

            // Limpiar carrito y observaciones incluso si falla la generación del PDF
            cartObservations = "";
            document.getElementById("cartObservations").value = "";
            delete cart.quotation_id;
            delete cart.type;
            clearCart();

            // Sincronizar el carrito después de limpiar
            const timestamp = new Date().toISOString();
            getUserId().then(userId => {
                if (!userId) {
                    console.warn('Usuario no autenticado, guardando solo en IndexedDB');
                    saveCartToIndexedDB('anonymous', cart, timestamp).then(() => {
                        updateCartDisplay();
                    });
                    return;
                }
                Promise.all([
                    saveCartToIndexedDB(userId, cart, timestamp),
                    syncCartWithBackend(userId, cart, timestamp)
                ]).then(() => {
                    updateCartDisplay();
                }).catch(error => {
                    console.error('Error al sincronizar el carrito después de limpiar:', error);
                });
            });
        });
    } else {
        // Limpiar carrito y observaciones si el usuario no imprime
        cartObservations = "";
        document.getElementById("cartObservations").value = "";
        delete cart.quotation_id;
        delete cart.type;
        clearCart();

        // Sincronizar el carrito después de limpiar
        const timestamp = new Date().toISOString();
        getUserId().then(userId => {
            if (!userId) {
                console.warn('Usuario no autenticado, guardando solo en IndexedDB');
                saveCartToIndexedDB('anonymous', cart, timestamp).then(() => {
                    updateCartDisplay();
                });
                return;
            }
            Promise.all([
                saveCartToIndexedDB(userId, cart, timestamp),
                syncCartWithBackend(userId, cart, timestamp)
            ]).then(() => {
                updateCartDisplay();
            }).catch(error => {
                console.error('Error al sincronizar el carrito después de limpiar:', error);
            });
        });
    }
}

// Función para mostrar toast
function showToast(type, message) {
    const toastContainer = document.querySelector('.toast-container');
    const toast = document.createElement('div');
    toast.className = `toast align-items-center text-bg-${type} border-0`;
    toast.role = 'alert';
    toast.innerHTML = `
        <div class="d-flex">
            <div class="toast-body">${message}</div>
            <button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast" aria-label="Close"></button>
        </div>
    `;
    toastContainer.appendChild(toast);
    const bsToast = new bootstrap.Toast(toast);
    bsToast.show();
    setTimeout(() => toast.remove(), 5000); // Remover después de 5 segundos
}

function generatePDF() {
    return new Promise((resolve, reject) => {

        if (typeof window.jspdf === 'undefined' || !window.jspdf.jsPDF) {
            console.error("jsPDF no está definido. Asegúrate de que la biblioteca esté cargada.");
            showToast('danger', 'Error: No se pudo generar el PDF. Falta la biblioteca jsPDF.');
            reject(new Error("jsPDF no está definido"));
            return;
        }

        const { jsPDF } = window.jspdf;
        const doc = new jsPDF({
            orientation: "portrait",
            unit: "mm",
            format: "a4"
        });

        doc.setFont("helvetica", "normal");

        // Constantes para el manejo del espacio
        const PAGE_HEIGHT = doc.internal.pageSize.getHeight();
        const FOOTER_HEIGHT = 20;
        const BOTTOM_MARGIN = FOOTER_HEIGHT + 10;
        const CONTENT_BOTTOM_LIMIT = PAGE_HEIGHT - BOTTOM_MARGIN;

        const currentDate = new Date().toLocaleString("es-AR", {
            day: "2-digit",
            month: "2-digit",
            year: "numeric",
            hour: "2-digit",
            minute: "2-digit",
            hour12: false
        });
        const expiryDate = new Date();
        expiryDate.setDate(expiryDate.getDate() + 1);
        const validUntil = expiryDate.toLocaleString("es-AR", {
            day: "2-digit",
            month: "2-digit",
            year: "numeric"
        });

        const storeId = document.getElementById("storeFilter").value || "BA001GC";
        let direccionSucursal = "Dirección no disponible";
        let vendedor = "Vendedor no disponible";

        const cartItems = JSON.parse(JSON.stringify(cart.items.filter(item => item.productId)));
        const cartClient = cart.client ? JSON.parse(JSON.stringify(cart.client)) : null;
        const cartCopy = { items: cartItems, client: cartClient };

        const addLogo = () => {
            return new Promise((resolve) => {
                const img = new Image();
                img.crossOrigin = "Anonymous";
                img.onload = () => {
                    const pageWidth = doc.internal.pageSize.getWidth();
                    const logoWidth = 50;
                    const logoHeight = 12;
                    const xPosition = pageWidth - logoWidth - 5;
                    doc.addImage(img, "PNG", xPosition, 20, logoWidth, logoHeight);
                    resolve();
                };
                img.onerror = () => {
                    console.warn("No se pudo cargar el logo.");
                    resolve();
                };
                img.src = "/static/img/logo_0.png";
            });
        };

        const addFooter = () => {
            return new Promise((resolve) => {
                const img = new Image();
                img.crossOrigin = "Anonymous";
                img.onload = () => {
                    const pageWidth = doc.internal.pageSize.getWidth();
                    const footerWidth = pageWidth - 20;
                    const footerHeight = (img.height * footerWidth) / img.width;
                    const footerYPosition = PAGE_HEIGHT - footerHeight - 10;
                    const pageCount = doc.internal.getNumberOfPages();
                    for (let i = 1; i <= pageCount; i++) {
                        doc.setPage(i);
                        doc.addImage(img, "PNG", 10, footerYPosition, footerWidth, footerHeight);
                    }
                    resolve(footerHeight);
                };
                img.onerror = () => {
                    console.warn("No se pudo cargar el pie de página.");
                    resolve(FOOTER_HEIGHT);
                };
                img.src = "/static/img/pie.png";
            });
        };

        const checkPageBreak = (currentY, spaceNeeded, footerHeight) => {
            if (currentY + spaceNeeded > (PAGE_HEIGHT - footerHeight - 10)) {
                doc.addPage();
                return 10;
            }
            return currentY;
        };

        const addTextWithPageBreak = (text, x, y, footerHeight, options = {}) => {
            const lineHeight = doc.getLineHeight() / doc.internal.scaleFactor;
            const lines = doc.splitTextToSize(text, options.maxWidth || (doc.internal.pageSize.getWidth() - x - 10));
            let currentY = y;

            lines.forEach(line => {
                currentY = checkPageBreak(currentY, lineHeight, footerHeight);
                doc.text(line, x, currentY, options);
                currentY += lineHeight;
            });

            return currentY;
        };

        const generateContent = async () => {
            await addLogo();
            let footerHeight = await addFooter();
            const pageWidth = doc.internal.pageSize.getWidth();
            let currentY = 10;

            // Título
            doc.setFontSize(16);
            const titleWidth = doc.getTextWidth("Presupuesto");
            currentY = checkPageBreak(currentY, 15, footerHeight);
            doc.text("Presupuesto", pageWidth / 2 - titleWidth / 2, currentY);
            currentY += 15;

            // Datos del presupuesto
            doc.setFontSize(8);
            currentY = checkPageBreak(currentY, 18, footerHeight);
            doc.text(`Presupuesto Nro. ${lastQuotationNumber || 'N/A'}`, 10, currentY);
            doc.text(`Fecha y Hora: ${currentDate}`, 10, currentY + 6);
            doc.text(`Válido hasta: ${validUntil}`, 10, currentY + 12);
            currentY += 18;

            // Dirección sucursal
            currentY = checkPageBreak(currentY, 10, footerHeight);
            try {
                const response = await fetch(`/api/datos_tienda/${storeId}`);
                if (!response.ok) throw new Error("Error al obtener datos de la sucursal");
                const data = await response.json();
                direccionSucursal = data.direccion_completa_unidad_operativa || "Dirección no disponible";
            } catch (error) {
                console.error("Error al obtener datos de la sucursal:", error);
            }
            doc.text(direccionSucursal, pageWidth - 10, currentY, { align: "right" });
            currentY += 10;

            // Datos del cliente
            doc.setFontSize(10);
            currentY = checkPageBreak(currentY, 25, footerHeight);
            doc.text("Preparado para", 10, currentY);
            doc.setFontSize(8);
            const clienteNombre = cartCopy.client?.nombre_cliente || "Consumidor Final";
            const clienteId = cartCopy.client?.numero_cliente || "N/A";
            const clienteIva = cartCopy.client?.tipo_contribuyente || "N/A";
            doc.text(`Código de Cliente: ${clienteId}`, 10, currentY + 8);
            doc.text(`Nombre de Cliente: ${clienteNombre}`, 10, currentY + 14);
            doc.text(`Condición IVA: ${clienteIva}`, 10, currentY + 20);
            currentY += 25;

            // Vendedor
            currentY = checkPageBreak(currentY, 10, footerHeight);
            try {
                const response = await fetch('/api/user_info');
                if (!response.ok) throw new Error(`Error al obtener vendedor: ${response.status}`);
                const data = await response.json();
                vendedor = data.nombre_completo || "Vendedor no disponible";
            } catch (error) {
                console.warn("Error al obtener vendedor:", error);
                vendedor = "Usuario desconocido";
            }
            doc.text(`Vendedor: ${vendedor}`, pageWidth - 10, currentY, { align: "right" });
            currentY += 10;

            // Tabla de productos
            const tableHeaders = [
                "Código",
                "Descripción",
                "Cantidad",
                "U.M",
                "Precio Unitario Lista",
                "Precio Unitario con Desc.",
                "Importe Total"
            ];

            const tableData = cartCopy.items.map(item => {
                const precioFinalConIva = convertirMonedaANumero(item.precioLista) || 0;
                const precioFinalConDescuento = convertirMonedaANumero(item.price) || 0;
                const quantity = parseFloat(item.quantity) || 1;
                const totalDescuento = (precioFinalConIva - precioFinalConDescuento) * quantity;
                const porcentajeDescuento = precioFinalConIva !== 0
                    ? ((precioFinalConIva - precioFinalConDescuento) / precioFinalConIva) * 100
                    : 0;
                const importeTotal = precioFinalConDescuento * quantity;

                const row = [
                    item.productId || "N/A",
                    item.productName || "Producto sin nombre",
                    { content: quantity.toFixed(2).replace(".", ","), styles: { halign: "right" } },
                    item.unidadMedida || "un",
                    `$${formatearMoneda(precioFinalConIva)}`,
                    `$${formatearMoneda(precioFinalConDescuento)}`,
                    `$${formatearMoneda(importeTotal)}`
                ];

                // Solo agregar la fila de descuento si hay un descuento real
                if (totalDescuento > 0) {
                    row.push({
                        content: `Descuento: $${formatearMoneda(totalDescuento)} (${porcentajeDescuento.toFixed(2).replace(".", ",")}%)`,
                        colSpan: 7,
                        styles: { fontSize: 7, halign: "left", textColor: [100, 100, 100] }
                    });
                }

                return row;
            });

            const flattenedData = [];
            tableData.forEach(row => {
                flattenedData.push(row.slice(0, 7));
                if (row.length > 7) {
                    flattenedData.push(row.slice(7));
                }
            });

            doc.autoTable({
                startY: currentY,
                head: [tableHeaders],
                body: flattenedData,
                theme: "grid",
                headStyles: { fillColor: [180, 185, 199], textColor: [255, 255, 255], fontSize: 7 },
                bodyStyles: { fontSize: 8, lineHeight: 1.1 },
                columnStyles: {
                    0: { cellWidth: 25 },
                    1: { cellWidth: 55, overflow: "linebreak" },
                    2: { cellWidth: 20, halign: "right" },
                    3: { cellWidth: 15, halign: "center" },
                    4: { cellWidth: 25, halign: "right" },
                    5: { cellWidth: 25, halign: "right" },
                    6: { cellWidth: 25, halign: "right" }
                },
                styles: { minCellHeight: 8, cellPadding: 1 },
                margin: { left: 10, right: 10, bottom: footerHeight + 10 },
                pageBreak: "auto",
                didDrawPage: (data) => {
                    currentY = data.cursor.y;
                    const pageWidth = doc.internal.pageSize.getWidth();
                    const footerWidth = pageWidth - 20;
                    const footerYPosition = PAGE_HEIGHT - footerHeight - 10;
                    const img = new Image();
                    img.src = "/static/img/pie.png";
                    doc.addImage(img, "PNG", 10, footerYPosition, footerWidth, footerHeight);
                }
            });

            currentY = doc.lastAutoTable.finalY + 5;

            // Condiciones generales
            currentY = checkPageBreak(currentY, 10, footerHeight);
            doc.setFontSize(8);
            currentY = addTextWithPageBreak("Condiciones Generales:", 10, currentY, footerHeight);
            currentY = addTextWithPageBreak("• Oferta no vinculante, sujeta a modificación sin previo aviso", 15, currentY, footerHeight);

            // Observaciones
            currentY = checkPageBreak(currentY, 10, footerHeight);
            currentY = addTextWithPageBreak("Observaciones:", 10, currentY, footerHeight);
            if (cartObservations) {
                currentY = addTextWithPageBreak(cartObservations, 15, currentY, footerHeight, { maxWidth: 180 });
            } else {
                currentY = addTextWithPageBreak("Sin observaciones", 15, currentY, footerHeight);
            }

            // Calcular totales
            const totalDescuentos = cartCopy.items.reduce((sum, item) => {
                const precioFinalConIva = convertirMonedaANumero(item.precioLista) || 0;
                const precioFinalConDescuento = convertirMonedaANumero(item.price) || 0;
                const quantity = parseFloat(item.quantity) || 1;
                return sum + (precioFinalConIva - precioFinalConDescuento) * quantity;
            }, 0);

            const total = cartCopy.items.reduce((sum, item) => {
                const price = convertirMonedaANumero(item.price) || 0;
                const quantity = parseFloat(item.quantity) || 1;
                return sum + price * quantity;
            }, 0);

            // Crear tabla de totales solo si hay descuentos o un total válido
            const totalRows = [];
            if (totalDescuentos > 0) {
                totalRows.push(["Total de descuentos aplicados", "", "", "", "", "", `$${formatearMoneda(totalDescuentos)}`]);
            }
            totalRows.push(["Total", "", "", "", "", "", `$${formatearMoneda(total)}`]);

            doc.autoTable({
                startY: currentY,
                body: totalRows,
                theme: "plain",
                styles: { fontSize: 8, lineWidth: 0.1 },
                columnStyles: {
                    0: { cellWidth: 60, halign: "left" },
                    6: { cellWidth: 30, halign: "right" }
                },
                margin: { left: 10, right: 10, bottom: footerHeight + 10 },
                didDrawPage: (data) => {
                    currentY = data.cursor.y;
                    const pageWidth = doc.internal.pageSize.getWidth();
                    const footerWidth = pageWidth - 20;
                    const footerYPosition = PAGE_HEIGHT - footerHeight - 10;
                    const img = new Image();
                    img.src = "/static/img/pie.png";
                    doc.addImage(img, "PNG", 10, footerYPosition, footerWidth, footerHeight);
                }
            });

            currentY = doc.lastAutoTable.finalY + 6;
            currentY = checkPageBreak(currentY, 8, footerHeight);
            doc.setFontSize(10);
            currentY = addTextWithPageBreak("Forma de Pago", 10, currentY, footerHeight);
            doc.setFontSize(8);
            currentY = addTextWithPageBreak("Efectivo", 10, currentY, footerHeight);
            currentY = addTextWithPageBreak(`${formatearMoneda(total)}`, 10, currentY, footerHeight);

            const pdfBlob = doc.output('blob');
            const pdfUrl = URL.createObjectURL(pdfBlob);
            const printWindow = window.open(pdfUrl);
            printWindow.onload = () => {
                printWindow.print();
                printWindow.onfocus = () => {
                    setTimeout(() => {
                        URL.revokeObjectURL(pdfUrl);
                        printWindow.close();
                    }, 0);
                };
            };

            resolve();
        };

        generateContent().catch(error => {
            console.error("Error en generateContent:", error);
            showToast('danger', `Error al generar el PDF: ${error.message}`);
            reject(error);
        });
    });
}

// Abrir el modal de búsqueda de clientes
function openClientSearchModal() {
    const modal = new bootstrap.Modal(document.getElementById("clientSearchModal"));
    document.getElementById("clientSearchInput").value = "";
    document.getElementById("clientSearchResults").innerHTML = "";
    document.getElementById("selectedClientInfo").style.display = "none";
    document.getElementById("addClientToCartBtn").disabled = true;
    modal.show();
    setTimeout(() => document.getElementById("clientSearchInput").focus(), 500);
}

// Búsqueda al presionar Enter
document.addEventListener("DOMContentLoaded", function () {
    const addClientButton = document.querySelector('.btn-outline-success.ms-2');
    if (addClientButton) {
        addClientButton.addEventListener('click', function (e) {
            e.preventDefault();
            openAddClientModal();
        });
    }
    const clientSearchInput = document.getElementById("clientSearchInput");
    if (clientSearchInput) {
        clientSearchInput.addEventListener("keypress", (e) => {
            if (e.key === "Enter") {
                e.preventDefault();
                const query = clientSearchInput.value.trim();
                if (query.length >= 3) {
                    searchClients(query);
                } else {
                    document.getElementById("clientSearchResults").innerHTML = '<p class="text-muted">Por favor, ingresa al menos 3 caracteres y presiona Enter.</p>';
                }
            }
        });
    }
});

async function searchClients(query) {
    try {
        showSpinner();
        console.debug(`Iniciando búsqueda de clientes con query: ${query}`);
        const response = await fetchWithAuth(`/api/clientes/search?query=${encodeURIComponent(query)}`);
        const clients = await response.json(); // Procesar respuesta como JSON
        console.debug('Respuesta de /api/clientes/search:', clients);

        if (!Array.isArray(clients)) {
            console.error('La respuesta no es un array:', clients);
            throw new Error('Formato de respuesta inválido: se esperaba un array de clientes');
        }

        displayClientSearchResults(clients);
    } catch (error) {
        console.error("Error en búsqueda de clientes:", error);
        const resultsContainer = document.getElementById("clientSearchResults");
        if (resultsContainer) {
            resultsContainer.innerHTML = `<p class="text-danger">Error al buscar clientes: ${error.message}</p>`;
        }
        showToast('danger', `Error al buscar clientes: ${error.message}`);
    } finally {
        hideSpinner();
    }
}

function displayClientSearchResults(clients) {
    const resultsContainer = document.getElementById("clientSearchResults");
    if (!resultsContainer) {
        console.error("No se encontró #clientSearchResults en el DOM");
        showToast('danger', 'Error: No se pudo renderizar los resultados de búsqueda');
        return;
    }

    resultsContainer.innerHTML = "";
    console.debug(`Renderizando ${clients.length} clientes`);

    if (!Array.isArray(clients) || clients.length === 0) {
        resultsContainer.innerHTML = '<p class="text-muted">No se encontraron clientes.</p>';
        return;
    }

    const fragment = document.createDocumentFragment();
    clients.forEach(client => {
        const item = document.createElement("a");
        item.href = "#";
        item.className = "list-group-item list-group-item-action";
        item.innerHTML = `
            ${client.nombre_cliente || 'N/A'} (NIF: ${client.nif || "N/A"} | TIF: ${client.tif || "N/A"} | T. Cont: ${client.tipo_contribuyente || "N/A"} | G. Imp: ${client.grupo_impuestos || "N/A"})
        `;
        item.onclick = (e) => {
            e.preventDefault();
            console.debug('Cliente seleccionado:', client);
            selectClient(client);
        };
        fragment.appendChild(item);
    });

    resultsContainer.appendChild(fragment);
}

function selectClient(client) {
  selectedClient = client;
  const infoContainer = document.getElementById("selectedClientInfo");
  document.getElementById("clientName").textContent = client.nombre_cliente || "N/A";
  document.getElementById("clientNumber").textContent = client.numero_cliente || "N/A";
  document.getElementById("clientBlocked").textContent = client.bloqueado || "N/A";
  document.getElementById("clientType").textContent = client.tipo_contribuyente || "N/A";
  document.getElementById("clientCreditLimit").textContent = client.limite_credito ? `$${client.limite_credito.toFixed(2)}` : "N/A";
  document.getElementById("clientTaxGroup").textContent = client.grupo_impuestos || "N/A";
  document.getElementById("clientNif").textContent = client.nif || "N/A";
  document.getElementById("clientTif").textContent = client.tif || "N/A";
  document.getElementById("clientAddress").textContent = client.direccion_completa || "N/A";
  document.getElementById("clientModDate").textContent = client.fecha_modificacion || "N/A";
  document.getElementById("clientCreateDate").textContent = client.fecha_creacion || "N/A";
  document.getElementById("clientEmail").textContent = client.email_contacto || "N/A";
  document.getElementById("clientPhone").textContent = client.telefono_contacto || "N/A";
  infoContainer.style.display = "block";
  document.getElementById("addClientToCartBtn").disabled = false;
}

function addClientToCart() {
    if (!selectedClient) {
        showToast('danger', 'No se ha seleccionado un cliente');
        return;
    }

    cart.client = {
        numero_cliente: selectedClient.numero_cliente,
        nombre_cliente: selectedClient.nombre_cliente,
        nif: selectedClient.nif,
        direccion_completa: selectedClient.direccion_completa,
        bloqueado: selectedClient.bloqueado,
        tipo_contribuyente: selectedClient.tipo_contribuyente,
        limite_credito: selectedClient.limite_credito,
        grupo_impuestos: selectedClient.grupo_impuestos,
        tif: selectedClient.tif,
        email_contacto: selectedClient.email_contacto,
        telefono_contacto: selectedClient.telefono_contacto,
        fecha_creacion: selectedClient.fecha_creacion,
        fecha_modificacion: selectedClient.fecha_modificacion
    };

    const timestamp = new Date().toISOString();
    getUserId().then(userId => {
        Promise.all([
            saveCartToIndexedDB(userId, cart, timestamp),
            syncCartWithBackend(userId, cart, timestamp)
        ]).then(() => {
            updateCartDisplay();
            bootstrap.Modal.getInstance(document.getElementById('clientSearchModal')).hide();
        }).catch(error => {
            console.error('Error al guardar cliente en el carrito:', error);
            showToast('danger', 'Error al añadir el cliente');
        });
    });
}

function removeClientFromCart() {
    if (cart.type === 'd365' && cart.quotation_id) {
        const modal = new bootstrap.Modal(document.getElementById("removeClientWarningModal"));
        document.getElementById("currentQuotationId").textContent = cart.quotation_id || "N/A";

        const confirmButton = document.getElementById("confirmRemoveClient");
        const confirmRemoval = new Promise((resolve) => {
            const onConfirm = () => {
                resolve(true);
                modal.hide();
                confirmButton.removeEventListener("click", onConfirm);
            };
            confirmButton.addEventListener("click", onConfirm);

            document.getElementById("removeClientWarningModal").addEventListener('hidden.bs.modal', () => {
                resolve(false);
                confirmButton.removeEventListener("click", onConfirm);
            }, { once: true });
        });

        modal.show();

        confirmRemoval.then((confirmed) => {
            if (confirmed) {
                cart.client = null;
                cart.quotation_id = null;
                cart.type = 'new';
                const timestamp = new Date().toISOString();
                getUserId().then(userId => {
                    Promise.all([
                        saveCartToIndexedDB(userId, cart, timestamp),
                        syncCartWithBackend(userId, cart, timestamp)
                    ]).then(() => {
                        updateCartDisplay();
                        showToast('success', 'Cliente eliminado. Se generará un nuevo presupuesto.');
                    });
                });
            } else {
                showToast('info', 'Eliminación de cliente cancelada.');
            }
        });
    } else {
        cart.client = null;
        const timestamp = new Date().toISOString();
        getUserId().then(userId => {
            Promise.all([
                saveCartToIndexedDB(userId, cart, timestamp),
                syncCartWithBackend(userId, cart, timestamp)
            ]).then(() => {
                updateCartDisplay();
            });
        });
    }
}

async function generatePdfOnly() {
    if (!cart.items.length) {
        showToast('danger', 'El carrito está vacío.');
        return;
    }

    showSpinner();
    try {
        const data = await fetchWithAuth('/api/generate_pdf_quotation_id');
        lastQuotationNumber = data.quotation_id;

        const storeId = document.getElementById("storeFilter").value || "BA001GC";
        const quotationData = {
            quotation_id: lastQuotationNumber,
            type: "local",
            store_id: storeId,
            client: cart.client || null,
            items: cart.items.filter(item => item.productId),
            observations: cartObservations,
            timestamp: new Date().toISOString()
        };

        const saveResponse = await fetchWithAuth('/api/save_local_quotation', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(quotationData)
        });

        await generatePDF();

        cartObservations = "";
        document.getElementById("cartObservations").value = "";
        delete cart.quotation_id;
        delete cart.type;
        clearCart();

        const timestamp = new Date().toISOString();
        const userId = await getUserId();
        if (!userId) {
            console.warn('Usuario no autenticado, guardando solo en IndexedDB');
            await saveCartToIndexedDB('anonymous', cart, timestamp);
            updateCartDisplay();
            showToast('success', 'PDF generado y presupuesto guardado');
            return;
        }
        await Promise.all([
            saveCartToIndexedDB(userId, cart, timestamp),
            syncCartWithBackend(userId, cart, timestamp)
        ]);
        updateCartDisplay();
        showToast('success', `PDF generado y presupuesto guardado con ID: ${lastQuotationNumber}`);
    } catch (error) {
        console.error("Error al generar PDF o guardar presupuesto:", error);
        showToast('danger', `Error: ${error.message}`);
    } finally {
        toggleCart();
        hideSpinner();
    }
}

function toggleCartButtonDetails() {
    const cartButton = document.querySelector('.cart-float-btn');
    cartButton.classList.toggle('expanded');
}

// Asegúrate de que el evento onclick del botón combine ambas acciones
document.addEventListener("DOMContentLoaded", function () {
    const cartButton = document.querySelector('.cart-float-btn');
    if (cartButton) {
        // Combinar toggleCart y toggleCartButtonDetails en un solo clic
        cartButton.onclick = function () {
            toggleCart(); // Abrir/cerrar el overlay del carrito
            toggleCartButtonDetails(); // Mostrar/ocultar detalles
        };

        // Opcional: Cerrar detalles al salir del hover si no está expandido manualmente
        cartButton.addEventListener('mouseleave', function () {
            if (!this.classList.contains('expanded')) {
                this.classList.remove('expanded');
            }
        });
    }
});

// Abrir el modal para agregar cliente
function openAddClientModal() {
    const modalElement = document.getElementById("addClientModal");
    if (!modalElement) {
        console.error("Error: El elemento con ID 'addClientModal' no se encuentra en el DOM.");
        showToast("danger", "No se pudo abrir el formulario para agregar cliente.");
        return;
    }
    if (typeof bootstrap === "undefined" || !bootstrap.Modal) {
        console.error("Error: Bootstrap no está cargado o la clase Modal no está definida.");
        showToast("danger", "Error de configuración: Bootstrap no está disponible.");
        return;
    }
    const modal = new bootstrap.Modal(modalElement);
    modal.show();

    // Limpiar y deshabilitar campos al abrir
    document.getElementById("newClientDni").value = "";
    document.getElementById("dniValidationMessage").textContent = "";
    togglePersonalFields(false);
    toggleAddressFields(false);
    document.getElementById("newClientZipCode").value = "";
    document.getElementById("newClientCitySelect").innerHTML = '<option value="">Seleccione una ciudad</option>';
    document.getElementById("newClientStreet").value = "";
    document.getElementById("newClientStreetNumber").value = "";
    document.getElementById("newClientReference").value = "";
    document.getElementById("newClientLatitude").value = "";
    document.getElementById("newClientLongitude").value = "";
    initMap();
}

// Inicializar el mapa de Google Maps
let map, geocoder, marker;
let postalCodeData = [];
let selectedCityData = null;

function initMap() {
    if (typeof google === 'undefined' || !google.maps) {
        console.error("Google Maps no está cargado aún. Esperando carga...");
        setTimeout(initMap, 500);
        return;
    }

    map = new google.maps.Map(document.getElementById("map"), {
        zoom: 8,
        center: { lat: -34.6037, lng: -58.3816 },
    });
    geocoder = new google.maps.Geocoder();
    marker = new google.maps.Marker({
        map: map,
        position: { lat: -34.6037, lng: -58.3816 },
        draggable: true,
    });

    if (navigator.geolocation) {
        navigator.geolocation.getCurrentPosition(
            (position) => {
                const userLocation = {
                    lat: position.coords.latitude,
                    lng: position.coords.longitude,
                };
                map.setCenter(userLocation);
                map.setZoom(15);
                marker.setPosition(userLocation);
                updateLatLongFields(userLocation.lat, userLocation.lng);
            },
            (error) => {
                console.warn("Error al obtener la ubicación del usuario:", error.message);
            },
            {
                enableHighAccuracy: true,
                timeout: 10000,
                maximumAge: 0,
            }
        );
    }

    const streetInput = document.getElementById("newClientStreet");
    const streetNumberInput = document.getElementById("newClientStreetNumber");
    const citySelect = document.getElementById("newClientCitySelect");

    if (streetInput && streetNumberInput && citySelect) {
        streetInput.addEventListener("blur", () => {
            if (selectedCityData && streetInput.value.trim()) {
                geocodeFullAddress();
            }
        });

        streetNumberInput.addEventListener("blur", () => {
            if (selectedCityData && streetInput.value.trim()) {
                geocodeFullAddress();
            }
        });

        citySelect.addEventListener("change", () => {
            fillAddressFields();
            const street = streetInput.value.trim();
            if (street) {
                geocodeFullAddress();
            }
        });
    }

    google.maps.event.addListener(marker, "dragend", function () {
        const position = marker.getPosition();
        updateLatLongFields(position.lat(), position.lng());
        reverseGeocode(position.lat(), position.lng());
    });
}

function geocodeFullAddress() {
    const street = document.getElementById("newClientStreet").value.trim();
    const streetNumber = document.getElementById("newClientStreetNumber").value.trim();
    const zipCode = document.getElementById("newClientZipCode").value.trim();
    if (!selectedCityData || !street) {
        document.getElementById("geocodeAddressDisplay").textContent = "Faltan datos: calle o ciudad no seleccionada.";
        return;
    }

    // Construir la dirección completa con código postal
    const address = `${street}${streetNumber ? " " + streetNumber : " S/N"}, ${selectedCityData.AddressCity}, ${selectedCityData.CountyName}, Argentina`;

    const displayElement = document.getElementById("geocodeAddressDisplay");
    displayElement.textContent = `Geolocalizando: ${address}`;

    geocoder.geocode({ address: address }, (results, status) => {
        if (status === "OK" && results[0]) {
            const location = results[0].geometry.location;
            map.setCenter(location);
            map.setZoom(15);
            marker.setPosition(location);
            updateLatLongFields(location.lat(), location.lng());
            displayElement.textContent = `Geolocalización exitosa: ${address} (Lat: ${location.lat().toFixed(6)}, Lng: ${location.lng().toFixed(6)})`;
        } else {
            console.warn("Geocodificación fallida:", status);
            showToast('warning', 'No se pudo geolocalizar la dirección ingresada. Ajuste el marcador manualmente.');
            displayElement.textContent = `Fallo en geolocalización: ${address} (Estado: ${status})`;
        }
    });
}

// Función para obtener la dirección desde coordenadas (geocodificación inversa)
function reverseGeocode(lat, lng) {
    const latlng = { lat: parseFloat(lat), lng: parseFloat(lng) };
    geocoder.geocode({ location: latlng }, (results, status) => {
        if (status === "OK" && results[0]) {
            updateLatLongFields(lat, lng);
            const displayElement = document.getElementById("geocodeAddressDisplay");
            displayElement.textContent = `Ubicación ajustada manualmente: Lat: ${lat.toFixed(6)}, Lng: ${lng.toFixed(6)} (Dirección aproximada: ${results[0].formatted_address})`;
        } else {
            console.warn("Geocodificación inversa fallida:", status);
            showToast('warning', 'No se pudo obtener la dirección a partir de las coordenadas.');
        }
    });
}

// Actualizar los campos de latitud y longitud
function updateLatLongFields(lat, lng) {
    const latField = document.getElementById("newClientLatitude");
    const lngField = document.getElementById("newClientLongitude");
    if (latField && lngField) {
        latField.value = lat.toFixed(10);
        lngField.value = lng.toFixed(10);
    }
}

// Ajustar las funciones existentes para integrar la geolocalización
function fillAddressFields() {
    const citySelect = document.getElementById("newClientCitySelect");
    const selectedCity = citySelect.value;
    selectedCityData = postalCodeData.find(item => item.AddressCity === selectedCity);

    if (selectedCityData) {
        toggleAddressFields(true);
        document.getElementById("newClientStreet").focus();
        const street = document.getElementById("newClientStreet").value.trim();
        if (street) {
            geocodeFullAddress();
        }
    } else {
        toggleAddressFields(false);
        document.getElementById("geocodeAddressDisplay").textContent = "No hay ciudad seleccionada.";
    }
}

let videoStream = null;

// Abrir el modal del escáner
function openScannerModal() {
    const modal = new bootstrap.Modal(document.getElementById("scannerModal"));
    modal.show();
    startScanner();
}

// Iniciar el escáner
function startScanner() {
    const video = document.getElementById("scannerVideo");
    const scannerStatus = document.getElementById("scannerStatus");

    if (navigator.mediaDevices && navigator.mediaDevices.getUserMedia) {
        navigator.mediaDevices.getUserMedia({ video: { facingMode: "environment" } })
            .then(stream => {
                videoStream = stream;
                video.srcObject = stream;
                video.play();
                scannerStatus.textContent = "Escaneando...";
                scanCode(video);
            })
            .catch(err => {
                console.error("Error al acceder a la cámara:", err);
                scannerStatus.textContent = "No se pudo acceder a la cámara. Por favor, permite el acceso.";
            });
    } else {
        scannerStatus.textContent = "El escaneo no es compatible con este dispositivo.";
    }
}

// Detener el escáner
function stopScanner() {
    if (videoStream) {
        videoStream.getTracks().forEach(track => track.stop());
        videoStream = null;
    }
    const video = document.getElementById("scannerVideo");
    video.srcObject = null;
}

// Escanear QR o código de barras
function scanCode(video) {
    const canvas = document.createElement("canvas");
    const context = canvas.getContext("2d");

    function tick() {
        if (video.readyState === video.HAVE_ENOUGH_DATA) {
            canvas.height = video.videoHeight;
            canvas.width = video.videoWidth;
            context.drawImage(video, 0, 0, canvas.width, canvas.height);

            // Intentar escanear QR con jsQR
            const imageData = context.getImageData(0, 0, canvas.width, canvas.height);
            const qrCode = jsQR(imageData.data, imageData.width, imageData.height);
            if (qrCode) {
                handleScanResult(qrCode.data);
                return;
            }

            // Intentar escanear código de barras con QuaggaJS
            Quagga.decodeSingle({
                src: canvas.toDataURL("image/png"),
                numOfWorkers: 0, // Desactiva workers para simplicidad
                decoder: {
                    readers: ["code_128_reader", "ean_reader", "ean_8_reader", "upc_reader"]
                }
            }, result => {
                if (result && result.codeResult) {
                    handleScanResult(result.codeResult.code);
                } else {
                    requestAnimationFrame(tick);
                }
            });
        } else {
            requestAnimationFrame(tick);
        }
    }
    requestAnimationFrame(tick);
}

// Manejar el resultado del escaneo
function handleScanResult(code) {
    stopScanner();
    const modal = bootstrap.Modal.getInstance(document.getElementById("scannerModal"));
    modal.hide();

    // Verificar si el resultado es una URL y extraer el codigo_articulo
    let productCode = code;
    try {
        const urlPattern = /^(https?:\/\/.*\/)?(.*)\.html$/i;
        const match = code.match(urlPattern);
        if (match && match[2]) {
            productCode = match[2]; // Extraer el codigo_articulo antes de .html
        } else {
            showToast("warning", `El código escaneado no parece ser una URL válida, usando como código directo: ${code}`);
        }
    } catch (error) {
        console.error("Error al procesar la URL escaneada:", error);
        showToast("danger", "Error al procesar el código escaneado.");
        return;
    }

    // Obtener la tienda seleccionada
    const selectedStore = document.getElementById("storeFilter").value || lastStore || 'BA001GC';

    // Buscar el producto en la tienda seleccionada y abrir el modal de cantidad
    fetch(`/api/productos/by_code?code=${encodeURIComponent(productCode)}&store=${encodeURIComponent(selectedStore)}`)
        .then(response => response.json())
        .then(data => {
            if (data.error || data.message || !data.length) {
                showToast("warning", data.message || `Error: ${data.error || "Producto no encontrado"}`);
            } else {
                const product = data[0]; // Tomar el primer producto encontrado
                // Abrir el modal de cantidad con los datos del producto
                showQuantityModalFromScan(productCode, product.nombre_producto, product.precio_final_con_descuento);
            }
        })
        .catch(error => {
            console.error("Error al buscar producto:", error);
            showToast("danger", "Error al buscar el producto escaneado.");
        });
}

// Nueva función para abrir el modal de cantidad desde el escaneo
function showQuantityModalFromScan(productId, productName, price) {
    const parsedPrice = convertirMonedaANumero(String(price));
    const product = products.find(p => p.numero_producto === productId) || {
        numero_producto: productId,
        nombre_producto: productName,
        precio_final_con_descuento: price,
        precio_final_con_iva: price, // Valor por defecto
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
        precioLista
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

function openRecoverQuotationModal() {
    const modal = new bootstrap.Modal(document.getElementById('recoverQuotationModal'));
    modal.show();

    const searchInput = document.getElementById('quotationSearchInput');

    // Limpiar input y resultados al abrir
    searchInput.value = '';
    document.getElementById('quotationList').innerHTML = '';

    // Ejecutar búsqueda inicial
    searchQuotations();
}

// Buscar presupuestos según el tipo seleccionado
async function searchQuotations() {
    const query = document.getElementById("quotationSearchInput").value.trim();
    const type = document.getElementById("quotationTypeSelect").value;
    const quotationList = document.getElementById("quotationList");
    quotationList.innerHTML = "";

    // Limpiar manejadores de eventos previos
    quotationList.removeEventListener('click', handleQuotationClick);

    try {
        if (type === "local") {
            const response = await fetch('/api/local_quotations');
            if (!response.ok) {
                console.error('DEBUG: Error al obtener presupuestos locales:', response.status);
                throw new Error('Error al obtener presupuestos locales');
            }
            const quotations = await response.json();

            const filteredQuotations = quotations.filter(q =>
                q.quotation_id.toLowerCase().includes(query.toLowerCase())
            );

            if (!filteredQuotations.length) {
                quotationList.innerHTML = '<p class="text-muted">No se encontraron presupuestos locales.</p>';
                return;
            }

            filteredQuotations.forEach((quotation, index) => {
                const item = document.createElement("a");
                item.href = "#";
                item.className = "list-group-item list-group-item-action";
                item.dataset.quotationId = quotation.quotation_id;
                item.innerHTML = `${quotation.quotation_id} | ${quotation.client_name || 'Sin cliente'} | ${new Date(quotation.timestamp).toLocaleString()}`;
                item.addEventListener('click', (e) => {
                    e.preventDefault();
                    e.stopPropagation();
                    try {
                        loadQuotation(quotation.quotation_id, 'local');
                    } catch (error) {
                        console.error('DEBUG: Error al llamar loadQuotation:', error);
                        showToast('danger', `Error al cargar presupuesto ${quotation.quotation_id}: ${error.message}`);
                    }
                });
                quotationList.appendChild(item);
            });
        } else if (type === "d365") {
            if (!query.startsWith("VENT1-")) {
                quotationList.innerHTML = '<p class="text-muted">Ingresa un ID válido de D365 (VENT1-XXXXX).</p>';
                return;
            }

            showSpinner();
            const store = document.getElementById("storeFilter").value || lastStore || 'BA001GC';
            const response = await fetch(`/api/d365_quotation/${query}?store=${store}`);
            hideSpinner();

            if (!response.ok) {
                const errorData = await response.json();
                console.error('DEBUG: Error al obtener presupuesto D365:', errorData);
                quotationList.innerHTML = `<p class="text-danger">${errorData.error || errorData.message}</p>`;
                return;
            }

            const quotation = await response.json();

            if (!quotation.quotation_id) {
                quotationList.innerHTML = '<p class="text-muted">No se encontraron presupuestos D365.</p>';
                return;
            }

            // Generar un único elemento para el presupuesto
            const item = document.createElement("a");
            item.href = "#";
            item.className = "list-group-item list-group-item-action";
            item.dataset.quotationId = quotation.quotation_id;
            item.innerHTML = `${quotation.quotation_id} | ${quotation.client?.nombre_cliente || 'Sin cliente'} | ${new Date(quotation.timestamp).toLocaleString()}`;
            item.addEventListener('click', (e) => {
                e.preventDefault();
                e.stopPropagation();
                try {
                    loadQuotation(quotation.quotation_id, 'd365');
                } catch (error) {
                    console.error('DEBUG: Error al llamar loadQuotation:', error);
                    showToast('danger', `Error al cargar presupuesto ${quotation.quotation_id}: ${error.message}`);
                }
            });
            quotationList.appendChild(item);
        }

        // Agregar manejador delegado para depuración
        quotationList.addEventListener('click', handleQuotationClick);
    } catch (error) {
        console.error('DEBUG: Error al buscar presupuestos:', error);
        quotationList.innerHTML = '<p class="text-danger">Error al cargar presupuestos.</p>';
    }
}

function handleQuotationClick(e) {
    console.log('DEBUG: Clic detectado en quotationList:', { target: e.target, quotationId: e.target.dataset.quotationId });
}

document.addEventListener("DOMContentLoaded", function () {
    const quotationSearchInput = document.getElementById("quotationSearchInput");
    if (quotationSearchInput) {
        // Eliminar el evento oninput si existe
        quotationSearchInput.oninput = null;

        // Agregar evento keypress para buscar al presionar Enter
        quotationSearchInput.addEventListener("keypress", (e) => {
            if (e.key === "Enter") {
                e.preventDefault();
                const query = quotationSearchInput.value.trim();
                if (query.length >= 3) {
                    searchQuotations();
                } else {
                    const quotationList = document.getElementById("quotationList");
                    quotationList.innerHTML = '<p class="text-muted">Por favor, ingresa al menos 3 caracteres y presiona Enter.</p>';
                }
            }
        });
    }
});

// Cargar un presupuesto en el carrito
async function loadQuotation(quotationId, type) {
    try {
        showSpinner();
        const store = document.getElementById("storeFilter").value || lastStore || 'BA001GC';
        const url = type === 'local' ? `/api/local_quotation/${quotationId}` : `/api/d365_quotation/${quotationId}?store=${store}`;

        const response = await fetch(url);
        if (!response.ok) {
            const errorData = await response.json();
            console.error('DEBUG: Error en la respuesta del servidor:', errorData);
            throw new Error(errorData.error || 'Error al cargar presupuesto');
        }
        const quotation = await response.json();

        // Verificar si el presupuesto D365 está confirmado
        if (type === 'd365' && quotation.header.SalesQuotationStatus === "Confirmed") {
            const createNew = await showConfirmedQuotationModal(quotation.quotation_id, quotation.header.GeneratedSalesOrderNumber);
            if (!createNew) {
                showToast('info', 'Carga del presupuesto cancelada.');
                return; // Salir si el usuario cancela
            }
            // Si el usuario elige crear un nuevo presupuesto, cargar las líneas como un nuevo presupuesto
            quotation.quotation_id = null;
            quotation.type = 'new';
        }

        // Verificar si el presupuesto D365 contenía ítems de flete o servicio
        if (type === 'd365' && quotation.has_flete) {
            const continueLoading = await showFleteWarningModal();
            if (!continueLoading) {
                showToast('info', 'Carga del presupuesto cancelada.');
                return; // Salir si el usuario cancela
            }
        }

        // Armar estructura de cart con datos crudos del backend
        const rawCartStructure = {
            items: quotation.items || [],
            client: quotation.client || null,
            quotation_id: quotation.quotation_id || null,
            type: quotation.quotation_id ? type : 'new',
            observations: quotation.observations || ''
        };
        // Mapear ítems directamente
        const itemsWithNumericPrices = (quotation.items || []).map(item => ({
            productId: item.productId || '',
            productName: item.productName || 'Producto desconocido',
            price: convertirMonedaANumero(item.price || 0),
            precioLista: convertirMonedaANumero(item.precioLista || item.price || 0),
            quantity: Number(item.quantity) || 1,
            multiplo: Number(item.multiplo) || 1,
            unidadMedida: item.unidadMedida || 'Un',
            available: true
        })).filter(item => item.productId);

        // Actualizar el carrito
        cart = {
            items: itemsWithNumericPrices,
            client: quotation.client || null,
            quotation_id: quotation.quotation_id || null,
            type: quotation.quotation_id ? type : 'new',
            observations: quotation.observations || ''
        };
        cartObservations = cart.observations;
        const observationsInput = document.getElementById("cartObservations");
        if (observationsInput) {
            observationsInput.value = cartObservations;
        }

        // Actualizar el filtro de tienda
        document.getElementById("storeFilter").value = quotation.store_id || "BA001GC";
        await updateCartPrices(document.getElementById("storeFilter").value);

        // Renderizar directamente
        updateCartDisplay();
        showToast('success', `Presupuesto ${quotationId} cargado en el carrito${!quotation.quotation_id ? ' como nuevo' : ''}.`);
        const modal = bootstrap.Modal.getInstance(document.getElementById("recoverQuotationModal"));
        modal.hide();
    } catch (error) {
        console.error("Error al cargar presupuesto:", error);
        showToast('danger', `Error: ${error.message}`);
    } finally {
        hideSpinner();
    }
}

function showConfirmedQuotationModal(quotationId, orderNumber) {
    return new Promise((resolve) => {
        const modalElement = document.getElementById('confirmedQuotationModal');
        if (!modalElement) {
            console.error('Error: No se encontró el elemento con ID "confirmedQuotationModal" en el DOM.');
            showToast('danger', 'No se pudo abrir el modal de advertencia de presupuesto confirmado.');
            resolve(false); // Continuar sin mostrar el modal
            return;
        }

        // Llenar los datos del presupuesto y pedido en el modal
        const quotationIdSpan = modalElement.querySelector('#confirmedQuotationId');
        const orderNumberSpan = modalElement.querySelector('#confirmedOrderNumber');
        quotationIdSpan.textContent = quotationId || 'N/A';
        orderNumberSpan.textContent = orderNumber || 'N/A';

        const modal = new bootstrap.Modal(modalElement);
        modal.show();

        const createNewButton = modalElement.querySelector('#createNewQuotation');
        const cancelButton = modalElement.querySelector('#cancelNewQuotation');

        const onCreateNew = () => {
            resolve(true);
            modal.hide();
            createNewButton.removeEventListener('click', onCreateNew);
        };

        const onCancel = () => {
            resolve(false);
            modal.hide();
            createNewButton.removeEventListener('click', onCreateNew);
        };

        createNewButton.addEventListener('click', onCreateNew);
        cancelButton.addEventListener('click', onCancel);

        modalElement.addEventListener('hidden.bs.modal', () => {
            resolve(false); // Por defecto, cancelar si se cierra el modal
            createNewButton.removeEventListener('click', onCreateNew);
            cancelButton.removeEventListener('click', onCancel);
        }, { once: true });
    });
}

function showFleteWarningModal() {
    return new Promise((resolve) => {
        const modalElement = document.getElementById('fleteWarningModal');
        if (!modalElement) {
            console.error('Error: No se encontró el elemento con ID "fleteWarningModal" en el DOM.');
            showToast('danger', 'No se pudo abrir el modal de advertencia de flete.');
            resolve(false); // Continuar sin mostrar el modal
            return;
        }

        const modal = new bootstrap.Modal(modalElement);
        modal.show();

        const continueButton = modalElement.querySelector('#confirmFleteRemoval');
        const cancelButton = modalElement.querySelector('[data-bs-dismiss="modal"]');

        const onContinue = () => {
            resolve(true);
            modal.hide();
            continueButton.removeEventListener('click', onContinue);
        };

        const onCancel = () => {
            resolve(false);
            modal.hide();
            continueButton.removeEventListener('click', onContinue);
        };

        continueButton.addEventListener('click', onContinue);
        cancelButton.addEventListener('click', onCancel);

        modalElement.addEventListener('hidden.bs.modal', () => {
            resolve(false); // Por defecto, cancelar si se cierra el modal
            continueButton.removeEventListener('click', onContinue);
            cancelButton.removeEventListener('click', onCancel);
        }, { once: true });
    });
}

function togglePersonalFields(enable) {
    const fields = ["newClientName", "newClientLastName", "newClientEmail", "newClientPhone", "newClientZipCode"];
    fields.forEach(id => {
        const element = document.getElementById(id);
        if (element) element.disabled = !enable;
    });
}

function toggleAddressFields(enable) {
    const fields = ["newClientCitySelect", "newClientStreet", "newClientStreetNumber", "newClientReference"];
    fields.forEach(id => {
        const element = document.getElementById(id);
        if (element) element.disabled = !enable;
    });
    document.getElementById("saveClientBtn").disabled = !enable;
}

// Validación del DNI
async function validateDni() {
    const dni = document.getElementById("newClientDni").value.trim();
    const messageElement = document.getElementById("dniValidationMessage");

    // Validación de longitud mínima
    if (!dni || dni.length < 6) {
        messageElement.textContent = "El DNI debe tener al menos 6 dígitos.";
        togglePersonalFields(false);
        toggleAddressFields(false);
        return;
    }

    // Mostrar spinner durante la validación
    showSpinner();
    try {
        const response = await fetch("/api/clientes/validate", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ dni }),
        });
        const result = await response.json();

        if (!response.ok) throw new Error(result.error || "Error al validar DNI");

        if (result.exists) {
            messageElement.textContent = "El cliente ya existe con este DNI.";
            togglePersonalFields(false);
            toggleAddressFields(false);
        } else {
            messageElement.textContent = "";
            togglePersonalFields(true); // Habilitar campos personales y código postal
            toggleAddressFields(false); // Mantener dirección bloqueada hasta seleccionar ciudad
            document.getElementById("newClientName").focus(); // Poner foco en nombre
        }
    } catch (error) {
        messageElement.textContent = "Error al validar DNI: " + error.message;
        togglePersonalFields(false);
        toggleAddressFields(false);
    } finally {
        hideSpinner();
    }
}

async function loadPostalCodeData() {
    const zipCode = document.getElementById("newClientZipCode").value.trim();
    const citySelect = document.getElementById("newClientCitySelect");

    if (!zipCode) {
        citySelect.innerHTML = '<option value="">Seleccione una ciudad</option>';
        toggleAddressFields(false);
        return;
    }

    // Mostrar spinner durante la consulta
    showSpinner();
    try {
        const response = await fetch("/api/direcciones/codigo_postal", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ codigo_postal: zipCode }),
        });
        const data = await response.json();

        if (!response.ok) throw new Error(data.error || "Error al consultar código postal");

        postalCodeData = data;
        citySelect.innerHTML = '<option value="">Seleccione una ciudad</option>';
        if (data.length === 0) {
            citySelect.innerHTML += '<option value="">No se encontraron ciudades</option>';
            toggleAddressFields(false);
        } else {
            data.forEach(item => {
                const option = document.createElement("option");
                option.value = item.AddressCity;
                option.textContent = `${item.AddressCity} (${item.CountyName})`;
                citySelect.appendChild(option);
            });
            citySelect.disabled = false; // Habilitar el selector de ciudades
        }
    } catch (error) {
        console.error("Error:", error);
        citySelect.innerHTML = '<option value="">Error al cargar ciudades</option>';
        toggleAddressFields(false);
    } finally {
        hideSpinner();
    }
}

function fillAddressFields() {
    const citySelect = document.getElementById("newClientCitySelect");
    const selectedCity = citySelect.value;
    selectedCityData = postalCodeData.find(item => item.AddressCity === selectedCity);

    if (selectedCityData) {
        toggleAddressFields(true); // Habilitar campos de dirección
        document.getElementById("newClientStreet").focus();
        // Geolocalizar automáticamente si ya hay calle y altura
        const street = document.getElementById("newClientStreet").value.trim();
        const streetNumber = document.getElementById("newClientStreetNumber").value.trim();
        if (street && streetNumber) {
            geocodeFullAddress();
        }
    } else {
        toggleAddressFields(false); // Mantener bloqueados si no se selecciona ciudad
    }
}

async function saveNewClient() {
    const datosCliente = {
        dni: document.getElementById("newClientDni").value.trim(),
        nombre: document.getElementById("newClientName").value.trim(),
        apellido: document.getElementById("newClientLastName").value.trim(),
        email: document.getElementById("newClientEmail").value.trim(),
        telefono: document.getElementById("newClientPhone").value.trim(),
        codigo_postal: document.getElementById("newClientZipCode").value.trim(),
        ciudad: selectedCityData?.AddressCity || "",
        estado: selectedCityData?.AddressState || "",
        condado: selectedCityData?.AddressCounty || "",
        calle: document.getElementById("newClientStreet").value.trim(),
        altura: document.getElementById("newClientStreetNumber").value.trim(),
        referencia: document.getElementById("newClientReference").value.trim(),
        latitud: parseFloat(document.getElementById("newClientLatitude").value) || 0,
        longitud: parseFloat(document.getElementById("newClientLongitude").value) || 0
    };

    // Validar campos requeridos
    const requiredFields = ["dni", "nombre", "apellido", "email", "telefono", "codigo_postal", "ciudad", "calle", "altura"];
    for (const field of requiredFields) {
        if (!datosCliente[field]) {
            showToast("danger", `El campo ${field} es obligatorio.`);
            return;
        }
    }

    try {
        showSpinner();
        const response = await fetch("/api/clientes/create", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(datosCliente),
        });
        const result = await response.json();

        if (!response.ok) throw new Error(result.error || "Error al crear cliente");

        showToast("success", "Cliente creado exitosamente: " + result.customer_id);
        const modal = bootstrap.Modal.getInstance(document.getElementById("addClientModal"));
        modal.hide();
        addClientToCartFromData(datosCliente, result.customer_id);

        // Limpiar los campos del modal
        document.getElementById("newClientDni").value = "";
        document.getElementById("newClientName").value = "";
        document.getElementById("newClientLastName").value = "";
        document.getElementById("newClientEmail").value = "";
        document.getElementById("newClientPhone").value = "";
        document.getElementById("newClientZipCode").value = "";
        document.getElementById("newClientCitySelect").innerHTML = '<option value="">Seleccione una ciudad</option>';
        document.getElementById("newClientStreet").value = "";
        document.getElementById("newClientStreetNumber").value = "";
        document.getElementById("newClientReference").value = "";
        document.getElementById("newClientLatitude").value = "";
        document.getElementById("newClientLongitude").value = "";
        document.getElementById("dniValidationMessage").textContent = "";
        document.getElementById("geocodeAddressDisplay").textContent = "";
        togglePersonalFields(false);
        toggleAddressFields(false);
        selectedCityData = null; // Resetear la selección de ciudad
        postalCodeData = []; // Limpiar datos de códigos postales
        // Reiniciar el mapa a una posición por defecto
        if (map && marker) {
            map.setCenter({ lat: -34.6037, lng: -58.3816 });
            map.setZoom(8);
            marker.setPosition({ lat: -34.6037, lng: -58.3816 });
        }

    } catch (error) {
        console.error("Error al crear cliente:", error);
        showToast("danger", `Error al crear cliente: ${error.message}`);
    } finally {
        hideSpinner();
        const modal = bootstrap.Modal.getInstance(document.getElementById("clientSearchModal"));
        modal.hide();
    }
}

function addClientToCartFromData(cliente, customer_id) {
    // Construir el objeto cart.client con todos los campos necesarios para el modal de detalles
    cart.client = {
        numero_cliente: customer_id, // Usar el ID generado por D365
        nombre_cliente: `${cliente.nombre} ${cliente.apellido}`,
        nif: cliente.dni,
        direccion_completa: `${cliente.calle} ${cliente.altura}, ${cliente.ciudad}`,
        email_contacto: cliente.email,
        telefono_contacto: cliente.telefono,
        bloqueado: "No",
        tipo_contribuyente: "CF",
        limite_credito: 5,
        grupo_impuestos: "C-CF",
        tif: "DNI",
        fecha_creacion: new Date().toISOString(),
        fecha_modificacion: new Date().toISOString()
    };
    updateCartDisplay();
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

// Guardar el carrito en IndexedDB
function saveCartToIndexedDB(userId, cartData, timestamp) {
    if (!db) {
        console.warn('IndexedDB no está inicializado');
        return Promise.resolve();
    }

    return new Promise((resolve, reject) => {
        const transaction = db.transaction([CART_STORE], 'readwrite');
        const store = transaction.objectStore(CART_STORE);
        const cartEntry = {
            userId: userId,
            cart: cartData,
            timestamp: timestamp
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

// Restaurar el carrito desde IndexedDB
function loadCartFromIndexedDB(userId) {
    if (!db) {
        console.warn('IndexedDB no está inicializado');
        return Promise.resolve(null);
    }

    return new Promise((resolve, reject) => {
        const transaction = db.transaction([CART_STORE], 'readonly');
        const store = transaction.objectStore(CART_STORE);
        const request = store.get(userId);

        request.onsuccess = (event) => {
            const result = event.target.result;
            if (result && result.cart) {
                resolve({ cart: result.cart, timestamp: result.timestamp });
            } else {
                resolve(null);
            }
        };

        request.onerror = (event) => {
            console.error('Error al cargar carrito desde IndexedDB:', event.target.error);
            reject(event.target.error);
        };
    });
}

// Obtener el ID del usuario autenticado (email)
async function getUserId() {
    try {
        const response = await fetch('/api/user_info', {
            credentials: 'include' // Incluir cookies de sesión
        });
        if (!response.ok) {
            throw new Error(`Error al obtener información del usuario: ${response.status}`);
        }
        const data = await response.json();
        if (!data.email) {
            console.warn('No se encontró email en la respuesta de /api/user_info:', data);
            return null;
        }
        sessionStorage.setItem('email', data.email);
        return data.email;
    } catch (error) {
        console.error('No se pudo obtener el ID del usuario:', error);
        const storedEmail = sessionStorage.getItem('email');
        if (storedEmail && storedEmail !== 'anonymous') {
            return storedEmail; // Usar email almacenado si existe y no es 'anonymous'
        }
        return null; // No devolver 'anonymous' para forzar autenticación
    }
}

// Sincronizar con el backend
async function syncCartWithBackend(userId, cartData, timestamp) {
    try {
        // Validar y corregir estructura del carrito antes de enviar
        if (!cartData || typeof cartData !== 'object') {
            console.error('Carrito inválido para sincronización:', cartData);
            throw new Error('Carrito inválido: no es un objeto');
        }

        // Asegurar que 'items' exista y sea un array
        cartData.items = Array.isArray(cartData.items) ? cartData.items : [];

        const response = await fetch('/api/save_user_cart', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ userId, cart: cartData, timestamp })
        });

        if (!response.ok) {
            const errorData = await response.json();
            console.error('Error en la respuesta del servidor:', errorData);
            throw new Error(errorData.error || 'Error al sincronizar carrito con el servidor');
        }

    } catch (error) {
        console.error('Error al sincronizar carrito:', error.message);
        showToast('danger', `Error al sincronizar el carrito con el servidor: ${error.message}`);
        throw error;
    }
}

// Obtener carrito del backend
async function loadCartFromBackend(userId) {
    try {
        const response = await fetch('/api/get_user_cart');
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.error || 'Error al obtener carrito del servidor');
        }
        const data = await response.json();
        return { cart: data.cart, timestamp: data.timestamp };
    } catch (error) {
        console.error('Error al obtener carrito del backend:', error);
        return null;
    }
}

// Comparar timestamps para resolver conflictos
function isTimestampNewer(timestampA, timestampB) {
    if (!timestampA) return false;
    if (!timestampB) return true;
    return new Date(timestampA) > new Date(timestampB);
}

// Modal para resolver conflictos
function showConflictModal(localCart, localTimestamp, backendCart, backendTimestamp) {
    return new Promise((resolve) => {
        const modal = document.createElement('div');
        modal.className = 'modal fade';
        modal.innerHTML = `
            <div class="modal-dialog modal-dialog-centered">
                <div class="modal-content">
                    <div class="modal-header bg-warning">
                        <h5 class="modal-title">Conflicto de carrito detectado</h5>
                        <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                    </div>
                    <div class="modal-body">
                        <p>Se encontraron dos versiones del carrito:</p>
                        <p><strong>Local</strong> (Última actualización: ${new Date(localTimestamp).toLocaleString('es-AR')}): ${localCart.items.length} ítems, ${localCart.client ? 'con cliente' : 'sin cliente'}, ${localCart.quotation_id || 'sin presupuesto'}</p>
                        <p><strong>Servidor</strong> (Última actualización: ${new Date(backendTimestamp).toLocaleString('es-AR')}): ${backendCart.items.length} ítems, ${backendCart.client ? 'con cliente' : 'sin cliente'}, ${backendCart.quotation_id || 'sin presupuesto'}</p>
                        <p>¿Cuál deseas usar?</p>
                        <button class="btn btn-primary" id="chooseLocal">Usar carrito local</button>
                        <button class="btn btn-secondary" id="chooseBackend">Usar carrito del servidor</button>
                    </div>
                </div>
            </div>
        `;
        document.body.appendChild(modal);
        const bsModal = new bootstrap.Modal(modal);
        bsModal.show();

        document.getElementById('chooseLocal').onclick = () => {
            resolve({ cart: localCart, timestamp: localTimestamp });
            bsModal.hide();
            modal.remove();
        };
        document.getElementById('chooseBackend').onclick = () => {
            resolve({ cart: backendCart, timestamp: backendTimestamp });
            bsModal.hide();
            modal.remove();
        };
        modal.addEventListener('hidden.bs.modal', () => {
            resolve({ cart: backendCart, timestamp: backendTimestamp }); // Por defecto, usar backend
            modal.remove();
        }, { once: true });
    });
}

// Inicializar IndexedDB y restaurar carrito
document.addEventListener('DOMContentLoaded', async () => {
    try {
        await initIndexedDB();
        const userId = await getUserId();

        if (!userId) {
            console.warn('Usuario no autenticado, inicializando carrito vacío');
            showToast('warning', 'Por favor, inicia sesión para sincronizar el carrito');
            // Intentar cargar desde IndexedDB como respaldo
            const localData = await loadCartFromIndexedDB('anonymous');
            if (localData && localData.cart) {
                cart = localData.cart;
                cartObservations = cart.observations || '';
                document.getElementById("cartObservations").value = cartObservations;
                updateCartDisplay();
            }
            return;
        }

        // Priorizar siempre la versión del servidor
        const backendData = await loadCartFromBackend(userId);
        if (backendData && backendData.cart) {
            cart = backendData.cart;
            await saveCartToIndexedDB(userId, cart, backendData.timestamp);
        } else {
            // Si el servidor no tiene datos, intentar cargar desde IndexedDB
            const localData = await loadCartFromIndexedDB(userId);
            if (localData && localData.cart) {
                cart = localData.cart;
                const timestamp = new Date().toISOString();
                await syncCartWithBackend(userId, cart, timestamp);
            } else {
                // Si no hay datos locales, usar carrito vacío
                cart = { items: [], client: null, quotation_id: null, type: 'new', observations: '' };
                const timestamp = new Date().toISOString();
                await saveCartToIndexedDB(userId, cart, timestamp);
                await syncCartWithBackend(userId, cart, timestamp);
            }
        }

        cartObservations = cart.observations || '';
        document.getElementById("cartObservations").value = cartObservations;
        updateCartDisplay();
    } catch (error) {
        console.error('Error al inicializar el carrito:', error);
        showToast('danger', 'Error al cargar el carrito');
        // Usar carrito vacío como última opción
        cart = { items: [], client: null, quotation_id: null, type: 'new', observations: '' };
        cartObservations = '';
        document.getElementById("cartObservations").value = cartObservations;
        updateCartDisplay();
    }
});

/***************************************
 * Archivo: scripts.js
 * Descripción: Gestión de filtros, paginación,
 * visualización de imágenes, carga dinámica de productos
 * y funcionalidad del botón flotante de WhatsApp.
 ***************************************/
document.addEventListener('DOMContentLoaded', () => {
  console.log('DOM fully loaded, initializing components');

  // WhatsApp Button Dragging and Functionality
  const whatsappBtn = document.getElementById('whatsappBtn');
  if (whatsappBtn) {
    console.log('WhatsApp button found, setting up drag and click events');
    let isDragging = false;
    let currentX = 0;
    let currentY = 0;
    let initialX = 0;
    let initialY = 0;
    let touchStartTime = 0;
    let mouseStartTime = 0;
    const TOUCH_THRESHOLD = 200; // Tiempo en ms para distinguir clic de arrastre
    const MOVE_THRESHOLD = 5; // Umbral de movimiento en píxeles para considerar arrastre
    const EDGE_MARGIN = 5; // Margen en píxeles desde los bordes del viewport

    // Load saved position from localStorage
    const savedPosition = localStorage.getItem('whatsappBtnPosition');
    if (savedPosition) {
      const { top, left } = JSON.parse(savedPosition);
      whatsappBtn.style.top = `${top}px`;
      whatsappBtn.style.left = `${left}px`;
      whatsappBtn.style.bottom = 'auto';
      whatsappBtn.style.right = 'auto';
      currentX = left;
      currentY = top;
      console.log('Restored WhatsApp button position:', { top, left });
    } else {
      console.log('No saved position found, using default position');
      currentX = EDGE_MARGIN; // Default left: 5px
      currentY = window.innerHeight - 60 - EDGE_MARGIN; // Default bottom: 20px, height: 60px
      whatsappBtn.style.left = `${currentX}px`;
      whatsappBtn.style.top = `${currentY}px`;
    }

    // Mouse events for dragging
    whatsappBtn.addEventListener('mousedown', (e) => {
      e.preventDefault();
      e.stopPropagation();
      initialX = e.clientX - currentX;
      initialY = e.clientY - currentY;
      mouseStartTime = Date.now();
      isDragging = false;
      console.log('Mouse down on WhatsApp button', { initialX, initialY, currentX, currentY });
      document.addEventListener('mousemove', onMouseDragging);
      document.addEventListener('mouseup', onMouseUp);
    });

    function onMouseDragging(e) {
      e.preventDefault();
      const deltaX = Math.abs(e.clientX - (initialX + currentX));
      const deltaY = Math.abs(e.clientY - (initialY + currentY));
      if (deltaX > MOVE_THRESHOLD || deltaY > MOVE_THRESHOLD) {
        isDragging = true;
        whatsappBtn.classList.add('dragging');
        requestAnimationFrame(() => {
          currentX = e.clientX - initialX;
          currentY = e.clientY - initialY;

          // Constrain within viewport with margin
          const rect = whatsappBtn.getBoundingClientRect();
          const maxX = window.innerWidth - rect.width - EDGE_MARGIN;
          const maxY = window.innerHeight - rect.height - EDGE_MARGIN;
          currentX = Math.max(EDGE_MARGIN, Math.min(currentX, maxX));
          currentY = Math.max(EDGE_MARGIN, Math.min(currentY, maxY));

          whatsappBtn.style.left = `${currentX}px`;
          whatsappBtn.style.top = `${currentY}px`;
          whatsappBtn.style.right = 'auto';
          whatsappBtn.style.bottom = 'auto';
          console.log('Dragging WhatsApp button (mouse)', { currentX, currentY });
        });
      }
    }

    function onMouseUp(e) {
      e.preventDefault();
      e.stopPropagation();
      document.removeEventListener('mousemove', onMouseDragging);
      document.removeEventListener('mouseup', onMouseUp);
      const duration = Date.now() - mouseStartTime;
      if (isDragging) {
        whatsappBtn.classList.remove('dragging');
        localStorage.setItem('whatsappBtnPosition', JSON.stringify({
          top: parseFloat(whatsappBtn.style.top || 0),
          left: parseFloat(whatsappBtn.style.left || 0)
        }));
        console.log('Stopped dragging, saved position:', {
          top: whatsappBtn.style.top,
          left: whatsappBtn.style.left
        });
      } else if (duration < TOUCH_THRESHOLD) {
        console.log('Mouse up detected as click, triggering enviarWhatsApp');
        enviarWhatsApp();
      }
      isDragging = false;
    }

    // Touch events for mobile
    whatsappBtn.addEventListener('touchstart', (e) => {
      e.preventDefault();
      e.stopPropagation();
      const touch = e.touches[0];
      initialX = touch.clientX - currentX;
      initialY = touch.clientY - currentY;
      touchStartTime = Date.now();
      isDragging = false;
      console.log('Touch start on WhatsApp button', { initialX, initialY, currentX, currentY });
      document.addEventListener('touchmove', onTouchDragging);
      document.addEventListener('touchend', onTouchEnd);
    });

    function onTouchDragging(e) {
      e.preventDefault();
      const touch = e.touches[0];
      const deltaX = Math.abs(touch.clientX - (initialX + currentX));
      const deltaY = Math.abs(touch.clientY - (initialY + currentY));
      if (deltaX > MOVE_THRESHOLD || deltaY > MOVE_THRESHOLD) {
        isDragging = true;
        whatsappBtn.classList.add('dragging');
        requestAnimationFrame(() => {
          currentX = touch.clientX - initialX;
          currentY = touch.clientY - initialY;

          // Constrain within viewport with margin
          const rect = whatsappBtn.getBoundingClientRect();
          const maxX = window.innerWidth - rect.width - EDGE_MARGIN;
          const maxY = window.innerHeight - rect.height - EDGE_MARGIN;
          currentX = Math.max(EDGE_MARGIN, Math.min(currentX, maxX));
          currentY = Math.max(EDGE_MARGIN, Math.min(currentY, maxY));

          whatsappBtn.style.left = `${currentX}px`;
          whatsappBtn.style.top = `${currentY}px`;
          whatsappBtn.style.right = 'auto';
          whatsappBtn.style.bottom = 'auto';
          console.log('Dragging WhatsApp button (touch)', { currentX, currentY });
        });
      }
    }

    function onTouchEnd(e) {
      e.preventDefault();
      e.stopPropagation();
      document.removeEventListener('touchmove', onTouchDragging);
      document.removeEventListener('touchend', onTouchEnd);
      const touchDuration = Date.now() - touchStartTime;
      if (isDragging) {
        whatsappBtn.classList.remove('dragging');
        localStorage.setItem('whatsappBtnPosition', JSON.stringify({
          top: parseFloat(whatsappBtn.style.top || 0),
          left: parseFloat(whatsappBtn.style.left || 0)
        }));
        console.log('Stopped touch dragging, saved position:', {
          top: whatsappBtn.style.top,
          left: whatsappBtn.style.left
        });
      } else if (touchDuration < TOUCH_THRESHOLD) {
        console.log('Touch end detected as tap, triggering enviarWhatsApp');
        enviarWhatsApp();
      }
      isDragging = false;
    }

    // WhatsApp Message Functionality
    function enviarWhatsApp() {
      console.log('enviarWhatsApp called');
      const modalElement = document.getElementById('whatsappModal');
      if (!modalElement) {
        console.error('Error: No se encontró el elemento whatsappModal');
        showToast('danger', 'No se pudo abrir el formulario de WhatsApp.');
        return;
      }

      // Limpiar campos y errores
      const numberInput = document.getElementById('whatsappNumber');
      const messageInput = document.getElementById('whatsappMessage');
      const errorElement = document.getElementById('whatsappNumberError');
      numberInput.value = '';
      messageInput.value = '';
      errorElement.style.display = 'none';

      // Inicializar modal de Bootstrap
      const modal = new bootstrap.Modal(modalElement);
      modal.show();

      // Enfocar el campo de número
      numberInput.focus();

      // Manejar envío
      const submitButton = document.getElementById('whatsappSubmit');
      const handleSubmit = () => {
        const numero = numberInput.value.trim();
        const mensaje = messageInput.value.trim();

        // Validar número
        if (!numero || !/^\d{6,}$/.test(numero)) {
          errorElement.textContent = 'Número inválido. Debe tener al menos 6 dígitos y contener solo números.';
          errorElement.style.display = 'block';
          numberInput.focus();
          return;
        }

        // Generar enlace de WhatsApp
        const link = `https://api.whatsapp.com/send/?phone=549${numero}&text=${encodeURIComponent(mensaje)}&type=phone_number&app_absent=0`;
        console.log('Opening WhatsApp link:', link);
        window.open(link, '_blank');

        // Cerrar modal
        modal.hide();
        submitButton.removeEventListener('click', handleSubmit);
      };

      // Agregar listener para el botón de envío
      submitButton.addEventListener('click', handleSubmit);

      // Permitir envío con Enter en el campo de número
      numberInput.addEventListener('keypress', (e) => {
        if (e.key === 'Enter') {
          e.preventDefault();
          handleSubmit();
        }
      });

      // Limpiar listener al cerrar el modal
      modalElement.addEventListener('hidden.bs.modal', () => {
        submitButton.removeEventListener('click', handleSubmit);
      }, { once: true });
    }

    // Verificar si Font Awesome está cargado
    if (!document.querySelector('i.fa-brands.fa-whatsapp')) {
      console.warn('Font Awesome WhatsApp icon not found, relying on fallback text');
    }
  } else {
    console.error('WhatsApp button not found in DOM');
  }
});

// Función para generar PDF de factura, basada en generatePDF pero adaptada
async function facturar() {
    if (!cart.items.length) {
        showToast('danger', 'El carrito está vacío.');
        return;
    }
    if (!cart.client || !cart.client.numero_cliente) {
        showToast('danger', 'Debe seleccionar un cliente antes de facturar.');
        return;
    }

    showSpinner();
    try {
        const storeId = document.getElementById("storeFilter").value || "BA001GC";
        const items = cart.items.filter(item => item.productId);

        // Calcular importes con redondeo a 2 decimales
        const neto = parseFloat(
            items.reduce((sum, item) => sum + (convertirMonedaANumero(item.price) * item.quantity), 0).toFixed(2)
        );
        const iva = parseFloat((neto * 0.21).toFixed(2)); // IVA al 21% sobre el neto total
        const total = parseFloat((neto + iva).toFixed(2));

        // Validar límites de AFIP (13 enteros + 2 decimales)
        const maxValue = 9999999999999.99; // 13 enteros + 2 decimales
        if (neto > maxValue || iva > maxValue || total > maxValue) {
            showToast('danger', 'El importe excede el límite permitido por AFIP (13 dígitos enteros + 2 decimales).');
            return;
        }

        const payload = {
            cart: {
                items: items,
                client: cart.client,
                observations: cartObservations
            },
            store_id: storeId,
            tipo_cbte: 6, // Factura B
            punto_venta: 587, // Punto de venta
            imp_neto: neto,
            imp_iva: iva,
            imp_total: total,
            concepto: 1,
            doc_nro: cart.client.nif || cart.client.numero_cliente || '0',
            fecha_cbte: new Date().toISOString().slice(0, 10).replace(/-/g, '') // Formato AAAAMMDD
        };

        console.debug("Enviando solicitud de facturación:", payload);

        const response = await fetchWithAuth('/modulo_facturacion_arca/facturar', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });

        // Verificar si la respuesta es un objeto Response válido
        if (!response || typeof response !== 'object' || typeof response.ok === 'undefined') {
            throw new Error('Respuesta inválida del servidor');
        }

        // Verificar estado de la respuesta
        if (!response.ok) {
            const errorText = await (response.text ? response.text() : Promise.resolve('Sin detalles de error'));
            console.error("Error en la respuesta del servidor:", errorText);
            throw new Error(`Error en la solicitud: ${response.status} - ${errorText}`);
        }

        const result = await response.json();
        if (result.autorizacion) {
            showToast('success', result.message);
            // Formatear el ID de factura: 4 dígitos para punto_vta + guión + 8 dígitos para nro_cbte
            const facturaId = `${String(payload.punto_venta).padStart(4, '0')}-${String(result.nro_cbte).padStart(8, '0')}`;
            await generateFacturaPDF(result.autorizacion, result.vencimiento, facturaId);
            // Limpiar carrito después de facturar
            cartObservations = "";
            document.getElementById("cartObservations").value = "";
            delete cart.quotation_id;
            delete cart.type;
            clearCart();
            const timestamp = new Date().toISOString();
            const userId = await getUserId();
            if (userId) {
                await Promise.all([
                    saveCartToIndexedDB(userId, cart, timestamp),
                    syncCartWithBackend(userId, cart, timestamp)
                ]);
            } else {
                await saveCartToIndexedDB('anonymous', cart, timestamp);
            }
            updateCartDisplay();
        } else {
            showToast('danger', result.error || 'Error al emitir la factura');
        }
    } catch (error) {
        console.error('Error al facturar:', error);
        const errorMessage = error.message || error.toString();
        showToast('danger', `Error al emitir la factura: ${errorMessage}`);
    } finally {
        hideSpinner();
    }
}

function generateFacturaPDF(cae, caefchvto, facturaId) {
    return new Promise((resolve, reject) => {
        if (typeof window.jspdf === 'undefined' || !window.jspdf.jsPDF) {
            console.error("jsPDF no está definido. Asegúrate de que la biblioteca esté cargada.");
            showToast('danger', 'Error: No se pudo generar el PDF. Falta la biblioteca jsPDF.');
            reject(new Error("jsPDF no está definido"));
            return;
        }

        const { jsPDF } = window.jspdf;
        const doc = new jsPDF({
            orientation: "portrait",
            unit: "mm",
            format: "a4"
        });

        doc.setFont("helvetica", "normal");

        // Constantes para el manejo del espacio
        const PAGE_HEIGHT = doc.internal.pageSize.getHeight();
        const FOOTER_HEIGHT = 20;
        const BOTTOM_MARGIN = FOOTER_HEIGHT + 10;
        const CONTENT_BOTTOM_LIMIT = PAGE_HEIGHT - BOTTOM_MARGIN;

        const currentDate = new Date().toLocaleString("es-AR", {
            day: "2-digit",
            month: "2-digit",
            year: "numeric",
            hour: "2-digit",
            minute: "2-digit",
            hour12: false
        });

        const storeId = document.getElementById("storeFilter").value || "BA001GC";
        let direccionSucursal = "Dirección no disponible";
        let vendedor = "Vendedor no disponible";

        const cartItems = JSON.parse(JSON.stringify(cart.items.filter(item => item.productId)));
        const cartClient = cart.client ? JSON.parse(JSON.stringify(cart.client)) : null;
        const cartCopy = { items: cartItems, client: cartClient };

        const addLogo = () => {
            return new Promise((resolve) => {
                const img = new Image();
                img.crossOrigin = "Anonymous";
                img.onload = () => {
                    const pageWidth = doc.internal.pageSize.getWidth();
                    const logoWidth = 50;
                    const logoHeight = 12;
                    const xPosition = pageWidth - logoWidth - 5;
                    doc.addImage(img, "PNG", xPosition, 20, logoWidth, logoHeight);
                    resolve();
                };
                img.onerror = () => {
                    console.warn("No se pudo cargar el logo.");
                    resolve();
                };
                img.src = "/static/img/logo_0.png";
            });
        };

        const addFooter = () => {
            return new Promise((resolve) => {
                const img = new Image();
                img.crossOrigin = "Anonymous";
                img.onload = () => {
                    const pageWidth = doc.internal.pageSize.getWidth();
                    const footerWidth = pageWidth - 20;
                    const footerHeight = (img.height * footerWidth) / img.width;
                    const footerYPosition = PAGE_HEIGHT - footerHeight - 10;
                    const pageCount = doc.internal.getNumberOfPages();
                    for (let i = 1; i <= pageCount; i++) {
                        doc.setPage(i);
                        doc.addImage(img, "PNG", 10, footerYPosition, footerWidth, footerHeight);
                    }
                    resolve(footerHeight);
                };
                img.onerror = () => {
                    console.warn("No se pudo cargar el pie de página.");
                    resolve(FOOTER_HEIGHT);
                };
                img.src = "/static/img/pie.png";
            });
        };

        const checkPageBreak = (currentY, spaceNeeded, footerHeight) => {
            if (currentY + spaceNeeded > (PAGE_HEIGHT - footerHeight - 10)) {
                doc.addPage();
                return 10;
            }
            return currentY;
        };

        const addTextWithPageBreak = (text, x, y, footerHeight, options = {}) => {
            const lineHeight = doc.getLineHeight() / doc.internal.scaleFactor;
            const lines = doc.splitTextToSize(text, options.maxWidth || (doc.internal.pageSize.getWidth() - x - 10));
            let currentY = y;

            lines.forEach(line => {
                currentY = checkPageBreak(currentY, lineHeight, footerHeight);
                doc.text(line, x, currentY, options);
                currentY += lineHeight;
            });

            return currentY;
        };

        const generateContent = async () => {
            await addLogo();
            let footerHeight = await addFooter();
            const pageWidth = doc.internal.pageSize.getWidth();
            let currentY = 10;

            // Título
            doc.setFontSize(16);
            const titleWidth = doc.getTextWidth("Factura Electrónica");
            currentY = checkPageBreak(currentY, 15, footerHeight);
            doc.text("Factura Electrónica", pageWidth / 2 - titleWidth / 2, currentY);
            currentY += 15;

            // Datos del comprobante
            doc.setFontSize(8);
            currentY = checkPageBreak(currentY, 24, footerHeight);
            doc.text(`Factura Nro. ${facturaId || 'N/A'}`, 10, currentY);
            doc.text(`Fecha y Hora: ${currentDate}`, 10, currentY + 6);
            doc.text(`CAE: ${cae}`, 10, currentY + 12);
            doc.text(`Vencimiento CAE: ${caefchvto}`, 10, currentY + 18);
            currentY += 24;

            // Dirección sucursal
            currentY = checkPageBreak(currentY, 10, footerHeight);
            try {
                const response = await fetch(`/api/datos_tienda/${storeId}`);
                if (!response.ok) throw new Error("Error al obtener datos de la sucursal");
                const data = await response.json();
                direccionSucursal = data.direccion_completa_unidad_operativa || "Dirección no disponible";
            } catch (error) {
                console.error("Error al obtener datos de la sucursal:", error);
            }
            doc.text(direccionSucursal, pageWidth - 10, currentY, { align: "right" });
            currentY += 10;

            // Datos del cliente
            doc.setFontSize(10);
            currentY = checkPageBreak(currentY, 25, footerHeight);
            doc.text("Facturado a", 10, currentY);
            doc.setFontSize(8);
            const clienteNombre = cartCopy.client?.nombre_cliente || "Consumidor Final";
            const clienteId = cartCopy.client?.numero_cliente || "N/A";
            const clienteIva = cartCopy.client?.tipo_contribuyente || "N/A";
            doc.text(`Código de Cliente: ${clienteId}`, 10, currentY + 8);
            doc.text(`Nombre de Cliente: ${clienteNombre}`, 10, currentY + 14);
            doc.text(`Condición IVA: ${clienteIva}`, 10, currentY + 20);
            currentY += 25;

            // Vendedor
            currentY = checkPageBreak(currentY, 10, footerHeight);
            try {
                const response = await fetch('/api/user_info');
                if (!response.ok) throw new Error(`Error al obtener vendedor: ${response.status}`);
                const data = await response.json();
                vendedor = data.nombre_completo || "Vendedor no disponible";
            } catch (error) {
                console.warn("Error al obtener vendedor:", error);
                vendedor = "Usuario desconocido";
            }
            doc.text(`Vendedor: ${vendedor}`, pageWidth - 10, currentY, { align: "right" });
            currentY += 10;

            // Tabla de productos
            const tableHeaders = [
                "Código",
                "Descripción",
                "Cantidad",
                "U.M",
                "Precio Unitario Lista",
                "Precio Unitario con Desc.",
                "Importe Total"
            ];

            const tableData = cartCopy.items.map(item => {
                const precioFinalConIva = convertirMonedaANumero(item.precioLista) || 0;
                const precioFinalConDescuento = convertirMonedaANumero(item.price) || 0;
                const quantity = parseFloat(item.quantity) || 1;
                const totalDescuento = (precioFinalConIva - precioFinalConDescuento) * quantity;
                const porcentajeDescuento = precioFinalConIva !== 0
                    ? ((precioFinalConIva - precioFinalConDescuento) / precioFinalConIva) * 100
                    : 0;
                const importeTotal = precioFinalConDescuento * quantity;

                const row = [
                    item.productId || "N/A",
                    item.productName || "Producto sin nombre",
                    { content: quantity.toFixed(2).replace(".", ","), styles: { halign: "right" } },
                    item.unidadMedida || "un",
                    `$${formatearMoneda(precioFinalConIva)}`,
                    `$${formatearMoneda(precioFinalConDescuento)}`,
                    `$${formatearMoneda(importeTotal)}`
                ];

                // Solo agregar la fila de descuento si hay un descuento real
                if (totalDescuento > 0) {
                    row.push({
                        content: `Descuento: $${formatearMoneda(totalDescuento)} (${porcentajeDescuento.toFixed(2).replace(".", ",")}%)`,
                        colSpan: 7,
                        styles: { fontSize: 7, halign: "left", textColor: [100, 100, 100] }
                    });
                }

                return row;
            });

            const flattenedData = [];
            tableData.forEach(row => {
                flattenedData.push(row.slice(0, 7));
                if (row.length > 7) {
                    flattenedData.push(row.slice(7));
                }
            });

            doc.autoTable({
                startY: currentY,
                head: [tableHeaders],
                body: flattenedData,
                theme: "grid",
                headStyles: { fillColor: [180, 185, 199], textColor: [255, 255, 255], fontSize: 7 },
                bodyStyles: { fontSize: 8, lineHeight: 1.1 },
                columnStyles: {
                    0: { cellWidth: 25 },
                    1: { cellWidth: 55, overflow: "linebreak" },
                    2: { cellWidth: 20, halign: "right" },
                    3: { cellWidth: 15, halign: "center" },
                    4: { cellWidth: 25, halign: "right" },
                    5: { cellWidth: 25, halign: "right" },
                    6: { cellWidth: 25, halign: "right" }
                },
                styles: { minCellHeight: 8, cellPadding: 1 },
                margin: { left: 10, right: 10, bottom: footerHeight + 10 },
                pageBreak: "auto",
                didDrawPage: (data) => {
                    currentY = data.cursor.y;
                    const pageWidth = doc.internal.pageSize.getWidth();
                    const footerWidth = pageWidth - 20;
                    const footerYPosition = PAGE_HEIGHT - footerHeight - 10;
                    const img = new Image();
                    img.src = "/static/img/pie.png";
                    doc.addImage(img, "PNG", 10, footerYPosition, footerWidth, footerHeight);
                }
            });

            currentY = doc.lastAutoTable.finalY + 5;

            // Condiciones generales
            currentY = checkPageBreak(currentY, 10, footerHeight);
            doc.setFontSize(8);
            currentY = addTextWithPageBreak("Condiciones Generales:", 10, currentY, footerHeight);
            currentY = addTextWithPageBreak("• Oferta no vinculante, sujeta a modificación sin previo aviso", 15, currentY, footerHeight);

            // Observaciones
            currentY = checkPageBreak(currentY, 10, footerHeight);
            currentY = addTextWithPageBreak("Observaciones:", 10, currentY, footerHeight);
            if (cartObservations) {
                currentY = addTextWithPageBreak(cartObservations, 15, currentY, footerHeight, { maxWidth: 180 });
            } else {
                currentY = addTextWithPageBreak("Sin observaciones", 15, currentY, footerHeight);
            }

            // Calcular totales
            const totalDescuentos = cartCopy.items.reduce((sum, item) => {
                const precioFinalConIva = convertirMonedaANumero(item.precioLista) || 0;
                const precioFinalConDescuento = convertirMonedaANumero(item.price) || 0;
                const quantity = parseFloat(item.quantity) || 1;
                return sum + (precioFinalConIva - precioFinalConDescuento) * quantity;
            }, 0);

            const total = cartCopy.items.reduce((sum, item) => {
                const price = convertirMonedaANumero(item.price) || 0;
                const quantity = parseFloat(item.quantity) || 1;
                return sum + price * quantity;
            }, 0);

            // Crear tabla de totales solo si hay descuentos o un total válido
            const totalRows = [];
            if (totalDescuentos > 0) {
                totalRows.push(["Total de descuentos aplicados", "", "", "", "", "", `$${formatearMoneda(totalDescuentos)}`]);
            }
            totalRows.push(["Total", "", "", "", "", "", `$${formatearMoneda(total)}`]);

            doc.autoTable({
                startY: currentY,
                body: totalRows,
                theme: "plain",
                styles: { fontSize: 8, lineWidth: 0.1 },
                columnStyles: {
                    0: { cellWidth: 60, halign: "left" },
                    6: { cellWidth: 30, halign: "right" }
                },
                margin: { left: 10, right: 10, bottom: footerHeight + 10 },
                didDrawPage: (data) => {
                    currentY = data.cursor.y;
                    const pageWidth = doc.internal.pageSize.getWidth();
                    const footerWidth = pageWidth - 20;
                    const footerYPosition = PAGE_HEIGHT - footerHeight - 10;
                    const img = new Image();
                    img.src = "/static/img/pie.png";
                    doc.addImage(img, "PNG", 10, footerYPosition, footerWidth, footerHeight);
                }
            });

            currentY = doc.lastAutoTable.finalY + 6;
            currentY = checkPageBreak(currentY, 8, footerHeight);
            doc.setFontSize(10);
            currentY = addTextWithPageBreak("Forma de Pago", 10, currentY, footerHeight);
            doc.setFontSize(8);
            currentY = addTextWithPageBreak("Efectivo", 10, currentY, footerHeight);
            currentY = addTextWithPageBreak(`${formatearMoneda(total)}`, 10, currentY, footerHeight);

            const pdfBlob = doc.output('blob');
            const pdfUrl = URL.createObjectURL(pdfBlob);
            const printWindow = window.open(pdfUrl);
            printWindow.onload = () => {
                printWindow.print();
                printWindow.onfocus = () => {
                    setTimeout(() => {
                        URL.revokeObjectURL(pdfUrl);
                        printWindow.close();
                    }, 0);
                };
            };

            resolve();
        };

        generateContent().catch(error => {
            console.error("Error en generateContent:", error);
            showToast('danger', `Error al generar el PDF: ${error.message}`);
            reject(error);
        });
    });
}

// Actualizar estado del botón de facturar
function updateFacturarButton() {
    const facturarBtn = document.getElementById('facturarBtn');
    if (facturarBtn) {
        facturarBtn.disabled = !(cart.items.length > 0 && cart.client && cart.client.numero_cliente);
    }
}

// Escuchar cambios en el carrito para habilitar/deshabilitar el botón
document.addEventListener('DOMContentLoaded', () => {
    updateCartDisplay(); // Inicializar al cargar
    updateFacturarButton(); // Inicializar estado del botón
    setInterval(updateFacturarButton, 1000); // Verificar cada segundo
});

function registerMixedPayment() {
    const data = {
        operacion_id: document.getElementById('operacionId')?.value || null,
        efectivo: document.getElementById('cashCheck')?.checked ? parseFloat(document.getElementById('cashAmount').value || 0) : 0,
        transferencia: document.getElementById('transferCheck')?.checked ? parseFloat(document.getElementById('transferAmount').value || 0) : 0,
        tarjeta: document.getElementById('cardCheck')?.checked ? parseFloat(document.getElementById('cardAmount').value || 0) : 0,
        referencia: document.getElementById('transferRef')?.value || ''
    };
    fetchWithAuth('/pagos/registrar', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data)
    })
    .then(r => r.json())
    .then(resp => {
        if (resp.status === 'ok') {
            showToast('success', 'Pago registrado');
        } else {
            showToast('danger', resp.error || 'Error al registrar pago');
        }
    })
    .catch(() => showToast('danger', 'Error de red al registrar pago'));
}
window.registerMixedPayment = registerMixedPayment;
