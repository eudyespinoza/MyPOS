// Navbar: Agrega enlace configs si admin
if (sessionStorage.getItem('role') === 'admin') {
  const nav = document.querySelector('.navbar-nav');
  const configLink = document.createElement('li');
  configLink.innerHTML = `<a class="btn btn-outline-light" href="/autenticacion_avanzada/configs_menu">Configuraciones</a>`;
  nav.appendChild(configLink);
}

// Integración Facturación en Carrito (botón visible al finalizar venta)
function updateCartDisplay() {
  // Código existente...
  const footer = document.querySelector('.cart-footer');
  const facturarBtn = document.createElement('button');
  facturarBtn.className = 'btn btn-success ms-2';
  facturarBtn.textContent = 'Facturar';
  facturarBtn.onclick = () => {
    if (cart.items.length > 0) {  // Visible si carrito no vacío
      const ventaData = { venta_id: cart.quotation_id, items: cart.items, cliente: cart.client, total: calculateTotal(), tipo_comprobante: 1 }; // Ejemplo Factura A
      facturarVenta(ventaData);
    } else {
      showToast('warning', 'Carrito vacío, no se puede facturar');
    }
  };
  footer.appendChild(facturarBtn);
}