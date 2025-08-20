const DB_NAME = 'ClientesDB';
const STORE_NAME = 'clientes_pendientes';
let db;

function openDB() {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(DB_NAME, 1);
    request.onupgradeneeded = event => {
      const db = event.target.result;
      if (!db.objectStoreNames.contains(STORE_NAME)) {
        db.createObjectStore(STORE_NAME, { keyPath: 'cuit' });
      }
    };
    request.onsuccess = event => {
      db = event.target.result;
      resolve(db);
    };
    request.onerror = () => reject(request.error);
  });
}

async function saveClienteOffline(cliente) {
  if (!db) await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(STORE_NAME, 'readwrite');
    tx.objectStore(STORE_NAME).put(cliente);
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
  });
}

async function getClientesPendientes() {
  if (!db) await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(STORE_NAME, 'readonly');
    const req = tx.objectStore(STORE_NAME).getAll();
    req.onsuccess = () => resolve(req.result || []);
    req.onerror = () => reject(req.error);
  });
}

async function deleteCliente(cuit) {
  if (!db) await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(STORE_NAME, 'readwrite');
    tx.objectStore(STORE_NAME).delete(cuit);
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
  });
}

async function syncClientes() {
  if (!navigator.onLine) return;
  const pendientes = await getClientesPendientes();
  for (const cliente of pendientes) {
    try {
      const resp = await fetch('/clientes/nuevo', {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: new URLSearchParams(cliente)
      });
      if (resp.ok) {
        await deleteCliente(cliente.cuit);
      }
    } catch (err) {
      console.warn('Sincronización pendiente', err);
    }
  }
}

window.addEventListener('online', syncClientes);

document.addEventListener('DOMContentLoaded', () => {
  const form = document.getElementById('clienteForm');
  if (!form) return;
  form.addEventListener('submit', async ev => {
    if (!navigator.onLine) {
      ev.preventDefault();
      const data = Object.fromEntries(new FormData(form));
      await saveClienteOffline(data);
      alert('Cliente almacenado offline y se sincronizará cuando haya conexión.');
    }
  });
});
