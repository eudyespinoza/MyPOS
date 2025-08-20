const DB_NAME = 'OfflineSales';
const STORE_NAME = 'salesQueue';

function openDB() {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(DB_NAME, 1);
    request.onupgradeneeded = () => {
      request.result.createObjectStore(STORE_NAME, { autoIncrement: true });
    };
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

async function saveSale(data) {
  const db = await openDB();
  const tx = db.transaction(STORE_NAME, 'readwrite');
  tx.objectStore(STORE_NAME).add(data);
  return tx.complete;
}

async function sendQueuedSales() {
  const db = await openDB();
  const tx = db.transaction(STORE_NAME, 'readwrite');
  const store = tx.objectStore(STORE_NAME);
  const all = store.getAll();
  return new Promise(resolve => {
    all.onsuccess = async () => {
      const items = all.result || [];
      for (const sale of items) {
        try {
          await fetch('/ventas', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(sale)
          });
        } catch (_) {
          // Si falla, mantener en cola
          return resolve();
        }
      }
      store.clear();
      resolve();
    };
  });
}

self.addEventListener('fetch', event => {
  if (event.request.method === 'POST' && event.request.url.includes('/ventas')) {
    event.respondWith(
      fetch(event.request.clone()).catch(async () => {
        const data = await event.request.clone().json();
        await saveSale(data);
        return new Response(JSON.stringify({ offline: true }), { status: 202 });
      })
    );
  }
});

self.addEventListener('message', event => {
  if (event.data === 'syncSales') {
    event.waitUntil(sendQueuedSales());
  }
});
