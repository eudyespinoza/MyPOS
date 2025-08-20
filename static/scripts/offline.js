if ('serviceWorker' in navigator) {
  window.addEventListener('online', () => {
    navigator.serviceWorker.controller && navigator.serviceWorker.controller.postMessage('syncSales');
  });
}
