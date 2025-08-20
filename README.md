# MyPOS Deployment Guide

## Servidor WSGI y balanceo de carga
- Utilice `gunicorn` como servidor WSGI.
- Ejecute con múltiples workers utilizando la configuración incluida (`gunicorn.conf.py`).
- Despliegue detrás de un balanceador de carga en la nube (p.ej. Azure Load Balancer o Application Gateway) para distribuir el tráfico.

## Pooling de conexiones y caché
- El módulo `db/fabric.py` implementa pooling de conexiones hacia la base de datos de Fabric/Azure para reducir la latencia y reutilizar conexiones.
- `app.py` utiliza Redis para almacenar en caché información de clientes y productos. Configure la variable de entorno `REDIS_URL` apuntando a un servicio administrado de Redis.

## Persistencia offline
- Se agregó un *service worker* (`static/service-worker.js`) y soporte en `offline.js` para guardar ventas en cola usando IndexedDB cuando no hay conexión. Al recuperar la conectividad, las ventas pendientes se sincronizan automáticamente.

## Requisitos de infraestructura y monitoreo
- Dimensionar la infraestructura para al menos **200 sesiones simultáneas**.
- Escalar horizontalmente el número de instancias detrás del balanceador según sea necesario.
- Monitorear uso de CPU, memoria, workers de Gunicorn y métricas de Redis.
- Integrar herramientas de monitoreo como Azure Monitor/Application Insights para trazas y alertas.
