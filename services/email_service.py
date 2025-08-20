import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import configparser
import os
import datetime
import logging

# Configuración básica de logging (puedes ajustarla según necesites)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(__file__), '../app.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def load_email_config():
    """Carga la configuración de correo desde config.ini."""
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), '../config.ini')  # Ruta relativa a config.ini en BASE_DIR
    config.read(config_path)
    if 'email' not in config:
        logger.error("La sección 'email' no se encontró en config.ini")
        raise KeyError("La sección 'email' no se encontró en config.ini")
    return config['email']

def enviar_correo_fallo(proceso, error):
    """Envía un correo notificando un fallo en un proceso."""
    try:
        email_config = load_email_config()
        smtp_server = email_config['smtp_server']
        smtp_port = int(email_config['smtp_port'])
        smtp_user = email_config['username']
        smtp_password = email_config['password']
        destinatarios = ['edespinoza@familiabercomat.com', 'rortigoza@familiabercomat.com']

        msg = MIMEMultipart()
        msg['From'] = smtp_user
        msg['To'] = ', '.join(destinatarios)
        msg['Subject'] = f"Fallo en el proceso: {proceso}"

        html_body = f"""
<html>
<head>
    <style>
        body {{ font-family: 'Arial', sans-serif; background-color: #f5f5f5; color: #333333; line-height: 1.6; }}
        .container {{ max-width: 600px; margin: 0 auto; padding: 20px; background-color: #ffffff; border-radius: 10px; box-shadow: 0 0 15px rgba(0, 0, 0, 0.1); }}
        h2 {{ color: #dc3545; font-size: 1.8em; }}
        .info {{ padding: 10px 0; font-size: 1.1em; }}
        .info strong {{ color: #2c3e50; }}
        .highlight {{ color: #dc3545; font-weight: bold; }}
        .footer {{ text-align: center; padding-top: 20px; font-size: 0.9em; color: #888888; }}
    </style>
</head>
<body>
    <div class="container">
        <h2>Error detectado</h2>
        <p>Se ha detectado un error en el proceso QA<span class="highlight">{proceso}</span>.</p>
        <div class="info">
            <p><strong>Detalles del error:</strong> {error}</p>
            <p><strong>Fecha y hora:</strong> {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>
        <p>Por favor, verificar los logs para más información.</p>
        <p>Equipo de Arquitectura</p>
        <div class="footer">
            <p>Este es un mensaje automático, por favor no respondas a este correo.</p>
        </div>
    </div>
</body>
</html>
        """
        msg.attach(MIMEText(html_body, 'html'))

        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.sendmail(smtp_user, destinatarios, msg.as_string())

        logger.info(f"Correo de fallo enviado a {', '.join(destinatarios)} para el proceso '{proceso}'")
    except Exception as e:
        logger.error(f"Error al enviar el correo de fallo para el proceso '{proceso}': {e}", exc_info=True)
