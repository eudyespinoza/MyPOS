import os
import requests


class PaywayClient:
    """Cliente simple para interactuar con el API de Payway."""

    def __init__(self, api_key: str | None = None, secret_key: str | None = None, base_url: str | None = None) -> None:
        self.api_key = api_key or os.getenv("PAYWAY_API_KEY", "")
        self.secret_key = secret_key or os.getenv("PAYWAY_SECRET_KEY", "")
        self.base_url = base_url or os.getenv("PAYWAY_BASE_URL", "https://apisandbox.payway.com.ar")
        self.session = requests.Session()
        if self.api_key and self.secret_key:
            self.session.headers.update(
                {
                    "apikey": self.api_key,
                    "secretkey": self.secret_key,
                    "Content-Type": "application/json",
                }
            )

    def create_payment(self, data: dict) -> dict:
        """Crea un pago en Payway."""
        url = f"{self.base_url}/payments"
        resp = self.session.post(url, json=data)
        resp.raise_for_status()
        return resp.json()

    def get_payment(self, payment_id: str) -> dict:
        """Obtiene el estado de un pago."""
        url = f"{self.base_url}/payments/{payment_id}"
        resp = self.session.get(url)
        resp.raise_for_status()
        return resp.json()
