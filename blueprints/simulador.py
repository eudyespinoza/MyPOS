from __future__ import annotations

from typing import Dict, Any

from flask import Blueprint, request, render_template, jsonify

# Blueprint para el simulador de pagos
simulador_bp = Blueprint("simulador", __name__)

# Reglas de financiación simuladas
REGLAS: Dict[str, Dict[str, Any]] = {
    "3_sin_interes": {"descripcion": "3 cuotas sin interés", "cuotas": 3, "coeficiente": 1.0},
    "6_con_interes": {"descripcion": "6 cuotas (15% interés)", "cuotas": 6, "coeficiente": 1.15},
}

# Sucursales disponibles (placeholder)
SUCURSALES = [
    {"id": "BA001", "nombre": "Casa Central"},
    {"id": "BA002", "nombre": "Sucursal Norte"},
]


def calcular_linea(monto: float, regla: Dict[str, Any]) -> Dict[str, float]:
    """Calcula el total y el valor de cada cuota para una regla dada."""
    coef = float(regla.get("coeficiente", 1))
    cuotas = int(regla.get("cuotas", 1)) or 1
    total = monto * coef
    return {
        "total": round(total, 2),
        "cuota": round(total / cuotas, 2),
        "cuotas": cuotas,
    }


@simulador_bp.route("/simulador", methods=["GET", "POST"])
def simulador() -> str:
    """Muestra el simulador de pagos o calcula una línea."""
    total_carrito = request.args.get("total_carrito", type=float, default=0.0)
    resultado = None

    if request.method == "POST":
        monto = request.form.get("monto", type=float, default=0.0)
        regla_id = request.form.get("regla")
        regla = REGLAS.get(regla_id)
        if regla:
            resultado = calcular_linea(monto, regla)
        else:
            return jsonify({"error": "Regla no encontrada"}), 400

    return render_template(
        "pagos/simulador.html",
        reglas=REGLAS,
        sucursales=SUCURSALES,
        total_carrito=total_carrito,
        resultado=resultado,
    )
