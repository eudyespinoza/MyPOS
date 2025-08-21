from flask import Blueprint, request, jsonify, session, render_template, send_file
from auth_module import login_required
from db.database import obtener_facturas_emitidas, obtener_saldos_por_vendedor
from io import BytesIO

caja_bp = Blueprint("caja", __name__)


@caja_bp.route("/apertura", methods=["POST"])
@login_required
def apertura_caja():
    """Abre la caja con un monto inicial opcional."""
    monto_inicial = request.json.get("monto_inicial", 0.0)
    session["caja_abierta"] = True
    session["monto_inicial"] = monto_inicial
    session.setdefault("movimientos_caja", [])
    return jsonify({"message": "Caja abierta", "monto_inicial": monto_inicial})


@caja_bp.route("/cierre", methods=["POST"])
@login_required
def cierre_caja():
    """Cierra la caja y limpia la sesión."""
    session.pop("caja_abierta", None)
    session.pop("monto_inicial", None)
    movimientos = session.pop("movimientos_caja", [])
    return jsonify({"message": "Caja cerrada", "movimientos": movimientos})


@caja_bp.route("/arqueo", methods=["POST"])
@login_required
def arqueo_caja():
    """Registra un arqueo de caja."""
    monto = request.json.get("monto", 0.0)
    session.setdefault("arqueos", []).append(monto)
    return jsonify({"message": "Arqueo registrado", "monto": monto})


@caja_bp.route("/movimientos", methods=["POST"])
@login_required
def movimientos_caja():
    """Registra un movimiento de caja (entrada/salida)."""
    movimiento = request.json or {}
    session.setdefault("movimientos_caja", []).append(movimiento)
    return jsonify({"message": "Movimiento registrado", "movimiento": movimiento})


# ---- Reportes ----

@caja_bp.route("/reportes/facturas", methods=["GET"])
@login_required
def reportes_facturas():
    """Vista con filtro de facturas emitidas."""
    start = request.args.get("start_date")
    end = request.args.get("end_date")
    data = obtener_facturas_emitidas(start, end) if start and end else []
    return render_template(
        "reportes/facturas.html",
        data=data,
        start_date=start,
        end_date=end,
    )


@caja_bp.route("/reportes/facturas/pdf", methods=["GET"])
@login_required
def reportes_facturas_pdf():
    start = request.args.get("start_date")
    end = request.args.get("end_date")
    data = obtener_facturas_emitidas(start, end)
    if not data:
        return jsonify({"error": "No hay datos"}), 404
    import pandas as pd
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle
    from reportlab.lib import colors

    df = pd.DataFrame(data)
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer)
    table_data = [df.columns.tolist()] + df.values.tolist()
    table = Table(table_data)
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                ("GRID", (0, 0), (-1, -1), 1, colors.black),
            ]
        )
    )
    doc.build([table])
    buffer.seek(0)
    return send_file(
        buffer,
        as_attachment=True,
        download_name="facturas.pdf",
        mimetype="application/pdf",
    )


@caja_bp.route("/reportes/facturas/excel", methods=["GET"])
@login_required
def reportes_facturas_excel():
    start = request.args.get("start_date")
    end = request.args.get("end_date")
    data = obtener_facturas_emitidas(start, end)
    if not data:
        return jsonify({"error": "No hay datos"}), 404
    import pandas as pd

    df = pd.DataFrame(data)
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)
    output.seek(0)
    return send_file(
        output,
        as_attachment=True,
        download_name="facturas.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@caja_bp.route("/reportes/saldos", methods=["GET"])
@login_required
def reportes_saldos():
    start = request.args.get("start_date")
    end = request.args.get("end_date")
    data = obtener_saldos_por_vendedor(start, end) if start and end else []
    return render_template(
        "reportes/saldos.html",
        data=data,
        start_date=start,
        end_date=end,
    )


@caja_bp.route("/reportes/saldos/pdf", methods=["GET"])
@login_required
def reportes_saldos_pdf():
    start = request.args.get("start_date")
    end = request.args.get("end_date")
    data = obtener_saldos_por_vendedor(start, end)
    if not data:
        return jsonify({"error": "No hay datos"}), 404
    import pandas as pd
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle
    from reportlab.lib import colors

    df = pd.DataFrame(data)
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer)
    table_data = [df.columns.tolist()] + df.values.tolist()
    table = Table(table_data)
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                ("GRID", (0, 0), (-1, -1), 1, colors.black),
            ]
        )
    )
    doc.build([table])
    buffer.seek(0)
    return send_file(
        buffer,
        as_attachment=True,
        download_name="saldos.pdf",
        mimetype="application/pdf",
    )


@caja_bp.route("/reportes/saldos/excel", methods=["GET"])
@login_required
def reportes_saldos_excel():
    start = request.args.get("start_date")
    end = request.args.get("end_date")
    data = obtener_saldos_por_vendedor(start, end)
    if not data:
        return jsonify({"error": "No hay datos"}), 404
    import pandas as pd

    df = pd.DataFrame(data)
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)
    output.seek(0)
    return send_file(
        output,
        as_attachment=True,
        download_name="saldos.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
