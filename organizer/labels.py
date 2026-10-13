import html
from typing import Optional

import pandas as pd


def parse_hora_to_seconds(value: object) -> Optional[int]:
    if value is None or pd.isna(value):
        return None
    s = str(value).strip().replace(" ", "")
    if not s:
        return None
    parts = s.split(":")
    if not all(part.isdigit() for part in parts):
        return None
    try:
        if len(parts) == 2:
            minutes = int(parts[0])
            seconds = int(parts[1])
            return minutes * 60 + seconds if seconds < 60 else None
        if len(parts) == 3:
            hours = int(parts[0])
            minutes = int(parts[1])
            seconds = int(parts[2])
            return hours * 3600 + minutes * 60 + seconds if minutes < 60 and seconds < 60 else None
    except ValueError:
        return None
    return None


def aggregate_labels(base: pd.DataFrame) -> pd.DataFrame:
    """Combine label rows, choosing the first timestamp numerically."""
    group_columns = ["ClienteKey" if "ClienteKey" in base.columns else "Cliente", "Produto"]
    aggregations = {
        "Quantidade": ("Quantidade", "sum"),
        "Preco": ("Preco", "max"),
    }
    for name in ("Cliente", "ClienteNome"):
        if name in base.columns and name not in group_columns:
            aggregations[name] = (name, "first")
    if "Hora" in base.columns:
        def earliest_hora(values: pd.Series):
            valid = [
                (seconds, value)
                for value in values
                if (seconds := parse_hora_to_seconds(value)) is not None
            ]
            if valid:
                return min(valid, key=lambda entry: entry[0])[1]
            return values.iloc[0] if not values.empty else ""

        aggregations["Hora"] = ("Hora", earliest_hora)
    return base.groupby(group_columns, as_index=False, dropna=False).agg(**aggregations)


def build_labels_html(blocks: list[dict]) -> str:
    parts = []
    for block in blocks:
        client = html.escape(str(block.get("cliente") or ""))
        reference = html.escape(str(block.get("referencia") or ""))
        quantity = html.escape(str(block.get("quantidade") or ""))
        unit_price = html.escape(str(block.get("preco_unit") or ""))
        parts.append(
            f"""
  <div class="label">
    <div class="client">{client}</div>
    <div class="line">{reference} — {quantity} / m</div>
    <div class="price">{unit_price}</div>
  </div>
"""
        )
    body = "\n".join(parts) if parts else "<div style='opacity:.75;font-family:Arial'>Sem etiquetas para imprimir.</div>"
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Etiquetas 10x15</title>
  <style>
    @page {{ size: 100mm 150mm; margin: 6mm; }}
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif; }}
    .label {{
      width: 100mm;
      height: 150mm;
      box-sizing: border-box;
      page-break-after: always;
      display: flex;
      flex-direction: column;
      justify-content: space-between;
      padding: 6mm;
      border: 1px solid rgba(0,0,0,0.12);
      border-radius: 6mm;
    }}
    .client {{ font-size: 20pt; font-weight: 800; line-height: 1.05; }}
    .line {{ font-size: 16pt; font-weight: 650; margin-top: 8mm; }}
    .price {{ font-size: 26pt; font-weight: 900; }}
    @media print {{
      body {{ margin: 0; }}
      .label {{ border: none; border-radius: 0; }}
    }}
  </style>
</head>
<body>
{body}
</body>
</html>"""
