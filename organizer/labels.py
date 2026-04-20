import html
from typing import Optional


def parse_hora_to_seconds(value: object) -> Optional[int]:
    s = str(value or "").strip().replace(" ", "")
    if not s:
        return None
    parts = [p for p in s.split(":") if p != ""]
    try:
        if len(parts) == 2:
            minutes = int(parts[0])
            seconds = int(parts[1])
            return minutes * 60 + seconds
        if len(parts) == 3:
            hours = int(parts[0])
            minutes = int(parts[1])
            seconds = int(parts[2])
            return hours * 3600 + minutes * 60 + seconds
    except ValueError:
        return None
    return None


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
