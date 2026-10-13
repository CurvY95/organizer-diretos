import hashlib
import math
import re
from dataclasses import dataclass
from typing import Optional

import pandas as pd

from .customers import with_customer_labels


@dataclass(frozen=True)
class ParsedData:
    orders: pd.DataFrame
    prices: pd.DataFrame
    merged: pd.DataFrame
    missing_price_keys: pd.DataFrame


REQUIRED_ORDERS_COLS = ["Cliente", "Produto", "Quantidade"]
REQUIRED_PRICES_COLS = ["Produto", "Preco"]

ORDERS_ALIASES = {
    "Cliente": ["Cliente", "Nome", "NOME", "cliente", "name"],
    "UserId": ["UserId", "UserID", "user_id", "USER_ID", "User ID", "ID", "id", "PSID", "psid"],
    "ProfileId": ["ProfileId", "ProfileID", "profile_id", "PROFILE_ID", "Profile ID", "Username", "username", "user_name"],
    "Produto": ["Produto", "Referência", "Referencia", "Referência ", "Ref", "REF", "produto", "ref"],
    "Quantidade": ["Quantidade", "Qtd", "QTD", "quantidade", "qtd"],
    "Hora": ["Hora", "hora", "Time", "time", "Horário", "Horario", "DataHora", "Data Hora", "Timestamp", "timestamp"],
    # Tampermonkey / exports often include the raw user message / comment.
    "Comentario": [
        "Comentario",
        "Comentário",
        "comment",
        "Comment",
        "Mensagem",
        "Message",
        "Observacao",
        "Observação",
        "OBS",
        "Obs",
        "Notas",
        "Note",
    ],
}

PRICES_ALIASES = {
    "Produto": ["Produto", "Ref", "REF", "Referência", "Referencia", "produto", "ref"],
    "Preco": ["Preco", "Preço", "Preço/m", "Price/m", "UnitPrice", "unitprice", "preco", "precos", "preços", "price", "valor"],
}


def normalize_produto_key(v: str) -> str:
    s = "" if v is None else str(v)
    s = s.replace("\u00a0", " ")  # non-breaking space
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    return s.lower()


def _normalize_col_name(c) -> str:
    # Column names can arrive as float/NaN when reading some CSV/XLSX exports.
    if c is None:
        c = ""
    try:
        # pandas may use numpy.nan (float) for empty headers
        if isinstance(c, float) and pd.isna(c):
            c = ""
    except Exception:
        pass
    c = str(c).strip()
    c = re.sub(r"\s+", " ", c)
    return c


def coerce_number_series(s: pd.Series) -> pd.Series:
    if s is None:
        return s

    def normalize_one(x) -> str:
        if x is None or pd.isna(x):
            return ""
        x = str(x).strip().replace("\u00a0", " ")
        # Currency is permitted only around the number. Stripping arbitrary
        # characters would turn invalid quantities ("2x") into valid charges.
        currency = r"(?:R\$|[€$£]|EUR|USD|BRL|GBP)"
        x = re.sub(rf"^{currency}\s*", "", x, flags=re.IGNORECASE)
        x = re.sub(rf"\s*{currency}$", "", x, flags=re.IGNORECASE)
        x = x.replace(" ", "")
        if not re.fullmatch(r"[+-]?(?:\d[\d,.]*|[,.]\d+)(?:[eE][+-]?\d+)?", x):
            return ""
        # Keep the exponent separate while normalizing decimal separators.
        mantissa, *exponent = re.split(r"[eE]", x)
        has_dot = "." in mantissa
        has_comma = "," in mantissa
        if has_dot and has_comma:
            # Decide decimal separator by last occurrence.
            if mantissa.rfind(",") > mantissa.rfind("."):
                # 1.234,56 -> 1234.56
                mantissa = mantissa.replace(".", "").replace(",", ".")
            else:
                # 1,234.56 -> 1234.56
                mantissa = mantissa.replace(",", "")
        elif has_comma and not has_dot:
            mantissa = mantissa.replace(",", ".")
        return mantissa + ("e" + exponent[0] if exponent else "")

    values = pd.to_numeric(s.map(normalize_one), errors="coerce")
    return values.where(values.map(math.isfinite))


def normalize_quantities(s: pd.Series, *, fill_missing_with=None) -> pd.Series:
    """Validate quantities, filling only cells that were originally empty."""
    missing = s.map(lambda value: pd.isna(value) or (isinstance(value, str) and not value.strip()))
    quantities = coerce_number_series(s)
    if fill_missing_with is not None:
        try:
            replacement = float(fill_missing_with)
        except (TypeError, ValueError) as exc:
            raise ValueError("Quantidade: o valor para preencher células vazias deve ser um número finito.") from exc
        if not math.isfinite(replacement):
            raise ValueError("Quantidade: o valor para preencher células vazias deve ser um número finito.")
        quantities = quantities.mask(missing, replacement)
    invalid = quantities.isna()
    if invalid.any():
        examples = "; ".join(
            f"linha {position + 1}: {value!r}"
            for position, (value, bad) in enumerate(zip(s.tolist(), invalid.tolist()))
            if bad
        )
        raise ValueError(
            "Encomendas: encontrei valores inválidos em `Quantidade`. "
            "Usa números finitos; apenas células vazias podem assumir a quantidade padrão. "
            f"Exemplos: {examples[:1000]}"
        )
    return quantities


def standardize_df_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [_normalize_col_name(c) for c in df.columns]
    return df


def apply_aliases(df: pd.DataFrame, aliases: dict[str, list[str]]) -> pd.DataFrame:
    df = df.copy()
    cols = list(df.columns)
    lower_map = {str(c).strip().lower(): c for c in cols}

    rename: dict[str, str] = {}
    for target, options in aliases.items():
        if target in df.columns:
            continue
        found = None
        for opt in options:
            key = str(opt).strip().lower()
            if key in lower_map:
                found = lower_map[key]
                break
        if found is not None and found != target:
            rename[found] = target

    if rename:
        df = df.rename(columns=rename)
    return df


def validate_required_cols(df: pd.DataFrame, required: list[str], label: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"{label}: faltam colunas obrigatórias: {', '.join(missing)}. "
            f"Colunas encontradas: {', '.join(map(str, df.columns))}"
        )


def detect_sheet(excel: pd.ExcelFile, kind: str) -> Optional[str]:
    # Simple heuristics by sheet name.
    candidates = excel.sheet_names
    lowered = {name: name.lower() for name in candidates}

    if kind == "orders":
        keywords = ["encom", "pedido", "order", "orders", "clientes"]
    else:
        keywords = ["preco", "preços", "precos", "price", "prices", "produto", "produtos"]

    for name, lname in lowered.items():
        if any(k in lname for k in keywords):
            return name
    return candidates[0] if candidates else None


def parse_inputs(
    orders_df: pd.DataFrame,
    prices_df: pd.DataFrame,
    *,
    fill_missing_quantity_with: Optional[float] = None,
) -> ParsedData:
    orders_df = standardize_df_columns(orders_df)
    prices_df = standardize_df_columns(prices_df)

    orders_df = apply_aliases(orders_df, ORDERS_ALIASES)
    prices_df = apply_aliases(prices_df, PRICES_ALIASES)

    validate_required_cols(orders_df, REQUIRED_ORDERS_COLS, "Encomendas")
    validate_required_cols(prices_df, REQUIRED_PRICES_COLS, "Preços")

    keep_cols = REQUIRED_ORDERS_COLS.copy()
    for opt in ["UserId", "ProfileId", "Hora", "Comentario"]:
        if opt in orders_df.columns:
            keep_cols.append(opt)

    orders = orders_df[keep_cols].copy()
    prices = prices_df[REQUIRED_PRICES_COLS].copy()

    orders["Cliente"] = orders["Cliente"].astype(str).str.strip()
    orders["Produto"] = orders["Produto"].astype(str).str.strip()
    if "UserId" in orders.columns:
        orders["UserId"] = orders["UserId"].fillna("").astype(str).str.strip()
    if "ProfileId" in orders.columns:
        orders["ProfileId"] = orders["ProfileId"].fillna("").astype(str).str.strip()
    if "Hora" in orders.columns:
        orders["Hora"] = orders["Hora"].astype(str).str.strip()
    if "Comentario" in orders.columns:
        orders["Comentario"] = orders["Comentario"].astype(str)

    orders = with_customer_labels(orders)

    prices["Produto"] = prices["Produto"].astype(str).str.strip()

    orders["Quantidade"] = normalize_quantities(
        orders["Quantidade"], fill_missing_with=fill_missing_quantity_with
    )
    prices["Preco"] = coerce_number_series(prices["Preco"])
    if prices["Preco"].isna().any():
        bad = prices[prices["Preco"].isna()][["Produto"]].head(20)
        raise ValueError(
            "Preços: encontrei valores inválidos em `Preco`. "
            f"Exemplos (até 20):\n{bad.to_string(index=False)}"
        )

    orders["ProdutoKey"] = orders["Produto"].map(normalize_produto_key)
    prices["ProdutoKey"] = prices["Produto"].map(normalize_produto_key)

    prices = prices.drop_duplicates(subset=["ProdutoKey"], keep="last")

    merged = orders.merge(prices[["ProdutoKey", "Preco"]], on="ProdutoKey", how="left", validate="m:1")
    merged["TotalItem"] = merged["Quantidade"] * merged["Preco"]

    missing = (
        merged[merged["Preco"].isna()][["ProdutoKey", "Produto"]]
        .drop_duplicates()
        .sort_values(["ProdutoKey"])
        .reset_index(drop=True)
    )

    return ParsedData(orders=orders, prices=prices, merged=merged, missing_price_keys=missing)


def apply_price_overrides(merged: pd.DataFrame, overrides: pd.DataFrame) -> pd.DataFrame:
    merged = merged.copy()
    if overrides is None or overrides.empty:
        return merged
    ov = overrides.copy()
    ov["ProdutoKey"] = ov["ProdutoKey"].map(normalize_produto_key)
    ov["Preco"] = coerce_number_series(ov["Preco"])
    ov = ov.dropna(subset=["ProdutoKey", "Preco"]).drop_duplicates(subset=["ProdutoKey"], keep="last")

    merged = merged.merge(
        ov[["ProdutoKey", "Preco"]].rename(columns={"Preco": "PrecoOverride"}),
        on="ProdutoKey",
        how="left",
    )
    merged["Preco"] = merged["PrecoOverride"].combine_first(merged["Preco"])
    merged = merged.drop(columns=["PrecoOverride"])
    merged["TotalItem"] = merged["Quantidade"] * merged["Preco"]
    return merged


def build_summary(merged: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    merged = with_customer_labels(merged)
    by_client = (
        merged.groupby("ClienteKey", dropna=False, as_index=False)
        .agg(Cliente=("Cliente", "first"), Total=("TotalItem", "sum"))
        .drop(columns=["ClienteKey"])
        .sort_values(["Cliente"])
    )
    detail_map: dict[str, pd.DataFrame] = {}
    for _, g in merged.groupby("ClienteKey", dropna=False):
        client = str(g["Cliente"].iloc[0])
        g2 = (
            g.groupby(["Produto"], as_index=False)
            .agg(
                Quantidade=("Quantidade", "sum"),
                Preco=("Preco", "max"),
                TotalItem=("TotalItem", "sum"),
            )
            .sort_values(["Produto"])
        )
        g2.attrs["customer_name"] = str(g["ClienteNome"].iloc[0])
        detail_map[str(client)] = g2
    return by_client, detail_map


def format_currency(v: float, currency: str) -> str:
    if currency.upper() == "EUR":
        s = f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return f"{s} €"
    if currency.upper() == "BRL":
        s = f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return f"R$ {s}"
    return f"{v:.2f} {currency}"


def build_message(
    client: str,
    details: pd.DataFrame,
    total: float,
    currency: str,
    intro: str,
    outro: str,
    total_line_template: str = "Total: {total}",
) -> str:
    lines: list[str] = []
    if intro.strip():
        lines.append(intro.strip())
    lines.append(f"{details.attrs.get('customer_name', client)}:")
    for _, row in details.iterrows():
        q = row["Quantidade"]
        p = row["Preco"]
        t = row["TotalItem"]
        q_str = f"{q:g}"
        lines.append(
            f"- {row['Produto']} — {q_str} x {format_currency(float(p), currency)} = {format_currency(float(t), currency)}"
        )
    total_line = str(total_line_template or "Total: {total}").replace("{total}", format_currency(float(total), currency))
    if total_line.strip():
        lines.append(total_line.strip())
    if outro.strip():
        lines.append(outro.strip())
    return "\n".join(lines).strip() + "\n"


def stable_orders_fingerprint(orders: pd.DataFrame) -> str:
    cols = ["Cliente", "Produto", "Quantidade"]
    if "ClienteKey" in orders.columns:
        cols.append("ClienteKey")
    df = orders[cols].copy()
    df["Cliente"] = df["Cliente"].astype(str).str.strip()
    df["Produto"] = df["Produto"].map(normalize_produto_key)
    df["Quantidade"] = pd.to_numeric(df["Quantidade"], errors="coerce").fillna(0)
    df = df.sort_values(cols).reset_index(drop=True)
    payload = df.to_csv(index=False).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:16]
