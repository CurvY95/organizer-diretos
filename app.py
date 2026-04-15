import io
import json
import os
import re
from dataclasses import dataclass
from typing import Optional
import hashlib
from datetime import datetime, timezone

import pandas as pd
import streamlit as st


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
    "Produto": ["Produto", "Referência", "Referencia", "Referência ", "Ref", "REF", "produto", "ref"],
    "Quantidade": ["Quantidade", "Qtd", "QTD", "quantidade", "qtd"],
}

PRICES_ALIASES = {
    "Produto": ["Produto", "Ref", "REF", "Referência", "Referencia", "produto", "ref"],
    "Preco": ["Preco", "Preço", "Preço/m", "Price/m", "UnitPrice", "unitprice", "preco", "price", "valor"],
}


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


def _coerce_number_series(s: pd.Series) -> pd.Series:
    if s is None:
        return s
    s2 = s.astype(str).str.strip()
    s2 = s2.str.replace("\u00a0", " ", regex=False)  # non-breaking space

    def normalize_one(x: str) -> str:
        x = (x or "").strip()
        if x == "":
            return ""
        x = x.replace(" ", "")
        x = re.sub(r"[^\d,.\-+]", "", x)
        has_dot = "." in x
        has_comma = "," in x
        if has_dot and has_comma:
            # Decide decimal separator by last occurrence.
            if x.rfind(",") > x.rfind("."):
                # 1.234,56 -> 1234.56
                x = x.replace(".", "").replace(",", ".")
            else:
                # 1,234.56 -> 1234.56
                x = x.replace(",", "")
        elif has_comma and not has_dot:
            x = x.replace(",", ".")
        return x

    s2 = s2.map(normalize_one)
    return pd.to_numeric(s2, errors="coerce")


def _standardize_df_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [_normalize_col_name(c) for c in df.columns]
    return df


def _apply_aliases(df: pd.DataFrame, aliases: dict[str, list[str]]) -> pd.DataFrame:
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


def _validate_required_cols(df: pd.DataFrame, required: list[str], label: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"{label}: faltam colunas obrigatórias: {', '.join(missing)}. "
            f"Colunas encontradas: {', '.join(map(str, df.columns))}"
        )


def _detect_sheet(excel: pd.ExcelFile, kind: str) -> Optional[str]:
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


def _load_from_xlsx(uploaded_file) -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
    raw = uploaded_file.getvalue()
    excel = pd.ExcelFile(io.BytesIO(raw))
    sheet_names = excel.sheet_names

    default_orders = _detect_sheet(excel, "orders")
    default_prices = _detect_sheet(excel, "prices")

    col1, col2 = st.columns(2)
    with col1:
        orders_sheet = st.selectbox(
            "Aba de encomendas",
            options=sheet_names,
            index=sheet_names.index(default_orders) if default_orders in sheet_names else 0,
        )
    with col2:
        prices_sheet = st.selectbox(
            "Aba de preços",
            options=sheet_names,
            index=sheet_names.index(default_prices) if default_prices in sheet_names else min(1, len(sheet_names) - 1),
        )

    orders_df = pd.read_excel(excel, sheet_name=orders_sheet)
    prices_df = pd.read_excel(excel, sheet_name=prices_sheet)
    return orders_df, prices_df, sheet_names


def _load_from_csvs(orders_file, prices_file) -> tuple[pd.DataFrame, pd.DataFrame]:
    orders_df = pd.read_csv(orders_file)
    prices_df = pd.read_csv(prices_file)
    return orders_df, prices_df


def parse_inputs(
    orders_df: pd.DataFrame,
    prices_df: pd.DataFrame,
    *,
    fill_missing_quantity_with: Optional[float] = None,
) -> ParsedData:
    orders_df = _standardize_df_columns(orders_df)
    prices_df = _standardize_df_columns(prices_df)

    orders_df = _apply_aliases(orders_df, ORDERS_ALIASES)
    prices_df = _apply_aliases(prices_df, PRICES_ALIASES)

    _validate_required_cols(orders_df, REQUIRED_ORDERS_COLS, "Encomendas")
    _validate_required_cols(prices_df, REQUIRED_PRICES_COLS, "Preços")

    # Keep optional columns (e.g. UserId) for UI/actions, but enforce required subset.
    keep_cols = REQUIRED_ORDERS_COLS + (["UserId"] if "UserId" in orders_df.columns else [])
    orders = orders_df[keep_cols].copy()
    prices = prices_df[REQUIRED_PRICES_COLS].copy()

    orders["Cliente"] = orders["Cliente"].astype(str).str.strip()
    orders["Produto"] = orders["Produto"].astype(str).str.strip()
    if "UserId" in orders.columns:
        orders["UserId"] = orders["UserId"].astype(str).str.strip()
    prices["Produto"] = prices["Produto"].astype(str).str.strip()

    orders["Quantidade"] = _coerce_number_series(orders["Quantidade"])
    prices["Preco"] = _coerce_number_series(prices["Preco"])

    if fill_missing_quantity_with is not None:
        orders["Quantidade"] = orders["Quantidade"].fillna(float(fill_missing_quantity_with))

    if orders["Quantidade"].isna().any():
        bad = orders[orders["Quantidade"].isna()][["Cliente", "Produto"]].head(20)
        raise ValueError(
            "Encomendas: encontrei valores inválidos em `Quantidade`. "
            f"Exemplos (até 20):\n{bad.to_string(index=False)}"
        )
    if prices["Preco"].isna().any():
        bad = prices[prices["Preco"].isna()][["Produto"]].head(20)
        raise ValueError(
            "Preços: encontrei valores inválidos em `Preco`. "
            f"Exemplos (até 20):\n{bad.to_string(index=False)}"
        )

    # Normalize keys to avoid issues like " m81" vs "m81"
    orders["ProdutoKey"] = orders["Produto"].astype(str).str.strip().str.lower()
    prices["ProdutoKey"] = prices["Produto"].astype(str).str.strip().str.lower()

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
    """
    overrides columns: ProdutoKey, Preco
    """
    merged = merged.copy()
    if overrides is None or overrides.empty:
        return merged
    ov = overrides.copy()
    ov["ProdutoKey"] = ov["ProdutoKey"].astype(str).str.strip().str.lower()
    ov["Preco"] = _coerce_number_series(ov["Preco"])
    ov = ov.dropna(subset=["ProdutoKey", "Preco"]).drop_duplicates(subset=["ProdutoKey"], keep="last")

    merged = merged.merge(ov[["ProdutoKey", "Preco"]].rename(columns={"Preco": "PrecoOverride"}), on="ProdutoKey", how="left")
    merged["Preco"] = merged["PrecoOverride"].combine_first(merged["Preco"])
    merged = merged.drop(columns=["PrecoOverride"])
    merged["TotalItem"] = merged["Quantidade"] * merged["Preco"]
    return merged


def build_summary(merged: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    by_client = (
        merged.groupby(["Cliente"], dropna=False, as_index=False)
        .agg(
            Total=("TotalItem", "sum"),
        )
        .sort_values(["Cliente"])
    )
    detail_map: dict[str, pd.DataFrame] = {}
    for client, g in merged.groupby("Cliente", dropna=False):
        g2 = (
            g.groupby(["Produto"], as_index=False)
            .agg(
                Quantidade=("Quantidade", "sum"),
                Preco=("Preco", "max"),
                TotalItem=("TotalItem", "sum"),
            )
            .sort_values(["Produto"])
        )
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
) -> str:
    lines: list[str] = []
    if intro.strip():
        lines.append(intro.strip())
    lines.append(f"{client}:")
    for _, row in details.iterrows():
        q = row["Quantidade"]
        p = row["Preco"]
        t = row["TotalItem"]
        q_str = f"{q:g}"
        lines.append(f"- {row['Produto']} — {q_str} x {format_currency(float(p), currency)} = {format_currency(float(t), currency)}")
    lines.append(f"Total: {format_currency(float(total), currency)}")
    if outro.strip():
        lines.append(outro.strip())
    return "\n".join(lines).strip() + "\n"


def load_local_state(path: str) -> dict:
    try:
        if not os.path.exists(path):
            return {}
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}


def save_local_state(path: str, data: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def stable_orders_fingerprint(orders: pd.DataFrame) -> str:
    cols = ["Cliente", "Produto", "Quantidade"]
    df = orders[cols].copy()
    df["Cliente"] = df["Cliente"].astype(str).str.strip()
    df["Produto"] = df["Produto"].astype(str).str.strip().str.lower()
    df["Quantidade"] = pd.to_numeric(df["Quantidade"], errors="coerce").fillna(0)
    df = df.sort_values(cols).reset_index(drop=True)
    payload = df.to_csv(index=False).encode("utf-8")
    import hashlib

    return hashlib.sha256(payload).hexdigest()[:16]


def template_version(intro: str, total_line_template: str, outro: str) -> str:
    payload = (intro or "") + "\n" + (total_line_template or "") + "\n" + (outro or "")
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:8]


SESSIONS_DIR = os.path.join(os.getcwd(), "saved", "sessions")
FB_PAGE_ID = "106851297526135"


def require_login() -> None:
    """
    Simple access gate so the app isn't public.
    Configure credentials via:
      - Streamlit secrets: AUTH_USER / AUTH_PASS
      - or env vars: ORGANIZER_USER / ORGANIZER_PASS
    """
    secrets = getattr(st, "secrets", {}) or {}
    expected_user = (secrets.get("AUTH_USER") if hasattr(secrets, "get") else None) or os.getenv("ORGANIZER_USER")
    expected_pass = (secrets.get("AUTH_PASS") if hasattr(secrets, "get") else None) or os.getenv("ORGANIZER_PASS")

    if not expected_user or not expected_pass:
        st.error(
            "Login não configurado. Defina `AUTH_USER`/`AUTH_PASS` em `st.secrets` "
            "ou `ORGANIZER_USER`/`ORGANIZER_PASS` nas variáveis de ambiente."
        )
        st.stop()

    if st.session_state.get("authenticated") is True:
        return

    with st.sidebar:
        st.divider()
        st.subheader("Login")
        u = st.text_input("Utilizador", key="auth_user")
        p = st.text_input("Password", type="password", key="auth_pass")
        do_login = st.button("Entrar", type="primary")
        if do_login:
            if u == expected_user and p == expected_pass:
                st.session_state["authenticated"] = True
                st.session_state.pop("auth_pass", None)
                st.rerun()
            else:
                st.error("Credenciais inválidas.")

    st.stop()


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def safe_session_id(ts_iso: str) -> str:
    return ts_iso.replace(":", "").replace("-", "").replace("+", "Z")


def list_sessions() -> list[dict]:
    if not os.path.isdir(SESSIONS_DIR):
        return []
    out: list[dict] = []
    for name in os.listdir(SESSIONS_DIR):
        if not name.endswith(".json"):
            continue
        path = os.path.join(SESSIONS_DIR, name)
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f) or {}
            out.append(
                {
                    "id": data.get("id") or name.replace(".json", ""),
                    "created_at": data.get("created_at") or "",
                    "label": data.get("label") or "",
                    "path": path,
                    "orders_rows": int(data.get("orders_rows") or 0),
                    "refs": int(data.get("refs") or 0),
                }
            )
        except Exception:
            continue
    out.sort(key=lambda x: x.get("created_at", ""), reverse=True)
    return out


def save_session(*, label: str, orders_for_calc: pd.DataFrame, price_overrides: dict, meta: dict) -> str:
    os.makedirs(SESSIONS_DIR, exist_ok=True)
    created_at = now_iso()
    sid = safe_session_id(created_at)
    path = os.path.join(SESSIONS_DIR, f"{sid}.json")

    orders_clean = orders_for_calc.copy()
    orders_clean = _standardize_df_columns(orders_clean)
    orders_clean = _apply_aliases(orders_clean, ORDERS_ALIASES)
    orders_clean = orders_clean[REQUIRED_ORDERS_COLS].copy()

    refs = int(orders_clean["Produto"].astype(str).str.strip().str.lower().nunique())

    payload = {
        "id": sid,
        "created_at": created_at,
        "label": label,
        "meta": meta or {},
        "orders_rows": int(orders_clean.shape[0]),
        "refs": refs,
        "orders": orders_clean.to_dict(orient="records"),
        "price_overrides": price_overrides or {},
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return sid


def load_session(session_path: str) -> dict:
    with open(session_path, "r", encoding="utf-8") as f:
        return json.load(f) or {}


def delete_session(session_path: str) -> None:
    os.remove(session_path)


st.set_page_config(page_title="Organizer Diretos", layout="wide", page_icon="🧾")

require_login()

st.markdown(
    """
<style>
  /* layout + typography */
  .block-container { padding-top: 1.25rem; padding-bottom: 2.5rem; max-width: 1200px; }
  h1, h2, h3 { letter-spacing: -0.02em; }
  /* subtle cards */
  .od-card {
    background: rgba(255,255,255,0.04);
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 14px;
    padding: 14px 14px 10px 14px;
  }
  .od-muted { opacity: 0.8; font-size: 0.95rem; }
  /* buttons */
  div.stButton > button, div.stDownloadButton > button {
    border-radius: 10px;
    padding: 0.55rem 0.9rem;
  }
  /* data editor */
  [data-testid="stDataFrame"] { border-radius: 12px; overflow: hidden; }
</style>
""",
    unsafe_allow_html=True,
)

col_a, col_b = st.columns([3, 2], vertical_alignment="bottom")
with col_a:
    st.title("Organizer Diretos")
    st.caption("Carrega o Excel (aba `Comments`), define preços no site e gera totais + mensagens prontas.")
with col_b:
    st.markdown(
        "<div class='od-card'><div class='od-muted'><b>Dica</b>: Preenche os preços todos de uma vez e clica <b>Guardar preços</b>.</div></div>",
        unsafe_allow_html=True,
    )

STATE_PATH = os.path.join(os.getcwd(), "saved", "organizer_state.json")

with st.sidebar:
    st.header("Configurações")
    if st.session_state.get("authenticated") is True:
        if st.button("Sair"):
            st.session_state["authenticated"] = False
            st.session_state.pop("loaded_session", None)
            st.rerun()

    st.subheader("Upload")
    st.caption("Este fluxo usa apenas a aba de comentários (ex.: `Comments`).")

    st.subheader("Moeda")
    currency = st.selectbox("Moeda", options=["EUR", "BRL", "USD"], index=0)

    st.subheader("Regras de quantidade")
    fill_missing_qty = st.checkbox("Se Quantidade estiver vazia, assumir 1", value=True)

    st.subheader("Mensagens")
    intro = st.text_input("Introdução", value="Oi! Segue o resumo da tua encomenda:")
    total_line_template = st.text_area(
        "Linha com total (use {total})",
        value="Total a pagar: {total}",
        height=70,
    )
    outro = st.text_input("Fecho", value="Obrigado!")

st.divider()

orders_df = None
prices_df = None
orders_source_label = None

tab_main, tab_history = st.tabs(["Trabalho atual", "Histórico"])

with tab_history:
    st.subheader("Histórico de sessões")
    sessions = list_sessions()
    if not sessions:
        st.info("Ainda não há sessões guardadas.")
    else:
        sessions_df = pd.DataFrame(sessions)[["created_at", "label", "orders_rows", "refs", "path"]]
        sessions_df = sessions_df.rename(
            columns={
                "created_at": "Data (UTC)",
                "label": "Nome",
                "orders_rows": "Linhas",
                "refs": "Referências",
                "path": "Arquivo",
            }
        )
        st.dataframe(sessions_df.drop(columns=["Arquivo"]), use_container_width=True)

        chosen = st.selectbox(
            "Abrir sessão",
            options=sessions,
            format_func=lambda s: f"{s['created_at']} — {s['label'] or s['id']}",
        )
        c1, c2 = st.columns([1, 1])
        with c1:
            if st.button("Abrir", type="primary"):
                data = load_session(chosen["path"])
                st.session_state["loaded_session"] = data
                st.success("Sessão carregada. Vá à aba 'Trabalho atual'.")
        with c2:
            with st.popover("Apagar sessão"):
                st.warning("Isto apaga a sessão localmente (não dá para recuperar).")
                confirm = st.checkbox("Confirmo que quero apagar", value=False, key="confirm_delete_session")
                if st.button("Apagar definitivamente", type="secondary", disabled=not confirm):
                    try:
                        delete_session(chosen["path"])
                        loaded = st.session_state.get("loaded_session") or {}
                        if loaded.get("id") == chosen.get("id"):
                            st.session_state.pop("loaded_session", None)
                        st.success("Sessão apagada.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Falha ao apagar: {e}")

with tab_main:
    # If a session was loaded, we can work without uploading again.
    loaded = st.session_state.get("loaded_session")
    if loaded and loaded.get("orders"):
        orders_df = pd.DataFrame(loaded["orders"])
        prices_df = pd.DataFrame(columns=["Produto", "Preco"])
        orders_source_label = f"Sessão: {loaded.get('label') or loaded.get('id')}"
        if "price_overrides" not in st.session_state or not st.session_state.get("price_overrides"):
            st.session_state["price_overrides"] = loaded.get("price_overrides") or {}
    else:
        uploaded = st.file_uploader(
            "Upload do ficheiro (.xlsx ou .csv)",
            type=["xlsx", "csv"],
            help="O Excel deve conter a aba `Comments` (ou semelhante). O CSV deve ter colunas como Cliente/Nome, Referência/Produto, Quantidade (e opcionalmente user_id).",
        )
        if uploaded is not None:
            try:
                name = (uploaded.name or "").lower()
                if name.endswith(".csv"):
                    orders_df = pd.read_csv(uploaded)
                    orders_source_label = f"CSV: {uploaded.name}"
                else:
                    raw = uploaded.getvalue()
                    excel = pd.ExcelFile(io.BytesIO(raw))
                    sheet_names = excel.sheet_names
                    default_orders = _detect_sheet(excel, "orders")
                    orders_sheet = st.selectbox(
                        "Aba de comentários / encomendas",
                        options=sheet_names,
                        index=sheet_names.index(default_orders) if default_orders in sheet_names else 0,
                    )
                    orders_df = pd.read_excel(excel, sheet_name=orders_sheet)
                    orders_source_label = f"Excel: {uploaded.name} / aba: {orders_sheet}"
                # We will always input prices in-app for this flow
                prices_df = pd.DataFrame(columns=["Produto", "Preco"])
            except Exception as e:
                st.error(f"Erro ao ler o ficheiro: {e}")

if orders_df is not None and prices_df is not None:
    try:
        tab_upload, tab_prices, tab_summary, tab_messages = st.tabs(
            ["1) Encomendas (Comments)", "2) Preços", "3) Resumo", "4) Mensagens"]
        )

        with tab_upload:
            st.subheader("Encomendas")
            st.caption("Edite as quantidades aqui. As outras abas refletem estas quantidades.")
            if orders_source_label:
                st.markdown(f"<div class='od-muted'>Fonte: <b>{orders_source_label}</b></div>", unsafe_allow_html=True)

            # Build an editable view with standardized columns
            orders_edit = _standardize_df_columns(orders_df)
            orders_edit = _apply_aliases(orders_edit, ORDERS_ALIASES)
            _validate_required_cols(orders_edit, REQUIRED_ORDERS_COLS, "Encomendas (Comments)")

            ui_cols = ["Cliente"]
            if "UserId" in orders_edit.columns:
                ui_cols.append("UserId")
            ui_cols += ["Produto", "Quantidade"]
            orders_edit = orders_edit[ui_cols].copy()
            orders_edit = orders_edit.rename(
                columns={"Cliente": "Cliente", "UserId": "User ID", "Produto": "Referência", "Quantidade": "Quantidade"}
            )
            # Force numeric dtype so the editor allows changing values reliably
            orders_edit["Quantidade"] = _coerce_number_series(orders_edit["Quantidade"])
            if fill_missing_qty:
                orders_edit["Quantidade"] = orders_edit["Quantidade"].fillna(1.0)

            col_cfg = {
                "Cliente": st.column_config.TextColumn("Cliente"),
                "Referência": st.column_config.TextColumn("Referência", disabled=True),
                "Quantidade": st.column_config.NumberColumn("Quantidade", min_value=0.0, step=0.5, format="%.3g"),
            }
            if "User ID" in orders_edit.columns:
                col_cfg["User ID"] = st.column_config.TextColumn("User ID", disabled=True)

            edited_orders = st.data_editor(
                orders_edit,
                use_container_width=True,
                num_rows="fixed",
                column_config={
                    **col_cfg,
                },
                key="comments_editor",
            )

            # Convert back to expected input shape
            orders_for_calc = edited_orders.rename(columns={"Referência": "Produto", "User ID": "UserId"}).copy()

            st.divider()
            st.subheader("Guardar sessão")
            c1, c2 = st.columns([2, 1])
            with c1:
                session_label = st.text_input(
                    "Nome da sessão",
                    value=(st.session_state.get("session_label") or ""),
                    placeholder="Ex.: Encomendas 14-04",
                )
                st.session_state["session_label"] = session_label
            with c2:
                if st.button("Guardar sessão", type="primary"):
                    if "price_overrides" not in st.session_state:
                        st.session_state["price_overrides"] = {}
                    sid = save_session(
                        label=session_label.strip(),
                        orders_for_calc=orders_for_calc,
                        price_overrides=st.session_state.get("price_overrides") or {},
                        meta={"source": orders_source_label or ""},
                    )
                    st.success(f"Sessão guardada: {sid}")

        parsed = parse_inputs(
            orders_for_calc,
            prices_df,
            fill_missing_quantity_with=1.0 if fill_missing_qty else None,
        )
        if "price_overrides" not in st.session_state:
            st.session_state["price_overrides"] = {}

        parsed_orders_fp = stable_orders_fingerprint(parsed.orders)
        local = load_local_state(STATE_PATH)
        saved_by_fp = (local.get("by_orders_fp") or {}).get(parsed_orders_fp) or {}
        if saved_by_fp.get("price_overrides") and not st.session_state["price_overrides"]:
            st.session_state["price_overrides"] = saved_by_fp["price_overrides"]

        # Full price table for all references present in orders
        price_table = (
            parsed.orders[["ProdutoKey", "Produto"]]
            .drop_duplicates()
            .sort_values(["ProdutoKey"])
            .reset_index(drop=True)
        )
        price_table["Preco"] = price_table["ProdutoKey"].map(st.session_state["price_overrides"])

        with tab_prices:
            st.subheader("Preços")
            st.caption("Edite tudo e clique em **Guardar preços** no final. Antes de guardar, as outras abas não mudam.")

            if "price_draft" not in st.session_state:
                st.session_state["price_draft"] = price_table[["Produto", "ProdutoKey", "Preco"]].copy()

            with st.form("prices_form", border=False):
                edited = st.data_editor(
                    st.session_state["price_draft"],
                    use_container_width=True,
                    num_rows="fixed",
                    column_config={
                        "Produto": st.column_config.TextColumn("Referência", disabled=True),
                        "ProdutoKey": st.column_config.TextColumn("Chave", disabled=True),
                        "Preco": st.column_config.NumberColumn("Preço (€/m)", min_value=0.0, step=0.1, format="%.2f"),
                    },
                    key="all_prices_editor",
                )

                c1, c2, c3 = st.columns([1, 1, 2])
                with c1:
                    do_save = st.form_submit_button("Guardar preços", type="primary")
                with c2:
                    do_reset = st.form_submit_button("Repor rascunho (voltar ao guardado)")
                with c3:
                    st.markdown("<div class='od-muted'>Os preços só aplicam depois de guardar.</div>", unsafe_allow_html=True)

            if do_reset:
                st.session_state["price_draft"] = price_table[["Produto", "ProdutoKey", "Preco"]].copy()
                st.rerun()

            # keep draft updated (but don't apply yet)
            st.session_state["price_draft"] = edited.copy()

            if do_save:
                for _, r in edited.iterrows():
                    k = str(r["ProdutoKey"]).strip().lower()
                    v = r["Preco"]
                    if pd.notna(v):
                        st.session_state["price_overrides"][k] = float(v)
                st.success("Preços guardados. As outras abas já usam estes valores.")
                st.rerun()

            overrides_df = pd.DataFrame(
                [{"ProdutoKey": k, "Preco": v} for k, v in st.session_state["price_overrides"].items()]
            )
            st.download_button(
                "Download preços guardados (.csv)",
                data=overrides_df.to_csv(index=False).encode("utf-8"),
                file_name="precos_inseridos_no_app.csv",
                mime="text/csv",
            )

        overrides_df = pd.DataFrame(
            [{"ProdutoKey": k, "Preco": v} for k, v in st.session_state["price_overrides"].items()]
        )
        merged = apply_price_overrides(parsed.merged, overrides_df)

        still_missing = merged[merged["Preco"].isna()][["ProdutoKey", "Produto"]].drop_duplicates()
        if not still_missing.empty:
            st.info(
                f"Ainda faltam preços para {len(still_missing)} referência(s). "
                "Preencha na aba '2) Preços' para liberar o resumo."
            )

        by_client, details = build_summary(merged.dropna(subset=["Preco"]))
        # Map Cliente -> UserId (if present)
        client_userid_map: dict[str, str] = {}
        if "UserId" in parsed.orders.columns:
            tmp = parsed.orders[["Cliente", "UserId"]].copy()
            tmp["Cliente"] = tmp["Cliente"].astype(str)
            tmp["UserId"] = tmp["UserId"].astype(str)
            tmp = tmp[(tmp["UserId"].str.strip() != "") & (tmp["UserId"].str.lower() != "nan")]
            for _, r in tmp.drop_duplicates(subset=["Cliente"]).iterrows():
                client_userid_map[str(r["Cliente"])] = str(r["UserId"]).strip()

        with tab_summary:
            st.subheader("Resumo")
            summary = merged.dropna(subset=["Preco"]).groupby("Cliente", as_index=False).agg(
                Total=("TotalItem", "sum"),
                QuantidadeTotal=("Quantidade", "sum"),
                ItensDiferentes=("ProdutoKey", "nunique"),
            )
            summary = summary.sort_values(["Cliente"])

            total_geral = float(summary["Total"].sum()) if not summary.empty else 0.0
            c1, c2, c3 = st.columns(3)
            c1.metric("Clientes", int(summary.shape[0]))
            c2.metric("Total geral", format_currency(total_geral, currency))
            c3.metric("Referências", int(merged["ProdutoKey"].nunique()) if "ProdutoKey" in merged.columns else 0)

            summary_display = summary.copy()
            summary_display["Total"] = summary_display["Total"].map(lambda v: format_currency(float(v), currency))
            st.dataframe(summary_display, use_container_width=True)

            st.subheader("Detalhe por cliente")
            tpl_ver = template_version(intro, total_line_template, outro)
            for client in summary["Cliente"].astype(str).tolist():
                with st.expander(f"{client}"):
                    d = details.get(client)
                    if d is None:
                        st.write("Sem itens com preço ainda.")
                        continue

                    client_total = float(summary[summary["Cliente"].astype(str) == client]["Total"].iloc[0])
                    st.markdown(
                        f"<div class='od-card'><b>Total a pagar</b><div style='font-size:1.25rem; margin-top:4px'>{format_currency(client_total, currency)}</div></div>",
                        unsafe_allow_html=True,
                    )

                    # Action buttons: copy message + open FB inbox
                    client_details = details.get(client)
                    client_msg = build_message(
                        client=client,
                        details=client_details,
                        total=client_total,
                        currency=currency,
                        intro=intro,
                        outro=outro,
                    )
                    user_id = client_userid_map.get(client, "")
                    chat_url = (
                        f"https://business.facebook.com/latest/inbox/all?asset_id={FB_PAGE_ID}"
                        f"&selected_item_id={user_id}&thread_type=FB_MESSAGE"
                        if user_id
                        else ""
                    )

                    st.markdown("<div class='od-muted' style='margin-top:8px'><b>Ações</b></div>", unsafe_allow_html=True)
                    a1, a2, a3, a4 = st.columns([1.2, 1.1, 1.5, 2.2])
                    btn_key_base = f"{client}_{tpl_ver}"
                    with a1:
                        st.components.v1.html(
                            f"""
<div>
  <button id="copy_{btn_key_base}" style="width:100%; padding:10px 12px; border-radius:10px; border:1px solid rgba(255,255,255,0.15); background: rgba(255,255,255,0.06); color: inherit; cursor:pointer;">
    COPIAR MENSAGEM
  </button>
  <div id="copystatus_{btn_key_base}" style="margin-top:6px; font-size:0.9rem; opacity:0.85;"></div>
</div>
<script>
(function() {{
  const btn = document.getElementById("copy_{btn_key_base}");
  const status = document.getElementById("copystatus_{btn_key_base}");
  if (!btn || btn.dataset.bound === "1") return;
  btn.dataset.bound = "1";
  const text = {json.dumps(client_msg)};
  btn.addEventListener("click", async () => {{
    try {{
      await navigator.clipboard.writeText(text);
      if (status) {{
        status.textContent = "Mensagem copiada!";
        setTimeout(() => {{ status.textContent = ""; }}, 2000);
      }}
    }} catch (e) {{
      if (status) status.textContent = "Falha ao copiar. (Permissões do browser)";
    }}
  }});
}})();
</script>
""",
                            height=90,
                        )
                    with a2:
                        if chat_url:
                            st.link_button("ABRIR CHAT", chat_url, use_container_width=True)
                        else:
                            st.button("ABRIR CHAT", disabled=True, use_container_width=True, help="Falta `UserId` no Excel.")
                    with a3:
                        st.components.v1.html(
                            f"""
<div>
  <button id="copyopen_{btn_key_base}" style="width:100%; padding:10px 12px; border-radius:10px; border:1px solid rgba(255,255,255,0.15); background: rgba(255,255,255,0.06); color: inherit; cursor:pointer;">
    COPIAR + ABRIR CHAT
  </button>
  <div id="copyopenstatus_{btn_key_base}" style="margin-top:6px; font-size:0.9rem; opacity:0.85;"></div>
</div>
<script>
(function() {{
  const btn = document.getElementById("copyopen_{btn_key_base}");
  const status = document.getElementById("copyopenstatus_{btn_key_base}");
  if (!btn || btn.dataset.bound === "1") return;
  btn.dataset.bound = "1";
  const text = {json.dumps(client_msg)};
  const url = {json.dumps(chat_url)};
  btn.addEventListener("click", async () => {{
    try {{
      await navigator.clipboard.writeText(text);
      if (status) {{
        status.textContent = "Mensagem copiada!";
        setTimeout(() => {{ status.textContent = ""; }}, 2000);
      }}
    }} catch (e) {{
      if (status) status.textContent = "Falha ao copiar. (Permissões do browser)";
    }}
    if (url) window.open(url, "_blank", "noopener,noreferrer");
  }});
}})();
</script>
""",
                            height=90,
                        )
                    with a4:
                        if user_id:
                            st.caption(f"User ID: `{user_id}`")
                        else:
                            st.caption("User ID: —")

                    msg_line = (total_line_template or "").replace("{total}", format_currency(client_total, currency))
                    if msg_line.strip():
                        st.text_area(
                            "Mensagem rápida (copiar)",
                            value=msg_line,
                            height=70,
                            key=f"quick_msg_{client}_{tpl_ver}",
                            disabled=True,
                        )

                    # Display with prices/totals formatted
                    d2 = details.get(client).copy()
                    d2["Preco"] = d2["Preco"].map(lambda v: format_currency(float(v), currency))
                    d2["TotalItem"] = d2["TotalItem"].map(lambda v: format_currency(float(v), currency))
                    st.dataframe(d2, use_container_width=True)

        with tab_messages:
            st.subheader("Mensagens")
            by_client2, details2 = build_summary(merged.dropna(subset=["Preco"]))
            totals_map = {str(r["Cliente"]): float(r["Total"]) for _, r in by_client2.iterrows()}

            allow_edit = st.checkbox("Permitir editar mensagem manualmente", value=False)
            client_selected = st.selectbox(
                "Escolha um cliente",
                options=by_client2["Cliente"].astype(str).tolist(),
            )
            msg = build_message(
                client=client_selected,
                details=details2[client_selected],
                total=totals_map.get(client_selected, float(details2[client_selected]["TotalItem"].sum())),
                currency=currency,
                intro=intro,
                outro=outro,
            )
            tpl_ver = template_version(intro, total_line_template, outro)
            if allow_edit:
                # when user edits manually, keep a stable key
                st.text_area("Mensagem deste cliente", value=msg, height=220, key="single_client_msg_editable")
            else:
                # disabled widgets update correctly when inputs change
                st.text_area(
                    "Mensagem deste cliente",
                    value=msg,
                    height=220,
                    key=f"single_client_msg_{client_selected}_{tpl_ver}",
                    disabled=True,
                )

            st.divider()
            st.subheader("Texto final (todos os clientes)")
            text_blocks: list[str] = []
            for client, d in details2.items():
                text_blocks.append(
                    build_message(
                        client=client,
                        details=d,
                        total=totals_map.get(client, float(d["TotalItem"].sum())),
                        currency=currency,
                        intro=intro,
                        outro=outro,
                    )
                )
            final_text = "\n".join(text_blocks).strip() + "\n"
            st.text_area(
                "Pronto para copiar",
                value=final_text,
                height=320,
                key=f"final_text_{tpl_ver}",
                disabled=True,
            )

        # Auto-save locally (prices + outputs) so closing browser doesn't lose work
        local = load_local_state(STATE_PATH)
        local.setdefault("by_orders_fp", {})
        local["by_orders_fp"][parsed_orders_fp] = {
            "price_overrides": st.session_state["price_overrides"],
            "final_text": final_text if "final_text" in locals() else "",
            "totals_csv": (summary.to_csv(index=False) if "summary" in locals() else ""),
            "last_updated": pd.Timestamp.utcnow().isoformat(),
        }
        save_local_state(STATE_PATH, local)
        st.caption(f"Salvo localmente em `{STATE_PATH}`")

        with tab_messages:
            c1, c2 = st.columns(2)
            with c1:
                st.download_button(
                    "Download texto (.txt)",
                    data=(final_text.encode("utf-8") if "final_text" in locals() else b""),
                    file_name="mensagens_por_pessoa.txt",
                    mime="text/plain",
                )
            with c2:
                if "summary" in locals():
                    csv_bytes = summary.to_csv(index=False).encode("utf-8")
                else:
                    csv_bytes = b""
                st.download_button(
                    "Download resumo (.csv)",
                    data=csv_bytes,
                    file_name="resumo_por_pessoa.csv",
                    mime="text/csv",
                )

        # Export cleaned comments for Excel (incl. optional UserId)
        with tab_upload:
            st.divider()
            st.subheader("Exportar encomendas (para Excel)")
            export_df = orders_for_calc.copy()
            cols_out = ["Cliente"]
            if "UserId" in export_df.columns:
                cols_out.append("UserId")
            cols_out += ["Produto", "Quantidade"]
            export_df = export_df[cols_out].rename(columns={"UserId": "user_id", "Produto": "referencia", "Quantidade": "quantidade"})
            st.download_button(
                "Download encomendas (.csv)",
                data=export_df.to_csv(index=False).encode("utf-8"),
                file_name="encomendas_comments.csv",
                mime="text/csv",
            )

    except Exception as e:
        st.error(str(e))
