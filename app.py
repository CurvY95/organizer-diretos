import base64
import html
import io
import json
import os
import re
from typing import Optional

import pandas as pd
import streamlit as st

from organizer import core as oc
from organizer import db as odb
from organizer import facebook as ofb
from organizer import storage_local as osl
from organizer import sessions_json as osj
from organizer import utils as ou


ORDERS_ALIASES = oc.ORDERS_ALIASES
PRICES_ALIASES = oc.PRICES_ALIASES
REQUIRED_ORDERS_COLS = oc.REQUIRED_ORDERS_COLS
REQUIRED_PRICES_COLS = oc.REQUIRED_PRICES_COLS


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
    orders_df = pd.read_csv(orders_file, encoding="utf-8-sig", encoding_errors="replace")
    prices_df = pd.read_csv(prices_file, encoding="utf-8-sig", encoding_errors="replace")
    return orders_df, prices_df


def _read_csv_bytes_best_effort(raw: bytes, *, sep: str) -> pd.DataFrame:
    """
    Windows/Excel often saves CSVs in cp1252/latin1; Mac edits are more often UTF-8.
    Try a small set of encodings so uploads don't crash with UnicodeDecodeError.
    """
    encodings = ["utf-8-sig", "utf-8", "cp1252", "latin1"]
    last_err: Optional[Exception] = None
    for enc in encodings:
        try:
            return pd.read_csv(
                io.BytesIO(raw),
                sep=sep,
                encoding=enc,
                encoding_errors="replace",
            )
        except Exception as e:
            last_err = e
            continue
    # Fallback: let pandas try (may still fail, but surfaces the real error)
    if last_err:
        raise last_err
    return pd.read_csv(io.BytesIO(raw), sep=sep)


_coerce_number_series = oc.coerce_number_series
_standardize_df_columns = oc.standardize_df_columns
_apply_aliases = oc.apply_aliases
_validate_required_cols = oc.validate_required_cols
_detect_sheet = oc.detect_sheet

parse_inputs = oc.parse_inputs
apply_price_overrides = oc.apply_price_overrides
build_summary = oc.build_summary
format_currency = oc.format_currency
build_message = oc.build_message
stable_orders_fingerprint = oc.stable_orders_fingerprint

load_local_state = osl.load_local_state
save_local_state = osl.save_local_state

FB_PAGE_ID = ofb.FB_PAGE_ID
_normalize_fb_target = ofb.normalize_fb_target
build_facebook_chat_url = ofb.build_facebook_chat_url
build_facebook_profile_url = ofb.build_facebook_profile_url

template_version = ou.template_version


def require_login() -> None:
    """
    Simple access gate so the app isn't public.
    Configure credentials via:
      - Streamlit secrets: AUTH_USER / AUTH_PASS
      - or env vars: ORGANIZER_USER / ORGANIZER_PASS
    """
    # `st.secrets` throws if no secrets.toml exists; treat as empty for local runs.
    try:
        secrets = getattr(st, "secrets", {}) or {}
        # force evaluation (Streamlit secrets is lazy)
        _ = len(secrets) if hasattr(secrets, "__len__") else 0
    except Exception:
        secrets = {}
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
        _logo_lg = _brand_logo_img_html()
        st.markdown(
            f"""
<div class="od-nav">
  {_logo_lg}
  <div class="od-nav-title">Diretos <span style="opacity:0.55;font-weight:700">Pro</span></div>
  <div class="od-nav-sub od-muted">Acesso reservado ao painel</div>
</div>
""",
            unsafe_allow_html=True,
        )
        st.markdown("<div class='od-card-h' style='margin:0.5rem 0 0.35rem'>Credenciais</div>", unsafe_allow_html=True)
        u = st.text_input("Utilizador", key="auth_user", placeholder="O teu utilizador")
        p = st.text_input("Password", type="password", key="auth_pass", placeholder="••••••••")
        do_login = st.button("Entrar no painel", type="primary", width="stretch")
        if do_login:
            if u == expected_user and p == expected_pass:
                st.session_state["authenticated"] = True
                st.session_state.pop("auth_pass", None)
                st.rerun()
            else:
                st.error("Credenciais inválidas.")

    _, mid, _ = st.columns([1, 2.2, 1])
    _bn_login = html.escape(_brand_display_name())
    _lg_login = _brand_logo_img_html()
    with mid:
        st.markdown(
            f"""
<div class="od-hero" style="margin-top:1rem">
  {_lg_login}
  <div class="od-hero-kicker">{_bn_login} <span class="od-badge">Pro</span></div>
  <div class="od-hero-title">Fecha o teu direto com precisão</div>
  <div class="od-hero-sub od-muted">Preços, mensagens por cliente, etiquetas e histórico — num único fluxo comercial. Utiliza o login à esquerda para continuar.</div>
</div>
""",
            unsafe_allow_html=True,
        )
    st.stop()


now_iso = ou.now_iso
safe_session_id = ou.safe_session_id

list_sessions = osj.list_sessions
save_session = osj.save_session
load_session = osj.load_session
delete_session = osj.delete_session


def _db_cache_key() -> str:
    try:
        secrets = getattr(st, "secrets", {}) or {}
        _ = len(secrets) if hasattr(secrets, "__len__") else 0
    except Exception:
        secrets = {}
    url = ""
    schema = ""
    if hasattr(secrets, "get"):
        url = str(secrets.get("DATABASE_URL") or "").strip()
        schema = str(secrets.get("DB_SCHEMA") or "").strip()
    url = url or str(os.getenv("DATABASE_URL") or "").strip()
    schema = schema or str(os.getenv("DB_SCHEMA") or "").strip()
    return f"{schema}|{url}"


@st.cache_resource
def _db_engine_cached(cache_key: str):
    # Liga à BD (Postgres se DATABASE_URL, senão SQLite em `saved/organizer.db`).
    eng = odb.connect()
    odb.init_db(eng)
    return eng


def _db_engine():
    return _db_engine_cached(_db_cache_key())


def _is_postgres_engine(engine) -> bool:
    try:
        return str(engine.url).startswith("postgresql")
    except Exception:
        return False


def _client_ids_from_orders_df(df: pd.DataFrame) -> dict[str, dict[str, str]]:
    if df is None or df.empty:
        return {}
    d = df.copy()
    if "UserId" not in d.columns:
        d["UserId"] = ""
    if "ProfileId" not in d.columns:
        d["ProfileId"] = ""
    out: dict[str, dict[str, str]] = {}
    for _, r in d.drop_duplicates(subset=["Cliente"]).iterrows():
        cliente = str(r["Cliente"]).strip()
        user_id = str(r.get("UserId") or "").strip()
        profile_id = str(r.get("ProfileId") or "").strip()
        if user_id.lower() == "nan":
            user_id = ""
        if profile_id.lower() == "nan":
            profile_id = ""
        out[cliente] = {"user_id": user_id, "profile_id": profile_id}
    return out


def _merge_ids_fill_missing(
    base: dict[str, dict[str, str]],
    *layers: dict[str, dict[str, str]],
) -> dict[str, dict[str, str]]:
    """Preenche user_id/profile_id em falta a partir de camadas (ficheiro tem prioridade)."""
    out = {k: dict(v) for k, v in base.items()}
    for layer in layers:
        for nome, ids in layer.items():
            if nome not in out:
                continue
            cur = out[nome]
            if not (cur.get("user_id") or "").strip() and (ids.get("user_id") or "").strip():
                cur["user_id"] = str(ids.get("user_id") or "").strip()
            if not (cur.get("profile_id") or "").strip() and (ids.get("profile_id") or "").strip():
                cur["profile_id"] = str(ids.get("profile_id") or "").strip()
    return out


def _merged_session_rows() -> list[dict]:
    loc = list_sessions()
    eng = _db_engine()
    try:
        cloud = odb.list_sessions_with_payload(eng)
    except Exception:
        cloud = []
    by_id: dict[str, dict] = {str(s.get("id")): s for s in loc}
    for s in cloud:
        sid = str(s.get("id") or "")
        if sid:
            by_id[sid] = s
    rows = list(by_id.values())
    rows.sort(key=lambda x: str(x.get("created_at") or ""), reverse=True)
    return rows


def _orders_dirty() -> bool:
    try:
        if "orders_working" not in st.session_state or "orders_draft" not in st.session_state:
            return False
        w = st.session_state.get("orders_working")
        c = st.session_state.get("orders_draft")
        if not isinstance(w, pd.DataFrame) or not isinstance(c, pd.DataFrame):
            return False
        # Align common columns and normalize NaNs so equals is stable.
        common = [col for col in c.columns if col in w.columns]
        w2 = w[common].copy()
        c2 = c[common].copy()
        return not w2.fillna("").equals(c2.fillna(""))
    except Exception:
        return False


def _pending_comments_dirty() -> bool:
    return bool(st.session_state.get("pending_comment_clear_clients") or [])


def _prices_dirty() -> bool:
    try:
        draft = st.session_state.get("price_draft")
        if not isinstance(draft, pd.DataFrame):
            return False
        current = st.session_state.get("price_overrides") or {}
        new_overrides: dict[str, float] = {}
        for _, r in draft.iterrows():
            k = oc.normalize_produto_key(r.get("ProdutoKey") or "")
            if not k:
                continue
            v = r.get("Preco")
            if pd.notna(v):
                new_overrides[str(k)] = float(v)
        # Compare dicts with stable key set
        if set(new_overrides.keys()) != set(current.keys()):
            return True
        for k, v in new_overrides.items():
            if float(current.get(k) or 0.0) != float(v):
                return True
        return False
    except Exception:
        return False


def _has_unsaved_changes() -> bool:
    return _orders_dirty() or _prices_dirty() or _pending_comments_dirty()


def _apply_pending_comment_removals_to_df(df: pd.DataFrame) -> pd.DataFrame:
    pending = set([str(x) for x in (st.session_state.get("pending_comment_clear_clients") or [])])
    if not pending:
        return df
    out = df.copy()
    col_comment = "Comentário" if "Comentário" in out.columns else ("Comentario" if "Comentario" in out.columns else "")
    if not col_comment:
        return out
    mask = out["Cliente"].astype(str).isin(list(pending))
    out.loc[mask, col_comment] = ""
    return out


def _save_all_pending_changes() -> None:
    # Orders (also applies pending comment removals if any)
    if "orders_working" in st.session_state and isinstance(st.session_state.get("orders_working"), pd.DataFrame):
        w = st.session_state["orders_working"].copy()
        w = _apply_pending_comment_removals_to_df(w)
        st.session_state["orders_working"] = w
        st.session_state["orders_draft"] = w.copy()
    st.session_state["pending_comment_clear_clients"] = []

    # Prices
    draft = st.session_state.get("price_draft")
    if isinstance(draft, pd.DataFrame):
        new_overrides: dict[str, float] = {}
        for _, r in draft.iterrows():
            k = oc.normalize_produto_key(r.get("ProdutoKey") or "")
            if not k:
                continue
            v = r.get("Preco")
            if pd.notna(v):
                new_overrides[str(k)] = float(v)
        st.session_state["price_overrides"] = new_overrides
        st.session_state["prices_last_saved_at"] = pd.Timestamp.utcnow().isoformat()


def _discard_all_pending_changes() -> None:
    # Orders: drop working edits back to committed
    if "orders_draft" in st.session_state and isinstance(st.session_state.get("orders_draft"), pd.DataFrame):
        st.session_state["orders_working"] = st.session_state["orders_draft"].copy()
    st.session_state["pending_comment_clear_clients"] = []

    # Prices: reset draft back to saved overrides
    draft = st.session_state.get("price_draft")
    if isinstance(draft, pd.DataFrame):
        cur = st.session_state.get("price_overrides") or {}
        d2 = draft.copy()
        d2["ProdutoKey"] = d2["ProdutoKey"].astype(str).map(lambda s: oc.normalize_produto_key(s))
        d2["Preco"] = d2["ProdutoKey"].map(lambda k: cur.get(str(k), None))
        st.session_state["price_draft"] = d2


def _secrets_safe_get(key: str, default: str = "") -> str:
    try:
        secrets = getattr(st, "secrets", {}) or {}
        _ = len(secrets) if hasattr(secrets, "__len__") else 0
    except Exception:
        secrets = {}
    if hasattr(secrets, "get"):
        return str(secrets.get(key) or default).strip()
    return default


def _brand_primary_hex() -> str:
    h = _secrets_safe_get("BRAND_PRIMARY") or str(os.getenv("BRAND_PRIMARY") or "").strip() or "#22d3ee"
    h = h.strip()
    if not h.startswith("#"):
        h = "#" + h
    if len(h) not in (4, 7):
        return "#22d3ee"
    return h


def _hex_to_rgb_tuple(h: str) -> tuple[int, int, int]:
    hx = h.strip().lstrip("#")
    if len(hx) == 3:
        hx = "".join(c * 2 for c in hx)
    if len(hx) != 6:
        return (34, 211, 238)
    try:
        return int(hx[0:2], 16), int(hx[2:4], 16), int(hx[4:6], 16)
    except ValueError:
        return (34, 211, 238)


def _hex_darken(h: str, factor: float = 0.52) -> str:
    r, g, b = _hex_to_rgb_tuple(h)
    r = max(0, min(255, int(r * factor)))
    g = max(0, min(255, int(g * factor)))
    b = max(0, min(255, int(b * factor)))
    return f"#{r:02x}{g:02x}{b:02x}"


def _brand_css_variables_block() -> str:
    hex_c = _brand_primary_hex()
    r, g, b = _hex_to_rgb_tuple(hex_c)
    deep = _hex_darken(hex_c, 0.5)
    return (
        f"  --od-accent: {hex_c};\n"
        f"  --od-accent-deep: {deep};\n"
        f"  --od-accent-rgb: {r},{g},{b};\n"
        f"  --od-accent-dim: rgba({r},{g},{b},0.12);\n"
        f"  --od-accent-border: rgba({r},{g},{b},0.35);\n"
        f"  --od-glow: rgba({r},{g},{b},0.35);\n"
        f"  --od-tab-active: rgba({r},{g},{b},0.14);\n"
        f"  --od-hover-ring: rgba({r},{g},{b},0.45);\n"
        f"  --od-hover-shadow: rgba({r},{g},{b},0.12);\n"
        f"  --od-radial-hero: rgba({r},{g},{b},0.08);\n"
    )


def _brand_logo_data_uri() -> Optional[str]:
    path_env = str(os.getenv("BRAND_LOGO_PATH") or "").strip()
    path_secret = _secrets_safe_get("BRAND_LOGO_PATH")
    candidates = [
        p for p in (path_secret, path_env, os.path.join(os.getcwd(), "assets", "logo.png"), os.path.join(os.getcwd(), "assets", "logo.svg"))
        if p and str(p).strip()
    ]
    for p in candidates:
        try:
            if os.path.isfile(p):
                ext = os.path.splitext(p)[1].lower()
                mime = "image/svg+xml" if ext == ".svg" else "image/jpeg" if ext in (".jpg", ".jpeg") else "image/png"
                with open(p, "rb") as f:
                    b64 = base64.b64encode(f.read()).decode("ascii")
                return f"data:{mime};base64,{b64}"
        except OSError:
            continue
    return None


def _brand_logo_img_html(extra_class: str = "") -> str:
    uri = _brand_logo_data_uri()
    if not uri:
        return ""
    cls = ("od-brand-logo " + extra_class).strip()
    return f'<img class="{cls}" src="{uri}" alt="Logo" />'


def _brand_display_name() -> str:
    return _secrets_safe_get("BRAND_NAME") or "Organizer Diretos"


def _brand_tagline() -> str:
    return _secrets_safe_get("BRAND_TAGLINE") or "Operação comercial pós-live"


def _brand_page_title() -> str:
    name = _brand_display_name()
    return f"{name} · Diretos Pro"


st.set_page_config(page_title=_brand_page_title(), layout="wide", page_icon="✦")

_brand_vars = _brand_css_variables_block()
_OD_COMMERCIAL_CSS = """
<style>
  :root {
___BRAND_VARS___
    --od-font: ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
    --od-line: rgba(148, 163, 184, 0.18);
  }

  html, body, [class*="css"] { font-family: var(--od-font) !important; }

  .stApp {
    background: radial-gradient(1200px 600px at 10% -10%, var(--od-radial-hero), transparent 55%),
                radial-gradient(900px 500px at 100% 0%, rgba(99, 102, 241, 0.10), transparent 50%),
                linear-gradient(180deg, #070b12 0%, #0a0f1a 100%) !important;
  }

  .od-brand-logo {
    max-height: 42px;
    width: auto;
    display: block;
    margin-bottom: 0.55rem;
    object-fit: contain;
    border-radius: 10px;
  }
  .od-brand-logo.od-hero-logo { max-height: 52px; margin-bottom: 0.65rem; }

  .block-container {
    padding-top: 1.25rem;
    padding-bottom: 3rem;
    max-width: 1200px;
  }

  h1, h2, h3 { letter-spacing: -0.03em; font-weight: 700 !important; color: #f8fafc !important; }
  .stCaption, [data-testid="stCaptionContainer"] { color: #94a3b8 !important; }

  .od-muted { color: #94a3b8 !important; font-size: 0.95rem; line-height: 1.5; }
  .od-small { color: #64748b !important; font-size: 0.875rem; line-height: 1.45; }

  .od-hero {
    position: relative;
    overflow: hidden;
    background: linear-gradient(135deg, rgba(15, 23, 42, 0.95) 0%, rgba(30, 41, 59, 0.75) 100%);
    border: 1px solid var(--od-line);
    border-radius: 20px;
    padding: 1.35rem 1.5rem 1.25rem;
    margin-bottom: 0.5rem;
    box-shadow: 0 24px 48px -24px rgba(0, 0, 0, 0.55), 0 0 0 1px rgba(255,255,255,0.03) inset;
  }
  .od-hero::before {
    content: "";
    position: absolute;
    top: 0; left: 0; right: 0; height: 3px;
    background: linear-gradient(90deg, var(--od-accent), #818cf8, #34d399);
    opacity: 0.95;
  }
  .od-hero-kicker {
    display: inline-flex;
    align-items: center;
    gap: 0.5rem;
    font-size: 0.72rem;
    font-weight: 700;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: var(--od-accent) !important;
    margin-bottom: 0.5rem;
  }
  .od-badge {
    display: inline-block;
    padding: 0.15rem 0.5rem;
    border-radius: 999px;
    font-size: 0.65rem;
    font-weight: 800;
    letter-spacing: 0.06em;
    background: var(--od-accent-dim);
    color: #ecfeff !important;
    border: 1px solid var(--od-accent-border);
  }
  .od-hero-title {
    font-weight: 800;
    font-size: clamp(1.35rem, 2.5vw, 1.65rem);
    letter-spacing: -0.04em;
    color: #f8fafc !important;
    line-height: 1.2;
  }
  .od-hero-sub { margin-top: 0.55rem; max-width: 42rem; }

  .od-pill-row { display: flex; flex-wrap: wrap; gap: 0.5rem; margin-top: 1rem; }
  .od-pill {
    font-size: 0.78rem;
    font-weight: 600;
    color: #cbd5e1 !important;
    padding: 0.35rem 0.65rem;
    border-radius: 999px;
    border: 1px solid var(--od-line);
    background: rgba(15, 23, 42, 0.5);
  }

  .od-card {
    background: linear-gradient(165deg, rgba(30, 41, 59, 0.55) 0%, rgba(15, 23, 42, 0.4) 100%);
    border: 1px solid var(--od-line);
    border-radius: 18px;
    padding: 1rem 1.1rem 0.95rem;
    box-shadow: 0 16px 40px -28px rgba(0,0,0,0.5);
  }
  .od-card-h {
    font-size: 0.72rem;
    font-weight: 800;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: #94a3b8 !important;
    margin-bottom: 0.35rem;
  }
  .od-card + .od-card { margin-top: 10px; }

  hr { border: none; border-top: 1px solid var(--od-line); opacity: 1; }

  div.stButton > button, div.stDownloadButton > button, a[data-testid="stLinkButton"] {
    border-radius: 12px !important;
    padding: 0.55rem 1rem !important;
    font-weight: 600 !important;
    border: 1px solid var(--od-line) !important;
    transition: border-color 0.15s ease, box-shadow 0.15s ease, transform 0.12s ease !important;
  }
  div.stButton > button:hover, div.stDownloadButton > button:hover, a[data-testid="stLinkButton"]:hover {
    border-color: var(--od-hover-ring) !important;
    box-shadow: 0 0 0 1px var(--od-hover-shadow) !important;
  }
  div.stButton > button[data-testid="baseButton-primary"],
  button[kind="primary"] {
    background: linear-gradient(135deg, var(--od-accent-deep) 0%, var(--od-accent) 100%) !important;
    color: #042f2e !important;
    border: none !important;
    font-weight: 700 !important;
    box-shadow: 0 8px 24px -8px var(--od-glow) !important;
  }
  div.stButton > button[data-testid="baseButton-primary"]:hover {
    filter: brightness(1.06);
    box-shadow: 0 12px 28px -8px var(--od-glow) !important;
  }

  [data-testid="stDataFrame"], [data-testid="stDataEditor"] {
    border-radius: 14px !important;
    overflow: hidden !important;
    border: 1px solid var(--od-line) !important;
    box-shadow: 0 4px 24px -12px rgba(0,0,0,0.35);
  }

  [data-testid="stTabs"] { margin-top: 8px; }
  [data-testid="stTabs"] [role="tablist"] {
    gap: 0.35rem;
    background: rgba(15, 23, 42, 0.55);
    padding: 0.35rem;
    border-radius: 14px;
    border: 1px solid var(--od-line);
  }
  [data-testid="stTabs"] button[role="tab"] {
    border-radius: 10px !important;
    font-weight: 600 !important;
    padding: 0.45rem 0.85rem !important;
  }
  [data-testid="stTabs"] button[role="tab"][aria-selected="true"] {
    background: var(--od-tab-active) !important;
    color: #ecfeff !important;
  }

  section[data-testid="stSidebar"] > div { padding-top: 1rem; }
  section[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #0c1220 0%, #0a0e17 100%) !important;
    border-right: 1px solid var(--od-line) !important;
  }
  [data-testid="stSidebar"] .stRadio > label { font-weight: 700 !important; font-size: 0.8rem !important; color: #64748b !important; letter-spacing: 0.04em; text-transform: uppercase; }
  [data-testid="stSidebar"] .stRadio label p { font-weight: 600 !important; font-size: 0.95rem !important; color: #e2e8f0 !important; }

  .od-nav {
    background: linear-gradient(165deg, rgba(30, 41, 59, 0.5) 0%, rgba(15, 23, 42, 0.35) 100%);
    border: 1px solid var(--od-line);
    border-radius: 16px;
    padding: 1rem 1rem 0.85rem;
    margin-bottom: 12px;
    box-shadow: 0 12px 32px -20px rgba(0,0,0,0.45);
  }
  .od-nav-title { font-weight: 800; letter-spacing: -0.03em; font-size: 1.08rem; color: #f8fafc !important; }
  .od-nav-sub { margin-top: 0.25rem; font-size: 0.8rem !important; }
  .od-nav ul { list-style: none; padding-left: 0; margin: 10px 0 0 0; }
  .od-nav li { padding: 6px 8px; border-radius: 12px; border: 1px solid rgba(255,255,255,0.08); margin-bottom: 8px; }
  .od-nav li:last-child { margin-bottom: 0; }
  .od-nav li b { font-weight: 750; }

  [data-testid="stExpander"] details {
    border: 1px solid var(--od-line) !important;
    border-radius: 14px !important;
    background: rgba(15, 23, 42, 0.35) !important;
  }

  div[data-testid="stMetric"] {
    background: rgba(15, 23, 42, 0.45);
    border: 1px solid var(--od-line);
    border-radius: 14px;
    padding: 0.65rem 0.75rem;
  }
</style>
"""

st.markdown(_OD_COMMERCIAL_CSS.replace("___BRAND_VARS___", _brand_vars), unsafe_allow_html=True)

require_login()

col_a, col_b = st.columns([3, 2], vertical_alignment="bottom")
_brand_name_h = html.escape(_brand_display_name())
_brand_logo_h = _brand_logo_img_html("od-hero-logo")
with col_a:
    st.markdown(
        f"""
<div class="od-hero">
  {_brand_logo_h}
  <div class="od-hero-kicker">Vendas em direto <span class="od-badge">Pro</span></div>
  <div class="od-hero-title">{_brand_name_h}</div>
  <div class="od-hero-sub od-muted">Do export do Tampermonkey ao cliente final: preços, resumos, mensagens e etiquetas — num fluxo único, com histórico e sessões guardadas.</div>
  <div class="od-pill-row">
    <span class="od-pill">Importar Comments</span>
    <span class="od-pill">Preços &amp; totais</span>
    <span class="od-pill">Mensagens + Facebook</span>
    <span class="od-pill">Etiquetas 10×15</span>
  </div>
</div>
""",
        unsafe_allow_html=True,
    )
with col_b:
    st.markdown(
        """
<div class="od-card">
  <div class="od-card-h">Fluxo recomendado</div>
  <div class="od-muted">1) Encomendas &nbsp;→&nbsp; 2) <b>Guardar preços</b> &nbsp;→&nbsp; 3) Resumo / Mensagens</div>
  <div class="od-small" style="margin-top:10px">No fim, <b>Guardar sessão</b> para recuperar preços e rascunho noutro dia ou noutro PC (com base de dados).</div>
</div>
""",
        unsafe_allow_html=True,
    )

STATE_PATH = os.path.join(os.getcwd(), "saved", "organizer_state.json")

with st.sidebar:
    _logo_sb = _brand_logo_img_html()
    _tag_sb = html.escape(_brand_tagline())
    st.markdown(
        f"""
<div class="od-nav">
  {_logo_sb}
  <div class="od-nav-title">Diretos <span style="opacity:0.55;font-weight:700">Pro</span></div>
  <div class="od-nav-sub od-muted">{_tag_sb}</div>
</div>
""",
        unsafe_allow_html=True,
    )

    NAV_ITEMS = [
        ("Operação", "Operação"),
        ("Preços", "Preços"),
        ("Mensagens", "Mensagens"),
        ("Etiquetas", "Etiquetas"),
        ("Histórico", "Histórico"),
        ("Definições", "Definições"),
    ]
    st.session_state.setdefault("nav_committed", st.session_state.get("nav_page", "Operação"))

    def _on_nav_change():
        new_nav = st.session_state.get("nav_page")
        committed = st.session_state.get("nav_committed")
        if not new_nav or new_nav == committed:
            return
        if _has_unsaved_changes():
            st.session_state["nav_pending"] = new_nav
            # revert selection until user decides
            st.session_state["nav_page"] = committed
            st.session_state["show_unsaved_nav_dialog"] = True
        else:
            st.session_state["nav_committed"] = new_nav

    nav = st.radio(
        "Navegação",
        options=[k for k, _ in NAV_ITEMS],
        index=0,
        label_visibility="collapsed",
        key="nav_page",
        on_change=_on_nav_change,
        format_func=lambda k: {
            "Operação": "Operação",
            "Preços": "Preços",
            "Mensagens": "Mensagens",
            "Etiquetas": "Etiquetas",
            "Histórico": "Histórico",
            "Definições": "Definições",
        }.get(k, k),
    )

    nav_desc = {
        "Operação": "Importa Comments, ajusta encomendas e valida o resumo.",
        "Preços": "Importa/edita preços e aplica ao pedido.",
        "Mensagens": "Gera mensagens por cliente e ações (copiar/abrir chat).",
        "Etiquetas": "Gera etiquetas 10×15 para impressão.",
        "Histórico": "Sessões guardadas (local e/ou nuvem).",
        "Definições": "Moeda e templates das mensagens.",
    }
    st.caption(nav_desc.get(nav, ""))

    st.divider()
    st.markdown("<div class='od-card-h' style='margin-bottom:0.35rem'>Conta</div>", unsafe_allow_html=True)
    if st.session_state.get("authenticated") is True:
        if st.button("Sair"):
            st.session_state["authenticated"] = False
            st.session_state.pop("loaded_session", None)
            st.rerun()

st.divider()

# Unsaved changes modal (navigation guard)
if bool(st.session_state.get("show_unsaved_nav_dialog")):

    @st.dialog("Alterações não guardadas")
    def _unsaved_dialog():
        dirty_bits: list[str] = []
        if _orders_dirty():
            dirty_bits.append("Encomendas")
        if _pending_comments_dirty():
            dirty_bits.append("Comentários (em lote)")
        if _prices_dirty():
            dirty_bits.append("Preços")
        st.write("Tens alterações não guardadas em:")
        st.write(", ".join(dirty_bits) if dirty_bits else "—")
        st.caption("Queres sair desta página sem guardar? Podes guardar agora ou sair sem guardar.")

        c1, c2 = st.columns(2)
        with c1:
            if st.button("Guardar", type="primary", width="stretch"):
                _save_all_pending_changes()
                nxt = st.session_state.get("nav_pending") or st.session_state.get("nav_committed")
                st.session_state["nav_committed"] = nxt
                st.session_state["nav_page"] = nxt
                st.session_state["nav_pending"] = None
                st.session_state["show_unsaved_nav_dialog"] = False
                st.rerun()
        with c2:
            if st.button("Sair sem guardar", type="secondary", width="stretch"):
                _discard_all_pending_changes()
                nxt = st.session_state.get("nav_pending") or st.session_state.get("nav_committed")
                st.session_state["nav_committed"] = nxt
                st.session_state["nav_page"] = nxt
                st.session_state["nav_pending"] = None
                st.session_state["show_unsaved_nav_dialog"] = False
                st.rerun()

    _unsaved_dialog()

# Defaults for "Definições gerais"
st.session_state.setdefault("currency", "EUR")
st.session_state.setdefault("fill_missing_qty", True)
st.session_state.setdefault("intro", "Oi! Segue o resumo da tua encomenda:")
st.session_state.setdefault("total_line_template", "Total a pagar: {total}")
st.session_state.setdefault("outro", "Obrigado!")

currency = st.session_state.get("currency", "EUR")
fill_missing_qty = bool(st.session_state.get("fill_missing_qty", True))
intro = st.session_state.get("intro", "")
total_line_template = st.session_state.get("total_line_template", "")
outro = st.session_state.get("outro", "")

orders_df = None
prices_df = None
orders_source_label = None

if nav == "Definições":
    st.subheader("Definições")
    st.caption("Estas definições aplicam-se ao direto atual e à geração de mensagens/etiquetas.")
    c1, c2 = st.columns([1, 1])
    with c1:
        st.session_state["currency"] = st.selectbox("Moeda", options=["EUR", "BRL", "USD"], index=["EUR", "BRL", "USD"].index(currency))
        st.session_state["fill_missing_qty"] = st.checkbox("Se Quantidade estiver vazia, assumir 1", value=fill_missing_qty)
    with c2:
        st.markdown(
            "<div class='od-card'><div class='od-muted'><b>Dica</b></div><div class='od-small' style='margin-top:6px'>"
            "No Streamlit Cloud, configura <code>DATABASE_URL</code> (Postgres) em segredos para persistir "
            "sessões e IDs — só quando clicas <b>Guardar sessão</b> / <b>Aplicar</b>."
            "</div></div>",
            unsafe_allow_html=True,
        )

    st.divider()
    st.subheader("Mensagens")
    st.session_state["intro"] = st.text_input("Introdução", value=intro)
    st.session_state["total_line_template"] = st.text_area("Linha com total (use {total})", value=total_line_template, height=70)
    st.session_state["outro"] = st.text_input("Fecho", value=outro)

elif nav == "Histórico":
    st.subheader("Histórico de sessões")
    st.caption(
        "Lista **ficheiros locais** (`saved/sessions/`) e **sessões com JSON na base de dados** (quando `DATABASE_URL` está configurado)."
    )
    sessions = _merged_session_rows()
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
        show_df = sessions_df.drop(columns=["Arquivo"]).copy()
        if _is_postgres_engine(_db_engine()):
            show_df.insert(0, "Onde", sessions_df["Arquivo"].map(lambda p: "Nuvem (BD)" if str(p).startswith("db:") else "Local"))
        st.dataframe(show_df, width="stretch")

        chosen = st.selectbox(
            "Abrir sessão",
            options=sessions,
            format_func=lambda s: f"{s['created_at']} — {s['label'] or s['id']}",
        )
        c1, c2 = st.columns([1, 1])
        with c1:
            if st.button("Abrir", type="primary"):
                p = str(chosen.get("path") or "")
                try:
                    if p.startswith("db:"):
                        sid = p.split(":", 1)[1]
                        data = odb.get_session_payload(_db_engine(), session_id=sid) or {}
                    else:
                        data = load_session(p)
                    if not data.get("orders"):
                        st.error("Sessão vazia ou não encontrada.")
                    else:
                        st.session_state["loaded_session"] = data
                        st.success("Sessão carregada. Vá a **Trabalho atual**.")
                except Exception as e:
                    st.error(f"Falha ao abrir: {e}")
        with c2:
            with st.popover("Apagar sessão"):
                st.warning("Isto apaga esta sessão (local e/ou na base de dados). Não dá para recuperar.")
                confirm = st.checkbox("Confirmo que quero apagar", value=False, key="confirm_delete_session")
                if st.button("Apagar definitivamente", type="secondary", disabled=not confirm):
                    try:
                        p = str(chosen.get("path") or "")
                        if p.startswith("db:"):
                            sid = p.split(":", 1)[1]
                            odb.delete_session_by_id(_db_engine(), session_id=sid)
                        else:
                            delete_session(p)
                        loaded = st.session_state.get("loaded_session") or {}
                        if loaded.get("id") == chosen.get("id"):
                            st.session_state.pop("loaded_session", None)
                        st.success("Sessão apagada.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Falha ao apagar: {e}")

else:
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
                    # Tampermonkey export uses ';' delimiter and often includes UTF-8 BOM.
                    raw = uploaded.getvalue()
                    sample = raw[:4096].decode("utf-8-sig", errors="ignore")
                    first_line = (sample.splitlines() or [""])[0]
                    semicolons = first_line.count(";")
                    commas = first_line.count(",")
                    sep = ";" if semicolons > commas else ","
                    orders_df = _read_csv_bytes_best_effort(raw, sep=sep)
                    orders_source_label = f"CSV: {uploaded.name}"
                else:
                    raw = uploaded.getvalue()
                    excel = pd.ExcelFile(io.BytesIO(raw))
                    sheet_names = excel.sheet_names
                    default_orders = _detect_sheet(excel, "orders")
                    default_prices = _detect_sheet(excel, "prices")
                    orders_sheet = st.selectbox(
                        "Aba de comentários / encomendas",
                        options=sheet_names,
                        index=sheet_names.index(default_orders) if default_orders in sheet_names else 0,
                    )
                    prices_sheet = st.selectbox(
                        "Aba de preços (opcional)",
                        options=["(nenhuma)"] + sheet_names,
                        index=(1 + sheet_names.index(default_prices)) if default_prices in sheet_names else 0,
                        help="Se escolher uma aba aqui, os preços do Excel ficam disponíveis para importar automaticamente.",
                    )
                    orders_df = pd.read_excel(excel, sheet_name=orders_sheet)
                    orders_source_label = f"Excel: {uploaded.name} / aba: {orders_sheet}"
                    if prices_sheet and prices_sheet != "(nenhuma)":
                        try:
                            prices_df = pd.read_excel(excel, sheet_name=prices_sheet)
                        except Exception:
                            prices_df = pd.DataFrame(columns=["Produto", "Preco"])
                    else:
                        prices_df = pd.DataFrame(columns=["Produto", "Preco"])
                # CSV flow: we input prices in-app (or via upload on tab 2)
                if name.endswith(".csv"):
                    prices_df = pd.DataFrame(columns=["Produto", "Preco"])
            except Exception as e:
                st.error(f"Erro ao ler o ficheiro: {e}")

if nav in ("Operação", "Preços", "Mensagens", "Etiquetas") and orders_df is not None and prices_df is not None:
    try:
        tab_comments = None
        tab_upload = None
        tab_prices = None
        tab_summary = None
        tab_messages = None
        tab_labels = None

        if nav == "Operação":
            tab_comments, tab_upload, tab_summary = st.tabs(["Comentários", "Encomendas", "Resumo"])
        elif nav == "Preços":
            (tab_prices,) = st.tabs(["Preços"])
        elif nav == "Mensagens":
            (tab_messages,) = st.tabs(["Mensagens"])
        elif nav == "Etiquetas":
            (tab_labels,) = st.tabs(["Etiquetas 10×15"])

        if tab_comments is not None:
            with tab_comments:
                st.subheader("Comentários (texto original)")
                st.caption("Comentários ligados ao rascunho (editar/remover reflete em todo o lado).")

            # Use the shared draft if available (keeps tabs in sync)
            if "orders_draft" in st.session_state and isinstance(st.session_state["orders_draft"], pd.DataFrame):
                draft = st.session_state["orders_draft"].copy()
                # normalize column name for comment if present
                if "Comentário" in draft.columns and "Comentario" not in draft.columns:
                    draft = draft.rename(columns={"Comentário": "Comentario"})
                orders_view = draft.rename(
                    columns={
                        "User ID": "UserId",
                        "Profile ID": "ProfileId",
                        "Referência": "Produto",
                        "Quantidade": "Quantidade",
                        "Hora": "Hora",
                        "Comentario": "Comentario",
                    }
                )
            else:
                orders_view = _standardize_df_columns(orders_df)
                orders_view = _apply_aliases(orders_view, ORDERS_ALIASES)
                _validate_required_cols(orders_view, REQUIRED_ORDERS_COLS, "Encomendas (Comments)")

            cols = ["Cliente"]
            if "UserId" in orders_view.columns:
                cols.append("UserId")
            if "ProfileId" in orders_view.columns:
                cols.append("ProfileId")
            if "Hora" in orders_view.columns:
                cols.append("Hora")
            cols += ["Produto", "Quantidade"]
            if "Comentario" in orders_view.columns:
                cols.append("Comentario")

            view = orders_view[cols].copy()
            view["Cliente"] = view["Cliente"].astype(str).str.strip()
            view["Produto"] = view["Produto"].astype(str).str.strip()
            if "Comentario" in view.columns:
                view["Comentario"] = view["Comentario"].astype(str)

            if "Comentario" not in view.columns:
                st.info("Não encontrei coluna de comentário no ficheiro. (Procurei: Comentário/Comment/Mensagem/OBS/Notas)")
            else:
                clients = sorted(view["Cliente"].dropna().astype(str).unique().tolist())
                client_c = st.selectbox("Cliente", options=clients, key="comments_client_pick")
                vc = view[view["Cliente"].astype(str) == str(client_c)].copy()

                # Aggregate comments (some exports repeat per row)
                raw_comments = (
                    vc["Comentario"]
                    .dropna()
                    .astype(str)
                    .map(lambda s: s.strip())
                    .replace({"nan": "", "None": ""})
                )
                raw_comments = [c for c in raw_comments.tolist() if c]
                combined = "\n\n---\n\n".join(dict.fromkeys(raw_comments)).strip()

                st.text_area("Comentário(s)", value=combined or "—", height=200, disabled=True, key="comments_text")
                st.dataframe(vc, width="stretch")

                # Batch mode: mark many clients, apply once
                st.session_state.setdefault("pending_comment_clear_clients", [])
                pending = set([str(x) for x in (st.session_state.get("pending_comment_clear_clients") or [])])
                if client_c:
                    pending_hint = " (já marcado)" if str(client_c) in pending else ""
                else:
                    pending_hint = ""

                cbtn1, cbtn2, cbtn3 = st.columns([1, 1, 2])
                with cbtn1:
                    add_pending = st.button(
                        f"Marcar para remover{pending_hint}",
                        type="secondary",
                        key="mark_remove_comments_client",
                        disabled=not bool(client_c),
                    )
                with cbtn2:
                    clear_pending = st.button(
                        "Limpar lista",
                        type="secondary",
                        key="clear_remove_comments_list",
                        disabled=not bool(pending),
                    )
                with cbtn3:
                    apply_pending = st.button(
                        "Aplicar alterações (remover comentários)",
                        type="primary",
                        key="apply_remove_comments_list",
                        disabled=not bool(pending),
                        help="Aplica todas as remoções de uma vez (evita refresh/recalcular a cada clique).",
                    )

                if add_pending:
                    pending.add(str(client_c))
                    st.session_state["pending_comment_clear_clients"] = sorted(pending)

                if clear_pending:
                    st.session_state["pending_comment_clear_clients"] = []
                    pending = set()

                if pending:
                    st.caption(f"Marcados: **{len(pending)}** cliente(s) para remover comentários.")

                if apply_pending:
                    if "orders_draft" in st.session_state and isinstance(st.session_state["orders_draft"], pd.DataFrame):
                        od = st.session_state["orders_draft"].copy()
                        col_comment = "Comentário" if "Comentário" in od.columns else ("Comentario" if "Comentario" in od.columns else "")
                        if not col_comment:
                            st.warning("Não existe coluna de comentário no rascunho.")
                        else:
                            mask = od["Cliente"].astype(str).isin(list(pending))
                            od.loc[mask, col_comment] = ""
                            st.session_state["orders_draft"] = od
                            st.session_state["pending_comment_clear_clients"] = []
                            st.success("Alterações aplicadas.")
                            st.rerun()
                    else:
                        st.warning("Abra a aba 'Encomendas' para criar o rascunho antes de aplicar.")

                st.caption("Dica: marque vários clientes e aplique no fim. Assim não 'saltas' de refresh a cada remoção.")

        if tab_upload is not None:
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
                if "ProfileId" in orders_edit.columns:
                    ui_cols.append("ProfileId")
                if "Hora" in orders_edit.columns:
                    ui_cols.append("Hora")
                if "Comentario" in orders_edit.columns:
                    ui_cols.append("Comentario")
                ui_cols += ["Produto", "Quantidade"]

                orders_edit = orders_edit[ui_cols].copy()
                orders_edit = orders_edit.rename(
                    columns={
                        "Cliente": "Cliente",
                        "UserId": "User ID",
                        "ProfileId": "Profile ID",
                        "Hora": "Hora",
                        "Comentario": "Comentário",
                        "Produto": "Referência",
                        "Quantidade": "Quantidade",
                    }
                )
                # Soft-delete / exclude rows from totals
                if "Incluir" not in orders_edit.columns:
                    orders_edit["Incluir"] = True

                # Keep one shared draft across tabs/pages
                if (
                    st.session_state.get("orders_draft_source") != (orders_source_label or "")
                    or "orders_draft" not in st.session_state
                ):
                    st.session_state["orders_draft_source"] = (orders_source_label or "")
                    st.session_state["orders_draft"] = orders_edit.copy()
                # Committed vs working edits (SaaS-style: apply at the end)
                committed = st.session_state["orders_draft"].copy()
                committed["Quantidade"] = _coerce_number_series(committed["Quantidade"])
                if fill_missing_qty:
                    committed["Quantidade"] = committed["Quantidade"].fillna(1.0)

                if "orders_working" not in st.session_state or st.session_state.get("orders_working_source") != (
                    orders_source_label or ""
                ):
                    st.session_state["orders_working_source"] = (orders_source_label or "")
                    st.session_state["orders_working"] = committed.copy()

                col_cfg = {
                    "Incluir": st.column_config.CheckboxColumn("Incluir", help="Se desativar, esta linha não entra nas contas."),
                    "Cliente": st.column_config.TextColumn("Cliente"),
                    "Hora": st.column_config.TextColumn("Hora", disabled=True),
                    "Referência": st.column_config.TextColumn("Referência", disabled=True),
                    "Quantidade": st.column_config.NumberColumn("Quantidade", min_value=0.0, step=0.5, format="%.3g"),
                }
                if "Comentário" in committed.columns:
                    col_cfg["Comentário"] = st.column_config.TextColumn("Comentário", help="Editar/apagar aqui reflete em todo o lado.")
                if "User ID" in orders_edit.columns:
                    col_cfg["User ID"] = st.column_config.TextColumn("User ID", disabled=True)

                if "Profile ID" in orders_edit.columns:
                    col_cfg["Profile ID"] = st.column_config.TextColumn("Profile ID", disabled=True)

                st.markdown("<div class='od-card-h' style='margin-top:6px'>Edição (aplicar no fim)</div>", unsafe_allow_html=True)
                st.caption("Edite à vontade. Clique **Aplicar alterações** para atualizar o resumo/mensagens/etiquetas de uma vez.")

                with st.form("orders_form", border=False):
                    edited_orders = st.data_editor(
                        st.session_state["orders_working"],
                        width="stretch",
                        num_rows="fixed",
                        column_config={
                            **col_cfg,
                        },
                        key="comments_editor",
                    )
                    b1, b2, _ = st.columns([1, 1, 2])
                    with b1:
                        do_apply_orders = st.form_submit_button("Aplicar alterações", type="primary")
                    with b2:
                        do_reset_orders = st.form_submit_button("Repor (voltar ao guardado)")

                # Keep working state updated only inside the form flow
                st.session_state["orders_working"] = edited_orders.copy()

                if do_reset_orders:
                    st.session_state["orders_working"] = committed.copy()
                    st.success("Rascunho reposto.")
                    st.rerun()

                if do_apply_orders:
                    st.session_state["orders_draft"] = edited_orders.copy()
                    committed = st.session_state["orders_draft"].copy()
                    st.success("Alterações aplicadas.")
                    st.rerun()

                # Convert committed orders back to expected input shape (calculations use committed only)
                orders_for_calc = committed.rename(
                    columns={
                        "Referência": "Produto",
                        "User ID": "UserId",
                        "Profile ID": "ProfileId",
                        "Comentário": "Comentario",
                    }
                ).copy()
                if "Incluir" in orders_for_calc.columns:
                    orders_for_calc = orders_for_calc[orders_for_calc["Incluir"].fillna(True)].copy()
                    orders_for_calc = orders_for_calc.drop(columns=["Incluir"], errors="ignore")

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
                        eng = _db_engine()
                        local_st = load_local_state(STATE_PATH)
                        local_dir = local_st.get("client_ids") or {}
                        if not isinstance(local_dir, dict):
                            local_dir = {}
                        base_ids = _client_ids_from_orders_df(orders_for_calc)
                        bulk: dict[str, dict[str, str]] = {}
                        try:
                            bulk = odb.get_customer_ids_bulk(eng, clientes=list(base_ids.keys()))
                        except Exception:
                            bulk = {}
                        merged_ids = _merge_ids_fill_missing(base_ids, local_dir, bulk)
                        sid, payload = save_session(
                            label=session_label.strip(),
                            orders_for_calc=orders_for_calc,
                            price_overrides=st.session_state.get("price_overrides") or {},
                            meta={"source": orders_source_label or ""},
                        )
                        try:
                            odb.save_session_payload(eng, payload)
                        except Exception as e:
                            st.warning(f"Não gravei a sessão na base de dados: {e}")
                        rows_up: list[tuple[str, str, str]] = []
                        for nome, ids in merged_ids.items():
                            u = (ids.get("user_id") or "").strip()
                            p = (ids.get("profile_id") or "").strip()
                            if u or p:
                                rows_up.append((str(nome).strip(), u, p))
                        if rows_up:
                            try:
                                odb.upsert_customer_ids_bulk(eng, rows=rows_up)
                            except Exception as e:
                                st.warning(f"Não atualizei os IDs na base de dados: {e}")
                        try:
                            local_st2 = load_local_state(STATE_PATH)
                            local_st2.setdefault("client_ids", {})
                            for nome, ids in merged_ids.items():
                                k = str(nome).strip()
                                if not k:
                                    continue
                                u = (ids.get("user_id") or "").strip()
                                p = (ids.get("profile_id") or "").strip()
                                if not u and not p:
                                    continue
                                prev = local_st2["client_ids"].get(k) or {}
                                out_u = str(prev.get("user_id") or "").strip()
                                out_p = str(prev.get("profile_id") or "").strip()
                                if u:
                                    out_u = u
                                if p:
                                    out_p = p
                                local_st2["client_ids"][k] = {"user_id": out_u, "profile_id": out_p}
                            save_local_state(STATE_PATH, local_st2)
                        except Exception:
                            pass
                        st.success(f"Sessão guardada: {sid} (ficheiro local + base de dados, se configurada).")

        parsed = parse_inputs(
            orders_for_calc,
            prices_df,
            fill_missing_quantity_with=1.0 if fill_missing_qty else None,
        )
        if "price_overrides" not in st.session_state:
            st.session_state["price_overrides"] = {}

        parsed_orders_fp = stable_orders_fingerprint(parsed.orders)
        local = load_local_state(STATE_PATH)
        eng_ids = _db_engine()
        if st.session_state.get("_ids_bulk_fp") != parsed_orders_fp:
            st.session_state["_ids_bulk_fp"] = parsed_orders_fp
            try:
                client_names_bulk = parsed.orders["Cliente"].dropna().astype(str).str.strip().unique().tolist()
                st.session_state["_ids_bulk_map"] = odb.get_customer_ids_bulk(eng_ids, clientes=list(client_names_bulk))
            except Exception:
                st.session_state["_ids_bulk_map"] = {}
        saved_by_fp = (local.get("by_orders_fp") or {}).get(parsed_orders_fp) or {}
        # Nota: preços podem mudar de direto para direto. Não reutilizamos automaticamente preços
        # guardados localmente; apenas dentro de sessões carregadas/guardadas.

        # Full price table for all references present in orders
        price_table = (
            parsed.orders[["ProdutoKey", "Produto"]]
            .drop_duplicates()
            .sort_values(["ProdutoKey"])
            .reset_index(drop=True)
        )
        price_table["Preco"] = price_table["ProdutoKey"].map(st.session_state["price_overrides"])

        # If the uploaded XLSX included a prices sheet, prefill draft (optional).
        try:
            prices_std = _standardize_df_columns(prices_df) if prices_df is not None else pd.DataFrame()
            prices_std = _apply_aliases(prices_std, PRICES_ALIASES) if not prices_std.empty else prices_std
            if (not prices_std.empty) and ("Produto" in prices_std.columns) and ("Preco" in prices_std.columns):
                excel_map: dict[str, float] = {}
                tmpx = prices_std[["Produto", "Preco"]].copy()
                tmpx["Produto"] = tmpx["Produto"].astype(str).map(lambda s: s.strip())
                tmpx["ProdutoKey"] = tmpx["Produto"].map(lambda s: oc.normalize_produto_key(s))
                tmpx["Preco"] = _coerce_number_series(tmpx["Preco"])
                tmpx = tmpx.dropna(subset=["ProdutoKey", "Preco"])
                tmpx = tmpx[tmpx["ProdutoKey"].astype(str).str.strip() != ""]
                for _, r in tmpx.drop_duplicates(subset=["ProdutoKey"], keep="last").iterrows():
                    excel_map[str(r["ProdutoKey"])] = float(r["Preco"])

                if excel_map:
                    st.session_state["excel_prices_map"] = excel_map
        except Exception:
            pass

        if tab_prices is not None:
            with tab_prices:
                st.subheader("Preços")
                st.caption("Edite tudo e clique em **Guardar preços** no final. Antes de guardar, as outras abas não mudam.")

            st.markdown("<div class='od-card-h'>Upload de preços</div>", unsafe_allow_html=True)
            st.caption("Formato esperado: colunas tipo `Referencia` + `preços/precos` (CSV ou Excel). Pode conter referências a mais.")

            if bool(st.session_state.get("excel_prices_map")):
                with st.container(border=False):
                    cmap = st.session_state.get("excel_prices_map") or {}
                    order_keys_set = set(price_table["ProdutoKey"].astype(str).tolist())
                    matched = len(order_keys_set.intersection(set(cmap.keys())))
                    st.caption(f"Detetei preços no Excel: **{len(cmap)}** referência(s) · No pedido: **{matched}**")
                    cx1, cx2, _ = st.columns([1, 1, 2])
                    with cx1:
                        do_excel_draft = st.button("Usar preços do Excel (rascunho)", type="secondary", key="excel_prices_to_draft")
                    with cx2:
                        do_excel_save = st.button("Usar preços do Excel (guardar)", type="primary", key="excel_prices_to_save")
                    if do_excel_draft or do_excel_save:
                        excel_prices_map = st.session_state.get("excel_prices_map") or {}
                        draft = st.session_state.get("price_draft") or price_table[["Produto", "ProdutoKey", "Preco"]].copy()
                        draft = draft.copy()
                        draft["ProdutoKey"] = draft["ProdutoKey"].astype(str).map(lambda s: oc.normalize_produto_key(s))
                        draft["Preco"] = draft["ProdutoKey"].map(lambda k: excel_prices_map.get(str(k), None)).combine_first(
                            draft["Preco"]
                        )
                        st.session_state["price_draft"] = draft
                        if do_excel_save:
                            new_overrides: dict[str, float] = {}
                            for _, r in draft.iterrows():
                                k = oc.normalize_produto_key(r.get("ProdutoKey") or "")
                                if not k:
                                    continue
                                v = r.get("Preco")
                                if pd.notna(v):
                                    new_overrides[str(k)] = float(v)
                            st.session_state["price_overrides"] = new_overrides
                            st.session_state["prices_last_saved_at"] = pd.Timestamp.utcnow().isoformat()
                            st.success("Preços do Excel guardados. As outras abas já usam estes valores.")
                        else:
                            st.success("Preços do Excel aplicados ao rascunho. Clique em **Guardar preços** para aplicar.")
                        st.rerun()

            prices_upload = st.file_uploader(
                "Upload preços (.csv ou .xlsx)",
                type=["csv", "xlsx"],
                key="prices_upload_file",
                help="Ex.: Referencia | precos",
            )

            def _read_prices_upload(uploaded) -> pd.DataFrame:
                name = (uploaded.name or "").lower()
                raw = uploaded.getvalue()
                if name.endswith(".csv"):
                    sample = raw[:4096].decode("utf-8-sig", errors="ignore")
                    first_line = (sample.splitlines() or [""])[0]
                    semicolons = first_line.count(";")
                    commas = first_line.count(",")
                    sep = ";" if semicolons > commas else ","
                    return _read_csv_bytes_best_effort(raw, sep=sep)
                # xlsx
                excel = pd.ExcelFile(io.BytesIO(raw))
                sheet_names = excel.sheet_names
                sheet = st.selectbox(
                    "Aba de preços (Excel)",
                    options=sheet_names,
                    index=0,
                    key="prices_upload_sheet_pick",
                )
                return pd.read_excel(excel, sheet_name=sheet)

            def _infer_prices_columns(df: pd.DataFrame) -> tuple[str, str]:
                cols = [str(c) for c in df.columns]
                norm = {c: re.sub(r"[^a-z0-9]+", "", str(c).strip().lower()) for c in cols}
                ref_candidates = {"referencia", "referência", "ref", "produto", "product", "sku"}
                price_candidates = {"preco", "preço", "precos", "preços", "price", "valor", "unitprice"}

                ref_col = ""
                price_col = ""
                for c, n in norm.items():
                    if n in {re.sub(r"[^a-z0-9]+", "", x.lower()) for x in ref_candidates}:
                        ref_col = c
                        break
                for c, n in norm.items():
                    if n in {re.sub(r"[^a-z0-9]+", "", x.lower()) for x in price_candidates}:
                        price_col = c
                        break
                if not ref_col:
                    ref_col = cols[0] if cols else ""
                if not price_col:
                    # try second column fallback
                    price_col = cols[1] if len(cols) > 1 else ""
                return ref_col, price_col

            uploaded_prices_map: dict[str, float] = {}
            uploaded_rows = 0
            uploaded_extra = 0
            uploaded_matched = 0
            if prices_upload is not None:
                try:
                    up_df = _read_prices_upload(prices_upload)
                    if up_df is None or up_df.empty:
                        st.warning("Ficheiro de preços vazio.")
                    else:
                        ref_col, price_col = _infer_prices_columns(up_df)
                        if not ref_col or not price_col or ref_col not in up_df.columns or price_col not in up_df.columns:
                            st.error("Não consegui detetar as colunas `Referencia` e `preços/precos` no ficheiro.")
                        else:
                            tmpu = up_df[[ref_col, price_col]].copy()
                            tmpu = tmpu.rename(columns={ref_col: "Referencia", price_col: "Preco"})
                            tmpu["Referencia"] = tmpu["Referencia"].astype(str).map(lambda s: s.strip())
                            tmpu["ProdutoKey"] = tmpu["Referencia"].map(lambda s: oc.normalize_produto_key(s))
                            tmpu["Preco"] = _coerce_number_series(tmpu["Preco"])
                            tmpu = tmpu.dropna(subset=["ProdutoKey", "Preco"])
                            tmpu = tmpu[tmpu["ProdutoKey"].astype(str).str.strip() != ""]
                            uploaded_rows = int(tmpu.shape[0])
                            uploaded_prices_map = {
                                str(r["ProdutoKey"]): float(r["Preco"])
                                for _, r in tmpu.drop_duplicates(subset=["ProdutoKey"], keep="last").iterrows()
                            }

                            order_keys_set = set(price_table["ProdutoKey"].astype(str).tolist())
                            uploaded_keys_set = set(uploaded_prices_map.keys())
                            uploaded_matched = len(order_keys_set.intersection(uploaded_keys_set))
                            uploaded_extra = len(uploaded_keys_set.difference(order_keys_set))

                            st.caption(
                                f"Importado: **{len(uploaded_prices_map)}** referência(s) · "
                                f"No pedido: **{uploaded_matched}** · A mais: **{uploaded_extra}**"
                            )
                except Exception as e:
                    st.error(f"Falha ao ler preços: {e}")

            if "price_draft" not in st.session_state:
                st.session_state["price_draft"] = price_table[["Produto", "ProdutoKey", "Preco"]].copy()

            cimp1, cimp2, _ = st.columns([1, 1, 2])
            with cimp1:
                do_import_draft = st.button(
                    "Importar p/ rascunho",
                    type="secondary",
                    disabled=not bool(uploaded_prices_map),
                    help="Preenche o editor de preços (ainda não aplica ao resumo até guardares).",
                )
            with cimp2:
                do_import_and_save = st.button(
                    "Importar e Guardar preços",
                    type="primary",
                    disabled=not bool(uploaded_prices_map),
                    help="Aplica já ao resumo/mensagens/etiquetas.",
                )

            if do_import_draft or do_import_and_save:
                # Merge uploaded prices into the draft table (only keys that exist in current order)
                draft = st.session_state["price_draft"].copy()
                draft["ProdutoKey"] = draft["ProdutoKey"].astype(str).map(lambda s: oc.normalize_produto_key(s))
                draft["Preco"] = draft["ProdutoKey"].map(lambda k: uploaded_prices_map.get(str(k), None)).combine_first(
                    draft["Preco"]
                )
                st.session_state["price_draft"] = draft

                if do_import_and_save:
                    new_overrides: dict[str, float] = {}
                    for _, r in draft.iterrows():
                        k = oc.normalize_produto_key(r.get("ProdutoKey") or "")
                        if not k:
                            continue
                        v = r.get("Preco")
                        if pd.notna(v):
                            new_overrides[str(k)] = float(v)
                    st.session_state["price_overrides"] = new_overrides
                    st.session_state["prices_last_saved_at"] = pd.Timestamp.utcnow().isoformat()
                    st.success("Preços importados e guardados. As outras abas já usam estes valores.")
                else:
                    st.success("Preços importados para o rascunho. Clique em **Guardar preços** para aplicar.")
                st.rerun()

            with st.form("prices_form", border=False):
                edited = st.data_editor(
                    st.session_state["price_draft"],
                    width="stretch",
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
                new_overrides: dict[str, float] = {}
                for _, r in edited.iterrows():
                    k = oc.normalize_produto_key(r.get("ProdutoKey") or "")
                    if not k:
                        continue
                    v = r.get("Preco")
                    if pd.notna(v):
                        new_overrides[k] = float(v)
                st.session_state["price_overrides"] = new_overrides
                st.session_state["prices_last_saved_at"] = pd.Timestamp.utcnow().isoformat()
                st.success("Preços guardados. As outras abas já usam estes valores.")
                st.rerun()

            saved_count = len(st.session_state.get("price_overrides") or {})
            last_saved = st.session_state.get("prices_last_saved_at") or ""
            st.caption(f"Guardados: **{saved_count}** referência(s)" + (f" · Último save: `{last_saved}`" if last_saved else ""))

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

        # ID estável reutilizado quando abres uma sessão guardada.
        if "history_session_id" not in st.session_state:
            loaded = st.session_state.get("loaded_session") or {}
            st.session_state["history_session_id"] = loaded.get("id") or safe_session_id(now_iso())

        still_missing = merged[merged["Preco"].isna()][["ProdutoKey", "Produto"]].drop_duplicates()
        if not still_missing.empty:
            st.info(
                f"Ainda faltam preços para {len(still_missing)} referência(s). "
                "Preencha na aba '2) Preços' para liberar o resumo."
            )
            with st.expander("Diagnóstico preços", expanded=False):
                try:
                    order_keys = set(merged["ProdutoKey"].dropna().astype(str).tolist())
                    override_keys = set((st.session_state.get("price_overrides") or {}).keys())
                    st.write(
                        {
                            "refs_no_pedido": len(order_keys),
                            "refs_com_preco_guardado": len(override_keys),
                            "refs_que_casaram": len(order_keys.intersection(override_keys)),
                            "exemplos_em_falta": still_missing.head(10).to_dict(orient="records"),
                        }
                    )
                except Exception as e:
                    st.write(f"Falha no diagnóstico: {e}")

        by_client, details = build_summary(merged.dropna(subset=["Preco"]))
        file_ids_map: dict[str, dict[str, str]] = {}

        tmp_cols = ["Cliente"]
        if "UserId" in parsed.orders.columns:
            tmp_cols.append("UserId")
        if "ProfileId" in parsed.orders.columns:
            tmp_cols.append("ProfileId")

        tmp = parsed.orders[tmp_cols].copy()
        tmp["Cliente"] = tmp["Cliente"].astype(str)

        if "UserId" in tmp.columns:
            tmp["UserId"] = tmp["UserId"].astype(str).str.strip()
        else:
            tmp["UserId"] = ""

        if "ProfileId" in tmp.columns:
            tmp["ProfileId"] = tmp["ProfileId"].astype(str).str.strip()
        else:
            tmp["ProfileId"] = ""

        for _, r in tmp.drop_duplicates(subset=["Cliente"]).iterrows():
            cliente = str(r["Cliente"]).strip()
            user_id = str(r["UserId"]).strip()
            profile_id = str(r["ProfileId"]).strip()

            if user_id.lower() == "nan":
                user_id = ""
            if profile_id.lower() == "nan":
                profile_id = ""

            file_ids_map[cliente] = {
                "user_id": user_id,
                "profile_id": profile_id,
            }

        local_dir_ids = local.get("client_ids") or {}
        if not isinstance(local_dir_ids, dict):
            local_dir_ids = {}
        _bulk_ids_layer = st.session_state.get("_ids_bulk_map") or {}
        client_ids_map = _merge_ids_fill_missing(file_ids_map, local_dir_ids, _bulk_ids_layer)

        # Aprende IDs novos do ficheiro → `organizer_state.json` (SQLite/local). Com Postgres não gravamos a cada rerun.
        try:
            local_dir = dict(local_dir_ids)
            dir_changed = False
            for nome, ids in file_ids_map.items():
                key = str(nome).strip()
                saved = local_dir.get(key) or {}
                if not isinstance(saved, dict):
                    saved = {}

                new_user = str(ids.get("user_id") or "").strip()
                new_profile = str(ids.get("profile_id") or "").strip()
                prev_user = str(saved.get("user_id") or "").strip()
                prev_profile = str(saved.get("profile_id") or "").strip()

                updated = False
                out_user = prev_user
                out_profile = prev_profile
                if new_user and new_user != prev_user:
                    out_user = new_user
                    updated = True
                if new_profile and new_profile != prev_profile:
                    out_profile = new_profile
                    updated = True
                if updated:
                    local_dir[key] = {"user_id": out_user, "profile_id": out_profile}
                    dir_changed = True

            if dir_changed and not _is_postgres_engine(eng_ids):
                local["client_ids"] = local_dir
                save_local_state(STATE_PATH, local)
        except Exception:
            pass

        if tab_summary is not None:
            with tab_summary:
                st.subheader("Resumo")
                priced_rows = int(merged["Preco"].notna().sum()) if "Preco" in merged.columns else 0
                total_rows = int(merged.shape[0]) if merged is not None else 0
                saved_prices = len(st.session_state.get("price_overrides") or {})
                if priced_rows == 0:
                    st.warning(
                        f"Sem linhas com preço aplicado ainda. "
                        f"(Linhas: {total_rows} · Com preço: {priced_rows} · Preços guardados: {saved_prices})"
                    )
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
                st.dataframe(summary_display, width="stretch")

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
                    ids = client_ids_map.get(client, {})
                    user_id = ids.get("user_id", "")
                    profile_id = ids.get("profile_id", "")
                    # Só abrimos chat direto quando temos UserId.
                    chat_url = build_facebook_chat_url(user_id=user_id, profile_id="")
                    profile_url = build_facebook_profile_url(user_id=user_id, profile_id=profile_id)
                    inbox_base_url = f"https://business.facebook.com/latest/inbox/all/?asset_id={FB_PAGE_ID}&mailbox_id={FB_PAGE_ID}"
                    chat_or_inbox_url = chat_url or inbox_base_url

                    st.markdown("<div class='od-muted' style='margin-top:8px'><b>Ações</b></div>", unsafe_allow_html=True)
                    a1, a2, a3, a4, a5 = st.columns([1.2, 1.1, 1.1, 1.5, 1.8])
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
                            st.link_button(
                                "ABRIR CHAT",
                                chat_or_inbox_url,
                                width="stretch",
                                key=f"open_chat_{btn_key_base}",
                            )
                        else:
                            st.link_button(
                                "ABRIR INBOX",
                                chat_or_inbox_url,
                                width="stretch",
                                help="Sem UserID/ProfileID; abre o Inbox da página para pesquisar pelo nome.",
                                key=f"open_inbox_{btn_key_base}",
                            )
                    with a3:
                        if profile_url:
                            st.link_button(
                                "ABRIR PERFIL",
                                profile_url,
                                width="stretch",
                                key=f"open_profile_{btn_key_base}",
                            )
                        else:
                            st.link_button(
                                "ABRIR PERFIL",
                                "about:blank",
                                width="stretch",
                                disabled=True,
                                key=f"open_profile_disabled_{btn_key_base}",
                            )
                    with a4:
                        if chat_url:
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
  const url = {json.dumps(chat_or_inbox_url)};
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
                        else:
                            st.link_button(
                                "COPIAR + ABRIR CHAT",
                                "about:blank",
                                width="stretch",
                                disabled=True,
                                key=f"copyopen_disabled_{btn_key_base}",
                            )
                    with a5:
                        if user_id and profile_id:
                            st.caption(f"User ID: `{user_id}` | Profile ID: `{profile_id}`")
                        elif user_id:
                            st.caption(f"User ID: `{user_id}`")
                        elif profile_id:
                            st.caption(f"Profile ID: `{profile_id}`")
                        else:
                            st.caption("User/Profile ID: —")

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
                    st.dataframe(d2, width="stretch")

        if tab_messages is not None:
            with tab_messages:
                st.subheader("Mensagens")
                by_client2, details2 = build_summary(merged.dropna(subset=["Preco"]))
                if not details2:
                    priced_rows = int(merged["Preco"].notna().sum()) if "Preco" in merged.columns else 0
                    saved_prices = len(st.session_state.get("price_overrides") or {})
                    st.warning(
                        f"Sem mensagens ainda porque não há preços aplicados. "
                        f"(Com preço: {priced_rows} · Preços guardados: {saved_prices})"
                    )
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

            # Ações (na aba de Mensagens): copiar / abrir chat / copiar+abrir
            ids = client_ids_map.get(client_selected, {}) if "client_ids_map" in locals() else {}
            user_id = (ids.get("user_id") or "").strip()
            profile_id = (ids.get("profile_id") or "").strip()
            # Só abrimos chat direto quando temos UserId.
            chat_url = build_facebook_chat_url(user_id=user_id, profile_id="")
            profile_url = build_facebook_profile_url(user_id=user_id, profile_id=profile_id)
            inbox_base_url = f"https://business.facebook.com/latest/inbox/all/?asset_id={FB_PAGE_ID}&mailbox_id={FB_PAGE_ID}"
            chat_or_inbox_url = chat_url or inbox_base_url

            msg_to_copy = (
                str(st.session_state.get("single_client_msg_editable") or msg)
                if allow_edit
                else msg
            )

            st.markdown("<div class='od-muted' style='margin-top:8px'><b>Ações</b></div>", unsafe_allow_html=True)
            m1, m2, m3, m4 = st.columns([1.2, 1.1, 1.1, 1.5])
            msg_btn_key_base = f"msgtab_{client_selected}_{tpl_ver}"
            with m1:
                st.components.v1.html(
                    f"""
<div>
  <button id="copy_msgtab_{msg_btn_key_base}" style="width:100%; padding:10px 12px; border-radius:10px; border:1px solid rgba(255,255,255,0.15); background: rgba(255,255,255,0.06); color: inherit; cursor:pointer;">
    COPIAR MENSAGEM
  </button>
  <div id="copystatus_msgtab_{msg_btn_key_base}" style="margin-top:6px; font-size:0.9rem; opacity:0.85;"></div>
</div>
<script>
(function() {{
  const btn = document.getElementById("copy_msgtab_{msg_btn_key_base}");
  const status = document.getElementById("copystatus_msgtab_{msg_btn_key_base}");
  if (!btn || btn.dataset.bound === "1") return;
  btn.dataset.bound = "1";
  const text = {json.dumps(msg_to_copy)};
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
            with m2:
                if chat_url:
                    st.link_button(
                        "ABRIR CHAT",
                        chat_or_inbox_url,
                        width="stretch",
                        key=f"open_chat_msgtab_{msg_btn_key_base}",
                    )
                else:
                    st.link_button(
                        "ABRIR INBOX",
                        chat_or_inbox_url,
                        width="stretch",
                        help="Sem UserID/ProfileID; abre o Inbox da página para pesquisar pelo nome.",
                        key=f"open_inbox_msgtab_{msg_btn_key_base}",
                    )
            with m3:
                if profile_url:
                    st.link_button(
                        "ABRIR PERFIL",
                        profile_url,
                        width="stretch",
                        key=f"open_profile_msgtab_{msg_btn_key_base}",
                    )
                else:
                    st.link_button(
                        "ABRIR PERFIL",
                        "about:blank",
                        width="stretch",
                        disabled=True,
                        key=f"open_profile_disabled_msgtab_{msg_btn_key_base}",
                    )
            with m4:
                if chat_url:
                    st.components.v1.html(
                        f"""
<div>
  <button id="copyopen_msgtab_{msg_btn_key_base}" style="width:100%; padding:10px 12px; border-radius:10px; border:1px solid rgba(255,255,255,0.15); background: rgba(255,255,255,0.06); color: inherit; cursor:pointer;">
    COPIAR + ABRIR CHAT
  </button>
  <div id="copyopenstatus_msgtab_{msg_btn_key_base}" style="margin-top:6px; font-size:0.9rem; opacity:0.85;"></div>
</div>
<script>
(function() {{
  const btn = document.getElementById("copyopen_msgtab_{msg_btn_key_base}");
  const status = document.getElementById("copyopenstatus_msgtab_{msg_btn_key_base}");
  if (!btn || btn.dataset.bound === "1") return;
  btn.dataset.bound = "1";
  const text = {json.dumps(msg_to_copy)};
  const url = {json.dumps(chat_or_inbox_url)};
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
                else:
                    st.link_button(
                        "COPIAR + ABRIR CHAT",
                        "about:blank",
                        width="stretch",
                        disabled=True,
                        key=f"copyopen_disabled_msgtab_{msg_btn_key_base}",
                    )

            # Guardar/atualizar IDs (sem BD): fica apenas nesta sessão/ficheiro.
            with st.expander("Editar IDs deste cliente", expanded=False):
                cur_ids = client_ids_map.get(client_selected, {})
                c_user = st.text_input("UserId", value=str(cur_ids.get("user_id") or ""), key=f"dir_user_{client_selected}")
                c_profile = st.text_input(
                    "ProfileId (username)",
                    value=str(cur_ids.get("profile_id") or ""),
                    key=f"dir_profile_{client_selected}",
                )
                if st.button("Aplicar (guardar)", type="primary", key=f"apply_ids_{client_selected}"):
                    nu = _normalize_fb_target(c_user)
                    np = _normalize_fb_target(c_profile)
                    client_ids_map[client_selected] = {"user_id": nu, "profile_id": np}
                    try:
                        local2 = load_local_state(STATE_PATH)
                        local2.setdefault("client_ids", {})
                        local2["client_ids"][str(client_selected).strip()] = {"user_id": nu, "profile_id": np}
                        save_local_state(STATE_PATH, local2)
                    except Exception:
                        pass
                    try:
                        odb.upsert_customer_ids(
                            _db_engine(),
                            cliente=str(client_selected).strip(),
                            user_id=nu,
                            profile_id=np,
                        )
                    except Exception as e:
                        st.warning(f"Não gravei os IDs na base de dados: {e}")
                    st.success("Guardado (ficheiro local + base de dados, se configurada).")
                    st.rerun()

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

        if tab_labels is not None:
            with tab_labels:
                st.subheader("Etiquetas 10×15 (imprimir)")
                st.caption("Uma etiqueta por linha de produto: nome, referência+quantidade, preço unitário e (opcional) hora.")

                base = merged.dropna(subset=["Preco"]).copy()
                if base.empty:
                    priced_rows = int(merged["Preco"].notna().sum()) if "Preco" in merged.columns else 0
                    saved_prices = len(st.session_state.get("price_overrides") or {})
                    st.warning(
                        f"Sem etiquetas porque não há preços aplicados. "
                        f"(Com preço: {priced_rows} · Preços guardados: {saved_prices})"
                    )
                has_hora = "Hora" in base.columns
            agg_spec = {"Quantidade": ("Quantidade", "sum"), "Preco": ("Preco", "max")}
            if has_hora:
                agg_spec["Hora"] = ("Hora", "min")

            labels_df = (
                base.groupby(["Cliente", "Produto"], as_index=False)
                .agg(**{k: v for k, v in agg_spec.items()})
                .rename(columns={"Produto": "Referência"})
            )
            labels_df["Imprimir"] = True

            mode = st.radio("Etiquetas", options=["Selecionar (uma/várias)", "Todas"], horizontal=True, key="labels_pick_mode")
            order = st.selectbox("Ordenar por", options=(["Nome"] + (["Hora"] if has_hora else [])), index=0, key="labels_order")
            if order == "Nome":
                labels_df = labels_df.sort_values(["Cliente", "Referência"])
            elif order == "Hora" and has_hora:
                # Try to sort by parsed datetime; fallback to raw string
                dt = pd.to_datetime(labels_df["Hora"], errors="coerce", dayfirst=True)
                labels_df = labels_df.assign(_HoraSort=dt).sort_values(["_HoraSort", "Cliente", "Referência"]).drop(columns=["_HoraSort"])

            if mode == "Selecionar (uma/várias)":
                labels_df["Imprimir"] = False

            edited_labels = st.data_editor(
                labels_df,
                width="stretch",
                num_rows="fixed",
                column_config={
                    "Imprimir": st.column_config.CheckboxColumn("Imprimir"),
                    "Cliente": st.column_config.TextColumn("Cliente", disabled=True),
                    "Referência": st.column_config.TextColumn("Referência", disabled=True),
                    "Quantidade": st.column_config.NumberColumn("Qtd", disabled=True, format="%.3g"),
                    "Preco": st.column_config.NumberColumn("Preço unit.", disabled=True, format="%.2f"),
                    **({"Hora": st.column_config.TextColumn("Hora", disabled=True)} if has_hora else {}),
                },
                key="labels_editor",
            )

            def labels_html(blocks: list[dict]) -> str:
                parts = []
                for b in blocks:
                    parts.append(
                        f"""
  <div class="label">
    <div class="client">{b['cliente']}</div>
    <div class="line">{b['referencia']} — {b['quantidade']}</div>
    {f"<div class='od-small' style='opacity:.75'>Hora: {b['hora']}</div>" if b.get("hora") else ""}
    <div class="price">{b['preco_unit']}</div>
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

            blocks: list[dict] = []
            chosen_rows = edited_labels[edited_labels["Imprimir"].fillna(False)].copy()
            for _, row in chosen_rows.iterrows():
                blocks.append(
                    {
                        "cliente": str(row["Cliente"]),
                        "referencia": str(row["Referência"]),
                        "quantidade": f"{float(row['Quantidade']):g}",
                        "preco_unit": f"Preço: {format_currency(float(row['Preco']), currency)}",
                        "hora": (str(row["Hora"]) if has_hora and pd.notna(row.get("Hora")) and str(row.get("Hora")).strip() else ""),
                    }
                )

            html = labels_html(blocks)
            st.download_button(
                "Download etiquetas (HTML)",
                data=html.encode("utf-8"),
                file_name="etiquetas_10x15.html",
                mime="text/html",
            )
            with st.expander("Pré-visualização", expanded=False):
                st.components.v1.html(html, height=650, scrolling=True)
            st.info("Para imprimir: abre o HTML, escolhe papel 10×15 cm, margens mínimas e escala 100%.")

        # Auto-save locally (outputs) so closing browser doesn't lose work.
        # Não guardamos preços aqui para evitar reutilização entre diretos.
        local = load_local_state(STATE_PATH)
        local.setdefault("by_orders_fp", {})
        local["by_orders_fp"][parsed_orders_fp] = {
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
            if "ProfileId" in export_df.columns:
                cols_out.append("ProfileId")
            cols_out += ["Produto", "Quantidade"]
            export_df = export_df[cols_out].rename(
                columns={
                    "UserId": "user_id",
                    "ProfileId": "profile_id",
                    "Produto": "referencia",
                    "Quantidade": "quantidade",
                }
            )
            st.download_button(
                "Download encomendas (.csv)",
                data=export_df.to_csv(index=False).encode("utf-8"),
                file_name="encomendas_comments.csv",
                mime="text/csv",
            )

    except Exception as e:
        st.error(str(e))
