import io
import json
import os
import re
from typing import Optional

import pandas as pd
import streamlit as st

from organizer import db as odb
from organizer import core as oc
from organizer import facebook as ofb
from organizer import storage_local as osl
from organizer import sessions_json as osj
from organizer import utils as ou
from organizer import clients_directory_gsheets as ogs


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
    orders_df = pd.read_csv(orders_file)
    prices_df = pd.read_csv(prices_file)
    return orders_df, prices_df


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

_load_clients_directory_from_gsheets = ogs.load_clients_directory
upsert_client_in_gsheets = ogs.upsert_client

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


now_iso = ou.now_iso
safe_session_id = ou.safe_session_id

list_sessions = osj.list_sessions
save_session = osj.save_session
load_session = osj.load_session
delete_session = osj.delete_session


st.set_page_config(page_title="Organizer Diretos", layout="wide", page_icon="🧾")

require_login()


@st.cache_resource
def _get_db(_cache_bust: str):
    con = odb.connect()
    odb.init_db(con)
    return con


def _db_cache_bust_key() -> str:
    # Make DB connection cache sensitive to secrets/env changes.
    try:
        secrets = getattr(st, "secrets", {}) or {}
        _ = len(secrets) if hasattr(secrets, "__len__") else 0
    except Exception:
        secrets = {}

    db_url = ""
    schema = ""
    if hasattr(secrets, "get"):
        db_url = str(secrets.get("DATABASE_URL") or "").strip()
        schema = str(secrets.get("DB_SCHEMA") or "").strip()
    db_url = db_url or str(os.getenv("DATABASE_URL") or "").strip()
    schema = schema or str(os.getenv("DB_SCHEMA") or "").strip()
    return f"{schema}|{db_url}"


try:
    db_con = _get_db(_db_cache_bust_key())
except Exception as e:
    st.error("Falha a ligar à base de dados (Supabase/Postgres).")
    st.caption(
        "No Streamlit Cloud isto costuma ser: URL/pooler errado, password errada, ou password com caracteres "
        "especiais sem URL-encode. Usa o Transaction pooler (porta 6543) e `sslmode=require`."
    )
    st.caption(f"Detalhe: `{type(e).__name__}`")
    if type(e).__name__ == "ArgumentError":
        # Try to show a safe, non-secret diagnostic for URL formatting issues.
        try:
            from sqlalchemy.engine import make_url

            try:
                secrets = getattr(st, "secrets", {}) or {}
                _ = len(secrets) if hasattr(secrets, "__len__") else 0
            except Exception:
                secrets = {}

            db_url = ""
            if hasattr(secrets, "get"):
                db_url = str(secrets.get("DATABASE_URL") or "").strip()
            db_url = db_url or str(os.getenv("DATABASE_URL") or "").strip()
            parsed = make_url(db_url)
            q = dict(parsed.query or {})
            st.caption(
                "Diagnóstico (sem password): "
                f"driver=`{parsed.drivername}`, user=`{parsed.username}`, host=`{parsed.host}`, "
                f"port=`{parsed.port}`, db=`{parsed.database}`, sslmode=`{q.get('sslmode','')}`"
            )
        except Exception:
            st.caption(
                "Diagnóstico: o `DATABASE_URL` não está num formato válido para SQLAlchemy. "
                "Confirma que não tem espaços/linhas novas e que a password está URL-encoded se tiver caracteres especiais."
            )
    st.stop()

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

    st.subheader("Diretório de clientes (Google Sheets)")
    use_clients_dir = st.checkbox("Usar diretório de IDs (reutilizar nomes/IDs)", value=False)
    clients_sheet_id = st.text_input("Spreadsheet ID", value=st.session_state.get("clients_sheet_id", ""))
    clients_sheet_tab = st.text_input("Worksheet (aba)", value=st.session_state.get("clients_sheet_tab", "clients"))
    st.session_state["use_clients_dir"] = use_clients_dir
    st.session_state["clients_sheet_id"] = clients_sheet_id
    st.session_state["clients_sheet_tab"] = clients_sheet_tab
    if use_clients_dir:
        st.caption("A Sheet deve ter colunas: Cliente, UserId, ProfileId, updated_at")

st.divider()

orders_df = None
prices_df = None
orders_source_label = None

tab_main, tab_history = st.tabs(["Trabalho atual", "Histórico"])

with tab_history:
    st.subheader("Histórico")
    htab_sessions, htab_clients = st.tabs(["Sessões (JSON)", "Clientes (DB)"])

    with htab_clients:
        st.caption("Histórico persistente (base de dados). Aqui consegues ver o que cada pessoa comprou em diretos anteriores.")
        try:
            customers = odb.list_customers(db_con)
        except Exception as e:
            customers = []
            st.error(f"DB: falha ao listar clientes: {e}")

        if not customers:
            st.info("Ainda não há histórico na base de dados. Gere um direto e o app grava automaticamente.")
        else:
            search = st.text_input("Pesquisar cliente", value="", placeholder="Escreve parte do nome…", key="hist_cliente_search")
            filtered = customers
            if search.strip():
                s = search.strip().lower()
                filtered = [c for c in customers if s in c.lower()]
            cliente_h = st.selectbox("Cliente", options=filtered or customers, key="hist_cliente_pick")

            # Customer meta (notes/tags)
            try:
                meta = odb.get_customer_meta(db_con, cliente=cliente_h)
            except Exception as e:
                meta = {"notes": "", "tags": ""}
                st.warning(f"DB: não consegui carregar notas/tags: {e}")

            st.markdown("<div class='od-muted'><b>Perfil do cliente</b></div>", unsafe_allow_html=True)
            m1, m2 = st.columns([2, 1])
            with m1:
                notes = st.text_area("Notas internas", value=str(meta.get("notes") or ""), height=140, key="cust_notes")
                tags = st.text_input("Tags (separadas por vírgula)", value=str(meta.get("tags") or ""), key="cust_tags")
                if st.button("Guardar perfil", type="primary", key="save_cust_profile"):
                    try:
                        odb.upsert_customer_meta(db_con, cliente=cliente_h, notes=notes, tags=tags)
                        st.success("Perfil guardado.")
                    except Exception as e:
                        st.error(f"Falha ao guardar perfil: {e}")
            with m2:
                try:
                    stats = odb.customer_stats(db_con, cliente=cliente_h)
                except Exception as e:
                    stats = {"sessions_count": 0, "items_count": 0, "total_spent": 0.0, "last_session_at": ""}
                    st.warning(f"DB: falha ao calcular stats: {e}")
                st.metric("Diretos", int(stats.get("sessions_count") or 0))
                st.metric("Itens", int(stats.get("items_count") or 0))
                # Total spent is only meaningful when items had prices at the time of snapshot.
                st.metric("Total (com preço)", format_currency(float(stats.get("total_spent") or 0.0), currency))
                if stats.get("last_session_at"):
                    st.caption(f"Último direto: `{stats.get('last_session_at')}`")

            st.divider()
            ct1, ct2 = st.columns(2)
            with ct1:
                st.markdown("<div class='od-muted'><b>Top produtos</b></div>", unsafe_allow_html=True)
                try:
                    top = odb.customer_top_products(db_con, cliente=cliente_h, limit=50)
                    st.dataframe(pd.DataFrame(top), use_container_width=True)
                except Exception as e:
                    st.error(f"DB: falha ao listar top produtos: {e}")
            with ct2:
                st.markdown("<div class='od-muted'><b>Diretos anteriores</b></div>", unsafe_allow_html=True)
                try:
                    sess = odb.customer_sessions(db_con, cliente=cliente_h, limit=50)
                    st.dataframe(pd.DataFrame(sess), use_container_width=True)
                except Exception as e:
                    st.error(f"DB: falha ao listar sessões: {e}")

            st.divider()
            st.markdown("<div class='od-muted'><b>Linhas (histórico completo)</b></div>", unsafe_allow_html=True)
            try:
                hist_rows = odb.customer_history(db_con, cliente=cliente_h)
                st.dataframe(pd.DataFrame(hist_rows), use_container_width=True)
            except Exception as e:
                st.error(f"DB: falha ao buscar histórico: {e}")

    with htab_sessions:
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

            st.divider()
            st.subheader("Migrar sessões (JSON) → Base de dados")
            st.caption("Importa as sessões antigas para a BD. Se uma sessão já existir na BD, é ignorada.")
            if st.button("Migrar tudo para a BD", type="primary", key="migrate_json_to_db"):
                migrated = 0
                skipped = 0
                failed = 0
                for s in sessions:
                    try:
                        sid = str(s.get("id") or "").strip()
                        if not sid:
                            skipped += 1
                            continue
                        if odb.session_exists(db_con, session_id=sid):
                            skipped += 1
                            continue
                        data = load_session(s["path"])
                        created_at = str(data.get("created_at") or s.get("created_at") or "")
                        label = str(data.get("label") or s.get("label") or "")
                        source = str((data.get("meta") or {}).get("source") or "")
                        rows = []
                        for r in (data.get("orders") or []):
                            rows.append(
                                {
                                    "Cliente": str(r.get("Cliente") or ""),
                                    "Produto": str(r.get("Produto") or ""),
                                    "Quantidade": float(r.get("Quantidade") or 0.0),
                                    "Comentario": (str(r.get("Comentario")) if r.get("Comentario") is not None else None),
                                    # prices not available in JSON sessions
                                    "Preco": None,
                                    "TotalItem": None,
                                }
                            )
                        odb.save_snapshot(
                            db_con,
                            session_id=sid,
                            created_at=created_at or now_iso(),
                            label=label or "Sessão",
                            source=source,
                            merged_rows=rows,
                        )
                        migrated += 1
                    except Exception:
                        failed += 1
                st.success(f"Migração concluída. Migradas: {migrated} | Ignoradas: {skipped} | Falhas: {failed}")

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
                    # Tampermonkey export uses ';' delimiter and often includes UTF-8 BOM.
                    raw = uploaded.getvalue()
                    sample = raw[:4096].decode("utf-8-sig", errors="ignore")
                    first_line = (sample.splitlines() or [""])[0]
                    semicolons = first_line.count(";")
                    commas = first_line.count(",")
                    sep = ";" if semicolons > commas else ","
                    orders_df = pd.read_csv(io.BytesIO(raw), sep=sep, encoding="utf-8-sig")
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
        tab_comments, tab_upload, tab_prices, tab_summary, tab_messages = st.tabs(
            ["0) Comentários", "1) Encomendas (Comments)", "2) Preços", "3) Resumo", "4) Mensagens"]
        )

        with tab_comments:
            st.subheader("Comentários (texto original)")
            st.caption("Mostra o comentário real captado no Tampermonkey, se vier no Excel/CSV.")

            orders_view = _standardize_df_columns(orders_df)
            orders_view = _apply_aliases(orders_view, ORDERS_ALIASES)
            _validate_required_cols(orders_view, REQUIRED_ORDERS_COLS, "Encomendas (Comments)")

            cols = ["Cliente"]
            if "UserId" in orders_view.columns:
                cols.append("UserId")
            if "ProfileId" in orders_view.columns:
                cols.append("ProfileId")
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
                st.dataframe(vc, use_container_width=True)

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
            ui_cols += ["Produto", "Quantidade"]

            orders_edit = orders_edit[ui_cols].copy()
            orders_edit = orders_edit.rename(
                columns={
                    "Cliente": "Cliente",
                    "UserId": "User ID",
                    "ProfileId": "Profile ID",
                    "Produto": "Referência",
                    "Quantidade": "Quantidade",
                }
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

            if "Profile ID" in orders_edit.columns:
                col_cfg["Profile ID"] = st.column_config.TextColumn("Profile ID", disabled=True)

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
            orders_for_calc = edited_orders.rename(
                columns={
                    "Referência": "Produto",
                    "User ID": "UserId",
                    "Profile ID": "ProfileId",
                }
            ).copy()

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

        # Stable "direto session id" for DB snapshots: reuse loaded session id when available,
        # otherwise create one per run and keep it in session_state.
        if "history_session_id" not in st.session_state:
            loaded = st.session_state.get("loaded_session") or {}
            st.session_state["history_session_id"] = loaded.get("id") or safe_session_id(now_iso())

        # Persist snapshot in DB (only rows with price, so history reflects what was actually billed).
        try:
            rows_for_db = merged.dropna(subset=["Preco"]).copy()
            # Attach optional comment if present in the original orders
            orders_std = _standardize_df_columns(orders_for_calc)
            orders_std = _apply_aliases(orders_std, ORDERS_ALIASES)
            if "Comentario" in orders_std.columns:
                comm_map = (
                    orders_std[["Cliente", "Comentario"]]
                    .dropna(subset=["Cliente"])
                    .assign(Cliente=lambda d: d["Cliente"].astype(str).str.strip())
                )
                # For repeated rows, keep last non-empty comment.
                comm_map["Comentario"] = comm_map["Comentario"].astype(str).map(lambda s: s.strip())
                comm_map = comm_map[comm_map["Comentario"] != ""]
                comm_dict = {r["Cliente"]: r["Comentario"] for _, r in comm_map.iterrows()}
                rows_for_db["Comentario"] = rows_for_db["Cliente"].astype(str).map(lambda c: comm_dict.get(str(c).strip(), ""))

            # Build minimal payload
            payload = rows_for_db[["Cliente", "Produto", "Quantidade", "Preco", "TotalItem"]].copy()
            if "Comentario" in rows_for_db.columns:
                payload["Comentario"] = rows_for_db["Comentario"]
            merged_rows = payload.to_dict(orient="records")

            session_id = str(st.session_state["history_session_id"])
            created_at = (st.session_state.get("history_created_at") or now_iso())
            st.session_state["history_created_at"] = created_at
            label = (st.session_state.get("session_label") or "").strip() or "Direto"
            source = orders_source_label or ""
            odb.save_snapshot(
                db_con,
                session_id=session_id,
                created_at=created_at,
                label=label,
                source=source,
                merged_rows=merged_rows,
            )
        except Exception as e:
            st.warning(f"DB: não consegui gravar histórico automaticamente: {e}")

        still_missing = merged[merged["Preco"].isna()][["ProdutoKey", "Produto"]].drop_duplicates()
        if not still_missing.empty:
            st.info(
                f"Ainda faltam preços para {len(still_missing)} referência(s). "
                "Preencha na aba '2) Preços' para liberar o resumo."
            )

        by_client, details = build_summary(merged.dropna(subset=["Preco"]))
        client_ids_map: dict[str, dict[str, str]] = {}

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

            client_ids_map[cliente] = {
                "user_id": user_id,
                "profile_id": profile_id,
            }

        # Merge with shared directory (Google Sheets): only fill missing IDs.
        clients_dir_map: dict[str, dict[str, str]] = {}
        try:
            if st.session_state.get("use_clients_dir") and clients_sheet_id and clients_sheet_tab:
                clients_dir_map = _load_clients_directory_from_gsheets(clients_sheet_id, clients_sheet_tab)
        except Exception as e:
            st.warning(f"Diretório (Sheets): {e}")
            clients_dir_map = {}

        if clients_dir_map:
            for nome, ids in client_ids_map.items():
                if (not (ids.get("user_id") or "").strip()) and (clients_dir_map.get(nome) or {}).get("user_id"):
                    ids["user_id"] = (clients_dir_map[nome].get("user_id") or "").strip()
                if (not (ids.get("profile_id") or "").strip()) and (clients_dir_map.get(nome) or {}).get("profile_id"):
                    ids["profile_id"] = (clients_dir_map[nome].get("profile_id") or "").strip()

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
                                use_container_width=True,
                                key=f"open_chat_{btn_key_base}",
                            )
                        else:
                            st.link_button(
                                "ABRIR INBOX",
                                chat_or_inbox_url,
                                use_container_width=True,
                                help="Sem UserID/ProfileID; abre o Inbox da página para pesquisar pelo nome.",
                                key=f"open_inbox_{btn_key_base}",
                            )
                    with a3:
                        if profile_url:
                            st.link_button(
                                "ABRIR PERFIL",
                                profile_url,
                                use_container_width=True,
                                key=f"open_profile_{btn_key_base}",
                            )
                        else:
                            st.link_button(
                                "ABRIR PERFIL",
                                "about:blank",
                                use_container_width=True,
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
                                use_container_width=True,
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
                        use_container_width=True,
                        key=f"open_chat_msgtab_{msg_btn_key_base}",
                    )
                else:
                    st.link_button(
                        "ABRIR INBOX",
                        chat_or_inbox_url,
                        use_container_width=True,
                        help="Sem UserID/ProfileID; abre o Inbox da página para pesquisar pelo nome.",
                        key=f"open_inbox_msgtab_{msg_btn_key_base}",
                    )
            with m3:
                if profile_url:
                    st.link_button(
                        "ABRIR PERFIL",
                        profile_url,
                        use_container_width=True,
                        key=f"open_profile_msgtab_{msg_btn_key_base}",
                    )
                else:
                    st.link_button(
                        "ABRIR PERFIL",
                        "about:blank",
                        use_container_width=True,
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
                        use_container_width=True,
                        disabled=True,
                        key=f"copyopen_disabled_msgtab_{msg_btn_key_base}",
                    )

            # Guardar/atualizar IDs no diretório (Sheets)
            if st.session_state.get("use_clients_dir"):
                with st.expander("Guardar IDs deste cliente (para reutilizar)", expanded=False):
                    cur_ids = client_ids_map.get(client_selected, {})
                    c_user = st.text_input("UserId", value=str(cur_ids.get("user_id") or ""), key=f"dir_user_{client_selected}")
                    c_profile = st.text_input(
                        "ProfileId (username)",
                        value=str(cur_ids.get("profile_id") or ""),
                        key=f"dir_profile_{client_selected}",
                    )
                    if st.button("Guardar no diretório (Sheets)", type="primary", key=f"save_dir_{client_selected}"):
                        try:
                            upsert_client_in_gsheets(
                                spreadsheet_id=clients_sheet_id,
                                worksheet_name=clients_sheet_tab,
                                cliente=client_selected,
                                user_id=c_user,
                                profile_id=c_profile,
                            )
                            # Update current run map too
                            client_ids_map[client_selected] = {
                                "user_id": _normalize_fb_target(c_user),
                                "profile_id": _normalize_fb_target(c_profile),
                            }
                            st.success("Guardado no diretório.")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Falha ao guardar no diretório: {e}")

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
