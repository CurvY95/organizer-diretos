import os
from typing import Optional

import streamlit as st
try:
    import tomllib  # py311+
except Exception:  # pragma: no cover
    tomllib = None
    import tomli

try:
    import gspread
    from google.oauth2.service_account import Credentials
except Exception:
    gspread = None
    Credentials = None

from .facebook import normalize_fb_target
from .utils import now_iso


def _load_toml_if_exists(path: str) -> dict:
    try:
        if not os.path.exists(path):
            return {}
        with open(path, "rb") as f:
            if tomllib is not None:
                return tomllib.load(f) or {}
            return tomli.load(f) or {}
    except Exception:
        return {}


def _get_gsheets_service_account_info() -> Optional[dict]:
    secrets = getattr(st, "secrets", {}) or {}
    if hasattr(secrets, "get"):
        info = secrets.get("GSHEETS_SERVICE_ACCOUNT")
        if isinstance(info, dict) and info:
            return info

    local_secrets = _load_toml_if_exists(os.path.join(os.getcwd(), ".streamlit", "secrets.toml"))
    info = (local_secrets.get("GSHEETS_SERVICE_ACCOUNT") or {}) if isinstance(local_secrets, dict) else {}
    return info if isinstance(info, dict) and info else None


def _get_gsheets_client():
    if gspread is None or Credentials is None:
        raise RuntimeError("Dependências do Google Sheets não instaladas (gspread/google-auth).")

    info = _get_gsheets_service_account_info()
    if not info:
        raise RuntimeError(
            "Credenciais do Google Sheets não configuradas. "
            "Defina `GSHEETS_SERVICE_ACCOUNT` em `st.secrets` (ou em `.streamlit/secrets.toml`)."
        )

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


@st.cache_data(show_spinner=False, ttl=60)
def load_clients_directory(spreadsheet_id: str, worksheet_name: str) -> dict[str, dict[str, str]]:
    spreadsheet_id = str(spreadsheet_id or "").strip()
    worksheet_name = str(worksheet_name or "").strip()
    if not spreadsheet_id or not worksheet_name:
        return {}

    gc = _get_gsheets_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(worksheet_name)

    rows = ws.get_all_records() or []
    out: dict[str, dict[str, str]] = {}
    for r in rows:
        name = str(r.get("Cliente") or "").strip()
        if not name:
            continue
        out[name] = {
            "user_id": normalize_fb_target(r.get("UserId") or ""),
            "profile_id": normalize_fb_target(r.get("ProfileId") or ""),
        }
    return out


def _ensure_clients_sheet_headers(ws) -> None:
    headers = ws.row_values(1) or []
    expected = ["Cliente", "UserId", "ProfileId", "updated_at"]
    if [h.strip() for h in headers[: len(expected)]] != expected:
        ws.update("A1:D1", [expected])


def upsert_client(
    *,
    spreadsheet_id: str,
    worksheet_name: str,
    cliente: str,
    user_id: str,
    profile_id: str,
) -> None:
    spreadsheet_id = str(spreadsheet_id or "").strip()
    worksheet_name = str(worksheet_name or "").strip()
    cliente = str(cliente or "").strip()
    if not spreadsheet_id or not worksheet_name:
        raise ValueError("Spreadsheet ID / worksheet não definidos.")
    if not cliente:
        raise ValueError("Cliente vazio.")

    gc = _get_gsheets_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(worksheet_name)
    _ensure_clients_sheet_headers(ws)

    user_id = normalize_fb_target(user_id)
    profile_id = normalize_fb_target(profile_id)

    col_a = ws.col_values(1) or []
    row_idx = None
    for i, v in enumerate(col_a[1:], start=2):
        if str(v).strip() == cliente:
            row_idx = i
            break

    updated_at = now_iso()
    if row_idx is None:
        ws.append_row([cliente, user_id, profile_id, updated_at], value_input_option="RAW")
    else:
        ws.update(f"A{row_idx}:D{row_idx}", [[cliente, user_id, profile_id, updated_at]])

    load_clients_directory.clear()

