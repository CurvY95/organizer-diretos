import json
import os
from typing import Any

import pandas as pd

from .core import ORDERS_ALIASES, REQUIRED_ORDERS_COLS, apply_aliases, standardize_df_columns
from .utils import now_iso, safe_session_id


SESSIONS_DIR = os.path.join(os.getcwd(), "saved", "sessions")


def list_sessions() -> list[dict[str, Any]]:
    if not os.path.isdir(SESSIONS_DIR):
        return []
    out: list[dict[str, Any]] = []
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
    orders_clean = standardize_df_columns(orders_clean)
    orders_clean = apply_aliases(orders_clean, ORDERS_ALIASES)

    keep = REQUIRED_ORDERS_COLS.copy()
    if "Comentario" in orders_clean.columns:
        keep.append("Comentario")
    if "Ativo" in orders_clean.columns:
        keep.append("Ativo")
    orders_clean = orders_clean[keep].copy()

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


def load_session(session_path: str) -> dict[str, Any]:
    with open(session_path, "r", encoding="utf-8") as f:
        return json.load(f) or {}


def delete_session(session_path: str) -> None:
    os.remove(session_path)

