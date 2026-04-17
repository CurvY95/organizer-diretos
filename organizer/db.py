import os
from datetime import datetime, timezone
from typing import Any, Optional

import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def default_db_path() -> str:
    return os.path.join(os.getcwd(), "saved", "organizer.db")


def _get_database_url() -> str:
    # Prefer Streamlit secrets (prod/local), then env var, then SQLite fallback.
    try:
        secrets = getattr(st, "secrets", {}) or {}
        _ = len(secrets) if hasattr(secrets, "__len__") else 0
    except Exception:
        secrets = {}

    url = ""
    if hasattr(secrets, "get"):
        url = str(secrets.get("DATABASE_URL") or "").strip()
    url = url or str(os.getenv("DATABASE_URL") or "").strip()
    if url:
        # Supabase Postgres requires SSL. If caller forgot sslmode, default to require.
        if url.startswith("postgresql://") or url.startswith("postgres://"):
            if "sslmode=" not in url:
                joiner = "&" if "?" in url else "?"
                url = f"{url}{joiner}sslmode=require"
        return url

    path = default_db_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    return f"sqlite:///{path}"


def _get_schema_name() -> str:
    # Only used for Postgres deployments (Supabase). Defaults to "organizer".
    try:
        secrets = getattr(st, "secrets", {}) or {}
        _ = len(secrets) if hasattr(secrets, "__len__") else 0
    except Exception:
        secrets = {}

    schema = ""
    if hasattr(secrets, "get"):
        schema = str(secrets.get("DB_SCHEMA") or "").strip()
    schema = schema or str(os.getenv("DB_SCHEMA") or "").strip()
    return schema or "organizer"


def _tn(table: str, *, schema: str, is_sqlite: bool) -> str:
    # table name helper (schema-qualified for Postgres)
    if is_sqlite:
        return table
    return f"{schema}.{table}"


def connect(db_path: Optional[str] = None) -> Engine:
    # If db_path is provided, force SQLite at that location (dev/testing).
    if db_path:
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        url = f"sqlite:///{db_path}"
    else:
        url = _get_database_url()
    return create_engine(url, pool_pre_ping=True, future=True)


def init_db(engine: Engine) -> None:
    url = str(engine.url)
    is_sqlite = url.startswith("sqlite")
    schema = _get_schema_name()

    with engine.begin() as con:
        if is_sqlite:
            con.execute(text("PRAGMA journal_mode=WAL;"))
            con.execute(text("PRAGMA foreign_keys=ON;"))
        else:
            # Ensure isolated schema so this app doesn't collide with other apps in the same DB.
            con.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))

        con.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS {customers} (
                  cliente TEXT PRIMARY KEY,
                  notes TEXT NOT NULL DEFAULT '',
                  tags TEXT NOT NULL DEFAULT '',
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                );
                """
            ).bindparams(customers=text(_tn("customers", schema=schema, is_sqlite=is_sqlite)))
        )

        con.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS {sessions} (
                  id TEXT PRIMARY KEY,
                  created_at TEXT NOT NULL,
                  label TEXT NOT NULL,
                  source TEXT NOT NULL DEFAULT ''
                );
                """
            ).bindparams(sessions=text(_tn("sessions", schema=schema, is_sqlite=is_sqlite)))
        )

        # SERIAL works in Postgres; SQLite ignores but requires INTEGER PRIMARY KEY.
        if is_sqlite:
            con.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS {items} (
                      id INTEGER PRIMARY KEY AUTOINCREMENT,
                      session_id TEXT NOT NULL,
                      cliente TEXT NOT NULL,
                      produto TEXT NOT NULL,
                      quantidade REAL NOT NULL,
                      preco REAL,
                      total_item REAL,
                      comentario TEXT,
                      created_at TEXT NOT NULL,
                      FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE
                    );
                    """
                ).bindparams(items=text(_tn("items", schema=schema, is_sqlite=is_sqlite)))
            )
        else:
            con.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS {items} (
                      id BIGSERIAL PRIMARY KEY,
                      session_id TEXT NOT NULL REFERENCES {sessions}(id) ON DELETE CASCADE,
                      cliente TEXT NOT NULL,
                      produto TEXT NOT NULL,
                      quantidade DOUBLE PRECISION NOT NULL,
                      preco DOUBLE PRECISION,
                      total_item DOUBLE PRECISION,
                      comentario TEXT,
                      created_at TEXT NOT NULL
                    );
                    """
                )
                .bindparams(
                    items=text(_tn("items", schema=schema, is_sqlite=is_sqlite)),
                    sessions=text(_tn("sessions", schema=schema, is_sqlite=is_sqlite)),
                )
            )

        con.execute(text(f"CREATE INDEX IF NOT EXISTS idx_items_cliente ON {_tn('items', schema=schema, is_sqlite=is_sqlite)}(cliente);"))
        con.execute(text(f"CREATE INDEX IF NOT EXISTS idx_items_session ON {_tn('items', schema=schema, is_sqlite=is_sqlite)}(session_id);"))
        con.execute(text(f"CREATE INDEX IF NOT EXISTS idx_sessions_created_at ON {_tn('sessions', schema=schema, is_sqlite=is_sqlite)}(created_at);"))


def _ensure_customer_row(con, *, cliente: str) -> None:
    cliente = str(cliente or "").strip()
    if not cliente:
        return
    now = _utc_now_iso()
    con.execute(
        text(
            """
            INSERT INTO customers(cliente, notes, tags, created_at, updated_at)
            VALUES(:cliente, '', '', :now, :now)
            ON CONFLICT(cliente) DO NOTHING;
            """
        ),
        {"cliente": cliente, "now": now},
    )


def get_customer_meta(engine: Engine, *, cliente: str) -> dict[str, str]:
    url = str(engine.url)
    is_sqlite = url.startswith("sqlite")
    schema = _get_schema_name()
    with engine.begin() as con:
        _ensure_customer_row(con, cliente=cliente)
        r = con.execute(
            text(f"SELECT cliente, notes, tags, created_at, updated_at FROM {_tn('customers', schema=schema, is_sqlite=is_sqlite)} WHERE cliente = :cliente;"),
            {"cliente": str(cliente or "").strip()},
        ).mappings().first()
        if not r:
            return {"cliente": str(cliente or "").strip(), "notes": "", "tags": "", "created_at": "", "updated_at": ""}
        return dict(r)


def upsert_customer_meta(engine: Engine, *, cliente: str, notes: str, tags: str) -> None:
    cliente = str(cliente or "").strip()
    if not cliente:
        raise ValueError("Cliente vazio.")
    url = str(engine.url)
    is_sqlite = url.startswith("sqlite")
    schema = _get_schema_name()
    with engine.begin() as con:
        _ensure_customer_row(con, cliente=cliente)
        now = _utc_now_iso()
        con.execute(
            text(
                """
                UPDATE {customers}
                SET notes = :notes, tags = :tags, updated_at = :now
                WHERE cliente = :cliente;
                """
            ).bindparams(customers=text(_tn("customers", schema=schema, is_sqlite=is_sqlite))),
            {"notes": str(notes or ""), "tags": str(tags or ""), "now": now, "cliente": cliente},
        )


def upsert_session(con, *, session_id: str, created_at: str, label: str, source: str) -> None:
    con.execute(
        text(
            """
            INSERT INTO sessions(id, created_at, label, source)
            VALUES(:id, :created_at, :label, :source)
            ON CONFLICT(id) DO UPDATE SET
              created_at=excluded.created_at,
              label=excluded.label,
              source=excluded.source;
            """
        ),
        {"id": session_id, "created_at": created_at, "label": label or "", "source": source or ""},
    )


def replace_session_items(con, *, session_id: str, rows: list[dict[str, Any]]) -> None:
    # Full replace: delete existing then insert.
    con.execute(text("DELETE FROM items WHERE session_id = :sid;"), {"sid": session_id})
    created_at = _utc_now_iso()
    for r in rows:
        _ensure_customer_row(con, cliente=str(r.get("Cliente") or ""))
        con.execute(
            text(
                """
                INSERT INTO items(
                  session_id, cliente, produto, quantidade, preco, total_item, comentario, created_at
                ) VALUES (:sid, :cliente, :produto, :quantidade, :preco, :total_item, :comentario, :created_at);
                """
            ),
            {
                "sid": session_id,
                "cliente": str(r.get("Cliente") or ""),
                "produto": str(r.get("Produto") or ""),
                "quantidade": float(r.get("Quantidade") or 0.0),
                "preco": (float(r["Preco"]) if r.get("Preco") is not None else None),
                "total_item": (float(r["TotalItem"]) if r.get("TotalItem") is not None else None),
                "comentario": (str(r.get("Comentario")) if r.get("Comentario") is not None else None),
                "created_at": created_at,
            },
        )


def save_snapshot(
    engine: Engine,
    *,
    session_id: str,
    created_at: str,
    label: str,
    source: str,
    merged_rows: list[dict[str, Any]],
) -> None:
    with engine.begin() as con:
        # The caller uses the engine; we compute schema-qualified names in each query function.
        upsert_session(con, session_id=session_id, created_at=created_at, label=label, source=source)
        replace_session_items(con, session_id=session_id, rows=merged_rows)


def list_sessions(engine: Engine, limit: int = 50) -> list[dict[str, Any]]:
    url = str(engine.url)
    is_sqlite = url.startswith("sqlite")
    schema = _get_schema_name()
    with engine.begin() as con:
        rows = con.execute(
            text(
                """
                SELECT id, created_at, label, source
                FROM {sessions}
                ORDER BY created_at DESC
                LIMIT :lim;
                """
            ).bindparams(sessions=text(_tn("sessions", schema=schema, is_sqlite=is_sqlite))),
            {"lim": int(limit)},
        ).mappings().all()
        return [dict(r) for r in rows]


def list_customers(engine: Engine, limit: int = 5000) -> list[str]:
    url = str(engine.url)
    is_sqlite = url.startswith("sqlite")
    schema = _get_schema_name()
    with engine.begin() as con:
        rows = con.execute(
            text(
                """
                SELECT cliente
                FROM {items}
                WHERE cliente IS NOT NULL AND TRIM(cliente) <> ''
                GROUP BY cliente
                ORDER BY cliente ASC
                LIMIT :lim;
                """
            ).bindparams(items=text(_tn("items", schema=schema, is_sqlite=is_sqlite))),
            {"lim": int(limit)},
        ).mappings().all()
        return [str(r["cliente"]) for r in rows]


def customer_history(engine: Engine, *, cliente: str, limit: int = 3000) -> list[dict[str, Any]]:
    url = str(engine.url)
    is_sqlite = url.startswith("sqlite")
    schema = _get_schema_name()
    with engine.begin() as con:
        rows = con.execute(
            text(
                """
                SELECT
                  s.created_at AS session_created_at,
                  s.label AS session_label,
                  s.id AS session_id,
                  i.produto,
                  i.quantidade,
                  i.preco,
                  i.total_item,
                  i.comentario
                FROM {items} i
                JOIN {sessions} s ON s.id = i.session_id
                WHERE i.cliente = :cliente
                ORDER BY s.created_at DESC, i.id DESC
                LIMIT :lim;
                """
            ),
            {"cliente": str(cliente or "").strip(), "lim": int(limit)},
        ).bindparams(
            items=text(_tn("items", schema=schema, is_sqlite=is_sqlite)),
            sessions=text(_tn("sessions", schema=schema, is_sqlite=is_sqlite)),
        ).mappings().all()
        return [dict(r) for r in rows]


def customer_stats(engine: Engine, *, cliente: str) -> dict[str, Any]:
    url = str(engine.url)
    is_sqlite = url.startswith("sqlite")
    schema = _get_schema_name()
    with engine.begin() as con:
        r = con.execute(
            text(
                """
                SELECT
                  COUNT(DISTINCT i.session_id) AS sessions_count,
                  COUNT(*) AS items_count,
                  SUM(COALESCE(i.total_item, 0)) AS total_spent,
                  MAX(s.created_at) AS last_session_at
                FROM {items} i
                JOIN {sessions} s ON s.id = i.session_id
                WHERE i.cliente = :cliente;
                """
            ),
            {"cliente": str(cliente or "").strip()},
        ).bindparams(
            items=text(_tn("items", schema=schema, is_sqlite=is_sqlite)),
            sessions=text(_tn("sessions", schema=schema, is_sqlite=is_sqlite)),
        ).mappings().first()
        out = dict(r) if r else {}
        out["total_spent"] = float(out.get("total_spent") or 0.0)
        out["sessions_count"] = int(out.get("sessions_count") or 0)
        out["items_count"] = int(out.get("items_count") or 0)
        out["last_session_at"] = out.get("last_session_at") or ""
        return out


def customer_top_products(engine: Engine, *, cliente: str, limit: int = 50) -> list[dict[str, Any]]:
    url = str(engine.url)
    is_sqlite = url.startswith("sqlite")
    schema = _get_schema_name()
    with engine.begin() as con:
        rows = con.execute(
            text(
                """
                SELECT
                  produto,
                  COUNT(DISTINCT session_id) AS vezes,
                  SUM(quantidade) AS quantidade_total
                FROM {items}
                WHERE cliente = :cliente
                GROUP BY produto
                ORDER BY vezes DESC, quantidade_total DESC, produto ASC
                LIMIT :lim;
                """
            ),
            {"cliente": str(cliente or "").strip(), "lim": int(limit)},
        ).bindparams(items=text(_tn("items", schema=schema, is_sqlite=is_sqlite))).mappings().all()
        out = [dict(r) for r in rows]
        for r in out:
            r["vezes"] = int(r.get("vezes") or 0)
            r["quantidade_total"] = float(r.get("quantidade_total") or 0.0)
        return out


def customer_sessions(engine: Engine, *, cliente: str, limit: int = 50) -> list[dict[str, Any]]:
    url = str(engine.url)
    is_sqlite = url.startswith("sqlite")
    schema = _get_schema_name()
    with engine.begin() as con:
        rows = con.execute(
            text(
                """
                SELECT
                  s.id AS session_id,
                  s.created_at,
                  s.label,
                  s.source,
                  SUM(COALESCE(i.total_item, 0)) AS total_spent
                FROM {sessions} s
                JOIN {items} i ON i.session_id = s.id
                WHERE i.cliente = :cliente
                GROUP BY s.id, s.created_at, s.label, s.source
                ORDER BY s.created_at DESC
                LIMIT :lim;
                """
            ),
            {"cliente": str(cliente or "").strip(), "lim": int(limit)},
        ).bindparams(
            items=text(_tn("items", schema=schema, is_sqlite=is_sqlite)),
            sessions=text(_tn("sessions", schema=schema, is_sqlite=is_sqlite)),
        ).mappings().all()
        out = [dict(r) for r in rows]
        for r in out:
            r["total_spent"] = float(r.get("total_spent") or 0.0)
        return out


def session_exists(engine: Engine, *, session_id: str) -> bool:
    url = str(engine.url)
    is_sqlite = url.startswith("sqlite")
    schema = _get_schema_name()
    with engine.begin() as con:
        r = con.execute(
            text("SELECT 1 FROM {sessions} WHERE id = :id LIMIT 1;").bindparams(
                sessions=text(_tn("sessions", schema=schema, is_sqlite=is_sqlite))
            ),
            {"id": str(session_id or "").strip()},
        ).first()
        return r is not None


