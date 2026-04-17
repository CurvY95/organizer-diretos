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


def _validate_schema_name(schema: str) -> str:
    schema = str(schema or "").strip() or "organizer"
    # Avoid SQL injection when interpolating identifiers.
    if not schema.replace("_", "").isalnum() or schema[0].isdigit():
        raise ValueError("DB_SCHEMA inválido. Use apenas letras/números/underscore e não comece por número.")
    return schema


def _table_names(engine: Engine) -> dict[str, Any]:
    url = str(engine.url)
    is_sqlite = url.startswith("sqlite")
    schema = _validate_schema_name(_get_schema_name())
    return {
        "is_sqlite": is_sqlite,
        "schema": schema,
        "customers": _tn("customers", schema=schema, is_sqlite=is_sqlite),
        "sessions": _tn("sessions", schema=schema, is_sqlite=is_sqlite),
        "items": _tn("items", schema=schema, is_sqlite=is_sqlite),
    }


def connect(db_path: Optional[str] = None) -> Engine:
    # If db_path is provided, force SQLite at that location (dev/testing).
    if db_path:
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        url = f"sqlite:///{db_path}"
    else:
        url = _get_database_url()
    return create_engine(url, pool_pre_ping=True, future=True)


def init_db(engine: Engine) -> None:
    tn = _table_names(engine)
    is_sqlite = bool(tn["is_sqlite"])
    schema = str(tn["schema"])
    customers = str(tn["customers"])
    sessions = str(tn["sessions"])
    items = str(tn["items"])

    with engine.begin() as con:
        if is_sqlite:
            con.execute(text("PRAGMA journal_mode=WAL;"))
            con.execute(text("PRAGMA foreign_keys=ON;"))
        else:
            # Ensure isolated schema so this app doesn't collide with other apps in the same DB.
            con.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))

        con.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {customers} (
                  cliente TEXT PRIMARY KEY,
                  user_id TEXT,
                  profile_id TEXT,
                  notes TEXT NOT NULL DEFAULT '',
                  tags TEXT NOT NULL DEFAULT '',
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                );
                """
            )
        )

        # Backwards-compatible migrations (in case the table existed before these columns)
        if not is_sqlite:
            con.execute(text(f"ALTER TABLE {customers} ADD COLUMN IF NOT EXISTS user_id TEXT;"))
            con.execute(text(f"ALTER TABLE {customers} ADD COLUMN IF NOT EXISTS profile_id TEXT;"))
        else:
            # SQLite: ADD COLUMN IF NOT EXISTS isn't supported everywhere; ignore failures.
            try:
                con.execute(text(f"ALTER TABLE {customers} ADD COLUMN user_id TEXT;"))
            except Exception:
                pass
            try:
                con.execute(text(f"ALTER TABLE {customers} ADD COLUMN profile_id TEXT;"))
            except Exception:
                pass

        con.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {sessions} (
                  id TEXT PRIMARY KEY,
                  created_at TEXT NOT NULL,
                  label TEXT NOT NULL,
                  source TEXT NOT NULL DEFAULT ''
                );
                """
            )
        )

        # SERIAL works in Postgres; SQLite ignores but requires INTEGER PRIMARY KEY.
        if is_sqlite:
            con.execute(
                text(
                    f"""
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
                )
            )
        else:
            con.execute(
                text(
                    f"""
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
            )

        con.execute(text(f"CREATE INDEX IF NOT EXISTS idx_items_cliente ON {items}(cliente);"))
        con.execute(text(f"CREATE INDEX IF NOT EXISTS idx_items_session ON {items}(session_id);"))
        con.execute(text(f"CREATE INDEX IF NOT EXISTS idx_sessions_created_at ON {sessions}(created_at);"))


def _ensure_customer_row(con, *, cliente: str, customers_table: str) -> None:
    cliente = str(cliente or "").strip()
    if not cliente:
        return
    now = _utc_now_iso()
    con.execute(
        text(
            f"""
            INSERT INTO {customers_table}(cliente, user_id, profile_id, notes, tags, created_at, updated_at)
            VALUES(:cliente, NULL, NULL, '', '', :now, :now)
            ON CONFLICT(cliente) DO NOTHING;
            """
        ),
        {"cliente": cliente, "now": now},
    )


def get_customer_meta(engine: Engine, *, cliente: str) -> dict[str, str]:
    tn = _table_names(engine)
    customers = str(tn["customers"])
    with engine.begin() as con:
        _ensure_customer_row(con, cliente=cliente, customers_table=customers)
        r = con.execute(
            text(
                f"SELECT cliente, user_id, profile_id, notes, tags, created_at, updated_at "
                f"FROM {customers} WHERE cliente = :cliente;"
            ),
            {"cliente": str(cliente or "").strip()},
        ).mappings().first()
        if not r:
            return {
                "cliente": str(cliente or "").strip(),
                "user_id": "",
                "profile_id": "",
                "notes": "",
                "tags": "",
                "created_at": "",
                "updated_at": "",
            }
        return dict(r)


def upsert_customer_meta(engine: Engine, *, cliente: str, notes: str, tags: str) -> None:
    cliente = str(cliente or "").strip()
    if not cliente:
        raise ValueError("Cliente vazio.")
    tn = _table_names(engine)
    customers = str(tn["customers"])
    with engine.begin() as con:
        _ensure_customer_row(con, cliente=cliente, customers_table=customers)
        now = _utc_now_iso()
        con.execute(
            text(
                f"""
                UPDATE {customers}
                SET notes = :notes, tags = :tags, updated_at = :now
                WHERE cliente = :cliente;
                """
            ),
            {"notes": str(notes or ""), "tags": str(tags or ""), "now": now, "cliente": cliente},
        )


def upsert_customer_ids(engine: Engine, *, cliente: str, user_id: str, profile_id: str) -> None:
    cliente = str(cliente or "").strip()
    if not cliente:
        raise ValueError("Cliente vazio.")
    tn = _table_names(engine)
    customers = str(tn["customers"])
    with engine.begin() as con:
        _ensure_customer_row(con, cliente=cliente, customers_table=customers)
        now = _utc_now_iso()
        con.execute(
            text(
                f"""
                UPDATE {customers}
                SET user_id = :user_id, profile_id = :profile_id, updated_at = :now
                WHERE cliente = :cliente;
                """
            ),
            {
                "user_id": (str(user_id or "").strip() or None),
                "profile_id": (str(profile_id or "").strip() or None),
                "now": now,
                "cliente": cliente,
            },
        )


def get_customer_ids(engine: Engine, *, cliente: str) -> dict[str, str]:
    meta = get_customer_meta(engine, cliente=cliente)
    return {
        "user_id": str(meta.get("user_id") or "").strip(),
        "profile_id": str(meta.get("profile_id") or "").strip(),
    }


def upsert_customer_ids_bulk(engine: Engine, *, rows: list[tuple[str, str, str]]) -> int:
    """
    Bulk upsert of (cliente, user_id, profile_id) within a single transaction.
    Returns number of updated rows requested.
    """
    rows = rows or []
    if not rows:
        return 0
    tn = _table_names(engine)
    customers = str(tn["customers"])
    now = _utc_now_iso()
    with engine.begin() as con:
        for cliente, user_id, profile_id in rows:
            cliente = str(cliente or "").strip()
            if not cliente:
                continue
            _ensure_customer_row(con, cliente=cliente, customers_table=customers)
            con.execute(
                text(
                    f"""
                    UPDATE {customers}
                    SET user_id = :user_id, profile_id = :profile_id, updated_at = :now
                    WHERE cliente = :cliente;
                    """
                ),
                {
                    "user_id": (str(user_id or "").strip() or None),
                    "profile_id": (str(profile_id or "").strip() or None),
                    "now": now,
                    "cliente": cliente,
                },
            )
    return len(rows)


def ensure_customer(engine: Engine, *, cliente: str) -> None:
    cliente = str(cliente or "").strip()
    if not cliente:
        raise ValueError("Cliente vazio.")
    tn = _table_names(engine)
    customers = str(tn["customers"])
    with engine.begin() as con:
        _ensure_customer_row(con, cliente=cliente, customers_table=customers)


def list_all_customers(engine: Engine, limit: int = 5000) -> list[str]:
    """
    Lists customers from `customers` table (includes clients without orders/items).
    """
    tn = _table_names(engine)
    customers = str(tn["customers"])
    with engine.begin() as con:
        rows = con.execute(
            text(
                f"""
                SELECT cliente
                FROM {customers}
                WHERE cliente IS NOT NULL AND TRIM(cliente) <> ''
                ORDER BY cliente ASC
                LIMIT :lim;
                """
            ),
            {"lim": int(limit)},
        ).mappings().all()
        return [str(r["cliente"]) for r in rows]


def get_customer_ids_bulk(engine: Engine, *, clientes: list[str]) -> dict[str, dict[str, str]]:
    """
    Bulk fetch user_id/profile_id for many customers.
    Returns: {cliente: {user_id, profile_id}}
    """
    names = [str(c).strip() for c in (clientes or []) if str(c).strip()]
    if not names:
        return {}
    tn = _table_names(engine)
    customers = str(tn["customers"])

    # Build portable IN (...) query (SQLite + Postgres via SQLAlchemy text).
    placeholders = ", ".join([f":n{i}" for i in range(len(names))])
    params = {f"n{i}": names[i] for i in range(len(names))}

    with engine.begin() as con:
        rows = con.execute(
            text(
                f"""
                SELECT cliente, COALESCE(user_id, '') AS user_id, COALESCE(profile_id, '') AS profile_id
                FROM {customers}
                WHERE cliente IN ({placeholders});
                """
            ),
            params,
        ).mappings().all()

    out: dict[str, dict[str, str]] = {}
    for r in rows:
        out[str(r["cliente"])] = {
            "user_id": str(r.get("user_id") or "").strip(),
            "profile_id": str(r.get("profile_id") or "").strip(),
        }
    return out


def upsert_session(con, *, session_id: str, created_at: str, label: str, source: str, sessions_table: str) -> None:
    con.execute(
        text(
            f"""
            INSERT INTO {sessions_table}(id, created_at, label, source)
            VALUES(:id, :created_at, :label, :source)
            ON CONFLICT(id) DO UPDATE SET
              created_at=excluded.created_at,
              label=excluded.label,
              source=excluded.source;
            """
        ),
        {"id": session_id, "created_at": created_at, "label": label or "", "source": source or ""},
    )


def replace_session_items(
    con,
    *,
    session_id: str,
    rows: list[dict[str, Any]],
    items_table: str,
    customers_table: str,
) -> None:
    # Full replace: delete existing then insert.
    con.execute(text(f"DELETE FROM {items_table} WHERE session_id = :sid;"), {"sid": session_id})
    created_at = _utc_now_iso()
    for r in rows:
        _ensure_customer_row(con, cliente=str(r.get("Cliente") or ""), customers_table=customers_table)
        con.execute(
            text(
                f"""
                INSERT INTO {items_table}(
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
        tn = _table_names(engine)
        upsert_session(
            con,
            session_id=session_id,
            created_at=created_at,
            label=label,
            source=source,
            sessions_table=str(tn["sessions"]),
        )
        replace_session_items(
            con,
            session_id=session_id,
            rows=merged_rows,
            items_table=str(tn["items"]),
            customers_table=str(tn["customers"]),
        )


def list_sessions(engine: Engine, limit: int = 50) -> list[dict[str, Any]]:
    url = str(engine.url)
    is_sqlite = url.startswith("sqlite")
    schema = _validate_schema_name(_get_schema_name())
    sessions = _tn("sessions", schema=schema, is_sqlite=is_sqlite)
    with engine.begin() as con:
        rows = con.execute(
            text(
                f"""
                SELECT id, created_at, label, source
                FROM {sessions}
                ORDER BY created_at DESC
                LIMIT :lim;
                """
            ),
            {"lim": int(limit)},
        ).mappings().all()
        return [dict(r) for r in rows]


def list_customers(engine: Engine, limit: int = 5000) -> list[str]:
    url = str(engine.url)
    is_sqlite = url.startswith("sqlite")
    schema = _validate_schema_name(_get_schema_name())
    items = _tn("items", schema=schema, is_sqlite=is_sqlite)
    with engine.begin() as con:
        rows = con.execute(
            text(
                f"""
                SELECT cliente
                FROM {items}
                WHERE cliente IS NOT NULL AND TRIM(cliente) <> ''
                GROUP BY cliente
                ORDER BY cliente ASC
                LIMIT :lim;
                """
            ),
            {"lim": int(limit)},
        ).mappings().all()
        return [str(r["cliente"]) for r in rows]


def customer_history(engine: Engine, *, cliente: str, limit: int = 3000) -> list[dict[str, Any]]:
    url = str(engine.url)
    is_sqlite = url.startswith("sqlite")
    schema = _validate_schema_name(_get_schema_name())
    items = _tn("items", schema=schema, is_sqlite=is_sqlite)
    sessions = _tn("sessions", schema=schema, is_sqlite=is_sqlite)
    with engine.begin() as con:
        rows = con.execute(
            text(
                f"""
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
        ).mappings().all()
        return [dict(r) for r in rows]


def customer_stats(engine: Engine, *, cliente: str) -> dict[str, Any]:
    url = str(engine.url)
    is_sqlite = url.startswith("sqlite")
    schema = _validate_schema_name(_get_schema_name())
    items = _tn("items", schema=schema, is_sqlite=is_sqlite)
    sessions = _tn("sessions", schema=schema, is_sqlite=is_sqlite)
    with engine.begin() as con:
        r = con.execute(
            text(
                f"""
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
    schema = _validate_schema_name(_get_schema_name())
    items = _tn("items", schema=schema, is_sqlite=is_sqlite)
    with engine.begin() as con:
        rows = con.execute(
            text(
                f"""
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
        ).mappings().all()
        out = [dict(r) for r in rows]
        for r in out:
            r["vezes"] = int(r.get("vezes") or 0)
            r["quantidade_total"] = float(r.get("quantidade_total") or 0.0)
        return out


def customer_sessions(engine: Engine, *, cliente: str, limit: int = 50) -> list[dict[str, Any]]:
    url = str(engine.url)
    is_sqlite = url.startswith("sqlite")
    schema = _validate_schema_name(_get_schema_name())
    items = _tn("items", schema=schema, is_sqlite=is_sqlite)
    sessions = _tn("sessions", schema=schema, is_sqlite=is_sqlite)
    with engine.begin() as con:
        rows = con.execute(
            text(
                f"""
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
        ).mappings().all()
        out = [dict(r) for r in rows]
        for r in out:
            r["total_spent"] = float(r.get("total_spent") or 0.0)
        return out


def session_exists(engine: Engine, *, session_id: str) -> bool:
    url = str(engine.url)
    is_sqlite = url.startswith("sqlite")
    schema = _validate_schema_name(_get_schema_name())
    sessions = _tn("sessions", schema=schema, is_sqlite=is_sqlite)
    with engine.begin() as con:
        r = con.execute(
            text(f"SELECT 1 FROM {sessions} WHERE id = :id LIMIT 1;"),
            {"id": str(session_id or "").strip()},
        ).first()
        return r is not None


