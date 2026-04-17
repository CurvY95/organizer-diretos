import os
import sqlite3
from datetime import datetime, timezone
from typing import Any, Optional


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def default_db_path() -> str:
    return os.path.join(os.getcwd(), "saved", "organizer.db")


def connect(db_path: Optional[str] = None) -> sqlite3.Connection:
    path = db_path or default_db_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    return con


def init_db(con: sqlite3.Connection) -> None:
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA foreign_keys=ON;")

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS customers (
          cliente TEXT PRIMARY KEY,
          notes TEXT NOT NULL DEFAULT '',
          tags TEXT NOT NULL DEFAULT '',
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );
        """
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS sessions (
          id TEXT PRIMARY KEY,
          created_at TEXT NOT NULL,
          label TEXT NOT NULL,
          source TEXT NOT NULL DEFAULT ''
        );
        """
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS items (
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

    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_items_cliente ON items(cliente);"
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_items_session ON items(session_id);"
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_sessions_created_at ON sessions(created_at);"
    )
    con.commit()


def _ensure_customer_row(con: sqlite3.Connection, *, cliente: str) -> None:
    cliente = str(cliente or "").strip()
    if not cliente:
        return
    now = _utc_now_iso()
    con.execute(
        """
        INSERT INTO customers(cliente, notes, tags, created_at, updated_at)
        VALUES(?, '', '', ?, ?)
        ON CONFLICT(cliente) DO NOTHING;
        """,
        (cliente, now, now),
    )


def get_customer_meta(con: sqlite3.Connection, *, cliente: str) -> dict[str, str]:
    _ensure_customer_row(con, cliente=cliente)
    cur = con.execute(
        "SELECT cliente, notes, tags, created_at, updated_at FROM customers WHERE cliente = ?;",
        (str(cliente or "").strip(),),
    )
    r = cur.fetchone()
    if not r:
        return {"cliente": str(cliente or "").strip(), "notes": "", "tags": "", "created_at": "", "updated_at": ""}
    return dict(r)


def upsert_customer_meta(con: sqlite3.Connection, *, cliente: str, notes: str, tags: str) -> None:
    cliente = str(cliente or "").strip()
    if not cliente:
        raise ValueError("Cliente vazio.")
    _ensure_customer_row(con, cliente=cliente)
    now = _utc_now_iso()
    con.execute(
        """
        UPDATE customers
        SET notes = ?, tags = ?, updated_at = ?
        WHERE cliente = ?;
        """,
        (str(notes or ""), str(tags or ""), now, cliente),
    )
    con.commit()


def upsert_session(con: sqlite3.Connection, *, session_id: str, created_at: str, label: str, source: str) -> None:
    con.execute(
        """
        INSERT INTO sessions(id, created_at, label, source)
        VALUES(?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
          created_at=excluded.created_at,
          label=excluded.label,
          source=excluded.source;
        """,
        (session_id, created_at, label or "", source or ""),
    )


def replace_session_items(con: sqlite3.Connection, *, session_id: str, rows: list[dict[str, Any]]) -> None:
    # Full replace: delete existing then insert.
    con.execute("DELETE FROM items WHERE session_id = ?;", (session_id,))
    created_at = _utc_now_iso()
    for r in rows:
        _ensure_customer_row(con, cliente=str(r.get("Cliente") or ""))
        con.execute(
            """
            INSERT INTO items(
              session_id, cliente, produto, quantidade, preco, total_item, comentario, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                session_id,
                str(r.get("Cliente") or ""),
                str(r.get("Produto") or ""),
                float(r.get("Quantidade") or 0.0),
                (float(r["Preco"]) if r.get("Preco") is not None else None),
                (float(r["TotalItem"]) if r.get("TotalItem") is not None else None),
                (str(r.get("Comentario")) if r.get("Comentario") is not None else None),
                created_at,
            ),
        )


def save_snapshot(
    con: sqlite3.Connection,
    *,
    session_id: str,
    created_at: str,
    label: str,
    source: str,
    merged_rows: list[dict[str, Any]],
) -> None:
    upsert_session(con, session_id=session_id, created_at=created_at, label=label, source=source)
    replace_session_items(con, session_id=session_id, rows=merged_rows)
    con.commit()


def list_sessions(con: sqlite3.Connection, limit: int = 50) -> list[dict[str, Any]]:
    cur = con.execute(
        """
        SELECT id, created_at, label, source
        FROM sessions
        ORDER BY created_at DESC
        LIMIT ?;
        """,
        (int(limit),),
    )
    return [dict(r) for r in cur.fetchall()]


def list_customers(con: sqlite3.Connection, limit: int = 5000) -> list[str]:
    cur = con.execute(
        """
        SELECT cliente
        FROM items
        WHERE cliente IS NOT NULL AND TRIM(cliente) <> ''
        GROUP BY cliente
        ORDER BY cliente ASC
        LIMIT ?;
        """,
        (int(limit),),
    )
    return [str(r["cliente"]) for r in cur.fetchall()]


def customer_history(con: sqlite3.Connection, *, cliente: str, limit: int = 3000) -> list[dict[str, Any]]:
    cur = con.execute(
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
        FROM items i
        JOIN sessions s ON s.id = i.session_id
        WHERE i.cliente = ?
        ORDER BY s.created_at DESC, i.id DESC
        LIMIT ?;
        """,
        (cliente, int(limit)),
    )
    return [dict(r) for r in cur.fetchall()]


def customer_stats(con: sqlite3.Connection, *, cliente: str) -> dict[str, Any]:
    cur = con.execute(
        """
        SELECT
          COUNT(DISTINCT i.session_id) AS sessions_count,
          COUNT(*) AS items_count,
          SUM(COALESCE(i.total_item, 0)) AS total_spent,
          MAX(s.created_at) AS last_session_at
        FROM items i
        JOIN sessions s ON s.id = i.session_id
        WHERE i.cliente = ?;
        """,
        (str(cliente or "").strip(),),
    )
    r = cur.fetchone()
    out = dict(r) if r else {}
    out["total_spent"] = float(out.get("total_spent") or 0.0)
    out["sessions_count"] = int(out.get("sessions_count") or 0)
    out["items_count"] = int(out.get("items_count") or 0)
    out["last_session_at"] = out.get("last_session_at") or ""
    return out


def customer_top_products(con: sqlite3.Connection, *, cliente: str, limit: int = 50) -> list[dict[str, Any]]:
    cur = con.execute(
        """
        SELECT
          produto,
          COUNT(DISTINCT session_id) AS vezes,
          SUM(quantidade) AS quantidade_total
        FROM items
        WHERE cliente = ?
        GROUP BY produto
        ORDER BY vezes DESC, quantidade_total DESC, produto ASC
        LIMIT ?;
        """,
        (str(cliente or "").strip(), int(limit)),
    )
    rows = [dict(r) for r in cur.fetchall()]
    for r in rows:
        r["vezes"] = int(r.get("vezes") or 0)
        r["quantidade_total"] = float(r.get("quantidade_total") or 0.0)
    return rows


def customer_sessions(con: sqlite3.Connection, *, cliente: str, limit: int = 50) -> list[dict[str, Any]]:
    cur = con.execute(
        """
        SELECT
          s.id AS session_id,
          s.created_at,
          s.label,
          s.source,
          SUM(COALESCE(i.total_item, 0)) AS total_spent
        FROM sessions s
        JOIN items i ON i.session_id = s.id
        WHERE i.cliente = ?
        GROUP BY s.id, s.created_at, s.label, s.source
        ORDER BY s.created_at DESC
        LIMIT ?;
        """,
        (str(cliente or "").strip(), int(limit)),
    )
    rows = [dict(r) for r in cur.fetchall()]
    for r in rows:
        r["total_spent"] = float(r.get("total_spent") or 0.0)
    return rows


def session_exists(con: sqlite3.Connection, *, session_id: str) -> bool:
    cur = con.execute("SELECT 1 FROM sessions WHERE id = ? LIMIT 1;", (str(session_id or "").strip(),))
    return cur.fetchone() is not None


