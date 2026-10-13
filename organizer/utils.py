import hashlib
import json
from datetime import datetime, timezone
from typing import Any


def script_json(value: Any) -> str:
    """Serialize data safely inside an HTML script element.

    JSON escaping alone leaves closing script tags meaningful to the HTML
    parser, even when they are inside a JavaScript string.
    """
    payload = json.dumps(value, ensure_ascii=False)
    for character, escaped in (
        ("<", "\\u003c"), (">", "\\u003e"), ("&", "\\u0026"),
        ("\u2028", "\\u2028"), ("\u2029", "\\u2029"),
    ):
        payload = payload.replace(character, escaped)
    return payload


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def safe_session_id(ts_iso: str) -> str:
    return ts_iso.replace(":", "").replace("-", "").replace("+", "Z")


def template_version(intro: str, total_line_template: str, outro: str) -> str:
    payload = (intro or "") + "\n" + (total_line_template or "") + "\n" + (outro or "")
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:8]
