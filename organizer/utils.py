import hashlib
from datetime import datetime, timezone


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def safe_session_id(ts_iso: str) -> str:
    return ts_iso.replace(":", "").replace("-", "").replace("+", "Z")


def template_version(intro: str, total_line_template: str, outro: str) -> str:
    payload = (intro or "") + "\n" + (total_line_template or "") + "\n" + (outro or "")
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:8]

