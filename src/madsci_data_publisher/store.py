"""The data store the publisher writes into, and the credential filter in front of it.

For now the store is a local directory of Parquet files in the layout the MCP layer's
Data Exposer reads (`DATA_EXPOSER_DATA_DIR`). Every table and JSON file passes through
`redact()` on its way in, so no credential can reach the store from any code path.
Files are written atomically and only when their content changed.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from pathlib import Path
from typing import Any

# Several MADSci manager startup events carry database URLs with passwords, and the
# database version checker logs migration commands with `--db_url`. These patterns come
# from the RAPID-446 run export prototype, where they were checked against real events.
_PLACEHOLDERS = {"", "none", "null", "***redacted***", "<redacted>"}
_CREDENTIAL_URL = re.compile(r"(?i)\b([a-z][a-z0-9+.\-]*://)[^\s@/\"']*:[^\s@\"']*@")
_CREDENTIAL_ARG = re.compile(r"(?i)(--?(?:pass(?:word)?|db_url)['\",\s=]+)(?!None\b)[^'\",\s\]]+")
_CREDENTIAL_KV = re.compile(
    r"(?i)\b(password|passwd|pwd|secret|token|api_key|access_key)(['\"]?\s*[:=]\s*['\"]?)(?!None\b)[^,)\s'\"]+"
)
_CREDENTIAL_KEY = re.compile(r"(?i)(password|passwd|secret|token|api_?key|access_?key|db_url|mongo_db|document_db)")


def redact(obj: Any) -> Any:
    """A copy of `obj` with credentials replaced by `<redacted>`."""
    if isinstance(obj, dict):
        return {
            k: "<redacted>"
            if _CREDENTIAL_KEY.search(str(k)) and isinstance(v, str) and v.strip().lower() not in _PLACEHOLDERS
            else redact(v)
            for k, v in obj.items()
        }
    if isinstance(obj, list):
        return [redact(v) for v in obj]
    if isinstance(obj, str):
        obj = _CREDENTIAL_URL.sub(r"\1<redacted>@", obj)
        obj = _CREDENTIAL_ARG.sub(r"\1<redacted>", obj)
        return _CREDENTIAL_KV.sub(r"\1\2<redacted>", obj)
    return obj


class DataStore:
    def __init__(self, root: str | os.PathLike, fmt: str = "parquet") -> None:
        if fmt not in ("parquet", "json"):
            raise ValueError("fmt must be 'parquet' or 'json'")
        if fmt == "parquet":
            try:
                import pandas  # noqa: F401
                import pyarrow  # noqa: F401
            except ImportError as e:
                raise RuntimeError(
                    "Parquet output needs pandas and pyarrow: pip install 'madsci-data-publisher[parquet]'"
                ) from e
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.fmt = fmt
        self._hashes: dict[str, str] = {}

    def write_table(self, name: str, rows: list[dict[str, Any]], columns: list[str]) -> bool:
        """Write one table. Returns True if the file changed."""
        rows = redact([{c: row.get(c) for c in columns} for row in rows])
        digest = hashlib.sha256(json.dumps(rows, sort_keys=True, default=str).encode()).hexdigest()
        path = self.root / f"{name}.{self.fmt}"
        if self._hashes.get(name) == digest and path.exists():
            return False
        if self.fmt == "parquet":
            import pandas as pd

            frame = pd.DataFrame(rows, columns=columns)
            self._atomic(path, lambda p: frame.to_parquet(p, index=False))
        else:
            self._atomic(path, lambda p: Path(p).write_text(json.dumps(rows, indent=1, default=str)))
        self._hashes[name] = digest
        return True

    def read_table(self, name: str) -> list[dict[str, Any]]:
        path = self.root / f"{name}.{self.fmt}"
        if not path.exists():
            return []
        if self.fmt == "parquet":
            import pandas as pd

            return json.loads(pd.read_parquet(path).to_json(orient="records"))
        return json.loads(path.read_text())

    def write_json(self, name: str, obj: Any) -> Path:
        path = self.root / name
        text = json.dumps(redact(obj), indent=1, default=str)
        self._atomic(path, lambda p: Path(p).write_text(text))
        return path

    def read_json(self, name: str) -> dict:
        try:
            return json.loads((self.root / name).read_text())
        except (OSError, ValueError):
            return {}

    def _atomic(self, path: Path, write) -> None:
        fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
        os.close(fd)
        try:
            write(tmp)
            os.chmod(tmp, 0o644)  # mkstemp creates 0600; readers may run as another user
            os.replace(tmp, path)
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)
