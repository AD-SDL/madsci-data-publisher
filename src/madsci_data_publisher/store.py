"""The data store the publisher writes into.

For now this is a local directory of Parquet files in the layout the MCP layer's Data
Exposer reads (`DATA_EXPOSER_DATA_DIR`). Each table is written atomically and only when
its content changed, so a reader never sees a half-written file.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any


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
        rows = [{c: row.get(c) for c in columns} for row in rows]
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

            frame = pd.read_parquet(path)
            return json.loads(frame.to_json(orient="records"))
        return json.loads(path.read_text())

    def write_json(self, name: str, obj: Any) -> Path:
        path = self.root / name
        self._atomic(path, lambda p: Path(p).write_text(json.dumps(obj, indent=1, default=str)))
        return path

    def _atomic(self, path: Path, write) -> None:
        fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
        os.close(fd)
        try:
            write(tmp)
            # mkstemp creates 0600; readers of the store may run as another user.
            os.chmod(tmp, 0o644)
            os.replace(tmp, path)
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)
