"""
fsspec-compatible filesystem backed by a SQL database.
"""

from __future__ import annotations

import io
import threading
from datetime import datetime
from typing import Any

from banal import ensure_dict
from fsspec.spec import AbstractFileSystem
from sqlalchemy import (
    Column,
    DateTime,
    Integer,
    LargeBinary,
    MetaData,
    Table,
    Unicode,
    create_engine,
    delete,
    func,
    insert,
    select,
    update,
)
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.postgresql import insert as psql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.pool import StaticPool

from anystore.functools import weakref_cache as cache

TABLE_NAME = "anystore"
POOL_SIZE = 5


@cache
def _get_engine(url: str, **kwargs) -> Engine:
    if ":memory:" in url:
        kwargs.setdefault("poolclass", StaticPool)
        kwargs.setdefault("connect_args", {"check_same_thread": False})
    elif "pool_size" not in kwargs:
        kwargs["pool_size"] = POOL_SIZE
    return create_engine(url, **kwargs)


def _get_insert(engine: Engine):
    dialect = engine.dialect.name
    if dialect == "sqlite":
        return sqlite_insert
    if dialect == "mysql":
        return mysql_insert
    if dialect in ("postgresql", "postgres"):
        return psql_insert
    raise RuntimeError(f"Unsupported database dialect: {dialect}")


def _make_table(name: str, metadata: MetaData) -> Table:
    return Table(
        name,
        metadata,
        Column("key", Unicode(), primary_key=True, unique=True, index=True),
        Column("value", LargeBinary(), nullable=True),
        Column(
            "timestamp",
            DateTime(timezone=True),
            server_default=func.now(),
        ),
        Column("ttl", Integer(), nullable=True),
    )


class SqlFileSystem(AbstractFileSystem):
    """A flat key-value filesystem stored in a SQL database.

    Directories are emulated: any key containing ``/`` implicitly creates
    parent "directories".

    Parameters
    ----------
    url : str
        SQLAlchemy connection URL (e.g. ``sqlite:///my.db``,
        ``postgresql://user:pass@host/db``).
    table : str
        Table name.  Defaults to ``"anystore"``.
    engine_kwargs : dict
        Extra keyword arguments forwarded to ``create_engine``.
    """

    protocol = "sql"
    root_marker = ""

    def __init__(
        self,
        url: str = "sqlite:///:memory:",
        table: str = TABLE_NAME,
        engine_kwargs: dict[str, Any] | None = None,
        **storage_options,
    ):
        super().__init__(
            url=url,
            table=table,
            engine_kwargs=engine_kwargs,
            **storage_options,
        )
        engine_kwargs = ensure_dict(engine_kwargs)
        self._engine = _get_engine(url, **engine_kwargs)
        self._insert_func = _get_insert(self._engine)
        metadata = MetaData()
        self._table = _make_table(table, metadata)
        metadata.create_all(self._engine, tables=[self._table], checkfirst=True)
        self._local = threading.local()

    def _get_conn(self) -> Connection:
        if not hasattr(self._local, "conn"):
            self._local.conn = self._engine.connect()
        return self._local.conn

    # ------------------------------------------------------------------
    # Required: ls
    # ------------------------------------------------------------------

    def ls(self, path: str, detail: bool = True, **kwargs) -> list:  # type: ignore[override]
        path = self._strip_protocol(path).strip("/")

        conn = self._get_conn()
        prefix = f"{path}/" if path else ""
        stmt = select(self._table.c.key, self._table.c.value, self._table.c.timestamp)
        if prefix:
            stmt = stmt.where(self._table.c.key.like(f"{prefix}%"))

        rows = conn.execute(stmt).fetchall()

        entries: dict[str, dict] = {}
        for key, value, ts in rows:
            key = key.strip("/")
            if prefix:
                relative = key[len(prefix) :]
            else:
                relative = key

            if "/" in relative:
                # this is a nested key – surface the immediate child dir
                child = relative.split("/", 1)[0]
                child_path = f"{prefix}{child}" if prefix else child
                if child_path not in entries:
                    entries[child_path] = {
                        "name": child_path,
                        "size": 0,
                        "type": "directory",
                    }
            else:
                entries[key] = {
                    "name": key,
                    "size": len(value) if value is not None else 0,
                    "type": "file",
                    "created": ts,
                }

        result = list(entries.values())
        if not detail:
            return [e["name"] for e in result]
        return result

    # ------------------------------------------------------------------
    # info – override for efficiency (avoids ls on parent)
    # ------------------------------------------------------------------

    def info(self, path: str, **kwargs) -> dict:
        path = self._strip_protocol(path).strip("/")
        if not path:
            return {"name": "", "size": 0, "type": "directory"}

        conn = self._get_conn()
        stmt = select(
            self._table.c.key, self._table.c.value, self._table.c.timestamp
        ).where(self._table.c.key == path)
        row = conn.execute(stmt).first()
        if row:
            _, value, ts = row
            return {
                "name": path,
                "size": len(value) if value is not None else 0,
                "type": "file",
                "created": ts,
            }

        # Check if path is an implicit directory
        prefix = f"{path}/"
        stmt = (
            select(self._table.c.key)
            .where(self._table.c.key.like(f"{prefix}%"))
            .limit(1)
        )
        row = conn.execute(stmt).first()
        if row:
            return {"name": path, "size": 0, "type": "directory"}

        raise FileNotFoundError(path)

    # ------------------------------------------------------------------
    # _open – return a file-like object
    # ------------------------------------------------------------------

    def _open(  # type: ignore[override]
        self,
        path: str,
        mode: str = "rb",
        **kwargs,
    ) -> io.BytesIO | SqlFileWriter:
        path = self._strip_protocol(path).strip("/")
        if "r" in mode:
            conn = self._get_conn()
            stmt = select(self._table.c.value).where(self._table.c.key == path)
            row = conn.execute(stmt).first()
            if row is None:
                raise FileNotFoundError(path)
            data = row[0] or b""
            return io.BytesIO(data)
        else:
            return SqlFileWriter(self, path)

    # ------------------------------------------------------------------
    # pipe_file / cat_file – efficient bulk read/write
    # ------------------------------------------------------------------

    def cat_file(self, path: str, start=None, end=None, **kwargs) -> bytes:
        path = self._strip_protocol(path).strip("/")
        conn = self._get_conn()
        stmt = select(self._table.c.value).where(self._table.c.key == path)
        row = conn.execute(stmt).first()
        if row is None:
            raise FileNotFoundError(path)
        data = row[0] or b""
        if start is not None or end is not None:
            data = data[start:end]
        return data

    def pipe_file(self, path: str, value: bytes, mode="overwrite", **kwargs) -> None:
        path = self._strip_protocol(path).strip("/")
        if mode == "create" and self.exists(path):
            raise FileExistsError(path)
        self._upsert(path, value)

    def _upsert(self, key: str, value: bytes, ttl: int | None = None) -> None:
        conn = self._get_conn()
        stmt = insert(self._table).values(key=key, value=value, ttl=ttl)
        try:
            conn.execute(stmt)
        except Exception:
            stmt = (
                update(self._table)
                .where(self._table.c.key == key)
                .values(value=value, ttl=ttl)
            )
            conn.execute(stmt)
        conn.commit()

    # ------------------------------------------------------------------
    # rm_file
    # ------------------------------------------------------------------

    def rm_file(self, path: str) -> None:
        path = self._strip_protocol(path).strip("/")
        conn = self._get_conn()
        stmt = delete(self._table).where(self._table.c.key == path)
        conn.execute(stmt)
        conn.commit()

    def _rm(self, path: str) -> None:
        self.rm_file(path)

    # ------------------------------------------------------------------
    # mkdir / makedirs – no-op for flat store
    # ------------------------------------------------------------------

    def mkdir(self, path: str, create_parents: bool = True, **kwargs) -> None:
        pass

    def makedirs(self, path: str, exist_ok: bool = False) -> None:
        pass

    # ------------------------------------------------------------------
    # Protocol helpers
    # ------------------------------------------------------------------

    @classmethod
    def _strip_protocol(cls, path: str) -> str:
        if path.startswith("sql://"):
            path = path[len("sql://") :]
        return path.strip("/")

    def created(self, path: str) -> datetime | None:
        info = self.info(path)
        return info.get("created")


class SqlFileWriter(io.BytesIO):
    """Write buffer that flushes to SQL on close."""

    def __init__(self, fs: SqlFileSystem, path: str):
        super().__init__()
        self._fs = fs
        self._path = path

    def close(self) -> None:
        if not self.closed:
            self._fs._upsert(self._path, self.getvalue())
        super().close()
