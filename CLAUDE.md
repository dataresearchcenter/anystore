# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Environment

Use the `.venv` virtualenv for all Python/poetry commands:
```bash
.venv/bin/python -m <module>
.venv/bin/pytest ...
```

## Installing packages

Use the `.venv` virtualenv pip directly instead of `make install` (poetry is not on PATH):
```bash
.venv/bin/pip install -e ".[sql,redis,api]"
```

## Common Commands

```bash
# Install dependencies (all extras)
# make install  # requires poetry on PATH; use .venv/bin/pip instead

# Run all tests
make test

# Run a single test file
.venv/bin/pytest tests/test_store.py -v

# Run a specific test
.venv/bin/pytest tests/test_store.py::test_function_name -v

# Linting
make lint

# Type checking
make typecheck

# Pre-commit hooks
make pre-commit

# Build package
make build

# Build + publish docs (zensical, then s3 sync)
make documentation
```

## Architecture

**anystore** is a unified key-value storage interface supporting multiple backends (local filesystem, S3, Redis, SQL, HTTP, memory) via fsspec.

### Layer Hierarchy (strict top-down imports)

```
Layer 0:  types, exceptions
Layer 1:  settings
Layer 2:  logging
Layer 3:  logic/        — pure business logic (constants, serialize, uri, io, virtual)
Layer 4:  util/         — checksum, data helpers, misc
Layer 5:  model/        — pydantic models (StoreModel, Info, Stats, BaseModel)
Layer 6:  fs/           — custom fsspec filesystem implementations
Layer 7:  store/        — Store, Keys, UriResource, virtual store
Layer 8:  io/           — smart_read/write/stream, Writer, SmartHandler
Layer 9:  decorators.py — @anycache, @error_handler
Layer 10: interface/    — Queue, Tags, Lock, RateLimit
Layer 11: api/          — FastAPI REST API
Layer 12: cli.py, __init__.py
```

Each layer may only import from layers above it (lower number). Lazy imports
are used sparingly for circular breaks at layer boundaries (e.g.
`model/base.py` lazily imports `io.smart_read`), as are `TYPE_CHECKING`-only
imports (e.g. `logic/io.py` type-hints `Store`).

### Package Layout

- **`logic/`** — Pure business logic, no store/fs dependencies
  - `constants.py` — `CHUNK_SIZE` (from `shutil.COPY_BUFSIZE`), `DEFAULT_MODE`, scheme constants
  - `io.py` — `stream`, `stream_bytes`, `iter_lines` (low-level I/O primitives)
  - `serialize.py` — Serialization modes (`auto`, `json`, `pickle`, `raw`)
  - `uri.py` — URI parsing, validation, `UriHandler`
  - `virtual.py` — `VirtualIO` wrapper class
- **`util/`** — Utility functions (re-exports all public names from submodules and `logic/uri` via `__init__.py`)
  - `checksum.py` — `make_checksum`, `make_data_checksum`, `make_signature_key`, `make_uri_key`
  - `data.py` — `clean_dict`, `dict_merge`, `model_dump`, `pydantic_merge`, json/yaml dumpers
  - `misc.py` — `rm_rf`, `mask_uri`, `ensure_uuid`, `get_extension`, `guess_mimetype`, `Took`
- **`model/`** — Pydantic models
  - `base.py` — `BaseModel` plus json/yaml/remote mixins
  - `store.py` — `StoreModel` (store configuration)
  - `info.py` — `Info` (normalized `fs.info()`) and `Stats`
- **`fs/`** — Custom fsspec filesystem implementations
  - `local.py` — `AnyLocalFileSystem` (overrides `file://`/`local://` with fast `exists` and lazy `iter_find`)
  - `api.py` — `ApiFileSystem` (`anystore+http(s)://` scheme)
  - `redis.py` — `RedisFileSystem` (falls back to fakeredis when `REDIS_DEBUG` is set)
  - `sql.py` — `SqlFileSystem` (SQLite, PostgreSQL, MySQL)
- **`store/`** — Main store interface
  - `base.py` — `Store` class (main entry point, extends `StoreModel`)
  - `__init__.py` — `get_store(uri)` factory with a process-wide store cache
  - `keys.py` — `Keys`: relative/absolute key conversion per backend
  - `resource.py` — `UriResource`, key-bound facade over `Store`
  - `virtual.py` — `get_virtual_store()`, temporary store for downloads
- **`io/`** — User-facing I/O (package with submodules)
  - `handler.py` — `SmartHandler`, `smart_open`
  - `read.py` — `smart_read`, `smart_stream`, `smart_stream_csv/json`, model variants, `open_virtual`
  - `write.py` — `smart_write`, `Writer`, `ModelWriter`, csv/json variants
  - `logging.py` — `logged_items` (tqdm/structlog progress wrapper)
- **`decorators.py`** — `@anycache`, `@async_anycache`, `@error_handler`, `@async_error_handler`
- **`interface/`** — Higher-level abstractions: `Queue`/`Queues`, `Tags`, `Lock`, `RateLimit`, plus cached
  `get_tags` / `get_lock` / `get_queue` / `get_rate_limit` factories in `__init__.py`
- **`api/`** — FastAPI REST API for exposing a store over HTTP
  - `app.py` — `create_app(store)` factory
  - `routes.py` — CRUD endpoints (GET/PUT/DELETE/HEAD/PATCH)
  - `util.py` — Streaming helpers (chunked reads, range parsing)
- **`cli.py`** — Typer CLI (`anystore` command)

### Store Pattern

1. `get_store(uri)` dispatches to the appropriate backend based on URI scheme
2. `Store._fs` lazily initializes the fsspec filesystem via `fsspec.url_to_fs()` (a `cached_property`, as is `Store._keys`)
3. `store/keys.py` handles conversion between relative user keys and absolute backend keys (each backend has different key prefix semantics)
4. `get_store()` caches stores in a process-wide dict guarded by a lock, keyed by a checksum of `(uri, kwargs)`; the backend fs is instantiated eagerly once so a missing optional dependency raises `ImportError` at store creation
5. Serialization modes (`auto`, `json`, `pickle`, `raw`) are handled in `logic/serialize.py`

### Import Rules

- `logic/` and `util/` are leaf layers — they must not import from `model/`, `fs/`, `store/`, `io/`, or higher
- `util/__init__.py` re-exports all public names from submodules and `logic/uri` (e.g. `from anystore.util import clean_dict`)
- `io/__init__.py` re-exports the public API for convenience (`from anystore.io import smart_read`)
- Lazy imports are only acceptable to break circular dependencies at layer boundaries

### API + ApiFileSystem

The API module exposes any store over HTTP. `ApiFileSystem` is a fsspec filesystem that talks to this API, registered as `anystore+http(s)://`. It subclasses fsspec's `HTTPFileSystem` so standard HTTP operations (range reads, seekable files) are handled natively — only listing, writes, and deletes are overridden. All reads and writes stream in chunks to avoid buffering large blobs in memory.

### Entry Points

- **CLI**: `anystore` command via `anystore.cli:cli`
- **Python**: `from anystore import get_store, anycache, smart_read, smart_write`
- **Config**: Environment variables with `ANYSTORE_` prefix (base settings like `DEBUG`, `REDIS_DEBUG`, `LOG_LEVEL` are unprefixed)
- **fsspec**: Custom schemes registered via entry points in `pyproject.toml` — `file`/`local` (overriding fsspec's own `LocalFileSystem`), `sql`/`sqlite`/`mysql`/`postgresql`, `redis`, `anystore+http(s)`

### Testing Notes

- `pyproject.toml`'s `[tool.pytest_env]` sets `REDIS_DEBUG=1`, so redis tests run against **fakeredis** — no local Redis server needed despite the `redis://localhost` URIs
- `conftest.py` autouse fixtures spawn a `http.server` on port **8000** (serving `tests/fixtures`) and a moto S3 server on port **8888**; both ports must be free
- S3 tests use `moto` for AWS mocking
- `tests/fs_shared.py` holds the shared filesystem test suite, imported by each `test_fs_*.py` with backend-specific `fs`/`key` fixtures
- API filesystem tests start a real uvicorn server with `port=0` for random port allocation
- `test_store.py::_test_store()` is a shared roundtrip test exercised against every backend
- `test_store_sql` child store prefix test is a known limitation (SQL URIs are ambiguous about file path vs key prefix)
