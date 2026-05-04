from __future__ import annotations

import errno

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from anystore.api.routes import router
from anystore.exceptions import DoesNotExist
from anystore.store import Store, get_store


def _err(status_code: int, exc: Exception) -> JSONResponse:
    return JSONResponse(status_code=status_code, content={"detail": str(exc)})


async def _not_found_handler(_request: Request, exc: Exception) -> JSONResponse:
    return _err(404, exc)


async def _bad_request_handler(_request: Request, exc: Exception) -> JSONResponse:
    return _err(400, exc)


async def _forbidden_handler(_request: Request, exc: Exception) -> JSONResponse:
    return _err(403, exc)


async def _os_error_handler(_request: Request, exc: Exception) -> JSONResponse:
    # FileNotFoundError / PermissionError are handled by their own entries via
    # MRO lookup; this only fires for bare OSError.
    assert isinstance(exc, OSError)
    if exc.errno == errno.ENOSPC:
        return _err(507, exc)
    if exc.errno in (errno.EPERM, errno.EACCES, errno.EROFS, errno.ELOOP):
        return _err(403, exc)
    return _err(500, exc)


def create_app(store: Store | None = None) -> FastAPI:
    app = FastAPI(docs_url=None, redoc_url=None)

    if store is None:
        store = get_store()

    app.state.store = store
    app.include_router(router)
    app.add_exception_handler(DoesNotExist, _not_found_handler)
    app.add_exception_handler(FileNotFoundError, _not_found_handler)
    app.add_exception_handler(ValueError, _bad_request_handler)
    app.add_exception_handler(PermissionError, _forbidden_handler)
    app.add_exception_handler(FileExistsError, _forbidden_handler)
    app.add_exception_handler(OSError, _os_error_handler)

    return app
