from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from anystore.api.routes import router
from anystore.exceptions import DoesNotExist
from anystore.store.base import Store


async def _not_found_handler(_request: Request, exc: Exception) -> JSONResponse:
    return JSONResponse(status_code=404, content={"detail": str(exc)})


def create_app(store: Store | None = None) -> FastAPI:
    app = FastAPI(docs_url=None, redoc_url="/")

    if store is None:
        from anystore.store import get_store

        store = get_store()

    app.state.store = store
    app.include_router(router)
    app.add_exception_handler(DoesNotExist, _not_found_handler)
    app.add_exception_handler(FileNotFoundError, _not_found_handler)

    return app
