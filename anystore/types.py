from datetime import datetime
from os import PathLike
from pathlib import Path
from typing import (
    Annotated,
    Any,
    AnyStr,
    Generator,
    Type,
    TypeAlias,
    TypeVar,
)

from pydantic import BaseModel, HttpUrl
from pydantic.functional_validators import BeforeValidator


def _validate_http_url(v: Any) -> str:
    """Validate as HttpUrl but return as string."""
    if isinstance(v, HttpUrl):
        return str(v)
    # Validate by parsing as HttpUrl, then return as string
    return str(HttpUrl(v))


# HttpUrl that validates as URL but is typed and behaves as plain string
HttpUrlStr = Annotated[str, BeforeValidator(_validate_http_url)]

Uri: TypeAlias = PathLike | Path | str
Value: TypeAlias = str | bytes
Model: TypeAlias = Type["BaseModel"]
M = TypeVar("M", bound="BaseModel")
V = TypeVar("V")
Raise = TypeVar("Raise", bound=bool)
SDict: TypeAlias = dict[str, Any]

TS: TypeAlias = datetime

StrGenerator: TypeAlias = Generator[str, None, None]
BytesGenerator: TypeAlias = Generator[bytes, None, None]
AnyStrGenerator: TypeAlias = Generator[AnyStr, None, None]
SDictGenerator: TypeAlias = Generator[SDict, None, None]
ModelGenerator: TypeAlias = Generator["BaseModel", None, None]
MGenerator: TypeAlias = Generator[M, None, None]
