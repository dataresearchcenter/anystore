from functools import lru_cache
from typing import Self

import orjson
import yaml
from pydantic import BaseModel as _BaseModel
from pydantic import field_validator

from anystore.logging import get_logger
from anystore.types import SDict, Uri
from anystore.util import clean_dict, model_dump

log = get_logger(__name__)


@lru_cache(128)
def cached_from_uri(uri: Uri) -> str:
    """
    Cache remote sources on runtime
    """
    from anystore.io import smart_read

    log.info("Loading `%s` ..." % uri)
    return smart_read(uri)


class JsonMixin:
    """
    Load a pydantic model from a local or remote uri with json data
    """

    @classmethod
    def from_json_str(cls, data: str, **kwargs) -> Self:
        loaded = clean_dict({**orjson.loads(data), **clean_dict(kwargs)})
        return cls(**loaded)

    @classmethod
    def from_json_uri(cls, uri: Uri, **kwargs) -> Self:
        data = cached_from_uri(uri)
        return cls.from_json_str(data, **kwargs)


class YamlMixin:
    """
    Load a pydantic model from a local or remote uri with yaml data
    """

    @classmethod
    def from_yaml_str(cls, data: str, **kwargs) -> Self:
        loaded = clean_dict({**yaml.safe_load(data), **clean_dict(kwargs)})
        return cls(**loaded)

    @classmethod
    def from_yaml_uri(cls, uri: Uri, **kwargs) -> Self:
        data = cached_from_uri(uri)
        return cls.from_yaml_str(data, **kwargs)


class RemoteMixin(JsonMixin, YamlMixin):
    @classmethod
    def _from_uri(cls, uri: Uri, **kwargs) -> Self:
        try:
            return cls.from_json_uri(uri, **kwargs)
        except orjson.JSONDecodeError:
            return cls.from_yaml_uri(uri, **kwargs)


class BaseModel(_BaseModel, RemoteMixin):
    def __hash__(self) -> int:
        return hash(repr(self.model_dump()))

    @field_validator("*", mode="before")
    @classmethod
    def empty_str_to_none(cls, v) -> str | None:
        if v == "":
            return None
        return v

    def to_dict(self) -> SDict:
        return model_dump(self, clean=True)
