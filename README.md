[![anystore on pypi](https://img.shields.io/pypi/v/anystore)](https://pypi.org/project/anystore/)
[![Python test and package](https://github.com/dataresearchcenter/anystore/actions/workflows/python.yml/badge.svg)](https://github.com/dataresearchcenter/anystore/actions/workflows/python.yml)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit)](https://github.com/pre-commit/pre-commit)
[![Coverage Status](https://coveralls.io/repos/github/dataresearchcenter/anystore/badge.svg?branch=main)](https://coveralls.io/github/dataresearchcenter/anystore?branch=main)
[![AGPLv3+ License](https://img.shields.io/pypi/l/anystore)](./LICENSE)

# anystore

Store anything anywhere. `anystore` provides a high-level storage and retrieval interface for various supported _store_ backends, such as `redis`, `sql`, `file`, `http`, cloud-storages and anything else supported by [`fsspec`](https://filesystem-spec.readthedocs.io/en/latest/index.html).

Think of it as a `key -> value` store, and `anystore` acts as a cache backend. And when _keys_ become filenames and _values_ become byte blobs, `anystore` becomes actually a file-like storage backend – but always with the same and interchangeable interface.

### Why?

[In our several data engineering projects](https://dataresearchcenter.org/projects) we always wrote boilerplate code that handles the featureset of `anystore` but not in a reusable way. This library shall be a stable foundation for data wrangling related python projects.

### Examples

#### Base cli interface:

```shell
anystore -i ./local/foo.txt -o s3://mybucket/other.txt

echo "hello" | anystore -o sftp://user:password@host:/tmp/world.txt

anystore -i https://dataresearchcenter.org > index.html

anystore --store sqlite:///db keys <prefix>

anystore --store redis://localhost put foo "bar"

anystore --store redis://localhost get foo  # -> "bar"
```
#### Use in your applications:

```python
from anystore import smart_read, smart_write

data = smart_read("s3://mybucket/data.txt")
smart_write(".local/data", data)
```

#### Simple cache example via decorator:

Use case: [`@anycache` is used for api view cache in `ftmq-api`](https://github.com/dataresearchcenter/ftmq-api/blob/main/ftmq_api/views.py)

```python
from anystore import get_store, anycache

cache = get_store("redis://localhost")

@anycache(store=cache, key_func=lambda q: f"api/list/{q.make_key()}", ttl=60)
def get_list_view(q: Query) -> Response:
    result = ... # complex computing will be cached
    return result
```

#### Mirror file collections:

```python
from anystore import get_store

source = get_store("https://example.org/documents/archive1")  # directory listing
target = get_store("s3://mybucket/files", backend_config={"client_kwargs": {
    "aws_access_key_id": "my-key",
    "aws_secret_access_key": "***",
    "endpoint_url": "https://s3.local"
}})  # can be configured via ENV as well

for path in source.iterate_keys():
    # streaming copy:
    with source.open(path) as i:
        with target.open(path, "wb") as o:
            i.write(o.read())
```

## Documentation

Find the docs at [docs.investigraph.dev/lib/anystore](https://docs.investigraph.dev/lib/anystore)

## Used by

- [ftmq](https://github.com/dataresearchcenter/ftmq), a query interface layer for [followthemoney](https://followthemoney.tech) data
- [investigraph](https://github.com/dataresearchcenter/investigraph),  a framework to manage collections of structured [followthemoney](https://followthemoney.tech) data
- [ftmq-api](https://github.com/dataresearchcenter/ftmq-api), a simple api on top off `ftmq` built with [FastApi](https://fastapi.tiangolo.com/)
- [ftm-geocode](https://github.com/dataresearchcenter/ftm-geocode), batch parse and geocode addresses from followthemoney entities
- [ftm-datalake](https://github.com/dataresearchcenter/ftm-datalake), a library to crawl, sync and move around document collections (in progress)


## Development

This package is using [poetry](https://python-poetry.org/) for packaging and dependencies management, so first [install it](https://python-poetry.org/docs/#installation).

Clone this repository to a local destination.

Within the repo directory, run

    poetry install --with dev

This installs a few development dependencies, including [pre-commit](https://pre-commit.com/) which needs to be registered:

    poetry run pre-commit install

Before creating a commit, this checks for correct code formatting (isort, black) and some other useful stuff (see: `.pre-commit-config.yaml`)

### testing

`anystore` uses [pytest](https://docs.pytest.org/en/stable/) as the testing framework.

    make test

## License and Copyright

`anystore`, (C) 2024 investigativedata.io

`anystore`, (C) 2025 investigativedata.io, Data and Researc Center – DARC

`anystore` is licensed under the AGPLv3 or later license.

Prior to version 0.3.0, `anystore` was released under the GPL-3.0 license.

see [NOTICE](./NOTICE) and [LICENSE](./LICENSE)
