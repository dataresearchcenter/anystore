[![anystore on pypi](https://img.shields.io/pypi/v/anystore)](https://pypi.org/project/anystore/)
[![Python test and package](https://github.com/investigativedata/anystore/actions/workflows/python.yml/badge.svg)](https://github.com/investigativedata/anystore/actions/workflows/python.yml)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit)](https://github.com/pre-commit/pre-commit)
[![Coverage Status](https://coveralls.io/repos/github/investigativedata/anystore/badge.svg?branch=main)](https://coveralls.io/github/investigativedata/anystore?branch=main)
[![AGPLv3+ License](https://img.shields.io/pypi/l/anystore)](./LICENSE)

# anystore

Store anything anywhere. `anystore` provides a high-level storage and retrieval interface for various supported _store_ backends, such as `redis`, `sql`, `file`, `http`, cloud-storages and anything else supported by [`fsspec`](https://filesystem-spec.readthedocs.io/en/latest/index.html).

Think of it as a `key -> value` store, and `anystore` acts as a [**cache backend**](./cache.md). And when _keys_ are filenames and _values_ are byte blobs, `anystore` becomes actually a [**file-like storage backend**](./storage.md) – but always with the same and interchangeable interface.

## Quickstart

    pip install anystore

    anystore --help

[Get started](./quickstart.md)

## Why?

[In our several data engineering projects](https://investigativedata.io/projects) we always wrote boilerplate code that handles the featureset of `anystore` but not in a reusable way. This library shall be a stable foundation for data wrangling related python projects.

## Used by

- [ftmq](https://github.com/investigativedata/ftmq), a query interface layer for [followthemoney](https://followthemoney.tech) data
- [investigraph](https://github.com/investigativedata/investigraph),  a framework to manage collections of structured [followthemoney](https://followthemoney.tech) data
- [ftmq-api](https://github.com/investigativedata/ftmq-api), a simple api on top off `ftmq` built with [FastApi](https://fastapi.tiangolo.com/)
- [ftm-geocode](https://github.com/investigativedata/ftm-geocode), batch parse and geocode addresses from followthemoney entities
- [leakrfc](https://github.com/investigativedata/leakrfc), a library to crawl, sync and move around document collections

## License and Copyright

`anystore`, (C) 2024 investigativedata.io
`anystore`, (C) 2025 investigativedata.io

`anystore` is licensed under the AGPLv3 or later license.

Prior to version 0.3.0, `anystore` was released under the GPL-3.0 license.

see [NOTICE](./NOTICE) and [LICENSE](./LICENSE)
