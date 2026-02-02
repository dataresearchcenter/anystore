`anystore` provides a simple high-level interface for _retrieving_ and _writing_ data in a _store_. Data is identified by a _key_. A diverse range of storage _backends_ is supported.

This library is a collection of shared basic features used for [projects](https://dataresearchcenter.io/projects) and [applications](https://github.com/dataresearchcenter/) related to data storage, data streaming and data engineering.

## Use cases

- [x] A simple storage interface for multiple tenants
- [x] A shared cache for collaborative data analysis work
- [x] A caching implementation, e.g. for an [api](https://docs.investigraph.dev/lib/ftmq-api)
- [x] Read and write data from different local or remote backends
- [x] Crawl documents from various remote endpoints

## Basic features

- [x] List available keys, optionally by glob or prefix filters
- [x] Get data at a given key
- [x] Write data to a given key
- [x] Delete data at a given key
- [x] Stream data line by line
- [x] Read and write contents with a `BinaryIO` handler
- [x] Serialize data in different ways, including `pydantic` models

### Limitations

The goal of `anystore` is to provide a simple high-level interface that works the same way for multiple backends. This allows to develop applications with configurable or swappable backends, depending on the actual use case and scalability requirements. This has, on purpose, some limitations or issues to consider:

- [ ] Writing data just overwrites existing keys without checking
- [ ] No logic for _renaming_ keys or _moving_ data
- [ ] No logic for _changing_ (e.g. appending) data to an existing key
- [ ] More backend specific features, like `SET` datatype in redis or queries in sql are not supported via this interface

## Supported backends

- [x] All file-like backends supported by `fsspec` ([here](https://filesystem-spec.readthedocs.io/en/latest/api.html#built-in-implementations) and [here](https://filesystem-spec.readthedocs.io/en/latest/api.html#other-known-implementations))
- [x] Sql via [sqlalchemy](https://www.sqlalchemy.org/) such as sqlite, postgres, mysql
- [x] Redis compatible (redis or e.g. [kvrocks](https://kvrocks.apache.org/))
- [x] A simple in-memory implementation

## Shared utilities

As well this package contains a lot of [useful helpers](./reference/util.md)
