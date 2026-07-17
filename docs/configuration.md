The store backends can be configured via environment vars and/or overridden during runtime initialization.

## Initializing a configurable store

Initialize a store with the configured defaults from environment:

```python
from anystore import get_store

store = get_store()
```

Initialize a store with override options:

```python
from anystore import get_store

store = get_store(
    uri="redis://localhost/my-prefix",
    default_ttl=3600,
)
```

### Environment vars

All the keyword arguments to [`get_store`][anystore.store.get_store] can be configured as default values via environment vars, using uppercase names and `ANYSTORE_` prefix.

Nested config dicts (such as `backend_config`) can be configured via environment vars as well, using `__` as a nesting separator:

```bash
ANYSTORE_BACKEND_CONFIG__ANON=true
```

Environment vars values will be casted based on their type (e.g. "0" for "False", "yes" for "True").

## Configuration

The actual backend is inferred from the uri scheme. If the uri is a relative path, the scheme will be `file` and therefore the store a local file storage backend.

Backend-specific settings can be passed through via `backend_config` or `ANYSTORE_BACKEND_CONFIG__*`

| Setting                                                | init keyword         | env var                       | Default    |
| ------------------------------------------------------ | -------------------- | ----------------------------- | ---------- |
| Base store uri                                         | `uri`                | `ANYSTORE_URI`                | ./anystore |
| [Serialization mode](./serialization.md)               | `serialization_mode` | `ANYSTORE_SERIALIZATION_MODE` | auto       |
| Error handling for non-existing keys                   | `raise_on_nonexist`  | `ANYSTORE_RAISE_ON_NONEXIST`  | `True`     |
| Key ttl for backends that support it (e.g. redis, sql) | `default_ttl`        | `ANYSTORE_DEFAULT_TTL`        | 0          |

## File-like backends

`anystore` is built on top of [`fsspec`](https://filesystem-spec.readthedocs.io/en/latest/index.html), and therefore any supported backend is configurable. For some backends, additional libraries need to be installed. Refer to their documentation.

The backend is inferred by the `scheme` of the uri.

### Commonly used

```bash
ANYSTORE_URI=./local-data                          # relative local path
ANYSTORE_URI=file:///var/data                      # absolute local path
ANYSTORE_URI=s3://my_bucket                        # AWS or S3 like
ANYSTORE_URI=s3://my_bucket/prefix/foo             # Sub path in S3
ANYSTORE_URI=gcs://my_bucket                       # Google cloud storage
ANYSTORE_URI=sftp://my_server.net:/var/lib/data    # Connect via ssh
ANYSTORE_URI=https://example.org/files             # Read-only via file listing
```

## Backend specific config

### Redis

`uri` / `ANYSTORE_URI` can be a full redis connection string, e.g.:

    redis://user:password@hostname:port

A key prefix for all stored data can be set via the uri path:

    redis://localhost/my-prefix

A numeric first path segment selects the redis database instead of acting as a key prefix:

    redis://localhost/1            # use redis db 1
    redis://localhost/1/my-prefix  # use redis db 1, prefix keys with "my-prefix"

#### debug mode (fakeredis)

For development purposes, fake redis via `REDIS_DEBUG=1` in environment.

### S3 like

Configure parameters via `client_kwargs` in `backend_config`, e.g.:

```python
from anystore import get_store

store = get_store(uri="s3://mybucket/data", backend_config={"client_kwargs": {
    "aws_access_key_id": "my-key",
    "aws_secret_access_key": "***",
    "endpoint_url": "https://s3.local"
}})
```

Or via environment vars:

```bash
AWS_ACCESS_KEY_ID=***
AWS_SECRET_ACCESS_KEY=***
FSSPEC_S3_ENDPOINT_URL=https://s3.local
```

### SQL

`uri` / `ANYSTORE_URI` can be a full sql connection string:

```bash
# absolute
sqlite:////var/data.db
# relative to current wd
sqlite:///data.db

# postgres with credentials and ssl params
postgresql://user:password@hostname:port/database?sslmode=verify-full
```

Configure parameters via `sql` in `backend_config` or env `ANYSTORE_BACKEND_CONFIG__SQL__*`

#### sql settings and their defaults

```python
from anystore import get_store

store = get_store(uri="postgresql:///db", backend_config={"sql": {
    "table": "anystore",
    "pool_size": 5,
    "engine_kwargs": {}  # sqlalchemy kwargs
}})

```

```bash
ANYSTORE_BACKEND_CONFIG__SQL__TABLE=anystore
ANYSTORE_BACKEND_CONFIG__SQL__POOL_SIZE=5
```
