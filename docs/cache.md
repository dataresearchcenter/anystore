`anystore` can be used as a simple cache backend and provides some useful helpers for caching parts of an application. For performance reasons, the storage backend should be a `redis` instance or a `sql database`, but of course any other (e.g. file-like) backend could be configured.

All high level functions described in the [basic usage](./usage.md) of course work here as well.

## Set up

Assuming a `redis` server is running at localhost, `anystore` could be configured via environment vars (see [configuration](./configuration.md)) or store initialization at runtime.

```bash
export ANYSTORE_URI=redis://localhost

# takes any valid redis connection uri string:
export ANYSTORE_URI=redis://user:password@host:port
```

A useful example configuration for a 1 hour redis cache (assuming all data is json serializable) could look like this:

```python
from anystore import get_store

store = get_store(
    uri="redis://localhost",
    serialization_mode="json",
    raise_on_nonexist=False,  # just return `None` for missing data
    default_ttl=3600,
    backend_config={
        "redis_prefix": "my-cache-001"
    }
)
```

All these options could be configured with these environment variables:

```bash
ANYSTORE_URI=redis://localhost
ANYSTORE_SERIALIZATION_MODE=json
ANYSTORE_RAISE_ON_NONEXIST=0
ANYSTORE_DEFAULT_TTL=3600
ANYSTORE_BACKEND_CONFIG__REDIS_PREFIX=my-cache-001
```

## Use the cache

Simply set up the store and store and retrieve data from it:

```python
from anystore import get_store

# if configured via env var
store = get_store()
# or configure during runtime
store = get_store("redis://localhost")

# store data valid for 1 hour
store.put("cache_key", data, ttl=3600)

# retrieve and remove from cache
res = store.pop("cache_key")

```

### `anycache` decorator

Decorate a function with it to retrieve the cached result on next call. By default, the cache key is computed from the input arguments (see below for configuration).

```python
from anystore import anycache

@anycache()
def calculate(data, ttl=3600):
    # a very time consuming task
    return result

# 1. time: slow
res = calculate(100)

# 2. time: fast, as now cached
res = calculate(100)

```

### decorator options

Most notably the `store` to use for the decorator and a `key_func` to compute the cache key based on the input.

```python
from anystore import get_store, anycache

store = get_store("s3://slow-cache")

# transform the input `name` to uppercase for the cache key
@anycache(store=store, key_func: lambda x: x.upper())
def get_result(name):
    # do something
    return result

```

For all the parameters, [see reference](./reference/decorators.md)

## Example

[`@anycache` is used for api view cache in `ftmq-api`](https://github.com/dataresearchcenter/ftmstore-fastapi/blob/main/ftmstore_fastapi/views.py)

```python
from anystore import get_store, anycache

cache = get_store("redis://localhost")

@anycache(store=cache, key_func=lambda q: f"api/list/{q.make_key()}", ttl=60)
def get_list_view(q: Query) -> Response:
    result = ... # complex computing will be cached
    return result
```
