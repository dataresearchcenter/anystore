Initialize a store (see [configuration](./configuration.md)) and basic usage:

```python
from anystore import get_store

store = get_store()

# write data
store.put("key1", "my content")

# get the data
store.get("key1")

# use path-like keys:
store.put("topic/maths", 100)
store.put("topic/engineering", 200)

# iterate topics
for key in store.iterate_keys(prefix="topic"):
    print(key)
```

## Typing of values

See [serialization](./serialization.md). Per default, serialization happens automatically with some heuristics, but can be manually controlled.

```python
# strings
store.put("foo", "bar")
assert store.get("foo") == "bar"

# numbers
store.put("data", 1)
assert store.get("data") == 1

# json
store.put("data", {"foo": "bar"})
data = store.get("data")
assert data["foo"] == "bar"
```

## Write data

Data can be written with the [`put`][anystore.store.BaseStore.put] function or via a [file-like handler][anystore.store.BaseStore.open].

Any existing data will be overwritten without warning.

```python
# simple data put
store.put("key", "my content")

# use handler (for larger data streams)
with store.open("key", "wb") as fh:
    fh.write(data)
```

## Read data

Data can be read in different ways. Use [`get`][anystore.store.BaseStore.get] for just returning the value, [`pop`][anystore.store.BaseStore.pop] for deleting the entry after reading, [`stream`][anystore.store.BaseStore.stream] for reading a data stream line by line or a [file-like handler][anystore.store.BaseStore.open] for reading data similar to pythons built-in `open()`.

```python
# simple data get
data = store.get("key")

# delete after reading (similar to redis GETDEL)
data = store.pop("key")

# stream line by line
for line in store.stream("key"):
    print(line)

# use handler (for larger data streams)
with store.open("key", "rb") as fh:
    data = fh.read()
```

## Delete data

Just call the [`delete`][anystore.store.BaseStore.delete] function:

```python
store.delete("key")
```

## Handling of missing data

There are two ways to deal with missing data (a `key` that doesn't exist in the store). Either fail silently (return `None`) or raise an exception which is the default.

This behaviour can be [configured](./configuration.md) globally or per function call:

```python
from anystore.exceptions import DoesNotExist

# default behaviour with exception
try:
    data = store.get("key")
except DoesNotExist:
    data = "__missing__

# don't raise the exception
data = store.get("key", raise_on_nonexist=False)
```

## Key content info

Obtain meta information about the content (value or actual file) stored at the given key. This contains the `size` of the content (in bytes) and a `created_at` and `updated_at` timestamp. See [`Stats`][anystore.model.Stats].

```python
metadata = store.info("key")
```

## Dive deeper

Read further for using `anystore` as a [cache backend](./cache.md) or [blob storage](./storage.md).
