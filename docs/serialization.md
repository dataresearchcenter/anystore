Data written to and read from a store can have different types and needs to be (de)serialized.

Serialization is needed when writing to the store ([`store.put`][anystore.store.base.Store.put] function), deserialization is needed when reading from a store ([`store.get`][anystore.store.base.Store.get], [`store.pop`][anystore.store.base.Store.pop] or [`store.stream`][anystore.store.base.Store.stream]).

Serialization can be configured in [store settings](./configuration.md), or during runtime.

## Defaults

Without further options, serialization mode is "auto" (see below), which makes it easy to store and retrieve arbitrary data without too much trouble. This applies to primitive data types (`str`, `int`, ...) as well as data structures that can be represented as `json`. More complex structures will be pickled via `cloudpickle` and deserialized on retrieval, but one should consider handling serialization more explicit in such cases.

Put some different data to the store:

```python
# a text string
store.put("foo", "bar")

# a number
store.put("foo", 1)

# a dictionary
store.put("foo", {"data": 1})

# a pydantic object
store.put("foo", data)

# an arbitrary object
store.put("func", lambda x: x*2)
func = stiore.get("func")
assert func(2) == 4
```

When retrieving back these values, they will be converted back to the same type (even the lambda function), **except the pydantic model**. This will be returned as the data dictionary, see below for explicitly working with pydantic models.

## Use the serialization mode

Control how data is serialized using the `mode` keyword. The four modes are:

- "raw": Return value as is, assuming bytes
- "json": Use `orjson` to (de)serialize
- "pickle": Use `cloudpickle` to (de)serialize
- "auto": Try different serialization methods, the default (see above)

```python
store.put("data", "hello")
# this will return bytes:
store.get("data", mode="raw")

# explicitly use json serialization
store.put("data", {1: 2}, mode="json")
```

## Store and retrieve pydantic models

Pass through the `model` option to work with pydantic data:

```python
data = MyPydanticModel(hello="world")
store.put("data", data, model=MyPydanticModel)
# retrieve the data as the pydantic model:
res = store.get("data", model=MyPydanticModel)
```

## Use custom functions

Use `serialization_func` and `deserialization_func` as options:

```python
# a generator cannot be saved to a store
data = range(100)

def convert(data):
    return [d for d in data]

store.put("data", data, serialization_func=convert)

# convert back to the generator
# (that's stupid, but you get the idea...)
def unconvert(data):
    return (d for d in data)

result = store.get("data", deserialization_func=unconvert)
```

Of course just lambda functions could be used here as well:

```python
store.put("double_data", 4, serialization_func=lambda x: x*2)
```

## Reference

see [reference details](./reference/serialize.md).
