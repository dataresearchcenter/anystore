Any `anystore` backend can act as a file-like blob storage. Of course, this makes most sense to _actual_ file-like backends such as a local directory or a remote s3-compatible endpoint, but technically a redis instance can act as a blob storage as well.

!!! note ""
    In general, in the blob storage use-case, store `keys` are actual file paths and the corresponding `values` are the file contents as a byte stream.

All high level functions described in the [basic usage](./usage.md) of course work here as well.

## Configure store

Configure the base uri via environment `ANYSTORE_URI` or during runtime. [See configuration](./configuration.md).

[Serialization mode](./serialization.md) must be set to `raw` as we are always dealing with the byte blobs of the files.

Set `ANYSTORE_SERIALIZATION_MODE=raw` in environment or configure during runtime:

```python
from anystore import get_store

store = get_store("./data/files", serialization_mode="raw")
```

## List and retrieve contents

Iterate through paths, optionally filtering via `prefix` or `glob`:

```python
for key in store.iterate_keys(glob="**/*.pdf"):
    print(key)

for key in store.iterate_keys(prefix="txt_files/src"):
    print(key)
```

Command line can be used, too:

```bash
anystore keys --glob "*.pdf" > files.txt
anystore keys --prefix "src" > files.txt
```

Retrieve content of a file (this is only useful for small files, consider using the file handler below for bigger blobs).

```python
# change serialization mode to "auto" to retrieve a string instead of bytes
content = store.get("path/file.txt", serialization_mode="auto")

# use "json" mode if feasible
data = store.get("data.json", serialization_mode="json")
```

Stream a file line by line:

```python
# each line will be serialized from json
for data in store.stream("data.jsonl", serialization_mode="json"):
    yield data
```

## Write (small) files

```python
content = "hello world"
store.put("data.txt", content)
```

This is particularly useful to easily upload results of a data wrangling process to remote targets:

```python
result = calculate_data()
store = get_store("s3://bucket/dataset", serialization_mode="json")
store.put("index.json", result)
```

For bigger data chunks that should be streamed, consider using the file handler below.

## Get a file handler

Similar to pythons built-in `open()`, a `BinaryIO` handler can be accessed:

```python
import csv
from anystore import get_store

store = get_store("s3://my_bucket")

with store.open("path/to/file.csv") as fh:
    yield from csv.reader(fh)
```

### Write data

```python
with store.open("file.pdf", "wb") as fh:
    fh.write(content)
```

## Delete a file

As described in the [basic usage](./usage.md), the `pop` or `delete` methods can be used to delete a file. This is obviously irreversible.

```python
store.delete("file.pdf")

print(store.pop("file2.pdf"))
```

## Copy contents from and to stores

Recursively crawl a http file listing:

```python
from anystore import get_store

source = get_store("https://example.org/files")
target = get_store("./downloads")

for path in source.iterate_keys():
    # streaming copy:
    with source.open(path) as i:
        with target.open(path, "wb") as o:
            i.write(o.read())
```

Migrate the text files from a local directory to a redis backend:

```python
from anystore import get_store

source = get_store("./local_data")
target = get_store("redis://localhost")

for path in source.iterate_keys(glob="**/*.txt"):
    # streaming copy:
    with source.open(path) as i:
        with target.open(path, "wb") as o:
            i.write(o.read())
```

Now, the content would be accessible in the redis store as it would be a file store:

```python
with target.open("foo/content.txt") as fh:
    return fh.read()
```

## Process remote files locally

### Get a handler

Download a remote file for temporary use and get an open file handler. The local file will be cleaned up when leaving the context.

```python
from anystore import get_store
from anystore.virtual import open_virtual

remote = get_store("s3://my_bucket")

for key in remote.iterate_keys(glob="*.pdf"):
    with open_virtual(key, remote) as fh:
        process_pdf(fh)
```

### Get a temporary local path

Download a remote file for temporary use and get it's local path. The local file will be cleaned up when leaving the context.

```python
from anystore import get_store
from anystore.virtual import get_virtual_path

remote = get_store("s3://my_bucket")

for key in remote.iterate_keys(glob="*.pdf"):
    with get_virtual_path(key, remote) as path:
        do_something(path)
```

[Reference](./reference/virtual.md)
