# anystore

Data storage and retrieval CLI.

**Usage**:

```console
$ anystore [OPTIONS] COMMAND [ARGS]...
```

**Options**:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `--version` / `--no-version` | bool | Show version | `no-version` |
| `--store` | TEXT | Store base uri | `.anystore` |
| `--install-completion` |  | Install completion for the current shell. |  |
| `--show-completion` |  | Show completion for the current shell, to copy it or customize the installation. |  |
| `--help` |  | Show this message and exit. |  |

**Commands**:

| Name | Description |
| --- | --- |
| `get` | Get content of a `key` from a store |
| `put` | Put content for a `key` to a store |
| `keys` | Iterate keys in given store |
| `io` | Generic i/o streaming wrapper which is wrapped around `fsspec` |
| `csv2json` | Generic i/o wrapper for streaming input csv data to json objects |
| `settings` | Show current runtime settings |

## `anystore get`

Get content of a `key` from a store.

**Usage**:

```console
$ anystore get [OPTIONS] KEY
```

**Arguments**:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `KEY` | TEXT | *required* |  |

**Options**:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `-o` | TEXT |  | `-` |
| `--help` |  | Show this message and exit. |  |

## `anystore put`

Put content for a `key` to a store.

**Usage**:

```console
$ anystore put [OPTIONS] KEY [VALUE]
```

**Arguments**:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `KEY` | TEXT | *required* |  |
| `VALUE` | TEXT | Use this value instead of reading `-i` uri |  |

**Options**:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `-i` | TEXT |  | `-` |
| `--help` |  | Show this message and exit. |  |

## `anystore keys`

Iterate keys in given store.

**Usage**:

```console
$ anystore keys [OPTIONS]
```

**Options**:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `-o` | TEXT | Output uri | `-` |
| `--glob` | TEXT | Key glob |  |
| `--prefix` | TEXT | Key prefix |  |
| `--exclude-prefix` | TEXT | Exclude key prefix |  |
| `--info` / `--no-info` | bool | Print metadata | `no-info` |
| `--help` |  | Show this message and exit. |  |

## `anystore io`

Generic i/o streaming wrapper which is wrapped around `fsspec`.

**Example**:

```bash
anystore io -i ./data.csv -o s3://my_bucket/data.csv
```

**Usage**:

```console
$ anystore io [OPTIONS]
```

**Options**:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `-i` | TEXT | Input uri | `-` |
| `-o` | TEXT | Output uri | `-` |
| `--help` |  | Show this message and exit. |  |

## `anystore csv2json`

Generic i/o wrapper for streaming input csv data to json objects.

**Example**:

```bash
cat data.csv | anystore csv2json
```

**Usage**:

```console
$ anystore csv2json [OPTIONS]
```

**Options**:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `-i` | TEXT | Input uri | `-` |
| `-o` | TEXT | Output uri | `-` |
| `--help` |  | Show this message and exit. |  |

## `anystore settings`

Show current runtime settings.

**Usage**:

```console
$ anystore settings [OPTIONS]
```

**Options**:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `--help` |  | Show this message and exit. |  |
