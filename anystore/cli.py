from typing import Annotated, Any, Optional

import typer
from rich import print
from rich.console import Console
from structlog import BoundLogger

from anystore import __version__
from anystore.io import (
    Writer,
    smart_open,
    smart_read,
    smart_stream_csv,
    smart_write,
)
from anystore.logging import configure_logging
from anystore.mirror import mirror
from anystore.settings import Settings
from anystore.store import get_store

settings = Settings()
cli = typer.Typer(no_args_is_help=True, pretty_exceptions_enable=settings.debug)
console = Console(stderr=True)

state = {"uri": settings.uri}


class ErrorHandler:
    def __init__(self, logger: BoundLogger | None = None) -> None:
        self.log = logger

    def __enter__(self) -> Any:
        pass

    def __exit__(self, e, msg, _):
        if isinstance(msg, BrokenPipeError):
            pass
        elif e is not None:
            if settings.debug:
                raise e
            if self.log is not None:
                self.log.error(f"{e.__name__}: {msg}")
            else:
                console.print(f"[red][bold]{e.__name__}[/bold]: {msg}[/red]")


@cli.callback(invoke_without_command=True)
def cli_store(
    version: Annotated[Optional[bool], typer.Option(..., help="Show version")] = False,
    store: Annotated[
        Optional[str], typer.Option(..., help="Store base uri")
    ] = settings.uri,
):
    if version:
        print(__version__)
        raise typer.Exit()
    if store:
        state["uri"] = store
    configure_logging()


@cli.command("get")
def cli_get(
    key: str,
    o: Annotated[str, typer.Option("-o")] = "-",
):
    """
    Get content of a `key` from a store
    """
    with ErrorHandler():
        S = get_store(uri=state["uri"], serialization_mode="raw")
        value = S.get(key)
        smart_write(o, value, mode="wb")


@cli.command("put")
def cli_put(
    key: str,
    value: Annotated[
        Optional[str],
        typer.Argument(..., help="Use this value instead of reading `-i` uri"),
    ] = None,
    i: Annotated[str, typer.Option("-i")] = "-",
):
    """
    Put content for a `key` to a store
    """
    with ErrorHandler():
        S = get_store(uri=state["uri"])
        value = value or smart_read(i)
        S.put(key, value)


@cli.command("keys")
def cli_keys(
    o: Annotated[str, typer.Option("-o", help="Output uri")] = "-",
    glob: Annotated[Optional[str], typer.Option(..., help="Key glob")] = None,
    prefix: Annotated[Optional[str], typer.Option(..., help="Key prefix")] = None,
    exclude_prefix: Annotated[
        Optional[str], typer.Option(..., help="Exclude key prefix")
    ] = None,
    info: Annotated[Optional[bool], typer.Option(..., help="Print metadata")] = False,
):
    """
    Iterate keys in given store
    """
    with ErrorHandler():
        S = get_store(uri=state["uri"])
        with smart_open(o, "wb") as out:
            for key in S.iterate_keys(
                prefix=prefix, exclude_prefix=exclude_prefix, glob=glob
            ):
                if info:
                    data = S.info(key)
                    line = (data.model_dump_json() + "\n").encode()
                else:
                    line = f"{key}\n".encode()
                out.write(line)


@cli.command("mirror")
def cli_mirror(
    i: Annotated[str, typer.Option("-i", help="Input store uri")],
    o: Annotated[str, typer.Option("-o", help="Output store uri")],
    glob: Annotated[Optional[str], typer.Option(..., help="Key glob")] = None,
    prefix: Annotated[Optional[str], typer.Option(..., help="Key prefix")] = None,
    exclude_prefix: Annotated[
        Optional[str], typer.Option(..., help="Exclude key prefix")
    ] = None,
    overwrite: Annotated[
        bool, typer.Option(..., help="Overwrite existing data")
    ] = False,
):
    """
    Mirror stores
    """
    with ErrorHandler():
        source = get_store(i)
        target = get_store(o)
        mirror(
            source=source,
            target=target,
            glob=glob,
            prefix=prefix,
            exclude_prefix=exclude_prefix,
            overwrite=overwrite,
        )


@cli.command("io")
def cli_io(
    i: Annotated[str, typer.Option("-i", help="Input uri")] = "-",
    o: Annotated[str, typer.Option("-o", help="Output uri")] = "-",
):
    """
    Generic i/o streaming wrapper which is wrapped around `fsspec`

    Example:
        anystore io -i ./data.csv -o s3://my_bucket/data.csv
    """
    with ErrorHandler():
        with smart_open(i) as reader:
            with smart_open(o, "wb") as writer:
                writer.write(reader.read())


@cli.command("csv2json")
def cli_csv2json(
    i: Annotated[str, typer.Option("-i", help="Input uri")] = "-",
    o: Annotated[str, typer.Option("-o", help="Output uri")] = "-",
):
    """
    Generic i/o wrapper for streaming input csv data to json objects

    Example:
        cat data.csv | anystore csv2json
    """
    with ErrorHandler():
        with Writer(o) as w:
            for row in smart_stream_csv(i):
                w.write(row)


@cli.command("settings")
def cli_settings():
    """Show current runtime settings"""
    console.print(settings)
