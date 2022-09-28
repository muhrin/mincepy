# -*- coding: utf-8 -*-
import logging
import sys
import uuid

import click
import pytray.pretty
import tabulate

import mincepy.plugins

__all__ = ("mince",)

# pylint: disable=import-outside-toplevel


def set_print_logging(level=logging.WARNING):
    mince_logger = logging.getLogger("mincepy")
    mince_logger.setLevel(level)

    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(message)s")
    handler.setFormatter(formatter)
    mince_logger.addHandler(handler)


@click.group()
def mince():
    pass


@mince.command()
@click.argument("uri", type=str)
def gui(uri):
    """Start the mincepy gui (mincepy_gui package must be installed)"""
    try:
        import mincepy_gui
    except ImportError:
        click.echo(
            "mincepy-gui not found, please install (e.g. via pip install mincepy-gui)"
        )
        sys.exit(1)
    else:
        mincepy_gui.start(uri)


@mince.command()
def plugins():
    type_plugins = mincepy.plugins.get_types()
    click.echo("Types:")

    headers = "Type", "Class"
    plugin_info = []
    for plugin in type_plugins:
        if isinstance(plugin, mincepy.TypeHelper):
            row = "Type Helper", pytray.pretty.type_string(type(plugin))
        elif isinstance(plugin, type) and issubclass(plugin, mincepy.SavableObject):
            row = "Savable Object", pytray.pretty.type_string(plugin)
        else:
            row = "Unrecognised!", pytray.pretty.type_string(plugin)
        plugin_info.append(row)

    click.echo(tabulate.tabulate(plugin_info, headers))


@mince.command()
@click.option("--yes", is_flag=True, default=False, help="Yes to all prompts")
@click.argument("uri", type=str)
@click.pass_context
def migrate(ctx, yes, uri):
    try:
        hist = mincepy.create_historian(uri)
    except ValueError as exc:
        click.echo(exc)
        sys.exit(1)
    else:
        if isinstance(ctx.obj, dict) and "helpers" in ctx.obj:
            hist.register_types(ctx.obj["helpers"])

        click.echo("Looking for records to migrate...")
        records = tuple(hist.migrations.find_migratable_records())
        if not records:
            click.echo("No migrations necessary")
            return

        click.echo(f"Found {len(records)} records to migrate")
        if yes or click.confirm("Migrate all?"):
            set_print_logging(logging.INFO)
            hist.migrations.migrate_records(records)
        else:
            sys.exit(2)


@mince.command()
def tid():
    """Create a new type id"""
    click.echo(str(uuid.uuid4()))


@mince.command()
@click.argument("uri", type=str)
@click.option("--deleted/--no-deleted", default=True, help="Purge all deleted objects")
@click.option(
    "--unreferenced/--no-unreferenced",
    default=True,
    help="Purge all snapshots that are not referenced by live objects",
)
@click.option("--yes", "-y", is_flag=True, default=False, help="Yes to all prompts")
@click.option("-v", "--verbose", count=True)
def purge(uri, deleted, unreferenced, yes, verbose):
    """Purge the snapshots collection of any unused objects"""
    if verbose:
        set_print_logging(logging.INFO if verbose == 1 else logging.DEBUG)

    try:
        hist = mincepy.create_historian(uri)
    except ValueError as exc:
        click.echo(exc)
        sys.exit(1)
    else:
        click.echo("Searching records...")
        res = hist.purge(deleted=deleted, unreferenced=unreferenced, dry_run=True)
        click.echo(
            f"Found {len(res.deleted_purged)} deleted "
            f"and {len(res.unreferenced_purged)} unreferenced snapshot(s)"
        )

        to_delete = list(res.deleted_purged | res.unreferenced_purged)
        if to_delete and (yes or click.confirm("Do you want to delete them?")):
            click.echo("Deleting...", nl=False)
            hist.archive.bulk_write(list(map(mincepy.operations.Delete, to_delete)))
            click.echo("done")
