import logging
import sys
import typing

import click
import pytray.pretty
import tabulate

import mincepy.plugins

__all__ = ('mince',)

# pylint: disable=import-outside-toplevel


def set_print_logging(level=logging.WARNING):
    mince_logger = logging.getLogger('mincepy')
    mince_logger.setLevel(level)

    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(message)s')
    handler.setFormatter(formatter)
    mince_logger.addHandler(handler)


@click.group()
def mince():
    pass


@mince.command()
@click.argument('uri', type=str)
def gui(uri):
    """Start the mincepy gui (mincepy_gui package must be installed)"""
    try:
        import mincepy_gui
    except ImportError:
        click.echo("mincepy-gui not found, please install (e.g. via pip install mincepy-gui)")
        sys.exit(1)
    else:
        mincepy_gui.start(uri)


@mince.command()
def plugins():
    type_plugins = mincepy.plugins.get_types()
    click.echo("Types:")

    headers = 'Type', 'Class'
    plugin_info = []
    for plugin in type_plugins:
        if isinstance(plugin, mincepy.TypeHelper):
            row = 'Type Helper', pytray.pretty.type_string(type(plugin))
        elif isinstance(plugin, type) and issubclass(plugin, mincepy.SavableObject):
            row = 'Savable Object', pytray.pretty.type_string(plugin)
        else:
            row = 'Unrecognised!', pytray.pretty.type_string(plugin)
        plugin_info.append(row)

    click.echo(tabulate.tabulate(plugin_info, headers))
