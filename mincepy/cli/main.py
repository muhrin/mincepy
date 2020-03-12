import typing

import click
import tabulate

import mincepy.plugins

__all__ = ('mince',)

# pylint: disable=import-outside-toplevel


@click.group()
def mince():
    pass


@mince.command()
def gui():
    import mincepy_gui
    mincepy_gui.start()


@mince.command()
def plugins():
    type_plugins = mincepy.plugins.get_types()
    click.echo("Types:")

    headers = 'Type', 'Class'
    plugin_info = []
    for plugin in type_plugins:
        if isinstance(plugin, mincepy.TypeHelper):
            row = 'Type Helper', pretty_type_string(type(plugin))
        elif isinstance(plugin, type) and issubclass(plugin, mincepy.SavableObject):
            row = 'Savable Object', pretty_type_string(plugin)
        else:
            row = 'Unrecognised!', pretty_type_string(plugin)
        plugin_info.append(row)

    click.echo(tabulate.tabulate(plugin_info, headers))


def pretty_type_string(obj_type: typing.Type) -> str:
    """Given an type will return a simple type string"""
    type_str = str(obj_type)
    if type_str.startswith('<class '):
        return type_str[8:-2]
    return type_str
