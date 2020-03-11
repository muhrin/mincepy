import click

__all__ = ('mince',)

# pylint: disable=import-outside-toplevel


@click.group()
def mince():
    pass


@mince.command()
def gui():
    import mincepy_gui
    mincepy_gui.start()
