import logging
import sys

import click

from . import main


@main.mince.group()
def dev():
    """Commands to help with development and testing"""


@dev.command()
@click.argument('uri', type=str)
def populate(uri):
    """Populate the database with testing data"""
    import mincepy.testing  # pylint: disable=import-outside-toplevel
    main.set_print_logging(logging.INFO)
    try:
        hist = mincepy.create_historian(uri)
    except ValueError as exc:
        click.echo(exc)
        sys.exit(1)
    else:
        mincepy.testing.populate(hist)
        click.echo("Successfully populated database at '{}'".format(uri))
