import mincepy.gui
from . import main


@main.mince.command()
def gui():
    mincepy.gui.run_application()
