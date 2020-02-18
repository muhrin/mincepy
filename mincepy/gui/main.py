import sys

from PySide2 import QtWidgets

from . import common
from . import views

__all__ = ('run_application',)


def run_application(app_common: common.AppCommon = None):
    app = QtWidgets.QApplication([])

    if app_common is None:
        app_common = common.AppCommon()

    widget = views.MainWindow(app_common)
    widget.resize(800, 600)
    widget.show()

    sys.exit(app.exec_())


if __name__ == "__main__":
    run_application()
