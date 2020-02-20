from concurrent.futures import Future

from PySide2.QtCore import Qt

import mincepy

__all__ = 'default_executor', 'default_create_historian', 'AppCommon'


def default_executor(func, msg=None, blocking=False):
    future = Future()
    try:
        future.set_result(func())
    except Exception as exc:
        future.set_exception(exc)
    return future


def default_create_historian(uri) -> mincepy.Historian:
    historian = mincepy.create_historian(uri)
    mincepy.set_historian(historian)
    return historian


class AppCommon:

    def __init__(self,
                 default_connect_uri='mongodb://localhost/test',
                 create_historian_callback=default_create_historian,
                 executor=default_executor):
        self.default_connect_uri = default_connect_uri
        self.create_historian_callback = create_historian_callback
        self.executor = executor
        self.type_viewers = []

    def call_viewers(self, obj):
        for viewer in self.type_viewers:
            if viewer(obj):
                return


DataRole = Qt.UserRole  # Role to get the actual data associated with an index
