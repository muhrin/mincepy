from concurrent.futures import Future

import mincepy


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
