import functools

from .historian import get_historian
from . import types

__all__ = ('track', 'FunctionCall')


class InvalidStateError(Exception):
    pass


class FunctionCall:
    DEFINING_ATTRIBUTES = ('_function', '_args', '_kwargs', '_result', '_exception', '_done')

    def __init__(self, func, *args, **kwargs):
        self._function = func.__name__
        self._args = args
        self._kwargs = kwargs
        self._result = None
        self._exception = None
        self._done = False

    def __eq__(self, other):
        if not isinstance(other, FunctionCall):
            return False

        return types.eq_attributes(self, other, self.DEFINING_ATTRIBUTES)

    @property
    def function(self):
        return self._function

    @property
    def args(self):
        return self._args

    @property
    def kwargs(self):
        return self._kwargs

    def result(self):
        if not self.done():
            raise InvalidStateError("Not done yet")

        return self._result

    def set_result(self, result):
        assert not self._done
        self._result = result
        self._done = True

    def set_exception(self, exc):
        assert not self._done
        self._exception = str(exc)
        self._done = True

    def done(self):
        return self._done

    def yield_hashables(self, hasher):
        yield from types.yield_hashable_attributes(self, self.DEFINING_ATTRIBUTES, hasher)


def track(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        historian = get_historian()
        call = FunctionCall(func, *args, **kwargs)
        historian.save(call)
        try:
            result = func(*args, **kwargs)
            call.set_result(result)
        except Exception as exc:
            call.set_exception(exc)
        else:
            return result
        finally:
            historian.save(call)

    return wrapper
