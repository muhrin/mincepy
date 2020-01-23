import pathlib
import uuid

from . import helpers

__all__ = ('PathHelper',)


class PathHelper(helpers.TypeHelper):
    TYPE = pathlib.PosixPath
    TYPE_ID = uuid.UUID('78e5c6b8-f194-41ae-aead-b231953318e1')
    IMMUTABLE = True

    def yield_hashables(self, obj: pathlib.Path, hasher):
        yield from hasher.yield_hashables(str(obj))

    def eq(self, one, other) -> bool:
        return one == other

    def save_instance_state(self, obj: pathlib.Path, depositor):
        return str(obj)

    def new(self, encoded_saved_state):
        return pathlib.Path(encoded_saved_state)

    def load_instance_state(self, obj: pathlib.Path, saved_state, depositor):
        pass  # Done it all in new


class TupleHelper(helpers.TypeHelper):
    TYPE = tuple
    TYPE_ID = uuid.UUID('fd9d2f50-71d6-4e70-90b7-117f23d9cbaf')
    IMMUTABLE = True

    def yield_hashables(self, obj: tuple, hasher):
        yield from hasher.yield_hashables(obj)

    def eq(self, one, other) -> bool:
        return one == other

    def save_instance_state(self, obj: tuple, depositor):
        return list(obj)

    def new(self, encoded_saved_state):
        return tuple(encoded_saved_state)

    def load_instance_state(self, obj: pathlib.Path, saved_state, depositor):
        pass  # Done it all in new
