# -*- coding: utf-8 -*-
from argparse import Namespace
import pathlib
import uuid

from . import helpers

__all__ = 'PathHelper', 'TupleHelper', 'NamespaceHelper'


class PathHelper(helpers.BaseHelper):
    TYPE = pathlib.Path
    TYPE_ID = uuid.UUID('78e5c6b8-f194-41ae-aead-b231953318e1')
    IMMUTABLE = True

    def yield_hashables(self, obj: pathlib.Path, hasher):  # pylint: disable=no-self-use
        yield from hasher.yield_hashables(str(obj))

    def save_instance_state(self, obj: pathlib.Path, _saver):  # pylint: disable=no-self-use
        return str(obj)

    def new(self, encoded_saved_state):  # pylint: disable=no-self-use
        return pathlib.Path(encoded_saved_state)

    def load_instance_state(self, obj: pathlib.Path, saved_state, _loader):  # pylint: disable=no-self-use
        pass  # Done it all in new


class TupleHelper(helpers.BaseHelper):
    TYPE = tuple
    TYPE_ID = uuid.UUID('fd9d2f50-71d6-4e70-90b7-117f23d9cbaf')
    IMMUTABLE = True

    def save_instance_state(self, obj: tuple, _saver):  # pylint: disable=no-self-use
        return list(obj)

    def new(self, encoded_saved_state):  # pylint: disable=no-self-use
        return tuple(encoded_saved_state)

    def load_instance_state(self, obj: pathlib.Path, saved_state, _loader):  # pylint: disable=no-self-use
        pass  # Done it all in new

    def yield_hashables(self, obj, hasher):  # pylint: disable=no-self-use
        for entry in obj:
            yield from hasher.yield_hashables(entry)


class NamespaceHelper(helpers.BaseHelper):
    TYPE = Namespace
    TYPE_ID = uuid.UUID('c43f8329-0d68-4d12-9a35-af8f5ecc4f90')

    def yield_hashables(self, obj, hasher):  # pylint: disable=no-self-use
        yield from hasher.yield_hashables(vars(obj))

    def save_instance_state(self, obj, _saver):  # pylint: disable=no-self-use
        return vars(obj)

    def load_instance_state(self, obj, saved_state, _loader):  # pylint: disable=no-self-use
        obj.__init__(**saved_state)


HISTORIAN_TYPES = NamespaceHelper(), TupleHelper(), PathHelper()
