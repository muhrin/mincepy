import collections
import uuid

from . import depositors
from . import types

__all__ = ('List', 'Str', 'Dict')


class UserType(types.SavableComparable):
    """Mixin for helping user types to be compatible with the historian.
    These typically have a .data member that stores the actual data (list, dict, str, etc)"""
    data = None  # placeholder

    def yield_hashables(self, hasher):
        yield from hasher.yield_hashables(self.data)

    def eq(self, other) -> bool:
        if type(self) != type(other):
            return False

        return self.data == other.data


class List(collections.UserList, UserType):
    TYPE_ID = uuid.UUID('2b033f70-168f-4412-99ea-d1f131e3a25a')

    def save_instance_state(self, _depositor: depositors.Depositor):
        return self.data

    def load_instance_state(self, state, _depositor: depositors.Depositor):
        self.__init__(state)


class Dict(collections.UserDict, UserType):
    TYPE_ID = uuid.UUID('a7584078-95b6-4e00-bb8a-b077852ca510')

    def save_instance_state(self, _depositor: depositors.Depositor):
        return self.data

    def load_instance_state(self, state, _depositor: depositors.Depositor):
        self.__init__(state)


class Str(collections.UserString, UserType):
    TYPE_ID = uuid.UUID('350f3634-4a6f-4d35-b229-71238ce9727d')

    def save_instance_state(self, _depositor: depositors.Depositor):
        return self.data

    def load_instance_state(self, state, _depositor: depositors.Depositor):
        self.data = state
