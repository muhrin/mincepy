import collections
import uuid

from . import depositor

__all__ = ('List', 'Str', 'Dict')


class List(collections.UserList):
    TYPE_ID = uuid.UUID('2b033f70-168f-4412-99ea-d1f131e3a25a')

    def save_instance_state(self, referencer: depositor.Referencer):
        return referencer.ref_many(self.data)

    def load_instance_state(self, state, lookup: depositor.Referencer):
        self.__init__(lookup.deref_many(state))


class Dict(collections.UserDict):
    TYPE_ID = uuid.UUID('a7584078-95b6-4e00-bb8a-b077852ca510')

    def save_instance_state(self, referencer: depositor.Referencer):
        return referencer.ref_many(self.data)

    def load_instance_state(self, state, lookup: depositor.Referencer):
        self.__init__(lookup.deref_many(state))


class Str(collections.UserString):
    TYPE_ID = uuid.UUID('350f3634-4a6f-4d35-b229-71238ce9727d')

    def save_instance_state(self, referencer: depositor.Referencer):
        return referencer.ref_many(self.data)

    def load_instance_state(self, state, lookup: depositor.Referencer):
        self.__init__(lookup.deref_many(state))
