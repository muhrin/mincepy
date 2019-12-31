import uuid

from . import depositor

__all__ = ('List', 'Tuple', 'Dict')


class List(list):
    TYPE_ID = uuid.UUID('2b033f70-168f-4412-99ea-d1f131e3a25a')

    def save_instance_state(self, referencer: depositor.Referencer):
        return referencer.ref_many(self)

    def load_instance_state(self, encoded_value, lookup: depositor.Referencer):
        self.__init__(lookup.deref_many(encoded_value))


class Tuple(tuple):
    pass


class Dict(dict):
    pass
