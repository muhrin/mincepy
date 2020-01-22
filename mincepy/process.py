import contextlib
import uuid

from . import depositors
from . import types

__all__ = ('Process',)


class Process(types.SavableComparable):
    TYPE_ID = uuid.UUID('bcf03171-a1f1-49c7-b890-b7f9d9f9e5a2')
    STACK = []

    @classmethod
    def current_process(cls):
        if not cls.STACK:
            return None
        return cls.STACK[-1]

    DEFINING_ATTRIBUTES = ('_name',)

    def __init__(self, name: str):
        super(Process, self).__init__()
        self._name = name

    def __eq__(self, other):
        if not isinstance(other, Process):
            return False

        return self.name == other.name

    @property
    def name(self) -> str:
        return self._name

    @contextlib.contextmanager
    def running(self):
        self.STACK.append(self)
        yield
        if self.STACK[-1] != self:
            raise RuntimeError("Someone has corrupted the process stack!\n"
                               "Expected to find '{}' on top but bound:{}".format(self, self.STACK))
        self.STACK.pop()

    def yield_hashables(self, hasher):
        yield from types.yield_hashable_attributes(self, self.DEFINING_ATTRIBUTES, hasher)

    def save_instance_state(self, depositor: depositors.Depositor):  # pylint: disable=unused-argument
        return {'name': self.name}

    def load_instance_state(self, saved_state, depositor: depositors.Depositor):  # pylint: disable=unused-argument
        Process.__init__(self, saved_state['name'])
