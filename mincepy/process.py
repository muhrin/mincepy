import contextlib
import uuid

from . import depositor


class ProcessRecord:
    TYPE_ID = uuid.UUID('bcf03171-a1f1-49c7-b890-b7f9d9f9e5a2')
    STACK = []

    DEFINING_ATTRIBUTES = ('_name',)

    def __init__(self, name: str):
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    @contextlib.contextmanager
    def running(self):
        self.STACK.append(self)
        yield
        if self.STACK[-1] != self:
            raise RuntimeError(
                "Someone has corrupted the process stack!\n"
                "Expected to find '{}' on top but bound:{}".format(self, self.STACK))
        self.STACK.pop()

    def yield_hashables(self, hasher):
        yield from types.yield_hashable_attributes(self, self.DEFINING_ATTRIBUTES, hasher)

    def save_instance_state(self, _: depositor.Referencer):
        return {'name': self.name}

    def load_instance_state(self, state, _: depositor.Referencer):
        self.__init__(state['name'])
