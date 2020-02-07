import contextlib
import uuid

import mincepy.builtins

__all__ = ('Process',)


class Process(mincepy.builtins.Archivable):
    TYPE_ID = uuid.UUID('bcf03171-a1f1-49c7-b890-b7f9d9f9e5a2')
    STACK = []
    ATTRS = ('_name', '_running')

    @classmethod
    def current_process(cls):
        if not cls.STACK:
            return None
        return cls.STACK[-1]

    def __init__(self, name: str):
        super(Process, self).__init__()
        self._name = name
        self._running = False

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
        self._running = True
        try:
            yield
        finally:
            if self.STACK[-1] != self:
                raise RuntimeError("Someone has corrupted the process stack!\n"
                                   "Expected to find '{}' on top but found:{}".format(self, self.STACK))
            self._running = False
            self.STACK.pop()
