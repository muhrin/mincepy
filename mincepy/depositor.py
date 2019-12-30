from abc import ABCMeta, abstractmethod

__all__ = ('Referencer',)


class Referencer(metaclass=ABCMeta):
    @abstractmethod
    def ref(self, obj):
        """Get a persistent reference for the given object"""

    @abstractmethod
    def deref(self, persistent_id):
        """Retrieve an object given a persistent reference"""
