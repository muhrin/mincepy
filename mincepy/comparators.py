import numbers
import collections.abc
from operator import itemgetter
import uuid

from . import helpers

__all__ = ('SimpleHelper', 'BytesEquator')


class SimpleHelper(helpers.TypeHelper):

    def yield_hashables(self, obj, hasher):
        yield from obj.yield_hashables(hasher)

    def eq(self, one, other) -> bool:
        return one == other

    def save_instance_state(self, obj, saver):
        return obj.save_instance_state(saver)

    def load_instance_state(self, obj, saved_state, loader):
        return obj.load(saved_state, loader)


class BytesEquator(SimpleHelper):
    TYPE = collections.abc.ByteString

    def yield_hashables(self, obj, hasher):
        yield obj


class StrEquator(SimpleHelper):
    TYPE = str

    def yield_hashables(self, obj: str, hasher):
        yield obj.encode('utf-8')


class SequenceEquator(SimpleHelper):
    TYPE = collections.abc.Sequence

    def yield_hashables(self, obj: collections.abc.Sequence, hasher):
        for entry in obj:
            yield from hasher.yield_hashables(entry)


class SetEquator(SimpleHelper):
    TYPE = collections.abc.Set

    def yield_hashables(self, obj: collections.abc.Set, hasher):
        for entry in sorted(obj):
            yield from hasher.yield_hashables(entry)


class MappingEquator(SimpleHelper):
    TYPE = collections.abc.Mapping

    def yield_hashables(self, obj, hasher):

        def hashed_key_mapping(mapping):
            for key, value in mapping.items():
                yield tuple(hasher.yield_hashables(key)), value

        for key_hashables, value in sorted(hashed_key_mapping(obj), key=itemgetter(0)):
            # Yield all the key hashables
            yield from key_hashables
            # And now all the value hashables for that entry
            yield from hasher.yield_hashables(value)


class OrderedDictEquator(SimpleHelper):
    TYPE = collections.OrderedDict

    def yield_hashables(self, obj, hasher):
        for key, val in sorted(obj, key=itemgetter(0)):
            yield from hasher.yield_hashables(key)
            yield from hasher.yield_hashables(val)


class RealEquator(SimpleHelper):
    TYPE = numbers.Real

    def yield_hashables(self, obj, hasher):
        yield from hasher.yield_hashables(hasher.float_to_str(obj))


class ComplexEquator(SimpleHelper):
    TYPE = numbers.Complex

    def yield_hashables(self, obj: numbers.Complex, hasher):
        yield from hasher.yield_hashables(obj.real)
        yield from hasher.yield_hashables(obj.imag)


class IntegerEquator(SimpleHelper):
    TYPE = numbers.Integral

    def yield_hashables(self, obj: numbers.Integral, hasher):
        yield from hasher.yield_hashables(u'{}'.format(obj))


class BoolEquator(SimpleHelper):
    TYPE = bool

    def yield_hashables(self, obj, hasher):
        yield b'\x01' if obj else b'\x00'


class NoneEquator(SimpleHelper):
    TYPE = type(None)

    def yield_hashables(self, obj, hasher):
        yield from hasher.yield_hashables('None')


class TupleEquator(SimpleHelper):
    TYPE = tuple

    def yield_hashables(self, obj, hasher):
        yield from hasher.yield_hashables(obj)


class UuidEquator(SimpleHelper):
    TYPE = uuid.UUID

    def yield_hashables(self, obj: uuid.UUID, hasher):
        yield obj.bytes
