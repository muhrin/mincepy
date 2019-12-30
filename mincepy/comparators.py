import numbers
import collections.abc
from operator import itemgetter
import typing

from .types import TypeEquator

__all__ = ('SimpleEquator', 'BytesEquator')


class SimpleEquator(TypeEquator):
    def eq(self, one, other) -> bool:
        return one == other


class BytesEquator(SimpleEquator):
    TYPE = collections.abc.ByteString

    def yield_hashables(self, value, hasher):
        yield value


class StrEquator(SimpleEquator):
    TYPE = str

    def yield_hashables(self, value: str, hasher):
        yield value.encode('utf-8')


class SequenceEquator(SimpleEquator):
    TYPE = collections.abc.Sequence

    def yield_hashables(self, value: collections.abc.Sequence, hasher):
        for entry in value:
            yield from hasher.yield_hashables(entry)


class SetEquator(SimpleEquator):
    TYPE = collections.abc.Set

    def yield_hashables(self, value: collections.abc.Set, hasher):
        for entry in sorted(value):
            yield from hasher.yield_hashables(entry)


class MappingEquator(SimpleEquator):
    TYPE = collections.abc.Mapping

    def yield_hashables(self, value, hasher):
        def hashed_key_mapping(mapping):
            for key, value in mapping.items():
                yield tuple(hasher.yield_hashables(key)), value

        for key_hashables, value in sorted(hashed_key_mapping(value), key=itemgetter(0)):
            # Yield all the key hashables
            yield from key_hashables
            # And now all the value hashables for that entry
            yield from hasher.yield_hashables(value)


class OrderedDictEquator(SimpleEquator):
    TYPE = collections.OrderedDict

    def yield_hashables(self, value, hasher):
        for key, val in sorted(value, key=itemgetter(0)):
            yield from hasher.yield_hashables(key)
            yield from hasher.yield_hashables(val)


class RealEquator(SimpleEquator):
    TYPE = numbers.Real

    def yield_hashables(self, value, hasher):
        yield from hasher.yield_hashables(hasher.float_to_str(value))


class ComplexEquator(SimpleEquator):
    TYPE = numbers.Complex

    def yield_hashables(self, value: numbers.Complex, hasher):
        yield hasher.yield_hashables(value.real)
        yield hasher.yield_hashables(value.imag)


class IntegerEquator(SimpleEquator):
    TYPE = numbers.Integral

    def yield_hashables(self, value: numbers.Integral, hasher):
        yield from hasher.yield_hashables(u'{}'.format(value))


class BoolEquator(SimpleEquator):
    TYPE = bool

    def yield_hashables(self, value, hasher):
        yield b'\x01' if value else b'\x00'


class NoneEquator(SimpleEquator):
    TYPE = type(None)

    def yield_hashables(self, value, hasher):
        yield from hasher.yield_hashables('None')
