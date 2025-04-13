import collections.abc
import datetime
import numbers
from operator import itemgetter
import uuid

from typing_extensions import override

from . import helpers

__all__ = ("SimpleHelper", "BytesEquator")


class SimpleHelper(helpers.TypeHelper, obj_type=None, type_id=None):
    @override
    def yield_hashables(self, obj, hasher, /):
        yield from obj.yield_hashables(hasher)

    @override
    def eq(self, one, other, /) -> bool:  # pylint: disable=invalid-name
        return one == other

    @override
    def save_instance_state(self, obj, saver, /):
        return obj.save_instance_state(saver)

    @override
    def load_instance_state(self, obj, saved_state, loader, /):
        return obj.load(saved_state, loader)


class BytesEquator(SimpleHelper, obj_type=(bytes, bytearray), type_id=None):
    @override
    def yield_hashables(self, obj, hasher, /):
        yield obj


class StrEquator(SimpleHelper, obj_type=str, type_id=None):
    @override
    def yield_hashables(self, obj: str, hasher, /):
        yield obj.encode("utf-8")


class SequenceEquator(SimpleHelper, obj_type=collections.abc.Sequence, type_id=None):
    @override
    def yield_hashables(self, obj: collections.abc.Sequence, hasher, /):
        for entry in obj:
            yield from hasher.yield_hashables(entry)


class SetEquator(SimpleHelper, obj_type=collections.abc.Set, type_id=None):
    @override
    def yield_hashables(self, obj: collections.abc.Set, hasher, /):
        for entry in sorted(obj):
            yield from hasher.yield_hashables(entry)


class MappingEquator(SimpleHelper, obj_type=collections.abc.Mapping, type_id=None):
    @override
    def yield_hashables(self, obj: collections.abc.Mapping, hasher, /):
        def hashed_key_mapping(mapping):
            for key, value in mapping.items():
                yield tuple(hasher.yield_hashables(key)), value

        for key_hashables, value in sorted(hashed_key_mapping(obj), key=itemgetter(0)):
            # Yield all the key hashables
            yield from key_hashables
            # And now all the value hashables for that entry
            yield from hasher.yield_hashables(value)


class OrderedDictEquator(SimpleHelper, obj_type=collections.OrderedDict, type_id=None):
    @override
    def yield_hashables(self, obj: collections.OrderedDict, hasher, /):
        for key, val in sorted(obj, key=itemgetter(0)):
            yield from hasher.yield_hashables(key)
            yield from hasher.yield_hashables(val)


class RealEquator(SimpleHelper, obj_type=numbers.Real, type_id=None):
    @override
    def yield_hashables(self, obj, hasher, /):
        yield from hasher.yield_hashables(hasher.float_to_str(obj))


class ComplexEquator(SimpleHelper, obj_type=numbers.Complex, type_id=None):
    @override
    def yield_hashables(self, obj: numbers.Complex, hasher, /):
        yield from hasher.yield_hashables(obj.real)
        yield from hasher.yield_hashables(obj.imag)


class IntegerEquator(SimpleHelper, obj_type=numbers.Integral, type_id=None):
    @override
    def yield_hashables(self, obj: numbers.Integral, hasher, /):
        yield from hasher.yield_hashables(f"{obj}")


class BoolEquator(SimpleHelper, obj_type=bool, type_id=None):
    @override
    def yield_hashables(self, obj, hasher, /):
        yield b"\x01" if obj else b"\x00"


class NoneEquator(SimpleHelper, obj_type=type(None), type_id=None):
    @override
    def yield_hashables(self, obj, hasher, /):
        yield from hasher.yield_hashables("None")


class TupleEquator(SimpleHelper, obj_type=tuple, type_id=None):
    @override
    def yield_hashables(self, obj, hasher, /):
        yield from hasher.yield_hashables(obj)


class UuidEquator(SimpleHelper, obj_type=uuid.UUID, type_id=None):
    @override
    def yield_hashables(self, obj: uuid.UUID, hasher, /):
        yield obj.bytes


class DatetimeEquator(SimpleHelper, obj_type=datetime.datetime, type_id=None):
    @override
    def yield_hashables(self, obj: datetime.datetime, hasher, /):
        yield str(obj).encode("utf-8")
