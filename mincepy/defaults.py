from . import comparators
from . import types


def get_default_equators():
    return (
        comparators.BytesEquator(),
        comparators.StrEquator(),
        comparators.SequenceEquator(),
        comparators.SetEquator(),
        comparators.OrderedDictEquator(),
        comparators.MappingEquator(),
        comparators.RealEquator(),
        comparators.ComplexEquator(),
        comparators.IntegerEquator(),
        comparators.BoolEquator(),
        comparators.NoneEquator(),
        comparators.TupleEquator(),
    )
