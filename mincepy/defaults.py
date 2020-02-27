from . import comparators


def get_default_equators():
    return (
        comparators.SequenceEquator(),
        comparators.BytesEquator(),
        comparators.StrEquator(),
        comparators.SetEquator(),
        comparators.OrderedDictEquator(),
        comparators.MappingEquator(),
        comparators.ComplexEquator(),
        comparators.RealEquator(),
        comparators.IntegerEquator(),
        comparators.BoolEquator(),
        comparators.NoneEquator(),
        comparators.TupleEquator(),
    )
