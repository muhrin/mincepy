import mincepy.records
from mincepy.testing import *


def test_find_state(historian: mincepy.Historian):
    honda = Car('honda', 'green')
    porsche = Car('porsche', 'black')
    red_honda = Car('honda', 'red')
    fiat = Car('fiat', 'green')

    historian.save(honda, porsche, red_honda, fiat)
    hondas = list(historian.find(Car, state={'make': 'honda'}))
    assert len(hondas) == 2
    assert honda in hondas
    assert red_honda in hondas

    # Try without type
    greens = list(historian.find(state={'colour': 'green'}))
    assert len(greens) == 2
    assert honda in greens
    assert fiat in greens


def test_find_skip(historian: mincepy.Historian):
    cars = []
    for idx in range(10):
        cars.append(Car(idx))

    historian.save(*cars)
    # Try live
    makes = set(range(10))
    # Skip two at a time limiting to two at a time, effectively paginating
    for skip in range(5):
        results = list(historian.find(obj_type=Car, limit=2, skip=skip * 2))
        assert len(results) == 2, "Got no results on page {}".format(skip)
        makes.remove(results[0].make)
        makes.remove(results[1].make)

    assert not makes


def test_find_by_type_id(historian: mincepy.Historian):
    """Test that searching by the type id works too"""
    cars = []
    for idx in range(10):
        cars.append(Car(idx))

    historian.save(*cars)
    result = tuple(historian.find(obj_type=Car.TYPE_ID))
    assert len(result) == 10


def test_simple_sort(historian: mincepy.Historian):
    cars = mincepy.builtins.RefList()
    for idx in range(10):
        cars.append(Car(idx))

    historian.save(cars)
    results = list(historian.find_records(Car, sort=mincepy.records.CREATION_TIME))
    for idx, result in enumerate(results[1:]):
        # No need to subtract 1 from idx as we're already one behind because of the slicing
        assert result.creation_time >= results[idx].creation_time


def test_find_latest(historian: mincepy.Historian):
    """Check that search criteria is only applied to the latest version if -1 is used"""
    # Version 0
    car = Car('fiat', 'yellow')
    car.save()
    # Version 1
    car.colour = 'white'
    car.save()
    # Version 2
    car.colour = 'red'
    car.save()

    car2 = Car('ford', 'blue')
    car2.save()
    car2.colour = 'green'
    car2.save()

    # Find the original version
    results = tuple(historian.archive.find(version=0, state=dict(colour='yellow')))
    assert len(results) == 1

    # Now search for a white fiat as the latest version
    results = tuple(historian.archive.find(version=-1, state=dict(colour='white')))
    assert len(results) == 0

    # Now do a version-unrestricted search
    results = tuple(historian.archive.find(version=None, state=dict(colour='yellow')))
    assert len(results) == 1
    assert results[0].version == 0

    # Now do a version-unrestricted search
    results = tuple(historian.archive.find(version=None, state=dict(make='fiat')))
    assert len(results) == 3


def test_find_predicates(historian: mincepy.Historian):
    """Test using the query predicates"""

    skoda = Car('skoda', 'green')
    ferrari = Car('ferrari', 'yellow')
    bugatti = Car('bugatti', 'green')
    historian.save(skoda, ferrari, bugatti)

    # OR
    results = tuple(
        historian.find_records(state=mincepy.q.or_({'make': 'skoda'}, {'make': 'ferrari'})))
    assert len(results) == 2
    makes = [record.state['make'] for record in results]
    assert 'skoda' in makes
    assert 'ferrari' in makes

    # AND
    results = tuple(
        historian.find_records(state=mincepy.q.and_({'make': 'skoda'}, {'colour': 'green'})))
    assert len(results) == 1
    makes = [record.state['make'] for record in results]
    assert 'skoda' in makes


@pytest.mark.skip(
    reason="This test takes a long time and consumes a lot of memory but if a MongoDB issue occurs "
    "that involves running out of memory it may be useful again")
def test_sort_with_many_entries(historian: mincepy.Historian):
    """Test for mincepy_gui issue #11 which was caused by a query with a large number of objects
    that was sorted.  This exceeded MongoDBs memory limits.  The fix is to simply allow disk use."""
    cars = []
    for i in range(1000000):
        cars.append(Car())

    historian.save(*cars)

    next(historian.find_records(Car, sort='state.make'))
