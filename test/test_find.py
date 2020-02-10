from mincepy.testing import *


def test_find_state(historian: mincepy.Historian):
    honda = Car('honda', 'green')
    porsche = Car('porsche', 'black')
    red_honda = Car('honda', 'red')
    fiat = Car('fiat', 'green')

    historian.save(honda, porsche, red_honda, fiat)
    hondas = list(historian.find(Car, criteria={'make': 'honda'}))
    assert len(hondas) == 2
    assert honda in hondas
    assert red_honda in hondas

    # Try without type
    greens = list(historian.find(criteria={'colour': 'green'}))
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
