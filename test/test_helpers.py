import uuid

import pytest

import mincepy
from mincepy import testing

# pylint: disable=too-few-public-methods


def test_type_helper(historian: mincepy.Historian):
    """Check that a type helper can be used to make a non-historian compatible type compatible"""

    class Bird:

        def __init__(self, specie='hoopoe'):
            self.specie = specie

    class BirdHelper(mincepy.TypeHelper):
        TYPE = Bird
        TYPE_ID = uuid.UUID('5cc59e03-ea5d-43ff-8814-3b6f2e22cd76')

        specie = mincepy.field()

    bird = Bird()
    with pytest.raises(TypeError):
        historian.save(bird)

    # Now register the helper...
    historian.register_type(BirdHelper())
    # ...and we should be able to save
    assert historian.save(bird) is not None


def test_transaction_snapshots(historian: mincepy.Historian):

    class ThirdPartyPerson:
        """A class from a third party library"""

        def __init__(self, name):
            self.name = name

    class PersonHelper(mincepy.TypeHelper):
        TYPE_ID = uuid.UUID('62d8c767-14bc-4437-a9a3-ca5d0ce65d9b')
        TYPE = ThirdPartyPerson
        INJECT_CREATION_TRACKING = True

        def yield_hashables(self, obj, hasher):
            yield from hasher.yield_hashables(obj.name)

        def eq(self, one, other) -> bool:
            return one.name == other.name

        def save_instance_state(self, obj, saver):
            return obj.name

        def load_instance_state(self, obj, saved_state, loader):
            obj.name = saved_state.name

    person_helper = PersonHelper()
    historian.register_type(person_helper)

    person_maker = mincepy.Process('person maker')

    with person_maker.running():
        martin = ThirdPartyPerson('Martin')

    historian.save(martin)
    assert historian.created_by(martin) == historian.get_obj_id(person_maker)


class Boat:

    def __init__(self, make: str, length: float, owner: testing.Person = None):
        self.make = make
        self.length = length
        self.owner = owner


class BoatHelper(mincepy.TypeHelper):
    TYPE_ID = uuid.UUID('4d82b67a-dbcb-4388-b20e-8542c70491d1')
    TYPE = Boat

    # Describe how to store the properties
    make = mincepy.field()
    length = mincepy.field()
    owner = mincepy.field(ref=True)


def test_simple_helper(historian: mincepy.Historian):
    historian.register_type(BoatHelper())

    jenneau = Boat('jenneau', 38.9)
    jenneau_id = historian.save(jenneau)
    del jenneau

    jenneau = historian.load(jenneau_id)
    assert jenneau.make == 'jenneau'
    assert jenneau.length == 38.9

    # Now check that references work
    martin = testing.Person('martin', 35)
    jenneau.owner = martin
    historian.save(jenneau)
    del jenneau

    jenneau = historian.load(jenneau_id)
    assert jenneau.owner is martin


class Powerboat(Boat):
    TYPE_ID = uuid.UUID('924ef5b2-ce20-40b0-8c98-4da470f6c2c3')
    horsepower = mincepy.field()

    def __init__(self, make: str, length: float, horsepower: float, owner: testing.Person = None):
        super().__init__(make, length, owner)
        self.horsepower = horsepower


class PowerboatHelper(BoatHelper):
    TYPE_ID = uuid.UUID('924ef5b2-ce20-40b0-8c98-4da470f6c2c3')
    TYPE = Powerboat

    horsepower = mincepy.field()


def test_subclass_helper(historian: mincepy.Historian):
    historian.register_type(PowerboatHelper())

    quicksilver = Powerboat('quicksilver', length=7.0, horsepower=115)
    quicksilver_id = historian.save(quicksilver)
    del quicksilver

    quicksilver = historian.load(quicksilver_id)
    assert quicksilver.make == 'quicksilver'
    assert quicksilver.length == 7.
    assert quicksilver.horsepower == 115

    martin = testing.Person('martin', 35)
    quicksilver.owner = martin
    historian.save(quicksilver)
    del quicksilver

    quicksilver = historian.load(quicksilver_id)
    assert quicksilver.owner is martin
