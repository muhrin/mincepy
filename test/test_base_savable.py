import uuid

import mincepy
import mincepy.builtins
from mincepy.testing import Car


def test_save_as_ref(historian: mincepy.Historian):
    """Test the 'AsRef' functionality of BaseSavableObject"""

    class Person(mincepy.SimpleSavable):
        TYPE_ID = uuid.UUID('692429b6-a08b-489a-aa09-6eb3174b6405')
        ATTRS = (mincepy.AsRef('car'), 'name')  # Save the car by reference

        def __init__(self, name: str, car):
            super().__init__()
            self.name = name
            self.car = car

    car = Car()
    # Both martin and sonia have the same car
    martin = Person('martin', car)
    sonia = Person('sonia', car)
    martin_id, sonia_id = historian.save(martin, sonia)
    del martin, sonia, car

    # No reload and check they still have the same car
    martin, sonia = historian.load(martin_id, sonia_id)
    assert martin.car is not None
    assert martin.name == 'martin'
    assert martin.car is sonia.car
    assert sonia.name == 'sonia'
