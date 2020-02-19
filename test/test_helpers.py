import uuid

import mincepy


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
