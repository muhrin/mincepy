import uuid

import mincepy


class CarV1(mincepy.SimpleSavable):
    TYPE_ID = uuid.UUID('297808e4-9bc7-4f0a-9f8d-850a5f558663')
    ATTRS = ('colour', 'make')

    def __init__(self, colour: str, make: str):
        super(CarV1, self).__init__()
        self.colour = colour
        self.make = make

    def save_instance_state(self, saver: mincepy.Saver):
        super(CarV1, self).save_instance_state(saver)
        # Here, I decide to store as an array
        return [self.colour, self.make]

    def load_instance_state(self, saved_state, loader: mincepy.Loader):
        self.colour = saved_state[0]
        self.make = saved_state[1]


class CarV2(CarV1):

    class V1toV2(mincepy.ObjectMigration):
        VERSION = 1

        @classmethod
        def upgrade(cls, saved_state, migrator: 'mincepy.Migrator') -> dict:
            return dict(colour=saved_state[0], make=saved_state[1])

    # Set the migration
    LATEST_MIGRATION = V1toV2

    def save_instance_state(self, saver: mincepy.Saver):
        # I've changed my mind, I'd like to store it as a dict
        return dict(colour=self.colour, make=self.make)

    def load_instance_state(self, saved_state, loader: mincepy.Loader):
        self.colour = saved_state['colour']
        self.make = saved_state['make']


class CarV3(CarV2):

    class V2toV3(mincepy.ObjectMigration):
        VERSION = 2
        PREVIOUS = CarV2.V1toV2

        @classmethod
        def upgrade(cls, saved_state, migrator: 'mincepy.Migrator') -> dict:
            # Augment the saved state
            saved_state['reg'] = 'unknown'
            return saved_state

    # Set the migration
    LATEST_MIGRATION = V2toV3

    def __init__(self, colour: str, make: str, reg=None):
        super().__init__(colour, make)
        self.reg = reg

    def save_instance_state(self, saver: mincepy.Saver):
        # We now add a reg field
        return dict(colour=self.colour, make=self.make, reg=self.reg)

    def load_instance_state(self, saved_state, loader: mincepy.Loader):
        self.colour = saved_state['colour']
        self.make = saved_state['make']
        self.reg = saved_state['reg']


class StoreByValue(mincepy.SimpleSavable):
    ATTRS = ('ref',)
    TYPE_ID = uuid.UUID('40377bfc-901c-48bb-a85c-1dd692cddcae')

    def __init__(self, ref):
        super().__init__()
        self.ref = ref


class StoreByRef(StoreByValue):

    class ToRefMigration(mincepy.ObjectMigration):
        VERSION = 1

        @classmethod
        def upgrade(cls, saved_state, migrator: 'mincepy.Migrator') -> dict:
            # Replace the value stored version with a reference
            saved_state['ref'] = mincepy.ObjRef(saved_state['ref'])
            return saved_state

    # Changed my mind, want to store by value now
    ATTRS = (mincepy.AsRef('ref'),)
    LATEST_MIGRATION = ToRefMigration
