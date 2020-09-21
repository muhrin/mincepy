import uuid

import mincepy

# pylint: disable=invalid-name


class CarV0(mincepy.ConvenientSavable):
    TYPE_ID = uuid.UUID('297808e4-9bc7-4f0a-9f8d-850a5f558663')
    colour = mincepy.field()
    make = mincepy.field()

    def __init__(self, colour: str, make: str):
        super().__init__()
        self.colour = colour
        self.make = make

    def save_instance_state(self, saver: mincepy.Saver):
        super().save_instance_state(saver)
        # Here, I decide to store as an array
        return [self.colour, self.make]

    def load_instance_state(self, saved_state, loader: mincepy.Loader):
        self.colour = saved_state[0]
        self.make = saved_state[1]


class CarV1(mincepy.ConvenientSavable):
    TYPE_ID = uuid.UUID('297808e4-9bc7-4f0a-9f8d-850a5f558663')
    colour = mincepy.field()
    make = mincepy.field()

    class V0toV1(mincepy.ObjectMigration):
        VERSION = 1

        @classmethod
        def upgrade(cls, saved_state, loader: 'mincepy.Loader') -> dict:
            return dict(colour=saved_state[0], make=saved_state[1])

    # Set the migration
    LATEST_MIGRATION = V0toV1

    def __init__(self, colour: str, make: str):
        super().__init__()
        self.colour = colour
        self.make = make

    # This time we don't overwrite save/load instance state and let the SavableObject save the
    # attributes as a dictionary


class CarV2(mincepy.ConvenientSavable):
    TYPE_ID = uuid.UUID('297808e4-9bc7-4f0a-9f8d-850a5f558663')
    colour = mincepy.field()
    make = mincepy.field()
    reg = mincepy.field()  # New attribute

    class V1toV2(mincepy.ObjectMigration):
        VERSION = 2
        PREVIOUS = CarV1.V0toV1

        @classmethod
        def upgrade(cls, saved_state, loader: 'mincepy.Loader') -> dict:
            # Augment the saved state
            saved_state['reg'] = 'unknown'
            return saved_state

    # Set the migration
    LATEST_MIGRATION = V1toV2

    def __init__(self, colour: str, make: str, reg=None):
        super().__init__()
        self.colour = colour
        self.make = make
        self.reg = reg


class HatchbackCarV0(CarV0):
    """A hatchback that inherits from CarV0"""
    TYPE_ID = uuid.UUID('d4131d3c-c140-4959-a545-21082dae9f1b')


class HatchbackCarV1(CarV1):
    """A hatchback that inherits from CarV1, simulating what would have happened if parent was
    migrated"""
    TYPE_ID = uuid.UUID('d4131d3c-c140-4959-a545-21082dae9f1b')


class StoreByValue(mincepy.ConvenientSavable):
    TYPE_ID = uuid.UUID('40377bfc-901c-48bb-a85c-1dd692cddcae')
    ref = mincepy.field()

    def __init__(self, ref):
        super().__init__()
        self.ref = ref


class StoreByRef(mincepy.ConvenientSavable):
    TYPE_ID = uuid.UUID('40377bfc-901c-48bb-a85c-1dd692cddcae')
    ref = mincepy.field(ref=True)

    class ToRefMigration(mincepy.ObjectMigration):
        VERSION = 1

        @classmethod
        def upgrade(cls, saved_state, loader: 'mincepy.Loader') -> dict:
            # Replace the value stored version with a reference
            saved_state['ref'] = mincepy.ObjRef(saved_state['ref'])
            return saved_state

    # Changed my mind, want to store by value now
    LATEST_MIGRATION = ToRefMigration

    def __init__(self, ref):
        super().__init__()
        self.ref = ref


class A(mincepy.ConvenientSavable):
    TYPE_ID = uuid.UUID('a50f21bc-899e-445f-baf7-0a1a373e51fc')
    migrations = mincepy.field()

    class Migration(mincepy.ObjectMigration):
        VERSION = 11

        @classmethod
        def upgrade(cls, saved_state, loader: 'mincepy.Loader'):
            saved_state['migrations'].append('A V11')

    LATEST_MIGRATION = Migration

    def __init__(self):
        super().__init__()
        self.migrations = []


class B(A):
    TYPE_ID = uuid.UUID('f1c07f5f-bf64-441d-8dc7-bbde65eb6fa2')

    class Migration(mincepy.ObjectMigration):
        VERSION = 2

        @classmethod
        def upgrade(cls, saved_state, loader: 'mincepy.Loader'):
            saved_state['migrations'].append('B V2')

    LATEST_MIGRATION = Migration


class BV3(A):
    TYPE_ID = uuid.UUID('f1c07f5f-bf64-441d-8dc7-bbde65eb6fa2')

    class Migration(mincepy.ObjectMigration):
        VERSION = 3
        PREVIOUS = B.Migration

        @classmethod
        def upgrade(cls, saved_state, loader: 'mincepy.Loader'):
            pass

    LATEST_MIGRATION = Migration


class C(B):
    TYPE_ID = uuid.UUID('c76153c1-82d0-4048-bdbe-937889c7fac9')


class C_BV3(BV3):
    TYPE_ID = uuid.UUID('c76153c1-82d0-4048-bdbe-937889c7fac9')
