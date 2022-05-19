# -*- coding: utf-8 -*-
import mincepy.type_registry
import mincepy.testing


def test_basics():
    registry = mincepy.type_registry.TypeRegistry()

    # Register using the object type
    registry.register_type(mincepy.testing.Car)
    assert mincepy.testing.Car in registry
    registry.unregister_type(mincepy.testing.Car)
    assert mincepy.testing.Car not in registry
