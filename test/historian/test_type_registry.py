from mincepy import type_registry
from test.common import A, B, C

# pylint: disable=invalid-name


def test_get_version_info():
    registry = type_registry.TypeRegistry()
    registry.register_type(A)
    registry.register_type(B)
    registry.register_type(C)

    c_info = registry.get_version_info(C)
    # Check versions
    assert c_info[A.TYPE_ID] == A.LATEST_MIGRATION.VERSION
    assert c_info[B.TYPE_ID] == B.LATEST_MIGRATION.VERSION
    assert c_info[C.TYPE_ID] == C.LATEST_MIGRATION.VERSION


def test_automatic_registration_of_parent():
    """Test that if a historian type with historian type ancestor(s) is registered then so are its
    ancestors"""
    registry = type_registry.TypeRegistry()
    registry.register_type(C)
    assert A in registry
    assert B in registry
