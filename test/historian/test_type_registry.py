# -*- coding: utf-8 -*-
import pytest

from mincepy import type_registry

from test import common  # pylint: disable=wrong-import-order

# pylint: disable=invalid-name


def test_get_version_info():
    registry = type_registry.TypeRegistry()
    registry.register_type(common.A)
    registry.register_type(common.B)
    registry.register_type(common.C)

    c_info = registry.get_version_info(common.C)
    # Check versions
    assert c_info[common.A.TYPE_ID] == common.A.LATEST_MIGRATION.VERSION
    assert c_info[common.B.TYPE_ID] == common.B.LATEST_MIGRATION.VERSION
    assert c_info[common.C.TYPE_ID] == common.C.LATEST_MIGRATION.VERSION


def test_automatic_registration_of_parent():
    """Test that if a historian type with historian type ancestor(s) is registered then so are its
    ancestors"""
    registry = type_registry.TypeRegistry()
    registry.register_type(common.C)
    assert common.A in registry
    assert common.B in registry


def test_get_type_id():
    registry = type_registry.TypeRegistry()
    with pytest.raises(ValueError):
        registry.get_type_id(common.A)

    registry.register_type(common.A)
    # This should return the superclass as we haven't registered B yet
    assert registry.get_type_id(common.B) == common.A.TYPE_ID

    registry.register_type(common.B)
    assert registry.get_type_id(common.B) == common.B.TYPE_ID


def test_get_helper_from_type_id():
    registry = type_registry.TypeRegistry()
    registry.register_type(common.A)
    helper = registry.get_helper_from_type_id(common.A.TYPE_ID)
    assert helper.TYPE is common.A
    assert helper.TYPE_ID == common.A.TYPE_ID
