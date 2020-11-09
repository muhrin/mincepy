# -*- coding: utf-8 -*-
from . import common_helpers
from . import testing


def get_types():
    """Provide a list of all historian types"""
    types = list()
    types.extend(common_helpers.HISTORIAN_TYPES)
    types.extend(testing.HISTORIAN_TYPES)

    return types
