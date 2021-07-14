# -*- coding: utf-8 -*-
import pytest

from mincepy import staging


def test_basics():
    with pytest.raises(RuntimeError):
        staging.StagingArea()

    assert staging.get_info(object(), create=False) is None

    with pytest.raises(KeyError):
        staging.remove(object())

    # These calls should do nothing
    staging.replace(object(), object())
