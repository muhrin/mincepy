import pytest

import mincepy
from mincepy.testing import Car


def test_meta_index_unique(historian: mincepy.Historian):
    historian.meta.create_index('reg', True)
    car = Car()
    car.save(with_meta={'reg': 'VD495'})

    historian.archive.find_meta(dict(reg='VD495'))

    with pytest.raises(mincepy.DuplicateKeyError):
        Car().save(with_meta={'reg': 'VD495'})
