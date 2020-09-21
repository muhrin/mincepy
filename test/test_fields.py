import pytest

import mincepy
from mincepy import fields


class Image(fields.WithFields):
    width = mincepy.field()
    height = mincepy.field()
    _fmt = mincepy.field(store_as='format')

    def __init__(self, width: int, height: int, fmt: str, creator=None):
        self.width = width
        self.height = height
        self._fmt = fmt
        self._creator = creator

    def area(self):
        return self.width * self.height

    @mincepy.field(store_as='_creator')
    def creator(self):
        return self._creator

    @creator.setter
    def creator(self, value):
        self._creator = value


def test_fields():
    # pylint: disable=protected-access

    img = Image(1024, 768, 'png')
    img_dict = fields.save_instance_state(img)
    assert img_dict == dict(
        width=1024,
        height=768,
        format='png',  # Because we asked 'fmt' to be stored as 'format'
        _creator=None,
    )

    img.creator = 'martin'
    img_dict = fields.save_instance_state(img)
    assert img_dict == dict(
        width=1024,
        height=768,
        format='png',  # Because we asked 'fmt' to be stored as 'format'
        _creator='martin',
    )

    img_dict['_creator'] = 'sonia'
    fields.load_instance_state(img, img_dict)
    assert img.creator == 'sonia'

    # Now check what it does if we miss a value
    with pytest.raises(ValueError):
        fields.load_instance_state(img, dict(width=512, height=512), ignore_missing=False)

    # Check that it hasn't destroyed the state
    assert img.width == 1024
    assert img.height == 768
    assert img._fmt == 'png'
    assert img.creator == 'sonia'

    fields.load_instance_state(img, {}, ignore_missing=True)
    assert img.width is None
    assert img.height is None
    assert img._creator is None


class Animal(fields.WithFields):

    def __init__(self):
        self._specie = None

    def set_specie(self, specie):
        self._specie = specie

    def get_specie(self):
        return self._specie

    def del_specie(self):
        del self._specie

    specie = mincepy.field()(get_specie, set_specie, del_specie)


def test_field_property():
    seal = Animal()
    seal.specie = 'seal'
    assert seal.specie == 'seal'
    assert fields.save_instance_state(seal) == dict(specie='seal')
    del seal.specie
    with pytest.raises(AttributeError):
        print(seal.specie)
