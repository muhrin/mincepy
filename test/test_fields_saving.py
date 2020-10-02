# -*- coding: utf-8 -*-
import argparse
import datetime

import pytest

import mincepy
from mincepy import expr
from mincepy import fields
from mincepy import saving

# pylint: disable=too-few-public-methods, invalid-name, pointless-statement, protected-access


class Image(fields.WithFields):
    width = fields.field()
    height = fields.field()
    _fmt = mincepy.field(store_as='format')

    def __init__(self, width: int, height: int, fmt: str, creator=None):
        super().__init__()
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


class Profile(fields.WithFields):
    name = fields.field()
    thumbnail = fields.field(type=Image)  # Of type image

    def __init__(self, name, thumbnail):
        self.name = name
        self.thumbnail = thumbnail


def test_field_basics():
    # Can't use full stop in field name
    with pytest.raises(ValueError):
        fields.FieldProperties(store_as='my.field')

    # Try getting an invalid attribute
    field = fields.field(type=Image)
    with pytest.raises(AttributeError):
        field.area

    # However, this is fine with dynamic fields
    field = fields.field(dynamic=True)
    assert isinstance(field.area, fields.Field)
    # The dynamic property should be propagated
    assert isinstance(field.area.height, fields.Field)

    # By default a field is read/write/deletable depending on the object
    field = fields.field(attr='value')
    ns = argparse.Namespace()
    field.__set__(ns, 5)
    assert field.__get__(ns) == 5
    field.__delete__(ns)
    assert 'value' not in ns.__dict__

    # however, if it is converted to a property it becomes read-only until a setter is given
    ns = argparse.Namespace()
    ns.value = 5
    field = fields.field(attr='value')(lambda obj: getattr(obj, 'value'))
    assert field.__get__(ns) == 5
    with pytest.raises(AttributeError):
        field.__set__(ns, 10)
    # But if we add the setter...
    field.setter(lambda obj, value: setattr(obj, 'value', value))
    field.__set__(ns, 10)
    assert field.__get__(ns) == 10

    # Now check deleting
    with pytest.raises(AttributeError):
        field.__delete__(ns)
    field.deleter(lambda obj: delattr(obj, 'value'))
    field.__delete__(ns)
    with pytest.raises(AttributeError):
        field.__get__(ns)

    # Test unreadable attribute
    field = fields.field(attr='unexistent')

    class Test:
        pass

    with pytest.raises(AttributeError):
        field.__get__(Test())


def test_immutable_property():
    """Check that we can handle immutable properties"""

    class Timestamp(fields.WithFields):

        def __init__(self):
            self._creation_time = datetime.datetime.now()

        @fields.field(attr='_creation_time')
        def creation_time(self):
            return self._creation_time

        # No setter for creation time

    ts = Timestamp()
    saved_state = saving.save_instance_state(ts)
    # Now manually fiddle the creation time
    ctime = ts.creation_time
    ts._creation_time = None
    saving.load_instance_state(ts, saved_state)

    # Check that it's been restored correctly
    assert (ts._creation_time == ctime) is True  # pylint: disable=comparison-with-callable
    assert ts.creation_time == ctime


def test_fields():
    # pylint: disable=protected-access
    img = Image(1024, 768, 'png')
    img_dict = saving.save_instance_state(img)
    assert img_dict == dict(
        width=1024,
        height=768,
        format='png',  # Because we asked 'fmt' to be stored as 'format'
        _creator=None,
    )

    img.creator = 'martin'
    img_dict = saving.save_instance_state(img)
    assert img_dict == dict(
        width=1024,
        height=768,
        format='png',  # Because we asked 'fmt' to be stored as 'format'
        _creator='martin',
    )

    img_dict['_creator'] = 'sonia'
    saving.load_instance_state(img, img_dict)
    assert img.creator == 'sonia'

    # Now check what it does if we miss a value
    with pytest.raises(ValueError):
        saving.load_instance_state(img, dict(width=512, height=512), ignore_missing=False)

    # Check that it hasn't destroyed the state
    assert img.width == 1024
    assert img.height == 768
    assert img._fmt == 'png'
    assert img.creator == 'sonia'

    saving.load_instance_state(img, {}, ignore_missing=True)
    assert img.width is None
    assert img.height is None
    assert img._creator is None

    # Check that we can get field using getitem
    width_field = Image['width']
    assert isinstance(width_field, fields.Field)
    assert width_field._properties.attr_name == 'width'


def test_nested_fields():
    assert expr.query_expr(Profile.thumbnail.width == 64) == {'thumbnail.width': 64}

    assert expr.query_expr(Profile.thumbnail == {'width': 64, 'height': 128}) == \
           {'thumbnail': {'width': 64, 'height': 128}}

    # Add a query context
    tag_eq_holiday = fields.field('tag') == 'holiday'
    Profile.thumbnail.set_query_context(tag_eq_holiday)
    # check the query context is being used
    assert expr.query_expr(Profile.thumbnail == {'width': 64, 'height': 128}) == \
           {'$and': [{'tag': 'holiday'}, {'thumbnail': {'width': 64, 'height': 128}}]}

    # now check that it is carried over to 'width'
    assert expr.query_expr(Profile.thumbnail.width == 64) == \
           {'$and': [{'tag': 'holiday'}, {'thumbnail.width': 64}]}


class Animal(fields.WithFields):

    def __init__(self):
        self._specie = None

    def set_specie(self, specie):
        self._specie = specie

    def get_specie(self):
        return self._specie

    def del_specie(self):
        del self._specie

    specie = fields.field()(get_specie, set_specie, del_specie)


def test_field_property():
    seal = Animal()
    seal.specie = 'seal'
    assert seal.specie == 'seal'
    assert saving.save_instance_state(seal) == dict(specie='seal')
    del seal.specie
    with pytest.raises(AttributeError):
        print(seal.specie)

    assert repr(Animal.specie._properties).startswith('FieldProperties(')


def test_with_fields_constructor():
    """Check the default constructor provided by with fields"""

    class Car(fields.WithFields):
        make = fields.field('_make')
        colour = fields.field(default='red')

    car = Car(make='ferrari', colour='red')
    assert car.make == 'ferrari'
    assert car._make == 'ferrari'
    assert car.colour == 'red'

    with pytest.raises(ValueError):
        Car(year=1085)

    # Test default colour
    assert Car(make='honda').colour == 'red'
