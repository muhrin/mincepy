import mincepy
from mincepy.testing import Person


def test_references_simple(historian: mincepy.Historian):
    address_book = mincepy.RefList()
    for i in range(10):
        address_book.append(Person(name='test', age=i))
    address_book.save()

    refs = historian.references.references(address_book.obj_id)
    assert len(refs) == len(address_book)
    assert not set(person.obj_id for person in address_book) - set(refs)

    refs = historian.references.referenced_by(address_book[0].obj_id)
    assert len(refs) == 1
    assert refs[0] == address_book.obj_id
