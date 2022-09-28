# -*- coding: utf-8 -*-
import uuid

import pytest

import mincepy
from mincepy import testing
from mincepy.testing import Car

# pylint: disable=protected-access


def test_transaction_snapshots(historian: mincepy.Historian):
    ferrari = Car("ferrari")
    historian.save(ferrari)
    ferrari_sid = historian.get_snapshot_id(ferrari)

    with historian.transaction():
        ferrari_snapshot_1 = historian.load_snapshot(ferrari_sid)
        with historian.transaction():
            ferrari_snapshot_2 = historian.load_snapshot(ferrari_sid)
            # Reference wise they should be unequal
            assert ferrari_snapshot_1 is not ferrari_snapshot_2
            assert ferrari is not ferrari_snapshot_1
            assert ferrari is not ferrari_snapshot_2

            # Value wise they should be equal
            assert ferrari == ferrari_snapshot_1
            assert ferrari == ferrari_snapshot_2

        # Now check within the same transaction the result is the same
        ferrari_snapshot_2 = historian.load_snapshot(ferrari_sid)
        # Reference wise they should be unequal
        assert ferrari_snapshot_1 is not ferrari_snapshot_2
        assert ferrari is not ferrari_snapshot_1
        assert ferrari is not ferrari_snapshot_2

        # Value wise they should be equal
        assert ferrari == ferrari_snapshot_1
        assert ferrari == ferrari_snapshot_2


def test_transaction_records(historian: mincepy.Historian):
    """Make sure that records within a transaction are not recreated at each save"""
    with historian.transaction():
        ferrari = Car("ferrari")

        # Save and get the record for the ferrari
        ferrari_id = historian.save(ferrari)
        record = historian.get_current_record(ferrari)

        ferrari_id2 = historian.save(ferrari)
        assert ferrari_id == ferrari_id2
        assert historian.get_current_record(ferrari) is record

        loaded = historian.load(ferrari_id)
        assert loaded is ferrari


def test_find(historian: mincepy.Historian):
    honda_id = historian.save(Car("honda"))
    zonda_id = historian.save(Car("zonda"))
    porsche_id = historian.save(Car("porsche"))

    cars = list(historian.find(Car))
    assert len(cars) == 3

    makes = [car.make for car in cars]
    assert "honda" in makes
    assert "zonda" in makes
    assert "porsche" in makes

    obj_ids = [car.obj_id for car in cars]
    assert honda_id in obj_ids
    assert zonda_id in obj_ids
    assert porsche_id in obj_ids


def test_sync(historian: mincepy.Historian, archive_uri):
    historian2 = mincepy.connect(archive_uri)
    historian2.register_types(mincepy.testing.HISTORIAN_TYPES)

    car = Car("ferrari", "red")
    historian.save_one(car)

    # Simulate saving the car from another connection
    same_car = historian2.load(car.obj_id)
    same_car.make = "honda"
    same_car.colour = "black"
    same_car.save()
    honda_record = historian2.get_current_record(same_car)
    del same_car

    # Now update and check the state
    historian.sync(car)
    assert car.make == "honda"
    assert car.colour == "black"

    # Also, check the cached record
    car_record = historian.get_current_record(car)
    assert car_record.snapshot_hash == honda_record.snapshot_hash
    assert car_record.state == honda_record.state

    # Check that syncing an unsaved object returns False (because there's nothing to do)
    assert historian.sync(Car()) is False

    # Test syncing an object that is deleted
    historian2.delete(car.obj_id)
    with pytest.raises(mincepy.NotFound):
        car.sync()
    #
    # car = Car()
    # historian.save(car)
    # with historian.transaction():
    #     historian.delete(car)
    #     with pytest.raises(mincepy.ObjectDeleted):
    #         historian.get_current_record(car)
    #     with pytest.raises(mincepy.ObjectDeleted):
    #         historian.sync(car)


def test_to_obj_id(historian: mincepy.Historian):
    car = Car()
    car_id = car.save()

    assert historian.to_obj_id(car_id) is car_id
    assert historian.to_obj_id(car) == car_id
    assert historian.to_obj_id(str(car_id)) == car_id
    assert historian.to_obj_id("carrot") is None
    assert historian.to_obj_id(historian.get_snapshot_id(car)) == car_id


def test_copy(historian: mincepy.Historian):
    car = Car("zonda")

    historian.save(car)
    car_copy = mincepy.copy(car)
    assert car == car_copy
    assert car is not car_copy
    car_copy.save()

    record = historian.get_current_record(car)
    copy_record = historian.get_current_record(car_copy)

    assert record is not copy_record
    assert copy_record.get_copied_from() == record.snapshot_id


def test_copy_unsaved(historian: mincepy.Historian):
    car = Car("porsche", "silver")
    car_copy = mincepy.copy(car)

    assert car_copy is not car
    assert car == car_copy

    # The cars should not be saved
    assert not historian.is_known(car)
    assert not historian.is_known(car_copy)


def test_save(historian: mincepy.Historian):
    car = Car("porsche", "yellow")
    with pytest.raises(ValueError):
        historian.save((car, {"speed": "fast"}, 124))


def test_is_trackable(historian: mincepy.Historian):
    assert historian.is_trackable(mincepy.testing.Car) is True
    assert historian.is_trackable(5) is False
    assert historian.is_trackable(5.6) is False
    assert historian.is_trackable("hello") is False
    assert historian.is_trackable(False) is False
    assert historian.is_trackable(b"byte me") is False


def test_delete_referenced(historian: mincepy.Historian):
    car = testing.Car()
    garage = testing.Garage(mincepy.ObjRef(car))
    garage.save()

    # Should be prevented from deleting the car as it is referenced by the garage
    try:
        historian.delete(car)
    except mincepy.ReferenceError as exc:
        assert exc.references == {car.obj_id}
    else:
        assert False, "Reference error should have been raised"

    historian.delete(garage)
    # Now safe to delete car
    historian.delete(car)

    car = testing.Car()
    garage = testing.Garage(mincepy.ObjRef(car))
    garage.save()

    # Now, check that deleting both together works
    historian.delete(car, garage)


def test_find_arg_types(historian: mincepy.Historian):
    """Test the argument types accepted by the historian find() method"""
    red_ferrari = testing.Car(colour="red", make="ferrari")
    green_ferrari = testing.Car(colour="green", make="ferrari")
    red_honda = testing.Car(colour="red", make="honda")
    martin = testing.Person(name="martin", age=35, car=red_honda)

    red_ferrari_id, green_ferrari_id, red_honda_id = historian.save(
        red_ferrari, green_ferrari, red_honda
    )
    martin_id = martin.save()

    # Test different possibilities for object ids being passed
    list(historian.find(obj_id=red_ferrari_id))
    list(
        historian.find(
            obj_id=[red_ferrari_id, green_ferrari_id, martin_id, red_honda_id]
        )
    )
    list(
        historian.find(
            obj_id=(red_ferrari_id, green_ferrari_id, martin_id, red_honda_id)
        )
    )
    list(historian.find(obj_id=str(red_ferrari_id)))

    # Test object types
    list(historian.find(obj_type=testing.Person))
    list(historian.find(obj_type=[testing.Person, testing.Car]))
    list(historian.find(obj_type=(testing.Person, testing.Car)))
    list(historian.find(obj_type=testing.Person.TYPE_ID))
    list(historian.find(obj_type=[testing.Person.TYPE_ID, testing.Car.TYPE_ID]))


def test_concurrent_modification(historian: mincepy.Historian, archive_uri: str):
    # Create a second historian connected to the same archive
    historian2 = mincepy.connect(archive_uri, use_globally=False)
    historian2.register_type(Car)

    ferrari = testing.Car(colour="red", make="ferrari")
    ferrari_id = historian.save(ferrari)
    ferrari2 = historian2.load(ferrari_id)

    assert ferrari_id == ferrari2.obj_id
    assert (
        ferrari is not ferrari2
    ), "The archive don't know about each other so the objects instances should not be the same"

    # Repaint
    ferrari.colour = "yellow"
    historian.save(ferrari)

    # Now change ferrari2 and see what happens
    ferrari2.colour = "green"
    with pytest.raises(mincepy.ModificationError):
        historian2.save(ferrari2)

    # Now, let's sync up
    assert historian2.sync(ferrari2), "ferrari2 hasn't been updated"
    assert ferrari2.colour == "yellow"


def test_replace_simple(historian: mincepy.Historian):
    def paint_shop(car, colour):
        """An imaginary function that modifies an object but returns a copy rather than an in
        place modification"""
        return Car(car.make, colour)

    honda = Car("honda", "yellow")
    honda_id = historian.save(honda)

    # Now paint the honda
    new_honda = paint_shop(honda, "green")
    assert historian.get_obj_id(honda) == honda_id

    # Now we know that this is a 'continuation' of the history of the original honda, so replace
    historian.replace(honda, new_honda)
    assert historian.get_obj_id(honda) is None

    assert historian.get_obj_id(new_honda) == honda_id
    historian.save(new_honda)
    del honda, new_honda

    loaded = historian.load(honda_id)
    assert loaded.make == "honda"
    assert loaded.colour == "green"

    with pytest.raises(RuntimeError):
        # Check that we can't replace in a transaction
        with historian.transaction():
            historian.replace(loaded, Car())


def test_snapshots_collection(historian: mincepy.Historian):
    ferrari = testing.Car(colour="red", make="ferrari")
    ferrari_id = ferrari.save()

    records = list(historian.snapshots.records.find())
    assert len(records) == 1

    snapshots = list(historian.snapshots.find())
    assert len(snapshots) == 1
    assert snapshots[0] == ferrari

    ferrari.colour = "brown"
    ferrari.save()

    records = list(historian.snapshots.records.find())
    assert len(records) == 2

    snapshots = list(historian.snapshots.find())
    assert len(snapshots) == 2
    assert set(car.colour for car in snapshots) == {"red", "brown"}

    assert (
        historian.snapshots.records.find(Car.colour == "brown", obj_id=ferrari_id)
        .one()
        .version
        == 1
    )


def test_objects_collection(historian: mincepy.Historian):
    ferrari = testing.Car(colour="red", make="ferrari")
    ferrari_id = ferrari.save()

    records = list(historian.objects.records.find())
    assert len(records) == 1

    objects = list(historian.objects.find())
    assert len(objects) == 1
    assert objects[0] is ferrari

    ferrari.colour = "brown"
    ferrari.save()

    records = list(historian.objects.records.find())
    assert len(records) == 1

    objects = list(historian.objects.find())
    assert len(objects) == 1
    assert set(car.colour for car in objects) == {"brown"}

    assert (
        historian.objects.records.find(Car.colour == "brown", obj_id=ferrari_id)
        .one()
        .version
        == 1
    )


def test_get_obj_type(historian: mincepy.Historian):
    assert historian.get_obj_type(Car.TYPE_ID) is Car


def test_get_obj_id(historian: mincepy.Historian):
    """Test the get_obj_id method"""
    unsaved = testing.Car()
    car = testing.Car()
    obj_id = car.save()

    assert historian.get_obj_id(car) is obj_id
    assert historian.get_obj_id(unsaved) is None
    with historian.transaction():
        assert historian.get_obj_id(car) is obj_id
        historian.delete(car)
        assert historian.get_obj_id(car) is None
    assert historian.get_obj_id(car) is None


def test_merge(historian: mincepy.Historian):
    with testing.temporary_historian(
        testing.create_archive_uri(db_name="test_historian")
    ) as remote:
        local = historian
        # remote = clean_test_historian

        remote_skoda = testing.Car(make="skoda", colour="green")
        skoda_id = remote.save(remote_skoda)
        assert remote_skoda._historian is remote

        result = local.merge(remote.objects.find(obj_id=skoda_id))
        assert remote.get_snapshot_id(remote_skoda) in result.merged
        assert local.find(obj_id=skoda_id).count() == 1

        # Now, let's update and see if we can merge
        remote_skoda.colour = "yellow"
        remote.save(remote_skoda)

        result = local.merge(remote.objects.find(obj_id=skoda_id))
        assert remote.get_snapshot_id(remote_skoda) in result.merged
        assert local.snapshots.find(obj_id=skoda_id).count() == 2
        assert local.find(obj_id=skoda_id).one().colour == "yellow"

        # Now, change both to the same thing
        local_skoda = local.load(skoda_id)
        assert local_skoda._historian is local
        assert local_skoda is not remote_skoda

        local_skoda.colour = "blue"
        local_skoda.save()

        remote_skoda.colour = "blue"
        remote_skoda.save()
        result = local.merge(remote.objects.find(obj_id=skoda_id))
        assert not result.merged  # None should have been transferred

        # Now check that conflicts are correctly handled
        remote_skoda.colour = "brown"
        remote_skoda.save()
        local_skoda.colour = "grey"
        local_skoda.save()
        with pytest.raises(mincepy.MergeError):
            local.merge(remote.objects.find(obj_id=skoda_id))


def test_large_merge(
    historian: mincepy.Historian,
    large_dataset,  # pylint: disable=unused-argument
):
    with testing.temporary_historian(
        testing.create_archive_uri(db_name="test_historian")
    ) as remote:
        local = historian

        all_objects = local.find()
        assert all_objects.count() > 0

        # Merge the large dataset into the remote historian
        result = remote.merge(all_objects)
        assert len(result.all) == len(result.merged) == local.find().count()
        assert local.find().count() == remote.find().count()


def test_merge_file(historian: mincepy.Historian):
    """Test that merging files works correctly"""
    file = historian.create_file("test.dat")
    file.write_text("bla bla")
    file.save()
    with testing.temporary_historian(
        testing.create_archive_uri(db_name="test_historian")
    ) as remote:
        local = historian
        # Merge the file into the remote
        result = remote.merge(local.find(obj_id=file.obj_id))
        assert len(result.all) == len(result.merged) == local.find().count()
        assert local.find().count() == remote.find().count()

        remote_file = remote.get(file.obj_id)
        assert file.read_text() == remote_file.read_text()

        # Now check that files contained within objects are correctly merged
        file_list = mincepy.List((file,))
        file_list.save()  # pylint: disable=no-member
        result = remote.merge(
            local.find(obj_id=file_list.obj_id)
        )  # pylint: disable=no-member
        assert len(result.merged) == 1
        assert historian.get_snapshot_id(file_list) in result.merged

        remote_file_list = remote.get(file_list.obj_id)  # pylint: disable=no-member
        assert file.read_text() == remote_file_list[0].read_text()


def test_primitive_subtypes(historian: mincepy.Historian):
    """This test catches the case where someone creates a subclass of a primitive.  This should not be
    treated as a primitive by the historian as we need to reload the correct type when retrieving from
    the database."""

    class DictSubclass(dict, mincepy.BaseSavableObject):
        TYPE_ID = uuid.UUID("67a939ee-4be6-4006-ac77-fd1dbf3b0642")

    custom_dict = DictSubclass()
    assert not historian.is_primitive(custom_dict)
    assert not mincepy.types.is_primitive(custom_dict)


def test_purge(historian: mincepy.Historian):
    car = Car()
    garage = testing.Garage(mincepy.ObjRef(car))
    car_id, _ = historian.save(car, garage)

    # Now update the car
    car.colour = "blue"
    car.save()

    records_count = historian.records.find().count()

    # This should not perge anything as v0 of the car has a reference from the garage, while
    # v1 is the current, live, version
    res = historian.purge(dry_run=False)
    assert not res.deleted_purged
    assert not res.unreferenced_purged
    assert records_count == historian.records.find().count()

    # Now mutate the car, save and purge again
    car.colour = "white"
    car.save()

    # This time, version 1 is unreferenced by anything and so can be safely deleted
    res = historian.purge(dry_run=False)
    assert not res.deleted_purged
    assert res.unreferenced_purged == {mincepy.SnapshotId(car_id, 1)}
    assert records_count == historian.records.find().count()
