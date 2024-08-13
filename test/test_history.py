import mincepy
from mincepy import testing


def test_db():
    """Test using the db() free-function to get type helpers"""
    helper = mincepy.db(testing.Car)
    assert isinstance(helper, mincepy.TypeHelper)
    assert helper.TYPE_ID == testing.Car.TYPE_ID
