from collections import OrderedDict
import typing

import click
import pymongo
from tabulate import tabulate

import mincepy
import mincepy.records


@click.command()
@click.option('--obj-type', default=None, help="The type of object to find")
@click.option('--filter', default=None, help="Filter on the state")
@click.option('--limit', default=0, help="Limit the number of results")
def query(obj_type, filter, limit):
    historian = mincepy.get_historian()

    results = historian.find_recods(obj_type, state=filter, limit=limit, version=-1)

    historian.register_types(mincepy.testing.HISTORIAN_TYPES)

    # Gather by object types
    gathered = {}
    for result in results:
        gathered.setdefault(result.type_id, []).append(result)

    for type_id, records in gathered.items():
        print("type: {}".format(type_id))
        print_records(records, historian)


SCALAR_VALUE = '<value>'
UNSET = ''
REF = 'ref'


def print_records(records: typing.Sequence[mincepy.records.DataRecord], historian):
    columns = OrderedDict()
    refs = []
    for record in records:
        try:
            helper = historian.get_helper(record.type_id)
            type_str = get_type_name(helper.TYPE) + "#{}".format(record.version)
        except KeyError:
            type_str = str(record.snapshot_id)
        if record.is_deleted_record():
            type_str += " [deleted]"
        refs.append(type_str)
    columns[REF] = refs

    for record in records:
        if not record.is_deleted_record():
            for column_name in get_all_columns(record.state):
                columns[tuple(column_name)] = []

    for column_name in columns.keys():
        if column_name != REF:
            columns[column_name] = [get_value(column_name, record.state) for record in records]

    rows = []
    for row in range(len(records)):
        rows.append([columns[column][row] for column in columns.keys()])

    print(
        tabulate(rows,
                 headers=[
                     ".".join(path) if isinstance(path, tuple) else path for path in columns.keys()
                 ]))


def get_all_columns(state):
    if isinstance(state, dict):
        if 'type_id' in state and 'state' in state:
            yield []
        else:
            for key, value in state.items():
                key_path = [key]
                if isinstance(value, dict):
                    for path in get_all_columns(value):
                        yield key_path + path
                else:
                    yield key_path
    elif isinstance(state, list):
        yield from [(idx,) for idx in range(len(state))]
    else:
        yield SCALAR_VALUE


def get_value(title, state):
    if isinstance(state, (dict, list)):
        idx = title[0]
        value = state[idx]
        # Check for references
        if isinstance(value, dict) and set(value.keys()) == {'type_id', 'state'}:
            return str(mincepy.records.SnapshotId(*value['state']))

        if len(title) > 1:
            return get_value(title[1:], value)

        try:
            return state[idx]
        except (KeyError, IndexError):
            return UNSET

    if title == SCALAR_VALUE:
        return state

    return UNSET


def get_type_name(obj_type):
    try:
        return "{}.{}".format(obj_type.__module__, obj_type.__name__)
    except AttributeError:
        return str(obj_type)


if __name__ == '__main__':
    client = pymongo.MongoClient()
    db = client.test_database
    mongo_archive = mincepy.mongo.MongoArchive(db)

    hist = mincepy.Historian(mongo_archive)
    mincepy.set_historian(hist)

    query()
