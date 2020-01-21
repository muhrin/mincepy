from collections import OrderedDict
import typing

import click
import pymongo
from tabulate import tabulate

import mincepy


@click.command()
@click.option('--obj-type', default=None, help="The type of object to find")
@click.option('--filter', default=None, help="Filter on the state")
@click.option('--limit', default=0, help="Limit the number of results")
def query(obj_type, filter, limit):
    historian = mincepy.get_historian()

    results = historian.find(obj_type, criteria=filter, limit=limit, as_objects=False,
                             version=-1)  # type: typing.Sequence[mincepy.DataRecord]

    # Gather by object types
    gathered = {}
    for result in results:
        gathered.setdefault(result.type_id, []).append(result)

    for type_id, records in gathered.items():
        print("type: {}".format(type_id))
        print_records(records)


SCALAR_VALUE = '<value>'
UNSET = ''
REF = 'ref'


def print_records(records: typing.Sequence[mincepy.DataRecord]):
    columns = OrderedDict()
    columns[REF] = [
        record.get_reference() if not record.is_deleted_record() else "{} [deleted]".format(record.get_reference())
        for record in records
    ]

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

    print(tabulate(rows, headers=[".".join(path) if isinstance(path, tuple) else path for path in columns.keys()]))


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
            return str(mincepy.Ref(*value['state']))

        if len(title) > 1:
            return get_value(title[1:], value)

        try:
            return state[idx]
        except (KeyError, IndexError):
            return UNSET

    if title == SCALAR_VALUE:
        return state

    return UNSET


if __name__ == '__main__':
    client = pymongo.MongoClient()
    db = client.test_database
    mongo_archive = mincepy.mongo.MongoArchive(db)

    hist = mincepy.Historian(mongo_archive)
    mincepy.set_historian(hist)

    query()
