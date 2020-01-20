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
        print(type_id)
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
        for column_name in get_all_columns(record):
            columns[column_name] = []

    for column_name in columns.keys():
        if column_name != REF:
            columns[column_name] = [get_value(column_name, record.state) for record in records]

    rows = []
    for row in range(len(records)):
        rows.append([columns[column][row] for column in columns.keys()])

    print(tabulate(rows, headers=columns.keys()))


def get_all_columns(record):
    if isinstance(record.state, dict):
        for key in record.state:
            yield key
    elif isinstance(record.state, list):
        yield from range(len(record.state))
    else:
        if not record.is_deleted_record():
            yield SCALAR_VALUE


def get_value(title, state):
    if isinstance(state, (dict, list)):
        try:
            return state[title]
        except (KeyError, IndexError):
            return UNSET
    if title == SCALAR_VALUE:
        return state

    return UNSET


def get_row(headers, state):
    if isinstance(state, dict, list):
        for title in headers:
            yield state[title]
    else:
        yield state


def get_dict_table(records: typing.Sequence[mincepy.DataRecord]):
    # Gather columns

    columns = set()
    for record in records:
        columns.update(record.state.keys())

    table = []
    for record in records:
        row = [str(record.get_reference())]
        for column in columns:
            row.append(str(record.state.get(column, '-')))
        table.append(row)

    headers = ['ref']
    headers.extend(columns)

    return headers, table


def dict_to_rows(data: typing.Mapping, path=None):
    path = path or []
    for key, value in data.items():
        current_path = path + [key]
        if isinstance(value, typing.Mapping):
            yield from dict_to_rows(value, current_path)
        else:
            yield current_path


def get_list_table(records: typing.Sequence[mincepy.DataRecord]):
    longest = max(len(record.state) for record in records)
    return list(range(longest)), [str(record.state) for record in records]


if __name__ == '__main__':
    client = pymongo.MongoClient()
    db = client.test_database
    mongo_archive = mincepy.mongo.MongoArchive(db)

    hist = mincepy.Historian(mongo_archive)
    mincepy.set_historian(hist)

    query()
