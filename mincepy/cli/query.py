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

    results = historian.find(obj_type, criteria=filter, limit=limit,
                             as_objects=False)  # type: typing.Sequence[mincepy.DataRecord]

    # Gather by object types
    gathered = {}
    for result in results:
        gathered.setdefault(result.type_id, []).append(result)

    for type_id, records in gathered.items():
        print(type_id)
        print_records(records)


def print_records(records: typing.Sequence[mincepy.DataRecord]):
    if isinstance(records[0].state, dict):
        headers, table = get_dict_table(records)
    elif isinstance(records[0], list):
        headers, table = get_list_table(records)
    else:
        headers, table = [str(record.state) for record in records]

    print(tabulate(table, headers=headers))


def get_dict_table(records: typing.Sequence[mincepy.DataRecord]):
    # Gather columns

    columns = set()
    for record in records:
        columns.update(record.state.keys())

    table = []
    for record in records:
        row = [record.obj_id]
        for column in columns:
            row.append(str(record.state.get(column, '-')))
        table.append(row)

    headers = ['Obj-id']
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
