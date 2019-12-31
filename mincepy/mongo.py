import typing

import bson

from .archive import BaseArchive, DataRecord, TypeCodec

__all__ = ('MongoArchive',)


class MongoArchive(BaseArchive):
    DATA_COLLECTION = 'data'

    def __init__(self, db, codecs=tuple()):
        super(MongoArchive, self).__init__(codecs)
        self._data_collection = db[self.DATA_COLLECTION]

    def create_archive_id(self):
        return bson.ObjectId()

    def save(self, record: DataRecord):
        entry = {
            '_id': record.obj_id,
            'type_id': record.type_id,
            'ancestor_id': record.ancestor_id,
            'encoded_value': record.encoded_value,
            'hash': record.obj_hash
        }
        self._data_collection.insert_one(entry)

    def save_many(self, records: typing.List[DataRecord]):
        for record in records:
            self.save(record)

    def load(self, archive_id) -> DataRecord:
        entry = self._data_collection.find_one(filter=archive_id)
        return DataRecord(entry['_id'], entry['type_id'], entry['ancestor_id'], entry['encoded_value'], entry['hash'])
