import uuid

from . import archive


class InMemory(archive.BaseArchive):
    """An archive that keeps things in memory"""

    def __init__(self, codecs=tuple()):
        super(InMemory, self).__init__(codecs)
        self._records = {}

    def create_archive_id(self):
        return uuid.uuid4()

    def save(self, record: archive.DataRecord):
        self._records[record.persistent_id] = record

    def load(self, archive_id) -> archive.DataRecord:
        return self._records[archive_id]
