import tempfile
import uuid

import bson
import gridfs

import mincepy

__all__ = ('GridFsFile',)


class GridFsFile(mincepy.BaseFile):
    TYPE_ID = uuid.UUID('3bf3c24e-f6c8-4f70-956f-bdecd7aed091')
    ATTRS = '_persistent_id', '_file_id'

    def __init__(self,
                 file_bucket: gridfs.GridFSBucket,
                 filename: str = None,
                 encoding: str = None):
        super().__init__(filename, encoding)
        self._file_store = file_bucket
        self._file_id = None
        self._persistent_id = bson.ObjectId()
        self._buffer_file = _create_buffer_file()

    def open(self, mode='r', **kwargs):
        self._ensure_buffer()
        if 'b' not in mode:
            kwargs.setdefault('encoding', self.encoding)
        return open(self._buffer_file, mode, **kwargs)

    def save_instance_state(self, saver: mincepy.Saver):
        filename = self.filename or ""
        with open(self._buffer_file, 'rb') as fstream:
            self._file_id = self._file_store.upload_from_stream(filename, fstream)

        return super().save_instance_state(saver)

    def load_instance_state(self, saved_state, loader: mincepy.Loader):
        super().load_instance_state(saved_state, loader)
        self._file_store = loader.get_archive().get_gridfs_bucket()  # type: gridfs.GridFSBucket
        # Don't copy the file over now, do it lazily when the file is first opened
        self._buffer_file = None

    def _ensure_buffer(self):
        if self._buffer_file is None:
            if self._file_id is not None:
                self._update_buffer()
            else:
                _create_buffer_file()

    def _update_buffer(self):
        self._buffer_file = _create_buffer_file()
        with open(self._buffer_file, 'wb') as fstream:
            self._file_store.download_to_stream(self._file_id, fstream)


def _create_buffer_file():
    tmp_file = tempfile.NamedTemporaryFile(delete=False)
    tmp_path = tmp_file.name
    tmp_file.close()
    return tmp_path
