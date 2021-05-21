# -*- coding: utf-8 -*-
import pathlib
import shutil
import tempfile
from typing import Optional, BinaryIO

from . import type_ids
from . import base_savable
from . import fields

__all__ = 'File', 'BaseFile'


class File(base_savable.SimpleSavable):
    """A mincePy file object.  These should not be instantiated directly but using Historian.create_file()"""

    TYPE_ID = type_ids.FILE_TYPE_ID
    READ_SIZE = 256  # The number of bytes to read at a time

    def __init__(self, file_store, filename: str = None, encoding=None):
        super().__init__()
        self._file_store = file_store
        self._filename = filename
        self._encoding = encoding
        self._file_id = None
        self._buffer_file = _create_buffer_file()

    @fields.field('_filename')
    def filename(self) -> Optional[str]:
        return self._filename

    @fields.field('_encoding')
    def encoding(self) -> Optional[str]:
        return self._encoding

    @fields.field('_file_id')
    def file_id(self):
        return self._file_id

    def open(self, mode='r', **kwargs) -> BinaryIO:
        """Open returning a file like object that supports close() and read()"""
        self._ensure_buffer()
        if 'b' not in mode:
            kwargs.setdefault('encoding', self.encoding)
        return open(self._buffer_file, mode, **kwargs)

    def from_disk(self, path):
        """Copy the contents of a disk file to this file"""
        with open(str(path), 'r', encoding=self.encoding) as disk_file:
            with self.open('w') as this:
                shutil.copyfileobj(disk_file, this)

    def to_disk(self, path: [str, pathlib.Path]):
        """Copy the contents of this file to disk.

        :param path: the path can be either a folder in which case the file contents are written to
            `path / self.filename` or path can be a full file path in which case that will be used.
        """
        file_path = pathlib.Path(str(path))
        if file_path.is_dir():
            file_path /= self.filename

        with open(str(file_path), 'w', encoding=self._encoding) as disk_file:
            with self.open('r') as this:
                shutil.copyfileobj(this, disk_file)

    def write_text(self, text: str, encoding=None):
        encoding = encoding or self._encoding
        with self.open('w', encoding=encoding) as fileobj:
            fileobj.write(text)

    def read_text(self, encoding=None) -> str:
        """Read the contents of the file as text.
        This function is named as to mirror pathlib.Path"""
        encoding = encoding or self._encoding
        with self.open('r', encoding=encoding) as fileobj:
            return fileobj.read()

    def save_instance_state(self, saver):
        filename = self.filename or ''
        with open(self._buffer_file, 'rb') as fstream:
            self._file_id = self._file_store.upload_from_stream(filename, fstream)

        return super().save_instance_state(saver)

    def load_instance_state(self, saved_state, loader):
        super().load_instance_state(saved_state, loader)
        self._file_store = loader.get_archive().file_store
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

    def __str__(self):
        contents = [str(self._filename)]
        if self._encoding is not None:
            contents.append(f'({self._encoding})')
        return ' '.join(contents)

    def __eq__(self, other) -> bool:
        """Compare the contents of two files

        If both files do not exist they are considered equal.
        """
        if not isinstance(other, File) or self.filename != other.filename:  # pylint: disable=comparison-with-callable
            return False

        try:
            with self.open() as my_file:
                try:
                    with other.open() as other_file:
                        while True:
                            my_line = my_file.readline(self.READ_SIZE)
                            other_line = other_file.readline(self.READ_SIZE)
                            if my_line != other_line:
                                return False
                            if my_line == '' and other_line == '':
                                return True
                except FileNotFoundError:
                    return False
        except FileNotFoundError:
            # Our file doesn't exist, make sure the other doesn't either
            try:
                with other.open():
                    return False
            except FileNotFoundError:
                return True

    def yield_hashables(self, hasher):
        """Hash the contents of the file"""
        try:
            with self.open('rb') as opened:
                while True:
                    line = opened.read(self.READ_SIZE)
                    if line == b'':
                        return
                    yield line
        except FileNotFoundError:
            yield from hasher.yield_hashables(None)


def _create_buffer_file():
    tmp_file = tempfile.NamedTemporaryFile(delete=False)
    tmp_path = tmp_file.name
    tmp_file.close()
    return tmp_path


BaseFile = File  # Here just for legacy reasons.  Deprecate in 1.0
