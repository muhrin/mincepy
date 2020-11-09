# -*- coding: utf-8 -*-
import abc
import pathlib
import shutil
from typing import Optional, BinaryIO

from . import base_savable
from . import fields

__all__ = 'File', 'BaseFile'


class File(base_savable.SimpleSavable):
    READ_SIZE = 256  # The number of bytes to read at a time

    def __init__(self, filename: str = None, encoding=None):
        super().__init__()
        self._filename = filename
        self._encoding = encoding

    @fields.field('_filename')
    def filename(self) -> Optional[str]:
        return self._filename

    @fields.field('_encoding')
    def encoding(self) -> Optional[str]:
        return self._encoding

    @abc.abstractmethod
    def open(self, mode='r', **kwargs) -> BinaryIO:
        """Open returning a file like object that supports close() and read()"""

    def from_disk(self, path):
        """Copy the contents of a disk file to this file"""
        with open(str(path), 'r', encoding=self.encoding) as disk_file:
            with self.open('w') as this:
                shutil.copyfileobj(disk_file, this)

    def to_disk(self, folder: [str, pathlib.Path]):
        """Copy the contents of this file to a file on disk in the given folder"""
        file_path = pathlib.Path(str(folder)) / self.filename
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

    def __str__(self):
        contents = [str(self._filename)]
        if self._encoding is not None:
            contents.append('({})'.format(self._encoding))
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


BaseFile = File  # Here just for legacy reasons.  Deprecate in 1.0
