import pathlib
import uuid

import mincepy

from .common import Car


def test_path_helper(historian: mincepy.Historian):
    historian.register_type(mincepy.common_helpers.PathHelper())

    class File(mincepy.Archivable):
        TYPE_ID = uuid.UUID('8d645bb8-4657-455b-8b61-8613bc8a0acf')
        ATTRS = ('file',)

        def __init__(self, file):
            super(File, self).__init__()
            self.file = file

        def save_instance_state(self, depositor):
            return self.file

        def load_instance_state(self, saved_state, depositor):
            self.__init__(saved_state)

    file = File(pathlib.Path('some_path'))
    file_id = historian.save(file)
    del file
    loaded = historian.load(file_id)
    assert loaded.file == pathlib.Path('some_path')


def test_tuple_helper(historian: mincepy.Historian):
    historian.register_type(mincepy.common_helpers.TupleHelper())

    container = mincepy.builtins.List()
    container.append((Car('ferrari'),))

    container_id = historian.save(container)
    del container
    loaded = historian.load(container_id)
    assert loaded[0][0] == Car('ferrari')
