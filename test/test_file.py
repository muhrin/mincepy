import io
import os
import shutil

import pytest

import mincepy


def test_file(tmp_path, historian: mincepy.Historian):
    INITIAL_DATA = os.urandom(1024)
    binary_path = tmp_path / 'binary_test'
    with open(str(binary_path), 'wb') as file:
        file.write(INITIAL_DATA)

    mince_file = mincepy.builtins.DiskFile(binary_path)
    file_id = historian.save(mince_file)
    del mince_file

    loaded = historian.load(file_id)
    with loaded.open() as file:
        buffer = io.BytesIO()
        shutil.copyfileobj(file, buffer)
        assert buffer.getvalue() == INITIAL_DATA


def test_file_changing(tmp_path, historian: mincepy.Historian):
    encoding = 'utf-8'
    INITIAL_DATA = "Initial string".encode(encoding)
    binary_path = tmp_path / 'binary_test'
    with open(str(binary_path), 'wb') as file:
        file.write(INITIAL_DATA)

    mince_file = mincepy.builtins.DiskFile(binary_path, encoding=encoding)
    historian.save(mince_file)

    # Now let's append to the file
    NEW_DATA = "Second string".encode(encoding)
    with open(str(binary_path), 'ab') as file:
        file.write(NEW_DATA)

    historian.save(mince_file)
    history = historian.history(mince_file)
    assert len(history) == 2

    with history[0].obj.open() as file:
        buffer = io.BytesIO()
        shutil.copyfileobj(file, buffer)
        assert INITIAL_DATA == buffer.getvalue()

    with history[1].obj.open() as file:
        buffer = io.BytesIO()
        shutil.copyfileobj(file, buffer)
        assert INITIAL_DATA + NEW_DATA == buffer.getvalue()


def test_file_no_found(tmp_path, historian: mincepy.Historian):
    file_path = tmp_path / 'inexistent'
    mince_file = mincepy.builtins.DiskFile(file_path)

    with pytest.raises(FileNotFoundError):
        with mince_file.open():
            pass

    file_id = historian.save(mince_file)
    del mince_file

    loaded = historian.load(file_id)

    with pytest.raises(FileNotFoundError):
        with loaded.open():
            pass


def test_nested_files_in_list(historian: mincepy.Historian):
    inexistent = mincepy.builtins.DiskFile('none')
    my_list = mincepy.builtins.List()
    my_list.append(inexistent)

    list_id = historian.save(my_list)
    del my_list

    loaded = historian.load(list_id)
    assert len(loaded) == 1
    assert loaded[0].filename == 'none'


def test_nested_files_in_dict(historian: mincepy.Historian):
    inexistent = mincepy.builtins.DiskFile('none')
    my_dict = mincepy.builtins.Dict()
    my_dict['file'] = inexistent

    list_id = historian.save(my_dict)
    del my_dict

    loaded = historian.load(list_id)
    assert len(loaded) == 1
    assert loaded['file'].filename == 'none'
