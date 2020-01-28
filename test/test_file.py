import io
import os
import shutil

import pytest

import mincepy


def test_file_basics(historian: mincepy.Historian):
    encoding = 'utf-8'
    INITIAL_DATA = 'hello there'
    file = historian.create_file(encoding)
    with file.open('w') as stream:
        stream.write(INITIAL_DATA)

    file_id = historian.save(file)
    del file

    loaded = historian.load(file_id)
    with loaded.open('r') as file:
        buffer = io.StringIO()
        shutil.copyfileobj(file, buffer)
        assert buffer.getvalue() == INITIAL_DATA


def test_file_changing(tmp_path, historian: mincepy.Historian):
    encoding = 'utf-8'
    INITIAL_DATA = "Initial string"
    mince_file = historian.create_file(encoding=encoding)

    with mince_file.open('w') as file:
        file.write(INITIAL_DATA)

    historian.save(mince_file)

    # Now let's append to the file
    NEW_DATA = "Second string"
    with mince_file.open('a') as file:
        file.write(NEW_DATA)

    historian.save(mince_file)
    history = historian.history(mince_file)
    assert len(history) == 2

    with history[0].obj.open() as file:
        buffer = io.StringIO()
        shutil.copyfileobj(file, buffer)
        assert INITIAL_DATA == buffer.getvalue()

    with history[1].obj.open() as file:
        buffer = io.StringIO()
        shutil.copyfileobj(file, buffer)
        assert INITIAL_DATA + NEW_DATA == buffer.getvalue()


def test_nested_files_in_list(historian: mincepy.Historian):
    file = historian.create_file()
    my_list = mincepy.builtins.List()
    my_list.append(file)

    list_id = historian.save(my_list)
    del my_list

    loaded = historian.load(list_id)
    assert len(loaded) == 1
    assert loaded[0].filename == None


def test_nested_files_in_dict(historian: mincepy.Historian):
    file = historian.create_file()
    my_dict = mincepy.builtins.Dict()
    my_dict['file'] = file

    list_id = historian.save(my_dict)
    del my_dict

    loaded = historian.load(list_id)
    assert len(loaded) == 1
    assert loaded['file'].filename == None


def test_nested_files_in_list_mutating(tmp_path, historian: mincepy.Historian):
    encoding = 'utf-8'
    INITIAL_DATA = "First string".encode(encoding)
    my_file = historian.create_file()
    with my_file.open('wb') as file:
        file.write(INITIAL_DATA)

    my_list = mincepy.builtins.List()
    my_list.append(my_file)

    list_id = historian.save(my_list)

    # Now let's append to the file
    NEW_DATA = "Second string".encode(encoding)
    with my_file.open('ab') as file:
        file.write(NEW_DATA)

    # Save the list again
    historian.save(my_list)
    del my_list

    loaded = historian.load(list_id)
    with loaded[0].open('rb') as contents:
        buffer = io.BytesIO()
        shutil.copyfileobj(contents, buffer)
        assert buffer.getvalue() == INITIAL_DATA + NEW_DATA
