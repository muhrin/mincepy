# -*- coding: utf-8 -*-
import bson

from mincepy import archives

# pylint: disable=invalid-name

# Problem with subscripting archive, bug report here:
# https://github.com/PyCQA/pylint/issues/2822
_Archive = archives.Archive[bson.ObjectId]  # pylint: disable=unsubscriptable-object
SnapshotId = _Archive.SnapshotId
