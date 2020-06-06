import bson

import mincepy

# pylint: disable=invalid-name

# Problem with subscripting archive, bug report here:
# https://github.com/PyCQA/pylint/issues/2822
_Archive = mincepy.Archive[bson.ObjectId]  # pylint: disable=unsubscriptable-object
SnapshotId = _Archive.SnapshotId
