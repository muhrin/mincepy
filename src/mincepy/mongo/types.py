import bson

from mincepy import archives

_Archive = archives.Archive[bson.ObjectId]
SnapshotId = _Archive.SnapshotId
