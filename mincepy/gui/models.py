from collections import namedtuple
import typing

import PySide2
from PySide2 import QtCore, QtGui
from PySide2.QtCore import QModelIndex
from PySide2.QtCore import QObject, Signal, Qt

import mincepy

__all__ = ('DbModel',)

OBJ_TYPE = '[type]'
CTIME = '[ctime]'
VERSION = '[version]'
VALUE = '[value]'
UNSET = ''

TOOLTIPS = {OBJ_TYPE: 'Object type', CTIME: 'Creation time', VERSION: 'Version', VALUE: 'String value'}

SnapshotRecord = namedtuple("SnapshotRecord", 'snapshot record')


class DbModel(QObject):
    # Signals
    historian_changed = Signal(mincepy.Historian)

    def __init__(self):
        super().__init__()
        self._historian = None

    @property
    def historian(self) -> mincepy.Historian:
        return self._historian

    @historian.setter
    def historian(self, historian):
        self._historian = historian
        self.historian_changed.emit(self._historian)


class DataRecordQueryModel(QtCore.QAbstractTableModel):
    # Signals
    type_restriction_changed = Signal(object)

    def __init__(self, db_model: DbModel, parent=None):
        super().__init__(parent)
        self._db_model = db_model
        self._query = {}
        self._results = None
        self._type_restriction = None

        # Exclude obj_id which is a column header
        self._column_names = mincepy.DataRecord._fields[1:]
        # If the historian changes then we get invalidated
        self._db_model.historian_changed.connect(lambda hist: self._invalidate_results())

    @property
    def db_model(self):
        return self._db_model

    @property
    def column_names(self):
        return self._column_names

    def get_query(self):
        return self._query

    def set_query(self, query: dict):
        """Set the query that will be passed to historian.find()"""
        self._query = query
        self._invalidate_results()

    def set_type_restriction(self, type_id):
        update = self._type_restriction != type_id
        self._type_restriction = type_id
        if update:
            self._invalidate_results()
            self.type_restriction_changed.emit(self._type_restriction)

    def get_type_restriction(self):
        return self._type_restriction

    def get_records(self):
        self._ensure_results_current()
        return self._results

    def rowCount(self, parent: QtCore.QModelIndex = QModelIndex()) -> int:
        self._ensure_results_current()

        if self._results is None:
            self._update_results()

        return len(self._results)

    def columnCount(self, parent: QtCore.QModelIndex = QModelIndex()) -> int:
        return len(self.column_names)

    def headerData(self, section: int, orientation: PySide2.QtCore.Qt.Orientation, role: int = ...) -> typing.Any:
        if role != Qt.DisplayRole:
            return None

        if orientation == QtCore.Qt.Orientation.Horizontal:
            return self.column_names[section]

        self._ensure_results_current()

        return str(self._results[section].obj_id)

    def data(self, index: PySide2.QtCore.QModelIndex, role: int = ...) -> typing.Any:
        self._ensure_results_current()
        if role == Qt.DisplayRole:
            value = getattr(self._results[index.row()], self.column_names[index.column()])
            return str(value)

        return None

    def _ensure_results_current(self):
        if self._results is None:
            self._update_results()

    def _update_results(self):
        if self._query is None or self._db_model.historian is None:
            self._results = []
        else:
            query = self._query.copy()
            if self._type_restriction is not None:
                query['obj_type'] = self._type_restriction
            self._results = list(self._db_model.historian.find(**query, as_objects=False))

    def _invalidate_results(self):
        self.beginResetModel()
        self._results = None
        self._ensure_results_current()
        self.endResetModel()


class EntriesTable(QtCore.QAbstractTableModel):
    DEFAULT_COLUMNS = [OBJ_TYPE, CTIME, VERSION, VALUE]

    def __init__(self, query_model: DataRecordQueryModel, parent=None):
        super().__init__(parent)
        self._query_model = query_model
        self._query_model.modelReset.connect(self._invalidate)

        self._snapshots = None
        self._columns = self.DEFAULT_COLUMNS
        self._show_objects = True

    @property
    def query_model(self):
        return self._query_model

    def get_show_as_objects(self):
        return self._show_objects

    def set_show_as_objects(self, as_objects):
        if self._show_objects != as_objects:
            self._show_objects = as_objects
            self._invalidate()

    def rowCount(self, _parent: PySide2.QtCore.QModelIndex = ...) -> int:
        self._ensure_results_current()
        return len(self._snapshots)

    def columnCount(self, _parent: PySide2.QtCore.QModelIndex = ...) -> int:
        self._ensure_results_current()
        return len(self._columns)

    def headerData(self, section: int, orientation: PySide2.QtCore.Qt.Orientation, role: int = ...) -> typing.Any:
        if role != Qt.DisplayRole:
            return None

        self._ensure_results_current()

        if orientation == QtCore.Qt.Orientation.Horizontal:
            if section > len(self._columns):
                return None
            return self._columns[section]

        if section > len(self._snapshots):
            return None

        return str(self._snapshots[section].record.obj_id)

    def data(self, index: PySide2.QtCore.QModelIndex, role: int = ...) -> typing.Any:
        self._ensure_results_current()
        column_name = self._columns[index.column()]
        if role == Qt.DisplayRole:
            value = self._get_value_string(self._snapshots[index.row()], self._columns[index.column()])
            return value
        if role == Qt.FontRole:
            if column_name.startswith('['):
                font = QtGui.QFont()
                font.setItalic(True)
                return font
        if role == Qt.ToolTipRole:
            if column_name == CTIME:
                return "Created on"

        return None

    def _invalidate(self):
        self.beginResetModel()
        self._snapshots = None
        self._columns = self.DEFAULT_COLUMNS
        self._ensure_results_current()
        self.endResetModel()

    def _ensure_results_current(self):
        if self._snapshots is None:
            self._update_snapshots()
            self._update_columns()

    def _update_snapshots(self):
        self._snapshots = []
        historian = self._query_model.db_model.historian
        if historian is None:
            return

        for record in self._query_model.get_records():
            self._snapshots.append(SnapshotRecord(self._get_snapshot_state(record), record))

        self._snapshots.sort(key=lambda entry: entry.record.type_id)

    def _get_snapshot_state(self, record):
        if self._show_objects:
            historian = self._query_model.db_model.historian
            try:
                return historian.load_snapshot(record.get_reference())
            except TypeError:
                pass  # Fall back to displaying the state

        return record.state

    def _update_columns(self):
        historian = self._query_model.db_model.historian
        if historian is None:
            return

        columns = set()
        for entry in self._snapshots:
            columns.update(self._get_columns(entry.snapshot))

        columns = list(columns)
        columns.sort()
        self._columns = self.DEFAULT_COLUMNS + columns

    def _get_columns(self, obj):
        if isinstance(obj, dict):
            return obj.keys()

        try:
            return vars(obj).keys()
        except TypeError:
            return ()

    def _get_value_string(self, snapshot_record: SnapshotRecord, attr) -> str:
        if attr == OBJ_TYPE:
            historian = self._query_model.db_model.historian
            try:
                return str(historian.get_obj_type(snapshot_record.record.type_id))
            except TypeError:
                return str(snapshot_record.record.type_id)

        if attr == CTIME:
            return str(snapshot_record.record.creation_time)
        if attr == VERSION:
            return str(snapshot_record.record.version)

        state = snapshot_record.snapshot

        if attr == VALUE:
            return str(state)

        if isinstance(state, dict):
            try:
                return str(state[attr])
            except KeyError:
                return UNSET

        try:
            return str(getattr(state, attr))
        except (AttributeError, TypeError):
            return UNSET
