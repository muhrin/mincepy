"""Module for tree model related methods and classes"""
from abc import ABCMeta, abstractmethod
import collections
import operator
import typing

import PySide2
from PySide2 import QtCore
from PySide2.QtCore import QModelIndex, Qt, Signal, Slot

import mincepy
from . import common
from .models import pretty_type_string


class BaseTreeItem(metaclass=ABCMeta):

    def __init__(self, column_data: collections.Sequence, parent=None):
        self._data = column_data
        self._parent = parent

    def data(self, column: int):
        """Get the data at the given column"""
        if column < 0 or column >= self.column_count():
            return None

        return self._data[column]

    def row(self) -> int:
        """Get the row of this item within its parent"""
        if self._parent is None:
            return 0

        return self._parent.child_row(self)

    def column_count(self) -> int:
        """Get the number of columns in this item"""
        return len(self._data)

    def parent(self):
        """Get the parent of this tree item"""
        return self._parent

    @abstractmethod
    def child(self, row: int):
        """Get the child at the given row"""

    @abstractmethod
    def child_count(self) -> int:
        """Get the number of children this item has"""

    @abstractmethod
    def child_row(self, tree_item) -> int:
        """Get the row number given a child item"""


class DataTreeItem(BaseTreeItem):
    """Tree item that directly sores the required data internally"""

    def __init__(self, column_data: collections.Sequence, parent=None):
        super().__init__(column_data, parent)
        self._children = []

    def append_child(self, child):
        self._children.append(child)

    def child(self, row: int):
        if row < 0 or row >= self.child_count():
            return None

        return self._children[row]

    def child_count(self) -> int:
        return len(self._children)

    def child_row(self, tree_item) -> int:
        return self._children.index(tree_item)


class LazyMappingItem(BaseTreeItem):

    def __init__(self, column_data: collections.Sequence, raw_data, child_builder, num_children, parent=None):
        super(LazyMappingItem, self).__init__(column_data, parent)
        self._raw_data = raw_data
        self._child_builder = child_builder
        self._num_children = num_children
        self._children = {}

    def child_count(self) -> int:
        return self._num_children

    def child(self, row: int):
        if row < 0 or row >= self.child_count():
            return None

        if row not in self._children:
            self._children[row] = self._child_builder(self._raw_data, row, self)
            if len(self._children) == self._num_children:
                # Can discard the raw data and builder now
                self._raw_data = None
                self._child_builder = None

        return self._children[row]

    def child_row(self, tree_item) -> int:
        for row, child in self._children.items():
            if tree_item is child:
                return row
        raise ValueError("'{}' is not a child".format(tree_item))


class RecordTree(QtCore.QAbstractItemModel):
    COLUMN_HEDARES = 'Property', 'Type', 'Value'

    object_activated = Signal(object)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._root_item = DataTreeItem(self.COLUMN_HEDARES)
        self._data_record = None  # type: typing.Optional[mincepy.DataRecord]

    @Slot(QModelIndex)
    def activate_entry(self, index: QModelIndex):
        obj = self.data(index, role=common.DataRole)
        if obj:
            self.object_activated.emit(obj)

    def index(self, row: int, column: int, parent: PySide2.QtCore.QModelIndex = ...) -> \
            PySide2.QtCore.QModelIndex:
        if not self.hasIndex(row, column, parent):
            return QModelIndex()

        if not parent.isValid():
            parent_item = self._root_item
        else:
            parent_item = parent.internalPointer()

        child_item = parent_item.child(row)
        if child_item is not None:
            return self.createIndex(row, column, child_item)

        return QModelIndex()

    def parent(self, child: QModelIndex) -> QModelIndex:
        if not child.isValid():
            return QModelIndex()

        child_item = child.internalPointer()  # type: DataTreeItem
        parent_item = child_item.parent()  # type: DataTreeItem

        if parent_item is self._root_item:
            return QModelIndex()

        return self.createIndex(parent_item.row(), 0, parent_item)

    def rowCount(self, parent: PySide2.QtCore.QModelIndex = ...) -> int:
        if parent.column() > 0:
            return 0

        if not parent.isValid():
            parent_item = self._root_item
        else:
            parent_item = parent.internalPointer()

        return parent_item.child_count()

    def columnCount(self, _parent: PySide2.QtCore.QModelIndex = ...) -> int:
        return self._root_item.column_count()

    def data(self, index: PySide2.QtCore.QModelIndex, role: int = ...) -> typing.Any:
        if not index.isValid():
            return None

        if role == Qt.DisplayRole:
            item = index.internalPointer()
            value = item.data(index.column())
            return str(value)
        if role == common.DataRole:
            return index.internalPointer().data(index.column())

    def flags(self, index: PySide2.QtCore.QModelIndex) -> PySide2.QtCore.Qt.ItemFlags:
        if not index.isValid():
            return Qt.NoItemFlags

        return super(RecordTree, self).flags(index)

    def headerData(self, section: int, orientation: PySide2.QtCore.Qt.Orientation, role: int = ...) -> \
            typing.Any:
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            return self._root_item.data(section)

        return None

    def set_record(self, record: mincepy.DataRecord, obj=None):
        """Set the data to visualise, the object instance can optionally be provided"""
        self.beginResetModel()
        self._data_record = record
        # Now build the three
        if self._data_record is None:
            self._root_item = DataTreeItem(self.COLUMN_HEDARES)
        else:
            tree_dict = record._asdict()
            if obj is not None:
                tree_dict['obj'] = obj
            self._root_item = LazyMappingItem(self.COLUMN_HEDARES, tree_dict, self._item_builder, len(tree_dict))
        self.endResetModel()

    def _item_builder(self, build_from, row, parent=None):
        if isinstance(build_from, collections.Sequence):
            key = str(row)
            try:
                child = build_from[row]
            except Exception as exc:
                child = "Error getting child: {}".format(exc)
        elif isinstance(build_from, collections.Mapping):
            entry = sorted(build_from.items(), key=operator.itemgetter(0))[row]
            key, child = str(entry[0]), entry[1]
        else:
            raise TypeError("Type '{}' does not support children")

        column_data = (key, pretty_type_string(type(child)), child)

        # Is the child nested?
        nested_child_data = None
        if not isinstance(child, str) and \
                (isinstance(child, (collections.Sequence, collections.Mapping))):
            # We have a sequence or mapping
            nested_child_data = child
        else:
            try:
                nested_child_data = vars(child)
            except TypeError:
                pass

        if nested_child_data is not None:
            # We have a nested child so get a lazy item
            return LazyMappingItem(column_data, nested_child_data, self._item_builder, len(nested_child_data), parent)

        # Fall back to a plain unnested item
        return DataTreeItem(column_data, parent)
