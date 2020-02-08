import sys
import concurrent.futures

import pymongo
from PySide2 import QtCore, QtWidgets, QtGui

import mincepy


class TypeBox(QtWidgets.QComboBox):

    def __init__(self, parent=None, historian=None):
        super().__init__(parent)
        self._historian = historian or mincepy.get_historian()

        self.setEditable(True)
        self._types = []

    def populate_types(self):
        results = self._historian.get_archive().find()
        self._types = list(set(result.type_id for result in results))
        types_names = self._get_type_names(self._types)

        self.addItems(types_names)
        completer = QtWidgets.QCompleter(types_names)
        self.setCompleter(completer)

    def _get_type_names(self, types):
        type_names = []
        for type_id in types:
            try:
                helper = self._historian.get_helper(type_id)
            except ValueError:
                type_names.append(str(type))
            else:
                type_names.append(mincepy.analysis.get_type_name(helper.TYPE))

        return type_names

    def get_type_from_index(self, idx):
        return self._types[idx]


class EntryList(QtWidgets.QTableWidget):

    def __init__(self, parent=None, historian=None):
        super().__init__(parent)
        self._historian = historian or mincepy.get_historian()
        self._current_type = None

    def set_entry_type(self, entry_type, progress=None):
        if self._current_type == entry_type:
            return

        self._current_type = entry_type

        archive = self._historian.get_archive()

        total = archive.count(type_id=entry_type)
        results = archive.find(type_id=entry_type)

        def update_progress(idx):
            progress.setValue(idx / total * 100.)

        data = mincepy.analysis.get_table(results, update_progress)

        self.setColumnCount(len(data[0]) - 1)
        self.setRowCount(len(data) - 1)

        self.setHorizontalHeaderLabels(data[0][1:])
        self.setVerticalHeaderLabels([row[0] for row in data[1:]])

        for row, row_data in enumerate(data[1:]):
            for col, item_value in enumerate(row_data[1:]):
                self.setItem(row, col, QtWidgets.QTableWidgetItem(item_value))

        progress.setValue(100)


class MyWidget(QtWidgets.QWidget):

    def __init__(self):
        super().__init__()

        self._executor = concurrent.futures.ThreadPoolExecutor()
        self._num_running = 0

        self.type_box = TypeBox(self)
        self.type_box.currentIndexChanged.connect(self.type_changed)

        self.entry_list = EntryList(self)

        self.progress = QtWidgets.QProgressBar(self)

        self.layout = QtWidgets.QVBoxLayout()
        self.layout.addWidget(self.type_box)
        self.layout.addWidget(self.entry_list)
        self.layout.addWidget(self.progress)
        self.setLayout(self.layout)

        self._submit_task(self.type_box.populate_types)

    def type_changed(self, idx):
        self._submit_task(self.entry_list.set_entry_type, self.type_box.get_type_from_index(idx), self.progress)

    def _submit_task(self, func, *args, **kwargs):
        # self._executor.submit(fn, *args, **kwargs).add_done_callback(self._task_done)

        self._num_running += 1
        if self._num_running == 1:
            QtWidgets.QApplication.setOverrideCursor(QtGui.QCursor(QtCore.Qt.WaitCursor))
        try:
            func(*args, **kwargs)
        finally:
            self._num_running -= 1
            if self._num_running == 0:
                QtWidgets.QApplication.restoreOverrideCursor()

    def _task_done(self, _fut):
        self._num_running -= 1
        if self._num_running == 0:
            QtWidgets.QApplication.restoreOverrideCursor()


def init_historian():
    client = pymongo.MongoClient()
    database = client.test_database
    mongo_archive = mincepy.mongo.MongoArchive(database)
    hist = mincepy.Historian(mongo_archive)
    hist.register_types(mincepy.testing.HISTORIAN_TYPES)
    mincepy.set_historian(hist)


if __name__ == "__main__":
    init_historian()
    app = QtWidgets.QApplication([])

    widget = MyWidget()
    widget.resize(800, 600)
    widget.show()

    sys.exit(app.exec_())
