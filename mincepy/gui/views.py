from concurrent.futures import ThreadPoolExecutor, Future
from functools import partial
import json
import uuid

from PySide2 import QtCore, QtWidgets, QtGui
from PySide2.QtCore import Signal

import mincepy
from . import common
from . import models
from . import tree_models

__all__ = 'TypeDropDown', 'ConnectionWidget', 'MincepyWidget', 'MainWindow'


class UUIDEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, uuid.UUID):
            # if the obj is uuid, we simply return the value of uuid
            return repr(obj)
        return json.JSONEncoder.default(self, obj)


class UUIDDecoder(json.JSONDecoder):

    def decode(self, s):
        decoded = super(UUIDDecoder, self).decode(s)

        def to_uuid(entry, path):
            if isinstance(entry, str) and entry.startswith('UUID('):
                try:
                    return uuid.UUID(entry[6:-2])
                except ValueError:
                    pass
            return entry

        return mincepy.utils.transform(to_uuid, decoded)


class TypeDropDown(QtWidgets.QComboBox):
    """Drop down combo box that lists the types available in archive"""
    ALL = None

    # Signals
    selected_type_changed = Signal(object)

    def __init__(self, query_model: models.DataRecordQueryModel, parent=None):
        super().__init__(parent)
        self._query_model = query_model
        query_model.db_model.historian_changed.connect(self._update)

        self.setEditable(True)
        self._types = [None]
        self.addItem(self.ALL)

        def selection_changed(index):
            restrict_type = self._types[index]
            self.selected_type_changed.emit(restrict_type)

        self.currentIndexChanged.connect(selection_changed)

    @property
    def _historian(self):
        return self._query_model.db_model.historian

    def _update(self):
        self.clear()

        results = self._historian.get_archive().find()
        self._types = [None]
        self._types.extend(list(set(result.type_id for result in results)))

        type_names = self._get_type_names(self._types)

        self.addItems(type_names)
        completer = QtWidgets.QCompleter(type_names)
        self.setCompleter(completer)

    def _get_type_names(self, types):
        type_names = []
        for type_id in types:
            try:
                helper = self._historian.get_helper(type_id)
            except TypeError:
                type_names.append(str(type_id))
            else:
                type_names.append(mincepy.analysis.get_type_name(helper.TYPE))

        return type_names


class FilterControlPanel(QtWidgets.QWidget):
    # Signals
    display_as_class_changed = Signal(object)

    def __init__(self, entries_table: models.EntriesTable, parent=None):
        super().__init__(parent)
        self._entries_table = entries_table

        layout = QtWidgets.QVBoxLayout()
        layout.addWidget(self._create_controls())
        layout.addWidget(self._create_query())
        self.setLayout(layout)

    def _create_display_as_class_checkbox(self):
        # Show snapshot class checkbox
        display_class_checkbox = QtWidgets.QCheckBox('Display as class', self)
        display_class_checkbox.setCheckState(QtCore.Qt.Checked)
        display_class_checkbox.stateChanged.connect(
            lambda state: self._entries_table.set_show_as_objects(state == QtCore.Qt.Checked))
        self._entries_table.set_show_as_objects(display_class_checkbox.checkState() == QtCore.Qt.Checked)

        return display_class_checkbox

    def _create_type_drop_down(self):
        type_drop_down = TypeDropDown(self._entries_table.query_model, self)
        type_drop_down.selected_type_changed.connect(self._entries_table.query_model.set_type_restriction)
        type_drop_down.setSizePolicy(QtWidgets.QSizePolicy.Policy.Expanding,
                                     type_drop_down.sizePolicy().verticalPolicy())
        return type_drop_down

    def _create_refresh_button(self):
        refresh = QtWidgets.QPushButton("Refresh")
        refresh.clicked.connect(self._entries_table.refresh)
        return refresh

    def _create_controls(self):
        panel = QtWidgets.QWidget()

        layout = QtWidgets.QHBoxLayout()
        layout.setMargin(0)
        layout.setSpacing(6)
        layout.addWidget(QtWidgets.QLabel("Type:"))
        layout.addWidget(self._create_type_drop_down())
        layout.addWidget(self._create_display_as_class_checkbox())
        layout.addWidget(self._create_refresh_button())
        panel.setLayout(layout)

        return panel

    def _create_query(self):
        panel = QtWidgets.QWidget()

        layout = QtWidgets.QHBoxLayout()
        layout.setMargin(0)
        layout.setSpacing(6)
        layout.addWidget(QtWidgets.QLabel("Query:"))
        layout.addWidget(self._create_query_line())
        panel.setLayout(layout)

        return panel

    def _create_query_line(self):
        query_line = QtWidgets.QLineEdit()

        def set_query_edited():
            palette = query_line.palette()
            palette.setColor(palette.Base, QtGui.QColor(192, 212, 192))
            query_line.setPalette(palette)

        def reset_query_edited():
            palette = query_line.palette()
            palette.setColor(palette.Base, QtGui.QColor(255, 255, 255))
            query_line.setPalette(palette)

        def query_changed(new_query: dict):
            new_text = json.dumps(new_query, cls=UUIDEncoder)
            if new_text != query_line.text():
                query_line.setText(new_text)
            reset_query_edited()

        def text_edited(_text):
            try:
                current_query = json.dumps(self._entries_table.query_model.get_query(), cls=UUIDEncoder)
            except json.decoder.JSONDecodeError:
                pass
            else:
                if _text != current_query:
                    set_query_edited()
                else:
                    reset_query_edited()

        def query_submitted():
            new_text = query_line.text()
            try:
                query = json.loads(new_text, cls=UUIDDecoder)
            except json.decoder.JSONDecodeError as exc:
                QtWidgets.QErrorMessage(self).showMessage(str(exc))
            else:
                self._entries_table.query_model.set_query(query)

        self._entries_table.query_model.query_changed.connect(query_changed)
        query_line.returnPressed.connect(query_submitted)
        query_line.textEdited.connect(text_edited)

        return query_line


class ConnectionWidget(QtWidgets.QWidget):
    # Signals
    connection_requested = Signal(str)
    historian_created = Signal(mincepy.Historian)

    def __init__(self,
                 default_connect_uri='',
                 create_historian_callback=common.default_create_historian,
                 executor=common.default_executor,
                 parent=None):
        super().__init__(parent)
        self._executor = executor
        self._create_historian = create_historian_callback

        self._connection_string = QtWidgets.QLineEdit(self)
        self._connection_string.setText(default_connect_uri)
        self._connect_button = QtWidgets.QPushButton('Connect', self)

        layout = QtWidgets.QHBoxLayout()
        layout.addWidget(self._connection_string)
        layout.addWidget(self._connect_button)
        self.setLayout(layout)

        self._connection_string.returnPressed.connect(self._connect_pushed)
        self._connect_button.clicked.connect(self._connect_pushed)

    def _connect_pushed(self):
        uri = self._connection_string.text()
        self._executor(partial(
            self._connect,
            uri,
        ), "Connecting", blocking=True)

    def _connect(self, uri):
        try:
            historian = self._create_historian(uri)
        except Exception as exc:
            err_msg = "Error creating historian with uri '{}':\n{}".format(uri, exc)
            raise RuntimeError(err_msg)
        else:
            self.historian_created.emit(historian)

        return "Connected to {}".format(uri)


class MincepyWidget(QtWidgets.QWidget):
    object_activated = Signal(object)

    def __init__(self, app_common: common.AppCommon):
        super().__init__()

        # Models
        # The model
        self._db_model = models.DbModel()
        self._data_records = models.DataRecordQueryModel(self._db_model, executor=app_common.executor, parent=self)

        self._entries_table = models.EntriesTable(self._data_records, parent=self)
        self._entries_table.object_activated.connect(self._activate_object)

        # Create the views
        # Set up the connect panel of the GUI
        connect_panel = ConnectionWidget(default_connect_uri=app_common.default_connect_uri,
                                         create_historian_callback=app_common.create_historian_callback,
                                         executor=app_common.executor,
                                         parent=self)

        connect_panel.historian_created.connect(self._historian_created)

        control_panel = FilterControlPanel(self._entries_table, self)

        self.layout = QtWidgets.QVBoxLayout()
        self.layout.addWidget(connect_panel)
        self.layout.addWidget(control_panel)
        self.layout.addWidget(self._create_display_panel(self._entries_table))
        self.setLayout(self.layout)

    @property
    def db_model(self):
        return self._db_model

    def _activate_object(self, obj):
        self.object_activated.emit(obj)

    def _historian_created(self, historian):
        self._db_model.historian = historian

    def _create_display_panel(self, entries_table: models.EntriesTable):
        panel = QtWidgets.QSplitter(QtCore.Qt.Vertical)

        entries_view = QtWidgets.QTableView(panel)
        entries_view.setSortingEnabled(True)
        entries_view.setModel(entries_table)
        entries_view.doubleClicked.connect(entries_table.activate_entry)

        record_tree = tree_models.RecordTree(parent=panel)
        record_tree.object_activated.connect(self._activate_object)
        record_tree_view = QtWidgets.QTreeView(panel)
        record_tree_view.setModel(record_tree)
        record_tree_view.doubleClicked.connect(record_tree.activate_entry)

        def row_changed(current, _previous):
            entries_table = self._entries_table
            record = entries_table.get_record(current.row())
            snapshot = entries_table.get_snapshot(current.row())
            record_tree.set_record(record, snapshot)

        entries_view.selectionModel().currentRowChanged.connect(row_changed)

        panel.addWidget(entries_view)
        panel.addWidget(record_tree_view)

        return panel


class MainWindow(QtWidgets.QMainWindow):

    def __init__(self, app_common: common.AppCommon):
        super().__init__()
        self._app_common = app_common
        self._executor = ThreadPoolExecutor()
        self._tasks = []

        app_common.executor = self._execute

        self.setCentralWidget(self._create_main_widget())
        self._create_status_bar()

        self._task_done_signal.connect(self._task_done)

    def _create_main_widget(self):
        main_widget = MincepyWidget(self._app_common)
        main_widget.object_activated.connect(self._object_activated)
        return main_widget

    def _create_status_bar(self):
        self.statusBar().showMessage('Ready')

    def _execute(self, func, msg=None, blocking=False) -> Future:
        future = self._executor.submit(func)
        QtWidgets.QApplication.setOverrideCursor(QtCore.Qt.WaitCursor if blocking else QtCore.Qt.BusyCursor)
        self._tasks.append(future)
        future.add_done_callback(self._task_done_signal.emit)
        if msg is not None:
            self.statusBar().showMessage(msg)

        return future

    _task_done_signal = Signal(Future)

    @QtCore.Slot(object)
    def _object_activated(self, obj):
        if self._app_common.type_viewers:
            self._execute(partial(self._app_common.call_viewers, obj), msg="Calling viewer", blocking=True)

    @QtCore.Slot(Future)
    def _task_done(self, future):
        self._tasks.remove(future)
        QtWidgets.QApplication.restoreOverrideCursor()

        if not self._tasks:
            self.statusBar().clearMessage()

        try:
            new_msg = future.result()
            if new_msg is not None:
                self.statusBar().showMessage(new_msg, 1000)
        except Exception as exc:
            QtWidgets.QErrorMessage(self).showMessage(str(exc))
