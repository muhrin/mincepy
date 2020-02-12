from PySide2 import QtCore, QtWidgets
from PySide2.QtCore import Signal

import mincepy
from . import models

__all__ = 'TypeDropDown', 'ConnectionWidget', 'MincepyWidget'


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

        layout = QtWidgets.QHBoxLayout()
        layout.addWidget(self._create_type_drop_down())
        layout.addWidget(self._create_display_as_class_checkbox())
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

        # Create an lay out the panel
        panel = QtWidgets.QWidget()
        layout = QtWidgets.QHBoxLayout()
        layout.addWidget(QtWidgets.QLabel("Restrict type:"))
        layout.addWidget(type_drop_down)
        panel.setLayout(layout)

        return panel


class ConnectionWidget(QtWidgets.QWidget):
    # Signals
    connection_requested = Signal(str)

    def __init__(self, default_connect_uri='', parent=None):
        super().__init__(parent)
        self._connection_string = QtWidgets.QLineEdit(self)
        self._connection_string.setText(default_connect_uri)
        self._connect_button = QtWidgets.QPushButton('Connect', self)

        layout = QtWidgets.QHBoxLayout()
        layout.addWidget(self._connection_string)
        layout.addWidget(self._connect_button)
        self.setLayout(layout)

        self._connect_button.clicked.connect(self._connect_pushed)

    def _connect_pushed(self):
        string = self._connection_string.text()
        self.connection_requested.emit(string)


class MincepyWidget(QtWidgets.QWidget):

    def __init__(self, default_connect_uri='', create_historian_callback=None):
        super().__init__()

        def default_create_historian(uri) -> mincepy.Historian:
            historian = mincepy.create_historian(uri)
            mincepy.set_historian(historian)
            return historian

        self._create_historian_callback = create_historian_callback or default_create_historian

        # The model
        self._db_model = models.DbModel()
        self._data_records = models.DataRecordQueryModel(self._db_model, self)

        # Set up the connect panel of the GUI
        connect_panel = ConnectionWidget(default_connect_uri, self)
        connect_panel.connection_requested.connect(self._connect)

        entries_table = models.EntriesTable(self._data_records, self)
        entries_view = QtWidgets.QTableView()
        entries_view.setModel(entries_table)

        control_panel = FilterControlPanel(entries_table, self)

        self.layout = QtWidgets.QVBoxLayout()
        self.layout.addWidget(connect_panel)
        self.layout.addWidget(control_panel)
        self.layout.addWidget(entries_view)
        self.setLayout(self.layout)

    @property
    def db_model(self):
        return self._db_model

    def _connect(self, uri):
        try:
            historian = self._create_historian_callback(uri)
            self._db_model.historian = historian
        except ValueError as exc:
            err_msg = "Error creating historian with uri '{}':\n{}".format(uri, exc)
            QtWidgets.QErrorMessage(self).showMessage(err_msg)
