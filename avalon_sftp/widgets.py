
from avalon.vendor.Qt import QtWidgets, QtCore
from .model import JobSourceModel, JobStagingProxyModel, JobUploadProxyModel
from .delegates import ProgressDelegate


class JobWidget(QtWidgets.QWidget):

    staging = QtCore.Signal()
    staged = QtCore.Signal()
    canceling = QtCore.Signal()
    canceled = QtCore.Signal()

    def __init__(self, parent=None):
        super(JobWidget, self).__init__(parent=parent)

        # Model and Proxies
        #
        model = JobSourceModel()
        staging_proxy = JobStagingProxyModel()
        upload_proxy = JobUploadProxyModel(self)

        def proxy_setup(proxy):
            proxy.setSourceModel(model)
            proxy.setDynamicSortFilter(True)
            proxy.setFilterCaseSensitivity(QtCore.Qt.CaseInsensitive)

        proxy_setup(staging_proxy)
        proxy_setup(upload_proxy)

        self.model = model
        self.staging_proxy = staging_proxy
        self.upload_proxy = upload_proxy

        # Views
        #
        staging_view = QtWidgets.QTreeView()
        upload_view = QtWidgets.QTreeView()

        def view_setup(view, proxy):
            view.setIndentation(10)
            view.setAllColumnsShowFocus(True)
            view.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)
            view.setSortingEnabled(True)
            view.sortByColumn(1, QtCore.Qt.AscendingOrder)
            view.setAlternatingRowColors(True)
            view.setStyleSheet("""
                QTreeView::item{
                    padding: 5px 1px;
                    border: 0px;
                }
            """)
            _ = QtWidgets.QAbstractItemView.ExtendedSelection
            view.setSelectionMode(_)
            view.setModel(proxy)

        view_setup(staging_view, self.staging_proxy)
        view_setup(upload_view, self.upload_proxy)

        progress_delegate = ProgressDelegate(upload_view)
        column = self.model.UPLOAD_COLUMNS.index("progress")
        upload_view.setItemDelegateForColumn(column, progress_delegate)

        staging_view.customContextMenuRequested.connect(self.on_staging_menu)
        upload_view.customContextMenuRequested.connect(self.on_upload_menu)

        self.staging_view = staging_view
        self.upload_view = upload_view

        # Contorls and Layout
        #
        upload_body = QtWidgets.QWidget()
        upload_layout = QtWidgets.QVBoxLayout(upload_body)
        upload_layout.addWidget(self.upload_view)
        # --
        staging_body = QtWidgets.QWidget()
        staging_layout = QtWidgets.QVBoxLayout(staging_body)

        line_input = QtWidgets.QLineEdit()
        send_btn = QtWidgets.QPushButton("Send")

        input_layout = QtWidgets.QHBoxLayout()
        input_layout.addWidget(line_input)
        input_layout.addWidget(send_btn)

        skip_exists = QtWidgets.QCheckBox("Skip if exists")

        staging_layout.addWidget(self.staging_view)
        staging_layout.addLayout(input_layout)
        staging_layout.addWidget(skip_exists)

        self.line_input = line_input
        self.send_btn = send_btn

        layout = QtWidgets.QVBoxLayout(self)

        splitter = QtWidgets.QSplitter()
        splitter.setOrientation(QtCore.Qt.Vertical)
        splitter.setStyleSheet("QSplitter{ border: 0px; }")
        splitter.addWidget(upload_body)
        splitter.addWidget(staging_body)
        splitter.setSizes([200, 120])

        layout.addWidget(splitter)

        # Connect
        #
        send_btn.clicked.connect(self.stage)
        self.model.staging.connect(self.on_staging)
        self.model.staged.connect(self.on_staged)
        self.model.canceling.connect(self.on_canceling)
        self.model.canceled.connect(self.on_canceled)

    def on_staging_menu(self, point):
        point_index = self.staging_view.indexAt(point)
        if not point_index.isValid():
            return

        if self.model.is_busy():
            return

        menu = QtWidgets.QMenu(self)

        upload_sel_action = QtWidgets.QAction("Upload Selected", menu)
        upload_sel_action.triggered.connect(self.act_upload_selected)

        upload_all_action = QtWidgets.QAction("Upload All", menu)
        upload_all_action.triggered.connect(self.act_upload_all)

        clear_action = QtWidgets.QAction("Clear..", menu)
        clear_action.triggered.connect(self.act_clear)

        menu.addAction(upload_sel_action)
        menu.addAction(upload_all_action)
        menu.addAction(clear_action)

        # Show the context action menu
        global_point = self.staging_view.mapToGlobal(point)
        action = menu.exec_(global_point)
        if not action:
            return

    def on_upload_menu(self, point):
        point_index = self.upload_view.indexAt(point)
        if not point_index.isValid():
            return

        print("UPLOAD MENU")

    def on_quit(self):
        self.model.stop()

    def stage(self):
        job_file = self.line_input.text()
        job_file = r"C:\Users\david\Dropbox\github\AVALON\avalon-sftp\lighting_v0036_patrick.sftp.job"
        self.model.stage(job_file)

    def on_staging(self):
        self.staging.emit()
        self.send_btn.setEnabled(False)
        self.line_input.setEnabled(False)

    def on_staged(self):
        self.staged.emit()
        self.send_btn.setEnabled(True)
        self.line_input.setEnabled(True)
        self.line_input.setText("")

    def on_canceling(self):
        self.canceling.emit()

    def on_canceled(self):
        self.canceled.emit()
        self.send_btn.setEnabled(True)
        self.line_input.setEnabled(True)

    def act_upload_selected(self):
        """Upload selected jobs only
        Stage Menu Action
        """
        selection_model = self.staging_view.selectionModel()
        selection = selection_model.selection()
        source_selection = self.staging_proxy.mapSelectionToSource(selection)

        model = self.model
        status_column = model.UPLOAD_COLUMNS.index("status")

        for index in source_selection.indexes():
            if index.column() == status_column:
                print("X")
                model.setData(index, 1, role=model.UploadDisplayRole)

    def act_upload_all(self):
        """Upload all jobs
        Stage Menu Action
        """
        proxy = self.staging_proxy
        model = self.model
        status_column = model.UPLOAD_COLUMNS.index("status")

        indexes = list()
        for row in range(proxy.rowCount()):
            index = proxy.index(row, status_column)
            index = proxy.mapToSource(index)
            indexes.append(index)

        for index in indexes:
            model.setData(index, 1, role=model.UploadDisplayRole)

    def act_clear(self):
        """Clear all staging jobs
        Stage Menu Action
        """
        self.model.clear()
