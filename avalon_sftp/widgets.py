
from avalon.vendor.Qt import QtWidgets
from .model import VersionWorkerModel


class VersionListWidget(QtWidgets.QWidget):

    def __init__(self, parent=None):
        super(VersionListWidget, self).__init__(parent=parent)

        model = VersionWorkerModel()

        view = QtWidgets.QTreeView()

        layout = QtWidgets.QVBoxLayout(self)
        layout.addWidget(view)

        self.model = model
        self.view = view

        self.view.setModel(self.model)

    def refresh(self):
        self.model.refresh()


class WorkerWidget(QtWidgets.QWidget):

    def __init__(self, parent=None):
        super(WorkerWidget, self).__init__(parent=parent)

        start_btn = QtWidgets.QPushButton("START")
        exist_btn = QtWidgets.QPushButton("Check Exists")
        upload_btn = QtWidgets.QPushButton("Upload All")

        layout = QtWidgets.QHBoxLayout(self)
        layout.addWidget(start_btn)
        layout.addWidget(exist_btn)
        layout.addWidget(upload_btn)

        self.start_btn = start_btn
        self.exist_btn = exist_btn
        self.upload_btn = upload_btn
