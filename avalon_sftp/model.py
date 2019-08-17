
from avalon.vendor.Qt import QtCore
from avalon.tools.projectmanager.model import TreeModel, Node

from .lib import JobProducer, JobConsumer
from .worker import job_generator


class JobSourceModel(TreeModel):

    staging = QtCore.Signal()
    staged = QtCore.Signal()
    canceling = QtCore.Signal()
    canceled = QtCore.Signal()

    STAGING_COLUMNS = [
        "project",
        "type",
        "detail",
        "site",
        "count",
        "size",
    ]

    StagingDisplayRole = QtCore.Qt.UserRole + 10
    StagingSortRole = QtCore.Qt.UserRole + 11

    UPLOAD_COLUMNS = [
        "project",
        "type",
        "detail",
        "status",
        "progress",
    ]

    UploadDisplayRole = QtCore.Qt.UserRole + 20
    UploadSortRole = QtCore.Qt.UserRole + 21
    UploadProgressRole = QtCore.Qt.UserRole + 22

    STATUS = [
        "staging",
        "pending",
        "uploading",
        "completed",
        "failed",
    ]

    def __init__(self, parent=None):
        super(JobSourceModel, self).__init__(parent=parent)
        self._producer = None
        self._consumer = None
        self._appended = set()

    def clear(self):
        super(JobSourceModel, self).clear()
        self._appended = set()

    def is_busy(self):
        producer = self._producer
        consumer = self._consumer

        if producer is None or consumer is None:
            return False

        return producer.producing or consumer.consuming

    def stage(self, job_file):
        """
        Args:
            job_file (str): JSON file

        """
        self.staging.emit()

        generator = job_generator(job_file)
        producer = JobProducer(generator)
        consumer = JobConsumer(producer, consum=self._append)

        # Start staging
        producer.start()
        consumer.start(callback=self.staged.emit)

        self._producer = producer
        self._consumer = consumer

    def _append(self, data):
        # Check duplicated
        _id = data["_id"]
        if _id in self._appended:
            return

        # Start
        root = QtCore.QModelIndex()
        last = self.rowCount(root)

        self.beginInsertRows(root, last, last)

        node = Node()
        node.update(data)
        self.add_child(node)
        self._appended.add(_id)

        self.endInsertRows()

    def stop(self):
        producer = self._producer
        consumer = self._consumer

        if producer is None or consumer is None:
            return

        if producer.producing:
            producer.stop()
        if consumer.consuming:
            consumer.stop()

        if producer.producing or consumer.consuming:
            # Wait till they both stopped.
            self.canceling.emit()
            while producer.producing or consumer.consuming:
                pass
            self.canceled.emit()

    def columnCount(self, parent):
        return max(len(self.STAGING_COLUMNS), len(self.UPLOAD_COLUMNS))

    def data(self, index, role):
        if not index.isValid():
            return

        if role == self.StagingDisplayRole or role == self.StagingSortRole:
            node = index.internalPointer()
            column = index.column()
            key = self.STAGING_COLUMNS[column]

            return node.get(key, None)

        if role == self.UploadDisplayRole or role == self.UploadSortRole:
            node = index.internalPointer()
            column = index.column()
            key = self.UPLOAD_COLUMNS[column]

            value = node.get(key, None)
            if (key == "status" and
                    value is not None and value < len(self.STATUS)):
                return self.STATUS[value]

            return node.get(key, None)

        if role == self.UploadProgressRole:
            node = index.internalPointer()
            return node.get("progress", 0)

        return super(JobSourceModel, self).data(index, role)

    def setData(self, index, value, role):
        """Change the data on the nodes.

        Returns:
            bool: Whether the edit was successful

        """
        if index.isValid():
            if role == self.StagingDisplayRole:
                node = index.internalPointer()
                column = index.column()

                key = self.STAGING_COLUMNS[column]
                node[key] = value
                # passing `list()` for PyQt5 (see PYSIDE-462)
                self.dataChanged.emit(index, index, list())

                return True  # must return true if successful

            if role == self.UploadDisplayRole:
                node = index.internalPointer()
                column = index.column()

                key = self.UPLOAD_COLUMNS[column]
                node[key] = value
                # passing `list()` for PyQt5 (see PYSIDE-462)
                self.dataChanged.emit(index, index, list())

                return True  # must return true if successful

        return False


class JobStagingProxyModel(QtCore.QSortFilterProxyModel):

    COLUMNS = JobSourceModel.STAGING_COLUMNS
    StagingDisplayRole = JobSourceModel.StagingDisplayRole
    StagingSortRole = JobSourceModel.StagingSortRole

    def __init__(self, parent=None):
        super(JobStagingProxyModel, self).__init__(parent=parent)
        self.setSortRole(self.StagingSortRole)

    def columnCount(self, parent):
        return len(self.COLUMNS)

    def headerData(self, section, orientation, role):

        if role == QtCore.Qt.DisplayRole:
            if section < len(self.COLUMNS):
                label = self.COLUMNS[section]
                return "Size (MB)" if label == "size" else label.capitalize()

        super(JobStagingProxyModel,
              self).headerData(section, orientation, role)

    def data(self, index, role):
        if not index.isValid():
            return None

        if role == QtCore.Qt.DisplayRole or role == QtCore.Qt.EditRole:
            model = self.sourceModel()
            index = self.mapToSource(index)
            return model.data(index, self.StagingDisplayRole)

        if role == self.StagingSortRole:
            model = self.sourceModel()
            index = self.mapToSource(index)
            return model.data(index, role)

    def filterAcceptsRow(self, row=0, parent=QtCore.QModelIndex()):
        model = self.sourceModel()
        index = model.index(row, 0, parent=parent)

        # Ensure index is valid
        if not index.isValid() or index is None:
            return True

        # Get the node data and validate
        node = model.data(index, TreeModel.NodeRole)

        return node.get("status") == 0


class JobUploadProxyModel(QtCore.QSortFilterProxyModel):

    COLUMNS = JobSourceModel.UPLOAD_COLUMNS
    UploadDisplayRole = JobSourceModel.UploadDisplayRole
    UploadSortRole = JobSourceModel.UploadSortRole

    def __init__(self, parent=None):
        super(JobUploadProxyModel, self).__init__(parent=parent)
        self.setSortRole(self.UploadSortRole)

    def columnCount(self, parent):
        return len(self.COLUMNS)

    def headerData(self, section, orientation, role):

        if role == QtCore.Qt.DisplayRole:
            if section < len(self.COLUMNS):
                return self.COLUMNS[section].capitalize()

        super(JobUploadProxyModel, self).headerData(section, orientation, role)

    def data(self, index, role):
        if not index.isValid():
            return None

        if role == QtCore.Qt.DisplayRole or role == QtCore.Qt.EditRole:
            model = self.sourceModel()
            index = self.mapToSource(index)
            return model.data(index, self.UploadDisplayRole)

        if role == self.UploadSortRole:
            model = self.sourceModel()
            index = self.mapToSource(index)
            return model.data(index, role)

    def filterAcceptsRow(self, row=0, parent=QtCore.QModelIndex()):
        model = self.sourceModel()
        index = model.index(row, 0, parent=parent)

        # Ensure index is valid
        if not index.isValid() or index is None:
            return True

        # Get the node data and validate
        node = model.data(index, TreeModel.NodeRole)

        if node.get("status") > 0:
            import threading
            # Testing...
            def update():
                if node["progress"] >= 100:
                    return
                node["progress"] += 1
                # self.dataChanged.emit(index, index, list())
                self.parent().update()
                threading.Timer(0.1, update).start()

            update()

            return True

        return node.get("status") > 0
