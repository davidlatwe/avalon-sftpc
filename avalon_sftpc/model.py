
import threading
from multiprocessing import Queue
from weakref import WeakValueDictionary

from avalon import io
from avalon.vendor import qtawesome
from avalon.vendor.Qt import QtCore
from avalon.tools.projectmanager.model import TreeModel, Node

_Uploader = None
_PackageProducer = None
# ^^^
# For the scenario like generating job package in Maya, which in an environment
# that may not have the dependency module `pysftp` installed, but requires and
# only needs to access `.util`, we need to delay import stuff from `work.py` so
# the `ImportError` can be avoided.
#
# Above two attributes were get assigned when `app.show` is called, and depends
# on *demo* enabled or not, use different classes:
#
# if demo:
#     _Uploader = mock.MockUploader
#     _PackageProducer = mock.MockPackageProducer
# else:
#     _Uploader = worker.Uploader
#     _PackageProducer = worker.PackageProducer
#


class JobItem(object):
    """
    """

    __slots__ = ("_id", "site", "content", "transferred", "result",
                 "__weakref__")

    def __init__(self, job_id, site, content):
        self._id = str(job_id)
        self.site = site
        self.content = content
        self.transferred = 0
        self.result = 0


class PackageItem(Node):
    """
    """

    def __init__(self, data):
        self.byte = data.pop("byte")  # To comput progress
        self.hash = data.pop("hash")
        self.jobs = [JobItem(io.ObjectId(), data["site"], content)
                     for content in data["files"]]
        self.total = len(self.jobs)

        super(PackageItem, self).__init__(data)

        self["progress"] = self.progress

    def progress(self):
        """Return transfer progress percentage

        Returns:
            float: Upload progress percentage

        """
        uploaded = 0
        errored = False
        transferred = 0

        for job in self.jobs:
            transferred += job.transferred

            if job.result == 0:
                pass  # Still pending
            elif job.result == 1:
                uploaded += 1
            else:
                errored = True

        if transferred > 0:
            if transferred < self.byte:
                if not errored:
                    self["status"] = 2  # Uploading
                else:
                    self["status"] = 3  # Errored
            else:
                if not errored:
                    self["status"] = 4  # Completed
                else:
                    self["status"] = 5  # End with error

        return transferred / self.byte * 100, uploaded, self.total

    def __eq__(self, other):
        # Assume we only compare with other `PackageItem` instance
        return self.hash == other.hash

    def __hash__(self):
        return self.hash


class JobSourceModel(TreeModel):  # QueueModel ?

    MAX_CONNECTIONS = 10

    staging = QtCore.Signal()
    staged = QtCore.Signal()
    canceling = QtCore.Signal()
    canceled = QtCore.Signal()

    STAGING_COLUMNS = [
        "project",
        "type",
        "description",
        "site",
        "count",
        "size",
    ]

    StagingDisplayRole = QtCore.Qt.UserRole + 10
    StagingSortRole = QtCore.Qt.UserRole + 11

    UPLOAD_COLUMNS = [
        "project",
        "type",
        "description",
        "status",  # This has been hidden
        "progress",
    ]

    UploadDisplayRole = QtCore.Qt.UserRole + 20
    UploadSortRole = QtCore.Qt.UserRole + 21
    UploadDecorationRole = QtCore.Qt.UserRole + 22
    UploadErrorRole = QtCore.Qt.UserRole + 23

    STATUS = [
        "staging",
        "pending",
        "uploading",
        "errored",
        "completed",
        "endWithError",
    ]

    STATUS_ICON = [
        ("meh-o", "#999999"),
        ("clock-o", "#95A1A5"),
        ("paper-plane", "#52D77B"),
        ("paper-plane", "#ECA519"),
        ("check-circle", "#5AB6E4"),
        ("warning", "#EC534E"),
    ]

    def __init__(self, parent=None):
        super(JobSourceModel, self).__init__(parent=parent)

        self.jobsref = WeakValueDictionary()
        self.pipe_in = Queue()
        self.pipe_out = Queue()

        self.producer = _PackageProducer()
        self.consumers = [_Uploader(self.pipe_in, self.pipe_out, id)
                          for id in range(self.MAX_CONNECTIONS)]
        self.consume()

        self.status_icon = [
            qtawesome.icon("fa.{}".format(icon), color=color)
            for icon, color in self.STATUS_ICON
        ]

    def is_staging(self):
        return self.producer.producing

    def is_uploading(self):
        return any(c.consuming for c in self.consumers)

    def stage(self, job_file):
        """
        Args:
            job_file (str): JSON file

        """
        self.staging.emit()

        # Start staging
        self.producer.start(resource=job_file,
                            on_produce=self._append,
                            on_complete=self.staged.emit)

    def _append(self, data):

        package = PackageItem(data)

        # Check duplicated
        all_nodes = self._root_node.children()
        if package in all_nodes:
            # If duplicated package has completed, allow to stage
            # again.
            find = list(reversed(all_nodes))
            if find[find.index(package)]["status"] <= 2:
                return

        # Start
        root = QtCore.QModelIndex()
        last = self.rowCount(root)

        self.beginInsertRows(root, last, last)
        self.add_child(package)
        self.endInsertRows()

    def stop(self):
        """Stop all activities"""
        if self.producer.producing:
            self.producer.stop()

        for consumer in self.consumers:
            consumer.stop()

        if self.is_staging() or self.is_uploading():
            # Wait till they both stopped.
            self.canceling.emit()
            while self.is_staging() or self.is_uploading():
                pass
            self.canceled.emit()

    def pending(self, package):
        for job in package.jobs:
            self.pipe_in.put(job)
            self.jobsref[job._id] = job

    def consume(self):
        for c in self.consumers:
            c.start()

        def update():
            while True:
                id, progress, result, process_id = self.pipe_out.get()
                job = self.jobsref[id]
                job.transferred = progress
                job.result = result

                if result == 0:
                    # Still uploading
                    self.consumers[process_id].consuming = True
                else:
                    # Upload completed or error occurred
                    self.consumers[process_id].consuming = False

        updator = threading.Thread(target=update, daemon=True)
        updator.start()

    def clear_stage(self):
        all_nodes = self._root_node.children()

        if all(n.get("status", 0) == 0 for n in all_nodes):
            # All staged, clear all
            self.clear()
            return

        # Remove staged only
        root = QtCore.QModelIndex()
        for node in list(all_nodes):
            if node.get("status", 0) == 0:
                row = node.row()
                self.beginRemoveRows(root, row, row)
                all_nodes.remove(node)
                self.endRemoveRows()

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
            if key == "progress":
                return value()
            return value

        if role == self.UploadDecorationRole:
            # Put icon to 'progress' column
            if index.column() == 4:
                node = index.internalPointer()
                status = node.get("status", 0)
                return self.status_icon[status]

        if role == self.UploadErrorRole:
            node = index.internalPointer()
            if any(job.result not in (0, 1) for job in node.jobs):
                return node  # Only return package that has failed job
            else:
                return None

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
                if key == "status":
                    if value == 1:
                        # Push to pending
                        self.pending(node)

                if key == "progress":
                    # `progress` should not be set
                    return

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
    UploadDecorationRole = JobSourceModel.UploadDecorationRole

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

        if role == QtCore.Qt.DecorationRole:
            model = self.sourceModel()
            index = self.mapToSource(index)
            return model.data(index, self.UploadDecorationRole)

    def filterAcceptsRow(self, row=0, parent=QtCore.QModelIndex()):
        model = self.sourceModel()
        index = model.index(row, 0, parent=parent)

        # Ensure index is valid
        if not index.isValid() or index is None:
            return True

        node = model.data(index, TreeModel.NodeRole)

        return node.get("status", 0) > 0
