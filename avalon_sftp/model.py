
import os
import hashlib
import threading
from multiprocessing import Queue
from weakref import WeakValueDictionary

from avalon import io
from avalon.vendor.Qt import QtCore
from avalon.tools.projectmanager.model import TreeModel, Node

from .worker import PackageProducer, MockUploader


class JobItem(object):

    __slots__ = ("_id", "src", "dst", "site", "transferred", "__weakref__")

    def __init__(self, id, src, dst, site):
        self._id = id
        self.site = site
        self.src = src
        self.dst = dst
        self.transferred = 0


class PackageItem(Node):
    """
    """

    def __init__(self, data):
        super(PackageItem, self).__init__(data)

        site = data["site"]

        self.jobs = list()
        self.byte = 0  # To comput progress
        self.site = site
        self.hash = site  # As prefix
        # Ensure unique and sort for hashing
        files = sorted(set(data["files"]))
        self._digest(files)

    def _digest(self, files):
        """To get package's identity and pack jobs
        * Analyze files to prevent duplicate package
        * Spawning job object for uploader to update progress on each file
        """
        jobs = list()
        total_size = 0
        hash_obj = hashlib.sha512()

        for src, dst in files:
            hash_obj.update(src.encode())
            hash_obj.update(dst.encode())

            # Making job object
            job_id = str(io.ObjectId())
            jobs.append(JobItem(job_id, src, dst, self.site))

            # Summing file size
            # total_size += os.path.getsize(src)
            total_size += 1000  # Testing, each job's size is 1000

        if total_size == 0:
            raise Exception("Package size is 0, this should not happen.")

        self.byte = total_size
        self.jobs = jobs
        self.hash += str(hash_obj.digest())

        data = {
            "status": 0,
            "count": len(jobs),
            "size": round(total_size / float(1024**2), 2),  # (MB)
        }
        self.update(data)

    def progress(self):
        """Return transfer progress percentage

        Returns:
            float: Upload progress percentage

        """
        transferred = 0
        for job in self.jobs:
            transferred += job.transferred

        if transferred > 0:
            if transferred < self.byte:
                self["status"] = 2
            else:
                self["status"] = 3

        return transferred / self.byte * 100

    def __getitem__(self, key):
        if key == "progress":
            return self.progress()
        return super(PackageItem, self).__getitem__(key)

    def get(self, key, default=None):
        try:
            return self.__getitem__(key)
        except KeyError:
            return default

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
        "status",
        "progress",
    ]

    UploadDisplayRole = QtCore.Qt.UserRole + 20
    UploadSortRole = QtCore.Qt.UserRole + 21

    STATUS = [
        "staging",
        "pending",
        "uploading",
        "completed",
        "failed",
    ]

    def __init__(self, parent=None):
        super(JobSourceModel, self).__init__(parent=parent)

        self.jobsref = WeakValueDictionary()
        self.pendings = Queue()
        self.progress = Queue()
        self.producer = PackageProducer()
        self.consumers = [MockUploader(self.pendings, self.progress)
                          for i in range(self.MAX_CONNECTIONS)]
        self.consume()

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
        if package in self._root_node.children():
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
            self.pendings.put(job)
            self.jobsref[job._id] = job

    def consume(self):
        for c in self.consumers:
            c.start()

        def update():
            while True:
                id, progress = self.progress.get()
                job = self.jobsref[id]
                job.transferred = progress

        updator = threading.Thread(target=update, daemon=True)
        updator.start()

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

        node = model.data(index, TreeModel.NodeRole)

        return node.get("status") > 0
