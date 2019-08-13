
import threading
from avalon.vendor.Qt import QtCore
from avalon.tools.projectmanager.model import TreeModel, Node

from .lib import ConsumerCondition
from .worker import MockJobProducer


class VersionWorkerModel(TreeModel):

    COLUMNS = [
        "asset",
        "family",
        "subset",
        "version",
        "progress",
        "action",
    ]

    def __init__(self, parent=None):
        super(VersionWorkerModel, self).__init__(parent=parent)
        self._producer = MockJobProducer()

    def refresh(self):
        producer = self._producer
        deque = producer.deque

        if producer.producing:
            producer.stop()
            print("Stopping..")
        while producer.producing:
            # Wait till stopped.
            pass

        self.clear()
        pro = producer.from_workfile()

        def consume():
            condition = ConsumerCondition(producer.condition,
                                          lambda: not deque and pro.is_alive())
            while True:
                with condition:
                    if producer.producing or deque:
                        data = deque.popleft()
                    else:
                        break
                self.add_item(data)

        consumer = threading.Thread(target=consume, daemon=True)
        consumer.start()

    def add_item(self, data):
        root = QtCore.QModelIndex()
        last = self.columnCount(root)
        self.beginInsertRows(root, last, last)

        node = Node()
        node.update(data)
        node.update({
            "progress": 0,
            "action": "HAHA",
        })

        self.add_child(node)

        self.endInsertRows()
