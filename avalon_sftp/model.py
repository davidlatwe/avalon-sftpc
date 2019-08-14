
import threading
from collections import deque

from avalon.vendor.Qt import QtCore
from avalon.tools.projectmanager.model import TreeModel, Node

from .lib import ProducerCondition, ConsumerCondition
from .worker import MockJobGenerator


class JobProducer(object):

    def __init__(self, generator):
        self.generator = generator
        self.producing = False
        self.interrupted = False
        self.deque = deque()
        self.thread = {"_": None}
        self.condition = threading.Condition()

    def stop(self):
        self.interrupted = True
        self.generator.stop()

    def is_alive(self):
        return self.thread["_"] and self.thread["_"].is_alive()

    def start(self):
        if self.thread["_"]:
            self.stop()
            print("Stopping producer..")
            # Wait till previous thread stopped.
            while self.thread["_"]:
                pass

        def produce():
            condition = ProducerCondition(self.condition)

            self.deque.clear()
            self.interrupted = False
            self.producing = True

            for job in self.generator.start():
                with condition:
                    if not self.interrupted:
                        self.deque.append(job)
                    else:
                        break

            # Bye
            with condition:
                self.producing = False
            self.thread["_"] = None

        self.thread["_"] = threading.Thread(target=produce, daemon=True)
        self.thread["_"].start()


class JobConsumer(object):

    def __init__(self, producer, model):
        self.producer = producer
        self.model = model
        self.consuming = False
        self.interrupted = False
        self.deque = producer.deque
        self.thread = {"_": None}
        self.condition = producer.condition

    def stop(self):
        self.interrupted = True

    def is_alive(self):
        return self.thread["_"] and self.thread["_"].is_alive()

    def start(self):
        if self.thread["_"]:
            self.stop()
            print("Stopping consumer..")
            # Wait till previous thread stopped.
            while self.thread["_"]:
                pass

        producer = self.producer
        deque = self.deque
        pauser = (lambda: not deque and producer.is_alive())

        def consume():
            condition = ConsumerCondition(self.condition, pauser)

            self.interrupted = False
            self.consuming = True

            while True:
                with condition:
                    if not self.interrupted and (producer.producing or deque):
                        job = deque.popleft()
                    else:
                        break

                self.model.add_item(job)
            # Bye
            self.consuming = False
            self.thread["_"] = None

        self.thread["_"] = threading.Thread(target=consume, daemon=True)
        self.thread["_"].start()


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

        generator = MockJobGenerator()
        producer = JobProducer(generator)
        consumer = JobConsumer(producer, self)

        self._producer = producer
        self._consumer = consumer

    def refresh(self):
        producer = self._producer
        consumer = self._consumer

        if producer.producing:
            producer.stop()
        if consumer.consuming:
            consumer.stop()

        while producer.producing or consumer.consuming:
            # Wait till they both stopped.
            pass

        self.clear()

        producer.start()
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
