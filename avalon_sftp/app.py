
import sys
from avalon.vendor.Qt import QtWidgets
from avalon import style
from .widgets import VersionListWidget, WorkerWidget


class Window(QtWidgets.QDialog):
    """Workfile uploader main window
    """

    def __init__(self, parent=None):
        super(Window, self).__init__(parent)

        self.setWindowTitle("Workfile Uploader")

        body = QtWidgets.QWidget()

        version = VersionListWidget()
        control = WorkerWidget()

        body_layout = QtWidgets.QVBoxLayout(body)
        body_layout.addWidget(version)
        body_layout.addWidget(control)

        layout = QtWidgets.QVBoxLayout(self)
        layout.addWidget(body)

        # Connect
        control.start_btn.clicked.connect(version.refresh)

        # Defaults
        self.resize(700, 500)


def show():
    app = QtWidgets.QApplication(sys.argv)
    window = Window()
    window.setStyleSheet(style.load_stylesheet())
    window.show()
    app.exec_()


def cli():
    show()
