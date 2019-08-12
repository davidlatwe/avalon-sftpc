
import sys
# from avalon.vendor.Qt import QtWidgets
from Qt import QtWidgets


class SceneUploader(QtWidgets.QDialog):

    def __init__(self, parent=None):
        super(SceneUploader, self).__init__(parent)

        button = QtWidgets.QProgressBar()
        button.setMinimum(0)
        button.setMaximum(100)
        for i in range(100):
            button.setValue(i)
        button.show()


class VersionUploader():
    # Like Loader, but only for upload
    pass


def show():
    app = QtWidgets.QApplication(sys.argv)
    window = SceneUploader()
    window.show()
    app.exec_()


def cli():
    show()


show()
