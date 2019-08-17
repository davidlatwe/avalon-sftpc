
import sys
from avalon.vendor.Qt import QtWidgets, QtCore
from avalon import tools, style
from .widgets import JobWidget


module = sys.modules[__name__]
module.window = None


class Window(QtWidgets.QDialog):
    """Avalon SFTP uploader main window
    """

    def __init__(self, parent=None):
        super(Window, self).__init__(parent)

        self.setWindowTitle("Avalon SFTP Uploader")

        body = QtWidgets.QWidget()

        stage = JobWidget()

        messenger = QtWidgets.QLabel()

        body_layout = QtWidgets.QVBoxLayout(body)
        body_layout.addWidget(stage)
        body_layout.addWidget(messenger)

        layout = QtWidgets.QVBoxLayout(self)
        layout.addWidget(body)

        stage.staging.connect(self.on_staging)
        stage.staged.connect(self.on_staged)
        stage.canceling.connect(self.on_canceling)
        stage.canceled.connect(self.on_canceled)

        self.stage = stage
        self.messenger = messenger

        # Defaults
        self.resize(700, 500)

    def closeEvent(self, event):
        self.stage.on_quit()
        return super(Window, self).closeEvent(event)

    def echo(self, message, repeat=0):
        messenger = self.messenger

        if repeat:
            messenger.setText(str(message[-1]))

            def repeater():
                message.insert(0, message.pop())
                self.echo(message, repeat)

            tools.lib.schedule(repeater, repeat, channel="message")
        else:
            messenger.setText(str(message))
            print(message)
            tools.lib.schedule(lambda: messenger.setText(""),
                               5000,
                               channel="message")

    def on_staging(self):
        anim_message = [
            "Staging.....",
            "Staging....",
            "Staging...",
            "Staging..",
            "Staging.",
        ]
        print(anim_message[0])
        self.echo(anim_message, repeat=200)

    def on_staged(self):
        self.echo("Complete !")

    def on_canceling(self):
        anim_message = [
            "Canceling...",
            "Canceling..",
            "Canceling.",
        ]
        print(anim_message[0])
        self.echo(anim_message, repeat=600)

    def on_canceled(self):
        self.echo("Canceled.")


def show(debug=False, parent=None):
    """Display Uploader GUI

    Arguments:
        debug (bool, optional): Run uploader in debug-mode,
            defaults to False
        parent (QtCore.QObject, optional): The Qt object to parent to.

    """

    # Remember window
    if module.window is not None:
        try:
            module.window.show()

            # If the window is minimized then unminimize it.
            if module.window.windowState() & QtCore.Qt.WindowMinimized:
                module.window.setWindowState(QtCore.Qt.WindowActive)

            # Raise and activate the window
            module.window.raise_()             # for MacOS
            module.window.activateWindow()     # for Windows

            return

        except RuntimeError as e:
            if not e.message.rstrip().endswith("already deleted."):
                raise

            # Garbage collected
            module.window = None

    if debug:
        import traceback
        sys.excepthook = lambda typ, val, tb: traceback.print_last()

    with tools.lib.application():
        window = Window(parent)
        window.setStyleSheet(style.load_stylesheet())
        window.show()

        module.window = window


def cli(*args):
    show()
