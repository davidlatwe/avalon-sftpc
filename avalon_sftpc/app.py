
import sys
import logging
from avalon.vendor.Qt import QtWidgets, QtCore, QtGui
from avalon.vendor import qtawesome
from avalon import tools, style
from .widgets import JobWidget

module = sys.modules[__name__]
module.window = None


main_logger = logging.getLogger("avalon-sftpc")
main_logger.setLevel(logging.INFO)

stream = logging.StreamHandler()
main_logger.addHandler(stream)


class Window(QtWidgets.QDialog):
    """Avalon SFTP uploader main window
    """

    def __init__(self, parent=None):
        super(Window, self).__init__(parent)

        self.setWindowTitle("Avalon SFTP Uploader")

        body = QtWidgets.QWidget()

        stage = JobWidget()

        statusline = StatusLineWidget(main_logger, self)

        body_layout = QtWidgets.QVBoxLayout(body)
        body_layout.addWidget(stage)
        body_layout.addWidget(statusline)

        layout = QtWidgets.QVBoxLayout(self)
        layout.addWidget(body)

        self.stage = stage
        self.statusline = statusline

        # Defaults
        self.resize(580, 700)

    def closeEvent(self, event):
        self.stage.on_quit()
        return super(Window, self).closeEvent(event)


class WidgetLogHandler(logging.Handler):

    def __init__(self, widget):
        super(WidgetLogHandler, self).__init__()
        self.widget = widget

        format = "%(message)s"
        formatter = logging.Formatter(format)
        self.setFormatter(formatter)

    def emit(self, record):
        dotting = record.msg.endswith(".....")
        try:
            log = self.format(record)
            level = record.levelno
            self.widget.echo.emit(level, log, dotting)
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            self.handleError(record)


class LogLevelIcon(QtWidgets.QWidget):

    def __init__(self, parent=None):
        super(LogLevelIcon, self).__init__(parent)

        self.level = logging.NOTSET
        self.icons = {
            logging.NOTSET: qtawesome.icon("fa.bell", color="#404040"),
            logging.DEBUG: qtawesome.icon("fa.bell", color="#5AD594"),
            logging.INFO: qtawesome.icon("fa.bell", color="#439BF2"),
            logging.WARNING: qtawesome.icon("fa.bell", color="#EED14E"),
            logging.ERROR: qtawesome.icon("fa.bell", color="#F53434"),
            logging.CRITICAL: qtawesome.icon("fa.bell", color="#F34FC1"),
        }

        self.setMinimumSize(18, 18)
        self.setMaximumSize(18, 18)

    def paintEvent(self, event):
        painter = QtGui.QPainter()
        painter.begin(self)
        painter.drawPixmap(0, 0, self.icons[self.level].pixmap(18, 18))
        painter.end()


class StatusLineWidget(QtWidgets.QWidget):

    echo = QtCore.Signal(int, str, int)

    def __init__(self, logger, parent=None):
        super(StatusLineWidget, self).__init__(parent)

        icon = LogLevelIcon()

        line = QtWidgets.QLineEdit()
        line.setReadOnly(True)
        line.setStyleSheet("""
            QLineEdit {
                border: 0px;
                padding: 0 8px;
                color: #AAAAAA;
                background: #363636;
            }
        """)

        body = QtWidgets.QHBoxLayout(self)
        body.addWidget(icon)
        body.addWidget(line)

        self.icon = icon
        self.line = line

        handler = WidgetLogHandler(self)
        logger.addHandler(handler)

        self.echo.connect(self.on_echo)

    def on_echo(self, level, log, dotting=False):
        icon = self.icon
        line = self.line

        ALARM = logging.WARNING

        if icon.level >= ALARM and level < ALARM:
            return

        def _echo(level, log):
            icon.level = level
            icon.update()
            line.setText(log)

        if dotting:
            if log.endswith("....."):
                log = log[:-4]
            else:
                log += "."

            _echo(level, log)

            def animator():
                self.on_echo(level, log, dotting)

            tools.lib.schedule(animator, 300, channel="statusline")

        else:
            _echo(level, log)

            if level < ALARM:
                # Back to default state
                tools.lib.schedule(lambda: _echo(0, ""),
                                   10000,
                                   channel="statusline")


def show(debug=False, demo=False, parent=None):
    """Display Uploader GUI

    Arguments:
        debug (bool, optional): Run uploader in debug-mode,
            defaults to False
        demo (bool, optional): Run uploader in demo-mode,
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

    # Assign workers
    if demo:
        from . import model, mock
        model._Uploader = mock.MockUploader
        model._PackageProducer = mock.MockPackageProducer
    else:
        from . import model, worker
        model._Uploader = worker.Uploader
        model._PackageProducer = worker.PackageProducer

    with tools.lib.application():
        window = Window(parent)
        window.setStyleSheet(style.load_stylesheet())
        window.show()

        module.window = window


def cli(*args):
    show()
