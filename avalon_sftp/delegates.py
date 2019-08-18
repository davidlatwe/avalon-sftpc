
from avalon.vendor.Qt import QtWidgets, QtCore


class ProgressDelegate(QtWidgets.QStyledItemDelegate):

    def __init__(self, parent=None):
        super(ProgressDelegate, self).__init__(parent)

        bar = QtWidgets.QProgressBar()
        bar.setStyleSheet("""
            QProgressBar {
                border: 1px solid grey;
                border-radius: 5px;
                background-color: #363636;
            }
            QProgressBar::chunk {
                background-color: #57B6DA;
                border-radius: 4px;
                margin: 1px;
            }
        """)

        opt_bar = QtWidgets.QStyleOptionProgressBar()
        opt_bar.initFrom(bar)

        opt_bar.minimum = 0
        opt_bar.maximum = 100
        opt_bar.textVisible = True
        # (NOTE) This aligns better than using `text-align: center` in
        #        style sheet.
        opt_bar.textAlignment = QtCore.Qt.AlignCenter

        self.opt_bar = opt_bar
        self.bar = bar
        self.style = bar.style()

        view = self.parent()
        proxy = view.model()
        model = proxy.sourceModel()

        self.proxy = proxy
        self.UploadDisplayRole = model.UploadDisplayRole
        self.CE_ProgressBar = QtWidgets.QStyle.CE_ProgressBar

    def paint(self, painter, option, index):
        super(ProgressDelegate, self).paint(painter, option, index)

        index = self.proxy.mapToSource(index)
        progress = index.data(self.UploadDisplayRole)

        opt_bar = self.opt_bar

        opt_bar.rect = option.rect
        opt_bar.rect.setHeight(option.rect.height() - 5)
        opt_bar.rect.setTop(option.rect.top() + 5)

        opt_bar.progress = int(progress)
        opt_bar.text = " %.2f %% " % progress

        self.style.drawControl(self.CE_ProgressBar,
                               opt_bar,
                               painter,
                               # (NOTE) This is the key to be fully *styled*
                               #        via style sheet.
                               self.bar)
