from PyQt5 import QtCore, QtWidgets
from coins import Coins

class MyWindow(QtWidgets.QWidget):
    def __init__(self, parent=None):

        QtWidgets.QWidget.__init__(self, parent)

        self.btnStart = QtWidgets.QPushButton("Start")
        self.btnExit = QtWidgets.QPushButton("Exit")
        self.btnload_coins = QtWidgets.QPushButton("Загрузить список монет")

        self.vBox = QtWidgets.QVBoxLayout()

        self.vBox.addWidget(self.btnload_coins)
        self.vBox.addWidget(self.btnStart)
        self.vBox.addWidget(self.btnExit)

        self.setLayout(self.vBox)

        self.btnExit.clicked.connect(QtWidgets.qApp.quit)

        self.btnStart.clicked.connect(self.on_clicked_startNow)
        self.btnload_coins.clicked.connect(self.on_clicked_load_coins)

    def on_clicked_startNow(self):
        Coins.data_request(self)

    def on_clicked_load_coins(self):
        Coins.coins_list_request()
        self.message_box("Список монет обновлен")

    # Вывод уведомления
    def message_box(self, text):
        dialog = QtWidgets.QMessageBox(QtWidgets.QMessageBox.Information, "Внимание", text,
                                       buttons=QtWidgets.QMessageBox.Ok, parent=(self))
        dialog.exec()