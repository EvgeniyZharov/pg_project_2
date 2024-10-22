from PyQt5 import QtWidgets
import sys
from PyQt5.QtCore import QTimer, Qt
from PyQt5.QtWidgets import QGraphicsView, QGraphicsScene, QGraphicsEllipseItem

import pandas as pd
import sys

from base_wind import BaseWindow
from reg_win import RegWindow
from graph_window import GraphWindow


class MyWindow(QtWidgets.QMainWindow):
    databases = list()
    tables = dict()

    tools = [
        "PK Index",
    ]

    settings = {
        "database": "",
        "table": "",
        "tool": "",
        "timer_counter": 0,
    }

    code_lines = [
        "Подключение к СУБД PostgreSQL.",
        "Настройка конфигурации анализируемой БД.",
        "Автоматическая настройка конфигурации.",
        "Сброс настроек конфигурации.",
        "Сбор статистики.",
        "Сбор статистики с различных источников в БД.",
        "Формирование отчетов.",
        "Рекомендация(-и):",
        "- Пропущен индекс по первичному ключу",
        "- Пропущен частичный индекс",
        "- Неэффективный индекс someIndex: увеличение времени записи больше времени чтения записи в индексе",
    ]

    def set_combo_box(self):
        self.ui.comboBox.clear()
        self.ui.comboBox.addItem(" ")
        for elem in self.databases:
            self.ui.comboBox.addItem(elem)

        self.ui.comboBox_3.clear()
        self.ui.comboBox_3.addItem(" ")
        for tool in self.tools:
            self.ui.comboBox_3.addItem(tool)

    def work_combo_1(self, text):
        if text:
            self.settings["database"] = text
            self.ui.comboBox_2.clear()
            self.ui.comboBox_2.addItem(" ")
            for elem in self.tables[text]:
                self.ui.comboBox_2.addItem(elem)

    def work_combo_2(self, text):
        if text:
            self.settings["table"] = text

    def work_combo_3(self, text):
        if text:
            self.settings["tool"] = text

    def update_text(self):
        if self.settings["timer_counter"] < 11:
            self.ui.textBrowser.append(f"{self.code_lines[self.settings['timer_counter']]}\n")
            self.settings["timer_counter"] += 1
        else:
            self.timer.stop()
            self.ui.export_btn.setEnabled(True)

    def work_btn(self):
        if self.settings["database"] and self.settings["table"] and self.settings["tool"]:
            self.ui.textBrowser.setText("Программа начала свою работу.\n")
            self.timer.timeout.connect(self.update_text)
            self.timer.start(2000)

    def work_start_btn(self):
        self.close()
        self.reg_window.open()

    def work_export_btn(self):
        data = {"Database": ["Company", "Company", "Company"],
                "Table": ["employee", "employee", "department"],
                "Index": ["WorkEmpIndex", "SalaryEmpIndex", "AddressDepartIndex"],
                "Index ID": [5, 23, 34],
                "Page Count": [1233, 2332, 1533],
                "Total Index Size (MB)": ["2344.00", "3231.60", "2353.40"],
                "Fragmentation (%)": ["2.03%", "0.04%", "1.12%"]}
        data = pd.DataFrame(data)
        data.to_csv("statistic.csv")

    def work_graph(self):
        self.graph_w.show()

    def __init__(self):
        super(MyWindow, self).__init__()
        self.ui = BaseWindow()
        self.ui.setupUi(self)
        self.set_combo_box()
        self.ui.comboBox.activated[str].connect(self.work_combo_1)
        self.ui.comboBox_2.activated[str].connect(self.work_combo_2)
        self.ui.comboBox_3.activated[str].connect(self.work_combo_3)
        self.ui.pushButton.clicked.connect(self.work_btn)
        self.ui.start_btn.clicked.connect(self.work_start_btn)
        self.ui.export_btn.clicked.connect(self.work_export_btn)
        self.ui.graph_btn.clicked.connect(self.work_graph)

        self.reg_window = QtWidgets.QDialog()
        self.reg_w = RegWindow()
        self.reg_w.setupUi(self.reg_window)
        self.reg_w.pushButton.clicked.connect(self.work_save_btn)

        self.graph_w = GraphWindow()

        self.timer = QTimer(self)
        # self.timer.timeout.connect(self.showTime)

    def work_save_btn(self):
        title = self.reg_w.lineEdit_4.text()
        ip = self.reg_w.lineEdit.text()
        password = self.reg_w.lineEdit_2.text()
        if len(ip) > 5 and len(password) > 6 and len(title) > 4:
            pattern = ["employee", "department", "role", "category"]
            self.databases.append(title)
            self.tables[title] = pattern
            self.reg_window.close()
            self.show()
            self.set_combo_box()
        else:
            print("error")


app = QtWidgets.QApplication([])
application = MyWindow()
application.show()

sys.exit(app.exec())
