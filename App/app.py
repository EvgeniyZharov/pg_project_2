from App.templates.main_menu import MainMenu
from App.templates.connect_menu import ConnectDBMenu
from App.templates.db_line_work import DBLineEdit
from App.templates.choice_table import ChoiceTableAdd
from App.templates.index_analize_menu import IndexAnalizeMenu
from App.templates.planer_menu import PlanerMenu
from App.templates.time_menu import TimeMenu
from App.templates.choice_index import ChoiceIndex

from App.pd_db import PostgresClient
import sys
import psycopg2
from PyQt5 import QtWidgets
from PyQt5.QtWidgets import QMessageBox
import json


class MyWindow(QtWidgets.QMainWindow):
    DB = ["PostgreSQL"]

    def __init__(self):
        """
        Конструктор для подготовки файла настроек settings.py, вспомогательных переменных,
        а также для подготовки окон и подключения кнопкам необходимого функционала.
        """
        super(MyWindow, self).__init__()
        self.settings_file = "settings.json"
        self.SETTINGS = None
        self.get_settings(self.settings_file)
        self.help_data = {
            "type_db_for_time": None,
            "db_for_line_request": None,
            "type_request_for_db": None,
            "db_for_analise": None,
            "table_for_analise": None,
            "type_time_planing": None,
            "on_tables": list(),
            "off_tables": list(),
            "on_indexes": list(),
            "off_indexes": list(),
            "months": list(),
            "days": list(),
            "hours": list(),
            "db_template": {
                "host": None,
                "port": "5432",
                "user": None,
                "password": None,
                "db_name": None,
                "type_db": None
            },
        }
        self.db_client = None

        self.mm = MainMenu()
        self.mm.setupUi(self)
        self.update_main_menu_browser()
        self.mm.pushButton.clicked.connect(self.analizeingex_show)
        # self.mm.pushButton_2.clicked.connect()
        self.mm.pushButton_3.clicked.connect(self.dbline_show)
        self.mm.pushButton_4.clicked.connect(self.conndb_show)
        self.mm.pushButton_5.clicked.connect(self.planer_show)

        self.conndb_menu = QtWidgets.QDialog()
        self.conndb_m = ConnectDBMenu()
        self.conndb_m.setupUi(self.conndb_menu)
        self.conndb_m.btn_connect_db.clicked.connect(self.get_data_db)
        self.conndb_m.box_db_types.clear()
        self.conndb_m.box_db_types.addItem(" ")
        for elem in self.DB:
            self.conndb_m.box_db_types.addItem(elem)
        self.conndb_m.box_db_types.activated[str].connect(self.box_db_type)

        self.dbline_menu = QtWidgets.QDialog()
        self.dbline_m = DBLineEdit()
        self.dbline_m.setupUi(self.dbline_menu)
        db_list = [elem["db_name"] for elem in self.SETTINGS["db_list"]]
        self.dbline_m.comboBox.addItem(" ")
        self.dbline_m.comboBox.addItems(db_list)
        self.dbline_m.comboBox.activated[str].connect(self.set_db_line)
        self.dbline_m.box_db_types.activated[str].connect(self.set_type_req)
        self.dbline_m.pushButton.clicked.connect(self.push_request_db)

        self.choicetable_menu = QtWidgets.QDialog()
        self.choicetable_m = ChoiceTableAdd()
        self.choicetable_m.setupUi(self.choicetable_menu)
        self.choicetable_m.comboBox.activated[str].connect(self.choice_tables_delete)
        self.choicetable_m.comboBox_2.activated[str].connect(self.choice_tables_add)
        self.choicetable_m.pushButton.clicked.connect(self.delete_all_tables)
        self.choicetable_m.pushButton_2.clicked.connect(self.add_all_tables)
        self.choicetable_m.pushButton_3.clicked.connect(self.save_tables)

        self.analizeingex_menu = QtWidgets.QDialog()
        self.analizeingex_m = IndexAnalizeMenu()
        self.analizeingex_m.setupUi(self.analizeingex_menu)
        self.update_boxes_db_analise()
        self.analizeingex_m.comboBox.activated[str].connect(self.set_db_analise)
        self.analizeingex_m.comboBox_2.activated[str].connect(self.set_table_analise)
        self.analizeingex_m.pushButton.clicked.connect(self.choice_tables)
        self.analizeingex_m.pushButton_2.clicked.connect(self.run_analise)
        self.analizeingex_m.pushButton_3.clicked.connect(self.download_stat_analise)

        self.planer_menu = QtWidgets.QDialog()
        self.planer_m = PlanerMenu()
        self.planer_m.setupUi(self.planer_menu)
        self.planer_m.btn_date.clicked.connect(self.timemenu_show)
        self.update_boxes_db_planer()
        self.planer_m.comboBox.activated[str].connect(self.set_db_planer)
        self.planer_m.comboBox_2.activated[str].connect(self.set_table_planer)
        self.planer_m.btn_all_tables.clicked.connect(self.choice_tables)
        self.planer_m.comboBox_3.activated[str].connect(self.set_index_planer)
        self.planer_m.btn_all_ind.clicked.connect(self.choice_indexes)
        self.planer_m.btn_create_task.clicked.connect(self.create_task)
        # self.planer_m.btn_setup_task.clicked.connect(self.setup_task)

        self.choiceindex_menu = QtWidgets.QDialog()
        self.choiceindex_m = ChoiceIndex()
        self.choiceindex_m.setupUi(self.choiceindex_menu)
        self.choiceindex_m.comboBox.activated[str].connect(self.choice_indexes_delete)
        self.choiceindex_m.comboBox_2.activated[str].connect(self.choice_indexes_add)
        self.choiceindex_m.pushButton.clicked.connect(self.delete_all_indexes)
        self.choiceindex_m.pushButton_2.clicked.connect(self.add_all_indexes)
        self.choiceindex_m.pushButton_3.clicked.connect(self.save_indexes)

        self.time_menu = QtWidgets.QDialog()
        self.time_m = TimeMenu()
        self.time_m.setupUi(self.time_menu)
        self.time_m.comboBox.activated[str].connect(self.choice_type_date)
        self.time_m.btn_reset.clicked.connect(self.reset_date)
        self.time_m.btn_save.clicked.connect(self.save_date)

    def get_settings(self, filename: str):
        """
        :param filename: название файла настроек, в ном содержится информация о БД, в которые входил пользователь,
        индексах и собранных файлах запуска .bat
        :return:
        """
        with open(filename) as file:
            self.SETTINGS = json.load(file)

    def set_settings(self, filename: str):
        """
        Функция для сохранения изменений в файл с настройками программы. Сохраняет новые БД,
        информацию о новых программах .bat.
        :param filename:
        :return:
        """
        with open(filename, "w") as file:
            json.dump(self.SETTINGS, file)
        print("ok")

    def conndb_show(self):
        """
        функция для запуска окна подключения к новой БД.
        :return:
        """
        self.conndb_menu.show()

    def dbline_show(self):
        """
        Функция для запуска окна взаимодействия с БД через интерфейс ПО.
        :return:
        """
        self.dbline_menu.show()

    def choicetable_show(self):
        """
        Функция для запуска окна более удобного выбора таблиц для анализа.
        :return:
        """
        self.choicetable_menu.show()

    def analizeingex_show(self):
        """
        Функция для запуска окна ввода параметров для анализа.
        :return:
        """
        self.analizeingex_menu.show()

    def planer_show(self):
        """
        Функция для запуска меню планировщика.
        :return:
        """
        self.planer_menu.show()

    def index_show(self):
        """
        Функция для запуска окна удобного выбора индексов
        :return:
        """
        self.choiceindex_menu.show()

    def timemenu_show(self):
        """
        Функция для запуска окна настройки тайминга для планировщика
        :return:
        """
        self.time_menu.show()

    def get_data_db(self):
        """
        Функция для проверки параметров подключения к БД.
        В случае корректных значений сохраняет параметры и сообщает пользователю об успехе.
        Сразу обновляет файл настроек settings.json, чтобы не нужно было постоянно вводить параметры для входа.
        :return:
        """
        db_type = self.help_data["type_db_for_time"]
        host = self.conndb_m.le_host.text()
        port = self.conndb_m.le_port.text()
        user = self.conndb_m.le_user.text()
        password = self.conndb_m.le_pass.text()
        db_name = self.conndb_m.le_db_name.text()
        connection = None
        msg = QMessageBox()
        msg.setIcon(QMessageBox.Information)
        msg.setWindowTitle("Подключение к БД")
        msg.setStandardButtons(QMessageBox.Ok)
        try:
            connection = psycopg2.connect(
                dbname=db_name,
                user=user,
                password=password,
                host=host,
                port=port
            )
            print("Подключение к PostgreSQL успешно установлено")
            db_data = self.help_data["db_template"].copy()
            db_data["host"] = host
            db_data["port"] = port
            db_data["user"] = user
            db_data["password"] = password
            db_data["db_name"] = db_name
            db_data["db_type"] = db_type
            self.SETTINGS["db_list"].append(db_data)
            self.set_settings(self.settings_file)
            msg.setText("Успешно!")
            msg.exec_()
            connection.close()
            self.update_main_menu_browser()
            self.conndb_menu.close()
        except Exception as ex:
            msg.setText("Ошибка!")
            msg.exec_()

    def box_db_type(self, text):
        """
        Функция для обработки бокса со значениями СУБД при добавлении новой.
        :param text:
        :return:
        """
        if text:
            self.help_data["type_db_for_time"] = text

    # Menu for request to DataBase

    def set_db_line(self, text):
        """
        Выбор БД, с которой хочет работать пользователь
        :param text:
        :return:
        """
        if text:
            self.help_data["db_for_line_request"] = text
            db = None
            for elem in self.SETTINGS["db_list"]:
                if elem["db_name"] == text:
                    db = elem
            msg = QMessageBox()
            msg.setIcon(QMessageBox.Information)
            msg.setWindowTitle("Подключение к БД")
            msg.setStandardButtons(QMessageBox.Ok)
            try:
                self.db_client = PostgresClient(db_name=db["db_name"],
                                                host=db["host"],
                                                port=db["port"],
                                                user=db["user"],
                                                password=db["password"])
                msg.setText("Успешно!")
                msg.exec_()
            except Exception as ex:
                msg.setText("Произошла ошибка.")
                msg.exec_()

    def set_type_req(self, text):
        """
        Функция для обработки бокса со значениями типа запроса к БД.
        :param text:
        :return:
        """
        if text:
            self.help_data["type_request_for_db"] = text

    def push_request_db(self):
        """
        Функция для выполнения запроса к БД через интерфейс
        :return:
        """
        if self.db_client:
            request = self.dbline_m.textEdit.toPlainText()
            if self.help_data["type_request_for_db"] == "Чтение":
                result = self.db_client.get_data(request=request)
            elif self.help_data["type_request_for_db"] == "Изменение БД":
                result = self.db_client.set_data(request=request)
            else:
                result = "Неправильный вариант запроса."
            self.dbline_m.textBrowser.setText(result)
        else:
            result = "Нужно выбрать БД, к которой хотите выполнить запрос."
            self.dbline_m.textBrowser.setText(result)

    # For index analise

    def update_boxes_db_analise(self):
        """
        Обновление боксов окна для анализа
        :return:
        """
        self.analizeingex_m.comboBox.clear()
        self.analizeingex_m.comboBox.addItem(" ")
        db_list = [elem["db_name"] for elem in self.SETTINGS["db_list"]]
        self.analizeingex_m.comboBox.addItems(db_list)

    def set_db_analise(self, text):
        """
        Функция для обработки бокса со значениями БД для анализа.
        :param text:
        :return:
        """
        if text:
            self.help_data["db_for_analise"] = text
            db = None
            for elem in self.SETTINGS["db_list"]:
                if elem["db_name"] == text:
                    db = elem
            msg = QMessageBox()
            msg.setIcon(QMessageBox.Information)
            msg.setWindowTitle("Подключение к БД")
            msg.setStandardButtons(QMessageBox.Ok)
            try:
                self.db_client = PostgresClient(db_name=db["db_name"],
                                                host=db["host"],
                                                port=db["port"],
                                                user=db["user"],
                                                password=db["password"])
                msg.setText("Успешно!")
                msg.exec_()
                request = """SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public';"""
                result = self.db_client.get_data(request=request)
                table_list = self.get_from_str_list(text=result)
                self.analizeingex_m.comboBox_2.clear()
                self.analizeingex_m.comboBox_2.addItem(" ")
                self.analizeingex_m.comboBox_2.addItems(table_list)
                self.help_data["off_tables"] = table_list
            except Exception as ex:
                msg.setText("Произошла ошибка.")
                msg.exec_()

    def set_table_analise(self, text):
        """
        Функция для обработки бокса со значениями таблицы для анализа.
        :param text:
        :return:
        """
        if text:
            self.help_data["on_tables"] = [text]

    def choice_tables(self):
        """
        Функция для выбора нужнных таблиц
        :return:
        """
        if self.db_client:
            if self.help_data["on_tables"]:
                self.choicetable_m.comboBox.addItems(self.help_data["on_tables"])
            request = """SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public';"""
            result = self.db_client.get_data(request=request)
            table_list = self.get_from_str_list(text=result)
            self.help_data["off_tables"] = [elem for elem in table_list if elem not in self.help_data["on_tables"]]
            self.choicetable_m.comboBox_2.addItems(self.help_data["off_tables"])
            self.choicetable_menu.show()
        else:
            msg = QMessageBox()
            msg.setIcon(QMessageBox.Information)
            msg.setWindowTitle("Подключение к БД")
            msg.setStandardButtons(QMessageBox.Ok)
            msg.setText("Произошла ошибка.")
            msg.exec_()

    def add_all_tables(self):
        """
        Функция для добавления всех таблиц
        :return:
        """
        self.help_data["on_tables"].extend(self.help_data["off_tables"])
        self.help_data["off_tables"] = list()
        self.choicetable_m.comboBox.clear()
        self.choicetable_m.comboBox_2.clear()
        self.choicetable_m.comboBox.addItems(self.help_data["on_tables"])
        self.choicetable_m.comboBox_2.addItems(self.help_data["off_tables"])

    def delete_all_tables(self):
        """
        Функция для удаления всех таблиц из выбора.
        :return:
        """
        self.help_data["off_tables"].extend(self.help_data["on_tables"])
        self.help_data["on_tables"] = list()
        self.choicetable_m.comboBox.clear()
        self.choicetable_m.comboBox_2.clear()
        self.choicetable_m.comboBox.addItems(self.help_data["on_tables"])
        self.choicetable_m.comboBox_2.addItems(self.help_data["off_tables"])

    def save_tables(self):
        """
        Функция для закрытия окна выбора таблицы
        :return:
        """
        self.choicetable_menu.close()

    def choice_tables_add(self, text):
        """
        Функция для обработки бокса со значениями таблиц на удаление из списка исследуемых.
        :param text:
        :return:
        """
        if text:
            self.help_data["on_tables"].append(text)
            self.help_data["off_tables"].remove(text)
            self.choicetable_m.comboBox.clear()
            self.choicetable_m.comboBox_2.clear()
            self.choicetable_m.comboBox.addItems(self.help_data["on_tables"])
            self.choicetable_m.comboBox_2.addItems(self.help_data["off_tables"])

    def choice_tables_delete(self, text):
        """
        Функция для обработки бокса со значениями таблиц на добавление в список изучаемых.
        :param text:
        :return:
        """
        if text:
            self.help_data["on_tables"].remove(text)
            self.help_data["off_tables"].append(text)
            self.choicetable_m.comboBox.clear()
            self.choicetable_m.comboBox_2.clear()
            self.choicetable_m.comboBox.addItems(self.help_data["on_tables"])
            self.choicetable_m.comboBox_2.addItems(self.help_data["off_tables"])

    def run_analise(self):
        """
        Функция для запуска анализа.
        :return:
        """
        if self.analizeingex_m.checkBox.isChecked():
            self.help_data["on_tables"].extend(self.help_data["off_tables"])
            self.help_data["off_tables"] = list()
        else:
            self.help_data["off_tables"].extend(self.help_data["on_tables"])
            self.help_data["on_tables"] = list()
        msg = QMessageBox()
        msg.setIcon(QMessageBox.Information)
        msg.setWindowTitle("Запуск анализа")
        msg.setStandardButtons(QMessageBox.Ok)
        if self.help_data["analise_system"]:
            pass
        else:
            msg.setText("Нужно выбрать вариант анализа индексов!")
            msg.exec_()

    def download_stat_analise(self):
        pass

    # For index planer

    def update_boxes_db_planer(self):
        """
        Обновление всех боксов
        :return:
        """
        self.planer_m.comboBox.clear()
        self.planer_m.comboBox.addItem(" ")
        db_list = [elem["db_name"] for elem in self.SETTINGS["db_list"]]
        self.planer_m.comboBox.addItems(db_list)

    def set_db_planer(self, text):
        """
        Функция для выбора БД и обновления комбобоксов с названиями таблиц.
        :param text:
        :return:
        """
        if text:
            self.help_data["db_for_analise"] = text
            db = None
            for elem in self.SETTINGS["db_list"]:
                if elem["db_name"] == text:
                    db = elem
            msg = QMessageBox()
            msg.setIcon(QMessageBox.Information)
            msg.setWindowTitle("Подключение к БД")
            msg.setStandardButtons(QMessageBox.Ok)
            try:
                self.db_client = PostgresClient(db_name=db["db_name"],
                                                host=db["host"],
                                                port=db["port"],
                                                user=db["user"],
                                                password=db["password"])
                msg.setText("Успешно!")
                msg.exec_()
                request = """SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public';"""
                result = self.db_client.get_data(request=request)
                table_list = self.get_from_str_list(text=result)
                self.planer_m.comboBox_2.clear()
                self.planer_m.comboBox_2.addItem(" ")
                self.planer_m.comboBox_2.addItems(table_list)
                index_list = [elem["title"] for elem in self.SETTINGS["index_list"]]
                self.planer_m.comboBox_3.clear()
                self.planer_m.comboBox_3.addItem(" ")
                self.planer_m.comboBox_3.addItems(index_list)
                self.help_data["off_tables"] = table_list
                self.help_data["off_indexes"] = index_list
            except Exception as ex:
                msg.setText("Произошла ошибка.")
                msg.exec_()

    def set_table_planer(self, text):
        """
        Функция для обработки бокса со значениями таблиц для планировщика.
        :param text:
        :return:
        """
        if text:
            self.help_data["on_tables"] = [text]

    def set_index_planer(self, text):
        """
        Функция для обработки бокса со значениями индексов для планировщика.
        :param text:
        :return:
        """
        if text:
            self.help_data["on_indexes"] = [text]

    def choice_indexes(self):
        """
        Функция для выбора нужного метода анализа
        :return:
        """
        if self.db_client:
            if self.help_data["on_indexes"]:
                self.choiceindex_m.comboBox.addItems(self.help_data["on_indexes"])
            index_list = [elem["title"] for elem in self.SETTINGS["index_list"]]
            self.help_data["off_indexes"] = [elem for elem in index_list if elem not in self.help_data["on_indexes"]]
            self.choiceindex_m.comboBox_2.addItems(self.help_data["off_indexes"])
            self.choiceindex_menu.show()
        else:
            msg = QMessageBox()
            msg.setIcon(QMessageBox.Information)
            msg.setWindowTitle("Подключение к БД")
            msg.setStandardButtons(QMessageBox.Ok)
            msg.setText("Произошла ошибка.")
            msg.exec_()

    def add_all_indexes(self):
        """
        Функция для добавления всех методов анализа
        :return:
        """
        self.help_data["on_indexes"].extend(self.help_data["off_indexes"])
        self.help_data["off_indexes"] = list()
        self.choiceindex_m.comboBox.clear()
        self.choiceindex_m.comboBox_2.clear()
        self.choiceindex_m.comboBox.addItems(self.help_data["on_indexes"])
        self.choiceindex_m.comboBox_2.addItems(self.help_data["off_indexes"])

    def delete_all_indexes(self):
        """
        Функция для удаления выбранных методов анализа
        :return:
        """
        self.help_data["off_indexes"].extend(self.help_data["on_indexes"])
        self.help_data["on_indexes"] = list()
        self.choiceindex_m.comboBox.clear()
        self.choiceindex_m.comboBox_2.clear()
        self.choiceindex_m.comboBox.addItems(self.help_data["on_indexes"])
        self.choiceindex_m.comboBox_2.addItems(self.help_data["off_indexes"])

    def save_indexes(self):
        """
        Функция для закрытия окна выбора индексов
        :return:
        """
        self.choiceindex_menu.close()

    def choice_indexes_add(self, text):
        """
        Функция для обработки бокса со значениями индексов на добавление.
        :param text:
        :return:
        """
        if text:
            self.help_data["on_indexes"].append(text)
            self.help_data["off_indexes"].remove(text)
            self.choiceindex_m.comboBox.clear()
            self.choiceindex_m.comboBox_2.clear()
            self.choiceindex_m.comboBox.addItems(self.help_data["on_indexes"])
            self.choiceindex_m.comboBox_2.addItems(self.help_data["off_indexes"])

    def choice_indexes_delete(self, text):
        """
        Функция для обработки бокса со значениями индексов на удаление.
        :param text:
        :return:
        """
        if text:
            self.help_data["on_indexes"].remove(text)
            self.help_data["off_indexes"].append(text)
            self.choicetable_m.comboBox.clear()
            self.choicetable_m.comboBox_2.clear()
            self.choicetable_m.comboBox.addItems(self.help_data["on_indexes"])
            self.choicetable_m.comboBox_2.addItems(self.help_data["off_indexes"])

    def create_task(self):
        """
        Функция для создания автономного скрипта Python.
        :return:
        """

        if self.planer_m.checkBox.isChecked():
            self.help_data["on_tables"].extend(self.help_data["off_tables"])
            self.help_data["off_tables"] = list()
        else:
            self.help_data["off_tables"].extend(self.help_data["on_tables"])
            self.help_data["on_tables"] = list()
        if self.planer_m.checkBox_2.isChecked():
            self.help_data["on_indexes"].extend(self.help_data["off_indexes"])
            self.help_data["off_indexes"] = list()
        else:
            self.help_data["off_indexes"].extend(self.help_data["on_indexes"])
            self.help_data["on_indexes"] = list()
        msg = QMessageBox()
        msg.setIcon(QMessageBox.Information)
        msg.setWindowTitle("Подключение к БД")
        msg.setStandardButtons(QMessageBox.Ok)
        if self.help_data["type_time_planing"] != "Настроить дни":
            type_to_time = {0: 3600, 1: 21600, 2: 43200, 3: 86400,
                            4: 259200, 5: 604800, 6: 1209600, 7: 2419200,
                            8: 7257600}
            types_date_planing = [
                "Каждый час", "Каждые 6 часов",
                "Каждые 12 часов", "Каждый день", "Каждые три дня",
                "Каждую неделю", "Каждые 2 недели", "Каждый месяц",
                "Каждый квартал"
            ]
            timing = type_to_time[types_date_planing.index(self.help_data["type_time_planing"])]

            bat_ind = len(self.SETTINGS["run_files"])
            bat_title = f"run_script_{bat_ind}.bat"

            script_content = """
# Этот скрипт просто выводит текущее время
print("ok")
import datetime
print("Текущее время:", datetime.datetime.now())
            """
            script_title = f"script_{bat_ind}.py"
            # Сохраняем скрипт в файл
            with open(script_title, 'w', encoding="utf-8") as script_file:
                script_file.write(script_content)
            bat_program = f"""
@echo off
:loop
python {script_title}
timeout /t {timing} > nul
goto loop
                        """

            self.SETTINGS["run_files"].append({"title": bat_title, "timing": f"{timing}", "script": script_title})
            self.set_settings(self.settings_file)
            with open(bat_title, 'w', encoding="utf-8") as bat_file:
                bat_file.write(bat_program)
            msg.setText("Задача успешно сформирована!")
            msg.exec_()
        else:
            pass

    # Dates settings planing

    def choice_type_date(self, text):
        """
        Функция для обработки бокса со значениями варианта тайминга планировщика.
        :param text:
        :return:
        """
        if text:
            self.help_data["type_time_planing"] = text
            self.time_m.btn_save.setEnabled(True)

    def reset_date(self):
        """
        Функция для сброса галочек в чекбоксах.
        :return:
        """
        self.time_m.m_1.setChecked(False)
        self.time_m.m_2.setChecked(False)
        self.time_m.m_3.setChecked(False)
        self.time_m.m_4.setChecked(False)
        self.time_m.m_5.setChecked(False)
        self.time_m.m_6.setChecked(False)
        self.time_m.m_7.setChecked(False)
        self.time_m.m_8.setChecked(False)
        self.time_m.m_9.setChecked(False)
        self.time_m.m_10.setChecked(False)
        self.time_m.m_11.setChecked(False)
        self.time_m.m_12.setChecked(False)
        self.time_m.d_1.setChecked(False)
        self.time_m.d_2.setChecked(False)
        self.time_m.d_3.setChecked(False)
        self.time_m.d_4.setChecked(False)
        self.time_m.d_5.setChecked(False)
        self.time_m.d_6.setChecked(False)
        self.time_m.d_7.setChecked(False)
        self.time_m.h_1.setChecked(False)
        self.time_m.h_2.setChecked(False)
        self.time_m.h_3.setChecked(False)
        self.time_m.h_4.setChecked(False)
        self.time_m.h_5.setChecked(False)
        self.time_m.h_6.setChecked(False)
        self.time_m.h_7.setChecked(False)
        self.time_m.h_7.setChecked(False)
        self.time_m.h_8.setChecked(False)
        self.time_m.h_9.setChecked(False)
        self.time_m.h_10.setChecked(False)
        self.time_m.h_11.setChecked(False)
        self.time_m.h_12.setChecked(False)
        self.time_m.h_13.setChecked(False)
        self.time_m.h_14.setChecked(False)
        self.time_m.h_15.setChecked(False)
        self.time_m.h_16.setChecked(False)
        self.time_m.h_17.setChecked(False)
        self.time_m.h_18.setChecked(False)
        self.time_m.h_19.setChecked(False)
        self.time_m.h_20.setChecked(False)
        self.time_m.h_21.setChecked(False)
        self.time_m.h_22.setChecked(False)
        self.time_m.h_23.setChecked(False)
        self.time_m.h_24.setChecked(False)

    def save_date(self):
        """
        Функция для получения значений чекбокса в удомном формате.
        :return:
        """
        month = [self.time_m.m_1.isChecked(),
                 self.time_m.m_2.isChecked(),
                 self.time_m.m_3.isChecked(),
                 self.time_m.m_4.isChecked(),
                 self.time_m.m_5.isChecked(),
                 self.time_m.m_6.isChecked(),
                 self.time_m.m_7.isChecked(),
                 self.time_m.m_8.isChecked(),
                 self.time_m.m_9.isChecked(),
                 self.time_m.m_10.isChecked(),
                 self.time_m.m_11.isChecked(),
                 self.time_m.m_12.isChecked(),
                 ]
        days = [
            self.time_m.d_1.isChecked(),
            self.time_m.d_2.isChecked(),
            self.time_m.d_3.isChecked(),
            self.time_m.d_4.isChecked(),
            self.time_m.d_5.isChecked(),
            self.time_m.d_6.isChecked(),
            self.time_m.d_7.isChecked(),
        ]
        hours = [
            self.time_m.h_1.isChecked(),
            self.time_m.h_2.isChecked(),
            self.time_m.h_3.isChecked(),
            self.time_m.h_4.isChecked(),
            self.time_m.h_5.isChecked(),
            self.time_m.h_6.isChecked(),
            self.time_m.h_7.isChecked(),
            self.time_m.h_7.isChecked(),
            self.time_m.h_8.isChecked(),
            self.time_m.h_9.isChecked(),
            self.time_m.h_10.isChecked(),
            self.time_m.h_11.isChecked(),
            self.time_m.h_12.isChecked(),
            self.time_m.h_13.isChecked(),
            self.time_m.h_14.isChecked(),
            self.time_m.h_15.isChecked(),
            self.time_m.h_16.isChecked(),
            self.time_m.h_17.isChecked(),
            self.time_m.h_18.isChecked(),
            self.time_m.h_19.isChecked(),
            self.time_m.h_20.isChecked(),
            self.time_m.h_21.isChecked(),
            self.time_m.h_22.isChecked(),
            self.time_m.h_23.isChecked(),
            self.time_m.h_24.isChecked(),
        ]
        self.help_data["month"] = month
        self.help_data["days"] = days
        self.help_data["hours"] = hours
        self.planer_m.btn_create_task.setEnabled(True)
        self.time_menu.close()

    # Updates for main menu

    def update_main_menu_browser(self):
        """
        Функция для обновления текста на главном меню. В соответствии с настройками.
        :return:
        """
        text = "Главное меню программы.\n" \
               "Здесь будет отображаться основная информация и подключенные БД.\n\n"
        for elem in self.SETTINGS["db_list"]:
            text += f"База данных: {elem['db_name']} (СУБД {elem['db_type']})\n"
        self.mm.textBrowser.setText(text)

    # Help functions

    def get_from_str_list(self, text: str):
        """
        Вспомогательная функция для перевода строкового значения в список элементов
        :param text:
        :return:
        """
        text = text.split()
        new_text = list()
        for elem in text:
            elem = elem.replace('[', '')
            elem = elem.replace(']', '')
            elem = elem[2:-4]
            new_text.append(elem)
        return new_text

    def create_script_file(self, file_name: str, content: str):
        """
        Функция для создания скрипта на python, где content - код, а file_name - название файла для скрипта
        :param file_name:
        :param content:
        :return:
        """
        with open(file_name, 'w', encoding="utf-8") as script_file:
            script_file.write(content)

    def create_bat_file(self, file_name: str, script_title: str, timing: int):
        """
        Функция для создания бат файла. file_name - название бат файла,
        script_title - название файла со скриптом на python,
        timing - время интервала между запуском функции скрипта.
        :param file_name:
        :param script_title:
        :param timing:
        :return:
        """
        bat_program = f"""
@echo off
:loop
python {script_title}
timeout /t {timing} > nul
goto loop
                                """
        self.SETTINGS["run_files"].append({"title": file_name, "timing": f"{timing}", "script": script_title})
        self.set_settings(self.settings_file)
        with open(file_name, 'w', encoding="utf-8") as bat_file:
            bat_file.write(bat_program)


if __name__ == "__main__":
    app = QtWidgets.QApplication([])
    application = MyWindow()
    application.show()

    sys.exit(app.exec())
