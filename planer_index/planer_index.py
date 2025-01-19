    def update_boxes_db_analise(self):
        self.analizeingex_m.comboBox.clear()
        self.analizeingex_m.comboBox.addItem(" ")
        db_list = [elem["db_name"] for elem in self.SETTINGS["db_list"]]
        self.analizeingex_m.comboBox.addItems(db_list)

    def set_db_analise(self, text):
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
        if text:
            self.help_data["on_tables"] = [text]

    def choice_tables(self):
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

    def add_all_tables(self):
        self.help_data["on_tables"].extend(self.help_data["off_tables"])
        self.help_data["off_tables"] = list()
        self.choicetable_m.comboBox.clear()
        self.choicetable_m.comboBox_2.clear()
        self.choicetable_m.comboBox.addItems(self.help_data["on_tables"])
        self.choicetable_m.comboBox_2.addItems(self.help_data["off_tables"])

    def delete_all_tables(self):
        self.help_data["off_tables"].extend(self.help_data["on_tables"])
        self.help_data["on_tables"] = list()
        self.choicetable_m.comboBox.clear()
        self.choicetable_m.comboBox_2.clear()
        self.choicetable_m.comboBox.addItems(self.help_data["on_tables"])
        self.choicetable_m.comboBox_2.addItems(self.help_data["off_tables"])

    def save_tables(self):
        self.choicetable_menu.close()

    def choice_tables_add(self, text):
        if text:
            self.help_data["on_tables"].append(text)
            self.help_data["off_tables"].remove(text)
            self.choicetable_m.comboBox.clear()
            self.choicetable_m.comboBox_2.clear()
            self.choicetable_m.comboBox.addItems(self.help_data["on_tables"])
            self.choicetable_m.comboBox_2.addItems(self.help_data["off_tables"])

    def choice_tables_delete(self, text):
        if text:
            self.help_data["on_tables"].remove(text)
            self.help_data["off_tables"].append(text)
            self.choicetable_m.comboBox.clear()
            self.choicetable_m.comboBox_2.clear()
            self.choicetable_m.comboBox.addItems(self.help_data["on_tables"])
            self.choicetable_m.comboBox_2.addItems(self.help_data["off_tables"])

    def run_analise(self):
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
        self.planer_m.comboBox.clear()
        self.planer_m.comboBox.addItem(" ")
        db_list = [elem["db_name"] for elem in self.SETTINGS["db_list"]]
        self.planer_m.comboBox.addItems(db_list)

    def set_db_planer(self, text):
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
        if text:
            self.help_data["on_tables"] = [text]

    def set_index_planer(self, text):
        if text:
            self.help_data["on_indexes"] = [text]

    def choice_indexes(self):
        if self.help_data["on_indexes"]:
            self.choiceindex_m.comboBox.addItems(self.help_data["on_indexes"])
        index_list = [elem["title"] for elem in self.SETTINGS["index_list"]]
        self.help_data["off_indexes"] = [elem for elem in index_list if elem not in self.help_data["on_indexes"]]
        self.choiceindex_m.comboBox_2.addItems(self.help_data["off_indexes"])
        self.choiceindex_menu.show()

    def add_all_indexes(self):
        self.help_data["on_indexes"].extend(self.help_data["off_indexes"])
        self.help_data["off_indexes"] = list()
        self.choiceindex_m.comboBox.clear()
        self.choiceindex_m.comboBox_2.clear()
        self.choiceindex_m.comboBox.addItems(self.help_data["on_indexes"])
        self.choiceindex_m.comboBox_2.addItems(self.help_data["off_indexes"])

    def delete_all_indexes(self):
        self.help_data["off_indexes"].extend(self.help_data["on_indexes"])
        self.help_data["on_indexes"] = list()
        self.choiceindex_m.comboBox.clear()
        self.choiceindex_m.comboBox_2.clear()
        self.choiceindex_m.comboBox.addItems(self.help_data["on_indexes"])
        self.choiceindex_m.comboBox_2.addItems(self.help_data["off_indexes"])

    def save_indexes(self):
        self.choiceindex_menu.close()

    def choice_indexes_add(self, text):
        if text:
            self.help_data["on_indexes"].append(text)
            self.help_data["off_indexes"].remove(text)
            self.choiceindex_m.comboBox.clear()
            self.choiceindex_m.comboBox_2.clear()
            self.choiceindex_m.comboBox.addItems(self.help_data["on_indexes"])
            self.choiceindex_m.comboBox_2.addItems(self.help_data["off_indexes"])

    def choice_indexes_delete(self, text):
        if text:
            self.help_data["on_indexes"].remove(text)
            self.help_data["off_indexes"].append(text)
            self.choicetable_m.comboBox.clear()
            self.choicetable_m.comboBox_2.clear()
            self.choicetable_m.comboBox.addItems(self.help_data["on_indexes"])
            self.choicetable_m.comboBox_2.addItems(self.help_data["off_indexes"])

    def create_task(self):
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
