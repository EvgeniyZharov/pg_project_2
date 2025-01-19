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
