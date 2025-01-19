    def set_db_line(self, text):
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
        if text:
            self.help_data["type_request_for_db"] = text

    def push_request_db(self):
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
