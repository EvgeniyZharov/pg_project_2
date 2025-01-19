    def get_data_db(self):
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
