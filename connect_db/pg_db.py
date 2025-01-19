import psycopg2


class PostgresClient:
    def __init__(self, db_name: str, user: str, password: str, host: str, port: str):
        self.db_name = db_name
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.conn = self.connect()

    def connect(self):
        try:
            connection = psycopg2.connect(
                dbname=self.db_name,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            print("Подключено успешно.")
            return connection
        except Exception:
            print("Произошла ошибка.")

    def get_data(self, request: str):
        try:
            cursor = self.conn.cursor()
            cursor.execute(request)
            result = cursor.fetchall()
            cursor.close()
            return str(result)
        except Exception as ex:
            return str(ex)

    def set_data(self, request: str):
        try:
            cursor = self.conn.cursor()
            cursor.execute(request)
            self.conn.commit()
            cursor.close()
            return "Ok"
        except Exception as ex:
            return str(ex)

    def close_db(self):
        self.conn.close()
