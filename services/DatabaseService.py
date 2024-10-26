import psycopg2
import logging

class DatabaseService:

    def __init__(self):
        self.connection = None
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def get_connection(self, host, port, user, password, database):
        try:
            # Establish the connection to PostgreSQL
            self.connection = psycopg2.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                database=database
            )
            logging.info("Connected to PostgreSQL")
        except Exception as err:
            logging.error("Database connection failed!", exc_info=True)
            self.connection = None

        return self.connection

    def close_connection(self):
        if self.connection:
            self.connection.close()
            self.connection = None
            logging.info("Database connection closed")

