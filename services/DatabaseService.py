import psycopg2
import logging

class Database_service:

    def __init__(self):
        self.connection = None
        self.cursor = None
        self.config = None
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def get_connection(self, host, port, user, password, database):
        self.config = {
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "database": database
        }
        return self._connect()

    def _connect(self):
        try:
            # Establish the connection to PostgreSQL
            self.connection = psycopg2.connect(**self.config)
            self.cursor = self.connection.cursor()
            logging.info("Connected to PostgreSQL")
        except Exception as err:
            logging.error("Database connection failed!", exc_info=True)
            self.connection = None
            self.cursor = None

        return self.cursor

    def _ensure_connection(self):
        """Ensure the connection and cursor are active, reconnect if necessary."""
        if not self.connection or self.connection.closed:
            logging.warning("Connection is closed. Attempting to reconnect...")
            self._connect()

        if self.connection and (not self.cursor or self.cursor.closed):
            try:
                self.cursor = self.connection.cursor()
                logging.info("Cursor reinitialized successfully")
            except Exception as err:
                logging.error("Failed to reinitialize cursor", exc_info=True)
                self.cursor = None

    def execute_query(self, query, params=None):
        self._ensure_connection()

        if not self.cursor:
            logging.error("Cannot execute query: No active database connection or cursor.")
            return None

        try:
            self.cursor.execute(query, params)
            logging.info("Query executed successfully")
            return self.cursor
        except Exception as err:
            logging.error("Failed to execute query", exc_info=True)
            return None

    def commit(self):
        self._ensure_connection()

        if self.connection:
            try:
                self.connection.commit()
                logging.info("Transaction committed")
            except Exception as err:
                logging.error("Failed to commit transaction", exc_info=True)

    def close_connection(self):
        if self.cursor:
            self.cursor.close()
            self.cursor = None
            logging.info("Cursor closed")

        if self.connection:
            self.connection.close()
            self.connection = None
            logging.info("Database connection closed")
