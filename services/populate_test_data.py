import psycopg2
from psycopg2 import sql
import random
from datetime import datetime, timedelta

conn = psycopg2.connect(
    dbname="pagila",
    user="postgres",
    password="123456",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

def generate_rental_data():
    if random.random() < 0.9:
        customer_id = random.randint(1, 2)
    else:
        customer_id = random.randint(3, 599)

    staff_id = random.randint(1, 2)
    inventory_id = random.randint(1, 4581)
    rental_date = datetime.now() - timedelta(days=random.randint(0, 30))

    return rental_date, inventory_id, customer_id, staff_id

# Синхронизация последовательности
try:
    cur.execute("SELECT setval('rental_rental_id_seq', (SELECT COALESCE(MAX(rental_id), 0) FROM rental) + 1);")
    conn.commit()
except Exception as e:
    print("Не удалось синхронизировать последовательность:", e)

try:
    for _ in range(10000000):
        rental_date, inventory_id, customer_id, staff_id = generate_rental_data()

        # Вставка с игнорированием дублирующихся записей по комбинации полей
        cur.execute(
            sql.SQL(
                """
                INSERT INTO rental (rental_date, inventory_id, customer_id, staff_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (rental_date, inventory_id, customer_id)
                DO NOTHING
                """
            ),
            (rental_date, inventory_id, customer_id, staff_id)
        )

    conn.commit()
    print("Данные успешно вставлены")

    cur.execute("CREATE INDEX IF NOT EXISTS idx_rental_customer_id ON rental (customer_id);")
    conn.commit()
    print("Индекс для customer_id создан успешно")

except Exception as e:
    print("Произошла ошибка:", e)
    conn.rollback()

finally:
    cur.close()
    conn.close()
