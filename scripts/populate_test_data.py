import psycopg2
from psycopg2 import sql
import random
from datetime import datetime, timedelta

conn = psycopg2.connect(
    dbname="pagila2",
    user="postgres",
    password="123456",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

def generate_rental_data():
    if random.random() < 0.8:
        customer_id = random.randint(1, 5)
    else:
        customer_id = random.randint(6, 599)

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
    for _ in range(100000):
        rental_date, inventory_id, customer_id, staff_id = generate_rental_data()
        
        cur.execute(
            sql.SQL("INSERT INTO rental (rental_date, inventory_id, customer_id, staff_id) VALUES (%s, %s, %s, %s)"),
            (rental_date, inventory_id, customer_id, staff_id)
        )

    conn.commit()
    print("Данные успешно вставлены")

except Exception as e:
    print("Произошла ошибка:", e)
    conn.rollback()

finally:
    cur.close()
    conn.close()
