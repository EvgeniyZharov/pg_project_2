import psycopg2
from psycopg2 import sql

# Подключаемся к базе данных
conn = psycopg2.connect(
    dbname="pagila",
    user="postgres",
    password="123456",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

cur.execute("""
    SELECT indexrelid::regclass AS index_name, pg_relation_size(indexrelid) AS index_size
    FROM pg_index
    WHERE indrelid = 'rental'::regclass AND indexrelid::regclass::text LIKE 'idx_rental_customer_id%';
""")
index_info = cur.fetchone()[1]

cur.execute("""
    SELECT most_common_vals, most_common_freqs
    FROM pg_stats
    WHERE tablename = 'rental' AND attname = 'customer_id';
""")

common_vals, common_freqs = cur.fetchone()

if common_vals:
    common_vals = [int(val) for val in common_vals.strip('{}').split(',')]

for (val, freq) in zip(common_vals, common_freqs):
    try:
        cur.execute(sql.SQL("EXPLAIN ANALYZE SELECT * FROM rental WHERE rental.customer_id = %s"), [val])
        plan = cur.fetchall()
        scan_type = "Index Scan" if any("Index Scan" in row[0] for row in plan) else "Seq Scan"
        print(f"\nПлан выполнения для id {val}: {scan_type}\n"
              f"Примерная память: {index_info * freq} Мб")
    except Exception as e:
        print(f"Произошла ошибка при выполнении запроса для customer_id = {val}: {e}")


cur.close()
conn.close()