from services.DatabaseService import Database_service

class Optimize_indexes:

    def __init__(self, host, port, user, password, database):
        self.database_service = Database_service()
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database


    def search_for_unused_indexes_by_frequent_values(self, table_name, index_name):

        def get_index_fields(index_name):
            """Получить список полей индекса (включая выражения)"""

            query = """
                SELECT 
                    CASE 
                        WHEN a.attname IS NOT NULL THEN a.attname
                        ELSE pg_get_expr(pg_index.indexprs, pg_index.indrelid, true)
                    END AS index_expression
                FROM 
                    pg_index
                CROSS JOIN 
                    generate_subscripts(pg_index.indkey, 1) AS n
                LEFT JOIN 
                    pg_attribute a 
                    ON a.attnum = pg_index.indkey[n] AND a.attrelid = pg_index.indrelid
                WHERE 
                    pg_index.indexrelid = %s::regclass;
            """

            result_cursor = self.database_service.execute_query(query, [index_name])

            return [(row[0] or row[1]) for row in result_cursor.fetchall()]

        def get_common_values(table_name, fields):
            """Получить частые значения и частоты для одиночного или составного индекса"""

            if len(fields) == 1:
                # Если поле одно, используем pg_stats

                query = """
                    SELECT most_common_vals, most_common_freqs
                        FROM pg_stats
                        WHERE tablename = %s AND attname = %s;
                """

                result_cursor = self.database_service.execute_query(query, [table_name, fields[0]])

                result = result_cursor.fetchone()

                if result and result[0]:
                    vals = [int(val) for val in result[0].strip('{}').split(',')]
                    freqs = result[1]
                    return list(zip(vals, freqs))

            else:
                # Если составной индекс, то выполняем запрос для получения частых значений
                query = "SELECT {}, COUNT(*) FROM {} GROUP BY {} ORDER BY COUNT(*) DESC LIMIT 10".format(
                    ', '.join(fields),  # Список полей через запятую
                    table_name,  # Название таблицы
                    ', '.join(fields)  # Список полей для GROUP BY
                )
                result_cursor = self.database_service.execute_query(query)
                results = result_cursor.fetchall()
                total_rows = get_row_count(table_name)
                values_with_freqs = [(tuple(row[:-1]), row[-1] / total_rows) for row in results]  # Добавляем частоту
                return values_with_freqs


        def get_row_count(table_name):
            """Получить общее количество строк в таблице"""
            query = f"SELECT COUNT(*) FROM {table_name};"
            result_cursor = self.database_service.execute_query(query)
            return result_cursor.fetchone()[0]

        def perform_analysis(table_name, index_size, values_with_freqs, condition_fields):
            """Анализ производительности индекса для заданных условий"""
            for val, freq in values_with_freqs:
                try:
                    # Убедимся, что val является итерируемым объектом
                    if not isinstance(val, (list, tuple)):
                        val = [val]  # Преобразуем в список, если это одно значение

                    # Формируем условия для WHERE
                    conditions = " AND ".join(f"{field} = %s" for field in condition_fields)

                    # Формируем полный запрос
                    query = f"EXPLAIN ANALYZE SELECT * FROM {table_name} WHERE {conditions}"

                    result_cursor = self.database_service.execute_query(query, val)

                    plan = result_cursor.fetchall()
                    scan_type = "Index Scan" if any("Index Scan" in row[0] for row in plan) else "Seq Scan"

                    # Используем размер индекса, скорректированный по частоте
                    estimated_index_weight = index_size * freq

                    print(
                        f"\nПлан выполнения для {dict(zip(condition_fields, val))} (частота {freq * 100:.2f}%): {scan_type}\n"
                        f"Примерная память для индекса: {estimated_index_weight / (1024 ** 2):.2f} Мб")
                except Exception as e:
                    print(f"Ошибка для условий {dict(zip(condition_fields, val))}: {e}")

        """Основной анализ индекса: одиночные поля и комбинации"""
        self.database_service.get_connection(self.host, self.port, self.user, self.password, self.database)

        index_fields = get_index_fields(index_name)
        print(f"Поля индекса {index_name}: {index_fields}")

        index_size = self.get_index_size(table_name, index_name)
        print(f"Индекс {index_name} занимает {index_size / (1024 ** 2):.2f} Мб")

        # Получаем значения для анализа, включая частоты
        common_values_with_freqs = get_common_values(table_name, index_fields)

        # Выполняем анализ на основе полученных значений и частот
        perform_analysis(table_name, index_size, common_values_with_freqs, index_fields)

        self.database_service.close_connection()


    def get_index_size(self, table_name, index_name):
        """Получить размер индекса"""

        query = """
            SELECT pg_relation_size(indexrelid) AS index_size
            FROM pg_index
            WHERE indrelid = %s::regclass
            AND indexrelid = %s::regclass;
        """

        result_cursor = self.database_service.execute_query(query, [table_name, index_name])

        return result_cursor.fetchone()[0]
