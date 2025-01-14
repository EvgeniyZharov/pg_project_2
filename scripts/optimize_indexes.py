from services.DatabaseService import Database_service
import re
import pandas as pd
from statistics import mean

class Optimize_indexes:

    def __init__(self, host, port, user, password, database):
        self.database_service = Database_service()
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database

    def search_for_unused_indexes_by_frequent_values(self, index_name):

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

            result_cursor = self.database_service.execute_query(query, (index_name,))

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

                result_cursor = self.database_service.execute_query(query, (table_name, fields[0]))
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

        index_size = self.get_index_size(index_name)
        print(f"Индекс {index_name} занимает {index_size / (1024 ** 2):.2f} Мб")

        table_name = self.get_info_by_index_name(index_name)[1]

        # Получаем значения для анализа, включая частоты
        common_values_with_freqs = get_common_values(table_name, index_fields)

        # Выполняем анализ на основе полученных значений и частот
        perform_analysis(table_name, index_size, common_values_with_freqs, index_fields)

        self.database_service.close_connection()

    def search_for_indexes_by_expressions(self):
        # Получение запросов из pg_stat_statements
        def get_queries_from_pg_stat_statements():
            """Получить запросы из pg_stat_statements с количеством вызовов и общей длительностью."""
            query = """
                SELECT query, calls, mean_exec_time
                FROM pg_stat_statements
                WHERE query IS NOT NULL
            """
            result_cursor = self.database_service.execute_query(query)
            results = result_cursor.fetchall()
            return results

        # Очистка выражения и индекса от параметров и подстановок, нормализация к нижнему регистру
        def clean_expression(expression):
            """
            Очистка выражения от параметров функций и подстановок, таких как $1 или 'month' в EXTRACT.
            Пример: 'EXTRACT($1 FROM rental_date)' -> 'extract(rental_date)'
                    'EXTRACT(month FROM rental_date)' -> 'extract(rental_date)'
            """
            # Убираем подстановки вида $1, $2 и т.д.
            expression = re.sub(r'\$\d+', '', expression, flags=re.IGNORECASE)

            # Убираем первый параметр внутри функции и нормализуем к нижнему регистру
            expression = re.sub(r'\b(\w+)\s*\(\s*\w*\s*FROM\s+(.+?)\s*\)', r'\1(\2)', expression, flags=re.IGNORECASE)

            # Убираем лишние пробелы и переводим в нижний регистр
            expression = re.sub(r'\s+', ' ', expression).strip().lower()

            return expression

        # Извлечение колонки из выражения
        def extract_column_from_expression(expression_content):
            """
            Извлекает название колонки из выражения (например, из EXTRACT или других).
            """
            match = re.search(r'from\s+(\w+)', expression_content, re.IGNORECASE)
            return match.group(1) if match else expression_content

        def check_index_usage(table_name, column_name, expression=None):
            """
            Проверка наличия индекса на колонку или выражение.
            """
            query = """
                    SELECT
                        i.indexrelid::regclass::text AS indexname,
                        array_agg(a.attname ORDER BY a.attnum) AS indexed_columns,
                        i.indexprs IS NOT NULL AS is_expression
                    FROM pg_index i
                    JOIN pg_class c ON c.oid = i.indrelid
                    LEFT JOIN pg_attribute a ON a.attnum = ANY(i.indkey) AND a.attnum > 0 AND a.attrelid = c.oid
                    WHERE c.relname = %s
                      AND i.indisvalid -- Только валидные индексы
                      AND NOT i.indisprimary -- Исключаем индексы для первичных ключей
                    GROUP BY i.indexrelid, i.indexprs
                    ORDER BY indexname;
                """

            result_cursor = self.database_service.execute_query(query, (table_name,))
            index_data = result_cursor.fetchall()

            relevant_indexes = {
                "column_index": [],
                "expression_index": [],
                "expression_first_in_composite": []
            }

            cleaned_expression = clean_expression(expression) if expression else None

            for indexname, indexed_column, is_expression in index_data:
                # Проверяем наличие индекса на колонку
                if column_name in indexed_column and not is_expression:
                    relevant_indexes["column_index"].append(indexname)

                # Проверяем наличие индекса для выражения
                if cleaned_expression and is_expression:
                    query = """
                            SELECT pg_get_expr(i.indexprs, i.indrelid)
                            FROM pg_index i
                            WHERE i.indexrelid = %s::regclass
                        """
                    result_cursor = self.database_service.execute_query(query, (indexname,))
                    expression_definition = clean_expression(result_cursor.fetchone()[0])
                    if expression_definition == cleaned_expression:
                        relevant_indexes["expression_index"].append(indexname)

            return relevant_indexes

        def analyze_queries():
            """Анализ запросов на наличие выражений с функциями и проверка использования индексов."""
            # Регулярное выражение для поиска функций (например, EXTRACT)
            expression_pattern = re.compile(
                r'\b(LOWER|UPPER|ABS|ROUND|SUBSTR|DATE_TRUNC|EXTRACT|DATE_PART|AGE|DATE_ADD|DATE_SUB)\s*\(\s*([^\)]+)\)',
                re.IGNORECASE
            )

            # Регулярное выражение для отсеивания команд, не относящихся к выборке данных
            non_select_pattern = re.compile(
                r'^\s*(CREATE|SET|DROP|SHOW|ALTER|COMMIT|BEGIN|EXPLAIN|VACUUM|GRANT|REVOKE|TRUNCATE)', re.IGNORECASE)

            queries = get_queries_from_pg_stat_statements()

            for query, calls, total_time in queries:
                # Отфильтровываем команды, не относящиеся к выборке данных
                if non_select_pattern.match(query):
                    continue

                # Ищем выражения по шаблону в каждом запросе
                matches = expression_pattern.findall(query)

                if matches:
                    for match in matches:
                        func = match[0]
                        expression_content = match[1]

                        # Пытаемся извлечь имя таблицы
                        table_match = re.search(r'FROM\s+(\w+)', query, re.IGNORECASE)
                        if table_match:
                            table_name = table_match.group(1)

                            # Очищаем выражение для нормализованного поиска индекса
                            cleaned_expression_content = clean_expression(f"{func}({expression_content})")
                            column_name = extract_column_from_expression(expression_content)
                            indexes = check_index_usage(table_name, column_name, cleaned_expression_content)

                            # Проверка наличия индексов
                            column_index_exists = bool(indexes["column_index"])
                            expression_index_exists = bool(indexes["expression_index"])

                            avg_time = total_time / calls if calls > 0 else 0  # Среднее время выполнения

                            # Если индекс по колонке есть, а по выражению нет
                            if column_index_exists and not expression_index_exists:
                                print(
                                    f"Индекс по колонке '{column_name}' в таблице '{table_name}' существует, но по выражению {cleaned_expression_content} отсутствует. "
                                    f"Количество вызовов: {calls}, Среднее время выполнения: {avg_time:.2f} ms")

        self.database_service.get_connection(self.host, self.port, self.user, self.password, self.database)
        analyze_queries()
        self.database_service.close_connection()

    def determining_effectiveness_of_index(self, index_name):

        # Функция для выполнения SQL-запроса с очисткой кэша
        def execute_query(query):
            try:
                # Создание нового соединения
                cursor = self.database_service.get_connection(self.host, self.port, self.user, self.password, self.database)

                # Отключаем планы и настраиваем параметры
                cursor.execute("DISCARD PLANS;")  # Отключаем кэширование
                cursor.execute("SET LOCAL enable_seqscan = OFF;")  # Отключаем последовательное сканирование
                cursor.execute(
                    "SET LOCAL random_page_cost = 1000;")  # Устанавливаем высокую стоимость случайного доступа
                cursor.execute("EXPLAIN ANALYZE " + query)  # Выполняем запрос с анализом
                results = cursor.fetchall()

                # Извлечение строки с временем выполнения
                execution_time_line = next(
                    line[0] for line in results if "Execution Time" in line[0]
                )
                execution_time = float(execution_time_line.split(":")[1].strip().replace(" ms", ""))

                # Закрытие соединения
                self.database_service.close_connection()

                return execution_time

            except Exception as e:
                print(f"Ошибка при выполнении запроса: {e}")
                return None

        # Функция для получения количества изменений таблицы и запросов к таблице
        def get_information_changes_and_selections(table_name):
            query = f"""
                    SELECT seq_scan + idx_scan AS sel_cnt, n_tup_ins + n_tup_upd + n_tup_del AS cng_cnt
                    FROM pg_stat_all_tables
                    WHERE relname = %s
                """
            result_cursor = self.database_service.execute_query(query, (table_name, ))
            rows = result_cursor.fetchall()
            return rows[0]

        # Функция для анализа распределения данных с учетом условия
        def analyze_distribution(table, column, condition=None):
            where_clause = f"WHERE {condition}" if condition else ""
            query = f"""
                SELECT {column}, COUNT(*) as frequency
                FROM {table}
                {where_clause}
                GROUP BY {column}
                ORDER BY frequency DESC;
            """
            result_cursor = self.database_service.execute_query(query)
            rows = result_cursor.fetchall()
            df = pd.DataFrame(rows, columns=["value", "frequency"])
            return df

        # Функция для получения информации о колонках и их типах
        def get_table_columns_with_types(table):
            query = f"""
                SELECT
                    c.column_name,
                    c.data_type,
                    c.is_nullable,
                    c.column_default,
                    tc.constraint_type,
                    kcu.table_name AS referenced_table,
                    kcu.column_name AS referenced_column
                FROM information_schema.columns c
                LEFT JOIN information_schema.key_column_usage kcu
                    ON c.table_name = kcu.table_name AND c.column_name = kcu.column_name
                LEFT JOIN information_schema.table_constraints tc
                    ON kcu.constraint_name = tc.constraint_name
                WHERE c.table_name = %s
                  AND (tc.constraint_type IS NULL OR tc.constraint_type <> 'PRIMARY KEY');
            """
            result_cursor = self.database_service.execute_query(query, (table,))
            rows = result_cursor.fetchall()

            columns_info = []
            for row in rows:
                columns_info.append({
                    "column_name": row[0],
                    "data_type": row[1],
                    "is_nullable": row[2] == 'YES',
                    "column_default": row[3],
                    "constraint_type": row[4],
                    "referenced_table": row[5],
                    "referenced_column": row[6],
                })
            return columns_info

        # Функция для получения доступных значений из связанной таблицы
        def get_foreign_key_values(referenced_table, referenced_column):
            query = f"SELECT DISTINCT {referenced_column} FROM {referenced_table} LIMIT 3;"
            result_cursor = self.database_service.execute_query(query)
            rows = result_cursor.fetchall()
            return [row[0] for row in rows]

        # Функция для генерации значений на основе типа и ограничений
        def generate_value(column_info):
            if column_info["constraint_type"] == "FOREIGN KEY":
                # Получить значения из связанной таблицы
                return get_foreign_key_values(column_info["referenced_table"], column_info["referenced_column"])
            elif column_info["data_type"] in ["integer", "bigint", "smallint"]:
                return [1, 10, 100]  # Пример значений
            elif column_info["data_type"] == "text":
                return ["example1", "example2", "example3"]
            elif column_info["data_type"] == "timestamp without time zone":
                return ["2024-01-01 00:00:00", "2024-06-01 12:00:00", "2024-12-31 23:59:59"]
            elif column_info["data_type"] == "boolean":
                return [True, False]
            else:
                return ["default_value"]

        # Функция для клонирования таблицы
        def clone_table(original_table, cloned_table):
            try:
                self.database_service.execute_query(f"DROP TABLE IF EXISTS {cloned_table}")
                self.database_service.execute_query(f"CREATE TABLE {cloned_table} AS TABLE {original_table} WITH NO DATA")
                self.database_service.execute_query(f"INSERT INTO {cloned_table} SELECT * FROM {original_table}")
                print(f"Таблица '{cloned_table}' успешно создана как копия '{original_table}'")
            except Exception as e:
                print(f"Ошибка при создании копии таблицы: {e}")

        # Функция для удаления таблицы
        def drop_table(table):
            try:
                self.database_service.execute_query(f"DROP TABLE IF EXISTS {table}")
                print(f"Таблица '{table}' успешно удалена")
            except Exception as e:
                print(f"Ошибка при удалении таблицы: {e}")

        # Основной процесс тестирования с клонированием таблицы
        def test_index_performance_with_clone(index_name, index_definition, original_table):

            # Имя клонированной таблицы
            cloned_table = original_table + "_test"
            cloned_index_definition = index_definition.replace(f"{original_table} USING", f"{cloned_table}  USING")
            cloned_index_definition = cloned_index_definition.replace(f"INDEX {index_name}", f"INDEX {index_name}_test")

            # Создание копии таблицы
            clone_table(original_table, cloned_table)
            self.database_service.commit()

            # Выполнение тестирования
            test_index_performance(cloned_index_definition, cloned_table, original_table)

            drop_table(cloned_table)
            self.database_service.commit()

        def test_index_performance(index_definition, cloned_table, original_table):

            # Извлечение таблицы и колонок из определения индекса
            index_body = index_definition.split("ON")[1].strip().split("(")[1].split(")")[0]
            columns_or_expressions = [part.strip() for part in index_body.split(",")]

            condition = None
            if "WHERE" in index_definition.upper():
                condition = index_definition.split("WHERE", 1)[1].strip()

            # Получение информации о колонках таблицы
            columns_info = get_table_columns_with_types(cloned_table)

            # Анализ распределения данных для индексируемых колонок
            print(f"Анализ распределения данных для таблицы '{cloned_table}'...")
            distributions = {}
            for col_or_expr in columns_or_expressions:
                col_name = col_or_expr.split("(")[-1].split(")")[0] if "(" in col_or_expr else col_or_expr
                try:
                    distributions[col_name] = analyze_distribution(cloned_table, col_name, condition)
                except Exception as e:
                    print(f"Не удалось проанализировать данные для {col_or_expr}: {e}")

            # Генерация значений для тестирования
            test_values = {}
            for column_info in columns_info:
                col_name = column_info["column_name"]
                if col_name in distributions:
                    # Используем распределение для индексируемых колонок
                    test_values[col_name] = [
                        distributions[col_name].iloc[0]["value"],  # Частое значение
                        distributions[col_name].iloc[-1]["value"],  # Редкое значение
                        distributions[col_name].iloc[len(distributions[col_name]) // 2]["value"]  # Среднее значение
                    ]
                else:
                    # Генерация значений на основе типа и ограничений
                    test_values[col_name] = generate_value(column_info)

            # Генерация комбинаций значений для тестирования
            test_combinations = list(zip(
                *(test_values[col] for col in distributions)  # Используем только колонки из индекса
            ))

            # Генерация запросов для вставки
            print("Генерация данных для вставки...")
            insert_queries = []
            num_records = len(test_values['customer_id'])

            for index in range(num_records):
                values = []
                for key in test_values:
                    value = test_values[key][index]
                    # Приведение Timestamp и строк к нужному формату
                    if isinstance(value, str):
                        values.append(f"'{value}'")
                    elif isinstance(value, pd.Timestamp):
                        values.append(f"'{value}'")
                    else:
                        values.append(str(value))

                # Формирование строки с колонками и значениями для запроса
                columns = ", ".join(test_values.keys())
                formatted_values = ", ".join(values)

                # Формирование SQL-запроса
                insert_queries.append(f"INSERT INTO {cloned_table} ({columns}) VALUES ({formatted_values});")

            # Генерация запросов для тестирования выборки
            select_queries = []
            for combination in test_combinations:
                conditions = " AND ".join([
                    f"{col} = '{val}'" for col, val in zip(distributions.keys(), combination)
                ])
                if condition:
                    full_condition = f"{conditions} AND {condition}"
                else:
                    full_condition = conditions
                select_queries.append(f"SELECT * FROM {cloned_table} WHERE {full_condition}")

            # Выполнение запросов, чтобы убрать влияние кэша
            execute_query(select_queries[0])
            execute_query(insert_queries[0])

            # Тестирование операций выборки
            print("Тестирование запросов на выборку...")
            select_times_without_index = [execute_query(query) for query in select_queries]

            # Тестирование операций вставки
            print("Тестирование вставки без индекса...")
            insert_times_without_index = [execute_query(query) for query in insert_queries]

            # Создание индекса
            print("Создание индекса...")
            self.database_service.execute_query(index_definition)
            self.database_service.commit()

            # Тестирование с индексом
            print("Тестирование запросов с индексом...")
            select_times_with_index = [execute_query(query) for query in select_queries]

            print("Тестирование вставки с индексом...")
            insert_times_with_index = [execute_query(query) for query in insert_queries]

            # Удаление индекса
            print("Удаление индекса...")
            self.database_service.execute_query(f"DROP INDEX IF EXISTS {index_definition.split(' ')[2]}")
            self.database_service.commit()

            # Результаты
            information_changes_and_selections = get_information_changes_and_selections(original_table)
            print(
                f'\nСуммарное количество изменений по таблице (обновление, добавление, удаление): {information_changes_and_selections[1]}\n'
                f'Суммарное количество запросов на выборку: {information_changes_and_selections[0]}\n'
                f'Отношение количества изменений к количестве на выборку: {information_changes_and_selections[1] / information_changes_and_selections[0]}')
            print("\nРезультаты тестирования:")
            print("1. Время выполнения операций выборки:")
            for i, combination in enumerate(test_combinations):
                print(f"  Комбинация значений: {combination}")
                print(f"    Без индекса: {select_times_without_index[i]:.3f} ms")
                print(f"    С индексом:  {select_times_with_index[i]:.3f} ms")

            print("\n2. Время выполнения операций вставки:")
            for i, combination in enumerate(test_combinations):
                print(f"  Комбинация значений: {combination}")
                print(f"    Без индекса: {insert_times_without_index[i]:.3f} ms")
                print(f"    С индексом:  {insert_times_with_index[i]:.3f} ms")

            # Коэффициенты поэлементного деления
            select_ratios = [
                without / with_index if with_index != 0 else None
                for without, with_index in zip(select_times_without_index, select_times_with_index)
            ]
            insert_ratios = [
                with_index / without if without != 0 else None
                for without, with_index in zip(insert_times_without_index, insert_times_with_index)
            ]

            # Усреднённые коэффициенты
            avg_select_ratio = mean([r for r in select_ratios if r is not None])
            avg_insert_ratio = mean([r for r in insert_ratios if r is not None])

            # Вывод
            print("\nСредние значения:")
            print(f"  Средний коэффициент поиска(without_index / with_index): {avg_select_ratio:.2f}")

            print(f"  Средний коэффициент вставки(with_index / without_index): {avg_insert_ratio:.2f}")

        self.database_service.get_connection(self.host, self.port, self.user, self.password, self.database)
        info_by_index_name = self.get_info_by_index_name(index_name)
        index_definition = info_by_index_name[4]
        original_table = info_by_index_name[1]
        index_name = info_by_index_name[2]
        test_index_performance_with_clone(index_name, index_definition, original_table)
        self.database_service.close_connection()

    def get_index_size(self, index_name):
        """Получить размер индекса"""

        query = """
            SELECT pg_relation_size(indexrelid) AS index_size
            FROM pg_index
            WHERE indexrelid = %s::regclass;
        """

        result_cursor = self.database_service.execute_query(query, (index_name,))

        return result_cursor.fetchone()[0]

    def get_info_by_index_name(self, index_name):
        query = """
            SELECT * 
            FROM pg_indexes
            WHERE indexname = %s
            """
        result_cursor = self.database_service.execute_query(query, (index_name,))
        index_data = result_cursor.fetchall()

        if not index_data:
            print(f"Индекс {index_name} не найден.")
            return

        return index_data[0]

    def get_index_names_for_search(self):
        query = """
        SELECT i.indexname 
        FROM pg_indexes i
        LEFT JOIN index_usage_stats u ON i.indexname = u.index_name
        WHERE i.schemaname NOT IN ('pg_catalog', 'information_schema')
        AND i.indexname NOT IN (
            SELECT conname 
            FROM pg_constraint 
            WHERE contype = 'p'
        )
        """
        result_cursor = self.database_service.execute_query(query)
        index_data = result_cursor.fetchall()

        if not index_data:
            return

        return index_data
