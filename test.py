from scripts.optimize_indexes import Optimize_indexes

optimize_indexes = Optimize_indexes("localhost", "5432", "postgres", "123456", "pagila7")
#optimize_indexes.search_for_unused_indexes_by_frequent_values("idx_rental_inventory_staff")
#optimize_indexes.search_for_indexes_by_expressions()
optimize_indexes.determining_effectiveness_of_index("idx_rental_rental_date")