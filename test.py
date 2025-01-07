from scripts.optimize_indexes import Optimize_indexes

optimize_indexes = Optimize_indexes("localhost", "5432", "postgres", "123456", "pagila7")
optimize_indexes.search_for_unused_indexes_by_frequent_values("rental", "idx_rental_inventory_staff")