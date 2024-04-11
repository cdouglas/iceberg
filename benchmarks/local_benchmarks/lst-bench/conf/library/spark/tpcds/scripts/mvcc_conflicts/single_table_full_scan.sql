/* SELECT ss_quantity, COUNT(ss_quantity) FROM ${catalog}.${database}.store_sales GROUP BY ss_quantity; */
DELECT FROM ${catalog}.${database}.store_sales WHERE ss_quantity = 10;