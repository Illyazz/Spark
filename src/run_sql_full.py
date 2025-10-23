# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("FullSQLQueriesWithOutput").getOrCreate()

# === Load all tables ===
orders = spark.read.csv("/user/Consultants/sqoop_data/orders", header=True, inferSchema=True)
#
order_items = spark.read.csv("/user/Consultants/sqoop_data/order_items", header=True, inferSchema=True)
customers = spark.read.csv("/user/Consultants/sqoop_data/customers", header=True, inferSchema=True)
products = spark.read.csv("/user/Consultants/sqoop_data/products", header=True, inferSchema=True)
shippers = spark.read.csv("/user/Consultants/sqoop_data/shippers", header=True, inferSchema=True)
statuses = spark.read.csv("/user/Consultants/sqoop_data/order_statuses", header=True, inferSchema=True)

# Register as SQL views
orders.createOrReplaceTempView("orders")
order_items.createOrReplaceTempView("order_items")
customers.createOrReplaceTempView("customers")
products.createOrReplaceTempView("products")
shippers.createOrReplaceTempView("shippers")
statuses.createOrReplaceTempView("order_statuses")

print("✅ Tables loaded and registered successfully!\n")

# === 1️⃣ Preview order_items ===
print("=== Preview: order_items ===")
order_items.show(10, truncate=False)

spark.sql("SELECT * FROM order_items").write.mode("overwrite").option("header", "true").csv("hdfs:///user/Consultants/tmp/DE011025/sql_results/order_items_preview")

# === 2️⃣ Customer total spending ===
spark.sql("""
CREATE OR REPLACE VIEW v_customer_order_value AS
SELECT
    c.customer_id,
    CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
    SUM(oi.quantity * oi.unit_price) AS total_spent
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
GROUP BY c.customer_id, c.first_name, c.last_name
ORDER BY total_spent DESC
""")

print("\n=== Customer Total Spending ===")
spark.sql("SELECT * FROM v_customer_order_value").show(10, truncate=False)

spark.sql("SELECT * FROM v_customer_order_value").write.mode("overwrite").option("header", "true").csv("hdfs:///user/Consultants/tmp/DE011025/sql_results/v_customer_order_value")

# === 3️⃣ Rank customers by spending ===
spark.sql("""
CREATE OR REPLACE VIEW v_customer_rank AS
SELECT
    customer_id,
    customer_name,
    total_spent,
    RANK() OVER (ORDER BY total_spent DESC) AS rank_by_spending
FROM v_customer_order_value
""")

print("\n=== Customer Rank by Spending ===")
spark.sql("SELECT * FROM v_customer_rank").show(10, truncate=False)

spark.sql("SELECT * FROM v_customer_rank").write.mode("overwrite").option("header", "true").csv("hdfs:///user/Consultants/tmp/DE011025/sql_results/v_customer_rank")

# === 4️⃣ Top products ===
spark.sql("""
CREATE OR REPLACE VIEW v_top_products AS
SELECT
    p.product_id,
    p.name AS product_name,
    SUM(oi.quantity) AS total_quantity_sold,
    SUM(oi.quantity * oi.unit_price) AS total_revenue,
    RANK() OVER (ORDER BY SUM(oi.quantity * oi.unit_price) DESC) AS product_rank
FROM order_items oi
JOIN products p ON oi.product_id = p.product_id
GROUP BY p.product_id, p.name
ORDER BY total_revenue DESC
""")

print("\n=== Top Products by Revenue ===")
spark.sql("SELECT * FROM v_top_products").show(10, truncate=False)

spark.sql("SELECT * FROM v_top_products").write.mode("overwrite").option("header", "true").csv("hdfs:///user/Consultants/tmp/DE011025/sql_results/v_top_products")

# === 5️⃣ Running total revenue ===
spark.sql("""
CREATE OR REPLACE VIEW v_revenue_running_total AS
SELECT
    o.order_date,
    SUM(oi.quantity * oi.unit_price) AS daily_revenue,
    SUM(SUM(oi.quantity * oi.unit_price)) OVER (ORDER BY o.order_date) AS running_total_revenue
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
GROUP BY o.order_date
ORDER BY o.order_date
""")

print("\n=== Running Total Revenue (by Order Date) ===")
spark.sql("SELECT * FROM v_revenue_running_total").show(10, truncate=False)

spark.sql("SELECT * FROM v_revenue_running_total").write.mode("overwrite").option("header", "true").csv("hdfs:///user/Consultants/tmp/DE011025/sql_results/v_revenue_running_total")

print("\n✅ All SQL results written to HDFS and printed to console successfully.")
spark.stop()
