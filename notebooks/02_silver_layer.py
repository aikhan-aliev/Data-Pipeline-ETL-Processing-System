# SILVER LAYER

from pyspark.sql.functions import *

# LOAD BRONZE TABLES

customers_df = spark.table("bronze_customers")
products_df = spark.table("bronze_products")
orders_df = spark.table("bronze_orders")
payments_df = spark.table("bronze_payments")

# ================= CUSTOMERS ==============

customers_df = (
    customers_df
    .withColumn("full_name", trim(col("full_name")))
    .withColumn("email", trim(lower(col("email"))))
    .withColumn("country", trim(lower(col("country"))))
    .withColumn("signup_date", try_to_date(col("signup_date"), "yyyy-MM-dd"))
)

customers_valid_cond = (
    col("customer_id").isNotNull() &
    col("email").isNotNull() &
    col("email").contains("@") &
    col("signup_date").isNotNull()
)

valid_customers = customers_df.filter(customers_valid_cond)

invalid_customers = customers_df.filter(~customers_valid_cond) \
    .withColumn("source_table", lit("customers")) \
    .withColumn("failed_rule", lit("basic_validation_failed")) \
    .withColumn("quarantine_timestamp", current_timestamp())


# ================= PRODUCTS ==============

products_df = (
    products_df
    .withColumn("product_name", trim(col("product_name")))
    .withColumn("category", trim(lower(col("category"))))
    .withColumn("unit_price", col("unit_price").cast("double"))
    .withColumn("is_active", col("is_active").cast("int"))
)

products_valid_cond = (
    col("product_id").isNotNull() &
    col("product_name").isNotNull() &
    col("unit_price").isNotNull() &
    (col("unit_price") >= 0)
)

valid_products = products_df.filter(products_valid_cond)

invalid_products = products_df.filter(~products_valid_cond) \
    .withColumn("source_table", lit("products")) \
    .withColumn("failed_rule", lit("basic_validation_failed")) \
    .withColumn("quarantine_timestamp", current_timestamp())


# ================= ORDERS ==============

orders_df = (
    orders_df
    .withColumn("order_status", trim(lower(col("order_status"))))
    .withColumn("order_date", try_to_date(col("order_date"), "yyyy-MM-dd"))
    .withColumn("quantity", col("quantity").cast("int"))
)

valid_order_statuses = ["pending", "shipped", "delivered", "cancelled"]

orders_valid_cond = (
    col("order_id").isNotNull() &
    col("customer_id").isNotNull() &
    col("product_id").isNotNull() &
    col("quantity").isNotNull() &
    (col("quantity") > 0) &
    col("order_date").isNotNull() &
    col("order_status").isin(valid_order_statuses)
)

valid_orders = orders_df.filter(orders_valid_cond)

invalid_orders = orders_df.filter(~orders_valid_cond) \
    .withColumn("source_table", lit("orders")) \
    .withColumn("failed_rule", lit("basic_validation_failed")) \
    .withColumn("quarantine_timestamp", current_timestamp())


# ================= PAYMENTS ==============

payments_df = (
    payments_df
    .withColumn("payment_status", trim(lower(col("payment_status"))))
    .withColumn("payment_method", trim(lower(col("payment_method"))))
    .withColumn("payment_date", try_to_date(col("payment_date"), "yyyy-MM-dd"))
    .withColumn("amount_paid", col("amount_paid").cast("double"))
)

valid_payment_statuses = ["paid", "failed", "refunded"]
valid_payment_methods = ["card", "paypal", "bank_transfer", "cash"]

payments_valid_cond = (
    col("payment_id").isNotNull() &
    col("order_id").isNotNull() &
    col("amount_paid").isNotNull() &
    (col("amount_paid") >= 0) &
    col("payment_status").isin(valid_payment_statuses) &
    col("payment_method").isin(valid_payment_methods)
)

valid_payments = payments_df.filter(payments_valid_cond)

invalid_payments = payments_df.filter(~payments_valid_cond) \
    .withColumn("source_table", lit("payments")) \
    .withColumn("failed_rule", lit("basic_validation_failed")) \
    .withColumn("quarantine_timestamp", current_timestamp())


# ============ QUARANTINE TABLE ========

silver_quarantine = (
    invalid_customers
    .unionByName(invalid_products, allowMissingColumns=True)
    .unionByName(invalid_orders, allowMissingColumns=True)
    .unionByName(invalid_payments, allowMissingColumns=True)
)

# SAVE SILVER TABLES

valid_customers.write.mode("overwrite").saveAsTable("silver_customers")
valid_products.write.mode("overwrite").saveAsTable("silver_products")
valid_orders.write.mode("overwrite").saveAsTable("silver_orders")
valid_payments.write.mode("overwrite").saveAsTable("silver_payments")

silver_quarantine.write \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("silver_quarantine")
# DEBUG

print("Customers:", valid_customers.count())
print("Products:", valid_products.count())
print("Orders:", valid_orders.count())
print("Payments:", valid_payments.count())
print("Quarantine:", silver_quarantine.count())

display(valid_customers)
display(valid_products)
display(valid_orders)
display(valid_payments)
display(silver_quarantine)