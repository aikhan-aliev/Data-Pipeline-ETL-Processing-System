from pyspark.sql.functions import lit

# VALIDATION CHECKS

silver_customers = spark.table("silver_customers")
silver_products = spark.table("silver_products")
silver_orders = spark.table("silver_orders")
silver_payments = spark.table("silver_payments")

checks = []

# Orders missing customer reference
missing_customer_ref = (
    silver_orders.alias("o")
    .join(silver_customers.alias("c"), col("o.customer_id") == col("c.customer_id"), "left_anti")
)
checks.append(("orders_missing_customer_reference", missing_customer_ref.count()))

# Orders missing product reference
missing_product_ref = (
    silver_orders.alias("o")
    .join(silver_products.alias("p"), col("o.product_id") == col("p.product_id"), "left_anti")
)
checks.append(("orders_missing_product_reference", missing_product_ref.count()))

# Payments missing order reference
missing_order_ref = (
    silver_payments.alias("p")
    .join(silver_orders.alias("o"), col("p.order_id") == col("o.order_id"), "left_anti")
)
checks.append(("payments_missing_order_reference", missing_order_ref.count()))

# Duplicate customer ids
duplicate_customer_ids = (
    silver_customers.groupBy("customer_id").count().filter(col("count") > 1).count()
)
checks.append(("duplicate_customer_id_count", duplicate_customer_ids))

# Duplicate product ids
duplicate_product_ids = (
    silver_products.groupBy("product_id").count().filter(col("count") > 1).count()
)
checks.append(("duplicate_product_id_count", duplicate_product_ids))

# Duplicate order ids
duplicate_order_ids = (
    silver_orders.groupBy("order_id").count().filter(col("count") > 1).count()
)
checks.append(("duplicate_order_id_count", duplicate_order_ids))

# Duplicate payment ids
duplicate_payment_ids = (
    silver_payments.groupBy("payment_id").count().filter(col("count") > 1).count()
)
checks.append(("duplicate_payment_id_count", duplicate_payment_ids))

# Negative value checks
negative_unit_price_count = silver_products.filter(col("unit_price") < 0).count()
checks.append(("negative_unit_price_count", negative_unit_price_count))

negative_amount_paid_count = silver_payments.filter(col("amount_paid") < 0).count()
checks.append(("negative_amount_paid_count", negative_amount_paid_count))

non_positive_quantity_count = silver_orders.filter(col("quantity") <= 0).count()
checks.append(("non_positive_quantity_count", non_positive_quantity_count))

validation_summary_df = spark.createDataFrame(checks, ["check_name", "failed_count"])

spark.sql("DROP TABLE IF EXISTS silver_validation_summary")
validation_summary_df.write.mode("overwrite").saveAsTable("silver_validation_summary")

display(validation_summary_df)