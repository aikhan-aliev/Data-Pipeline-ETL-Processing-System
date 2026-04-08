from pyspark.sql.functions import *

# GOLD LAYER — BUSINESS TABLES

silver_customers = spark.table("silver_customers")
silver_products = spark.table("silver_products")
silver_orders = spark.table("silver_orders")
silver_payments = spark.table("silver_payments")

# FACT TABLE

gold_order_facts = (
    silver_orders.alias("o")
    .join(silver_customers.alias("c"), col("o.customer_id") == col("c.customer_id"), "inner")
    .join(silver_products.alias("p"), col("o.product_id") == col("p.product_id"), "inner")
    .join(silver_payments.alias("pay"), col("o.order_id") == col("pay.order_id"), "left")
    .select(
        col("o.order_id"),
        col("o.order_date"),
        date_format(col("o.order_date"), "yyyy-MM").alias("order_month"),
        col("o.customer_id"),
        col("c.full_name"),
        col("c.country"),
        col("o.product_id"),
        col("p.product_name"),
        col("p.category"),
        col("o.quantity"),
        col("p.unit_price"),
        (col("o.quantity") * col("p.unit_price")).alias("order_total"),
        col("o.order_status"),
        col("pay.payment_id"),
        col("pay.payment_date"),
        col("pay.amount_paid"),
        col("pay.payment_status"),
        col("pay.payment_method"),
        when(col("pay.payment_status") == "paid", lit(True)).otherwise(lit(False)).alias("is_paid"),
        ((col("o.quantity") * col("p.unit_price")) - coalesce(col("pay.amount_paid"), lit(0.0))).alias("payment_gap")
    )
)

# CUSTOMER SUMMARY

gold_customer_summary = (
    gold_order_facts
    .groupBy("customer_id", "full_name", "country")
    .agg(
        countDistinct("order_id").alias("total_orders"),
        round(sum("order_total"), 2).alias("total_revenue"),
        round(avg("order_total"), 2).alias("avg_order_value"),
        sum(when(col("is_paid") == True, 1).otherwise(0)).alias("paid_orders"),
        sum(when(col("order_status") == "cancelled", 1).otherwise(0)).alias("cancelled_orders"),
        max("order_date").alias("last_order_date")
    )
)

# PRODUCT SUMMARY

gold_product_sales_summary = (
    gold_order_facts
    .groupBy("product_id", "product_name", "category")
    .agg(
        sum("quantity").alias("total_units_sold"),
        round(sum("order_total"), 2).alias("total_revenue"),
        countDistinct("customer_id").alias("unique_customers"),
        round(avg("quantity"), 2).alias("avg_quantity_per_order"),
        round(sum(when(col("is_paid") == True, col("order_total")).otherwise(0)), 2).alias("paid_revenue")
    )
)

# SAVE GOLD TABLES

spark.sql("DROP TABLE IF EXISTS gold_order_facts")
spark.sql("DROP TABLE IF EXISTS gold_customer_summary")
spark.sql("DROP TABLE IF EXISTS gold_product_sales_summary")

gold_order_facts.write.mode("overwrite").saveAsTable("gold_order_facts")
gold_customer_summary.write.mode("overwrite").saveAsTable("gold_customer_summary")
gold_product_sales_summary.write.mode("overwrite").saveAsTable("gold_product_sales_summary")

display(gold_order_facts)
display(gold_customer_summary)
display(gold_product_sales_summary)