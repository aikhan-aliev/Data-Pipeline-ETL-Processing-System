from pyspark.sql.functions import current_timestamp, lit, monotonically_increasing_id


customers_path = "/Volumes/workspace/default/etlprocessingsystem/customers.csv"
orders_path = "/Volumes/workspace/default/etlprocessingsystem/orders.csv"
products_path = "/Volumes/workspace/default/etlprocessingsystem/products.csv"
payments_path = "/Volumes/workspace/default/etlprocessingsystem/payments.json"

customers_df = spark.read.option("header", True).csv(customers_path)
orders_df = spark.read.option("header", True).csv(orders_path)
products_df = spark.read.option("header", True).csv(products_path)

payments_df = spark.read.json(payments_path)

batch_id = "001"

def add_metadata(df, source_data):
    return (
    df
    .withColumn("ingestion_time", current_timestamp())
    .withColumn("source_data", lit(source_data))
    .withColumn("batch_id", lit(batch_id))
    .withColumn("row_record_id", monotonically_increasing_id())
    )

customers_df = add_metadata(customers_df, "customers.csv")
orders_df = add_metadata(orders_df, "orders.csv")
products_df = add_metadata(products_df, "products.csv")
payments_df = add_metadata(payments_df, "payments.json")

customers_df.write.mode("overwrite").saveAsTable("bronze_customers")
orders_df.write.mode("overwrite").saveAsTable("bronze_orders")
products_df.write.mode("overwrite").saveAsTable("bronze_products")
payments_df.write.mode("overwrite").saveAsTable("bronze_payments")