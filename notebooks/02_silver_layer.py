from pyspark.sql.functions import *


silver_customers = spark.table("bronze_customers")
silver_orders = spark.table("bronze_orders")
silver_products = spark.table("bronze_products")
silver_payments = spark.table("bronze_payments")

silver_customers = (
    silver_customers
    .withColumn("full_name", trim(col("full_name")))
    .withColumn("email", trim(lower(col("email"))))
    .withColumn("country", trim(lower(col("country"))))
    .withColumn("signup_date", try_to_date(col("signup_date"), "yyyy-MM-dd"))
)

valid_cond = (
    col("customer_id").isNotNull() &
    col("email").isNotNull() & 
    col("email").contains("@") &
    col("signup_date").isNotNull()
)
valid_customers = silver_customers.filter(valid_cond)
invalid_customers = silver_customers.filter(~valid_cond)

invalid_customers = (
        invalid_customers
        .withColumn("source_table", lit("silver_customers"))
        .withColumn("failed_rule", lit("basic_validation_failed"))
        .withColumn("date_timestamp", current_timestamp())
)

valid_customers.write.mode("overwrite").saveAsTable("silver_customers")
invalid_customers.write.mode("overwrite").saveAsTable("silver_quarantine")
silver_quarantine = spark.table("silver_quarantine")

from pyspark.sql.functions import *


silver_customers = spark.table("bronze_customers")
silver_orders = spark.table("bronze_orders")
silver_products = spark.table("bronze_products")
silver_payments = spark.table("bronze_payments")

silver_customers = (
    silver_customers
    .withColumn("full_name", trim(lower(col("full_name"))))
    .withColumn("email", trim(lower(col("email"))))
    .withColumn("country", trim(lower(col("country"))))
    .withColumn("signup_date", try_to_date(col("signup_date"), "yyyy-MM-dd"))
)

valid_cond = (
    col("customer_id").isNotNull() &
    col("email").isNotNull() & 
    col("email").contains("@") &
    col("signup_date").isNotNull()
)
valid_customers = silver_customers.filter(valid_cond)
invalid_customers = silver_customers.filter(~valid_cond)

invalid_customers = (
        invalid_customers
        .withColumn("source_table", lit("silver_customers"))
        .withColumn("failed_rule", lit("basic_validation_failed"))
        .withColumn("date_timestamp", current_timestamp())
)

valid_customers.write.mode("overwrite").saveAsTable("silver_customers")
invalid_customers.write.mode("overwrite").saveAsTable("silver_quarantine")

display(silver_customers)
display(silver_quarantine)

