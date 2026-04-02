from pyspark.sql.functions import *
from pyspark.sql.types import *

customers_path = "/Volumes/workspace/default/etlprocessingsystem/customers.csv"
orders_path = "/Volumes/workspace/default/etlprocessingsystem/orders.csv"
products_path = "/Volumes/workspace/default/etlprocessingsystem/products.csv"
payments_path = "/Volumes/workspace/default/etlprocessingsystem/payments.json"

customers_df = spark.read.option("header", True).csv(customers_path)
orders_df = spark.read.option("header", True).csv(orders_path)
products_df = spark.read.option("header", True).csv(products_path)

payments_df = spark.read.json(payments_path)